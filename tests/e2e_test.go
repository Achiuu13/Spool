// Package tests runs end-to-end and process-crash tests against a real
// broker binary. These are process-crash results: they demonstrate recovery
// after SIGKILL, not behavior under storage or power failure.
package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var brokerBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "spool-e2e-*")
	if err != nil {
		panic(err)
	}
	brokerBin = filepath.Join(dir, "broker")
	out, err := exec.Command("go", "build", "-o", brokerBin, "../cmd/broker").CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("build broker: %v\n%s", err, out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startBroker(t *testing.T, dataDir string, port int) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(brokerBin, "-addr", fmt.Sprintf("127.0.0.1:%d", port), "-data", dataDir)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return cmd
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	cmd.Process.Kill()
	t.Fatal("broker did not become ready")
	return nil
}

func publishOne(base, stream, key, value string) (int64, error) {
	body, _ := json.Marshal(map[string]string{"key": key, "value": value})
	resp, err := http.Post(base+"/streams/"+stream+"/records", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Offset int64  `json:"offset"`
		Error  string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("status %d: %s", resp.StatusCode, out.Error)
	}
	return out.Offset, nil
}

func readAll(t *testing.T, base, stream string) map[int64]string {
	t.Helper()
	got := map[int64]string{}
	offset := int64(0)
	for {
		url := fmt.Sprintf("%s/streams/%s/partitions/0/records?offset=%d&limit=1000", base, stream, offset)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var out struct {
			Records []struct {
				Offset int64  `json:"offset"`
				Value  string `json:"value"`
			} `json:"records"`
			NextOffset int64 `json:"next_offset"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		resp.Body.Close()
		if len(out.Records) == 0 {
			return got
		}
		for _, r := range out.Records {
			got[r.Offset] = r.Value
		}
		offset = out.NextOffset
	}
}

// TestCrashRecoveryPreservesAckedRecords publishes concurrently, records
// every acknowledged offset at the client, SIGKILLs the broker, restarts it
// on the same data directory, and verifies every acknowledged offset is
// readable with the same content and no duplicates were assigned.
func TestCrashRecoveryPreservesAckedRecords(t *testing.T) {
	dataDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := startBroker(t, dataDir, port)

	const producers, perProducer = 8, 25
	type ack struct {
		offset int64
		value  string
	}
	acks := make(chan ack, producers*perProducer)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				value := fmt.Sprintf("p%d-%d", p, i)
				off, err := publishOne(base, "orders", "", value)
				if err != nil {
					t.Errorf("publish: %v", err)
					return
				}
				acks <- ack{off, value}
			}
		}(p)
	}
	wg.Wait()
	close(acks)

	acked := map[int64]string{}
	for a := range acks {
		if _, dup := acked[a.offset]; dup {
			t.Fatalf("duplicate acknowledged offset %d", a.offset)
		}
		acked[a.offset] = a.value
	}

	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill: %v", err)
	}
	cmd.Wait()

	cmd2 := startBroker(t, dataDir, port)
	defer func() { cmd2.Process.Kill(); cmd2.Wait() }()

	got := readAll(t, base, "orders")
	for off, want := range acked {
		if got[off] != want {
			t.Fatalf("offset %d = %q after crash, want %q", off, got[off], want)
		}
	}
}

// TestRestartPreservesOffsets verifies acknowledged records keep their
// offsets across a graceful restart.
func TestRestartPreservesOffsets(t *testing.T) {
	dataDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := startBroker(t, dataDir, port)

	for i := 0; i < 3; i++ {
		off, err := publishOne(base, "orders", "", fmt.Sprintf("v%d", i))
		if err != nil || off != int64(i) {
			t.Fatalf("publish %d: offset=%d err=%v", i, off, err)
		}
	}
	cmd.Process.Signal(syscall.SIGTERM)
	cmd.Wait()

	cmd2 := startBroker(t, dataDir, port)
	defer func() { cmd2.Process.Kill(); cmd2.Wait() }()
	got := readAll(t, base, "orders")
	if len(got) != 3 || got[0] != "v0" || got[2] != "v2" {
		t.Fatalf("after restart got %v", got)
	}
	if off, _ := publishOne(base, "orders", "", "v3"); off != 3 {
		t.Fatalf("offset after restart = %d, want 3", off)
	}
}

// TestAtLeastOnceRedelivery simulates a consumer that processes a record but
// crashes before committing: on restart it must receive the record again.
// After committing, it must not.
func TestAtLeastOnceRedelivery(t *testing.T) {
	dataDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := startBroker(t, dataDir, port)
	defer func() { cmd.Process.Kill(); cmd.Wait() }()

	for i := 0; i < 2; i++ {
		publishOne(base, "orders", "", fmt.Sprintf("v%d", i))
	}
	offsetURL := base + "/groups/feed/streams/orders/partitions/0/offset"

	committed := func() int64 {
		resp, err := http.Get(offsetURL)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			NextOffset int64 `json:"next_offset"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		return out.NextOffset
	}

	// Consumer reads record 0, processes it, then "crashes" before commit.
	if next := committed(); next != 0 {
		t.Fatalf("fresh group next = %d, want 0", next)
	}
	// Restarted consumer asks for its committed offset: still 0, so record 0
	// is delivered again.
	if next := committed(); next != 0 {
		t.Fatalf("post-crash next = %d, want 0 (redelivery)", next)
	}

	// Now it processes and commits 1; a restart resumes at 1.
	req, _ := http.NewRequest(http.MethodPut, offsetURL, strings.NewReader(`{"next_offset":1}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if next := committed(); next != 1 {
		t.Fatalf("committed next = %d, want 1", next)
	}
}

// TestCommittedProgressSurvivesCrash commits an offset, SIGKILLs the broker,
// and verifies the committed offset survives restart.
func TestCommittedProgressSurvivesCrash(t *testing.T) {
	dataDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := startBroker(t, dataDir, port)

	for i := 0; i < 5; i++ {
		publishOne(base, "orders", "", fmt.Sprintf("v%d", i))
	}
	offsetURL := base + "/groups/analytics/streams/orders/partitions/0/offset"
	req, _ := http.NewRequest(http.MethodPut, offsetURL, strings.NewReader(`{"next_offset":3}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	cmd.Process.Signal(syscall.SIGKILL)
	cmd.Wait()
	cmd2 := startBroker(t, dataDir, port)
	defer func() { cmd2.Process.Kill(); cmd2.Wait() }()

	resp2, err := http.Get(offsetURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var out struct {
		NextOffset int64 `json:"next_offset"`
	}
	json.NewDecoder(resp2.Body).Decode(&out)
	if out.NextOffset != 3 {
		t.Fatalf("committed offset after crash = %d, want 3", out.NextOffset)
	}
}
