package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

type assignment struct {
	MemberID   string `json:"member_id"`
	Generation int64  `json:"generation"`
	Partitions []int  `json:"partitions"`
}

func joinGroup(t *testing.T, base, group, stream string) assignment {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("%s/groups/%s/streams/%s/members", base, group, stream), "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var a assignment
	json.NewDecoder(resp.Body).Decode(&a)
	if resp.StatusCode != 200 {
		t.Fatalf("join: status %d", resp.StatusCode)
	}
	return a
}

func heartbeat(t *testing.T, base, group, stream, member string) (assignment, int) {
	t.Helper()
	url := fmt.Sprintf("%s/groups/%s/streams/%s/members/%s/heartbeat", base, group, stream, member)
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var a assignment
	json.NewDecoder(resp.Body).Decode(&a)
	return a, resp.StatusCode
}

func commitAs(t *testing.T, base, group, stream string, partition int, next int64, member string) int {
	t.Helper()
	url := fmt.Sprintf("%s/groups/%s/streams/%s/partitions/%d/offset", base, group, stream, partition)
	body, _ := json.Marshal(map[string]any{"next_offset": next, "member_id": member})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// TestGroupAssignsEachPartitionToOneMember joins two members and verifies
// the group's partitions are split with no overlap and full coverage.
func TestGroupAssignsEachPartitionToOneMember(t *testing.T) {
	dataDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := startBroker(t, dataDir, port, "-partitions", "4")
	defer func() { cmd.Process.Kill(); cmd.Wait() }()

	a1 := joinGroup(t, base, "feed", "orders")
	a2 := joinGroup(t, base, "feed", "orders")
	a1, code := heartbeat(t, base, "feed", "orders", a1.MemberID)
	if code != 200 {
		t.Fatalf("heartbeat: %d", code)
	}

	seen := map[int]int{}
	for _, p := range a1.Partitions {
		seen[p]++
	}
	for _, p := range a2.Partitions {
		seen[p]++
	}
	if len(seen) != 4 {
		t.Fatalf("partitions covered = %v, want all 4", seen)
	}
	for p, n := range seen {
		if n != 1 {
			t.Fatalf("partition %d owned by %d members", p, n)
		}
	}
}

// TestReassignmentOnMemberTimeout kills one member's heartbeats and checks
// the survivor is assigned every partition without losing committed
// progress, and that the dead member can no longer commit.
func TestReassignmentOnMemberTimeout(t *testing.T) {
	dataDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := startBroker(t, dataDir, port, "-partitions", "2", "-session-timeout", "1s")
	defer func() { cmd.Process.Kill(); cmd.Wait() }()

	a1 := joinGroup(t, base, "feed", "orders")
	a2 := joinGroup(t, base, "feed", "orders")
	a1, _ = heartbeat(t, base, "feed", "orders", a1.MemberID)
	a2, _ = heartbeat(t, base, "feed", "orders", a2.MemberID)
	if len(a1.Partitions) != 1 || len(a2.Partitions) != 1 {
		t.Fatalf("split = %v / %v, want 1 each", a1.Partitions, a2.Partitions)
	}

	// a2 commits progress on its partition, then goes silent.
	deadPartition := a2.Partitions[0]
	if code := commitAs(t, base, "feed", "orders", deadPartition, 7, a2.MemberID); code != 200 {
		t.Fatalf("owner commit rejected: %d", code)
	}

	// a1 keeps heartbeating past a2's session timeout.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var code int
		a1, code = heartbeat(t, base, "feed", "orders", a1.MemberID)
		if code != 200 {
			t.Fatalf("survivor heartbeat: %d", code)
		}
		if len(a1.Partitions) == 2 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(a1.Partitions) != 2 {
		t.Fatalf("survivor partitions = %v, want both after timeout", a1.Partitions)
	}

	// Committed progress survived the reassignment.
	resp, err := http.Get(fmt.Sprintf("%s/groups/feed/streams/orders/partitions/%d/offset", base, deadPartition))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		NextOffset int64 `json:"next_offset"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if out.NextOffset != 7 {
		t.Fatalf("committed offset after reassignment = %d, want 7", out.NextOffset)
	}

	// The expired member is fenced: its commit must be rejected.
	if code := commitAs(t, base, "feed", "orders", deadPartition, 9, a2.MemberID); code != 404 && code != 409 {
		t.Fatalf("stale commit status = %d, want 404 or 409", code)
	}
	// And the offset is unchanged.
	resp2, _ := http.Get(fmt.Sprintf("%s/groups/feed/streams/orders/partitions/%d/offset", base, deadPartition))
	json.NewDecoder(resp2.Body).Decode(&out)
	resp2.Body.Close()
	if out.NextOffset != 7 {
		t.Fatalf("offset after fenced commit = %d, want 7", out.NextOffset)
	}
}

// TestTwoGroupsReadIndependently shows one group splitting work while a
// second group independently reads the entire stream.
func TestTwoGroupsReadIndependently(t *testing.T) {
	dataDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := startBroker(t, dataDir, port, "-partitions", "2")
	defer func() { cmd.Process.Kill(); cmd.Wait() }()

	shared1 := joinGroup(t, base, "workers", "orders")
	shared2 := joinGroup(t, base, "workers", "orders")
	solo := joinGroup(t, base, "analytics", "orders")

	shared1, _ = heartbeat(t, base, "workers", "orders", shared1.MemberID)
	if len(shared1.Partitions)+len(shared2.Partitions) != 2 {
		t.Fatalf("workers split = %v / %v", shared1.Partitions, shared2.Partitions)
	}
	if len(solo.Partitions) != 2 {
		t.Fatalf("analytics solo member partitions = %v, want both", solo.Partitions)
	}
}

// TestKeyRoutingE2E verifies records with the same key land on the same
// partition through the HTTP API.
func TestKeyRoutingE2E(t *testing.T) {
	dataDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := startBroker(t, dataDir, port, "-partitions", "3")
	defer func() { cmd.Process.Kill(); cmd.Wait() }()

	partitionOf := func(key string) int {
		body, _ := json.Marshal(map[string]string{"key": key, "value": "v"})
		resp, err := http.Post(base+"/streams/orders/records", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Partition int `json:"partition"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		return out.Partition
	}

	for _, key := range []string{"order-1", "order-2", "order-3"} {
		first := partitionOf(key)
		for i := 0; i < 3; i++ {
			if p := partitionOf(key); p != first {
				t.Fatalf("key %q moved from partition %d to %d", key, first, p)
			}
		}
	}
}
