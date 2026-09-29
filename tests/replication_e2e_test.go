package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// clusterNode is one broker process in a 3-node e2e cluster.
type clusterNode struct {
	id      string
	base    string
	dataDir string
	port    int
	cmd     *exec.Cmd
}

// startCluster boots n broker processes wired as one replication cluster.
// It does not wait for readiness; use waitLeader.
func startCluster(t *testing.T, n int, extra ...string) []*clusterNode {
	t.Helper()
	nodes := make([]*clusterNode, n)
	specs := make([]string, n)
	for i := range nodes {
		port := freePort(t)
		nodes[i] = &clusterNode{
			id:      fmt.Sprintf("n%d", i+1),
			base:    fmt.Sprintf("http://127.0.0.1:%d", port),
			dataDir: t.TempDir(),
			port:    port,
		}
		specs[i] = nodes[i].id + "=" + nodes[i].base
	}
	peers := strings.Join(specs, ",")
	for _, cn := range nodes {
		startClusterNode(t, cn, peers, extra...)
	}
	t.Cleanup(func() {
		for _, cn := range nodes {
			if cn.cmd != nil && cn.cmd.Process != nil {
				cn.cmd.Process.Kill()
				cn.cmd.Wait()
			}
		}
	})
	return nodes
}

func startClusterNode(t *testing.T, cn *clusterNode, peers string, extra ...string) {
	t.Helper()
	args := append([]string{
		"-addr", fmt.Sprintf("127.0.0.1:%d", cn.port),
		"-data", cn.dataDir,
		"-node-id", cn.id,
		"-peers", peers,
		"-election-timeout", "200ms",
		"-heartbeat-interval", "50ms",
	}, extra...)
	cmd := exec.Command(brokerBin, args...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", cn.id, err)
	}
	cn.cmd = cmd
}

type raftStatus struct {
	ID          string `json:"id"`
	Term        int64  `json:"term"`
	Role        string `json:"role"`
	Leader      string `json:"leader"`
	LeaderURL   string `json:"leader_url"`
	CommitIndex int64  `json:"commit_index"`
	Applied     int64  `json:"applied"`
}

func nodeStatus(base string) (raftStatus, error) {
	var s raftStatus
	resp, err := http.Get(base + "/raft/status")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

// waitLeader blocks until the live nodes agree on a single leader and
// returns that leader's node.
func waitLeader(t *testing.T, nodes []*clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := map[string]bool{}
		var candidate *clusterNode
		reachable, informed := 0, 0
		for _, cn := range nodes {
			s, err := nodeStatus(cn.base)
			if err != nil {
				continue
			}
			reachable++
			if s.Leader != "" {
				informed++
				leaders[s.Leader] = true
			}
			if s.Role == "leader" {
				candidate = cn
			}
		}
		// Every reachable node must agree on the same leader before tests
		// route traffic through arbitrary nodes.
		if reachable > 0 && informed == reachable && len(leaders) == 1 && candidate != nil {
			return candidate
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("cluster did not elect a single agreed leader")
	return nil
}

func readPartition(t *testing.T, base, stream string, partition int) map[int64]string {
	t.Helper()
	got := map[int64]string{}
	offset := int64(0)
	for {
		url := fmt.Sprintf("%s/streams/%s/partitions/%d/records?offset=%d&limit=1000", base, stream, partition, offset)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Records []struct {
				Offset int64  `json:"offset"`
				Value  string `json:"value"`
			} `json:"records"`
			NextOffset int64 `json:"next_offset"`
		}
		err = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Records) == 0 {
			return got
		}
		for _, r := range out.Records {
			got[r.Offset] = r.Value
		}
		offset = out.NextOffset
	}
}

// TestClusterPublishesThroughLeaderAndRedirects publishes via a follower
// (following the 307) and verifies followers redirect client traffic.
func TestClusterPublishesThroughLeaderAndRedirects(t *testing.T) {
	nodes := startCluster(t, 3, "-partitions", "1")
	leader := waitLeader(t, nodes, 10*time.Second)

	var follower *clusterNode
	for _, cn := range nodes {
		if cn != leader {
			follower = cn
			break
		}
	}

	// Default client follows the 307, so publishing via a follower works.
	for i := 0; i < 10; i++ {
		if _, err := publishOne(follower.base, "orders", "", fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("publish via follower: %v", err)
		}
	}

	// The redirect itself points at the leader.
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	body, _ := json.Marshal(map[string]string{"value": "x"})
	resp, err := noRedirect.Post(follower.base+"/streams/orders/records", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("follower publish status = %d, want 307", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); !strings.HasPrefix(loc, leader.base) {
		t.Fatalf("redirect location = %q, want leader %q", loc, leader.base)
	}

	got := readPartition(t, leader.base, "orders", 0)
	if len(got) != 10 {
		t.Fatalf("leader has %d records, want 10", len(got))
	}
}

// TestFailoverPreservesAckedRecords kills the leader after acked publishes
// and verifies every acked record is readable from the new leader.
func TestFailoverPreservesAckedRecords(t *testing.T) {
	nodes := startCluster(t, 3, "-partitions", "1")
	leader := waitLeader(t, nodes, 10*time.Second)

	acked := map[int64]string{}
	for i := 0; i < 25; i++ {
		v := fmt.Sprintf("acked-%d", i)
		off, err := publishOne(leader.base, "orders", "", v)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		acked[off] = v
	}

	start := time.Now()
	leader.cmd.Process.Kill()
	leader.cmd.Wait()
	leader.cmd = nil

	var survivors []*clusterNode
	for _, cn := range nodes {
		if cn != leader {
			survivors = append(survivors, cn)
		}
	}
	newLeader := waitLeader(t, survivors, 15*time.Second)
	t.Logf("failover: new leader %s after %v", newLeader.id, time.Since(start))

	got := readPartition(t, newLeader.base, "orders", 0)
	for off, v := range acked {
		if got[off] != v {
			t.Fatalf("acked record at offset %d = %q, want %q", off, got[off], v)
		}
	}

	// The cluster still accepts writes with 2 of 3 nodes.
	if _, err := publishOne(newLeader.base, "orders", "", "after-failover"); err != nil {
		t.Fatalf("publish after failover: %v", err)
	}
}

// TestKilledNodeRejoinsAndCatchesUp kills a follower, keeps publishing,
// restarts it, and verifies it replays to the cluster's commit point.
func TestKilledNodeRejoinsAndCatchesUp(t *testing.T) {
	nodes := startCluster(t, 3, "-partitions", "1")
	leader := waitLeader(t, nodes, 10*time.Second)

	var follower *clusterNode
	for _, cn := range nodes {
		if cn != leader {
			follower = cn
			break
		}
	}
	follower.cmd.Process.Kill()
	follower.cmd.Wait()
	follower.cmd = nil

	for i := 0; i < 15; i++ {
		if _, err := publishOne(leader.base, "orders", "", fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("publish with node down: %v", err)
		}
	}

	specs := make([]string, len(nodes))
	for i, cn := range nodes {
		specs[i] = cn.id + "=" + cn.base
	}
	startClusterNode(t, follower, strings.Join(specs, ","), "-partitions", "1")

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ls, err1 := nodeStatus(leader.base)
		fs, err2 := nodeStatus(follower.base)
		if err1 == nil && err2 == nil && fs.Applied >= ls.CommitIndex && fs.Role == "follower" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("restarted node did not catch up to the cluster")
}

// TestQuorumLossRejectsWrites kills two of three nodes: the survivor must
// refuse to acknowledge publishes rather than risk losing them.
func TestQuorumLossRejectsWrites(t *testing.T) {
	nodes := startCluster(t, 3, "-partitions", "1")
	leader := waitLeader(t, nodes, 10*time.Second)
	if _, err := publishOne(leader.base, "orders", "", "before"); err != nil {
		t.Fatal(err)
	}

	for _, cn := range nodes {
		if cn != leader {
			cn.cmd.Process.Kill()
			cn.cmd.Wait()
			cn.cmd = nil
		}
	}

	if _, err := publishOne(leader.base, "orders", "", "doomed"); err == nil {
		t.Fatal("publish acknowledged without a quorum")
	}
}

// TestCommittedOffsetSurvivesFailover replicates a group offset commit and
// verifies the new leader serves it after the old leader dies.
func TestCommittedOffsetSurvivesFailover(t *testing.T) {
	nodes := startCluster(t, 3, "-partitions", "1")
	leader := waitLeader(t, nodes, 10*time.Second)

	if _, err := publishOne(leader.base, "orders", "", "v0"); err != nil {
		t.Fatal(err)
	}
	if code := commitAs(t, leader.base, "feed", "orders", 0, 1, ""); code != 200 {
		t.Fatalf("commit status = %d", code)
	}

	leader.cmd.Process.Kill()
	leader.cmd.Wait()
	leader.cmd = nil
	var survivors []*clusterNode
	for _, cn := range nodes {
		if cn != leader {
			survivors = append(survivors, cn)
		}
	}
	newLeader := waitLeader(t, survivors, 15*time.Second)

	resp, err := http.Get(newLeader.base + "/groups/feed/streams/orders/partitions/0/offset")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		NextOffset int64 `json:"next_offset"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if out.NextOffset != 1 {
		t.Fatalf("offset after failover = %d, want 1", out.NextOffset)
	}
}
