package replication

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testNode wraps a Node with an httptest server and a partition switch that
// drops traffic in both directions, simulating a network partition.
type testNode struct {
	id          string
	node        *Node
	server      *httptest.Server
	partitioned atomic.Bool

	mu      sync.Mutex
	applied [][]byte
}

// gateTransport fails outbound RPCs while the node is partitioned.
type gateTransport struct{ tn *testNode }

func (g *gateTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if g.tn.partitioned.Load() {
		return nil, fmt.Errorf("partitioned")
	}
	return http.DefaultTransport.RoundTrip(r)
}

type cluster struct {
	t     *testing.T
	nodes map[string]*testNode
}

func newCluster(t *testing.T, ids ...string) *cluster {
	t.Helper()
	c := &cluster{t: t, nodes: map[string]*testNode{}}
	urls := map[string]string{}
	for _, id := range ids {
		tn := &testNode{id: id}
		tn.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if tn.partitioned.Load() {
				http.Error(w, "partitioned", http.StatusBadGateway)
				return
			}
			tn.node.Handler().ServeHTTP(w, r)
		}))
		urls[id] = tn.server.URL
		c.nodes[id] = tn
	}
	for _, id := range ids {
		tn := c.nodes[id]
		peers := map[string]string{}
		for _, other := range ids {
			if other != id {
				peers[other] = urls[other]
			}
		}
		node, err := Open(Config{
			ID:                id,
			Peers:             peers,
			Advertise:         urls[id],
			Dir:               t.TempDir(),
			ElectionTimeout:   150 * time.Millisecond,
			HeartbeatInterval: 40 * time.Millisecond,
			Apply: func(op []byte) (any, error) {
				tn.mu.Lock()
				defer tn.mu.Unlock()
				tn.applied = append(tn.applied, op)
				return len(tn.applied), nil
			},
			HTTPClient: &http.Client{Transport: &gateTransport{tn: tn}, Timeout: time.Second},
		})
		if err != nil {
			t.Fatal(err)
		}
		tn.node = node
		node.Start()
		t.Cleanup(func() { node.Stop(); tn.server.Close() })
	}
	return c
}

func (c *cluster) leader(timeout time.Duration) *testNode {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*testNode
		for _, tn := range c.nodes {
			if !tn.partitioned.Load() && tn.node.IsLeader() {
				leaders = append(leaders, tn)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %v", timeout)
	return nil
}

func (tn *testNode) appliedOps() [][]byte {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	out := make([][]byte, len(tn.applied))
	copy(out, tn.applied)
	return out
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestElectsSingleLeader(t *testing.T) {
	c := newCluster(t, "n1", "n2", "n3")
	l := c.leader(3 * time.Second)
	// Every node agrees on who leads.
	waitFor(t, 2*time.Second, "leader agreement", func() bool {
		for _, tn := range c.nodes {
			if tn.node.Status().Leader != l.id {
				return false
			}
		}
		return true
	})
}

func TestProposeAppliesOnEveryNode(t *testing.T) {
	c := newCluster(t, "n1", "n2", "n3")
	l := c.leader(3 * time.Second)
	for i := 0; i < 5; i++ {
		res, err := l.node.Propose([]byte(fmt.Sprintf("op-%d", i)))
		if err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		if res.(int) != i+1 {
			t.Fatalf("apply result = %v, want %d", res, i+1)
		}
	}
	waitFor(t, 2*time.Second, "all nodes applied 5 ops", func() bool {
		for _, tn := range c.nodes {
			if len(tn.appliedOps()) != 5 {
				return false
			}
		}
		return true
	})
	for _, tn := range c.nodes {
		for i, op := range tn.appliedOps() {
			if string(op) != fmt.Sprintf("op-%d", i) {
				t.Fatalf("%s applied[%d] = %q", tn.id, i, op)
			}
		}
	}
}

func TestProposeOnFollowerRejected(t *testing.T) {
	c := newCluster(t, "n1", "n2", "n3")
	l := c.leader(3 * time.Second)
	for _, tn := range c.nodes {
		if tn == l {
			continue
		}
		if _, err := tn.node.Propose([]byte("x")); err != ErrNotLeader {
			t.Fatalf("follower propose err = %v, want ErrNotLeader", err)
		}
		if tn.node.LeaderURL() != l.server.URL {
			t.Fatalf("follower leader URL = %q, want %q", tn.node.LeaderURL(), l.server.URL)
		}
	}
}

func TestPartitionedFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, "n1", "n2", "n3")
	l := c.leader(3 * time.Second)
	var lagging *testNode
	for _, tn := range c.nodes {
		if tn != l {
			lagging = tn
			break
		}
	}
	lagging.partitioned.Store(true)
	for i := 0; i < 4; i++ {
		if _, err := l.node.Propose([]byte(fmt.Sprintf("op-%d", i))); err != nil {
			t.Fatalf("propose with 2/3 up: %v", err)
		}
	}
	if len(lagging.appliedOps()) != 0 {
		t.Fatalf("partitioned node applied %d ops", len(lagging.appliedOps()))
	}
	lagging.partitioned.Store(false)
	waitFor(t, 3*time.Second, "lagging node catch-up", func() bool {
		return len(lagging.appliedOps()) == 4
	})
}

func TestLeaderFailoverKeepsCommittedEntries(t *testing.T) {
	c := newCluster(t, "n1", "n2", "n3")
	l := c.leader(3 * time.Second)
	for i := 0; i < 3; i++ {
		if _, err := l.node.Propose([]byte(fmt.Sprintf("committed-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	l.partitioned.Store(true)

	l2 := c.leader(5 * time.Second)
	if l2 == l {
		t.Fatal("old leader still leads while partitioned")
	}
	if _, err := l2.node.Propose([]byte("after-failover")); err != nil {
		t.Fatalf("propose on new leader: %v", err)
	}
	waitFor(t, 2*time.Second, "new leader applied all 4", func() bool {
		return len(l2.appliedOps()) == 4
	})
	ops := l2.appliedOps()
	for i := 0; i < 3; i++ {
		if string(ops[i]) != fmt.Sprintf("committed-%d", i) {
			t.Fatalf("committed entry lost: applied[%d] = %q", i, ops[i])
		}
	}
}

func TestNoQuorumBlocksCommit(t *testing.T) {
	c := newCluster(t, "n1", "n2", "n3")
	l := c.leader(3 * time.Second)
	for _, tn := range c.nodes {
		if tn != l {
			tn.partitioned.Store(true)
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := l.node.Propose([]byte("doomed"))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("propose committed without a quorum")
		}
	case <-time.After(7 * time.Second):
		t.Fatal("propose did not fail in time")
	}
}

// TestStaleLeaderTruncatesUncommittedEntries partitions a leader that then
// accepts (but cannot commit) a proposal; the healed cluster forces it to
// drop the uncommitted entry and adopt the new leader's log.
func TestStaleLeaderTruncatesUncommittedEntries(t *testing.T) {
	c := newCluster(t, "n1", "n2", "n3")
	l := c.leader(3 * time.Second)
	if _, err := l.node.Propose([]byte("committed-0")); err != nil {
		t.Fatal(err)
	}

	l.partitioned.Store(true)
	// Stale leader appends locally; must not commit.
	go l.node.Propose([]byte("uncommitted"))
	waitFor(t, 2*time.Second, "stale leader local append", func() bool {
		return l.node.Status().LastIndex >= 2
	})

	l2 := c.leader(5 * time.Second)
	if _, err := l2.node.Propose([]byte("committed-1")); err != nil {
		t.Fatal(err)
	}

	l.partitioned.Store(false)
	waitFor(t, 5*time.Second, "old leader convergence", func() bool {
		s := l.node.Status()
		return s.Role == "follower" && s.Applied == l2.node.Status().Applied
	})
	ops := l.appliedOps()
	for _, op := range ops {
		if string(op) == "uncommitted" {
			t.Fatal("uncommitted entry from stale leader was applied")
		}
	}
	if len(ops) < 2 || string(ops[len(ops)-1]) != "committed-1" {
		t.Fatalf("old leader applied = %q, want to end with committed-1", ops)
	}
}

func TestRestartRecoversOplogAndTerm(t *testing.T) {
	dir := t.TempDir()
	var applied [][]byte
	cfg := Config{
		ID: "solo", Peers: map[string]string{}, Advertise: "http://solo",
		Dir:             dir,
		ElectionTimeout: 50 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond,
		Apply: func(op []byte) (any, error) { applied = append(applied, op); return nil, nil },
	}
	n, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	waitFor(t, 2*time.Second, "solo election", n.IsLeader)
	if _, err := n.Propose([]byte("persisted")); err != nil {
		t.Fatal(err)
	}
	term := n.Status().Term
	n.Stop()

	applied = nil
	n2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Stop()
	if n2.Status().Term < term {
		t.Fatalf("term regressed after restart: %d < %d", n2.Status().Term, term)
	}
	if n2.Status().LastIndex < 1 {
		t.Fatalf("oplog lost entries: last index %d", n2.Status().LastIndex)
	}
	n2.Start()
	waitFor(t, 2*time.Second, "solo re-election and replay", func() bool {
		return n2.IsLeader() && n2.Status().Applied >= 1
	})
	found := false
	for _, op := range applied {
		if string(op) == "persisted" {
			found = true
		}
	}
	if !found {
		t.Fatal("committed entry not re-applied after restart")
	}
}
