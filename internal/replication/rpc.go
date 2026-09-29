package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const maxEntriesPerBatch = 256

type voteRequest struct {
	Term         int64  `json:"term"`
	Candidate    string `json:"candidate"`
	LastLogIndex int64  `json:"last_log_index"`
	LastLogTerm  int64  `json:"last_log_term"`
}

type voteResponse struct {
	Term    int64 `json:"term"`
	Granted bool  `json:"granted"`
}

type wireEntry struct {
	Term int64  `json:"term"`
	Op   []byte `json:"op,omitempty"`
}

type appendRequest struct {
	Term         int64       `json:"term"`
	Leader       string      `json:"leader"`
	PrevIndex    int64       `json:"prev_index"`
	PrevTerm     int64       `json:"prev_term"`
	Entries      []wireEntry `json:"entries,omitempty"`
	LeaderCommit int64       `json:"leader_commit"`
}

type appendResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}

// Status is a point-in-time view for operators and tests.
type Status struct {
	ID          string `json:"id"`
	Term        int64  `json:"term"`
	Role        string `json:"role"`
	Leader      string `json:"leader"`
	LeaderURL   string `json:"leader_url"`
	LastIndex   int64  `json:"last_index"`
	CommitIndex int64  `json:"commit_index"`
	Applied     int64  `json:"applied"`
}

// Handler returns the internal RPC endpoints, mounted under /raft/.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", n.handleVote)
	mux.HandleFunc("POST /raft/entries", n.handleAppendEntries)
	mux.HandleFunc("GET /raft/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(n.Status())
	})
	return mux
}

// Status reports the node's current view of the cluster.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	last, _ := n.lastLogLocked()
	return Status{
		ID:          n.cfg.ID,
		Term:        n.term,
		Role:        n.role.String(),
		Leader:      n.leaderID,
		LeaderURL:   n.leaderURLLocked(),
		LastIndex:   last,
		CommitIndex: n.commitIndex,
		Applied:     n.lastApplied,
	}
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == leader
}

// LeaderURL returns the client-facing URL of the current leader, or "" if
// no leader is known.
func (n *Node) LeaderURL() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderURLLocked()
}

func (n *Node) leaderURLLocked() string {
	switch n.leaderID {
	case "":
		return ""
	case n.cfg.ID:
		return n.cfg.Advertise
	default:
		return n.cfg.Peers[n.leaderID]
	}
}

// Propose appends an op to the replicated log and blocks until it commits
// and applies (returning Apply's result), or fails. Only the leader accepts
// proposals.
func (n *Node) Propose(op []byte) (any, error) {
	n.mu.Lock()
	if n.role != leader {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	index, err := n.appendEntryLocked(entry{term: n.term, op: op})
	if err != nil {
		n.mu.Unlock()
		return nil, err
	}
	ch := make(chan proposeResult, 1)
	n.waiters[index] = waiter{term: n.term, ch: ch}
	n.advanceCommitLocked() // a single-node majority is just this append
	n.broadcastLocked()
	n.mu.Unlock()

	select {
	case res := <-ch:
		return res.value, res.err
	case <-time.After(5 * time.Second):
		n.mu.Lock()
		delete(n.waiters, index)
		n.mu.Unlock()
		return nil, ErrNoQuorum
	case <-n.stop:
		return nil, ErrNoQuorum
	}
}

// appendEntryLocked persists an entry to the durable oplog and the cache.
func (n *Node) appendEntryLocked(e entry) (int64, error) {
	value := make([]byte, 8+len(e.op))
	binary.BigEndian.PutUint64(value[:8], uint64(e.term))
	copy(value[8:], e.op)
	index, err := n.oplog.AppendRecord(time.Now().UnixNano(), nil, value)
	if err != nil {
		return 0, err
	}
	n.entries = append(n.entries, e)
	return index, nil
}

// startElectionLocked moves to candidate, votes for itself, and solicits
// votes; on a majority it becomes leader and replicates a no-op entry so
// earlier-term entries can be committed (Raft's current-term commit rule).
func (n *Node) startElectionLocked() {
	n.term++
	n.votedFor = n.cfg.ID
	n.role = candidate
	if err := n.persistHardStateLocked(); err != nil {
		n.logger.Printf("replication: persist vote: %v", err)
		return
	}
	n.resetElectionDeadlineLocked()
	term := n.term
	lastIndex, lastTerm := n.lastLogLocked()
	req := voteRequest{Term: term, Candidate: n.cfg.ID, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	n.logger.Printf("replication: %s starting election for term %d", n.cfg.ID, term)

	votes := 1
	if votes >= n.majority() { // single-node cluster
		n.becomeLeaderLocked()
		return
	}
	for id, url := range n.cfg.Peers {
		go func(id, url string) {
			var resp voteResponse
			if err := n.post(url+"/raft/vote", req, &resp); err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.term {
				n.stepDownLocked(resp.Term)
				return
			}
			if n.role != candidate || n.term != term || !resp.Granted {
				return
			}
			votes++
			if votes >= n.majority() {
				n.becomeLeaderLocked()
			}
		}(id, url)
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = leader
	n.leaderID = n.cfg.ID
	n.nextIndex = map[string]int64{}
	n.matchIndex = map[string]int64{}
	for id := range n.cfg.Peers {
		n.nextIndex[id] = int64(len(n.entries))
		n.matchIndex[id] = -1
	}
	n.logger.Printf("replication: %s elected leader for term %d", n.cfg.ID, n.term)
	// No-op entry in the new term; committing it commits everything before it.
	if _, err := n.appendEntryLocked(entry{term: n.term}); err != nil {
		n.logger.Printf("replication: append no-op: %v", err)
	}
	n.advanceCommitLocked()
	n.broadcastLocked()
}

// broadcastLocked sends AppendEntries to every peer from its nextIndex.
func (n *Node) broadcastLocked() {
	term := n.term
	for id, url := range n.cfg.Peers {
		next := n.nextIndex[id]
		prevIndex, prevTerm := next-1, int64(-1)
		if prevIndex >= 0 {
			prevTerm = n.entries[prevIndex].term
		}
		batch := n.entries[next:]
		if len(batch) > maxEntriesPerBatch {
			batch = batch[:maxEntriesPerBatch]
		}
		req := appendRequest{
			Term: term, Leader: n.cfg.ID,
			PrevIndex: prevIndex, PrevTerm: prevTerm,
			LeaderCommit: n.commitIndex,
		}
		for _, e := range batch {
			req.Entries = append(req.Entries, wireEntry{Term: e.term, Op: e.op})
		}
		sent := int64(len(batch))
		go func(id, url string) {
			var resp appendResponse
			if err := n.post(url+"/raft/entries", req, &resp); err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.term {
				n.stepDownLocked(resp.Term)
				return
			}
			if n.role != leader || n.term != term {
				return
			}
			if resp.Success {
				if m := prevIndex + sent; m > n.matchIndex[id] {
					n.matchIndex[id] = m
					n.nextIndex[id] = m + 1
					n.advanceCommitLocked()
				}
				if n.nextIndex[id] < int64(len(n.entries)) {
					n.broadcastLocked() // follower is behind; keep streaming
				}
			} else if n.nextIndex[id] > 0 {
				n.nextIndex[id]--
				n.broadcastLocked() // walk back to the divergence point
			}
		}(id, url)
	}
}

// advanceCommitLocked commits the highest index replicated on a majority,
// counting only entries from the current term.
func (n *Node) advanceCommitLocked() {
	for i := int64(len(n.entries)) - 1; i > n.commitIndex; i-- {
		if n.entries[i].term != n.term {
			break
		}
		count := 1 // self
		for _, m := range n.matchIndex {
			if m >= i {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = i
			n.kickApply()
			break
		}
	}
}

func (n *Node) handleVote(w http.ResponseWriter, r *http.Request) {
	var req voteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	if req.Term > n.term {
		n.stepDownLocked(req.Term)
	}
	resp := voteResponse{Term: n.term}
	lastIndex, lastTerm := n.lastLogLocked()
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)
	if req.Term == n.term && upToDate && (n.votedFor == "" || n.votedFor == req.Candidate) {
		n.votedFor = req.Candidate
		if err := n.persistHardStateLocked(); err == nil {
			resp.Granted = true
			n.resetElectionDeadlineLocked()
		}
	}
	n.mu.Unlock()
	writeJSON(w, resp)
}

func (n *Node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req appendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	resp := appendResponse{Term: n.term}
	if req.Term < n.term {
		n.mu.Unlock()
		writeJSON(w, resp)
		return
	}
	if req.Term > n.term {
		n.stepDownLocked(req.Term)
	}
	n.role = follower
	n.leaderID = req.Leader
	n.resetElectionDeadlineLocked()
	resp.Term = n.term

	// Log-matching check: the entry before the batch must agree.
	if req.PrevIndex >= 0 &&
		(req.PrevIndex >= int64(len(n.entries)) || n.entries[req.PrevIndex].term != req.PrevTerm) {
		n.mu.Unlock()
		writeJSON(w, resp)
		return
	}

	ok := true
	for i, we := range req.Entries {
		index := req.PrevIndex + 1 + int64(i)
		if index < int64(len(n.entries)) {
			if n.entries[index].term == we.Term {
				continue // already have it
			}
			// Conflict: truncate our uncommitted divergent suffix.
			if err := n.oplog.TruncateFrom(index); err != nil {
				n.logger.Printf("replication: truncate: %v", err)
				ok = false
				break
			}
			n.entries = n.entries[:index]
			n.failWaitersLocked(ErrNoQuorum)
		}
		if _, err := n.appendEntryLocked(entry{term: we.Term, op: we.Op}); err != nil {
			n.logger.Printf("replication: follower append: %v", err)
			ok = false
			break
		}
	}
	if ok {
		lastNew := req.PrevIndex + int64(len(req.Entries))
		if req.LeaderCommit > n.commitIndex {
			n.commitIndex = min(req.LeaderCommit, lastNew)
			n.kickApply()
		}
		resp.Success = true
	}
	n.mu.Unlock()
	writeJSON(w, resp)
}

func (n *Node) post(url string, req, resp any) error {
	body, _ := json.Marshal(req)
	httpResp, err := n.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("replication: rpc status %s", httpResp.Status)
	}
	return json.NewDecoder(httpResp.Body).Decode(resp)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
