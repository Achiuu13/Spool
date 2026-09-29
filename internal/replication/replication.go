// Package replication implements Raft-style log replication for a small,
// fixed cluster: term-based leader election, majority-ack commits, follower
// catch-up with conflict truncation, and term fencing of stale leaders.
//
// The replicated operation log reuses the durable framed log from
// internal/log: entry index = record offset, record value = [8B term][op].
// Ops are opaque bytes; committed ops are handed to Config.Apply in log
// order on every node, which is how derived broker state stays identical
// across the cluster. See docs/replication.md for the design rationale.
package replication

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	spoollog "github.com/Achiuu13/spool/internal/log"
)

// ErrNotLeader is returned by Propose on a non-leader; callers should
// redirect clients to LeaderURL.
var ErrNotLeader = errors.New("replication: not the leader")

// ErrNoQuorum is returned when a proposal cannot reach a majority in time.
var ErrNoQuorum = errors.New("replication: no quorum, entry not committed")

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case leader:
		return "leader"
	case candidate:
		return "candidate"
	default:
		return "follower"
	}
}

// Config configures a Node.
type Config struct {
	// ID is this node's name; Peers maps the other nodes' IDs to base URLs.
	ID    string
	Peers map[string]string
	// Advertise is this node's client-facing base URL, used in redirects.
	Advertise string
	// Dir holds the operation log and persisted term/vote state.
	Dir string
	// ElectionTimeout is the base follower patience; the actual deadline is
	// randomized in [1x, 2x). Zero means 300ms.
	ElectionTimeout time.Duration
	// HeartbeatInterval is the leader's replication cadence. Zero means 75ms.
	HeartbeatInterval time.Duration
	// Apply is called exactly once per committed entry, in log order.
	// Its result is returned to the Propose call that created the entry.
	Apply func(op []byte) (any, error)
	// HTTPClient overrides the default RPC client (tests inject faults).
	HTTPClient *http.Client
	Logger     *log.Logger
}

type entry struct {
	term int64
	op   []byte // nil for leader no-op entries
}

type waiter struct {
	term int64
	ch   chan proposeResult
}

type proposeResult struct {
	value any
	err   error
}

type hardState struct {
	Term     int64  `json:"term"`
	VotedFor string `json:"voted_for"`
}

// Node is one member of the replication cluster.
type Node struct {
	cfg    Config
	client *http.Client
	logger *log.Logger

	mu               sync.Mutex
	oplog            *spoollog.Log
	entries          []entry
	term             int64
	votedFor         string
	role             role
	leaderID         string
	commitIndex      int64 // highest committed entry index, -1 when none
	lastApplied      int64
	nextIndex        map[string]int64
	matchIndex       map[string]int64
	electionDeadline time.Time
	waiters          map[int64]waiter

	applyKick chan struct{}
	stop      chan struct{}
	done      sync.WaitGroup
}

// Open loads the operation log and persisted term state from cfg.Dir.
// Call Start to begin elections and replication.
func Open(cfg Config) (*Node, error) {
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = 300 * time.Millisecond
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 75 * time.Millisecond
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	oplog, err := spoollog.Open(filepath.Join(cfg.Dir, "oplog"), spoollog.Options{SyncEveryAppend: true})
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:         cfg,
		client:      cfg.HTTPClient,
		logger:      cfg.Logger,
		oplog:       oplog,
		commitIndex: -1,
		lastApplied: -1,
		waiters:     map[int64]waiter{},
		applyKick:   make(chan struct{}, 1),
		stop:        make(chan struct{}),
	}
	if n.client == nil {
		n.client = &http.Client{Timeout: 2 * time.Second}
	}
	if n.logger == nil {
		n.logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	// Rebuild the in-memory entry cache from the durable oplog.
	for off := int64(0); off < oplog.NextOffset(); {
		recs, err := oplog.Read(off, 1024)
		if err != nil {
			oplog.Close()
			return nil, err
		}
		for _, r := range recs {
			if len(r.Value) < 8 {
				oplog.Close()
				return nil, fmt.Errorf("replication: malformed oplog entry at %d", r.Offset)
			}
			e := entry{term: int64(binary.BigEndian.Uint64(r.Value[:8]))}
			if len(r.Value) > 8 {
				e.op = r.Value[8:]
			}
			n.entries = append(n.entries, e)
			off = r.Offset + 1
		}
	}
	if err := n.loadHardState(); err != nil {
		oplog.Close()
		return nil, err
	}
	n.resetElectionDeadlineLocked()
	return n, nil
}

// Start launches the election ticker and apply loop.
func (n *Node) Start() {
	n.done.Add(2)
	go n.run()
	go n.applyLoop()
}

// Stop shuts down background loops and closes the oplog.
func (n *Node) Stop() {
	close(n.stop)
	n.done.Wait()
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failWaitersLocked(errors.New("replication: node stopped"))
	n.oplog.Close()
}

func (n *Node) hardStatePath() string { return filepath.Join(n.cfg.Dir, "hardstate.json") }

func (n *Node) loadHardState() error {
	data, err := os.ReadFile(n.hardStatePath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var hs hardState
	if err := json.Unmarshal(data, &hs); err != nil {
		return fmt.Errorf("replication: corrupt hard state: %w", err)
	}
	n.term, n.votedFor = hs.Term, hs.VotedFor
	return nil
}

// persistHardStateLocked fsyncs term and vote before any RPC reply that
// depends on them, so a restart cannot double-vote or regress its term.
func (n *Node) persistHardStateLocked() error {
	data, _ := json.Marshal(hardState{Term: n.term, VotedFor: n.votedFor})
	tmp, err := os.CreateTemp(n.cfg.Dir, "hardstate-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err == nil {
		err = tmp.Sync()
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), n.hardStatePath())
}

func (n *Node) resetElectionDeadlineLocked() {
	d := n.cfg.ElectionTimeout
	n.electionDeadline = time.Now().Add(d + time.Duration(rand.Int63n(int64(d))))
}

func (n *Node) lastLogLocked() (index, term int64) {
	if len(n.entries) == 0 {
		return -1, -1
	}
	i := int64(len(n.entries) - 1)
	return i, n.entries[i].term
}

func (n *Node) majority() int { return (len(n.cfg.Peers)+1)/2 + 1 }

// stepDownLocked adopts a higher term observed from any RPC.
func (n *Node) stepDownLocked(term int64) {
	n.term = term
	n.votedFor = ""
	n.role = follower
	n.persistHardStateLocked()
	n.resetElectionDeadlineLocked()
}

func (n *Node) failWaitersLocked(err error) {
	for i, w := range n.waiters {
		w.ch <- proposeResult{err: err}
		delete(n.waiters, i)
	}
}

func (n *Node) kickApply() {
	select {
	case n.applyKick <- struct{}{}:
	default:
	}
}

// run drives elections and leader heartbeats on a coarse tick.
func (n *Node) run() {
	defer n.done.Done()
	ticker := time.NewTicker(n.cfg.HeartbeatInterval / 3)
	defer ticker.Stop()
	var lastBroadcast time.Time
	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
		}
		n.mu.Lock()
		switch n.role {
		case leader:
			if time.Since(lastBroadcast) >= n.cfg.HeartbeatInterval {
				lastBroadcast = time.Now()
				n.broadcastLocked()
			}
		default:
			if time.Now().After(n.electionDeadline) {
				n.startElectionLocked()
				lastBroadcast = time.Now()
			}
		}
		n.mu.Unlock()
	}
}

// applyLoop applies committed entries in order and resolves waiters.
func (n *Node) applyLoop() {
	defer n.done.Done()
	for {
		select {
		case <-n.stop:
			return
		case <-n.applyKick:
		}
		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			i := n.lastApplied + 1
			e := n.entries[i]
			w, waiting := n.waiters[i]
			delete(n.waiters, i)
			n.mu.Unlock()

			var res proposeResult
			if e.op != nil {
				res.value, res.err = n.cfg.Apply(e.op)
			}
			if waiting {
				if w.term != e.term {
					res = proposeResult{err: ErrNoQuorum} // entry was replaced by another leader's
				}
				w.ch <- res
			}
			n.mu.Lock()
			n.lastApplied = i
			n.mu.Unlock()
		}
	}
}
