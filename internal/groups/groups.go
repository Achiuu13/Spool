// Package groups implements consumer-group membership: joining, heartbeats,
// session expiry, range partition assignment, and commit fencing.
//
// Each partition of a stream is assigned to at most one active member of a
// group at a time. Membership is in-memory (single node); committed offsets
// are persisted separately by the offsets store, so progress survives both
// broker restarts and reassignment.
package groups

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	// ErrUnknownMember means the member never joined or its session expired.
	ErrUnknownMember = errors.New("groups: unknown or expired member")
	// ErrNotOwner means the member does not currently own the partition, so
	// its commit was fenced off.
	ErrNotOwner = errors.New("groups: member does not own this partition")
)

// Options configures the membership manager.
type Options struct {
	// SessionTimeout expires members that have not heartbeated. Zero means
	// 10 seconds.
	SessionTimeout time.Duration
}

// Assignment is what a member learns when it joins or heartbeats.
type Assignment struct {
	MemberID   string `json:"member_id"`
	Generation int64  `json:"generation"`
	Partitions []int  `json:"partitions"`
}

type member struct {
	lastSeen time.Time
}

type group struct {
	partitions int
	generation int64
	members    map[string]*member
	owners     map[int]string // partition -> member id
}

// Manager tracks membership for all (group, stream) pairs.
type Manager struct {
	opts Options
	now  func() time.Time

	mu     sync.Mutex
	groups map[string]*group // key: group + "\x00" + stream
}

func NewManager(opts Options) *Manager {
	if opts.SessionTimeout <= 0 {
		opts.SessionTimeout = 10 * time.Second
	}
	return &Manager{opts: opts, now: time.Now, groups: map[string]*group{}}
}

func key(groupName, stream string) string { return groupName + "\x00" + stream }

func newMemberID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Join adds a new member to the group for a stream with the given partition
// count, triggering a rebalance.
func (m *Manager) Join(groupName, stream string, partitions int) (Assignment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[key(groupName, stream)]
	if !ok {
		g = &group{partitions: partitions, members: map[string]*member{}, owners: map[int]string{}}
		m.groups[key(groupName, stream)] = g
	}
	m.expireLocked(g)
	id := newMemberID()
	g.members[id] = &member{lastSeen: m.now()}
	m.rebalanceLocked(g)
	return m.assignmentLocked(g, id), nil
}

// Heartbeat refreshes the member's session and returns its (possibly
// changed) assignment. ErrUnknownMember means the caller must rejoin.
func (m *Manager) Heartbeat(groupName, stream, memberID string) (Assignment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[key(groupName, stream)]
	if !ok {
		return Assignment{}, ErrUnknownMember
	}
	m.expireLocked(g)
	mem, ok := g.members[memberID]
	if !ok {
		return Assignment{}, ErrUnknownMember
	}
	mem.lastSeen = m.now()
	return m.assignmentLocked(g, memberID), nil
}

// Leave removes the member and rebalances.
func (m *Manager) Leave(groupName, stream, memberID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[key(groupName, stream)]
	if !ok {
		return ErrUnknownMember
	}
	if _, ok := g.members[memberID]; !ok {
		return ErrUnknownMember
	}
	delete(g.members, memberID)
	m.rebalanceLocked(g)
	return nil
}

// Authorize reports whether the member currently owns the partition. It is
// the fence that stops an expired or reassigned member from committing
// progress for a partition it no longer owns.
func (m *Manager) Authorize(groupName, stream, memberID string, partition int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[key(groupName, stream)]
	if !ok {
		return ErrUnknownMember
	}
	m.expireLocked(g)
	if _, ok := g.members[memberID]; !ok {
		return ErrUnknownMember
	}
	if g.owners[partition] != memberID {
		return ErrNotOwner
	}
	return nil
}

// expireLocked drops members whose sessions have timed out and rebalances
// if any were dropped.
func (m *Manager) expireLocked(g *group) {
	cutoff := m.now().Add(-m.opts.SessionTimeout)
	dropped := false
	for id, mem := range g.members {
		if mem.lastSeen.Before(cutoff) {
			delete(g.members, id)
			dropped = true
		}
	}
	if dropped {
		m.rebalanceLocked(g)
	}
}

// rebalanceLocked recomputes range assignment: members sorted by id, each
// getting a contiguous span of partitions, and bumps the generation.
func (m *Manager) rebalanceLocked(g *group) {
	g.generation++
	g.owners = map[int]string{}
	if len(g.members) == 0 {
		return
	}
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	per := g.partitions / len(ids)
	extra := g.partitions % len(ids)
	p := 0
	for i, id := range ids {
		n := per
		if i < extra {
			n++
		}
		for j := 0; j < n; j++ {
			g.owners[p] = id
			p++
		}
	}
}

func (m *Manager) assignmentLocked(g *group, memberID string) Assignment {
	a := Assignment{MemberID: memberID, Generation: g.generation}
	for p := 0; p < g.partitions; p++ {
		if g.owners[p] == memberID {
			a.Partitions = append(a.Partitions, p)
		}
	}
	return a
}
