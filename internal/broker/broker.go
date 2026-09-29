// Package broker coordinates streams and their partition logs, and exposes
// committed consumer progress. Core path: API -> broker -> partition log.
package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/Achiuu13/spool/internal/groups"
	spoollog "github.com/Achiuu13/spool/internal/log"
	"github.com/Achiuu13/spool/internal/offsets"
)

var (
	streamNameRE     = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,199}$`)
	ErrInvalidStream = errors.New("broker: invalid stream name")
	ErrNoSuchStream  = errors.New("broker: stream does not exist")
	ErrNoSuchPart    = errors.New("broker: partition does not exist")
)

// Options configures a Broker.
type Options struct {
	DataDir         string
	SyncEveryAppend bool
	// DefaultPartitions is the partition count for newly created streams.
	// Zero means 1. Existing streams keep the count in their meta.json.
	DefaultPartitions int
	Groups            groups.Options
}

type stream struct {
	logs []*spoollog.Log
	rr   atomic.Uint32 // round-robin counter for keyless records
}

// Broker owns the partition logs under DataDir, the offsets store, and
// consumer-group membership.
type Broker struct {
	opts    Options
	offsets *offsets.Store
	groups  *groups.Manager

	mu      sync.Mutex
	streams map[string]*stream
}

type streamMeta struct {
	Partitions int `json:"partitions"`
}

// Open loads existing streams from DataDir and the committed-offsets store.
func Open(opts Options) (*Broker, error) {
	if opts.DefaultPartitions <= 0 {
		opts.DefaultPartitions = 1
	}
	streamsDir := filepath.Join(opts.DataDir, "streams")
	if err := os.MkdirAll(streamsDir, 0o755); err != nil {
		return nil, err
	}
	store, err := offsets.Open(filepath.Join(opts.DataDir, "offsets.json"))
	if err != nil {
		return nil, err
	}
	b := &Broker{
		opts:    opts,
		offsets: store,
		groups:  groups.NewManager(opts.Groups),
		streams: map[string]*stream{},
	}

	entries, err := os.ReadDir(streamsDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() || !streamNameRE.MatchString(e.Name()) {
			continue
		}
		if _, err := b.openStreamLocked(e.Name(), 0); err != nil {
			b.Close()
			return nil, fmt.Errorf("broker: recover stream %q: %w", e.Name(), err)
		}
	}
	return b, nil
}

// openStreamLocked opens an existing stream directory, or creates one with
// createWith partitions when createWith > 0. Caller need not hold mu
// during Open (single-threaded); at runtime it is called under mu.
func (b *Broker) openStreamLocked(name string, createWith int) (*stream, error) {
	dir := filepath.Join(b.opts.DataDir, "streams", name)
	metaPath := filepath.Join(dir, "meta.json")
	var meta streamMeta
	data, err := os.ReadFile(metaPath)
	switch {
	case err == nil:
		if err := json.Unmarshal(data, &meta); err != nil || meta.Partitions <= 0 {
			return nil, fmt.Errorf("broker: corrupt meta for stream %q", name)
		}
	case os.IsNotExist(err):
		if _, statErr := os.Stat(dir); statErr == nil && createWith <= 0 {
			// Pre-partitioning data directory: single partition.
			meta.Partitions = 1
		} else if createWith > 0 {
			meta.Partitions = createWith
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
			data, _ := json.Marshal(meta)
			if err := os.WriteFile(metaPath, data, 0o644); err != nil {
				return nil, err
			}
		} else {
			return nil, ErrNoSuchStream
		}
	default:
		return nil, err
	}

	s := &stream{}
	for p := 0; p < meta.Partitions; p++ {
		l, err := spoollog.Open(filepath.Join(dir, fmt.Sprintf("%d.log", p)),
			spoollog.Options{SyncEveryAppend: b.opts.SyncEveryAppend})
		if err != nil {
			for _, opened := range s.logs {
				opened.Close()
			}
			return nil, err
		}
		s.logs = append(s.logs, l)
	}
	b.streams[name] = s
	return s, nil
}

func (b *Broker) stream(name string, create bool) (*stream, error) {
	createWith := 0
	if create {
		createWith = b.opts.DefaultPartitions
	}
	return b.streamWithCount(name, createWith)
}

// streamWithCount returns the stream, creating it with createWith partitions
// when createWith > 0 and it does not exist yet.
func (b *Broker) streamWithCount(name string, createWith int) (*stream, error) {
	if !streamNameRE.MatchString(name) {
		return nil, ErrInvalidStream
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.streams[name]; ok {
		return s, nil
	}
	if createWith <= 0 {
		return nil, ErrNoSuchStream
	}
	return b.openStreamLocked(name, createWith)
}

// route picks the partition for a record: FNV-1a hash of the key modulo the
// partition count, so the same key always lands on the same partition.
// Keyless records are distributed round-robin.
func (s *stream) route(key []byte) int {
	n := uint32(len(s.logs))
	if n == 1 {
		return 0
	}
	if len(key) == 0 {
		return int((s.rr.Add(1) - 1) % n)
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % n)
}

// Publish appends a record to the stream, creating the stream on first use.
// It returns the assigned partition and offset once the record is durable
// per the configured sync policy.
func (b *Broker) Publish(name string, key, value []byte) (partition int, offset int64, err error) {
	s, err := b.stream(name, true)
	if err != nil {
		return 0, 0, err
	}
	p := s.route(key)
	off, err := s.logs[p].Append(key, value)
	return p, off, err
}

// Read returns up to limit records from the partition starting at offset.
func (b *Broker) Read(name string, partition int, offset int64, limit int) ([]spoollog.Record, error) {
	s, err := b.stream(name, false)
	if err != nil {
		return nil, err
	}
	if partition < 0 || partition >= len(s.logs) {
		return nil, ErrNoSuchPart
	}
	return s.logs[partition].Read(offset, limit)
}

// Partitions returns the stream's partition count.
func (b *Broker) Partitions(name string) (int, error) {
	s, err := b.stream(name, false)
	if err != nil {
		return 0, err
	}
	return len(s.logs), nil
}

// NextOffset returns the next offset the partition will assign.
func (b *Broker) NextOffset(name string, partition int) (int64, error) {
	s, err := b.stream(name, false)
	if err != nil {
		return 0, err
	}
	if partition < 0 || partition >= len(s.logs) {
		return 0, ErrNoSuchPart
	}
	return s.logs[partition].NextOffset(), nil
}

func (b *Broker) validateGroupTarget(name string, partition int) error {
	s, err := b.stream(name, false)
	if err != nil {
		return err
	}
	if partition < 0 || partition >= len(s.logs) {
		return ErrNoSuchPart
	}
	return nil
}

// CommitOffset durably stores the next offset to read for a group/partition.
// When memberID is non-empty, the commit is fenced: it is rejected with
// groups.ErrNotOwner unless that member currently owns the partition.
func (b *Broker) CommitOffset(group, name string, partition int, next int64, memberID string) error {
	if err := b.validateGroupTarget(name, partition); err != nil {
		return err
	}
	if memberID != "" {
		if err := b.groups.Authorize(group, name, memberID, partition); err != nil {
			return err
		}
	}
	return b.offsets.Commit(group, name, partition, next)
}

// CommittedOffset returns the stored next offset for a group/partition,
// or 0 if the group has not committed yet.
func (b *Broker) CommittedOffset(group, name string, partition int) (int64, error) {
	if err := b.validateGroupTarget(name, partition); err != nil {
		return 0, err
	}
	next, _ := b.offsets.Get(group, name, partition)
	return next, nil
}

// JoinGroup registers a new member, creating the stream if needed so
// consumers can join before the first publish.
func (b *Broker) JoinGroup(group, name string) (groups.Assignment, error) {
	s, err := b.stream(name, true)
	if err != nil {
		return groups.Assignment{}, err
	}
	return b.groups.Join(group, name, len(s.logs))
}

// Heartbeat refreshes a member's session and returns its current assignment.
func (b *Broker) Heartbeat(group, name, memberID string) (groups.Assignment, error) {
	return b.groups.Heartbeat(group, name, memberID)
}

// LeaveGroup removes a member and reassigns its partitions.
func (b *Broker) LeaveGroup(group, name, memberID string) error {
	return b.groups.Leave(group, name, memberID)
}

// Close closes all partition logs.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, s := range b.streams {
		for _, l := range s.logs {
			if err := l.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
