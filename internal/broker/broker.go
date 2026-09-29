// Package broker coordinates streams and their partition logs, and exposes
// committed consumer progress. Core path: API -> broker -> partition log.
package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	spoollog "github.com/Achiuu13/spool/internal/log"
	"github.com/Achiuu13/spool/internal/offsets"
)

// The single-partition version uses partition 0 throughout.
const partitionCount = 1

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
}

// Broker owns the partition logs under DataDir and the offsets store.
type Broker struct {
	opts    Options
	offsets *offsets.Store

	mu      sync.Mutex
	streams map[string]*spoollog.Log // stream name -> partition 0 log
}

// Open loads existing streams from DataDir and the committed-offsets store.
func Open(opts Options) (*Broker, error) {
	streamsDir := filepath.Join(opts.DataDir, "streams")
	if err := os.MkdirAll(streamsDir, 0o755); err != nil {
		return nil, err
	}
	store, err := offsets.Open(filepath.Join(opts.DataDir, "offsets.json"))
	if err != nil {
		return nil, err
	}
	b := &Broker{opts: opts, offsets: store, streams: map[string]*spoollog.Log{}}

	entries, err := os.ReadDir(streamsDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() || !streamNameRE.MatchString(e.Name()) {
			continue
		}
		l, err := spoollog.Open(filepath.Join(streamsDir, e.Name(), "0.log"),
			spoollog.Options{SyncEveryAppend: opts.SyncEveryAppend})
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("broker: recover stream %q: %w", e.Name(), err)
		}
		b.streams[e.Name()] = l
	}
	return b, nil
}

func (b *Broker) log(stream string, create bool) (*spoollog.Log, error) {
	if !streamNameRE.MatchString(stream) {
		return nil, ErrInvalidStream
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if l, ok := b.streams[stream]; ok {
		return l, nil
	}
	if !create {
		return nil, ErrNoSuchStream
	}
	dir := filepath.Join(b.opts.DataDir, "streams", stream)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l, err := spoollog.Open(filepath.Join(dir, "0.log"),
		spoollog.Options{SyncEveryAppend: b.opts.SyncEveryAppend})
	if err != nil {
		return nil, err
	}
	b.streams[stream] = l
	return l, nil
}

// Publish appends a record to the stream, creating the stream on first use.
// It returns the assigned partition and offset once the record is durable
// per the configured sync policy.
func (b *Broker) Publish(stream string, key, value []byte) (partition int, offset int64, err error) {
	l, err := b.log(stream, true)
	if err != nil {
		return 0, 0, err
	}
	off, err := l.Append(key, value)
	return 0, off, err
}

// Read returns up to limit records from the partition starting at offset.
func (b *Broker) Read(stream string, partition int, offset int64, limit int) ([]spoollog.Record, error) {
	if partition < 0 || partition >= partitionCount {
		return nil, ErrNoSuchPart
	}
	l, err := b.log(stream, false)
	if err != nil {
		return nil, err
	}
	return l.Read(offset, limit)
}

// NextOffset returns the next offset the partition will assign.
func (b *Broker) NextOffset(stream string, partition int) (int64, error) {
	if partition < 0 || partition >= partitionCount {
		return 0, ErrNoSuchPart
	}
	l, err := b.log(stream, false)
	if err != nil {
		return 0, err
	}
	return l.NextOffset(), nil
}

// CommitOffset durably stores the next offset to read for a group/partition.
func (b *Broker) CommitOffset(group, stream string, partition int, next int64) error {
	if !streamNameRE.MatchString(stream) {
		return ErrInvalidStream
	}
	if partition < 0 || partition >= partitionCount {
		return ErrNoSuchPart
	}
	return b.offsets.Commit(group, stream, partition, next)
}

// CommittedOffset returns the stored next offset for a group/partition,
// or 0 if the group has not committed yet.
func (b *Broker) CommittedOffset(group, stream string, partition int) (int64, error) {
	if !streamNameRE.MatchString(stream) {
		return 0, ErrInvalidStream
	}
	if partition < 0 || partition >= partitionCount {
		return 0, ErrNoSuchPart
	}
	next, _ := b.offsets.Get(group, stream, partition)
	return next, nil
}

// Close closes all partition logs.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, l := range b.streams {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
