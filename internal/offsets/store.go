// Package offsets persists committed consumer progress: the next offset to
// read for each (group, stream, partition).
//
// Commits are written atomically: the full state is serialized to a temp
// file, fsynced, and renamed over the previous file. If Spool stops partway
// through a commit, the previous committed state survives intact; a commit
// is never half-applied.
package offsets

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store holds committed next-offsets, backed by a single JSON file.
type Store struct {
	path string

	mu        sync.Mutex
	committed map[string]int64
}

func key(group, stream string, partition int) string {
	return fmt.Sprintf("%s/%s/%d", group, stream, partition)
}

// Open loads the store at path, creating an empty one if absent.
func Open(path string) (*Store, error) {
	s := &Store{path: path, committed: map[string]int64{}}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &s.committed); err != nil {
		return nil, fmt.Errorf("offsets: corrupt store %s: %w", path, err)
	}
	return s, nil
}

// Commit durably records the next offset to read for the group/partition.
func (s *Store) Commit(group, stream string, partition int, next int64) error {
	if next < 0 {
		return fmt.Errorf("offsets: negative next offset %d", next)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(group, stream, partition)
	prev, had := s.committed[k]
	s.committed[k] = next
	if err := s.flushLocked(); err != nil {
		if had {
			s.committed[k] = prev
		} else {
			delete(s.committed, k)
		}
		return err
	}
	return nil
}

// Get returns the committed next offset, or 0/false if none exists.
func (s *Store) Get(group, stream string, partition int) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next, ok := s.committed[key(group, stream, partition)]
	return next, ok
}

func (s *Store) flushLocked() error {
	data, err := json.Marshal(s.committed)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".offsets-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.path)
}
