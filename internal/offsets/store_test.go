package offsets

import (
	"path/filepath"
	"testing"
)

func TestCommitPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.json")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Commit("analytics", "orders", 0, 8); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	next, ok := s2.Get("analytics", "orders", 0)
	if !ok || next != 8 {
		t.Fatalf("Get = %d, %v; want 8, true", next, ok)
	}
}

func TestGetMissingGroup(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "offsets.json"))
	if next, ok := s.Get("nobody", "orders", 0); ok || next != 0 {
		t.Fatalf("Get = %d, %v; want 0, false", next, ok)
	}
}

func TestGroupsAreIndependent(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "offsets.json"))
	s.Commit("feed", "orders", 0, 3)
	s.Commit("analytics", "orders", 0, 10)
	if next, _ := s.Get("feed", "orders", 0); next != 3 {
		t.Fatalf("feed next = %d, want 3", next)
	}
	if next, _ := s.Get("analytics", "orders", 0); next != 10 {
		t.Fatalf("analytics next = %d, want 10", next)
	}
}

func TestRejectNegativeCommit(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "offsets.json"))
	if err := s.Commit("g", "s", 0, -1); err == nil {
		t.Fatal("expected error for negative commit")
	}
}
