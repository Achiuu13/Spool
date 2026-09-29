package groups

import (
	"testing"
	"time"
)

func newTestManager(timeout time.Duration) (*Manager, *time.Time) {
	m := NewManager(Options{SessionTimeout: timeout})
	now := time.Now()
	m.now = func() time.Time { return now }
	return m, &now
}

func TestSingleMemberOwnsAllPartitions(t *testing.T) {
	m, _ := newTestManager(time.Minute)
	a, err := m.Join("g", "s", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Partitions) != 3 {
		t.Fatalf("partitions = %v, want all 3", a.Partitions)
	}
}

func TestEachPartitionHasExactlyOneOwner(t *testing.T) {
	m, _ := newTestManager(time.Minute)
	a1, _ := m.Join("g", "s", 4)
	a2, _ := m.Join("g", "s", 4)
	// a1's assignment may have changed after a2 joined; re-fetch.
	a1, err := m.Heartbeat("g", "s", a1.MemberID)
	if err != nil {
		t.Fatal(err)
	}
	a2, _ = m.Heartbeat("g", "s", a2.MemberID)

	seen := map[int]int{}
	for _, p := range a1.Partitions {
		seen[p]++
	}
	for _, p := range a2.Partitions {
		seen[p]++
	}
	if len(seen) != 4 {
		t.Fatalf("assigned partitions = %v, want all 4 covered", seen)
	}
	for p, n := range seen {
		if n != 1 {
			t.Fatalf("partition %d assigned to %d members", p, n)
		}
	}
}

func TestLeaveTriggersReassignment(t *testing.T) {
	m, _ := newTestManager(time.Minute)
	a1, _ := m.Join("g", "s", 3)
	a2, _ := m.Join("g", "s", 3)
	if err := m.Leave("g", "s", a2.MemberID); err != nil {
		t.Fatal(err)
	}
	a1, _ = m.Heartbeat("g", "s", a1.MemberID)
	if len(a1.Partitions) != 3 {
		t.Fatalf("survivor partitions = %v, want all 3", a1.Partitions)
	}
}

func TestSessionTimeoutExpiresMember(t *testing.T) {
	m, now := newTestManager(10 * time.Second)
	a1, _ := m.Join("g", "s", 2)
	a2, _ := m.Join("g", "s", 2)

	// a1 keeps heartbeating; a2 goes silent past the timeout.
	*now = now.Add(6 * time.Second)
	m.Heartbeat("g", "s", a1.MemberID)
	*now = now.Add(6 * time.Second)

	a1After, err := m.Heartbeat("g", "s", a1.MemberID)
	if err != nil {
		t.Fatal(err)
	}
	if len(a1After.Partitions) != 2 {
		t.Fatalf("survivor partitions = %v, want both", a1After.Partitions)
	}
	if _, err := m.Heartbeat("g", "s", a2.MemberID); err != ErrUnknownMember {
		t.Fatalf("expired member heartbeat err = %v, want ErrUnknownMember", err)
	}
}

func TestStaleMemberCannotCommit(t *testing.T) {
	m, now := newTestManager(10 * time.Second)
	a1, _ := m.Join("g", "s", 2)
	old := a1.Partitions[0]
	if err := m.Authorize("g", "s", a1.MemberID, old); err != nil {
		t.Fatalf("owner authorize: %v", err)
	}

	// a1 expires; a2 takes over its partitions.
	*now = now.Add(11 * time.Second)
	a2, _ := m.Join("g", "s", 2)
	if err := m.Authorize("g", "s", a2.MemberID, old); err != nil {
		t.Fatalf("new owner authorize: %v", err)
	}
	if err := m.Authorize("g", "s", a1.MemberID, old); err != ErrUnknownMember {
		t.Fatalf("stale member authorize err = %v, want ErrUnknownMember", err)
	}
}

func TestNonOwnerCommitFenced(t *testing.T) {
	m, _ := newTestManager(time.Minute)
	a1, _ := m.Join("g", "s", 2)
	a2, _ := m.Join("g", "s", 2)
	a1, _ = m.Heartbeat("g", "s", a1.MemberID)
	a2, _ = m.Heartbeat("g", "s", a2.MemberID)
	if len(a1.Partitions) != 1 || len(a2.Partitions) != 1 {
		t.Fatalf("split = %v / %v, want 1 each", a1.Partitions, a2.Partitions)
	}
	if err := m.Authorize("g", "s", a1.MemberID, a2.Partitions[0]); err != ErrNotOwner {
		t.Fatalf("cross-member authorize err = %v, want ErrNotOwner", err)
	}
}

func TestGenerationBumpsOnRebalance(t *testing.T) {
	m, _ := newTestManager(time.Minute)
	a1, _ := m.Join("g", "s", 2)
	gen1 := a1.Generation
	a2, _ := m.Join("g", "s", 2)
	if a2.Generation <= gen1 {
		t.Fatalf("generation %d after join, want > %d", a2.Generation, gen1)
	}
}

func TestSeparateGroupsIndependent(t *testing.T) {
	m, _ := newTestManager(time.Minute)
	a1, _ := m.Join("feed", "s", 2)
	a2, _ := m.Join("analytics", "s", 2)
	if len(a1.Partitions) != 2 || len(a2.Partitions) != 2 {
		t.Fatalf("each group's sole member should own all partitions: %v / %v",
			a1.Partitions, a2.Partitions)
	}
}
