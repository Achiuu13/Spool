package broker

import (
	"fmt"
	"testing"
)

func openTestBroker(t *testing.T, dir string, partitions int) *Broker {
	t.Helper()
	b, err := Open(Options{DataDir: dir, SyncEveryAppend: false, DefaultPartitions: partitions})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return b
}

func TestKeyRoutingIsDeterministic(t *testing.T) {
	b := openTestBroker(t, t.TempDir(), 4)
	defer b.Close()
	keys := []string{"order-1", "order-2", "order-3", "user-42"}
	first := map[string]int{}
	for _, k := range keys {
		p, _, err := b.Publish("orders", []byte(k), []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		first[k] = p
	}
	for round := 0; round < 3; round++ {
		for _, k := range keys {
			p, _, _ := b.Publish("orders", []byte(k), []byte("v"))
			if p != first[k] {
				t.Fatalf("key %q routed to partition %d, previously %d", k, p, first[k])
			}
		}
	}
}

func TestKeylessRecordsSpreadAcrossPartitions(t *testing.T) {
	b := openTestBroker(t, t.TempDir(), 3)
	defer b.Close()
	seen := map[int]int{}
	for i := 0; i < 30; i++ {
		p, _, err := b.Publish("orders", nil, []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		seen[p]++
	}
	if len(seen) != 3 {
		t.Fatalf("keyless records hit %d partitions, want 3: %v", len(seen), seen)
	}
	for p, n := range seen {
		if n != 10 {
			t.Fatalf("round-robin uneven: partition %d got %d", p, n)
		}
	}
}

func TestPerPartitionOffsetsIndependent(t *testing.T) {
	b := openTestBroker(t, t.TempDir(), 3)
	defer b.Close()
	counts := map[int]int64{}
	for i := 0; i < 9; i++ {
		p, off, err := b.Publish("orders", nil, []byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if off != counts[p] {
			t.Fatalf("partition %d offset = %d, want %d", p, off, counts[p])
		}
		counts[p]++
	}
}

func TestPartitionCountSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	b := openTestBroker(t, dir, 5)
	b.Publish("orders", []byte("k"), []byte("v"))
	b.Close()

	// Reopen with a different default: existing stream keeps its count.
	b2 := openTestBroker(t, dir, 2)
	defer b2.Close()
	n, err := b2.Partitions("orders")
	if err != nil || n != 5 {
		t.Fatalf("Partitions = %d, %v; want 5", n, err)
	}
}

func TestRecordsSurviveReopenPerPartition(t *testing.T) {
	dir := t.TempDir()
	b := openTestBroker(t, dir, 3)
	type loc struct{ p int }
	wrote := map[string]int{}
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("key-%d", i)
		p, _, err := b.Publish("orders", []byte(k), []byte("v-"+k))
		if err != nil {
			t.Fatal(err)
		}
		wrote[k] = p
	}
	_ = loc{}
	b.Close()

	b2 := openTestBroker(t, dir, 3)
	defer b2.Close()
	for k, p := range wrote {
		recs, err := b2.Read("orders", p, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range recs {
			if string(r.Key) == k && string(r.Value) == "v-"+k {
				found = true
			}
		}
		if !found {
			t.Fatalf("key %q not found in partition %d after reopen", k, p)
		}
	}
}
