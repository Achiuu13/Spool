package log

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTemp(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "partition-0.log")
	l, err := Open(path, Options{SyncEveryAppend: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l, path
}

func TestAppendReadInOrder(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()
	for i := 0; i < 3; i++ {
		off, err := l.Append([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if off != int64(i) {
			t.Fatalf("offset = %d, want %d", off, i)
		}
	}
	recs, err := l.Read(0, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	for i, r := range recs {
		if r.Offset != int64(i) || string(r.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("record %d = %+v", i, r)
		}
	}
}

func TestReadFromMiddleOffset(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()
	for i := 0; i < 3; i++ {
		l.Append(nil, []byte(fmt.Sprintf("v%d", i)))
	}
	recs, err := l.Read(1, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 2 || recs[0].Offset != 1 || recs[1].Offset != 2 {
		t.Fatalf("got %+v", recs)
	}
}

func TestReadPastEndReturnsEmpty(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()
	l.Append(nil, []byte("v"))
	recs, err := l.Read(5, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("got %d records, want 0", len(recs))
	}
}

func TestRestartPreservesRecords(t *testing.T) {
	l, path := openTemp(t)
	for i := 0; i < 5; i++ {
		l.Append([]byte("key"), []byte(fmt.Sprintf("v%d", i)))
	}
	l.Close()

	l2, err := Open(path, Options{SyncEveryAppend: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if l2.NextOffset() != 5 {
		t.Fatalf("NextOffset = %d, want 5", l2.NextOffset())
	}
	recs, err := l2.Read(0, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	for i, r := range recs {
		if r.Offset != int64(i) || string(r.Value) != fmt.Sprintf("v%d", i) || !bytes.Equal(r.Key, []byte("key")) {
			t.Fatalf("record %d = %+v", i, r)
		}
	}
	if off, _ := l2.Append(nil, []byte("after")); off != 5 {
		t.Fatalf("append after reopen: offset = %d, want 5", off)
	}
}

func TestIncompleteTrailingWriteTruncated(t *testing.T) {
	l, path := openTemp(t)
	for i := 0; i < 3; i++ {
		l.Append(nil, []byte(fmt.Sprintf("v%d", i)))
	}
	l.Close()

	// Simulate a crash mid-write: append half of a valid frame.
	frame, _ := encodeFrame(Record{Offset: 3, Value: []byte("partial")})
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write(frame[:len(frame)/2])
	f.Close()

	l2, err := Open(path, Options{SyncEveryAppend: true})
	if err != nil {
		t.Fatalf("reopen after partial write: %v", err)
	}
	defer l2.Close()
	if l2.NextOffset() != 3 {
		t.Fatalf("NextOffset = %d, want 3", l2.NextOffset())
	}
	recs, err := l2.Read(0, 10)
	if err != nil || len(recs) != 3 {
		t.Fatalf("Read = %v records, err %v", len(recs), err)
	}
	if off, _ := l2.Append(nil, []byte("v3")); off != 3 {
		t.Fatalf("offset after truncation = %d, want 3", off)
	}
}

func TestTornFinalFrameTruncated(t *testing.T) {
	l, path := openTemp(t)
	l.Append(nil, []byte("v0"))
	l.Append(nil, []byte("v1"))
	l.Close()

	// Flip a byte inside the final frame's value: torn write at the tail.
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xFF
	os.WriteFile(path, data, 0o644)

	l2, err := Open(path, Options{SyncEveryAppend: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if l2.NextOffset() != 1 {
		t.Fatalf("NextOffset = %d, want 1 (bad tail truncated)", l2.NextOffset())
	}
}

func TestMidLogCorruptionReported(t *testing.T) {
	l, path := openTemp(t)
	l.Append(nil, []byte("v0"))
	posOfSecond := l.size
	l.Append(nil, []byte("v1"))
	l.Append(nil, []byte("v2"))
	l.Close()

	// Corrupt the middle record's body.
	f, _ := os.OpenFile(path, os.O_WRONLY, 0)
	f.WriteAt([]byte{0xFF}, posOfSecond+frameHeader+10)
	f.Close()

	_, err := Open(path, Options{SyncEveryAppend: true})
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want CorruptionError", err)
	}
	if ce.Offset != 1 {
		t.Fatalf("CorruptionError.Offset = %d, want 1", ce.Offset)
	}
}

func TestRejectOversizedRecord(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()
	_, err := l.Append(nil, make([]byte, MaxRecordBytes+1))
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("err = %v, want ErrRecordTooLarge", err)
	}
	if l.NextOffset() != 0 {
		t.Fatalf("rejected append must not consume an offset")
	}
}

func TestConcurrentAppendsUniqueOffsets(t *testing.T) {
	l, path := openTemp(t)
	const goroutines, perG = 8, 50
	offsets := make(chan int64, goroutines*perG)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				off, err := l.Append([]byte(fmt.Sprintf("g%d", g)), []byte("v"))
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				offsets <- off
			}
		}(g)
	}
	wg.Wait()
	close(offsets)
	seen := map[int64]bool{}
	for off := range offsets {
		if seen[off] {
			t.Fatalf("duplicate offset %d", off)
		}
		seen[off] = true
	}
	if len(seen) != goroutines*perG {
		t.Fatalf("got %d offsets, want %d", len(seen), goroutines*perG)
	}
	l.Close()

	// Every acknowledged record must be readable after reopen.
	l2, err := Open(path, Options{SyncEveryAppend: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	recs, err := l2.Read(0, goroutines*perG)
	if err != nil || len(recs) != goroutines*perG {
		t.Fatalf("Read = %d records, err %v", len(recs), err)
	}
}

func TestEmptyLogReadsEmpty(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()
	recs, err := l.Read(0, 10)
	if err != nil || len(recs) != 0 {
		t.Fatalf("Read on empty log = %v, %v", recs, err)
	}
}

func TestTruncateFromRemovesSuffix(t *testing.T) {
	l, path := openTemp(t)
	for i := 0; i < 5; i++ {
		if _, err := l.Append(nil, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.TruncateFrom(3); err != nil {
		t.Fatal(err)
	}
	if l.NextOffset() != 3 {
		t.Fatalf("NextOffset = %d, want 3", l.NextOffset())
	}
	// Appends continue from the truncation point.
	off, err := l.Append(nil, []byte("replacement"))
	if err != nil || off != 3 {
		t.Fatalf("append after truncate = %d, %v; want offset 3", off, err)
	}
	l.Close()

	// The truncation is durable.
	l2, err := Open(path, Options{SyncEveryAppend: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	recs, err := l2.Read(0, 100)
	if err != nil || len(recs) != 4 {
		t.Fatalf("after reopen: %d records, %v; want 4", len(recs), err)
	}
	if string(recs[3].Value) != "replacement" {
		t.Fatalf("record 3 = %q, want replacement", recs[3].Value)
	}
}

func TestTruncateFromPastEndIsNoop(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()
	l.Append(nil, []byte("v"))
	if err := l.TruncateFrom(5); err != nil {
		t.Fatal(err)
	}
	if l.NextOffset() != 1 {
		t.Fatalf("NextOffset = %d, want 1", l.NextOffset())
	}
	if err := l.TruncateFrom(-1); err != ErrOffsetOutOfRange {
		t.Fatalf("negative truncate err = %v", err)
	}
}

func TestAppendRecordUsesExplicitTimestamp(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()
	if _, err := l.AppendRecord(42, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	recs, err := l.Read(0, 1)
	if err != nil || len(recs) != 1 {
		t.Fatal(err)
	}
	if recs[0].Timestamp != 42 {
		t.Fatalf("timestamp = %d, want 42", recs[0].Timestamp)
	}
}
