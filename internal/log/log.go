package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// ErrOffsetOutOfRange is returned when a read starts before the log's first
// offset (always 0 in this version).
var ErrOffsetOutOfRange = errors.New("log: offset out of range")

// CorruptionError reports a damaged record found in the middle of the log.
// It is returned from Open so the operator can decide how to repair; the log
// refuses to serve rather than silently skipping data.
type CorruptionError struct {
	Path   string
	Pos    int64 // byte position of the damaged frame
	Offset int64 // expected offset at that position
	Reason string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("log: corrupt record in %s at byte %d (expected offset %d): %s",
		e.Path, e.Pos, e.Offset, e.Reason)
}

// Options configures a Log.
type Options struct {
	// SyncEveryAppend fsyncs after each append before acknowledging.
	// This is Spool's documented durability policy for acked records.
	SyncEveryAppend bool
}

// Log is a single append-only partition log backed by one file.
// Appends are serialized; reads use positional I/O and may run concurrently
// with appends.
type Log struct {
	path string
	opts Options

	mu         sync.Mutex
	f          *os.File
	positions  []int64 // offset -> byte position of its frame
	size       int64   // current file size (end of last valid frame)
	nextOffset int64
}

// Open opens or creates the log at path, scans it to rebuild the offset
// index, truncates an incomplete trailing record, and returns an error for
// corruption found before the end of the log.
func Open(path string, opts Options) (*Log, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{path: path, opts: opts, f: f}
	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// recover scans the file front to back, validating frame lengths, checksums,
// and sequential offsets. A damaged suffix (incomplete frame, or a bad final
// frame from a torn write) is truncated. Damage followed by more data is a
// CorruptionError.
func (l *Log) recover() error {
	info, err := l.f.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	var pos int64
	header := make([]byte, frameHeader)
	for pos < fileSize {
		reason, frameEnd := "", int64(-1)
		if _, err := l.f.ReadAt(header, pos); err != nil {
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				return l.truncateAt(pos) // partial header at tail
			}
			return err
		}
		bodyLen := binary.BigEndian.Uint32(header[0:4])
		if bodyLen < bodyOverhead || bodyLen > maxBodyBytes {
			reason = fmt.Sprintf("invalid body length %d", bodyLen)
		} else {
			frameEnd = pos + frameHeader + int64(bodyLen)
			if frameEnd > fileSize {
				return l.truncateAt(pos) // incomplete trailing record
			}
			body := make([]byte, bodyLen)
			if _, err := l.f.ReadAt(body, pos+frameHeader); err != nil {
				return err
			}
			if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(header[4:8]) {
				reason = "checksum mismatch"
			} else if rec, err := decodeBody(body); err != nil {
				reason = err.Error()
			} else if rec.Offset != l.nextOffset {
				reason = fmt.Sprintf("offset %d, expected %d", rec.Offset, l.nextOffset)
			}
		}
		if reason != "" {
			// A bad final frame is indistinguishable from a torn write:
			// truncate it. Bad data with more data after it is corruption.
			if frameEnd == fileSize {
				return l.truncateAt(pos)
			}
			return &CorruptionError{Path: l.path, Pos: pos, Offset: l.nextOffset, Reason: reason}
		}
		l.positions = append(l.positions, pos)
		l.nextOffset++
		pos = frameEnd
	}
	l.size = pos
	return nil
}

func (l *Log) truncateAt(pos int64) error {
	if err := l.f.Truncate(pos); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.size = pos
	return nil
}

// Append writes a record, syncs per policy, and returns its offset.
func (l *Log) Append(key, value []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := Record{
		Offset:    l.nextOffset,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
	}
	frame, err := encodeFrame(rec)
	if err != nil {
		return 0, err
	}
	if _, err := l.f.WriteAt(frame, l.size); err != nil {
		return 0, err
	}
	if l.opts.SyncEveryAppend {
		if err := l.f.Sync(); err != nil {
			return 0, err
		}
	}
	l.positions = append(l.positions, l.size)
	l.size += int64(len(frame))
	l.nextOffset++
	return rec.Offset, nil
}

// Read returns up to max records starting at offset, in order. Reading at or
// past the end returns an empty slice.
func (l *Log) Read(offset int64, max int) ([]Record, error) {
	if offset < 0 {
		return nil, ErrOffsetOutOfRange
	}
	l.mu.Lock()
	end := l.nextOffset
	var startPos int64
	if offset < end {
		startPos = l.positions[offset]
	}
	l.mu.Unlock()

	if offset >= end || max <= 0 {
		return []Record{}, nil
	}
	n := end - offset
	if int64(max) < n {
		n = int64(max)
	}
	records := make([]Record, 0, n)
	pos := startPos
	header := make([]byte, frameHeader)
	for int64(len(records)) < n {
		if _, err := l.f.ReadAt(header, pos); err != nil {
			return nil, fmt.Errorf("log: read frame header at %d: %w", pos, err)
		}
		bodyLen := binary.BigEndian.Uint32(header[0:4])
		if bodyLen < bodyOverhead || bodyLen > maxBodyBytes {
			return nil, fmt.Errorf("log: invalid body length %d at byte %d", bodyLen, pos)
		}
		body := make([]byte, bodyLen)
		if _, err := l.f.ReadAt(body, pos+frameHeader); err != nil {
			return nil, fmt.Errorf("log: read frame body at %d: %w", pos, err)
		}
		if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(header[4:8]) {
			return nil, fmt.Errorf("log: checksum mismatch at byte %d", pos)
		}
		rec, err := decodeBody(body)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
		pos += frameHeader + int64(bodyLen)
	}
	return records, nil
}

// NextOffset returns the offset the next appended record will receive.
func (l *Log) NextOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextOffset
}

// Close syncs and closes the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.f.Sync(); err != nil {
		l.f.Close()
		return err
	}
	return l.f.Close()
}
