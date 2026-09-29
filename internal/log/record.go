// Package log implements Spool's durable, append-only partition log.
//
// On-disk frame layout (all integers big-endian):
//
//	[4 bytes: body length][4 bytes: CRC-32C of body][body]
//
// body:
//
//	[8 bytes: offset][8 bytes: unix-nano timestamp]
//	[4 bytes: key length][key][4 bytes: value length][value]
package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// MaxRecordBytes caps len(key)+len(value) for a single record.
	MaxRecordBytes = 1 << 20 // 1 MiB

	bodyOverhead = 8 + 8 + 4 + 4 // offset + timestamp + key len + value len
	frameHeader  = 4 + 4         // body length + checksum
	maxBodyBytes = MaxRecordBytes + bodyOverhead
)

var (
	ErrRecordTooLarge = errors.New("log: record exceeds 1 MiB limit")
	castagnoli        = crc32.MakeTable(crc32.Castagnoli)
)

// Record is a single entry in a partition log.
type Record struct {
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       []byte `json:"key,omitempty"`
	Value     []byte `json:"value"`
}

func encodeFrame(r Record) ([]byte, error) {
	if len(r.Key)+len(r.Value) > MaxRecordBytes {
		return nil, ErrRecordTooLarge
	}
	bodyLen := bodyOverhead + len(r.Key) + len(r.Value)
	buf := make([]byte, frameHeader+bodyLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	body := buf[frameHeader:]
	binary.BigEndian.PutUint64(body[0:8], uint64(r.Offset))
	binary.BigEndian.PutUint64(body[8:16], uint64(r.Timestamp))
	binary.BigEndian.PutUint32(body[16:20], uint32(len(r.Key)))
	copy(body[20:], r.Key)
	p := 20 + len(r.Key)
	binary.BigEndian.PutUint32(body[p:p+4], uint32(len(r.Value)))
	copy(body[p+4:], r.Value)
	binary.BigEndian.PutUint32(buf[4:8], crc32.Checksum(body, castagnoli))
	return buf, nil
}

// decodeBody parses and validates a frame body whose checksum already matched.
func decodeBody(body []byte) (Record, error) {
	if len(body) < bodyOverhead {
		return Record{}, fmt.Errorf("log: body too short (%d bytes)", len(body))
	}
	r := Record{
		Offset:    int64(binary.BigEndian.Uint64(body[0:8])),
		Timestamp: int64(binary.BigEndian.Uint64(body[8:16])),
	}
	keyLen := binary.BigEndian.Uint32(body[16:20])
	if uint64(keyLen) > uint64(len(body)-bodyOverhead) {
		return Record{}, fmt.Errorf("log: key length %d exceeds body", keyLen)
	}
	p := 20 + int(keyLen)
	valLen := binary.BigEndian.Uint32(body[p : p+4])
	if uint64(valLen) != uint64(len(body)-p-4) {
		return Record{}, fmt.Errorf("log: value length %d does not match body", valLen)
	}
	if keyLen > 0 {
		r.Key = append([]byte(nil), body[20:p]...)
	}
	r.Value = append([]byte(nil), body[p+4:]...)
	return r, nil
}
