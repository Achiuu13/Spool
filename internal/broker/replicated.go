package broker

import (
	"encoding/json"
	"fmt"
)

// Replicated operations. In cluster mode every state change is encoded as
// an Op, committed through the replication log, and applied via ApplyOp on
// each node in the same order, so derived state stays identical.
//
// The leader makes all non-deterministic choices (partition, partition
// count, timestamp) before proposing; apply is pure.

const (
	OpPublish      = "publish"
	OpCommitOffset = "commit_offset"
)

// Op is one replicated state change.
type Op struct {
	Type   string `json:"type"`
	Stream string `json:"stream,omitempty"`

	// OpPublish fields. Partitions is the stream's total partition count so
	// replay creates the stream identically on every node.
	Partitions int    `json:"partitions,omitempty"`
	Partition  int    `json:"partition"`
	Timestamp  int64  `json:"timestamp,omitempty"`
	Key        []byte `json:"key,omitempty"`
	Value      []byte `json:"value,omitempty"`

	// OpCommitOffset fields.
	Group      string `json:"group,omitempty"`
	NextOffset int64  `json:"next_offset,omitempty"`
}

// PublishResult is ApplyOp's result for OpPublish.
type PublishResult struct {
	Partition int
	Offset    int64
}

// ChoosePartition resolves the partition a record with this key lands on,
// creating the stream if needed. The replication leader calls it at propose
// time so the choice replays deterministically on every node.
func (b *Broker) ChoosePartition(name string, key []byte) (partition, total int, err error) {
	s, err := b.stream(name, true)
	if err != nil {
		return 0, 0, err
	}
	return s.route(key), len(s.logs), nil
}

// AuthorizeCommit runs the consumer-group fence for an offset commit
// without committing. The leader calls it before proposing, since group
// membership is leader-local state.
func (b *Broker) AuthorizeCommit(group, name string, partition int, memberID string) error {
	if err := b.validateGroupTarget(name, partition); err != nil {
		return err
	}
	if memberID == "" {
		return nil
	}
	return b.groups.Authorize(group, name, memberID, partition)
}

// ApplyOp applies one committed replicated operation to derived state.
func (b *Broker) ApplyOp(raw []byte) (any, error) {
	var op Op
	if err := json.Unmarshal(raw, &op); err != nil {
		return nil, fmt.Errorf("broker: decode replicated op: %w", err)
	}
	switch op.Type {
	case OpPublish:
		s, err := b.streamWithCount(op.Stream, op.Partitions)
		if err != nil {
			return nil, err
		}
		if op.Partition < 0 || op.Partition >= len(s.logs) {
			return nil, ErrNoSuchPart
		}
		off, err := s.logs[op.Partition].AppendRecord(op.Timestamp, op.Key, op.Value)
		if err != nil {
			return nil, err
		}
		return PublishResult{Partition: op.Partition, Offset: off}, nil
	case OpCommitOffset:
		// Validated on the leader before proposing; apply unconditionally so
		// every node stores the commit even if it has not seen the stream yet.
		return nil, b.offsets.Commit(op.Group, op.Stream, op.Partition, op.NextOffset)
	default:
		return nil, fmt.Errorf("broker: unknown replicated op %q", op.Type)
	}
}
