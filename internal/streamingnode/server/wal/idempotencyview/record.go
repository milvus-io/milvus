// Package idempotencyview is the seam between the WAL summary and the
// idempotency interceptor.
//
// It holds only the two shapes they exchange and the DDL predicate that
// invalidates them. It imports nothing from the WAL server packages, so both
// the summary that produces these records and the interceptor that consumes
// them can depend on it without a cycle: the interceptor already reaches the
// summary the long way round, through interceptors -> recovery.
package idempotencyview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Record is one committed write fact: where and when it landed in the
// WAL, plus what the idempotency view remembers about it.
//
// It is a plain Go struct, not a generated message, because it is not the stored
// shape. A chunk stores a write split across sections -- identity and primary
// keys in one, the client key and row offsets in another -- so that a consumer
// can read only the part it needs. In memory there is no such consumer: whoever
// holds a record holds all of it. Keeping one struct here and splitting only at
// the codec means the split exists exactly where it pays for itself.
type Record struct {
	SourceMessageID *commonpb.MessageID
	SourceTimeTick  uint64

	// LastConfirmedMessageID is the position stamped on the original message. It
	// is carried so a duplicate append can answer with exactly what the first
	// append answered: the append response always has this field, and the
	// producer client rejects a response without it.
	LastConfirmedMessageID *commonpb.MessageID

	// IdempotencyKey is empty for a write no view remembers. Such a record is
	// never staged for a chunk: it materializes nothing for any consumer, and the
	// WAL consume checkpoint -- not a chunk -- is what records how far the vchannel
	// has advanced.
	IdempotencyKey string

	// InsertResult is what a duplicate append replays back to the client. Its two
	// halves are stored in different sections: RowOffsets with the key, Ids with
	// the write. They are rejoined on read.
	InsertResult *messagespb.IdempotentInsertResult
}

// Size estimates the record's memory footprint for the byte budgets that bound
// the staging buffer and the dedup window. The primary keys dominate it by far,
// so the estimate is theirs plus a small fixed remainder.
func (r *Record) Size() int {
	if r == nil {
		return 0
	}
	size := len(r.IdempotencyKey) + 8
	if r.SourceMessageID != nil {
		size += proto.Size(r.SourceMessageID)
	}
	if r.LastConfirmedMessageID != nil {
		size += proto.Size(r.LastConfirmedMessageID)
	}
	if r.InsertResult != nil {
		size += proto.Size(r.InsertResult)
	}
	return size
}

// Snapshot is what recovery hands the idempotency interceptor once it has
// replayed the chunks: one vchannel's records.
//
// It never leaves the process: built once at WAL open, consumed once, never
// stored or sent. That is why it is a plain Go struct rather than a message in
// streaming.proto -- a proto here would advertise a wire contract that does not
// exist.
type Snapshot struct {
	PChannel string
	VChannel string
	Records  []*Record
}

// InvalidatesIdempotencyWindow reports whether a DDL makes every retained entry
// of a vchannel meaningless, so the window must be dropped rather than kept.
//
// The danger is not a stale entry lingering; it is a stale entry being SERVED.
// An auto-derived key is a hash of the destination and the payload, with no
// collection generation and no partition id in it, so re-inserting the same rows
// after the data underneath them is gone hashes to the same key and is answered
// as a duplicate: nothing reaches the WAL and the client is told the write
// succeeded, with the original primary keys, into an empty collection.
//
//   - DropCollection reclaims the vchannel outright.
//   - TruncateCollection is IN PLACE: the collection id and the vchannel names
//     survive it (see rootcoord's truncate callback), so the WAL and the window
//     both survive with it, holding keys for rows that no longer exist.
//   - DropPartition removes rows a later partition of the same NAME would be
//     re-inserted into, and the auto key cannot tell the two apart. Dropping the
//     whole vchannel's window is broader than the partition, and deliberately so:
//     losing a dedup opportunity degrades to the behavior without this feature,
//     whereas serving a false duplicate silently discards a write.
func InvalidatesIdempotencyWindow(t message.MessageType) bool {
	switch t {
	case message.MessageTypeDropCollection,
		message.MessageTypeTruncateCollection,
		message.MessageTypeDropPartition:
		return true
	default:
		return false
	}
}
