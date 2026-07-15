package transformlog

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

const defaultBufferMaxRows = 1024

type buffer struct {
	entries             []transformEntry
	fromTimeTick        uint64
	toTimeTick          uint64
	rows                uint64
	maxRows             uint64
	flushing            bool
	flushTargetTimeTick uint64
}

func newBuffer(maxRows uint64) buffer {
	if maxRows == 0 {
		maxRows = defaultBufferMaxRows
	}
	return buffer{maxRows: maxRows}
}

func (b *buffer) append(msg message.ImmutableMessage, opt appendOption) bool {
	entry := transformEntryFromMessage(msg, opt)
	if entry == nil {
		return false
	}
	b.AppendEntry(entry)
	return true
}

func (b *buffer) AppendEntry(entry *transformEntry) {
	if entry == nil {
		return
	}
	timetick := entry.timeTick
	if len(b.entries) == 0 {
		b.fromTimeTick = timetick
	}
	b.toTimeTick = timetick
	b.rows += entry.rows
	b.entries = append(b.entries, *entry)
}

func (b *buffer) ShouldFlush() bool {
	return len(b.entries) > 0 && b.rows >= b.maxRows
}

func (b *buffer) StartFlush(timetick uint64) bool {
	if timetick == 0 {
		timetick = b.toTimeTick
	}
	if timetick == 0 {
		return false
	}
	if timetick > b.flushTargetTimeTick {
		b.flushTargetTimeTick = timetick
	}
	if b.flushing {
		return false
	}
	b.flushing = true
	return true
}

func (b *buffer) FinishFlush() {
	b.flushing = false
	b.flushTargetTimeTick = 0
}

func (b *buffer) IsFlushing() bool {
	return b.flushing
}

func (b *buffer) DataTimeTick() uint64 {
	return b.toTimeTick
}

func (b *buffer) FlushTargetTimeTick() uint64 {
	return b.flushTargetTimeTick
}

func (b *buffer) IsEmpty() bool {
	return len(b.entries) == 0
}

func (b *buffer) FlushChunk(chunkID uint64, timetick uint64) *streamingpb.TransformLogChunk {
	entries := b.flushEntriesThrough(timetick)
	if len(entries) == 0 {
		return nil
	}
	chunkEntries := make([]*streamingpb.TransformLogEntry, 0, len(entries))
	for _, entry := range entries {
		chunkEntries = append(chunkEntries, cloneTransformLogEntry(entry.entry))
	}
	return &streamingpb.TransformLogChunk{
		ChunkId: chunkID,
		Entries: chunkEntries,
	}
}

func (b *buffer) HasFlushWorkThrough(timetick uint64) bool {
	return len(b.entriesThrough(timetick)) > 0
}

func (b *buffer) flushEntriesThrough(timetick uint64) []transformEntry {
	entries := b.entriesThrough(timetick)
	if len(entries) == 0 {
		return nil
	}
	var rows uint64
	for idx, entry := range entries {
		if idx > 0 && rows+entry.rows > b.maxRows {
			return entries[:idx]
		}
		rows += entry.rows
		if rows >= b.maxRows {
			return entries[:idx+1]
		}
	}
	return entries
}

func (b *buffer) entriesThrough(timetick uint64) []transformEntry {
	for idx, entry := range b.entries {
		if entry.timeTick > timetick {
			return b.entries[:idx]
		}
	}
	return b.entries
}

func (b *buffer) DiscardThrough(timetick uint64) {
	kept := b.entries[:0]
	for _, entry := range b.entries {
		if entry.timeTick <= timetick {
			continue
		}
		kept = append(kept, entry)
	}
	b.entries = kept
	b.rebuildStats()
}

func (b *buffer) rebuildStats() {
	b.fromTimeTick = 0
	b.toTimeTick = 0
	b.rows = 0
	if len(b.entries) == 0 {
		return
	}
	b.fromTimeTick = b.entries[0].timeTick
	for _, entry := range b.entries {
		b.toTimeTick = entry.timeTick
		b.rows += entry.rows
	}
}

type transformEntry struct {
	timeTick uint64
	rows     uint64
	entry    *streamingpb.TransformLogEntry
}

func deleteEntryRows(request *msgpb.DeleteRequest) uint64 {
	return uint64(primaryKeyCount(request.GetPrimaryKeys()))
}

func transformEntryFromMessage(msg message.ImmutableMessage, opt appendOption) *transformEntry {
	switch messageutil.ClassifyTransformLogMessage(msg) {
	case messageutil.TransformLogKindDelete:
		return deleteTransformEntryFromMessage(msg, opt)
	case messageutil.TransformLogKindBarrier:
		return transformBarrierEntry(msg.TimeTick())
	default:
		return nil
	}
}

func deleteTransformEntryFromMessage(msg message.ImmutableMessage, opt appendOption) *transformEntry {
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		deleted := message.MustAsImmutableDeleteMessageV1(msg)
		return transformEntryFromDeletes(msg.TimeTick(), []message.ImmutableDeleteMessageV1{deleted}, opt)
	case message.MessageTypeTxn:
		txn := message.AsImmutableTxnMessage(msg)
		deletes := make([]message.ImmutableDeleteMessageV1, 0)
		_ = txn.RangeOver(func(im message.ImmutableMessage) error {
			if im.MessageType() == message.MessageTypeDelete {
				deletes = append(deletes, message.MustAsImmutableDeleteMessageV1(im))
			}
			return nil
		})
		return transformEntryFromDeletes(msg.TimeTick(), deletes, opt)
	default:
		return nil
	}
}

func transformEntryFromDeletes(timeTick uint64, deletes []message.ImmutableDeleteMessageV1, opt appendOption) *transformEntry {
	blocks := make([]*streamingpb.TransformDeleteBlock, 0, len(deletes))
	var rows uint64
	for _, deleted := range deletes {
		request := cloneDeleteRequest(deleted.MustBody())
		if request == nil {
			continue
		}
		if !opt.acceptDelete(request.GetPartitionID(), timeTick) {
			continue
		}
		rows += deleteEntryRows(request)
		blocks = append(blocks, &streamingpb.TransformDeleteBlock{
			PartitionId: request.GetPartitionID(),
			PrimaryKeys: request.GetPrimaryKeys(),
		})
	}
	if len(blocks) == 0 {
		return nil
	}
	return &transformEntry{
		timeTick: timeTick,
		rows:     rows,
		entry: &streamingpb.TransformLogEntry{
			TimeTick: timeTick,
			Entry: &streamingpb.TransformLogEntry_Delete{
				Delete: &streamingpb.TransformDeleteEntry{
					Blocks: blocks,
				},
			},
		},
	}
}

func transformBarrierEntry(timeTick uint64) *transformEntry {
	return &transformEntry{
		timeTick: timeTick,
		entry: &streamingpb.TransformLogEntry{
			TimeTick: timeTick,
			Entry: &streamingpb.TransformLogEntry_Barrier{
				Barrier: &streamingpb.TransformBarrierEntry{},
			},
		},
	}
}

func cloneTransformLogEntry(entry *streamingpb.TransformLogEntry) *streamingpb.TransformLogEntry {
	if entry == nil {
		return nil
	}
	return proto.Clone(entry).(*streamingpb.TransformLogEntry)
}

func cloneDeleteRequest(value *msgpb.DeleteRequest) *msgpb.DeleteRequest {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*msgpb.DeleteRequest)
}

func primaryKeyCount(ids *schemapb.IDs) int {
	if ids == nil {
		return 0
	}
	switch ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		return len(ids.GetIntId().GetData())
	case *schemapb.IDs_StrId:
		return len(ids.GetStrId().GetData())
	default:
		return 0
	}
}
