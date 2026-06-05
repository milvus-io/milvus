package growing

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const defaultTransformLogBufferMaxRows = 1024

type transformLogBuffer struct {
	entries             []deleteEntry
	fromTimeTick        uint64
	toTimeTick          uint64
	rows                uint64
	maxRows             uint64
	flushing            bool
	flushTargetTimeTick uint64
}

func newTransformLogBuffer(maxRows uint64) transformLogBuffer {
	if maxRows == 0 {
		maxRows = defaultTransformLogBufferMaxRows
	}
	return transformLogBuffer{maxRows: maxRows}
}

func (b *transformLogBuffer) AppendDelete(msg message.ImmutableDeleteMessageV1) {
	timetick := msg.TimeTick()
	if len(b.entries) == 0 {
		b.fromTimeTick = timetick
	}
	body := msg.MustBody()
	rows := deleteEntryRows(body)
	b.toTimeTick = timetick
	b.rows += rows
	b.entries = append(b.entries, deleteEntry{
		timeTick: timetick,
		rows:     rows,
		request:  cloneDeleteRequest(body),
	})
}

func (b *transformLogBuffer) ShouldFlush() bool {
	return len(b.entries) > 0 && b.rows >= b.maxRows
}

func (b *transformLogBuffer) StartFlush(timetick uint64) bool {
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

func (b *transformLogBuffer) FinishFlush() {
	b.flushing = false
	b.flushTargetTimeTick = 0
}

func (b *transformLogBuffer) IsFlushing() bool {
	return b.flushing
}

func (b *transformLogBuffer) DataTimeTick() uint64 {
	return b.toTimeTick
}

func (b *transformLogBuffer) FlushTargetTimeTick() uint64 {
	return b.flushTargetTimeTick
}

func (b *transformLogBuffer) IsEmpty() bool {
	return len(b.entries) == 0
}

func (b *transformLogBuffer) FlushPack(meta *streamingpb.VChannelMeta, schema *schemapb.CollectionSchema, timetick uint64) *deleteFlushPack {
	entries := b.flushEntriesThrough(timetick)
	if len(entries) == 0 {
		return nil
	}
	return &deleteFlushPack{
		VChannel:     meta.GetVchannel(),
		CollectionID: meta.GetCollectionInfo().GetCollectionId(),
		PartitionID:  common.AllPartitionsID,
		FromTimeTick: entries[0].timeTick,
		ToTimeTick:   entries[len(entries)-1].timeTick,
		Schema:       schema,
		Deletes:      cloneDeleteEntries(entries),
		StartPosition: &msgpb.MsgPosition{
			ChannelName: meta.GetVchannel(),
			Timestamp:   entries[0].timeTick,
		},
		Checkpoint: &msgpb.MsgPosition{
			ChannelName: meta.GetVchannel(),
			Timestamp:   entries[len(entries)-1].timeTick,
		},
	}
}

func (b *transformLogBuffer) HasFlushWorkThrough(timetick uint64) bool {
	return len(b.entriesThrough(timetick)) > 0
}

func (b *transformLogBuffer) flushEntriesThrough(timetick uint64) []deleteEntry {
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

func (b *transformLogBuffer) entriesThrough(timetick uint64) []deleteEntry {
	for idx, entry := range b.entries {
		if entry.timeTick > timetick {
			return b.entries[:idx]
		}
	}
	return b.entries
}

func (b *transformLogBuffer) DiscardThrough(timetick uint64) {
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

func (b *transformLogBuffer) rebuildStats() {
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

func deleteEntryRows(request *msgpb.DeleteRequest) uint64 {
	if rows := len(request.GetTimestamps()); rows > 0 {
		return uint64(rows)
	}
	return 1
}

func cloneDeleteEntries(entries []deleteEntry) []deleteEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]deleteEntry, 0, len(entries))
	for _, entry := range entries {
		cloned = append(cloned, deleteEntry{
			timeTick: entry.timeTick,
			rows:     entry.rows,
			request:  proto.Clone(entry.request).(*msgpb.DeleteRequest),
		})
	}
	return cloned
}
