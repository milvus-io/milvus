package transformlog

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type TransformLog interface {
	Append(message.ImmutableMessage, AppendOption) AppendResult
	Flush(context.Context, FlushOption) (FlushResult, error)
	Read(context.Context, transformlogapi.ReadOption) transformlogapi.Scanner
	Truncate(TruncateOption) TruncateResult

	Recover(context.Context, *streamingpb.VChannelTransformLogMeta) (RecoverResult, error)
	SnapshotMeta() *streamingpb.VChannelTransformLogMeta
	DataCheckpointTimeTick() uint64
	DataBarrierTimeTick() uint64
	HasDirty() bool
	ConsumeDirtyAndGetSnapshot() *streamingpb.VChannelTransformLogMeta
	MarkSnapshotPersisted(*streamingpb.VChannelTransformLogMeta)
	HasPendingWork() bool
}

type Config struct {
	VChannel string
	MaxRows  uint64
	Meta     *streamingpb.VChannelTransformLogMeta
	Store    Store
}

type AppendResult struct {
	Appended     bool
	ShouldFlush  bool
	DataTimeTick uint64
}

type AppendOption struct {
	DeleteFilter func(partitionID int64, timeTick uint64) bool
}

func (o AppendOption) acceptDelete(partitionID int64, timeTick uint64) bool {
	if o.DeleteFilter == nil {
		return true
	}
	return o.DeleteFilter(partitionID, timeTick)
}

type FlushOption struct {
	TargetTimeTick uint64
}

type FlushResult struct {
	Started            bool
	DurableTimeTick    uint64
	NextTargetTimeTick uint64
}

type TruncateOption struct {
	TimeTick uint64
}

type TruncateResult struct {
	Changed bool
}

type RecoverResult struct {
	Recovered          bool
	CheckpointTimeTick uint64
}

type transformLog struct {
	flushMu               sync.Mutex
	mu                    sync.Mutex
	vchannel              string
	meta                  *streamingpb.VChannelTransformLogMeta
	persistedDataTimeTick uint64
	dirty                 bool
	pendingDirtySnapshot  *streamingpb.VChannelTransformLogMeta
	buffer                buffer
	store                 Store

	retainedChunks []*streamingpb.TransformLogChunk

	subscriberMu sync.Mutex
	subscribers  map[*scanner]struct{}
}

func New(config Config) TransformLog {
	meta := cloneMetaOrNew(config.Meta)
	return &transformLog{
		vchannel:              config.VChannel,
		meta:                  meta,
		persistedDataTimeTick: meta.GetCheckpointTimeTick(),
		buffer:                newBuffer(config.MaxRows),
		store:                 config.Store,
		subscribers:           make(map[*scanner]struct{}),
	}
}

func (t *transformLog) Append(msg message.ImmutableMessage, opt AppendOption) AppendResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	if msg.TimeTick() <= t.meta.GetCheckpointTimeTick() || msg.TimeTick() <= t.buffer.DataTimeTick() {
		return AppendResult{DataTimeTick: t.buffer.DataTimeTick()}
	}
	if !t.buffer.Append(msg, opt) {
		return AppendResult{DataTimeTick: t.buffer.DataTimeTick()}
	}
	return AppendResult{
		Appended:     true,
		ShouldFlush:  t.buffer.ShouldFlush(),
		DataTimeTick: t.buffer.DataTimeTick(),
	}
}

func (t *transformLog) Flush(ctx context.Context, opt FlushOption) (FlushResult, error) {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()
	var work flushWork
	t.mu.Lock()
	if !t.buffer.StartFlush(opt.TargetTimeTick) && (!t.buffer.IsFlushing() || t.buffer.FlushTargetTimeTick() == 0) {
		t.mu.Unlock()
		return FlushResult{}, nil
	}
	targetTimeTick := t.buffer.FlushTargetTimeTick()
	if targetTimeTick > t.meta.GetCheckpointTimeTick() {
		work = t.prepareFlushLocked(targetTimeTick)
	} else {
		work = flushWork{TargetTimeTick: targetTimeTick}
	}
	t.mu.Unlock()

	if work.Chunk != nil {
		if t.store == nil {
			return FlushResult{}, errors.New("transform log store is nil")
		}
		if err := t.store.WriteTransformLogChunk(ctx, t.vchannel, work.Chunk); err != nil {
			return FlushResult{}, err
		}
	}

	t.mu.Lock()
	result, publishedEntries := t.commitFlushLocked(work)
	result.Started = true
	t.mu.Unlock()
	if len(publishedEntries) > 0 {
		t.publish(publishedEntries)
	}
	return result, nil
}

func (t *transformLog) Read(ctx context.Context, opt transformlogapi.ReadOption) transformlogapi.Scanner {
	t.mu.Lock()
	if opt.StartAfterTimeTick < t.meta.GetTruncateTimeTick() {
		t.mu.Unlock()
		return transformlogapi.NewErrorScanner(opt.Name, errors.Wrap(transformlogapi.ErrStartPointTruncated, "start point is truncated"))
	}
	chunks := snapshotChunks(t.retainedChunks)
	scanner := newScanner(opt.Name, opt.StartAfterTimeTick, liveAfterTimeTick(opt.StartAfterTimeTick, chunks))
	t.registerScanner(scanner)
	t.mu.Unlock()
	go scanner.send(ctx, t, chunks)
	return scanner
}

func (t *transformLog) Truncate(opt TruncateOption) TruncateResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	if opt.TimeTick <= t.meta.GetTruncateTimeTick() {
		return TruncateResult{}
	}
	t.meta.TruncateTimeTick = opt.TimeTick
	t.dirty = true
	for len(t.retainedChunks) > 0 {
		chunk := t.retainedChunks[0]
		entries := chunk.GetEntries()
		if len(entries) == 0 || entries[len(entries)-1].GetTimeTick() > opt.TimeTick {
			break
		}
		t.retainedChunks = t.retainedChunks[1:]
		if chunk.GetChunkId() >= t.meta.GetFirstChunkId() {
			t.meta.FirstChunkId = chunk.GetChunkId() + 1
		}
	}
	return TruncateResult{Changed: true}
}

func (t *transformLog) Recover(ctx context.Context, meta *streamingpb.VChannelTransformLogMeta) (RecoverResult, error) {
	t.mu.Lock()
	if meta != nil {
		t.meta = cloneMetaOrNew(meta)
		t.persistedDataTimeTick = t.meta.GetCheckpointTimeTick()
	}
	recoverMeta := cloneMeta(t.meta)
	t.retainedChunks = nil
	t.mu.Unlock()
	if t.store == nil || recoverMeta == nil || recoverMeta.GetFirstChunkId() == recoverMeta.GetNextChunkId() {
		return RecoverResult{}, nil
	}
	chunks := make([]*streamingpb.TransformLogChunk, 0, recoverMeta.GetNextChunkId()-recoverMeta.GetFirstChunkId())
	var lastTimeTick uint64
	for chunkID := recoverMeta.GetFirstChunkId(); chunkID < recoverMeta.GetNextChunkId(); chunkID++ {
		chunk, err := t.store.ReadTransformLogChunk(ctx, t.vchannel, chunkID)
		if err != nil {
			return RecoverResult{}, err
		}
		if err := validateChunk(chunk, chunkID, lastTimeTick); err != nil {
			return RecoverResult{}, err
		}
		lastTimeTick = chunk.GetEntries()[len(chunk.GetEntries())-1].GetTimeTick()
		chunks = append(chunks, chunk)
	}
	t.mu.Lock()
	t.retainedChunks = chunks
	t.mu.Unlock()
	return RecoverResult{Recovered: true, CheckpointTimeTick: recoverMeta.GetCheckpointTimeTick()}, nil
}

func (t *transformLog) SnapshotMeta() *streamingpb.VChannelTransformLogMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneMeta(t.meta)
}

func (t *transformLog) DataCheckpointTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.meta.GetCheckpointTimeTick()
}

func (t *transformLog) DataBarrierTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.persistedDataTimeTick
}

func (t *transformLog) HasDirty() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dirty
}

func (t *transformLog) ConsumeDirtyAndGetSnapshot() *streamingpb.VChannelTransformLogMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pendingDirtySnapshot != nil {
		return cloneMeta(t.pendingDirtySnapshot)
	}
	if !t.dirty {
		return nil
	}
	t.pendingDirtySnapshot = cloneMeta(t.meta)
	return cloneMeta(t.pendingDirtySnapshot)
}

func (t *transformLog) MarkSnapshotPersisted(snapshot *streamingpb.VChannelTransformLogMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if snapshot.GetCheckpointTimeTick() > t.persistedDataTimeTick {
		t.persistedDataTimeTick = snapshot.GetCheckpointTimeTick()
	}
	if t.pendingDirtySnapshot != nil && proto.Equal(t.pendingDirtySnapshot, snapshot) {
		t.pendingDirtySnapshot = nil
	}
	t.dirty = !proto.Equal(t.meta, snapshot)
}

func (t *transformLog) HasPendingWork() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.buffer.IsEmpty() ||
		t.buffer.IsFlushing() ||
		t.buffer.FlushTargetTimeTick() > t.persistedDataTimeTick
}

type flushWork struct {
	TargetTimeTick uint64
	Chunk          *streamingpb.TransformLogChunk
}

func (t *transformLog) prepareFlushLocked(targetTimeTick uint64) flushWork {
	return flushWork{
		TargetTimeTick: targetTimeTick,
		Chunk:          t.buffer.FlushChunk(t.meta.GetNextChunkId(), targetTimeTick),
	}
}

func (t *transformLog) commitFlushLocked(work flushWork) (FlushResult, []*streamingpb.TransformLogEntry) {
	var result FlushResult
	var publishedEntries []*streamingpb.TransformLogEntry
	if work.Chunk != nil {
		toTimeTick := work.Chunk.GetEntries()[len(work.Chunk.GetEntries())-1].GetTimeTick()
		t.buffer.DiscardThrough(toTimeTick)
		t.retainedChunks = append(t.retainedChunks, work.Chunk)
		publishedEntries = work.Chunk.GetEntries()
		if toTimeTick > t.meta.GetCheckpointTimeTick() {
			t.meta.CheckpointTimeTick = toTimeTick
		}
		if work.Chunk.GetChunkId() >= t.meta.GetNextChunkId() {
			t.meta.NextChunkId = work.Chunk.GetChunkId() + 1
		}
		t.dirty = true
		result.DurableTimeTick = toTimeTick
		if !t.buffer.HasFlushWorkThrough(work.TargetTimeTick) {
			result.DurableTimeTick = work.TargetTimeTick
		}
	} else if work.TargetTimeTick > t.meta.GetCheckpointTimeTick() {
		t.meta.CheckpointTimeTick = work.TargetTimeTick
		t.dirty = true
		result.DurableTimeTick = work.TargetTimeTick
	}

	nextDurableTimeTick := maxTimeTick(t.meta.GetCheckpointTimeTick(), result.DurableTimeTick)
	currentFlushTarget := t.buffer.FlushTargetTimeTick()
	t.buffer.FinishFlush()
	switch {
	case currentFlushTarget > nextDurableTimeTick:
		result.NextTargetTimeTick = currentFlushTarget
	case t.buffer.HasFlushWorkThrough(currentFlushTarget):
		result.NextTargetTimeTick = currentFlushTarget
	case t.buffer.ShouldFlush():
		result.NextTargetTimeTick = t.buffer.DataTimeTick()
	}
	return result, publishedEntries
}

func (t *transformLog) publish(entries []*streamingpb.TransformLogEntry) {
	t.subscriberMu.Lock()
	subscribers := make([]*scanner, 0, len(t.subscribers))
	for subscriber := range t.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	t.subscriberMu.Unlock()
	for _, entry := range entries {
		for _, subscriber := range subscribers {
			subscriber.publishEntry(entry)
		}
	}
}

func (t *transformLog) registerScanner(scanner *scanner) {
	t.subscriberMu.Lock()
	defer t.subscriberMu.Unlock()
	t.subscribers[scanner] = struct{}{}
}

func (t *transformLog) unregisterScanner(scanner *scanner) {
	t.subscriberMu.Lock()
	defer t.subscriberMu.Unlock()
	delete(t.subscribers, scanner)
}

func validateChunk(chunk *streamingpb.TransformLogChunk, expectedChunkID uint64, previousTimeTick uint64) error {
	if chunk == nil {
		return errors.Errorf("transform log chunk %d is nil", expectedChunkID)
	}
	if chunk.GetChunkId() != expectedChunkID {
		return errors.Errorf("transform log chunk id mismatch, expected %d, got %d", expectedChunkID, chunk.GetChunkId())
	}
	if len(chunk.GetEntries()) == 0 {
		return errors.Errorf("transform log chunk %d is empty", expectedChunkID)
	}
	for _, entry := range chunk.GetEntries() {
		if entry.GetTimeTick() <= previousTimeTick {
			return errors.Errorf("transform log chunk %d entries are not ordered", expectedChunkID)
		}
		previousTimeTick = entry.GetTimeTick()
	}
	return nil
}

func cloneMetaOrNew(meta *streamingpb.VChannelTransformLogMeta) *streamingpb.VChannelTransformLogMeta {
	if meta == nil {
		return &streamingpb.VChannelTransformLogMeta{}
	}
	return cloneMeta(meta)
}

func cloneMeta(meta *streamingpb.VChannelTransformLogMeta) *streamingpb.VChannelTransformLogMeta {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*streamingpb.VChannelTransformLogMeta)
}

func snapshotChunks(chunks []*streamingpb.TransformLogChunk) []*streamingpb.TransformLogChunk {
	if len(chunks) == 0 {
		return nil
	}
	// Retained chunks are immutable after they are appended or recovered. A shallow
	// slice snapshot is enough to isolate readers from truncate moving retainedChunks.
	snapshot := make([]*streamingpb.TransformLogChunk, len(chunks))
	copy(snapshot, chunks)
	return snapshot
}

func liveAfterTimeTick(startAfter uint64, chunks []*streamingpb.TransformLogChunk) uint64 {
	liveAfter := startAfter
	for _, chunk := range chunks {
		entries := chunk.GetEntries()
		if len(entries) == 0 {
			continue
		}
		if timeTick := entries[len(entries)-1].GetTimeTick(); timeTick > liveAfter {
			liveAfter = timeTick
		}
	}
	return liveAfter
}

func maxTimeTick(left uint64, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
