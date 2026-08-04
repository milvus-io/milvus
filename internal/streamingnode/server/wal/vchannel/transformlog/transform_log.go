package transformlog

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

type Config struct {
	VChannel            string
	MaxRows             uint64
	MaterializeMaxRows  uint64
	MaterializeMaxBytes uint64
	Meta                *streamingpb.VChannelTransformLogMeta
	Store               Store
	Materializer        Materializer
	Runtime             moduleapi.Runtime
}

type appendResult struct {
	Appended     bool
	ShouldFlush  bool
	DataTimeTick uint64
}

type appendOption struct {
	DeleteFilter func(partitionID int64, timeTick uint64) bool
}

func (o appendOption) acceptDelete(partitionID int64, timeTick uint64) bool {
	if o.DeleteFilter == nil {
		return true
	}
	return o.DeleteFilter(partitionID, timeTick)
}

type flushOption struct {
	TargetTimeTick uint64
}

type flushResult struct {
	Started            bool
	DurableTimeTick    uint64
	NextTargetTimeTick uint64
}

type materializeOption struct {
	TargetTimeTick uint64
}

type materializeResult struct {
	Started                 bool
	MaterializedTimeTick    uint64
	MaterializedRows        uint64
	MaterializedBytes       uint64
	HasMaterializedSegments bool
}

type truncateOption struct {
	TimeTick uint64
}

type truncateResult struct {
	Changed bool
}

type streamNotifier interface {
	notify(vchannel string)
}

type TransformLog struct {
	flushMu               sync.Mutex
	materializeMu         sync.Mutex
	mu                    sync.Mutex
	vchannel              string
	meta                  *streamingpb.VChannelTransformLogMeta
	persistedDataTimeTick uint64
	persistedMaterialized uint64
	syncUpTimeTick        uint64
	dirty                 bool
	pendingDirtySnapshot  *streamingpb.VChannelTransformLogMeta
	buffer                buffer
	store                 Store
	materializer          Materializer
	materializeMaxRows    uint64
	materializeMaxBytes   uint64
	runtime               moduleapi.Runtime
	metaAndData           bool
	flushTasks            []*transformFlushTask
	materializeTasks      []*transformMaterializeTask
	streamNotifier        streamNotifier

	chunks []*chunkDescriptor
}

func New(config Config) *TransformLog {
	meta := cloneMetaOrNew(config.Meta)
	return &TransformLog{
		vchannel:              config.VChannel,
		meta:                  meta,
		persistedDataTimeTick: meta.GetCheckpointTimeTick(),
		persistedMaterialized: meta.GetMaterializedTimeTick(),
		syncUpTimeTick:        meta.GetCheckpointTimeTick(),
		buffer:                newBuffer(config.MaxRows),
		store:                 config.Store,
		materializer:          config.Materializer,
		materializeMaxRows:    config.MaterializeMaxRows,
		materializeMaxBytes:   config.MaterializeMaxBytes,
		runtime:               config.Runtime,
		chunks:                newColdChunkDescriptorsFromMeta(meta),
	}
}

func (t *TransformLog) SwitchIntoMetaAndData() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metaAndData = true
}

func (t *TransformLog) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	if t == nil || msg == nil || !t.isMetaAndData() {
		return moduleapi.ObserveResult{}
	}
	kind := messageutil.ClassifyTransformLogMessage(msg)
	if kind == messageutil.TransformLogKindNone {
		return moduleapi.ObserveResult{}
	}
	var result appendResult
	switch kind {
	case messageutil.TransformLogKindDelete:
		if msg.TimeTick() <= t.dataCheckpointTimeTick() {
			return moduleapi.ObserveResult{}
		}
		result = t.append(msg, appendOption{})
	case messageutil.TransformLogKindBarrier:
		if msg.TimeTick() <= t.latestTimeTick() && !t.HasPendingWork() {
			return moduleapi.ObserveResult{}
		}
		result = t.syncUp(msg.TimeTick())
	}
	if !result.Appended && !t.HasPendingWork() {
		return moduleapi.ObserveResult{}
	}
	if result.Appended && result.ShouldFlush {
		t.submitFlushTask(result.DataTimeTick)
	} else if kind == messageutil.TransformLogKindBarrier && t.hasFlushWork() {
		t.submitFlushTask(result.DataTimeTick)
	}
	if t.shouldMaterializeByMessage(msg) {
		t.submitMaterializeTask(msg.TimeTick())
	}
	return moduleapi.ObserveResult{Data: t.dataBarrier()}
}

func (t *TransformLog) append(msg message.ImmutableMessage, opt appendOption) appendResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	if msg.TimeTick() <= t.meta.GetCheckpointTimeTick() || msg.TimeTick() <= t.buffer.DataTimeTick() {
		return appendResult{DataTimeTick: t.buffer.DataTimeTick()}
	}
	if !t.buffer.append(msg, opt) {
		return appendResult{DataTimeTick: t.buffer.DataTimeTick()}
	}
	t.notifyStreamLocked()
	return appendResult{
		Appended:     true,
		ShouldFlush:  t.buffer.ShouldFlush(),
		DataTimeTick: t.buffer.DataTimeTick(),
	}
}

func (t *TransformLog) syncUp(timeTick uint64) appendResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	if timeTick <= t.syncUpTimeTick {
		return appendResult{DataTimeTick: t.buffer.DataTimeTick()}
	}
	t.syncUpTimeTick = timeTick
	t.notifyStreamLocked()
	return appendResult{
		Appended:     true,
		ShouldFlush:  t.buffer.ShouldFlush(),
		DataTimeTick: t.buffer.DataTimeTick(),
	}
}

func (t *TransformLog) flush(ctx context.Context, opt flushOption) (flushResult, error) {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()
	var work flushWork
	t.mu.Lock()
	if !t.buffer.StartFlush(opt.TargetTimeTick) && (!t.buffer.IsFlushing() || t.buffer.FlushTargetTimeTick() == 0) {
		t.mu.Unlock()
		return flushResult{}, nil
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
			return flushResult{}, errors.New("transform log store is nil")
		}
		if err := t.store.WriteTransformLogChunk(ctx, t.vchannel, work.Chunk); err != nil {
			return flushResult{}, err
		}
	}

	t.mu.Lock()
	result := t.commitFlushLocked(work)
	result.Started = true
	t.mu.Unlock()
	return result, nil
}

func (t *TransformLog) materialize(ctx context.Context, opt materializeOption) (materializeResult, error) {
	t.materializeMu.Lock()
	defer t.materializeMu.Unlock()
	t.mu.Lock()
	targetTimeTick := opt.TargetTimeTick
	if targetTimeTick == 0 {
		targetTimeTick = t.meta.GetCheckpointTimeTick()
	}
	if targetTimeTick <= t.meta.GetMaterializedTimeTick() {
		t.mu.Unlock()
		return materializeResult{}, nil
	}
	latestTimeTick := t.latestTimeTickLocked()
	if targetTimeTick > latestTimeTick {
		targetTimeTick = latestTimeTick
	}
	if targetTimeTick <= t.meta.GetMaterializedTimeTick() {
		t.mu.Unlock()
		return materializeResult{}, nil
	}
	t.mu.Unlock()

	work, err := t.prepareMaterialize(ctx, targetTimeTick)
	if err != nil {
		return materializeResult{}, err
	}

	if len(work.Entries) > 0 {
		if t.materializer == nil {
			return materializeResult{}, errors.New("transform log materializer is nil")
		}
		if err := t.materializer.Materialize(ctx, MaterializeRequest{
			VChannel:       t.vchannel,
			TargetTimeTick: work.TargetTimeTick,
			Entries:        work.Entries,
			MaxRows:        t.materializeMaxRows,
			MaxBytes:       t.materializeMaxBytes,
		}); err != nil {
			return materializeResult{}, err
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	return t.commitMaterializeLocked(work), nil
}

func (t *TransformLog) truncate(opt truncateOption) truncateResult {
	var changed bool
	t.mu.Lock()
	if opt.TimeTick <= t.meta.GetTruncateTimeTick() {
		t.mu.Unlock()
		return truncateResult{}
	}
	t.meta.TruncateTimeTick = opt.TimeTick
	t.dirty = true
	changed = true
	for len(t.chunks) > 0 {
		chunk := t.chunks[0]
		if chunk.toTimeTick == 0 {
			t.mu.Unlock()
			if err := t.loadChunk(context.TODO(), chunk); err != nil {
				return truncateResult{Changed: changed}
			}
			t.mu.Lock()
			continue
		}
		if chunk.toTimeTick > opt.TimeTick {
			break
		}
		t.chunks = t.chunks[1:]
		if chunk.id >= t.meta.GetFirstChunkId() {
			t.meta.FirstChunkId = chunk.id + 1
		}
	}
	t.mu.Unlock()
	return truncateResult{Changed: changed}
}

func (t *TransformLog) SnapshotMeta() *streamingpb.VChannelTransformLogMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneMeta(t.meta)
}

func (t *TransformLog) LatestTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.latestTimeTickLocked()
}

func (t *TransformLog) latestTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.latestTimeTickLocked()
}

func (t *TransformLog) dataCheckpointTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.meta.GetCheckpointTimeTick()
}

func (t *TransformLog) DataBarrierTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.persistedDataTimeTick < t.meta.GetCheckpointTimeTick() {
		return t.persistedDataTimeTick
	}
	if t.buffer.HasFlushWorkThrough(t.syncUpTimeTick) {
		return t.persistedDataTimeTick
	}
	return maxTimeTick(t.persistedDataTimeTick, t.syncUpTimeTick)
}

func (t *TransformLog) MaterializedBarrierTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.persistedMaterialized
}

func (t *TransformLog) HasDirty() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dirty
}

func (t *TransformLog) ConsumeDirtyAndGetSnapshot() *streamingpb.VChannelTransformLogMeta {
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

func (t *TransformLog) MarkSnapshotPersisted(snapshot *streamingpb.VChannelTransformLogMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if snapshot.GetCheckpointTimeTick() > t.persistedDataTimeTick {
		t.persistedDataTimeTick = snapshot.GetCheckpointTimeTick()
	}
	if snapshot.GetMaterializedTimeTick() > t.persistedMaterialized {
		t.persistedMaterialized = snapshot.GetMaterializedTimeTick()
	}
	if t.pendingDirtySnapshot != nil && proto.Equal(t.pendingDirtySnapshot, snapshot) {
		t.pendingDirtySnapshot = nil
	}
	t.dirty = !proto.Equal(t.meta, snapshot)
}

func (t *TransformLog) HasPendingWork() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.buffer.IsEmpty() ||
		t.buffer.IsFlushing() ||
		t.buffer.FlushTargetTimeTick() > t.persistedDataTimeTick
}

func (t *TransformLog) hasFlushWork() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.buffer.IsEmpty() || t.buffer.IsFlushing()
}

func (t *TransformLog) shouldMaterialize(ctx context.Context) bool {
	t.mu.Lock()
	targetTimeTick := t.meta.GetCheckpointTimeTick()
	t.mu.Unlock()
	rows, bytes, err := t.pendingMaterializeStats(ctx, targetTimeTick)
	if err != nil {
		return false
	}
	maxRows := t.materializeMaxRows
	if maxRows == 0 {
		maxRows = defaultMaterializeMaxRows
	}
	maxBytes := t.materializeMaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaterializeMaxBytes
	}
	return rows >= maxRows || bytes >= maxBytes
}

func (t *TransformLog) isMetaAndData() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.metaAndData
}

func (t *TransformLog) shouldMaterializeByMessage(msg message.ImmutableMessage) bool {
	switch msg.MessageType() {
	case message.MessageTypeDropCollection,
		message.MessageTypeManualFlush,
		message.MessageTypeFlushAll:
		return true
	default:
		return false
	}
}

func (t *TransformLog) dataBarrier() walcheckpoint.Barrier {
	return walcheckpoint.BarrierFunc(t.DataBarrierTimeTick)
}

type flushWork struct {
	TargetTimeTick uint64
	Chunk          *streamingpb.TransformLogChunk
}

func (t *TransformLog) prepareFlushLocked(targetTimeTick uint64) flushWork {
	return flushWork{
		TargetTimeTick: targetTimeTick,
		Chunk:          t.buffer.FlushChunk(t.meta.GetNextChunkId(), targetTimeTick),
	}
}

func (t *TransformLog) commitFlushLocked(work flushWork) flushResult {
	var result flushResult
	if work.Chunk != nil {
		toTimeTick := work.Chunk.GetEntries()[len(work.Chunk.GetEntries())-1].GetTimeTick()
		t.buffer.DiscardThrough(toTimeTick)
		t.chunks = append(t.chunks, newLoadedChunkDescriptor(work.Chunk))
		if work.Chunk.GetChunkId() >= t.meta.GetNextChunkId() {
			t.meta.NextChunkId = work.Chunk.GetChunkId() + 1
		}
		t.dirty = true
		result.DurableTimeTick = toTimeTick
		if result.DurableTimeTick > t.meta.GetCheckpointTimeTick() {
			t.meta.CheckpointTimeTick = result.DurableTimeTick
		}
	}

	currentFlushTarget := t.buffer.FlushTargetTimeTick()
	hasMoreWorkThroughTarget := t.buffer.HasFlushWorkThrough(currentFlushTarget)
	t.buffer.FinishFlush()
	switch {
	case hasMoreWorkThroughTarget:
		result.NextTargetTimeTick = currentFlushTarget
	case t.buffer.ShouldFlush():
		result.NextTargetTimeTick = t.buffer.DataTimeTick()
	}
	return result
}

type materializeWork struct {
	TargetTimeTick uint64
	Entries        []*streamingpb.TransformLogEntry
	Rows           uint64
	Bytes          uint64
}

func (t *TransformLog) prepareMaterialize(ctx context.Context, targetTimeTick uint64) (materializeWork, error) {
	work := materializeWork{TargetTimeTick: targetTimeTick}
	t.mu.Lock()
	cursor := t.meta.GetMaterializedTimeTick()
	t.mu.Unlock()
	for {
		entry, ok, err := t.nextEntryAfter(ctx, cursor)
		if err != nil {
			return materializeWork{}, err
		}
		if !ok || entry.GetTimeTick() > targetTimeTick {
			return work, nil
		}
		cursor = entry.GetTimeTick()
		if !isTransformDeleteEntry(entry) {
			continue
		}
		work.Entries = append(work.Entries, entry)
		work.Rows += transformLogEntryRows(entry)
		work.Bytes += uint64(proto.Size(entry))
	}
}

func (t *TransformLog) pendingMaterializeStats(ctx context.Context, targetTimeTick uint64) (uint64, uint64, error) {
	t.mu.Lock()
	cursor := t.meta.GetMaterializedTimeTick()
	t.mu.Unlock()
	var rows uint64
	var bytes uint64
	for {
		entry, ok, err := t.nextEntryAfter(ctx, cursor)
		if err != nil {
			return 0, 0, err
		}
		if !ok || entry.GetTimeTick() > targetTimeTick {
			return rows, bytes, nil
		}
		cursor = entry.GetTimeTick()
		if !isTransformDeleteEntry(entry) {
			continue
		}
		rows += transformLogEntryRows(entry)
		bytes += uint64(proto.Size(entry))
	}
}

func (t *TransformLog) commitMaterializeLocked(work materializeWork) materializeResult {
	if work.TargetTimeTick <= t.meta.GetMaterializedTimeTick() {
		return materializeResult{}
	}
	t.meta.MaterializedTimeTick = work.TargetTimeTick
	t.dirty = true
	return materializeResult{
		Started:                 true,
		MaterializedTimeTick:    work.TargetTimeTick,
		MaterializedRows:        work.Rows,
		MaterializedBytes:       work.Bytes,
		HasMaterializedSegments: len(work.Entries) > 0,
	}
}

func (t *TransformLog) nextEntryAfter(ctx context.Context, after uint64) (*streamingpb.TransformLogEntry, bool, error) {
	for {
		t.mu.Lock()
		entry, chunkToLoad, err := t.nextEntryAfterLocked(after)
		t.mu.Unlock()
		if err != nil {
			return nil, false, err
		}
		if entry != nil {
			return entry, true, nil
		}
		if chunkToLoad == nil {
			return nil, false, nil
		}
		if err := t.loadChunk(ctx, chunkToLoad); err != nil {
			return nil, false, err
		}
	}
}

func (t *TransformLog) nextEntryAfterWithFrontier(ctx context.Context, after uint64) (*streamingpb.TransformLogEntry, bool, uint64, error) {
	for {
		t.mu.Lock()
		entry, chunkToLoad, err := t.nextEntryAfterLocked(after)
		frontier := t.latestTimeTickLocked()
		t.mu.Unlock()
		if err != nil {
			return nil, false, frontier, err
		}
		if entry != nil {
			return entry, true, frontier, nil
		}
		if chunkToLoad == nil {
			return nil, false, frontier, nil
		}
		if err := t.loadChunk(ctx, chunkToLoad); err != nil {
			return nil, false, frontier, err
		}
	}
}

func (t *TransformLog) nextEntryAfterLocked(after uint64) (*streamingpb.TransformLogEntry, *chunkDescriptor, error) {
	if after < t.meta.GetTruncateTimeTick() {
		return nil, nil, errors.Wrap(wal.ErrTransformLogStartPointTruncated, "start point is truncated")
	}
	for _, chunk := range t.chunks {
		if chunk.toTimeTick > 0 && chunk.toTimeTick <= after {
			continue
		}
		if !chunk.loaded() {
			return nil, chunk, nil
		}
		for _, entry := range chunk.entries {
			if entry.GetTimeTick() > after {
				return cloneTransformLogEntry(entry), nil, nil
			}
		}
	}
	for _, entry := range t.buffer.entries {
		if entry.timeTick > after {
			return cloneTransformLogEntry(entry.entry), nil, nil
		}
	}
	return nil, nil, nil
}

func (t *TransformLog) loadChunk(ctx context.Context, chunk *chunkDescriptor) error {
	for {
		t.mu.Lock()
		switch chunk.state {
		case chunkStateLoaded:
			t.mu.Unlock()
			return nil
		case chunkStateLoading:
			done := chunk.loadDone
			t.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		case chunkStateCold:
			if t.store == nil {
				t.mu.Unlock()
				return errors.New("transform log store is nil")
			}
			done := make(chan struct{})
			chunk.state = chunkStateLoading
			chunk.loadDone = done
			t.mu.Unlock()

			loaded, err := t.store.ReadTransformLogChunk(ctx, t.vchannel, chunk.id)
			var entries []*streamingpb.TransformLogEntry
			if err == nil {
				err = validateChunk(loaded, chunk.id, 0)
			}
			if err == nil {
				entries = cloneTransformLogEntries(loaded.GetEntries())
			}

			t.mu.Lock()
			if err == nil {
				chunk.setEntries(entries)
				err = t.validateLoadedChunkOrderLocked(chunk)
			}
			if err != nil {
				chunk.clearEntries()
				chunk.state = chunkStateCold
			}
			close(done)
			chunk.loadDone = nil
			t.mu.Unlock()
			return err
		}
	}
}

func (t *TransformLog) validateLoadedChunkOrderLocked(chunk *chunkDescriptor) error {
	if !chunk.loaded() {
		return nil
	}
	for idx, candidate := range t.chunks {
		if candidate != chunk {
			continue
		}
		if idx > 0 {
			previous := t.chunks[idx-1]
			if previous.toTimeTick > 0 && chunk.fromTimeTick <= previous.toTimeTick {
				return errors.Errorf("transform log chunk %d entries are not ordered", chunk.id)
			}
		}
		if idx+1 < len(t.chunks) {
			next := t.chunks[idx+1]
			if next.fromTimeTick > 0 && chunk.toTimeTick >= next.fromTimeTick {
				return errors.Errorf("transform log chunk %d entries are not ordered", chunk.id)
			}
		}
		return nil
	}
	return nil
}

func (t *TransformLog) setStreamNotifier(notifier streamNotifier) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.streamNotifier = notifier
}

func (t *TransformLog) notifyStreamLocked() {
	if t.streamNotifier != nil {
		t.streamNotifier.notify(t.vchannel)
	}
}

func (t *TransformLog) latestTimeTickLocked() uint64 {
	return maxTimeTick(maxTimeTick(t.meta.GetCheckpointTimeTick(), t.buffer.DataTimeTick()), t.syncUpTimeTick)
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

type chunkState int

const (
	chunkStateCold chunkState = iota
	chunkStateLoading
	chunkStateLoaded
)

type chunkDescriptor struct {
	id           uint64
	state        chunkState
	loadDone     chan struct{}
	fromTimeTick uint64
	toTimeTick   uint64
	entries      []*streamingpb.TransformLogEntry
}

func newColdChunkDescriptor(chunkID uint64) *chunkDescriptor {
	return &chunkDescriptor{
		id:    chunkID,
		state: chunkStateCold,
	}
}

func newLoadedChunkDescriptor(chunk *streamingpb.TransformLogChunk) *chunkDescriptor {
	descriptor := &chunkDescriptor{
		id:    chunk.GetChunkId(),
		state: chunkStateLoaded,
	}
	descriptor.setEntries(cloneTransformLogEntries(chunk.GetEntries()))
	return descriptor
}

func (c *chunkDescriptor) loaded() bool {
	return c.state == chunkStateLoaded
}

func (c *chunkDescriptor) setEntries(entries []*streamingpb.TransformLogEntry) {
	c.entries = entries
	if len(entries) == 0 {
		c.fromTimeTick = 0
		c.toTimeTick = 0
		return
	}
	c.fromTimeTick = entries[0].GetTimeTick()
	c.toTimeTick = entries[len(entries)-1].GetTimeTick()
	c.state = chunkStateLoaded
}

func (c *chunkDescriptor) clearEntries() {
	c.entries = nil
	c.fromTimeTick = 0
	c.toTimeTick = 0
}

func cloneMetaOrNew(meta *streamingpb.VChannelTransformLogMeta) *streamingpb.VChannelTransformLogMeta {
	if meta == nil {
		return &streamingpb.VChannelTransformLogMeta{}
	}
	return cloneMeta(meta)
}

func newColdChunkDescriptorsFromMeta(meta *streamingpb.VChannelTransformLogMeta) []*chunkDescriptor {
	if meta == nil || meta.GetFirstChunkId() == meta.GetNextChunkId() {
		return nil
	}
	chunks := make([]*chunkDescriptor, 0, meta.GetNextChunkId()-meta.GetFirstChunkId())
	for chunkID := meta.GetFirstChunkId(); chunkID < meta.GetNextChunkId(); chunkID++ {
		chunks = append(chunks, newColdChunkDescriptor(chunkID))
	}
	return chunks
}

func cloneMeta(meta *streamingpb.VChannelTransformLogMeta) *streamingpb.VChannelTransformLogMeta {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*streamingpb.VChannelTransformLogMeta)
}

func cloneTransformLogEntries(entries []*streamingpb.TransformLogEntry) []*streamingpb.TransformLogEntry {
	cloned := make([]*streamingpb.TransformLogEntry, 0, len(entries))
	for _, entry := range entries {
		cloned = append(cloned, cloneTransformLogEntry(entry))
	}
	return cloned
}

func maxTimeTick(left uint64, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}

func isTransformDeleteEntry(entry *streamingpb.TransformLogEntry) bool {
	if entry == nil {
		return false
	}
	return entry.GetDelete() != nil
}
