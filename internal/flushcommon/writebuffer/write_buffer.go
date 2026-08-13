package writebuffer

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	nonFlushTS uint64 = 0
)

// DataNodeFlowGraphBufferDataSize is collection-scoped while write buffers are
// channel-scoped. Serialize Add/Sub/Delete so one channel cannot delete the
// series between another channel's update and observation. A zero-valued
// series is deleted here, at the point the aggregate actually reaches zero;
// DataSyncService cleanup runs before RemoveChannel on the streaming path and
// therefore cannot safely own this collector's lifecycle.
var flowGraphBufferMetricMu sync.Mutex

// growingFlushCancelGrace bounds how long a canceled wait keeps waiting for an
// in-flight growing-source flush. Whatever is cancellable unwinds within it; an
// already-started native flush cannot be preempted at all, so waiting longer
// only turns a timeout into a hang.
const growingFlushCancelGrace = 30 * time.Second

// WriteBuffer is the interface for channel write buffer.
// It provides abstraction for channel write buffer and pk bloom filter & L0 delta logic.
type WriteBuffer interface {
	// HasSegment checks whether certain segment exists in this buffer.
	HasSegment(segmentID int64) bool
	// CreateNewGrowingSegment creates a new growing segment in the buffer.
	CreateNewGrowingSegment(info CreateGrowingSegmentInfo) error
	// BufferData is the method to buffer dml data msgs.
	BufferData(insertMsgs []*InsertData, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition, schemaVersion int32) error
	// FlushTimestamp set flush timestamp for write buffer
	SetFlushTimestamp(flushTs uint64)
	// GetFlushTimestamp get current flush timestamp
	GetFlushTimestamp() uint64
	// SealSegments is the method to perform `Sync` operation with provided options.
	SealSegments(ctx context.Context, segmentIDs []int64) error
	// SealAllSegments seal all segments in the write buffer.
	SealAllSegments(ctx context.Context)
	// DropPartitions mark segments as Dropped of the partition
	DropPartitions(partitionIDs []int64)
	// GetCheckpoint returns current channel checkpoint.
	// If there are any non-empty segment buffer, returns the earliest buffer start position.
	// Otherwise, returns latest buffered checkpoint.
	GetCheckpoint() *msgpb.MsgPosition
	// MemorySize returns the size in bytes currently used by this write buffer.
	MemorySize() int64

	// EvictableMemorySize is the subset of MemorySize the watchdog can actually
	// reclaim; see the implementation for why they differ.
	EvictableMemorySize() int64
	// EvictBuffer evicts buffer to sync manager which match provided sync policies.
	EvictBuffer(policies ...SyncPolicy)
	// AllowGrowingSourceFlush returns true if this write buffer may try growing-source flush.
	AllowGrowingSourceFlush() bool
	// GetGrowingFlushProgress reports growing-source progress as of right now.
	// If segmentIDs is empty, all tracked growing-source segments are returned;
	// otherwise tracked growing-source segments are added to the requested ones.
	GetGrowingFlushProgress(ctx context.Context, segmentIDs []int64) ([]GrowingFlushSegmentProgress, error)
	// FenceGrowingSourceAdmission stops new segments on this channel from being
	// admitted to growing-source mode. The release path calls it before
	// appending its ManualFlush; see the implementation for why that order is
	// load-bearing.
	FenceGrowingSourceAdmission()
	// WaitGrowingFlushDrained blocks until no segment on this channel still owes
	// a growing-source flush. The release path must not drop growing segments
	// before it returns — see the implementation for why.
	WaitGrowingFlushDrained(ctx context.Context, segmentIDs []int64) error
	// Close is the method to close and sink current buffer data.
	Close(ctx context.Context, drop bool)
}

type CreateGrowingSegmentInfo struct {
	PartitionID    int64
	SegmentID      int64
	StartPos       *msgpb.MsgPosition
	SchemaVersion  int32
	StorageVersion int64
}

type GrowingFlushSegmentProgress struct {
	SegmentID int64
	// FlushThroughTs is the WAL position this segment still has to be flushed
	// through, as a timestamp. Zero means nothing is outstanding.
	FlushThroughTs     uint64
	NeedReleaseHandoff bool
	SourceMode         metacache.FlushSourceMode
}

func NewWriteBuffer(channel string, metacache metacache.MetaCache, syncMgr syncmgr.SyncManager, opts ...WriteBufferOption) (WriteBuffer, error) {
	option := defaultWBOption(metacache)
	for _, opt := range opts {
		opt(option)
	}

	return NewL0WriteBuffer(channel, metacache, syncMgr, option)
}

// writeBufferBase is the common component for buffering data
type writeBufferBase struct {
	collectionID int64
	channelName  string

	metaWriter       syncmgr.MetaWriter
	allocator        allocator.Interface
	estSizePerRecord int
	metaCache        metacache.MetaCache

	mut     sync.RWMutex
	buffers map[int64]*segmentBuffer // segmentID => segmentBuffer

	syncPolicies []SyncPolicy
	syncMgr      syncmgr.SyncManager

	checkpoint     *msgpb.MsgPosition
	flushTimestamp *atomic.Uint64
	syncCtx        context.Context
	syncCancel     context.CancelFunc

	// metricBytes is what this write buffer has added to the collection-level
	// DataNodeFlowGraphBufferDataSize gauge and not yet taken back; settled once
	// on close. See addBufferMetric.
	// metricMu, not atomics: the settled check and the adjustment it guards must
	// be ONE step. A payload-release callback that passed an atomic check and
	// then resumed after settleBufferMetric had already published would emit the
	// very late Sub this mechanism exists to prevent. Leaf lock — nothing is
	// acquired while holding it.
	metricMu      sync.Mutex
	metricBytes   int64
	metricSettled bool

	// l0Segments / l0partition map a partition to its current L0 segment and
	// back. Task construction rotates the mapping before yielding the segment's
	// payload, so later deletes cannot join an L0 segment already being flushed.
	l0Segments  map[int64]int64
	l0partition map[int64]int64

	// growingSettled is closed and replaced every time a growing-source sync
	// stops being in flight. It is the growing counterpart of
	// writeBufferSyncEntry.done: without it, waiting for growing syncs to settle
	// could only be done by polling. Guarded by mut.
	growingSettled chan struct{}

	// errHandler is fatal: it is used for sync tasks whose payload was yielded
	// out of the buffer and that have no re-submit path, so the only safe
	// recovery is process restart plus WAL replay from the (unadvanced)
	// checkpoint.
	errHandler func(err error)
	// growingSourceErrHandler is non-fatal: growing-source syncs read from the
	// segcore growing segment and are re-submitted by
	// armRefRetryLocked, so a failed attempt loses nothing.
	growingSourceErrHandler func(err error)
	taskObserverCallback    func(t syncmgr.Task, err error) // execute when a sync task finished, should be concurrent safe.

	// Channel-level admission flag for trying growing-source flush. Actual segment
	// source selection remains sticky in metacache.
	allowGrowingSourceFlush bool

	growingSourceResolver GrowingSourceResolver

	// The per-segment growing ledger lives on the segment's refPayload inside
	// wb.buffers (see payload_ref.go); the sticky source decision itself lives
	// in metacache.SegmentInfo.flushSourceMode.
	growingSourceFailureMetricSettled bool

	// growingSourceAdmissionFence blocks NEW segments from choosing
	// FlushSourceGrowing once a release handoff has been prepared for this
	// channel. It records the highest provider registration token observed at
	// fence time; admission reopens only when a provider with a newer token
	// registers, i.e. the channel has been re-subscribed locally. Guarded by
	// mut. See decideGrowingFlushSource for why this must exist.
	growingSourceAdmissionFence uint64
	flushRetryInterval          time.Duration

	// writeBufferSyncQueues preserves task construction order per segment. The
	// sync manager owns concurrent Prepare and FIFO Commit/ACK; this queue owns
	// admission (the reorder window) and whole-task retry.
	writeBufferSyncQueues   map[int64]*writeBufferSyncQueue
	flushSourceModeNotifier FlushSourceModeNotifier
	dropping                bool
	closed                  bool

	// pre build logger
	logger *mlog.Logger
}

func newWriteBufferBase(channel string, metacache metacache.MetaCache, syncMgr syncmgr.SyncManager, option *writeBufferOption) (*writeBufferBase, error) {
	flushTs := atomic.NewUint64(nonFlushTS)
	flushTsPolicy := GetFlushTsPolicy(flushTs, metacache)
	option.syncPolicies = append(option.syncPolicies, flushTsPolicy)

	schema := metacache.GetSchema(0)
	estSize, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		return nil, err
	}
	syncCtx, syncCancel := context.WithCancel(context.Background())

	allowGrowingSourceFlush := typeutil.AllowGrowingSourceFlush(schema,
		paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool(),
		paramtable.Get().CommonCfg.EnableGrowingSourceFlush.GetAsBool())
	// Segment resolution and the admission fence's registration tokens go
	// through ONE resolver — see GrowingSourceResolver for why the two must
	// agree on the authority.
	var growingSourceResolver GrowingSourceResolver = registryGrowingSourceResolver{channel: channel}
	if option.growingSourceResolver != nil {
		growingSourceResolver = option.growingSourceResolver
	}
	wb := &writeBufferBase{
		channelName:             channel,
		collectionID:            metacache.Collection(),
		estSizePerRecord:        estSize,
		syncMgr:                 syncMgr,
		metaWriter:              option.metaWriter,
		allocator:               option.idAllocator,
		buffers:                 make(map[int64]*segmentBuffer),
		metaCache:               metacache,
		syncPolicies:            option.syncPolicies,
		flushTimestamp:          flushTs,
		syncCtx:                 syncCtx,
		syncCancel:              syncCancel,
		errHandler:              option.errorHandler,
		growingSourceErrHandler: option.growingSourceErrorHandler,
		taskObserverCallback:    option.taskObserverCallback,
		allowGrowingSourceFlush: allowGrowingSourceFlush,
		growingSourceResolver:   growingSourceResolver,
		growingSettled:          make(chan struct{}),
		l0Segments:              make(map[int64]int64),
		l0partition:             make(map[int64]int64),
		flushRetryInterval:      option.flushRetryInterval,
		writeBufferSyncQueues:   make(map[int64]*writeBufferSyncQueue),
		flushSourceModeNotifier: option.flushSourceModeNotifier,
	}

	wb.logger = mlog.With(mlog.Int64("collectionID", wb.collectionID),
		mlog.String("channel", wb.channelName))
	if wb.errHandler == nil {
		wb.errHandler = func(err error) {
			panic(err)
		}
	}

	// A nil handler would silently drop failure reporting for a path that is
	// expected to fail and retry, so never leave it unset even when the option
	// struct was built directly (tests, embedded callers). Rate-limited on
	// purpose: retries run as often as dataNode.flushRetryInterval allows (3s by
	// default, configurable down to the 100ms ticker floor), so an unrated warn here turns one
	// stuck segment into a log flood. The per-failure counter and the escalating
	// summary live in observeGrowingSourceSyncFailureLocked.
	if wb.growingSourceErrHandler == nil {
		wb.growingSourceErrHandler = func(err error) {
			wb.logger.RatedWarn(wb.syncCtx, rate.Limit(1),
				"growing-source sync failed, will retry", mlog.Err(err))
		}
	}

	return wb, nil
}

func (wb *writeBufferBase) HasSegment(segmentID int64) bool {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	_, ok := wb.buffers[segmentID]
	return ok
}

func (wb *writeBufferBase) SealSegments(ctx context.Context, segmentIDs []int64) error {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	return wb.sealSegments(ctx, segmentIDs)
}

func (wb *writeBufferBase) SealAllSegments(ctx context.Context) {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	// mark all segments sealed if they were growing
	//
	// FlushAll and AlterWAL come through here, and they are fences exactly like a
	// targeted ManualFlush: pin the checkpoint for the same reason and with the
	// same lifecycle (see sealActionLocked).
	// The Sealed state (plus the fence pin sealActionLocked installs) IS the
	// flush debt, for growing-backed segments exactly like owned ones.
	wb.metaCache.UpdateSegments(wb.sealActionLocked(),
		metacache.WithSegmentState(commonpb.SegmentState_Growing))
}

func (wb *writeBufferBase) DropPartitions(partitionIDs []int64) {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	wb.dropPartitions(partitionIDs)
}

func (wb *writeBufferBase) SetFlushTimestamp(flushTs uint64) {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	wb.flushTimestamp.Store(flushTs)
}

func (wb *writeBufferBase) GetFlushTimestamp() uint64 {
	return wb.flushTimestamp.Load()
}

// writeBufferWaitInterval is how often a wait on buffer state re-checks it.
// There is nothing to subscribe to: the state these waits watch is advanced by
// timeticks and sync callbacks that hold mut, and waking them individually
// would put the waiters on the critical path of every buffer mutation.
const writeBufferWaitInterval = 10 * time.Millisecond

// waitFor re-evaluates check on a fixed tick until it reports done or fails.
//
// check runs under mut.RLock and is handed wb.closed, because "the buffer is
// going away" means different things to different waiters: for some it is the
// failure they were guarding against, for others it is a legitimate way to stop
// waiting. Deciding that belongs to the caller.
//
// check runs with mut held, so it must not re-acquire mut, block, await a
// future, or reserve admission. Reading the metacache is allowed — that is the
// same lock order (mut -> metacache) the rest of this package already takes,
// and WaitGrowingFlushDrained relies on it.
func (wb *writeBufferBase) waitFor(ctx context.Context, check func(closed bool) (bool, error)) error {
	ticker := time.NewTicker(writeBufferWaitInterval)
	defer ticker.Stop()
	for {
		wb.mut.RLock()
		done, err := check(wb.closed)
		wb.mut.RUnlock()
		if err != nil || done {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (wb *writeBufferBase) MemorySize() int64 {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	return wb.totalBufferedMemorySizeLocked() + wb.totalWriteBufferPayloadMemorySizeLocked()
}

// addBufferMetric moves this write buffer's contribution to the collection-level
// buffered-size gauge and remembers the running total, so close can settle back
// exactly what this buffer put there.
//
// Every Add and every Sub goes through here. The gauge is keyed by
// (nodeID, collectionID) with no channel label, so a write buffer may only ever
// adjust it by its own deltas — never assign it an absolute value.
func (wb *writeBufferBase) addBufferMetric(delta int64) {
	if delta == 0 {
		return
	}
	wb.metricMu.Lock()
	defer wb.metricMu.Unlock()
	if wb.metricSettled {
		return
	}
	wb.metricBytes += delta
	wb.adjustBufferMetric(delta)
}

func (wb *writeBufferBase) adjustBufferMetric(delta int64) {
	nodeID := paramtable.GetStringNodeID()
	collectionID := fmt.Sprint(wb.collectionID)

	flowGraphBufferMetricMu.Lock()
	defer flowGraphBufferMetricMu.Unlock()

	gauge := metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(nodeID, collectionID)
	gauge.Add(float64(delta))
	metric := &dto.Metric{}
	if err := gauge.Write(metric); err == nil && metric.GetGauge().GetValue() == 0 {
		metrics.DataNodeFlowGraphBufferDataSize.DeleteLabelValues(nodeID, collectionID)
	}
}

// settleBufferMetric returns this write buffer's whole contribution to the gauge
// and stops it from accounting again.
//
// It must run on every close path, and everything after it is deliberately
// dropped rather than subtracted: the shutdown wait is BOUNDED, so a task may
// release its payload after this write buffer has returned its whole
// contribution and the collection aggregate has reached zero. A late Sub would
// recreate that series from zero as a negative value.
func (wb *writeBufferBase) settleBufferMetric() {
	wb.metricMu.Lock()
	defer wb.metricMu.Unlock()
	if wb.metricSettled {
		return
	}
	wb.metricSettled = true
	if wb.metricBytes != 0 {
		wb.adjustBufferMetric(-wb.metricBytes)
		wb.metricBytes = 0
	}
}

func (wb *writeBufferBase) totalBufferedMemorySizeLocked() int64 {
	var size int64
	for _, segBuf := range wb.buffers {
		size += segBuf.MemorySize()
	}
	return size
}

func (wb *writeBufferBase) totalWriteBufferPayloadMemorySizeLocked() int64 {
	var size int64
	for _, queue := range wb.writeBufferSyncQueues {
		for _, entry := range queue.entries {
			// A growing task reports 0: its rows stay in segcore.
			size += entry.task.PayloadBytes()
		}
	}
	return size
}

// EvictableMemorySize is the buffered subset the memory watchdog can actually
// reclaim. Payload already yielded to a task is resident memory too, but it is
// not evictable: its segment already has an owner, and picking it would only
// spin. Same for a segment whose queue is full or barred — see
// writeBufferSyncBlockedLocked.
func (wb *writeBufferBase) EvictableMemorySize() int64 {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	var size int64
	for segmentID, buffer := range wb.buffers {
		if wb.writeBufferSyncBlockedLocked(segmentID) {
			continue
		}
		if p, ok := buffer.payload.(*refPayload); ok && p.syncing {
			continue
		}
		size += buffer.MemorySize()
	}
	return size
}

// backpressureWaiterLocked returns a channel to wait on when this channel is
// holding more data for a segment than one flush is supposed to carry, counting
// both what is still buffered and what a task already took but has not written
// yet. Retained payload is the signal that matters: it only grows while flushes
// are failing, which is exactly when ingestion must slow down.
//
// The wait is on payload release, not on task completion. A task releases its
// payload as soon as object storage accepts it, while completion also waits for
// metadata — and, when a flush keeps failing, may not happen for a long time.
func (wb *writeBufferBase) backpressureWaiterLocked() <-chan struct{} {
	insertLimit := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	deleteLimit := paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64()
	for segmentID, queue := range wb.writeBufferSyncQueues {
		var insertBytes, deleteBytes int64
		var waiter *writeBufferSyncEntry
		for _, entry := range queue.entries {
			// Growing tasks hold no payload (they report 0): their rows stay in
			// segcore and never count against the flush budget.
			task := entry.task
			insertBytes += task.InsertPayloadBytes()
			deleteBytes += task.DeletePayloadBytes()
			if waiter == nil && task.PayloadBytes() > 0 && entry.payloadReleased != nil {
				waiter = entry
			}
		}
		if buffer := wb.buffers[segmentID]; buffer != nil {
			if buffer.payload != nil {
				insertBytes += buffer.payload.UnflushedBytes()
			}
			if buffer.deltaBuffer != nil {
				deleteBytes += buffer.deltaBuffer.size
			}
		}
		insertFull := insertBytes > 0 && insertLimit != noLimit && insertBytes >= insertLimit
		deleteFull := deleteBytes > 0 && deleteLimit != noLimit && deleteBytes >= deleteLimit
		if insertFull || deleteFull {
			if waiter != nil {
				return waiter.payloadReleased
			}
			// No retained payload, but the segment is over budget with a
			// flush-impaired queue: a Commit/meta phase stuck in retry holds
			// no payload, and the buffer is excluded from EvictableMemorySize
			// (force-syncing a blocked segment would violate ordering), so
			// without waiting on task COMPLETION here the buffered tail grows
			// without ANY bound. Gated on everFailed so a healthy pipeline
			// that is merely over budget mid-commit is not penalized. The
			// bufferManager retry ticker keeps re-driving the stuck task
			// while ingestion waits on it.
			if len(queue.entries) > 0 && queue.entries[0].everFailed {
				return queue.entries[0].done
			}
		}
	}
	return nil
}

// waitFlushCapacity slows ingestion while a segment is over its flush budget.
// The triggering batch is submitted before this wait, so a batch larger than
// the budget still makes progress and releases its own payload.
//
// The wait is BOUNDED. The flowgraph delivers BufferData and an eventual
// DropChannel on ONE goroutine, so an unbounded wait here can deadlock a drop:
// the DropChannel that would cancel this context is queued behind the very call
// that is waiting. After the bound, proceed anyway — the memory watchdog in
// bufferManager still force-syncs and evicts, and a wedged flowgraph would take
// the channel down with it.
func (wb *writeBufferBase) waitFlushCapacity() error {
	// Fast path first: this runs on the flowgraph goroutine for every msgpack,
	// including the pure-timetick ones, and a healthy channel never waits. Do
	// not pay for a timer and a ticker before knowing there is anything to wait
	// for.
	waiter, err := wb.backpressureWaiter()
	if err != nil || waiter == nil {
		return err
	}

	bound := time.NewTimer(
		paramtable.Get().DataNodeCfg.GracefulStopTimeout.GetAsDuration(time.Second))
	defer bound.Stop()
	// This is the flowgraph goroutine — the same one that delivers the
	// timeticks driveRetries rides on. While it is parked here the flush this
	// wait depends on is re-driven by the bufferManager retry ticker, which
	// runs on its own goroutine and reaches this buffer through the manager
	// map for as long as the channel is registered.
	for {
		select {
		case <-waiter:
		case <-bound.C:
			wb.logger.Warn(wb.syncCtx, "flush backpressure wait exceeded its bound; "+
				"proceeding so a pending DropChannel on this flowgraph goroutine "+
				"cannot deadlock behind this wait")
			return nil
		case <-wb.syncCtx.Done():
			return merr.WrapErrChannelNotFound(wb.channelName)
		}

		if waiter, err = wb.backpressureWaiter(); err != nil || waiter == nil {
			return err
		}
	}
}

// backpressureWaiter reports what this channel has to wait on before it may
// buffer more data. A nil channel and a nil error mean it may proceed now.
func (wb *writeBufferBase) backpressureWaiter() (<-chan struct{}, error) {
	wb.mut.RLock()
	defer wb.mut.RUnlock()
	if wb.closed || wb.dropping {
		return nil, merr.WrapErrChannelNotFound(wb.channelName)
	}
	return wb.backpressureWaiterLocked(), nil
}

func (wb *writeBufferBase) EvictBuffer(policies ...SyncPolicy) {
	logger := wb.logger

	wb.mut.Lock()

	// need valid checkpoint before triggering syncing
	if wb.checkpoint == nil {
		wb.mut.Unlock()
		logger.Warn(context.TODO(), "evict buffer before buffering data")
		return
	}

	ts := wb.checkpoint.GetTimestamp()
	segmentIDs := wb.getSegmentsToSync(ts, policies...)
	if len(segmentIDs) > 0 {
		logger.Info(context.TODO(), "evict buffer find segments to sync", mlog.Int64s("segmentIDs", segmentIDs))
	}

	wb.mut.Unlock()

	if len(segmentIDs) > 0 {
		futures := wb.syncSegments(wb.syncCtx, segmentIDs)
		if len(futures) > 0 {
			conc.AwaitAll(futures...)
		}
	}
}

// GetCheckpoint returns the position the channel checkpoint may not pass — the
// minimum over two candidate classes, both DERIVED on every call (from the
// segment buffers' replay origins and from the metacache seal pins), never
// registered: the old syncCheckpoint registry (Add/AddUnique/RemoveUnique keys)
// is gone, its role absorbed by the payload floor lists (owned) and ledger
// batches (ref).
//
// One pass, no candidate slice: this runs on every checkpoint report, so it
// tracks the running minimum (plus the segment/source it came from, for
// logging only) in locals.
func (wb *writeBufferBase) GetCheckpoint() *msgpb.MsgPosition {
	logger := wb.logger
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	var (
		minPos    *msgpb.MsgPosition
		minSegID  int64
		minSource string
	)
	consider := func(segmentID int64, position *msgpb.MsgPosition, source string) {
		if position == nil {
			return
		}
		if minPos == nil || position.GetTimestamp() < minPos.GetTimestamp() {
			minPos, minSegID, minSource = position, segmentID, source
		}
	}

	for _, buf := range wb.buffers {
		consider(buf.segmentID, buf.EarliestPosition(), "segment buffer")
	}

	// Segments that owe a flush pin the fence that sealed them. This candidate
	// class is DERIVED from metacache state, not from a registration: it exists
	// from the seal itself (before any sync task is built, when the buffer is
	// empty and no other candidate covers the segment) and it vanishes when the
	// segment leaves the metacache on flush/drop commit, so there is nothing to
	// unregister and nothing to leak. See sealActionLocked.
	for _, segment := range wb.metaCache.GetSegmentsBy(metacache.WithSegmentState(pendingFlushStates...)) {
		consider(segment.SegmentID(), pendingFlushFence(segment), "pending flush fence")
	}

	if minPos == nil {
		// all buffer are empty
		logger.RatedDebug(context.TODO(), rate.Limit(60), "checkpoint from latest consumed msg", mlog.Uint64("cpTimestamp", wb.checkpoint.GetTimestamp()))
		return wb.checkpoint
	}

	logger.RatedDebug(context.TODO(), rate.Limit(20), "checkpoint evaluated",
		mlog.String("cpSource", minSource),
		mlog.FieldSegmentID(minSegID),
		mlog.Uint64("cpTimestamp", minPos.GetTimestamp()))
	return minPos
}

// pendingFlushStates are the states in which a segment still owes the flush its
// seal fence asked for, and therefore still has to pin the channel checkpoint.
// Dropped is NOT one of them: a dropped segment owes no flush, and its
// metadata-only drop task deliberately pins nothing — drop authority is
// coordinator meta, which converges with zero DataNode drop-ack (see the
// pinning comment in getWriteBufferSyncTask).
var pendingFlushStates = []commonpb.SegmentState{
	commonpb.SegmentState_Sealed,
	commonpb.SegmentState_Flushing,
}

// pendingFlushState is the single spelling of "state is in the Sealed+Flushing
// subset" — a segment in it still owes the flush its seal fence asked for.
func pendingFlushState(state commonpb.SegmentState) bool {
	return lo.Contains(pendingFlushStates, state)
}

// terminalSegmentState reports whether state IS a terminal flush/drop debt:
// pending-flush (Sealed/Flushing) or Dropped. Only the terminal task's commit
// removes the segment or its buffer.
func terminalSegmentState(state commonpb.SegmentState) bool {
	return pendingFlushState(state) || state == commonpb.SegmentState_Dropped
}

// pendingFlushFence returns the position that a replay must resume from to
// regenerate this segment's outstanding flush obligation, or nil when the
// segment owes nothing (never sealed by a pinning path, or no longer owing).
func pendingFlushFence(segment *metacache.SegmentInfo) *msgpb.MsgPosition {
	if segment == nil {
		return nil
	}
	if !pendingFlushState(segment.State()) {
		return nil
	}
	return segment.PendingFlushCheckpoint()
}

func (wb *writeBufferBase) hasWriteBufferInsertPayload(segmentID int64) bool {
	buffer, ok := wb.buffers[segmentID]
	if !ok || buffer.payload == nil {
		return false
	}
	// Ref payloads account ledger rows, but those rows live in segcore — only
	// OWNED resident rows make a segment a write-buffer-payload segment.
	if _, ref := buffer.payload.(*refPayload); ref {
		return false
	}
	return buffer.payload.UnflushedRows() != 0 || buffer.payload.UnflushedBytes() != 0
}

func (wb *writeBufferBase) triggerSync() (segmentIDs []int64) {
	segmentsToSync := wb.getSegmentsToSync(wb.checkpoint.GetTimestamp(), wb.syncPolicies...)
	if len(segmentsToSync) > 0 {
		mlog.Info(context.TODO(), "write buffer get segments to sync", mlog.Int64s("segmentIDs", segmentsToSync))
	}

	return segmentsToSync
}

func (wb *writeBufferBase) sealSegments(ctx context.Context, segmentIDs []int64) error {
	existingIDs := make([]int64, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		_, ok := wb.metaCache.GetSegmentByID(segmentID)
		if !ok {
			if !wb.allowGrowingSourceFlush {
				mlog.Warn(ctx, "cannot find segment when sealSegments",
					mlog.Int64("segmentID", segmentID),
					mlog.String("channel", wb.channelName))
				return merr.WrapErrSegmentNotFound(segmentID)
			}
			mlog.Info(ctx, "segment not found in WriteBuffer metaCache, skipping seal",
				mlog.FieldSegmentID(segmentID),
				mlog.String("channel", wb.channelName))
			continue
		}
		existingIDs = append(existingIDs, segmentID)
	}
	// mark segment flushing if segment was growing
	if len(existingIDs) > 0 {
		wb.metaCache.UpdateSegments(wb.sealActionLocked(),
			metacache.WithSegmentIDs(existingIDs...),
			metacache.WithSegmentState(commonpb.SegmentState_Growing))
	}
	return nil
}

// sealActionLocked marks a growing segment sealed AND pins the channel
// checkpoint at the position whose replay redelivers the fence that sealed it.
//
// The pin has to exist from HERE, not from the moment the resulting sync task is
// built. A segment whose write buffer was already drained by an earlier periodic
// flush owes a metadata-only flush: there is no buffer to pin the checkpoint, no
// growing-source progress, and no sync entry yet. Without this, GetCheckpoint
// could report (and the reporter persist) a position past the fence; a crash
// there replays from beyond the ManualFlush, the fence is never redelivered, and
// the segment stays Growing until DataCoord's idle seal.
//
// The pin is DERIVED, not registered: GetCheckpoint reads it back off the
// metacache segment, which is removed when the flush (or drop) commits. There is
// no key for any path to forget to release.
//
// Combined into one action under the Growing filter on purpose: only the segment
// that actually transitions Growing -> Sealed here gets pinned, so a re-seal of
// an already sealed segment cannot move its pin (the action is set-if-nil as
// well, so both layers agree that the earliest fence wins).
//
// Caller must hold wb.mut, which is also what makes wb.checkpoint the position
// of the pack that carries this seal.
func (wb *writeBufferBase) sealActionLocked() metacache.SegmentAction {
	return metacache.MergeSegmentAction(
		metacache.UpdateState(commonpb.SegmentState_Sealed),
		metacache.SetPendingFlushCheckpointIfNil(typeutil.Clone(wb.checkpoint)),
	)
}

func (wb *writeBufferBase) dropPartitions(partitionIDs []int64) {
	// mark segment dropped if partition was dropped. The Dropped state IS the
	// drop debt for both payload modes.
	segIDs := wb.metaCache.GetSegmentIDsBy(metacache.WithPartitionIDs(partitionIDs))
	wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Dropped),
		metacache.WithSegmentIDs(segIDs...),
	)
}

func (wb *writeBufferBase) syncSegments(ctx context.Context, segmentIDs []int64) []*conc.Future[struct{}] {
	result := make([]*conc.Future[struct{}], 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		futures, stop := wb.syncSegment(ctx, segmentID)
		result = append(result, futures...)
		if stop {
			break
		}
	}
	return result
}

// buildSyncSubmissionsWithAdmission owns the reserve → build-under-lock →
// submit scaffold shared by syncSegment and submitDropSegment.
//
// It reserves an admission slot BEFORE taking wb.mut — admission may block
// while completion callbacks need wb.mut to finish older tasks and return
// their own slots — then runs buildLocked under wb.mut with the slot held, and
// submits whatever buildLocked produced after releasing the lock.
//
// buildLocked returns the submissions to submit and whether slot ownership was
// transferred (attached to a write-buffer entry, or left on a submission whose
// completion callback releases it). Unless transferred, the slot is released
// here — the single deferred Release below is the only release site on every
// path through this helper, so each caller keeps exactly one release per path.
//
// A reserve error or timeout is reported to the caller untouched (no slot is
// held on either): the periodic path keeps the payload buffered for a later
// round, while the drop path escalates.
func (wb *writeBufferBase) buildSyncSubmissionsWithAdmission(
	ctx context.Context,
	buildLocked func(admission syncmgr.SyncTaskAdmission) (submissions []syncTaskSubmission, transferred bool, err error),
) (futures []*conc.Future[struct{}], timedOut bool, err error) {
	admission, timedOut, err := wb.reserveSyncTask(ctx)
	if err != nil || timedOut {
		return nil, timedOut, err
	}
	admissionTransferred := false
	if admission != nil {
		defer func() {
			if !admissionTransferred {
				admission.Release()
			}
		}()
	}

	wb.mut.Lock()
	submissions, transferred, err := buildLocked(admission)
	admissionTransferred = transferred
	wb.mut.Unlock()
	if err != nil || len(submissions) == 0 {
		return nil, false, err
	}
	return wb.submitSyncTaskSubmissions(ctx, submissions), false, nil
}

func (wb *writeBufferBase) syncSegment(ctx context.Context, segmentID int64) ([]*conc.Future[struct{}], bool) {
	// Recheck lifecycle and task eligibility after admission. A task that owns
	// yielded data is then submitted before moving to the next segment.
	stopped := false
	futures, timedOut, err := wb.buildSyncSubmissionsWithAdmission(ctx,
		func(admission syncmgr.SyncTaskAdmission) ([]syncTaskSubmission, bool, error) {
			if wb.closed || wb.dropping {
				stopped = true
				return nil, false, nil
			}
			tasks := wb.getSyncTasksLocked(ctx, []int64{segmentID})
			transferred := false
			if admission != nil && len(tasks) > 0 {
				entry := wb.writeBufferSyncEntryLocked(tasks[0])
				if entry == nil {
					return nil, false, merr.WrapErrServiceInternalMsg(
						"sync task for segment %d has no write-buffer owner", segmentID)
				}
				// An owned entry retains the slot across retries; a growing
				// entry's completion callback releases it per attempt.
				entry.admission = admission
				transferred = true
			}
			submissions := make([]syncTaskSubmission, 0, len(tasks))
			for i, task := range tasks {
				submission := syncTaskSubmission{task: task}
				if i == 0 {
					submission.admission = admission
				}
				submissions = append(submissions, submission)
			}
			return submissions, transferred, nil
		})
	if err != nil {
		return []*conc.Future[struct{}]{completedSyncFuture(err)}, true
	}
	if timedOut {
		wb.logger.Warn(ctx, "sync task admission wait exceeded its bound; keep payload buffered for a later policy round",
			mlog.FieldSegmentID(segmentID))
		return nil, true
	}
	if stopped {
		return nil, true
	}
	return futures, false
}

// submitDropSegment builds the final task for one drained segment. Drop has
// already blocked normal submissions and waited for any existing owner, so the
// regular per-segment gate remains the only synchronization needed here.
func (wb *writeBufferBase) submitDropSegment(ctx context.Context, segmentID int64) ([]*conc.Future[struct{}], *writeBufferSyncEntry, error) {
	closed := false
	var writeBufferEntry *writeBufferSyncEntry
	futures, timedOut, err := wb.buildSyncSubmissionsWithAdmission(ctx,
		func(admission syncmgr.SyncTaskAdmission) ([]syncTaskSubmission, bool, error) {
			if wb.closed {
				closed = true
				return nil, false, nil
			}
			if _, ok := wb.refPayloadLocked(segmentID); ok {
				// The drop debt is the metacache Dropped state; record it so the
				// ref task builder freezes the drop flag and syncDropSegment's
				// re-drive loop can read the same debt back.
				wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Dropped),
					metacache.WithSegmentIDs(segmentID))
			}
			task, err := wb.getSyncTask(ctx, segmentID)
			if err != nil {
				if errors.Is(err, errGrowingSourceUnavailable) {
					wb.noteGrowingSourceCandidateFailed(segmentID)
				}
				return nil, false, err
			}
			if task == nil {
				return nil, false, merr.WrapErrServiceInternalMsg("segment %d still has an outstanding sync owner", segmentID)
			}
			if _, growing := task.(*syncmgr.GrowingSourceSyncTask); !growing {
				task.SetDrop()
			}
			entry := wb.writeBufferSyncEntryLocked(task)
			if entry == nil {
				return nil, false, merr.WrapErrServiceInternalMsg(
					"drop sync task for segment %d has no write-buffer owner", segmentID)
			}
			entry.admission = admission
			// Only the owned path hands its queue entry back to the caller to
			// wait on; the growing path's caller waits on the future instead
			// (its owesDrop loop re-drives until the drop settles).
			if _, owned := task.(*syncmgr.SyncTask); owned {
				writeBufferEntry = entry
			}
			return []syncTaskSubmission{{
				task:      task,
				admission: admission,
			}}, admission != nil, nil
		})
	if err != nil {
		return nil, nil, err
	}
	if timedOut {
		return nil, nil, merr.WrapErrServiceInternalMsg(
			"sync task admission wait exceeded its bound while dropping segment %d", segmentID)
	}
	if closed {
		return nil, nil, nil
	}
	return futures, writeBufferEntry, nil
}

// getSyncTasksLocked builds sync tasks and moves payload out of the write buffer.
// The caller must hold wb.mut and submit the returned tasks after releasing it.
func (wb *writeBufferBase) getSyncTasksLocked(ctx context.Context, segmentIDs []int64) []syncmgr.Task {
	result := make([]syncmgr.Task, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		syncTask, err := wb.getSyncTask(ctx, segmentID)
		if err != nil {
			if errors.Is(err, merr.ErrSegmentNotFound) {
				if _, ok := wb.refPayloadLocked(segmentID); ok {
					mlog.Fatal(ctx, "growing-source ledger outlived its metacache segment; refusing to discard checkpointed data",
						mlog.FieldSegmentID(segmentID),
						mlog.FieldVChannel(wb.channelName),
						mlog.Err(err))
				}
				mlog.Warn(ctx, "segment not found in meta", mlog.FieldSegmentID(segmentID))
				continue
			} else if errors.Is(err, errGrowingSourceUnavailable) {
				wb.noteGrowingSourceCandidateFailed(segmentID)
				mlog.Warn(ctx, "growing source unavailable when building sync task, retry later",
					mlog.Int64("segmentID", segmentID),
					mlog.String("channel", wb.channelName),
					mlog.Err(err))
				continue
			} else {
				mlog.Fatal(ctx, "failed to get sync task", mlog.FieldSegmentID(segmentID), mlog.Err(err))
			}
		}
		if syncTask != nil {
			result = append(result, syncTask)
		}
	}
	return result
}

// getSegmentsToSync applies all policies to get segments list to sync.
// **NOTE** shall be invoked within mutex protection
func (wb *writeBufferBase) getSegmentsToSync(ts typeutil.Timestamp, policies ...SyncPolicy) []int64 {
	// Empty buffers are invisible to policies. Buffers now persist across flush
	// rounds (their in-flight floors must stay reachable for GetCheckpoint), so
	// without this filter an empty segment with a flush in flight would be
	// re-selected by buffer-shape policies and produce no-op tasks; segments
	// that owe a flush without data are selected by the metacache-driven
	// Sealed/Dropped policies, exactly as before. Growing-backed (ref) buffers
	// enter the same policy input: their IsFull carries the growing trigger,
	// their MinTimestamp the staleness trigger.
	buffers := lo.Filter(lo.Values(wb.buffers), func(buffer *segmentBuffer, _ int) bool {
		return buffer != nil && !buffer.IsEmpty()
	})
	// Order-preserving dedup, not a Set: GetSealedSegmentsPolicy returns already
	// claimed (Flushing) segments before merely due (Sealed) ones, and that
	// order is the point — a claimed flush is finished before a new one starts.
	seen := typeutil.NewSet[int64]()
	var segments []int64
	add := func(ids ...int64) {
		for _, id := range ids {
			if !seen.Contain(id) {
				seen.Insert(id)
				segments = append(segments, id)
			}
		}
	}
	for _, policy := range policies {
		result := policy.SelectSegments(buffers, ts)
		if len(result) > 0 {
			mlog.Info(context.TODO(), "SyncPolicy selects segments", mlog.Int64s("segmentIDs", result), mlog.String("reason", policy.Reason()))
			add(result...)
		}
	}
	return lo.Filter(segments, func(segmentID int64, _ int) bool {
		payload, ok := wb.refPayloadLocked(segmentID)
		if !ok {
			return !wb.deferWriteBufferSyncLocked(segmentID)
		}
		// An outstanding rate limit binds here too. Policy selection runs on every
		// timetick, so without this a segment whose source just came back Pending
		// would be re-probed (and its task rebuilt) immediately, ignoring the
		// interval the retry drive honors. The debt lives on the same per-segment
		// queue intent the owned path uses.
		queue := wb.writeBufferSyncQueues[segmentID]
		if queue != nil && queue.intent.owes && !queue.intent.due(time.Now(), wb.retryInterval()) {
			return false
		}
		syncable := wb.refPayloadSyncableLocked(segmentID, payload)
		if syncable && queue != nil && queue.intent.owes {
			queue.intent.attempted(time.Now())
		}
		return syncable
	})
}

func (wb *writeBufferBase) getOrCreateBuffer(segmentID int64, timetick uint64) *segmentBuffer {
	buffer, ok := wb.buffers[segmentID]
	if !ok {
		var err error
		buffer, err = newSegmentBuffer(segmentID, wb.metaCache.GetSchema(timetick))
		if err != nil {
			// TODO avoid panic here
			panic(err)
		}
		wb.buffers[segmentID] = buffer
		if wb.allowGrowingSourceFlush {
			wb.metaCache.UpdateSegments(
				metacache.SetFlushSourceMode(metacache.FlushSourceWriteBuffer),
				metacache.WithSegmentIDs(segmentID),
			)
			wb.notifyFlushSourceMode(segmentID)
		}
	}

	return buffer
}

func (wb *writeBufferBase) notifyFlushSourceMode(segmentID int64) {
	if wb.flushSourceModeNotifier == nil {
		return
	}
	segment, ok := wb.metaCache.GetSegmentByID(segmentID)
	if !ok {
		return
	}
	switch mode := segment.FlushSourceMode(); mode {
	case metacache.FlushSourceWriteBuffer, metacache.FlushSourceGrowing:
		wb.flushSourceModeNotifier(segmentID, mode)
	}
}

type InsertData struct {
	segmentID   int64
	partitionID int64
	data        []*storage.InsertData
	bm25Stats   map[int64]*storage.BM25Stats

	pkField []storage.FieldData
	pkType  schemapb.DataType

	tsField []*storage.Int64FieldData
	rowNum  int64
}

func NewInsertData(segmentID, partitionID int64, cap int, pkType schemapb.DataType) *InsertData {
	data := &InsertData{
		segmentID:   segmentID,
		partitionID: partitionID,
		data:        make([]*storage.InsertData, 0, cap),
		pkField:     make([]storage.FieldData, 0, cap),
		pkType:      pkType,
	}
	return data
}

func (id *InsertData) Append(data *storage.InsertData, pkFieldData storage.FieldData, tsFieldData *storage.Int64FieldData) {
	id.data = append(id.data, data)
	id.pkField = append(id.pkField, pkFieldData)
	id.tsField = append(id.tsField, tsFieldData)
	id.rowNum += int64(data.GetRowNum())
}

func (id *InsertData) GetSegmentID() int64 {
	return id.segmentID
}

func (id *InsertData) SetBM25Stats(bm25Stats map[int64]*storage.BM25Stats) {
	id.bm25Stats = bm25Stats
}

func (id *InsertData) GetDatas() []*storage.InsertData {
	return id.data
}

func (wb *writeBufferBase) CreateNewGrowingSegment(info CreateGrowingSegmentInfo) error {
	_, ok := wb.metaCache.GetSegmentByID(info.SegmentID)
	// new segment
	if !ok {
		storageVersion, err := wb.resolveNewGrowingSegmentStorageVersion(info)
		if err != nil {
			return err
		}
		manifestPath := wb.newGrowingSegmentManifestPath(info.PartitionID, info.SegmentID, storageVersion)
		segmentInfo := &datapb.SegmentInfo{
			ID:             info.SegmentID,
			PartitionID:    info.PartitionID,
			CollectionID:   wb.collectionID,
			InsertChannel:  wb.channelName,
			StartPosition:  info.StartPos,
			State:          commonpb.SegmentState_Growing,
			StorageVersion: storageVersion,
			ManifestPath:   manifestPath,
			SchemaVersion:  info.SchemaVersion,
		}
		wb.metaCache.AddSegment(segmentInfo, func(_ *datapb.SegmentInfo) pkoracle.PkStat {
			return pkoracle.NewBloomFilterSetWithBatchSize(wb.getEstBatchSize())
		}, metacache.NewBM25StatsFactory, metacache.SetStartPosRecorded(false))
		mlog.Info(context.TODO(), "add growing segment", mlog.FieldSegmentID(info.SegmentID), mlog.String("channel", wb.channelName), mlog.Int64("storage version", storageVersion))
	}
	return nil
}

func (wb *writeBufferBase) resolveNewGrowingSegmentStorageVersion(info CreateGrowingSegmentInfo) (int64, error) {
	switch info.StorageVersion {
	case storage.StorageV2, storage.StorageV3:
		return info.StorageVersion, nil
	case storage.StorageV1:
		if streamingutil.IsStreamingServiceEnabled() {
			return 0, merr.WrapErrServiceInternalMsg("missing storage version for streaming growing segment, segmentID=%d", info.SegmentID)
		}
		inferred := storage.StorageV2
		reason := "default non-streaming storage version"
		if typeutil.HasTextField(wb.metaCache.GetSchema(0)) {
			inferred = storage.StorageV3
			reason = "TEXT field requires StorageV3"
		} else if paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
			inferred = storage.StorageV3
			reason = "common.storage.useLoonFFI enabled"
		}
		mlog.Warn(context.TODO(), "infer missing storage version for non-streaming growing segment",
			mlog.FieldSegmentID(info.SegmentID),
			mlog.Int64("collectionID", wb.collectionID),
			mlog.String("channel", wb.channelName),
			mlog.Int64("inferredStorageVersion", inferred),
			mlog.String("reason", reason))
		return inferred, nil
	default:
		return 0, merr.WrapErrServiceInternalMsg("unsupported storage version for growing segment, segmentID=%d storageVersion=%d",
			info.SegmentID, info.StorageVersion)
	}
}

func (wb *writeBufferBase) newGrowingSegmentManifestPath(partitionID int64, segmentID int64, storageVersion int64) string {
	if storageVersion != storage.StorageV3 {
		return ""
	}
	k := metautil.JoinIDPath(wb.collectionID, partitionID, segmentID)
	basePath := path.Join(paramtable.Get().MinioCfg.RootPath.GetValue(), common.SegmentInsertLogPath, k)
	return packed.MarshalManifestPath(basePath, packed.ManifestEarliest)
}

// bufferDelete buffers DeleteMsg into DeleteData.
func (wb *writeBufferBase) bufferDelete(segmentID int64, pks []storage.PrimaryKey, tss []typeutil.Timestamp, startPos, endPos *msgpb.MsgPosition) int64 {
	segBuf := wb.getOrCreateBuffer(segmentID, tss[0])
	bufSize := segBuf.deltaBuffer.Buffer(pks, tss, startPos, endPos)
	wb.addBufferMetric(bufSize)
	return bufSize
}

func (wb *writeBufferBase) getSyncTask(ctx context.Context, segmentID int64) (syncmgr.Task, error) {
	if payload, ok := wb.refPayloadLocked(segmentID); ok && payload.syncing {
		// Growing-source and write-buffer tasks share one segment-level gate.
		// The dispatcher would serialize the two tasks, but it cannot prevent the
		// second one from being built against counters the first has not
		// committed yet. Nothing to record: the metacache Sealed/Flushing/Dropped
		// state IS the debt, and the completion callback re-drives from it.
		return nil, nil
	}
	segmentInfo, ok := wb.metaCache.GetSegmentByID(segmentID) // wb.metaCache.GetSegmentsBy(metacache.WithSegmentIDs(segmentID))
	if !ok {
		mlog.Warn(ctx, "segment info not found in meta cache", mlog.FieldSegmentID(segmentID))
		return nil, merr.WrapErrSegmentNotFound(segmentID)
	}
	if wb.deferWriteBufferSyncLocked(segmentID) {
		return nil, nil
	}
	// Claim the flush. This is the point where its content is fixed: the seal
	// arrived in-band on this same flowgraph goroutine (DDNode -> WriteNode), so
	// every row of a Sealed segment is already buffered and no further row can
	// be assigned to it. Whatever this task takes IS the segment's tail.
	//
	// The claim is one-way. A task that fails to build or to commit leaves the
	// segment in Flushing, which GetSealedSegmentsPolicy selects, so the retry
	// resumes THIS flush rather than deciding a new one.
	if segmentInfo.State() == commonpb.SegmentState_Sealed {
		wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing),
			metacache.WithSegmentIDs(segmentID))
		// Re-read: metacache is copy-on-write, so the snapshot above still says
		// Sealed, and both task builders derive WithFlush from this state.
		if segmentInfo, ok = wb.metaCache.GetSegmentByID(segmentID); !ok {
			mlog.Warn(ctx, "segment vanished while claiming its flush", mlog.FieldSegmentID(segmentID))
			return nil, merr.WrapErrSegmentNotFound(segmentID)
		}
	}
	buffer := wb.buffers[segmentID]
	if buffer == nil {
		if segmentInfo.FlushSourceMode() == metacache.FlushSourceGrowing {
			// A growing-mode segment without a buffer (recovered sticky decision,
			// e.g. after WAL replay): create its ref buffer so the uniform
			// payload path below serves it; the normal insert path creates the
			// buffer at first insert.
			var err error
			buffer, err = wb.getOrCreateGrowingSegmentBufferLocked(segmentID)
			if err != nil {
				return nil, err
			}
		}
	}

	// ONE snapshot, ONE branch: the payload fixes the attempt's content, and
	// the presence of a growing side selects the task builder. The snapshot may
	// fail only for ref payloads (source Unavailable/Pending) — a retryable
	// debt the callers classify via errGrowingSourceUnavailable.
	var input *flushInput
	if buffer != nil {
		var err error
		input, err = buffer.payload.Snapshot(ctx, wb.checkpoint.GetTimestamp())
		if err != nil {
			return nil, err
		}
	}
	if input != nil && input.growing != nil {
		return wb.getGrowingSourceSyncTask(ctx, segmentInfo, buffer, input)
	}
	return wb.getWriteBufferSyncTask(ctx, segmentInfo, buffer, input)
}

// getOrCreateGrowingSegmentBufferLocked wires a segment into growing-source
// (ref) mode: a segmentBuffer whose payload is a refPayload ledger, seeded
// from the last durable flush position. Caller must hold wb.mut and have
// decided FlushSourceGrowing.
func (wb *writeBufferBase) getOrCreateGrowingSegmentBufferLocked(segmentID int64) (*segmentBuffer, error) {
	if buffer, ok := wb.buffers[segmentID]; ok {
		return buffer, nil
	}
	segment, ok := wb.metaCache.GetSegmentByID(segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(segmentID)
	}
	if segment.GetStorageVersion() != storage.StorageV3 {
		return nil, merr.WrapErrServiceInternalMsg("growing-source flush requires StorageV3 segment, segmentID=%d storageVersion=%d",
			segmentID, segment.GetStorageVersion())
	}
	payload := newRefPayload(wb, segmentID)
	// Where this segment was last flushed to. On a fresh segment it is zero; on
	// one recovered mid-flush it comes from the position the last successful
	// flush persisted.
	lastFlushedPosition := segment.LastFlushPosition()
	if lastFlushedPosition != nil {
		payload.lastFlushedPosition = typeutil.Clone(lastFlushedPosition)
	}
	payload.lastFlushedTs = lastFlushedPosition.GetTimestamp()
	buffer := newSegmentBufferWithPayload(segmentID, payload)
	wb.buffers[segmentID] = buffer
	return buffer, nil
}

// bufferInsert buffers one segment's insert pack through its payload. The
// payload implementation — owned Go memory vs a growing-source ledger — is
// chosen once, at buffer creation, by the same sticky decision as before
// (decideGrowingFlushSource); after that both modes take the identical path.
func (wb *writeBufferBase) bufferInsert(inData *InsertData, startPos, endPos *msgpb.MsgPosition, schemaVersion int32) error {
	if err := wb.CreateNewGrowingSegment(CreateGrowingSegmentInfo{
		PartitionID:   inData.partitionID,
		SegmentID:     inData.segmentID,
		StartPos:      startPos,
		SchemaVersion: schemaVersion,
	}); err != nil {
		return err
	}
	buffer := wb.buffers[inData.segmentID]
	if buffer == nil {
		if wb.allowGrowingSourceFlush &&
			wb.decideGrowingFlushSource(inData.segmentID, endPos) == metacache.FlushSourceGrowing {
			var err error
			if buffer, err = wb.getOrCreateGrowingSegmentBufferLocked(inData.segmentID); err != nil {
				return err
			}
		} else {
			buffer = wb.getOrCreateBuffer(inData.segmentID, startPos.GetTimestamp())
		}
	}

	added, err := buffer.payload.Buffer(inData, startPos, endPos)
	if err != nil {
		return err
	}

	if _, ref := buffer.payload.(*refPayload); ref {
		// SetFlushSourceMode is sticky: only the first call commits the choice,
		// so we can include it unconditionally here without overriding a prior
		// FlushSourceWriteBuffer decision.
		wb.metaCache.UpdateSegments(metacache.MergeSegmentAction(
			metacache.SetStartPositionIfNil(startPos),
			metacache.SetFlushSourceMode(metacache.FlushSourceGrowing),
			metacache.UpdateBufferedRows(buffer.payload.UnflushedRows()),
		), metacache.WithSegmentIDs(inData.segmentID))
		wb.notifyFlushSourceMode(inData.segmentID)
	} else {
		wb.metaCache.UpdateSegments(metacache.MergeSegmentAction(
			metacache.UpdateBufferedRows(buffer.payload.UnflushedRows()),
			metacache.SetStartPositionIfNil(startPos),
		), metacache.WithSegmentIDs(inData.segmentID))
	}

	wb.addBufferMetric(added)
	return nil
}

// getEstBatchSize returns the batch size based on estimated size per record and FlushBufferSize configuration value.
func (wb *writeBufferBase) getEstBatchSize() uint {
	sizeLimit := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	return uint(sizeLimit / int64(wb.estSizePerRecord))
}

func (wb *writeBufferBase) waitDropRetry(ctx context.Context) error {
	interval := wb.retryInterval()
	if interval < 0 {
		interval = 0
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// syncDropSegment uses the same task construction, write path, retry
// classification, and per-segment ownership as normal syncing. Drop only adds
// the final task flag and waits synchronously so DropChannel cannot overtake an
// unfinished retry.
func (wb *writeBufferBase) syncDropSegment(ctx context.Context, cancel context.CancelFunc, segmentID int64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		futures, writeBufferEntry, err := wb.submitDropSegment(ctx, segmentID)
		if err != nil {
			if errors.Is(err, errGrowingSourceUnavailable) {
				if err := wb.waitDropRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}

		if writeBufferEntry != nil {
			return wb.waitSyncsSettled(ctx, cancel, []*writeBufferSyncEntry{writeBufferEntry})
		}
		if len(futures) == 0 {
			return nil
		}

		err = conc.BlockOnAll(futures...)
		if err == nil {
			// Drop debt is the metacache Dropped state plus a still-live ref
			// buffer: the drop task's commit deletes the buffer, so a surviving
			// one means the drop has not settled (e.g. a frozen-manifest replay
			// committed first) and must be re-driven.
			wb.mut.RLock()
			payload, ok := wb.refPayloadLocked(segmentID)
			dropPending := ok && payload.owesDropLocked()
			wb.mut.RUnlock()
			if dropPending {
				continue
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if syncmgr.ClassifySyncError(ctx, err) == syncmgr.SyncTerminal {
			return err
		}
		if err := wb.waitDropRetry(ctx); err != nil {
			return err
		}
	}
}

// abortDrop closes every local ownership path before Close propagates a Drop
// failure. Data was not committed, so the channel checkpoint must stay pinned for
// WAL replay. The write buffer has already been removed from its manager and
// cannot safely accept more work after this point.
func (wb *writeBufferBase) abortDrop(cancel context.CancelFunc) {
	cancel()
	wb.syncCancel()
	wb.mut.Lock()
	// First parked sweep: entries already waiting for a retry re-drive would
	// otherwise burn the whole grace below for nothing (their done never
	// closes) and leak their payload after it.
	parked := wb.takeParkedWriteBufferSyncsLocked()
	waiters := wb.allWriteBufferSyncEntriesLocked()
	wb.mut.Unlock()
	wb.abandonParkedWriteBufferSyncs(parked, context.Canceled)
	// Already canceled, so this enters waitSyncsSettled's bounded branch
	// immediately: completion callbacks stay the owner of payload and queue
	// cleanup for anything the grace leaves behind.
	abortCtx, abortCancel := context.WithCancel(context.Background())
	abortCancel()
	_ = wb.waitSyncsSettled(abortCtx, nil, waiters)

	wb.mut.Lock()
	// buffers (owned and ref payloads alike) are deliberately NOT cleared. They
	// are one of the two candidate sources GetCheckpoint pins on (the other is
	// the metacache seal pin); with both empty it falls back to wb.checkpoint —
	// the latest CONSUMED position — which is past data this abort just declared
	// un-committed. Clearing them contradicted the guarantee stated above, and
	// only stayed harmless because the caller had already removed this buffer
	// from the manager and was about to re-panic. Neither is enforced here.
	//
	// Nothing is leaked by keeping them: the whole write buffer is unreachable
	// after this and dies with its maps. The gauge is a separate concern and is
	// settled explicitly.
	wb.settleGrowingSourceFailureMetricLocked()
	wb.settleBufferMetric()
	wb.closed = true
	wb.dropping = false
	// Second parked sweep, in the same critical section that publishes closed:
	// an in-flight callback racing the first sweep may have parked its entry
	// for a retry that will now never come. Any callback after this section
	// sees closed and goes terminal on its own.
	parked = wb.takeParkedWriteBufferSyncsLocked()
	wb.mut.Unlock()
	wb.abandonParkedWriteBufferSyncs(parked, context.Canceled)
}

func (wb *writeBufferBase) Close(ctx context.Context, drop bool) {
	wb.mut.Lock()
	if !drop {
		wb.closed = true
		wb.syncCancel()
		// Entries parked for a retry re-drive have no driver once closed is
		// set; release them now or their payload is retained forever.
		parked := wb.takeParkedWriteBufferSyncsLocked()
		waiters := wb.allWriteBufferSyncEntriesLocked()
		wb.mut.Unlock()
		wb.abandonParkedWriteBufferSyncs(parked, context.Canceled)
		_ = wb.waitSyncsSettled(ctx, nil, waiters)
		// After the bounded wait, fence metric ownership before late callbacks
		// can publish against a channel that no longer exists.
		wb.mut.Lock()
		wb.settleGrowingSourceFailureMetricLocked()
		wb.mut.Unlock()
		wb.settleBufferMetric()
		return
	}
	dropCtx, dropCancel := context.WithCancel(ctx)
	defer dropCancel()
	wb.dropping = true
	writeBufferWaiters := wb.allWriteBufferSyncEntriesLocked()
	wb.mut.Unlock()
	defer func() {
		if panicValue := recover(); panicValue != nil {
			wb.mut.RLock()
			closed := wb.closed
			wb.mut.RUnlock()
			if !closed {
				wb.abortDrop(dropCancel)
			}
			panic(panicValue)
		}
	}()

	// Existing logical tasks own older batches and must reach terminal success
	// before any buffered tail can be turned into the final drop task.
	if err := wb.waitSyncsSettled(dropCtx, dropCancel, writeBufferWaiters); err != nil {
		mlog.Error(ctx, "failed to drain outstanding sync tasks while dropping write buffer", mlog.Err(err))
		panic(err)
	}

	wb.mut.Lock()
	dropSegmentIDs := lo.Keys(wb.buffers)
	wb.mut.Unlock()

	for _, id := range dropSegmentIDs {
		if err := wb.syncDropSegment(dropCtx, dropCancel, id); err != nil {
			mlog.Error(ctx, "failed to sync final drop segment",
				mlog.Int64("segmentID", id),
				mlog.String("channel", wb.channelName),
				mlog.Err(err))
			panic(err)
		}
	}
	if err := dropCtx.Err(); err != nil {
		mlog.Error(ctx, "drop context canceled before channel metadata commit", mlog.Err(err))
		panic(err)
	}
	err := wb.metaWriter.DropChannel(dropCtx, wb.channelName)
	if err != nil {
		mlog.Error(ctx, "failed to drop channel", mlog.Err(err))
		// TODO change to remove channel in the future
		panic(err)
	}
	wb.mut.Lock()
	wb.closed = true
	wb.dropping = false
	wb.syncCancel()
	wb.settleGrowingSourceFailureMetricLocked()
	wb.settleBufferMetric()
	wb.mut.Unlock()
}

// prepareInsert transfers InsertMsg into organized InsertData grouped by segmentID
// also returns primary key field data
func PrepareInsert(collSchema *schemapb.CollectionSchema, pkField *schemapb.FieldSchema, insertMsgs []*msgstream.InsertMsg) ([]*InsertData, error) {
	bm25OutputFieldIDs, err := getBM25OutputFieldIDs(collSchema)
	if err != nil {
		return nil, err
	}

	groups := lo.GroupBy(insertMsgs, func(msg *msgstream.InsertMsg) int64 { return msg.SegmentID })
	segmentPartition := lo.SliceToMap(insertMsgs, func(msg *msgstream.InsertMsg) (int64, int64) { return msg.GetSegmentID(), msg.GetPartitionID() })

	result := make([]*InsertData, 0, len(groups))
	for segment, msgs := range groups {
		inData := &InsertData{
			segmentID:   segment,
			partitionID: segmentPartition[segment],
			data:        make([]*storage.InsertData, 0, len(msgs)),
			pkField:     make([]storage.FieldData, 0, len(msgs)),
		}

		for _, msg := range msgs {
			data, err := storage.InsertMsgToInsertData(msg, collSchema)
			if err != nil {
				mlog.Warn(context.TODO(), "failed to transfer insert msg to insert data", mlog.Err(err))
				return nil, err
			}

			if len(bm25OutputFieldIDs) > 0 {
				if inData.bm25Stats == nil {
					inData.bm25Stats = make(map[int64]*storage.BM25Stats)
				}
				if err := appendBM25StatsFromInsertData(inData.bm25Stats, bm25OutputFieldIDs, data); err != nil {
					return nil, err
				}
			}

			pkFieldData, err := storage.GetPkFromInsertData(collSchema, data)
			if err != nil {
				return nil, err
			}
			if pkFieldData.RowNum() != data.GetRowNum() {
				return nil, merr.WrapErrServiceInternal("pk column row num not match")
			}

			tsFieldData, err := storage.GetTimestampFromInsertData(data)
			if err != nil {
				return nil, err
			}
			if tsFieldData.RowNum() != data.GetRowNum() {
				return nil, merr.WrapErrServiceInternal("timestamp column row num not match")
			}

			inData.data = append(inData.data, data)
			inData.pkField = append(inData.pkField, pkFieldData)
			inData.tsField = append(inData.tsField, tsFieldData)
			inData.rowNum += int64(data.GetRowNum())
		}
		result = append(result, inData)
	}

	return result, nil
}

func getBM25OutputFieldIDs(schema *schemapb.CollectionSchema) ([]int64, error) {
	outputFieldIDs := make([]int64, 0)
	for _, fn := range schema.GetFunctions() {
		if fn.GetType() != schemapb.FunctionType_BM25 {
			continue
		}

		outputField := typeutil.GetFunctionOutputField(schema, fn)
		if outputField == nil {
			return nil, merr.WrapErrFunctionFailedMsg("function %s output field not found", fn.GetName())
		}

		outputFieldIDs = append(outputFieldIDs, outputField.GetFieldID())
	}
	return outputFieldIDs, nil
}

func appendBM25StatsFromInsertData(stats map[int64]*storage.BM25Stats, outputFieldIDs []int64, data *storage.InsertData) error {
	for _, outputFieldID := range outputFieldIDs {
		outputData, ok := data.Data[outputFieldID]
		if !ok {
			return merr.WrapErrFunctionFailedMsg("BM25 output field %d not found in insert data", outputFieldID)
		}

		sparseData, ok := outputData.(*storage.SparseFloatVectorFieldData)
		if !ok {
			return merr.WrapErrFunctionFailedMsg("BM25 output field %d is not sparse vector data", outputFieldID)
		}

		if _, ok := stats[outputFieldID]; !ok {
			stats[outputFieldID] = storage.NewBM25Stats()
		}
		stats[outputFieldID].AppendBytes(sparseData.GetContents()...)
	}
	return nil
}
