package writebuffer

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
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
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	nonFlushTS uint64 = 0
)

const defaultGrowingSourceRetryInterval = 100 * time.Millisecond

const growingSourceSyncFailureWarnThreshold = 600

var errGrowingSourceUnavailable = errors.New("growing source is unavailable")

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
	// EvictBuffer evicts buffer to sync manager which match provided sync policies.
	EvictBuffer(policies ...SyncPolicy)
	// AllowGrowingSourceFlush returns true if this write buffer may try growing-source flush.
	AllowGrowingSourceFlush() bool
	// GetGrowingFlushProgress returns growing-source progress for the given
	// segments after this write buffer has processed up to fenceTs. If segmentIDs
	// is empty, all tracked growing-source segments are returned. Otherwise,
	// tracked growing-source segments are added to the requested segmentIDs.
	GetGrowingFlushProgress(ctx context.Context, segmentIDs []int64, fenceTs uint64) ([]GrowingFlushSegmentProgress, error)
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
	SegmentID          int64
	TargetOffset       int64
	NeedReleaseHandoff bool
	SourceMode         metacache.FlushSourceMode
}

type checkpointCandidate struct {
	segmentID int64
	position  *msgpb.MsgPosition
	source    string
}

type checkpointCandidates struct {
	candidates *typeutil.ConcurrentMap[string, *checkpointCandidate]
}

type growingSourceProgress struct {
	segmentID           int64
	targetOffset        int64
	syncingOffset       int64
	syncing             bool
	pendingFlush        bool
	pendingCommitted    *growingSourcePendingCommittedFlush
	nonRetryableFailure bool
	batches             []growingSourceProgressBatch
	failureCount        int64
	lastFailure         string
}

type growingSourcePendingCommittedFlush struct {
	targetOffset  int64
	manifestPath  string
	bm25Stats     map[int64]*storage.BM25Stats
	insertBinlogs map[int64]*datapb.FieldBinlog
	pkStats       *storage.PrimaryKeyStats
}

// growingFlushSourceDecision is the in-memory result of decideGrowingFlushSource.
// sourceType reuses metacache.FlushSourceMode so that the writeBuffer and
// the metacache share a single concept of which subsystem owns the segment's
// payload at flush time. sourceType is always FlushSourceWriteBuffer or
// FlushSourceGrowing here (never Unknown).
type growingFlushSourceDecision struct {
	sourceType  metacache.FlushSourceMode
	sourceState syncmgr.GrowingSourceState
}

type growingSourceProgressBatch struct {
	startPosition *msgpb.MsgPosition
	endPosition   *msgpb.MsgPosition
	endOffset     int64
	rowNum        int64
}

func (p *growingSourceProgress) firstUncommittedPosition() *msgpb.MsgPosition {
	if len(p.batches) == 0 {
		return nil
	}
	return p.batches[0].startPosition
}

func (p *growingSourceProgress) checkpointFor(offset int64) *msgpb.MsgPosition {
	var checkpoint *msgpb.MsgPosition
	for _, batch := range p.batches {
		if batch.endOffset <= offset {
			checkpoint = batch.endPosition
			continue
		}
		break
	}
	return checkpoint
}

func (p *growingSourceProgress) ack(offset int64) {
	keepIdx := 0
	for keepIdx < len(p.batches) && p.batches[keepIdx].endOffset <= offset {
		keepIdx++
	}
	p.batches = p.batches[keepIdx:]
	if p.pendingCommitted != nil && offset >= p.pendingCommitted.targetOffset {
		p.pendingCommitted = nil
	}
	p.syncing = false
	p.syncingOffset = 0
	p.failureCount = 0
	p.lastFailure = ""
}

func (p *growingSourceProgress) failSync(err error) {
	p.syncing = false
	p.syncingOffset = 0
	p.failureCount++
	if err != nil {
		p.lastFailure = err.Error()
	}
}

func (p *growingSourceProgress) markNonRetryableFailure() {
	p.nonRetryableFailure = true
}

func isGrowingSourceLayoutMismatch(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Column count mismatch") ||
		strings.Contains(msg, "Column group size mismatch")
}

// growingSourceSyncFatal reports why a growing-source sync failure can never
// succeed on retry, or "" when the failure is worth retrying.
//
// Retry is the default. A growing-source flush re-reads the same offset range
// from the segcore growing segment, so retrying is cheap and loses nothing;
// only failures whose cause cannot change between attempts belong here.
//
// Storage-layer permanence is deliberately NOT consulted here. Today every
// loon FFI failure is wrapped as packed.ErrLoonTransient regardless of its
// real cause, so asking would classify a dead bucket as retryable and a
// throttle as fatal with equal confidence — worse than not asking. Retrying
// forever is the safe side of that ignorance: the rows stay pinned in the
// growing segment and the checkpoint stays put. Once the storage error
// classification lands, add the permanent branch here.
func growingSourceSyncFatal(err error) string {
	switch {
	case err == nil:
		return ""
	case isGrowingSourceLayoutMismatch(err):
		// The flush layout disagrees with what the segment materialized.
		// Re-reading the same rows reproduces it exactly.
		return "layout mismatch"
	case errors.Is(err, merr.ErrDataIntegrity):
		// The task refused its own inputs: empty manifest, row-count mismatch,
		// missing insert summary, a non-V3 segment, a target offset behind
		// already-flushed rows. Every one of these is re-derived from the same
		// segment state and the same offset range, so a retry reproduces it
		// exactly -- and retrying forever would pin the channel checkpoint with
		// nothing but rate-limited warnings to show for it.
		//
		// ErrServiceInternal is deliberately NOT here: on this path it means
		// "the source is not ready yet" (nil source, source behind the target
		// offset), which the next round genuinely can resolve.
		return "data integrity violation"
	case merr.IsNonRetryableErr(err):
		// Milvus-side terminal error: the request is malformed or the target
		// is gone. A restart can at least pick up corrected configuration;
		// retrying in-process cannot.
		return "non-retryable error"
	default:
		return ""
	}
}

func cloneBM25StatsMap(stats map[int64]*storage.BM25Stats) map[int64]*storage.BM25Stats {
	if len(stats) == 0 {
		return nil
	}
	cloned := make(map[int64]*storage.BM25Stats, len(stats))
	for fieldID, stat := range stats {
		if stat != nil {
			cloned[fieldID] = stat.Clone()
		}
	}
	return cloned
}

func getCandidatesKey(segmentID int64, timestamp uint64) string {
	return fmt.Sprintf("%d-%d", segmentID, timestamp)
}

func newCheckpointCandiates() *checkpointCandidates {
	return &checkpointCandidates{
		candidates: typeutil.NewConcurrentMap[string, *checkpointCandidate](), // segmentID-ts
	}
}

func (c *checkpointCandidates) Remove(segmentID int64, timestamp uint64) {
	c.candidates.Remove(getCandidatesKey(segmentID, timestamp))
}

func (c *checkpointCandidates) Add(segmentID int64, position *msgpb.MsgPosition, source string) {
	c.candidates.Insert(getCandidatesKey(segmentID, position.GetTimestamp()), &checkpointCandidate{segmentID, position, source})
}

func (c *checkpointCandidates) GetEarliestWithDefault(def *checkpointCandidate) *checkpointCandidate {
	result := def
	c.candidates.Range(func(_ string, candidate *checkpointCandidate) bool {
		if result == nil || candidate.position.GetTimestamp() < result.position.GetTimestamp() {
			result = candidate
		}
		return true
	})
	return result
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

	syncPolicies   []SyncPolicy
	syncCheckpoint *checkpointCandidates
	syncMgr        syncmgr.SyncManager

	checkpoint     *msgpb.MsgPosition
	processedTs    uint64
	flushTimestamp *atomic.Uint64
	syncCtx        context.Context
	syncCancel     context.CancelFunc

	// errHandler is fatal: it is used for sync tasks whose payload was yielded
	// out of the buffer and that have no re-submit path, so the only safe
	// recovery is process restart plus WAL replay from the (unadvanced)
	// checkpoint.
	errHandler func(err error)
	// growingSourceErrHandler is non-fatal: growing-source syncs read from the
	// segcore growing segment and are re-submitted by
	// scheduleGrowingSourceRetryLocked, so a failed attempt loses nothing.
	growingSourceErrHandler func(err error)
	taskObserverCallback    func(t syncmgr.Task, err error) // execute when a sync task finished, should be concurrent safe.

	// Channel-level admission flag for trying growing-source flush. Actual segment
	// source selection remains sticky in metacache.
	allowGrowingSourceFlush bool

	growingSourceResolver GrowingSourceResolver

	// growingSourceProgress tracks per-segment progress for segments backed by
	// an external growing source (FlushSourceGrowing). The sticky source
	// decision itself lives in metacache.SegmentInfo.flushSourceMode
	growingSourceProgress       map[int64]*growingSourceProgress
	growingSourceRetryInterval  time.Duration
	growingSourceRetryScheduled bool
	growingSourceRetryTimer     *time.Timer

	// ordinarySyncs owns every ordinary logical sync from the moment its
	// payload leaves wb.buffers until final success or a true abort. Presence in
	// this map is the per-segment gate: queued/running/retry-wait all prevent a
	// second pack from being built for the same segment.
	ordinarySyncs           map[int64]*ordinarySyncState
	ordinaryRetryAfterFunc  retryAfterFunc
	flushSourceModeNotifier FlushSourceModeNotifier
	dropping                bool
	closed                  bool

	// pre build logger
	logger                   *mlog.Logger
	cpRatedLogger            *mlog.Logger
	growingSourceRatedLogger *mlog.Logger
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
	growingSourceResolver := option.growingSourceResolver
	if growingSourceResolver == nil {
		// No custom resolver means use the process-local growing source registry.
		// If registry lookup misses, growing-source data falls back to WriteBuffer.
		growingSourceResolver = func(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
			return syncmgr.DefaultGrowingSourceRegistry().Resolve(channel, segmentID, targetOffset, endPos)
		}
	}
	growingSourceRetryInterval := option.growingSourceRetryInterval
	if growingSourceRetryInterval == 0 {
		growingSourceRetryInterval = defaultGrowingSourceRetryInterval
	}
	wb := &writeBufferBase{
		channelName:                channel,
		collectionID:               metacache.Collection(),
		estSizePerRecord:           estSize,
		syncMgr:                    syncMgr,
		metaWriter:                 option.metaWriter,
		allocator:                  option.idAllocator,
		buffers:                    make(map[int64]*segmentBuffer),
		metaCache:                  metacache,
		syncCheckpoint:             newCheckpointCandiates(),
		syncPolicies:               option.syncPolicies,
		flushTimestamp:             flushTs,
		syncCtx:                    syncCtx,
		syncCancel:                 syncCancel,
		errHandler:                 option.errorHandler,
		growingSourceErrHandler:    option.growingSourceErrorHandler,
		taskObserverCallback:       option.taskObserverCallback,
		allowGrowingSourceFlush:    allowGrowingSourceFlush,
		growingSourceResolver:      growingSourceResolver,
		growingSourceProgress:      make(map[int64]*growingSourceProgress),
		growingSourceRetryInterval: growingSourceRetryInterval,
		ordinarySyncs:              make(map[int64]*ordinarySyncState),
		ordinaryRetryAfterFunc: func(delay time.Duration, f func()) retryTimer {
			return time.AfterFunc(delay, f)
		},
		flushSourceModeNotifier: option.flushSourceModeNotifier,
	}

	wb.logger = mlog.With(mlog.Int64("collectionID", wb.collectionID),
		mlog.String("channel", wb.channelName))
	wb.cpRatedLogger = wb.logger
	wb.growingSourceRatedLogger = wb.logger
	if wb.errHandler == nil {
		wb.errHandler = func(err error) {
			panic(err)
		}
	}

	// A nil handler would silently drop failure reporting for a path that is
	// expected to fail and retry, so never leave it unset even when the option
	// struct was built directly (tests, embedded callers). Rate-limited on
	// purpose: the retry interval is 100ms, so an unrated warn here turns one
	// stuck segment into a log flood. The per-failure counter and the escalating
	// summary live in observeGrowingSourceSyncFailureLocked.
	if wb.growingSourceErrHandler == nil {
		wb.growingSourceErrHandler = func(err error) {
			wb.growingSourceRatedLogger.RatedWarn(context.TODO(), rate.Limit(1),
				"growing-source sync failed, will retry", mlog.Err(err))
		}
	}

	return wb, nil
}

func (wb *writeBufferBase) updateProcessedTsLocked(ts uint64) {
	if ts > wb.processedTs {
		wb.processedTs = ts
	}
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
	wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Sealed),
		metacache.WithSegmentState(commonpb.SegmentState_Growing))
	for _, progress := range wb.growingSourceProgress {
		progress.pendingFlush = true
	}
}

func (wb *writeBufferBase) DropPartitions(partitionIDs []int64) {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	wb.dropPartitions(partitionIDs)
}

func (wb *writeBufferBase) SetFlushTimestamp(flushTs uint64) {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	wb.flushTimestamp.Store(flushTs)
	wb.updateProcessedTsLocked(flushTs)
}

func (wb *writeBufferBase) GetFlushTimestamp() uint64 {
	return wb.flushTimestamp.Load()
}

func (wb *writeBufferBase) AllowGrowingSourceFlush() bool {
	return wb.allowGrowingSourceFlush
}

func (wb *writeBufferBase) CheckReleaseManualFlushNeed(segmentIDs []int64) bool {
	if len(segmentIDs) == 0 {
		return false
	}

	wb.mut.RLock()
	defer wb.mut.RUnlock()

	for _, segmentID := range segmentIDs {
		segment, ok := wb.metaCache.GetSegmentByID(segmentID)
		if !ok {
			return true
		}

		switch segment.FlushSourceMode() {
		case metacache.FlushSourceWriteBuffer:
			continue
		case metacache.FlushSourceGrowing:
			if segment.State() == commonpb.SegmentState_Flushed {
				continue
			}
			return true
		default:
			return true
		}
	}
	return false
}

func (wb *writeBufferBase) GetGrowingFlushProgress(ctx context.Context, segmentIDs []int64, fenceTs uint64) ([]GrowingFlushSegmentProgress, error) {
	if err := wb.waitProcessed(ctx, fenceTs); err != nil {
		return nil, err
	}

	wb.mut.RLock()
	if len(segmentIDs) == 0 {
		segmentIDs = lo.Keys(wb.growingSourceProgress)
	} else {
		segmentIDs = lo.Uniq(append(segmentIDs, lo.Keys(wb.growingSourceProgress)...))
	}

	progresses := make([]GrowingFlushSegmentProgress, 0, len(segmentIDs))
	releaseSegments := make([]syncmgr.GrowingSourceReleaseHandoffSegment, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		progress := GrowingFlushSegmentProgress{
			SegmentID:  segmentID,
			SourceMode: metacache.FlushSourceUnknown,
		}
		if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok {
			progress.SourceMode = segment.FlushSourceMode()
		}
		if growingProgress, ok := wb.growingSourceProgress[segmentID]; ok {
			progress.TargetOffset = growingProgress.targetOffset
			progress.NeedReleaseHandoff = wb.growingProgressRequiresHandoff(segmentID, growingProgress)
			progress.SourceMode = metacache.FlushSourceGrowing
		}
		if progress.NeedReleaseHandoff {
			releaseSegments = append(releaseSegments, syncmgr.GrowingSourceReleaseHandoffSegment{
				SegmentID:    segmentID,
				TargetOffset: progress.TargetOffset,
			})
		}
		progresses = append(progresses, progress)
	}
	wb.mut.RUnlock()

	if len(releaseSegments) > 0 {
		if err := syncmgr.DefaultGrowingSourceRegistry().PrepareGrowingSourceReleaseHandoff(ctx, wb.channelName, fenceTs, releaseSegments); err != nil {
			return nil, err
		}
	}
	return progresses, nil
}

func (wb *writeBufferBase) growingProgressRequiresHandoff(segmentID int64, progress *growingSourceProgress) bool {
	if progress == nil {
		return false
	}
	if len(progress.batches) > 0 {
		return true
	}
	segment, ok := wb.metaCache.GetSegmentByID(segmentID)
	if !ok {
		return false
	}
	return segment.FlushSourceMode() == metacache.FlushSourceGrowing &&
		segment.State() != commonpb.SegmentState_Flushed
}

func (wb *writeBufferBase) waitProcessed(ctx context.Context, fenceTs uint64) error {
	if fenceTs == 0 {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		wb.mut.RLock()
		processed := wb.processedTs
		closed := wb.closed
		wb.mut.RUnlock()
		if processed >= fenceTs {
			return nil
		}
		if closed {
			return merr.WrapErrChannelNotFound(wb.channelName)
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

	return wb.totalBufferedMemorySizeLocked() + wb.totalOrdinaryPayloadMemorySizeLocked()
}

func (wb *writeBufferBase) totalBufferedMemorySizeLocked() int64 {
	var size int64
	for _, segBuf := range wb.buffers {
		size += segBuf.MemorySize()
	}
	return size
}

func (wb *writeBufferBase) totalOrdinaryPayloadMemorySizeLocked() int64 {
	var size int64
	for _, state := range wb.ordinarySyncs {
		size += state.task.PayloadBytes()
	}
	return size
}

// EvictableMemorySize is the buffered subset that can be yielded immediately.
// Retained ordinary payload is resident memory, but selecting it for eviction
// would only spin while the existing logical owner is still active.
func (wb *writeBufferBase) EvictableMemorySize() int64 {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	var size int64
	for segmentID, buffer := range wb.buffers {
		if _, owned := wb.ordinarySyncs[segmentID]; owned {
			continue
		}
		if progress, ok := wb.growingSourceProgress[segmentID]; ok && progress.syncing {
			continue
		}
		size += buffer.MemorySize()
	}
	return size
}

func (wb *writeBufferBase) flushCapacityWaiterLocked() <-chan struct{} {
	insertLimit := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	deleteLimit := paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64()
	for segmentID, state := range wb.ordinarySyncs {
		insertBytes := state.task.InsertPayloadBytes()
		deleteBytes := state.task.DeletePayloadBytes()
		if buffer := wb.buffers[segmentID]; buffer != nil {
			if buffer.insertBuffer != nil {
				insertBytes += buffer.insertBuffer.size
			}
			if buffer.deltaBuffer != nil {
				deleteBytes += buffer.deltaBuffer.size
			}
		}
		insertFull := insertBytes > 0 && insertLimit != noLimit && insertBytes >= insertLimit
		deleteFull := deleteBytes > 0 && deleteLimit != noLimit && deleteBytes >= deleteLimit
		if !insertFull && !deleteFull {
			continue
		}
		if state.task.PayloadBytes() > 0 && state.payloadReleased != nil {
			return state.payloadReleased
		}
		return state.done
	}
	return nil
}

// waitFlushCapacity applies per-segment backpressure using the existing flush
// thresholds. The triggering batch is submitted before this wait, so a batch
// larger than the threshold can still make progress and release its payload.
func (wb *writeBufferBase) waitFlushCapacity() error {
	for {
		wb.mut.RLock()
		if wb.closed || wb.dropping {
			wb.mut.RUnlock()
			return merr.WrapErrChannelNotFound(wb.channelName)
		}
		waiter := wb.flushCapacityWaiterLocked()
		wb.mut.RUnlock()
		if waiter == nil {
			return nil
		}

		select {
		case <-waiter:
		case <-wb.syncCtx.Done():
			return merr.WrapErrChannelNotFound(wb.channelName)
		}
	}
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

func (wb *writeBufferBase) GetCheckpoint() *msgpb.MsgPosition {
	logger := wb.cpRatedLogger
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	candidates := lo.MapToSlice(wb.buffers, func(_ int64, buf *segmentBuffer) *checkpointCandidate {
		return &checkpointCandidate{buf.segmentID, buf.EarliestPosition(), "segment buffer"}
	})
	candidates = lo.Filter(candidates, func(candidate *checkpointCandidate, _ int) bool {
		return candidate.position != nil
	})
	for _, progress := range wb.growingSourceProgress {
		if position := progress.firstUncommittedPosition(); position != nil {
			candidates = append(candidates, &checkpointCandidate{
				segmentID: progress.segmentID,
				position:  position,
				source:    "growing-source progress",
			})
		}
	}

	checkpoint := wb.syncCheckpoint.GetEarliestWithDefault(lo.MinBy(candidates, func(a, b *checkpointCandidate) bool {
		return a.position.GetTimestamp() < b.position.GetTimestamp()
	}))

	if checkpoint == nil {
		// all buffer are empty
		logger.RatedDebug(context.TODO(), rate.Limit(60), "checkpoint from latest consumed msg", mlog.Uint64("cpTimestamp", wb.checkpoint.GetTimestamp()))
		return wb.checkpoint
	}

	logger.RatedDebug(context.TODO(), rate.Limit(20), "checkpoint evaluated",
		mlog.String("cpSource", checkpoint.source),
		mlog.FieldSegmentID(checkpoint.segmentID),
		mlog.Uint64("cpTimestamp", checkpoint.position.GetTimestamp()))
	return checkpoint.position
}

func (wb *writeBufferBase) hasWriteBufferInsertPayload(segmentID int64) bool {
	buffer, ok := wb.buffers[segmentID]
	return ok && buffer.insertBuffer != nil && !buffer.insertBuffer.IsEmpty()
}

func (wb *writeBufferBase) hasGrowingSourceProgress(segmentID int64) bool {
	_, ok := wb.growingSourceProgress[segmentID]
	return ok
}

func (wb *writeBufferBase) decideGrowingFlushSource(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) growingFlushSourceDecision {
	// 1. Honor the sticky decision recorded in metacache. Once the first
	//    insert for a segment commits a source choice, every subsequent call
	//    must return the same kind so that progress / payload tracking stays
	//    consistent for the segment's lifetime.
	if seg, ok := wb.metaCache.GetSegmentByID(segmentID); ok {
		if seg.GetStorageVersion() != storage.StorageV3 {
			return growingFlushSourceDecision{sourceType: metacache.FlushSourceWriteBuffer}
		}
		switch seg.FlushSourceMode() {
		case metacache.FlushSourceGrowing:
			state := wb.getGrowingSourceState(segmentID, targetOffset, endPos)
			return growingFlushSourceDecision{
				sourceType:  metacache.FlushSourceGrowing,
				sourceState: state,
			}
		case metacache.FlushSourceWriteBuffer:
			return growingFlushSourceDecision{sourceType: metacache.FlushSourceWriteBuffer}
		}
	}

	// 2. Fallback for the brief window where in-memory bookkeeping has been
	//    populated but the metacache sticky bit hasn't been set yet (e.g. on
	//    re-entry after a partial state).
	if wb.hasGrowingSourceProgress(segmentID) {
		state := wb.getGrowingSourceState(segmentID, targetOffset, endPos)
		return growingFlushSourceDecision{
			sourceType:  metacache.FlushSourceGrowing,
			sourceState: state,
		}
	}

	if wb.hasWriteBufferInsertPayload(segmentID) {
		return growingFlushSourceDecision{sourceType: metacache.FlushSourceWriteBuffer}
	}

	state := wb.getGrowingSourceState(segmentID, targetOffset, endPos)
	if state == syncmgr.GrowingSourceUsable || state == syncmgr.GrowingSourcePending {
		return growingFlushSourceDecision{
			sourceType:  metacache.FlushSourceGrowing,
			sourceState: state,
		}
	}
	wb.warnGrowingSourceFallback(segmentID, targetOffset, endPos)
	return growingFlushSourceDecision{sourceType: metacache.FlushSourceWriteBuffer}
}

func (wb *writeBufferBase) getGrowingSource(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	if wb.growingSourceResolver == nil {
		return nil, syncmgr.GrowingSourceUnavailable
	}
	return wb.growingSourceResolver(segmentID, targetOffset, endPos)
}

func (wb *writeBufferBase) getGrowingSourceState(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) syncmgr.GrowingSourceState {
	source, state := wb.getGrowingSource(segmentID, targetOffset, endPos)
	if source != nil {
		source.Release()
	}
	return state
}

func (wb *writeBufferBase) warnGrowingSourceFallback(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) {
	if !wb.allowGrowingSourceFlush {
		return
	}
	wb.growingSourceRatedLogger.RatedWarn(context.TODO(), rate.Limit(1), "growing-source source is unavailable, fallback to WriteBuffer",
		mlog.Int64("segmentID", segmentID),
		mlog.Int64("targetOffset", targetOffset),
		mlog.Any("endPosition", endPos),
	)
}

func (wb *writeBufferBase) growingSourceProgressSyncable(segmentID int64, progress *growingSourceProgress, rollbackFlushing bool, markSealedFlushing bool) (bool, bool) {
	if progress.nonRetryableFailure {
		return false, false
	}
	if progress.syncing {
		if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok &&
			(segment.State() == commonpb.SegmentState_Sealed || segment.State() == commonpb.SegmentState_Flushing) {
			progress.pendingFlush = true
		}
		return false, false
	}
	if progress.pendingCommitted != nil {
		if markSealedFlushing {
			if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok && segment.State() == commonpb.SegmentState_Sealed {
				wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing), metacache.WithSegmentIDs(segmentID))
			}
		}
		return true, false
	}
	if len(progress.batches) == 0 && !progress.pendingFlush {
		return false, false
	}
	if len(progress.batches) == 0 {
		segment, ok := wb.metaCache.GetSegmentByID(segmentID)
		if !ok || (segment.State() != commonpb.SegmentState_Sealed && segment.State() != commonpb.SegmentState_Flushing) {
			return false, false
		}
	}
	checkpoint := wb.checkpoint
	if len(progress.batches) > 0 {
		checkpoint = progress.batches[len(progress.batches)-1].endPosition
	}
	if checkpoint == nil {
		return false, false
	}
	state := wb.getGrowingSourceState(segmentID, progress.targetOffset, checkpoint)
	if state == syncmgr.GrowingSourceUsable {
		if markSealedFlushing {
			if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok && segment.State() == commonpb.SegmentState_Sealed {
				wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing), metacache.WithSegmentIDs(segmentID))
			}
		}
		return true, false
	}

	// GetSealedSegmentsPolicy moves Sealed -> Flushing before returning the
	// candidate. If the growing source is only pending, roll it back so the
	// sealed segment can be selected again when the source catches up.
	if rollbackFlushing {
		if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok && segment.State() == commonpb.SegmentState_Flushing {
			wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Sealed), metacache.WithSegmentIDs(segmentID))
		}
	}
	return false, true
}

// ordinarySyncFatal reports why an ordinary sync failure can never succeed on
// retry, or "" when it is worth re-submitting. Same split as the
// growing-source path: data-integrity violations are re-derived from the same
// batch and would pin the checkpoint forever, everything else gets another go.
func ordinarySyncFatal(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, merr.ErrDataIntegrity):
		return "data integrity violation"
	case merr.IsNonRetryableErr(err):
		return "non-retryable error"
	default:
		return ""
	}
}

func (wb *writeBufferBase) scheduleGrowingSourceRetryLocked() {
	if wb.closed || wb.dropping || wb.growingSourceRetryScheduled || wb.growingSourceRetryInterval < 0 || len(wb.growingSourceProgress) == 0 {
		return
	}
	wb.growingSourceRetryScheduled = true
	interval := wb.growingSourceRetryInterval
	wb.growingSourceRetryTimer = time.AfterFunc(interval, wb.retryGrowingSourceProgress)
}

func (wb *writeBufferBase) retryGrowingSourceProgress() {
	wb.mut.Lock()
	wb.growingSourceRetryScheduled = false
	wb.growingSourceRetryTimer = nil
	if wb.closed || wb.dropping || wb.checkpoint == nil || len(wb.growingSourceProgress) == 0 {
		wb.mut.Unlock()
		return
	}

	segmentIDs, retryNeeded := wb.getGrowingSourceSegmentsToRetry()
	if retryNeeded {
		wb.scheduleGrowingSourceRetryLocked()
	}

	var syncTasks []syncmgr.Task
	if len(segmentIDs) > 0 {
		wb.logger.Info(context.TODO(), "retry growing-source source sync", mlog.Int64s("segmentIDs", segmentIDs))
		syncTasks = wb.getSyncTasksLocked(wb.syncCtx, segmentIDs)
	}
	wb.mut.Unlock()

	if len(syncTasks) > 0 {
		futures := wb.submitSyncTasks(wb.syncCtx, syncTasks)
		if len(futures) > 0 {
			conc.AwaitAll(futures...)
		}
	}
}

// getGrowingSourceSegmentsToRetry returns syncable growing-source progress segments. If a
// sealed segment becomes usable during retry, it is moved to Flushing before
// sync so GrowingSourceSyncTask commits it as a flushed segment.
// **NOTE** shall be invoked within mutex protection
func (wb *writeBufferBase) getGrowingSourceSegmentsToRetry() ([]int64, bool) {
	segments := make([]int64, 0, len(wb.growingSourceProgress))
	retryNeeded := false
	for segmentID, progress := range wb.growingSourceProgress {
		syncable, retry := wb.growingSourceProgressSyncable(segmentID, progress, false, true)
		retryNeeded = retryNeeded || retry
		if syncable {
			segments = append(segments, segmentID)
		}
	}
	return segments, retryNeeded
}

func (wb *writeBufferBase) recordGrowingSourceProgress(inData *InsertData, startPos, endPos *msgpb.MsgPosition, schemaVersion int32, targetOffset int64) error {
	err := wb.CreateNewGrowingSegment(CreateGrowingSegmentInfo{
		PartitionID:   inData.partitionID,
		SegmentID:     inData.segmentID,
		StartPos:      startPos,
		SchemaVersion: schemaVersion,
	})
	if err != nil {
		return err
	}
	segment, ok := wb.metaCache.GetSegmentByID(inData.segmentID)
	if !ok {
		return merr.WrapErrSegmentNotFound(inData.segmentID)
	}
	if segment.GetStorageVersion() != storage.StorageV3 {
		return merr.WrapErrServiceInternalMsg("growing-source flush requires StorageV3 segment, segmentID=%d storageVersion=%d",
			inData.segmentID, segment.GetStorageVersion())
	}
	progress, ok := wb.growingSourceProgress[inData.segmentID]
	if !ok {
		progress = &growingSourceProgress{
			segmentID:    inData.segmentID,
			targetOffset: targetOffset - inData.rowNum,
		}
		wb.growingSourceProgress[inData.segmentID] = progress
	}
	progress.targetOffset += inData.rowNum
	progress.batches = append(progress.batches, growingSourceProgressBatch{
		startPosition: startPos,
		endPosition:   endPos,
		endOffset:     progress.targetOffset,
		rowNum:        inData.rowNum,
	})
	// SetFlushSourceMode is sticky: only the first call commits the choice,
	// so we can include it unconditionally here without overriding a prior
	// FlushSourceWriteBuffer decision.
	wb.metaCache.UpdateSegments(metacache.SegmentActions(
		metacache.SetStartPositionIfNil(startPos),
		metacache.SetFlushSourceMode(metacache.FlushSourceGrowing),
		wb.updateGrowingSourceBufferedRows(progress),
	), metacache.WithSegmentIDs(inData.segmentID))
	wb.notifyFlushSourceMode(inData.segmentID)
	return nil
}

func (wb *writeBufferBase) growingSourceTargetOffset(segmentID int64, rows int64) int64 {
	return wb.growingSourceBaseOffset(segmentID) + rows
}

func (wb *writeBufferBase) growingSourceBaseOffset(segmentID int64) int64 {
	if progress, ok := wb.growingSourceProgress[segmentID]; ok {
		return progress.targetOffset
	}
	if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok {
		return segment.NumOfRows()
	}
	return 0
}

func (wb *writeBufferBase) updateGrowingSourceBufferedRows(progress *growingSourceProgress) metacache.SegmentAction {
	return func(info *metacache.SegmentInfo) {
		bufferedRows := progress.targetOffset - info.FlushedRows() - info.SyncingRows()
		if bufferedRows < 0 {
			bufferedRows = 0
		}
		metacache.UpdateBufferedRows(bufferedRows)(info)
	}
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
		if progress, ok := wb.growingSourceProgress[segmentID]; ok {
			progress.pendingFlush = true
		}
	}
	// mark segment flushing if segment was growing
	if len(existingIDs) > 0 {
		wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Sealed),
			metacache.WithSegmentIDs(existingIDs...),
			metacache.WithSegmentState(commonpb.SegmentState_Growing))
	}
	return nil
}

func (wb *writeBufferBase) sealAllSegments(ctx context.Context) error {
	allSegmentIds := wb.metaCache.GetSegmentIDsBy()
	mlog.Info(ctx, "seal all segments", mlog.Int64s("segmentIDs", allSegmentIds))
	// mark segment flushing if segment was growing
	wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Sealed),
		metacache.WithSegmentIDs(allSegmentIds...),
		metacache.WithSegmentState(commonpb.SegmentState_Growing))
	return nil
}

func (wb *writeBufferBase) dropPartitions(partitionIDs []int64) {
	// mark segment dropped if partition was dropped
	segIDs := wb.metaCache.GetSegmentIDsBy(metacache.WithPartitionIDs(partitionIDs))
	wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Dropped),
		metacache.WithSegmentIDs(segIDs...),
	)
}

func (wb *writeBufferBase) syncSegments(ctx context.Context, segmentIDs []int64) []*conc.Future[struct{}] {
	result := make([]*conc.Future[struct{}], 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		// Build and submit one task before moving to the next segment so a task
		// that owns yielded data is never left detached from the sync manager.
		wb.mut.Lock()
		if wb.closed || wb.dropping {
			wb.mut.Unlock()
			break
		}
		tasks := wb.getSyncTasksLocked(ctx, []int64{segmentID})
		wb.mut.Unlock()
		if len(tasks) == 0 {
			continue
		}
		result = append(result, wb.submitSyncTasks(ctx, tasks)...)
	}
	return result
}

// submitDropSegment builds the final task for one drained segment. Drop has
// already blocked normal submissions and waited for any existing owner, so the
// regular per-segment gate remains the only synchronization needed here.
func (wb *writeBufferBase) submitDropSegment(ctx context.Context, segmentID int64) ([]*conc.Future[struct{}], *ordinarySyncState, error) {
	wb.mut.Lock()
	if wb.closed {
		wb.mut.Unlock()
		return nil, nil, nil
	}
	task, err := wb.getSyncTask(ctx, segmentID)
	if err != nil {
		if errors.Is(err, errGrowingSourceUnavailable) {
			wb.rollbackGrowingSourceSyncCandidate(segmentID)
		}
		wb.mut.Unlock()
		return nil, nil, err
	}
	if task == nil {
		wb.mut.Unlock()
		return nil, nil, merr.WrapErrServiceInternalMsg("segment %d still has an outstanding sync owner", segmentID)
	}
	var ordinaryState *ordinarySyncState
	switch typedTask := task.(type) {
	case *syncmgr.SyncTask:
		typedTask.WithDrop()
		ordinaryState = wb.ordinarySyncs[segmentID]
	case *syncmgr.GrowingSourceSyncTask:
		typedTask.WithDrop()
	default:
		wb.mut.Unlock()
		return nil, nil, merr.WrapErrServiceInternalMsg("unsupported drop task %T for segment %d", task, segmentID)
	}
	wb.mut.Unlock()
	return wb.submitSyncTasks(ctx, []syncmgr.Task{task}), ordinaryState, nil
}

// getSyncTasksLocked builds sync tasks and moves payload out of the write buffer.
// The caller must hold wb.mut and submit the returned tasks after releasing it.
func (wb *writeBufferBase) getSyncTasksLocked(ctx context.Context, segmentIDs []int64) []syncmgr.Task {
	result := make([]syncmgr.Task, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		syncTask, err := wb.getSyncTask(ctx, segmentID)
		if err != nil {
			if errors.Is(err, merr.ErrSegmentNotFound) {
				mlog.Warn(ctx, "segment not found in meta", mlog.FieldSegmentID(segmentID))
				continue
			} else if errors.Is(err, errGrowingSourceUnavailable) && wb.hasGrowingSourceProgress(segmentID) {
				wb.rollbackGrowingSourceSyncCandidate(segmentID)
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

func (wb *writeBufferBase) submitSyncTasks(ctx context.Context, syncTasks []syncmgr.Task) []*conc.Future[struct{}] {
	result := make([]*conc.Future[struct{}], 0, len(syncTasks))
	for _, syncTask := range syncTasks {
		var ordinaryAttempt uint64
		var ordinaryState *ordinarySyncState
		if ordinaryTask, ok := syncTask.(*syncmgr.SyncTask); ok {
			wb.mut.Lock()
			state, exists := wb.ordinarySyncs[ordinaryTask.SegmentID()]
			if exists && state.task == ordinaryTask && state.phase == ordinarySyncAttemptInFlight {
				ordinaryAttempt = state.attempt
				ordinaryState = state
			}
			wb.mut.Unlock()
			if ordinaryAttempt == 0 {
				continue
			}
		}

		future, err := wb.syncMgr.SyncData(ctx, syncTask, func(err error) error {
			if ordinaryTask, isOrdinary := syncTask.(*syncmgr.SyncTask); isOrdinary {
				outcome := wb.handleOrdinarySyncResult(ctx, ordinaryTask, ordinaryAttempt, err)
				return wb.finishOrdinarySyncOutcome(ordinaryState, syncTask, outcome)
			}

			var resyncGrowingSourceSegmentID int64
			if growingSourceTask, ok := syncTask.(*syncmgr.GrowingSourceSyncTask); ok {
				if err != nil {
					// Run releases its source in a defer. Dispatcher rejection and
					// queued-task cancellation never enter Run, so make the callback
					// path idempotently cover both cases.
					growingSourceTask.ReleaseSource()
				}
				wb.mut.Lock()
				if progress, exists := wb.growingSourceProgress[growingSourceTask.SegmentID()]; exists {
					if err != nil {
						if growingSourceTask.HasCommittedFlush() && growingSourceTask.CommittedManifestPath() != "" {
							progress.pendingCommitted = &growingSourcePendingCommittedFlush{
								targetOffset:  growingSourceTask.TargetOffset(),
								manifestPath:  growingSourceTask.CommittedManifestPath(),
								bm25Stats:     cloneBM25StatsMap(growingSourceTask.CommittedBM25Stats()),
								insertBinlogs: growingSourceTask.CommittedInsertBinlogs(),
								pkStats:       growingSourceTask.CommittedPKStats(),
							}
						}
						progress.failSync(err)
						wb.rollbackGrowingSourceSyncTaskLocked(growingSourceTask)
						wb.observeGrowingSourceSyncFailureLocked(growingSourceTask.SegmentID(), progress)
						if fatal := growingSourceSyncFatal(err); fatal != "" {
							// markNonRetryableFailure permanently parks this
							// segment: growingSourceProgressSyncable refuses it
							// forever, so its batches are never trimmed and the
							// channel checkpoint stays pinned at
							// firstUncommittedPosition. Left silent that is an
							// unbounded, alert-less stall — strictly worse than
							// a crash, because nothing ever reports it. Fail
							// loudly instead: the rows are still recoverable
							// from the WAL, and a human has to look at this.
							progress.markNonRetryableFailure()
							mlog.Error(ctx, "growing-source sync hit a non-retryable failure, escalating",
								mlog.String("reason", fatal),
								mlog.Int64("segmentID", growingSourceTask.SegmentID()),
								mlog.Int64("targetOffset", progress.targetOffset),
								mlog.String("lastFailure", progress.lastFailure))
							fatalErr := errors.Wrapf(err, "growing-source sync unrecoverable (%s), segmentID=%d targetOffset=%d",
								fatal, growingSourceTask.SegmentID(), progress.targetOffset)
							defer wb.errHandler(fatalErr)
						} else {
							wb.scheduleGrowingSourceRetryLocked()
						}
					} else {
						if growingSourceTask.IsFlush() {
							progress.pendingFlush = false
						}
						progress.ack(growingSourceTask.TargetOffset())
						wb.resetGrowingSourceSyncFailureMetric(growingSourceTask.SegmentID())
						if progress.pendingFlush && len(progress.batches) == 0 {
							segment, ok := wb.metaCache.GetSegmentByID(growingSourceTask.SegmentID())
							if !ok {
								delete(wb.growingSourceProgress, growingSourceTask.SegmentID())
							} else {
								if segment.State() == commonpb.SegmentState_Sealed {
									wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing), metacache.WithSegmentIDs(growingSourceTask.SegmentID()))
								}
								resyncGrowingSourceSegmentID = growingSourceTask.SegmentID()
							}
						} else if len(progress.batches) == 0 {
							segment, ok := wb.metaCache.GetSegmentByID(growingSourceTask.SegmentID())
							if growingSourceTask.IsFlush() || growingSourceTask.IsDrop() || !ok ||
								segment.State() == commonpb.SegmentState_Flushed ||
								segment.State() == commonpb.SegmentState_Dropped {
								delete(wb.growingSourceProgress, growingSourceTask.SegmentID())
							}
						}
					}
				}
				wb.mut.Unlock()
			}
			if resyncGrowingSourceSegmentID != 0 {
				wb.syncSegments(wb.syncCtx, []int64{resyncGrowingSourceSegmentID})
			}

			if err != nil {
				if wb.taskObserverCallback != nil {
					wb.taskObserverCallback(syncTask, err)
				}
				return err
			}

			if syncTask.StartPosition() != nil {
				wb.syncCheckpoint.Remove(syncTask.SegmentID(), syncTask.StartPosition().GetTimestamp())
			}

			if syncTask.IsFlush() {
				wb.metaCache.RemoveSegments(metacache.WithSegmentIDs(syncTask.SegmentID()))
				mlog.Info(ctx, "flushed segment removed", mlog.FieldSegmentID(syncTask.SegmentID()), mlog.String("channel", syncTask.ChannelName()))
			}
			if wb.taskObserverCallback != nil {
				wb.taskObserverCallback(syncTask, nil)
			}
			return nil
		})
		if err != nil {
			if ordinaryTask, ok := syncTask.(*syncmgr.SyncTask); ok {
				outcome := wb.handleOrdinarySyncResult(ctx, ordinaryTask, ordinaryAttempt, err)
				err = wb.finishOrdinarySyncOutcome(ordinaryState, syncTask, outcome)
				result = append(result, conc.Go(func() (struct{}, error) {
					return struct{}{}, err
				}))
				continue
			}
			if growingSourceTask, ok := syncTask.(*syncmgr.GrowingSourceSyncTask); ok {
				growingSourceTask.ReleaseSource()
				wb.mut.Lock()
				if progress, exists := wb.growingSourceProgress[growingSourceTask.SegmentID()]; exists {
					progress.failSync(err)
					wb.rollbackGrowingSourceSyncTaskLocked(growingSourceTask)
					wb.observeGrowingSourceSyncFailureLocked(growingSourceTask.SegmentID(), progress)
					wb.scheduleGrowingSourceRetryLocked()
				}
				wb.mut.Unlock()
				if wb.taskObserverCallback != nil {
					wb.taskObserverCallback(syncTask, err)
				}
				result = append(result, conc.Go(func() (struct{}, error) {
					return struct{}{}, err
				}))
				continue
			}
			mlog.Fatal(ctx, "failed to sync data", mlog.Int64("segmentID", syncTask.SegmentID()), mlog.Err(err))
		}
		result = append(result, future)
	}
	return result
}

// getSegmentsToSync applies all policies to get segments list to sync.
// **NOTE** shall be invoked within mutex protection
func (wb *writeBufferBase) getSegmentsToSync(ts typeutil.Timestamp, policies ...SyncPolicy) []int64 {
	buffers := lo.Filter(lo.Values(wb.buffers), func(buffer *segmentBuffer, _ int) bool {
		return buffer != nil
	})
	segments := typeutil.NewSet[int64]()
	for _, policy := range policies {
		result := policy.SelectSegments(buffers, ts)
		if len(result) > 0 {
			mlog.Info(context.TODO(), "SyncPolicy selects segments", mlog.Int64s("segmentIDs", result), mlog.String("reason", policy.Reason()))
			segments.Insert(result...)
		}
	}
	for segmentID, progress := range wb.growingSourceProgress {
		if len(policies) == 0 || wb.growingSourceProgressSelectedByPolicy(ts, segmentID, progress) {
			segments.Insert(segmentID)
		}
	}

	return lo.Filter(segments.Collect(), func(segmentID int64, _ int) bool {
		if state, ok := wb.ordinarySyncs[segmentID]; ok {
			// The older logical task owns ordering for this segment. Record the
			// policy edge so its terminal success immediately schedules the tail,
			// but do not build a second task yet.
			state.pending = true
			return false
		}
		progress, ok := wb.growingSourceProgress[segmentID]
		if !ok {
			return true
		}
		syncable, retry := wb.growingSourceProgressSyncable(segmentID, progress, segments.Contain(segmentID), false)
		if retry {
			wb.scheduleGrowingSourceRetryLocked()
		}
		return syncable
	})
}

func (wb *writeBufferBase) growingSourceProgressSelectedByPolicy(ts typeutil.Timestamp, segmentID int64, progress *growingSourceProgress) bool {
	if progress == nil {
		return false
	}
	if progress.nonRetryableFailure {
		return false
	}
	if progress.pendingFlush {
		return true
	}
	segment, ok := wb.metaCache.GetSegmentByID(segmentID)
	if ok {
		switch segment.State() {
		case commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Dropped:
			return true
		}
		if wb.growingSourceProgressFull(segment, progress) {
			return true
		}
	}
	startPos := progress.firstUncommittedPosition()
	if startPos == nil {
		return false
	}
	staleDuration := paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)
	current := tsoutil.PhysicalTime(ts)
	start := tsoutil.PhysicalTime(startPos.GetTimestamp())
	return current.Sub(start) > staleDuration
}

func (wb *writeBufferBase) growingSourceProgressFull(segment *metacache.SegmentInfo, progress *growingSourceProgress) bool {
	if segment == nil || progress == nil {
		return false
	}
	rows := progress.targetOffset - segment.FlushedRows() - segment.SyncingRows()
	if rows <= 0 {
		return false
	}
	if wb.estSizePerRecord <= 0 {
		return false
	}
	thresholdRows := int64(wb.getEstBatchSize())
	if thresholdRows <= 0 {
		return true
	}
	return rows >= thresholdRows
}

func (wb *writeBufferBase) rollbackGrowingSourceSyncCandidate(segmentID int64) {
	if progress, ok := wb.growingSourceProgress[segmentID]; ok {
		progress.failSync(errGrowingSourceUnavailable)
		wb.observeGrowingSourceSyncFailureLocked(segmentID, progress)
		wb.scheduleGrowingSourceRetryLocked()
	}
	if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok && segment.State() == commonpb.SegmentState_Flushing {
		wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Sealed), metacache.WithSegmentIDs(segmentID))
	}
}

func (wb *writeBufferBase) rollbackGrowingSourceSyncTaskLocked(task *syncmgr.GrowingSourceSyncTask) {
	if task.BatchRows() > 0 {
		wb.metaCache.UpdateSegments(metacache.AbortSyncing(task.BatchRows()), metacache.WithSegmentIDs(task.SegmentID()))
	}
	if task.StartPosition() != nil {
		wb.syncCheckpoint.Remove(task.SegmentID(), task.StartPosition().GetTimestamp())
	}
}

func (wb *writeBufferBase) observeGrowingSourceSyncFailureLocked(segmentID int64, progress *growingSourceProgress) {
	metrics.DataNodeGrowingSourceSyncFailureCount.WithLabelValues(
		paramtable.GetStringNodeID(),
		fmt.Sprint(wb.collectionID),
		wb.channelName,
	).Set(float64(progress.failureCount))

	if progress.failureCount < growingSourceSyncFailureWarnThreshold ||
		progress.failureCount%growingSourceSyncFailureWarnThreshold != 0 {
		return
	}

	wb.growingSourceRatedLogger.RatedWarn(context.TODO(), rate.Limit(1), "growing-source source sync keeps failing",
		mlog.Int64("segmentID", segmentID),
		mlog.Int64("failureCount", progress.failureCount),
		mlog.Int64("targetOffset", progress.targetOffset),
		mlog.String("lastFailure", progress.lastFailure),
	)
}

func (wb *writeBufferBase) resetGrowingSourceSyncFailureMetric(segmentID int64) {
	metrics.DataNodeGrowingSourceSyncFailureCount.WithLabelValues(
		paramtable.GetStringNodeID(),
		fmt.Sprint(wb.collectionID),
		wb.channelName,
	).Set(0)
	if progress, ok := wb.growingSourceProgress[segmentID]; ok {
		progress.failureCount = 0
		progress.lastFailure = ""
	}
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

func (wb *writeBufferBase) yieldBuffer(segmentID int64) ([]*storage.InsertData, map[int64]*storage.BM25Stats, *storage.DeleteData, *schemapb.CollectionSchema, *TimeRange, *msgpb.MsgPosition) {
	buffer, ok := wb.buffers[segmentID]
	if !ok {
		return nil, nil, nil, nil, nil, nil
	}

	// remove buffer and move it to sync manager
	delete(wb.buffers, segmentID)
	start := buffer.EarliestPosition()
	timeRange := buffer.GetTimeRange()
	insert, bm25, delta, schema := buffer.Yield()

	return insert, bm25, delta, schema, timeRange, start
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

	intPKTs map[int64]int64
	strPKTs map[string]int64
}

func NewInsertData(segmentID, partitionID int64, cap int, pkType schemapb.DataType) *InsertData {
	data := &InsertData{
		segmentID:   segmentID,
		partitionID: partitionID,
		data:        make([]*storage.InsertData, 0, cap),
		pkField:     make([]storage.FieldData, 0, cap),
		pkType:      pkType,
	}

	switch pkType {
	case schemapb.DataType_Int64:
		data.intPKTs = make(map[int64]int64)
	case schemapb.DataType_VarChar:
		data.strPKTs = make(map[string]int64)
	}

	return data
}

func (id *InsertData) Append(data *storage.InsertData, pkFieldData storage.FieldData, tsFieldData *storage.Int64FieldData) {
	id.data = append(id.data, data)
	id.pkField = append(id.pkField, pkFieldData)
	id.tsField = append(id.tsField, tsFieldData)
	id.rowNum += int64(data.GetRowNum())

	timestamps := tsFieldData.GetDataRows().([]int64)
	switch id.pkType {
	case schemapb.DataType_Int64:
		pks := pkFieldData.GetDataRows().([]int64)
		for idx, pk := range pks {
			ts, ok := id.intPKTs[pk]
			if !ok || timestamps[idx] < ts {
				id.intPKTs[pk] = timestamps[idx]
			}
		}
	case schemapb.DataType_VarChar:
		pks := pkFieldData.GetDataRows().([]string)
		for idx, pk := range pks {
			ts, ok := id.strPKTs[pk]
			if !ok || timestamps[idx] < ts {
				id.strPKTs[pk] = timestamps[idx]
			}
		}
	}
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

func (id *InsertData) pkExists(pk storage.PrimaryKey, ts uint64) bool {
	var ok bool
	var minTs int64
	switch pk.Type() {
	case schemapb.DataType_Int64:
		minTs, ok = id.intPKTs[pk.GetValue().(int64)]
	case schemapb.DataType_VarChar:
		minTs, ok = id.strPKTs[pk.GetValue().(string)]
	}

	return ok && ts > uint64(minTs)
}

func (id *InsertData) batchPkExists(pks []storage.PrimaryKey, tss []uint64, hits []bool) []bool {
	if len(pks) == 0 {
		return nil
	}

	pkType := pks[0].Type()
	switch pkType {
	case schemapb.DataType_Int64:
		for i := range pks {
			if !hits[i] {
				minTs, ok := id.intPKTs[pks[i].GetValue().(int64)]
				hits[i] = ok && tss[i] > uint64(minTs)
			}
		}
	case schemapb.DataType_VarChar:
		for i := range pks {
			if !hits[i] {
				minTs, ok := id.strPKTs[pks[i].GetValue().(string)]
				hits[i] = ok && tss[i] > uint64(minTs)
			}
		}
	}

	return hits
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
	metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(paramtable.GetStringNodeID(), fmt.Sprint(wb.collectionID)).Add(float64(bufSize))
	return bufSize
}

func (wb *writeBufferBase) getSyncTask(ctx context.Context, segmentID int64) (syncmgr.Task, error) {
	if state, ok := wb.ordinarySyncs[segmentID]; ok {
		// A queued, running, or retry-wait task owns the older logical batch.
		// Remember that another policy selected this segment, but leave all new
		// rows in wb.buffers until the owner reaches a terminal state.
		state.pending = true
		return nil, nil
	}
	if progress, ok := wb.growingSourceProgress[segmentID]; ok && progress.syncing {
		// Growing-source and write-buffer tasks share one segment-level gate.
		// The keyed dispatcher would serialize two tasks, but it cannot prevent
		// the second task from being built against counters that the first task
		// has not committed yet.
		if segment, exists := wb.metaCache.GetSegmentByID(segmentID); exists {
			switch segment.State() {
			case commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Dropped:
				progress.pendingFlush = true
			}
		}
		return nil, nil
	}
	segmentInfo, ok := wb.metaCache.GetSegmentByID(segmentID) // wb.metaCache.GetSegmentsBy(metacache.WithSegmentIDs(segmentID))
	if !ok {
		mlog.Warn(ctx, "segment info not found in meta cache", mlog.FieldSegmentID(segmentID))
		return nil, merr.WrapErrSegmentNotFound(segmentID)
	}
	if progress, ok := wb.growingSourceProgress[segmentID]; ok && !wb.hasWriteBufferInsertPayload(segmentID) {
		return wb.getGrowingSourceSyncTask(ctx, segmentInfo, progress)
	}
	var batchSize int64
	var insertMemSize int64
	var deleteMemSize int64
	var tsFrom, tsTo uint64

	insert, bm25, delta, schema, timeRange, startPos := wb.yieldBuffer(segmentID)
	if timeRange != nil {
		tsFrom, tsTo = timeRange.timestampMin, timeRange.timestampMax
	}

	if startPos != nil {
		wb.syncCheckpoint.Add(segmentID, startPos, "syncing task")
	}

	actions := []metacache.SegmentAction{}

	for _, chunk := range insert {
		batchSize += int64(chunk.GetRowNum())
		insertMemSize += int64(chunk.GetMemorySize())
	}
	if delta != nil {
		deleteMemSize += delta.Size()
	}

	actions = append(actions, metacache.StartSyncing(batchSize))
	wb.metaCache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(segmentID))

	pack := &syncmgr.SyncPack{}
	pack.WithInsertData(insert).
		WithDeleteData(delta).
		WithCollectionID(wb.collectionID).
		WithPartitionID(segmentInfo.PartitionID()).
		WithChannelName(wb.channelName).
		WithSegmentID(segmentID).
		WithStartPosition(startPos).
		WithTimeRange(tsFrom, tsTo).
		WithLevel(segmentInfo.Level()).
		WithDataSource(metrics.StreamingDataSourceLabel).
		WithCheckpoint(wb.checkpoint).
		WithBatchRows(batchSize).
		// HandleError is owned by syncManager and remains non-fatal for ordinary
		// logical tasks. The write buffer classifies the attempt result and only
		// invokes errHandler after proving the task cannot be retried.
		WithErrorHandler(nil)

	if len(bm25) != 0 {
		pack.WithBM25Stats(bm25)
	}

	if segmentInfo.State() == commonpb.SegmentState_Flushing ||
		segmentInfo.Level() == datapb.SegmentLevel_L0 { // Level zero segment will always be sync as flushed
		pack.WithFlush()
	}

	if segmentInfo.State() == commonpb.SegmentState_Dropped {
		pack.WithDrop()
	}

	payloadReleased := make(chan struct{})
	task := syncmgr.NewSyncTask().
		WithAllocator(wb.allocator).
		WithMetaWriter(wb.metaWriter).
		WithMetaCache(wb.metaCache).
		WithSchema(schema).
		WithSyncPack(pack).
		WithStorageConfig(packed.CreateStorageConfig()).
		WithPayloadAccounting(insertMemSize, deleteMemSize, func(released int64) {
			if released > 0 {
				metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(
					paramtable.GetStringNodeID(), fmt.Sprint(wb.collectionID)).Sub(float64(released))
			}
			close(payloadReleased)
		}).
		// Keep the storage write inside this logical attempt retrying until its
		// context is canceled. Submission and metadata failures are handled by
		// the per-segment logical retry state.
		WithWriteRetryOptions(retry.AttemptAlways(), retry.MaxSleepTime(10*time.Second),
			retry.RetryErr(func(error) bool { return true }))
	wb.ordinarySyncs[segmentID] = &ordinarySyncState{
		task:            task,
		phase:           ordinarySyncAttemptInFlight,
		attempt:         1,
		retryCtx:        ctx,
		payloadReleased: payloadReleased,
		done:            make(chan struct{}),
	}
	return task, nil
}

func (wb *writeBufferBase) getGrowingSourceSyncTask(ctx context.Context, segmentInfo *metacache.SegmentInfo, progress *growingSourceProgress) (syncmgr.Task, error) {
	if segmentInfo.GetStorageVersion() != storage.StorageV3 {
		return nil, merr.WrapErrServiceInternalMsg("growing-source sync requires StorageV3 segment, segmentID=%d storageVersion=%d",
			segmentInfo.SegmentID(), segmentInfo.GetStorageVersion())
	}
	targetOffset := progress.targetOffset
	pendingCommitted := progress.pendingCommitted
	if pendingCommitted != nil {
		targetOffset = pendingCommitted.targetOffset
	}
	checkpoint := progress.checkpointFor(targetOffset)
	startPos := progress.firstUncommittedPosition()
	if checkpoint == nil {
		checkpoint = startPos
	}
	if checkpoint == nil {
		checkpoint = wb.checkpoint
	}
	schemaTimestamp := uint64(0)
	if startPos != nil {
		schemaTimestamp = startPos.GetTimestamp()
	}
	var source syncmgr.GrowingFlushSource
	if pendingCommitted == nil {
		var state syncmgr.GrowingSourceState
		source, state = wb.getGrowingSource(progress.segmentID, targetOffset, checkpoint)
		if state != syncmgr.GrowingSourceUsable {
			if source != nil {
				source.Release()
			}
			return nil, errors.Wrapf(errGrowingSourceUnavailable, "segment %d state %d", progress.segmentID, state)
		}
	} else {
		var state syncmgr.GrowingSourceState
		source, state = wb.getGrowingSource(progress.segmentID, targetOffset, checkpoint)
		if state != syncmgr.GrowingSourceUsable {
			if source != nil {
				source.Release()
				source = nil
			}
			wb.logger.Warn(ctx, "growing source unavailable during committed flush ack retry; retrying SaveBinlogPaths without re-flush",
				mlog.Int64("segmentID", progress.segmentID),
				mlog.Int64("targetOffset", targetOffset),
				mlog.Int("state", int(state)))
		}
	}

	batchSize := targetOffset - segmentInfo.FlushedRows() - segmentInfo.SyncingRows()
	buildTask := func(batchRows int64) *syncmgr.GrowingSourceSyncTask {
		task := syncmgr.NewGrowingSourceSyncTask().
			WithCollectionID(wb.collectionID).
			WithPartitionID(segmentInfo.PartitionID()).
			WithSegmentID(progress.segmentID).
			WithChannelName(wb.channelName).
			WithStartPosition(startPos).
			WithCheckpoint(checkpoint).
			WithBatchRows(batchRows).
			WithTargetOffset(targetOffset).
			WithLevel(segmentInfo.Level()).
			WithMetaCache(wb.metaCache).
			WithMetaWriter(wb.metaWriter).
			WithSchema(wb.metaCache.GetSchema(schemaTimestamp)).
			WithAllocator(wb.allocator).
			WithStorageConfig(packed.CreateStorageConfig()).
			// Non-fatal on purpose: this task is re-submitted by
			// scheduleGrowingSourceRetryLocked, and the rows it flushes stay
			// pinned in the growing segment until CommitGrowingFlush, so a
			// failed attempt costs nothing but a round trip. Escalation to the
			// fatal handler happens only where recovery is impossible — see the
			// non-retryable branch in submitSyncTasks.
			WithFailureCallback(wb.growingSourceErrHandler).
			// Same as above: keep the critical write path retrying despite the
			// retry.Do InputError short-circuit.
			WithWriteRetryOptions(retry.AttemptAlways(), retry.MaxSleepTime(10*time.Second),
				retry.RetryErr(func(error) bool { return true }))
		if source != nil {
			task.WithSource(source)
		}
		if pendingCommitted != nil {
			task.WithCommittedFlush(pendingCommitted.manifestPath, cloneBM25StatsMap(pendingCommitted.bm25Stats), pendingCommitted.insertBinlogs)
			task.WithCommittedPKStats(pendingCommitted.pkStats)
		}
		if segmentInfo.State() == commonpb.SegmentState_Flushing {
			task.WithFlush()
		}
		if segmentInfo.State() == commonpb.SegmentState_Dropped {
			task.WithDrop()
		}
		return task
	}

	if batchSize <= 0 {
		progress.syncing = true
		progress.syncingOffset = targetOffset
		return buildTask(0), nil
	}

	if startPos != nil {
		wb.syncCheckpoint.Add(progress.segmentID, startPos, "growing source syncing task")
	}
	progress.syncing = true
	progress.syncingOffset = targetOffset
	wb.metaCache.UpdateSegments(metacache.StartSyncing(batchSize), metacache.WithSegmentIDs(progress.segmentID))

	return buildTask(batchSize), nil
}

// getEstBatchSize returns the batch size based on estimated size per record and FlushBufferSize configuration value.
func (wb *writeBufferBase) getEstBatchSize() uint {
	sizeLimit := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	return uint(sizeLimit / int64(wb.estSizePerRecord))
}

func (wb *writeBufferBase) waitGrowingSourceSyncs(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	canceled := false
	for {
		wb.mut.RLock()
		inFlight := false
		for _, progress := range wb.growingSourceProgress {
			if progress.syncing {
				inFlight = true
				break
			}
		}
		wb.mut.RUnlock()
		if !inFlight {
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		}
		if canceled {
			<-ticker.C
			continue
		}
		select {
		case <-ctx.Done():
			// Pre-existing attempts were submitted with wb.syncCtx before Drop
			// took ownership. Cancel both generations, then keep waiting until the
			// task callback has rolled back syncing rows and source ownership.
			wb.syncCancel()
			canceled = true
		case <-ticker.C:
		}
	}
}

func (wb *writeBufferBase) waitDropRetry(ctx context.Context) error {
	interval := wb.growingSourceRetryInterval
	if interval < 0 {
		interval = defaultGrowingSourceRetryInterval
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
		futures, ordinaryState, err := wb.submitDropSegment(ctx, segmentID)
		if err != nil {
			if errors.Is(err, errGrowingSourceUnavailable) {
				if err := wb.waitDropRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}

		if ordinaryState != nil {
			return wb.waitOrdinarySyncWaiters(ctx, cancel, []*ordinarySyncState{ordinaryState})
		}
		if len(futures) == 0 {
			return nil
		}

		err = conc.BlockOnAll(futures...)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if growingSourceSyncFatal(err) != "" {
			return err
		}
		if err := wb.waitDropRetry(ctx); err != nil {
			return err
		}
	}
}

// abortDrop closes every local ownership path before Close propagates a Drop
// failure. Data was not committed, so ordinary checkpoints stay pinned for WAL
// replay. The write buffer has already been removed from its manager and cannot
// safely accept more work after this point.
func (wb *writeBufferBase) abortDrop(cancel context.CancelFunc, terminalErr error) {
	waiters := wb.cancelOutstandingOrdinarySyncs(terminalErr, cancel)
	for _, state := range waiters {
		<-state.done
	}
	_ = wb.waitGrowingSourceSyncs(context.Background())

	wb.mut.Lock()
	if wb.growingSourceRetryTimer != nil {
		wb.growingSourceRetryTimer.Stop()
		wb.growingSourceRetryTimer = nil
	}
	wb.growingSourceRetryScheduled = false
	wb.growingSourceProgress = make(map[int64]*growingSourceProgress)
	bufferedBytes := wb.totalBufferedMemorySizeLocked()
	if bufferedBytes > 0 {
		metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(
			paramtable.GetStringNodeID(), fmt.Sprint(wb.collectionID)).Sub(float64(bufferedBytes))
	}
	wb.buffers = make(map[int64]*segmentBuffer)
	wb.closed = true
	wb.dropping = false
	wb.mut.Unlock()
}

func (wb *writeBufferBase) Close(ctx context.Context, drop bool) {
	wb.mut.Lock()
	wb.growingSourceRetryScheduled = false
	if wb.growingSourceRetryTimer != nil {
		wb.growingSourceRetryTimer.Stop()
		wb.growingSourceRetryTimer = nil
	}
	if !drop {
		wb.closed = true
		wb.syncCancel()
		completed := make([]*ordinarySyncState, 0)
		waiters := make([]*ordinarySyncState, 0, len(wb.ordinarySyncs))
		for _, state := range wb.ordinarySyncs {
			switch state.phase {
			case ordinarySyncRetryWait:
				wb.discardOrdinarySyncLocked(state, true)
				completed = append(completed, state)
			default:
				waiters = append(waiters, state)
			}
		}
		wb.mut.Unlock()
		for _, state := range completed {
			state.complete(context.Canceled)
		}
		for _, state := range waiters {
			<-state.done
		}
		_ = wb.waitGrowingSourceSyncs(context.Background())
		return
	}
	dropCtx, dropCancel := context.WithCancel(ctx)
	defer dropCancel()
	wb.dropping = true
	ordinaryTasks, ordinaryWaiters := wb.beginOrdinaryDropLocked(dropCtx)
	wb.mut.Unlock()
	defer func() {
		if panicValue := recover(); panicValue != nil {
			wb.mut.RLock()
			closed := wb.closed
			wb.mut.RUnlock()
			if !closed {
				terminalErr, ok := panicValue.(error)
				if !ok {
					terminalErr = merr.WrapErrServiceInternalMsg("drop callback panicked: %v", panicValue)
				}
				wb.abortDrop(dropCancel, terminalErr)
			}
			panic(panicValue)
		}
	}()

	// Existing logical tasks own older batches and must reach terminal success
	// before any buffered tail can be turned into the final drop task.
	if err := wb.waitOrdinarySyncs(dropCtx, dropCancel, ordinaryTasks, ordinaryWaiters); err != nil {
		mlog.Error(ctx, "failed to drain outstanding sync tasks while dropping write buffer", mlog.Err(err))
		panic(err)
	}
	if err := wb.waitGrowingSourceSyncs(dropCtx); err != nil {
		mlog.Error(ctx, "failed to drain outstanding growing-source tasks while dropping write buffer", mlog.Err(err))
		panic(err)
	}

	wb.mut.Lock()
	segmentIDs := typeutil.NewSet[int64]()
	for id := range wb.buffers {
		segmentIDs.Insert(id)
	}
	for id := range wb.growingSourceProgress {
		segmentIDs.Insert(id)
	}
	dropSegmentIDs := segmentIDs.Collect()
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
		switch pkField.GetDataType() {
		case schemapb.DataType_Int64:
			inData.intPKTs = make(map[int64]int64)
		case schemapb.DataType_VarChar:
			inData.strPKTs = make(map[string]int64)
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

			timestamps := tsFieldData.GetDataRows().([]int64)

			switch pkField.GetDataType() {
			case schemapb.DataType_Int64:
				pks := pkFieldData.GetDataRows().([]int64)
				for idx, pk := range pks {
					ts, ok := inData.intPKTs[pk]
					if !ok || timestamps[idx] < ts {
						inData.intPKTs[pk] = timestamps[idx]
					}
				}
			case schemapb.DataType_VarChar:
				pks := pkFieldData.GetDataRows().([]string)
				for idx, pk := range pks {
					ts, ok := inData.strPKTs[pk]
					if !ok || timestamps[idx] < ts {
						inData.strPKTs[pk] = timestamps[idx]
					}
				}
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
