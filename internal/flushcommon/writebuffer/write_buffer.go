package writebuffer

import (
	"context"
	"fmt"
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
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	nonFlushTS uint64 = 0
)

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

type checkpointCandidate struct {
	segmentID int64
	position  *msgpb.MsgPosition
	source    string
}

type checkpointCandidates struct {
	candidates *typeutil.ConcurrentMap[string, *checkpointCandidate]
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
	flushTimestamp *atomic.Uint64

	errHandler           func(err error)
	taskObserverCallback func(t syncmgr.Task, err error) // execute when a sync task finished, should be concurrent safe.

	closed bool

	// pre build logger
	logger        *mlog.Logger
	cpRatedLogger *mlog.Logger
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

	wb := &writeBufferBase{
		channelName:          channel,
		collectionID:         metacache.Collection(),
		estSizePerRecord:     estSize,
		syncMgr:              syncMgr,
		metaWriter:           option.metaWriter,
		allocator:            option.idAllocator,
		buffers:              make(map[int64]*segmentBuffer),
		metaCache:            metacache,
		syncCheckpoint:       newCheckpointCandiates(),
		syncPolicies:         option.syncPolicies,
		flushTimestamp:       flushTs,
		errHandler:           option.errorHandler,
		taskObserverCallback: option.taskObserverCallback,
	}

	wb.logger = mlog.With(mlog.Int64("collectionID", wb.collectionID),
		mlog.String("channel", wb.channelName))
	wb.cpRatedLogger = wb.logger

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
	wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Sealed),
		metacache.WithSegmentState(commonpb.SegmentState_Growing))
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
}

func (wb *writeBufferBase) GetFlushTimestamp() uint64 {
	return wb.flushTimestamp.Load()
}

func (wb *writeBufferBase) MemorySize() int64 {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	var size int64
	for _, segBuf := range wb.buffers {
		size += segBuf.MemorySize()
	}
	return size
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
	var syncTasks []syncmgr.Task
	if len(segmentIDs) > 0 {
		logger.Info(context.TODO(), "evict buffer find segments to sync", mlog.Int64s("segmentIDs", segmentIDs))
		syncTasks = wb.getSyncTasksLocked(context.Background(), segmentIDs)
	}

	wb.mut.Unlock()

	if len(syncTasks) > 0 {
		futures := wb.submitSyncTasks(context.Background(), syncTasks)
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
			mlog.Warn(ctx, "cannot find segment when sealSegments",
				mlog.Int64("segmentID", segmentID),
				mlog.String("channel", wb.channelName))
			return merr.WrapErrSegmentNotFound(segmentID)
		}
		existingIDs = append(existingIDs, segmentID)
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
	wb.mut.Lock()
	syncTasks := wb.getSyncTasksLocked(ctx, segmentIDs)
	wb.mut.Unlock()
	return wb.submitSyncTasks(ctx, syncTasks)
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
			} else {
				mlog.Fatal(ctx, "failed to get sync task", mlog.FieldSegmentID(segmentID), mlog.Err(err))
			}
		}
		result = append(result, syncTask)
	}
	return result
}

// settleSync releases the checkpoint pin a sync task holds, according to how
// the task ended. This is the only place a syncCheckpoint candidate is removed,
// so pin release follows the outcome rather than the order two statements happen
// to run in.
//
// SettleFailed deliberately keeps the pin. The payload was yielded out of the
// buffer and released without being persisted, so those rows exist only in the
// WAL; releasing the pin would let the channel checkpoint advance past data
// that was never written. The pin is reclaimed when the channel is torn down
// and replayed, which is what the escalating error handler forces.
func (wb *writeBufferBase) settleSync(segmentID int64, startPos *msgpb.MsgPosition, outcome metacache.SettleOutcome) {
	if startPos == nil {
		return
	}
	if outcome == metacache.SettleFailed {
		return
	}
	wb.syncCheckpoint.Remove(segmentID, startPos.GetTimestamp())
}

func (wb *writeBufferBase) submitSyncTasks(ctx context.Context, syncTasks []syncmgr.Task) []*conc.Future[struct{}] {
	result := make([]*conc.Future[struct{}], 0, len(syncTasks))
	for _, syncTask := range syncTasks {
		future, err := wb.syncMgr.SyncData(ctx, syncTask, func(err error) error {
			if wb.taskObserverCallback != nil {
				wb.taskObserverCallback(syncTask, err)
			}

			if err != nil {
				wb.settleSync(syncTask.SegmentID(), syncTask.StartPosition(), metacache.SettleFailed)
				return err
			}

			wb.settleSync(syncTask.SegmentID(), syncTask.StartPosition(), metacache.SettleCommitted)

			if syncTask.IsFlush() {
				wb.metaCache.RemoveSegments(metacache.WithSegmentIDs(syncTask.SegmentID()))
				mlog.Info(ctx, "flushed segment removed", mlog.FieldSegmentID(syncTask.SegmentID()), mlog.String("channel", syncTask.ChannelName()))
			}
			return nil
		})
		if err != nil {
			mlog.Fatal(ctx, "failed to sync data", mlog.Int64("segmentID", syncTask.SegmentID()), mlog.Err(err))
		}
		result = append(result, future)
	}
	return result
}

// getSegmentsToSync applies all policies to get segments list to sync.
// **NOTE** shall be invoked within mutex protection
func (wb *writeBufferBase) getSegmentsToSync(ts typeutil.Timestamp, policies ...SyncPolicy) []int64 {
	buffers := lo.Values(wb.buffers)
	segments := typeutil.NewSet[int64]()
	for _, policy := range policies {
		result := policy.SelectSegments(buffers, ts)
		if len(result) > 0 {
			mlog.Info(context.TODO(), "SyncPolicy selects segments", mlog.Int64s("segmentIDs", result), mlog.String("reason", policy.Reason()))
			segments.Insert(result...)
		}
	}

	return segments.Collect()
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
	}

	return buffer
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

func (id *InsertData) GetBM25Stats() map[int64]*storage.BM25Stats {
	return id.bm25Stats
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
	basePath := storage.SegmentManifestBasePath(packed.CreateStorageConfig().GetRootPath(), wb.collectionID, partitionID, segmentID)
	return packed.MarshalManifestPath(basePath, packed.ManifestEarliest)
}

// bufferDelete buffers DeleteMsg into DeleteData.
func (wb *writeBufferBase) bufferDelete(segmentID int64, pks []storage.PrimaryKey, tss []typeutil.Timestamp, startPos, endPos *msgpb.MsgPosition) {
	segBuf := wb.getOrCreateBuffer(segmentID, tss[0])
	bufSize := segBuf.deltaBuffer.Buffer(pks, tss, startPos, endPos)
	metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(paramtable.GetStringNodeID(), fmt.Sprint(wb.collectionID)).Add(float64(bufSize))
}

func (wb *writeBufferBase) getSyncTask(ctx context.Context, segmentID int64) (syncmgr.Task, error) {
	segmentInfo, ok := wb.metaCache.GetSegmentByID(segmentID) // wb.metaCache.GetSegmentsBy(metacache.WithSegmentIDs(segmentID))
	if !ok {
		mlog.Warn(ctx, "segment info not found in meta cache", mlog.FieldSegmentID(segmentID))
		return nil, merr.WrapErrSegmentNotFound(segmentID)
	}
	var batchSize int64
	var totalMemSize float64 = 0
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
		totalMemSize += float64(chunk.GetMemorySize())
	}

	if delta != nil {
		totalMemSize += float64(delta.Size())
	}

	// One reservation owns this batch's accounting from here to whichever
	// terminal outcome the task reaches. Created in the same metacache update
	// that the yielded payload is accounted against.
	reservation := metacache.NewSyncReservation(segmentID, batchSize)
	actions = append(actions, reservation.Apply())
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
		WithReservation(reservation).
		WithErrorHandler(wb.errHandler)

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

	metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(paramtable.GetStringNodeID(), fmt.Sprint(wb.collectionID)).Sub(totalMemSize)

	task := syncmgr.NewSyncTask().
		WithAllocator(wb.allocator).
		WithMetaWriter(wb.metaWriter).
		WithMetaCache(wb.metaCache).
		WithSchema(schema).
		WithSyncPack(pack).
		WithStorageConfig(packed.CreateStorageConfig()).
		// The flush write path must keep retrying: aborting surfaces the error
		// to SyncTask.HandleError, whose default callback panics the datanode.
		// retry.Do short-circuits InputError-typed errors unless an explicit
		// RetryErr predicate is supplied, so AttemptAlways alone is not enough.
		WithWriteRetryOptions(retry.AttemptAlways(), retry.MaxSleepTime(10*time.Second),
			retry.RetryErr(func(error) bool { return true }))
	return task, nil
}

// getEstBatchSize returns the batch size based on estimated size per record and FlushBufferSize configuration value.
func (wb *writeBufferBase) getEstBatchSize() uint {
	sizeLimit := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	return uint(sizeLimit / int64(wb.estSizePerRecord))
}

func (wb *writeBufferBase) Close(ctx context.Context, drop bool) {
	// sink all data and call Drop for meta writer
	wb.mut.Lock()
	wb.closed = true
	if !drop {
		wb.mut.Unlock()
		return
	}

	var syncTasks []syncmgr.Task
	segmentIDs := typeutil.NewSet[int64]()
	for id := range wb.buffers {
		segmentIDs.Insert(id)
	}
	for _, id := range segmentIDs.Collect() {
		syncTask, err := wb.getSyncTask(ctx, id)
		if err != nil {
			continue
		}
		if t, ok := syncTask.(*syncmgr.SyncTask); ok {
			t.WithDrop()
		}
		syncTasks = append(syncTasks, syncTask)
	}
	wb.mut.Unlock()

	futures := wb.submitDropSyncTasks(ctx, syncTasks)
	err := conc.AwaitAll(futures...)
	if err != nil {
		mlog.Error(ctx, "failed to sink write buffer data", mlog.Err(err))
		// TODO change to remove channel in the future
		panic(err)
	}
	err = wb.metaWriter.DropChannel(ctx, wb.channelName)
	if err != nil {
		mlog.Error(ctx, "failed to drop channel", mlog.Err(err))
		// TODO change to remove channel in the future
		panic(err)
	}
}

func (wb *writeBufferBase) submitDropSyncTasks(ctx context.Context, syncTasks []syncmgr.Task) []*conc.Future[struct{}] {
	futures := make([]*conc.Future[struct{}], 0, len(syncTasks))
	for _, syncTask := range syncTasks {
		f, err := wb.syncMgr.SyncData(ctx, syncTask, func(err error) error {
			if wb.taskObserverCallback != nil {
				wb.taskObserverCallback(syncTask, err)
			}

			if err != nil {
				wb.settleSync(syncTask.SegmentID(), syncTask.StartPosition(), metacache.SettleFailed)
				return err
			}
			// A drop task's rows are intentionally not kept, so the pin goes
			// even though nothing persisted them.
			wb.settleSync(syncTask.SegmentID(), syncTask.StartPosition(), metacache.SettleDiscarded)
			return nil
		})
		if err != nil {
			mlog.Fatal(ctx, "failed to sync segment", mlog.Int64("segmentID", syncTask.SegmentID()), mlog.Err(err))
		}
		futures = append(futures, f)
	}
	return futures
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
