package writebuffer

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	CreateNewGrowingSegment(partitionID int64, segmentID int64, startPos *msgpb.MsgPosition, storageVersion int64)
	// BufferData is the method to buffer dml data msgs.
	BufferData(insertMsgs []*InsertData, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error
	// FlushTimestamp set flush timestamp for write buffer
	SetFlushTimestamp(flushTs uint64)
	// GetFlushTimestamp get current flush timestamp
	GetFlushTimestamp() uint64
	// SealSegments is the method to perform `Sync` operation with provided options.
	SealSegments(ctx context.Context, segmentIDs []int64) error
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
	var result *checkpointCandidate = def
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
	collSchema       *schemapb.CollectionSchema
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

	// pre build logger
	logger        *log.MLogger
	cpRatedLogger *log.MLogger
}

func newWriteBufferBase(channel string, metacache metacache.MetaCache, syncMgr syncmgr.SyncManager, option *writeBufferOption) (*writeBufferBase, error) {
	flushTs := atomic.NewUint64(nonFlushTS)
	flushTsPolicy := GetFlushTsPolicy(flushTs, metacache)
	option.syncPolicies = append(option.syncPolicies, flushTsPolicy)

	schema := metacache.Schema()
	estSize, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		return nil, err
	}

	wb := &writeBufferBase{
		channelName:          channel,
		collectionID:         metacache.Collection(),
		collSchema:           schema,
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

	wb.logger = log.With(zap.Int64("collectionID", wb.collectionID),
		zap.String("channel", wb.channelName))
	wb.cpRatedLogger = wb.logger.WithRateGroup(fmt.Sprintf("writebuffer_cp_%s", wb.channelName), 1, 60)

	return wb, nil
}

func (wb *writeBufferBase) HasSegment(segmentID int64) bool {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	_, ok := wb.buffers[segmentID]
	return ok
}

func (wb *writeBufferBase) SealSegments(ctx context.Context, segmentIDs []int64) error {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	return wb.sealSegments(ctx, segmentIDs)
}

func (wb *writeBufferBase) DropPartitions(partitionIDs []int64) {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	wb.dropPartitions(partitionIDs)
}

func (wb *writeBufferBase) SetFlushTimestamp(flushTs uint64) {
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
	log := wb.logger
	wb.mut.Lock()
	defer wb.mut.Unlock()

	// need valid checkpoint before triggering syncing
	if wb.checkpoint == nil {
		log.Warn("evict buffer before buffering data")
		return
	}

	ts := wb.checkpoint.GetTimestamp()

	segmentIDs := wb.getSegmentsToSync(ts, policies...)
	if len(segmentIDs) > 0 {
		log.Info("evict buffer find segments to sync", zap.Int64s("segmentIDs", segmentIDs))
		conc.AwaitAll(wb.syncSegments(context.Background(), segmentIDs)...)
	}
}

func (wb *writeBufferBase) GetCheckpoint() *msgpb.MsgPosition {
	log := wb.cpRatedLogger
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
		log.RatedDebug(60, "checkpoint from latest consumed msg", zap.Uint64("cpTimestamp", wb.checkpoint.GetTimestamp()))
		return wb.checkpoint
	}

	log.RatedDebug(20, "checkpoint evaluated",
		zap.String("cpSource", checkpoint.source),
		zap.Int64("segmentID", checkpoint.segmentID),
		zap.Uint64("cpTimestamp", checkpoint.position.GetTimestamp()))
	return checkpoint.position
}

func (wb *writeBufferBase) triggerSync() (segmentIDs []int64) {
	segmentsToSync := wb.getSegmentsToSync(wb.checkpoint.GetTimestamp(), wb.syncPolicies...)
	if len(segmentsToSync) > 0 {
		log.Info("write buffer get segments to sync", zap.Int64s("segmentIDs", segmentsToSync))
		// ignore future here, use callback to handle error
		wb.syncSegments(context.Background(), segmentsToSync)
	}

	return segmentsToSync
}

func (wb *writeBufferBase) sealSegments(_ context.Context, segmentIDs []int64) error {
	for _, segmentID := range segmentIDs {
		_, ok := wb.metaCache.GetSegmentByID(segmentID)
		if !ok {
			log.Warn("cannot find segment when sealSegments", zap.Int64("segmentID", segmentID), zap.String("channel", wb.channelName))
			return merr.WrapErrSegmentNotFound(segmentID)
		}
	}
	// mark segment flushing if segment was growing
	wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Sealed),
		metacache.WithSegmentIDs(segmentIDs...),
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
	log := log.Ctx(ctx)
	result := make([]*conc.Future[struct{}], 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		syncTask, err := wb.getSyncTask(ctx, segmentID)
		if err != nil {
			if errors.Is(err, merr.ErrSegmentNotFound) {
				log.Warn("segment not found in meta", zap.Int64("segmentID", segmentID))
				continue
			} else {
				log.Fatal("failed to get sync task", zap.Int64("segmentID", segmentID), zap.Error(err))
			}
		}

		future, err := wb.syncMgr.SyncData(ctx, syncTask, func(err error) error {
			if wb.taskObserverCallback != nil {
				wb.taskObserverCallback(syncTask, err)
			}

			if err != nil {
				return err
			}

			if syncTask.StartPosition() != nil {
				wb.syncCheckpoint.Remove(syncTask.SegmentID(), syncTask.StartPosition().GetTimestamp())
			}

			if syncTask.IsFlush() {
				if paramtable.Get().DataNodeCfg.SkipBFStatsLoad.GetAsBool() || streamingutil.IsStreamingServiceEnabled() {
					wb.metaCache.RemoveSegments(metacache.WithSegmentIDs(syncTask.SegmentID()))
					log.Info("flushed segment removed", zap.Int64("segmentID", syncTask.SegmentID()), zap.String("channel", syncTask.ChannelName()))
				}
			}
			return nil
		})
		if err != nil {
			log.Fatal("failed to sync data", zap.Int64("segmentID", segmentID), zap.Error(err))
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
			log.Info("SyncPolicy selects segments", zap.Int64s("segmentIDs", result), zap.String("reason", policy.Reason()))
			segments.Insert(result...)
		}
	}

	return segments.Collect()
}

func (wb *writeBufferBase) getOrCreateBuffer(segmentID int64) *segmentBuffer {
	buffer, ok := wb.buffers[segmentID]
	if !ok {
		var err error
		buffer, err = newSegmentBuffer(segmentID, wb.collSchema)
		if err != nil {
			// TODO avoid panic here
			panic(err)
		}
		wb.buffers[segmentID] = buffer
	}

	return buffer
}

func (wb *writeBufferBase) yieldBuffer(segmentID int64) ([]*storage.InsertData, map[int64]*storage.BM25Stats, *storage.DeleteData, *TimeRange, *msgpb.MsgPosition) {
	buffer, ok := wb.buffers[segmentID]
	if !ok {
		return nil, nil, nil, nil, nil
	}

	// remove buffer and move it to sync manager
	delete(wb.buffers, segmentID)
	start := buffer.EarliestPosition()
	timeRange := buffer.GetTimeRange()
	insert, bm25, delta := buffer.Yield()

	return insert, bm25, delta, timeRange, start
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

func (wb *writeBufferBase) CreateNewGrowingSegment(partitionID int64, segmentID int64, startPos *msgpb.MsgPosition, storageVersion int64) {
	_, ok := wb.metaCache.GetSegmentByID(segmentID)
	// new segment
	if !ok {
		segmentInfo := &datapb.SegmentInfo{
			ID:             segmentID,
			PartitionID:    partitionID,
			CollectionID:   wb.collectionID,
			InsertChannel:  wb.channelName,
			StartPosition:  startPos,
			State:          commonpb.SegmentState_Growing,
			StorageVersion: storageVersion,
		}
		wb.metaCache.AddSegment(segmentInfo, func(_ *datapb.SegmentInfo) pkoracle.PkStat {
			return pkoracle.NewBloomFilterSetWithBatchSize(wb.getEstBatchSize())
		}, metacache.NewBM25StatsFactory, metacache.SetStartPosRecorded(false))
		log.Info("add growing segment", zap.Int64("segmentID", segmentID), zap.String("channel", wb.channelName))
	}
}

// bufferDelete buffers DeleteMsg into DeleteData.
func (wb *writeBufferBase) bufferDelete(segmentID int64, pks []storage.PrimaryKey, tss []typeutil.Timestamp, startPos, endPos *msgpb.MsgPosition) {
	segBuf := wb.getOrCreateBuffer(segmentID)
	bufSize := segBuf.deltaBuffer.Buffer(pks, tss, startPos, endPos)
	metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), fmt.Sprint(wb.collectionID)).Add(float64(bufSize))
}

func (wb *writeBufferBase) getSyncTask(ctx context.Context, segmentID int64) (syncmgr.Task, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("segmentID", segmentID),
	)
	segmentInfo, ok := wb.metaCache.GetSegmentByID(segmentID) // wb.metaCache.GetSegmentsBy(metacache.WithSegmentIDs(segmentID))
	if !ok {
		log.Warn("segment info not found in meta cache", zap.Int64("segmentID", segmentID))
		return nil, merr.WrapErrSegmentNotFound(segmentID)
	}
	var batchSize int64
	var totalMemSize float64 = 0
	var tsFrom, tsTo uint64

	insert, bm25, delta, timeRange, startPos := wb.yieldBuffer(segmentID)
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

	metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), fmt.Sprint(wb.collectionID)).Sub(totalMemSize)

	task := syncmgr.NewSyncTask().
		WithAllocator(wb.allocator).
		WithMetaWriter(wb.metaWriter).
		WithMetaCache(wb.metaCache).
		WithSyncPack(pack)
	return task, nil
}

// getEstBatchSize returns the batch size based on estimated size per record and FlushBufferSize configuration value.
func (wb *writeBufferBase) getEstBatchSize() uint {
	sizeLimit := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	return uint(sizeLimit / int64(wb.estSizePerRecord))
}

func (wb *writeBufferBase) Close(ctx context.Context, drop bool) {
	log := wb.logger
	// sink all data and call Drop for meta writer
	wb.mut.Lock()
	defer wb.mut.Unlock()
	if !drop {
		return
	}

	var futures []*conc.Future[struct{}]
	for id := range wb.buffers {
		syncTask, err := wb.getSyncTask(ctx, id)
		if err != nil {
			// TODO
			continue
		}
		switch t := syncTask.(type) {
		case *syncmgr.SyncTask:
			t.WithDrop()
		}

		f, err := wb.syncMgr.SyncData(ctx, syncTask, func(err error) error {
			if wb.taskObserverCallback != nil {
				wb.taskObserverCallback(syncTask, err)
			}

			if err != nil {
				return err
			}
			if syncTask.StartPosition() != nil {
				wb.syncCheckpoint.Remove(syncTask.SegmentID(), syncTask.StartPosition().GetTimestamp())
			}
			return nil
		})
		if err != nil {
			log.Fatal("failed to sync segment", zap.Int64("segmentID", id), zap.Error(err))
		}
		futures = append(futures, f)
	}

	err := conc.AwaitAll(futures...)
	if err != nil {
		log.Error("failed to sink write buffer data", zap.Error(err))
		// TODO change to remove channel in the future
		panic(err)
	}
	err = wb.metaWriter.DropChannel(ctx, wb.channelName)
	if err != nil {
		log.Error("failed to drop channel", zap.Error(err))
		// TODO change to remove channel in the future
		panic(err)
	}
}

// prepareInsert transfers InsertMsg into organized InsertData grouped by segmentID
// also returns primary key field data
func PrepareInsert(collSchema *schemapb.CollectionSchema, pkField *schemapb.FieldSchema, insertMsgs []*msgstream.InsertMsg) ([]*InsertData, error) {
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
				log.Warn("failed to transfer insert msg to insert data", zap.Error(err))
				return nil, err
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
