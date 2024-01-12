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
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	nonFlushTS uint64 = 0
)

// WriteBuffer is the interface for channel write buffer.
// It provides abstraction for channel write buffer and pk bloom filter & L0 delta logic.
type WriteBuffer interface {
	// HasSegment checks whether certain segment exists in this buffer.
	HasSegment(segmentID int64) bool
	// BufferData is the method to buffer dml data msgs.
	BufferData(insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error
	// FlushTimestamp set flush timestamp for write buffer
	SetFlushTimestamp(flushTs uint64)
	// GetFlushTimestamp get current flush timestamp
	GetFlushTimestamp() uint64
	// FlushSegments is the method to perform `Sync` operation with provided options.
	FlushSegments(ctx context.Context, segmentIDs []int64) error
	// GetCheckpoint returns current channel checkpoint.
	// If there are any non-empty segment buffer, returns the earliest buffer start position.
	// Otherwise, returns latest buffered checkpoint.
	GetCheckpoint() *msgpb.MsgPosition
	// Close is the method to close and sink current buffer data.
	Close(drop bool)
}

func NewWriteBuffer(channel string, metacache metacache.MetaCache, storageV2Cache *metacache.StorageV2Cache, syncMgr syncmgr.SyncManager, opts ...WriteBufferOption) (WriteBuffer, error) {
	option := defaultWBOption(metacache)
	for _, opt := range opts {
		opt(option)
	}

	switch option.deletePolicy {
	case DeletePolicyBFPkOracle:
		return NewBFWriteBuffer(channel, metacache, storageV2Cache, syncMgr, option)
	case DeletePolicyL0Delta:
		return NewL0WriteBuffer(channel, metacache, storageV2Cache, syncMgr, option)
	default:
		return nil, merr.WrapErrParameterInvalid("valid delete policy config", option.deletePolicy)
	}
}

// writeBufferBase is the common component for buffering data
type writeBufferBase struct {
	mut sync.RWMutex

	collectionID int64
	channelName  string

	metaWriter       syncmgr.MetaWriter
	collSchema       *schemapb.CollectionSchema
	estSizePerRecord int
	metaCache        metacache.MetaCache
	syncMgr          syncmgr.SyncManager
	broker           broker.Broker
	serializer       syncmgr.Serializer

	buffers map[int64]*segmentBuffer // segmentID => segmentBuffer

	syncPolicies   []SyncPolicy
	checkpoint     *msgpb.MsgPosition
	flushTimestamp *atomic.Uint64

	storagev2Cache *metacache.StorageV2Cache
}

func newWriteBufferBase(channel string, metacache metacache.MetaCache, storageV2Cache *metacache.StorageV2Cache, syncMgr syncmgr.SyncManager, option *writeBufferOption) (*writeBufferBase, error) {
	flushTs := atomic.NewUint64(nonFlushTS)
	flushTsPolicy := GetFlushTsPolicy(flushTs, metacache)
	option.syncPolicies = append(option.syncPolicies, flushTsPolicy)

	var serializer syncmgr.Serializer
	var err error
	if params.Params.CommonCfg.EnableStorageV2.GetAsBool() {
		serializer, err = syncmgr.NewStorageV2Serializer(
			storageV2Cache,
			metacache,
			option.metaWriter,
		)
	} else {
		serializer, err = syncmgr.NewStorageSerializer(
			metacache,
			option.metaWriter,
		)
	}
	if err != nil {
		return nil, err
	}

	schema := metacache.Schema()
	estSize, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		return nil, err
	}

	return &writeBufferBase{
		channelName:      channel,
		collectionID:     metacache.Collection(),
		collSchema:       schema,
		estSizePerRecord: estSize,
		syncMgr:          syncMgr,
		metaWriter:       option.metaWriter,
		buffers:          make(map[int64]*segmentBuffer),
		metaCache:        metacache,
		serializer:       serializer,
		syncPolicies:     option.syncPolicies,
		flushTimestamp:   flushTs,
		storagev2Cache:   storageV2Cache,
	}, nil
}

func (wb *writeBufferBase) HasSegment(segmentID int64) bool {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	_, ok := wb.buffers[segmentID]
	return ok
}

func (wb *writeBufferBase) FlushSegments(ctx context.Context, segmentIDs []int64) error {
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	return wb.flushSegments(ctx, segmentIDs)
}

func (wb *writeBufferBase) SetFlushTimestamp(flushTs uint64) {
	wb.flushTimestamp.Store(flushTs)
}

func (wb *writeBufferBase) GetFlushTimestamp() uint64 {
	return wb.flushTimestamp.Load()
}

func (wb *writeBufferBase) GetCheckpoint() *msgpb.MsgPosition {
	log := log.Ctx(context.Background()).
		With(zap.String("channel", wb.channelName)).
		WithRateGroup(fmt.Sprintf("writebuffer_cp_%s", wb.channelName), 1, 60)
	wb.mut.RLock()
	defer wb.mut.RUnlock()

	// syncCandidate from sync manager
	syncSegmentID, syncCandidate := wb.syncMgr.GetEarliestPosition(wb.channelName)

	type checkpointCandidate struct {
		segmentID int64
		position  *msgpb.MsgPosition
	}
	var bufferCandidate *checkpointCandidate

	candidates := lo.MapToSlice(wb.buffers, func(_ int64, buf *segmentBuffer) *checkpointCandidate {
		return &checkpointCandidate{buf.segmentID, buf.EarliestPosition()}
	})
	candidates = lo.Filter(candidates, func(candidate *checkpointCandidate, _ int) bool {
		return candidate.position != nil
	})

	if len(candidates) > 0 {
		bufferCandidate = lo.MinBy(candidates, func(a, b *checkpointCandidate) bool {
			return a.position.GetTimestamp() < b.position.GetTimestamp()
		})
	}

	var checkpoint *msgpb.MsgPosition
	var segmentID int64
	var cpSource string
	switch {
	case bufferCandidate == nil && syncCandidate == nil:
		// all buffer are empty
		log.RatedInfo(60, "checkpoint from latest consumed msg")
		return wb.checkpoint
	case bufferCandidate == nil && syncCandidate != nil:
		checkpoint = syncCandidate
		segmentID = syncSegmentID
		cpSource = "syncManager"
	case syncCandidate == nil && bufferCandidate != nil:
		checkpoint = bufferCandidate.position
		segmentID = bufferCandidate.segmentID
		cpSource = "segmentBuffer"
	case syncCandidate.GetTimestamp() >= bufferCandidate.position.GetTimestamp():
		checkpoint = bufferCandidate.position
		segmentID = bufferCandidate.segmentID
		cpSource = "segmentBuffer"
	case syncCandidate.GetTimestamp() < bufferCandidate.position.GetTimestamp():
		checkpoint = syncCandidate
		segmentID = syncSegmentID
		cpSource = "syncManager"
	}

	log.RatedInfo(20, "checkpoint evaluated",
		zap.String("cpSource", cpSource),
		zap.Int64("segmentID", segmentID),
		zap.Uint64("cpTimestamp", checkpoint.GetTimestamp()))
	return checkpoint
}

func (wb *writeBufferBase) triggerSync() (segmentIDs []int64) {
	segmentsToSync := wb.getSegmentsToSync(wb.checkpoint.GetTimestamp())
	if len(segmentsToSync) > 0 {
		log.Info("write buffer get segments to sync", zap.Int64s("segmentIDs", segmentsToSync))
		wb.syncSegments(context.Background(), segmentsToSync)
	}

	return segmentsToSync
}

func (wb *writeBufferBase) cleanupCompactedSegments() {
	segmentIDs := wb.metaCache.GetSegmentIDsBy(metacache.WithCompacted(), metacache.WithNoSyncingTask())
	// remove compacted only when there is no writebuffer
	targetIDs := lo.Filter(segmentIDs, func(segmentID int64, _ int) bool {
		_, ok := wb.buffers[segmentID]
		return !ok
	})
	if len(targetIDs) == 0 {
		return
	}
	removed := wb.metaCache.RemoveSegments(metacache.WithSegmentIDs(targetIDs...))
	if len(removed) > 0 {
		log.Info("remove compacted segments", zap.Int64s("removed", removed))
	}
}

func (wb *writeBufferBase) flushSegments(ctx context.Context, segmentIDs []int64) error {
	// mark segment flushing if segment was growing
	wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing),
		metacache.WithSegmentIDs(segmentIDs...),
		metacache.WithSegmentState(commonpb.SegmentState_Growing))
	// mark segment flushing if segment was importing
	wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing),
		metacache.WithSegmentIDs(segmentIDs...),
		metacache.WithImporting())
	return nil
}

func (wb *writeBufferBase) syncSegments(ctx context.Context, segmentIDs []int64) {
	log := log.Ctx(ctx)
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

		// discard Future here, handle error in callback
		_ = wb.syncMgr.SyncData(ctx, syncTask)
	}
}

// getSegmentsToSync applies all policies to get segments list to sync.
// **NOTE** shall be invoked within mutex protection
func (wb *writeBufferBase) getSegmentsToSync(ts typeutil.Timestamp) []int64 {
	buffers := lo.Values(wb.buffers)
	segments := typeutil.NewSet[int64]()
	for _, policy := range wb.syncPolicies {
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

func (wb *writeBufferBase) yieldBuffer(segmentID int64) (*storage.InsertData, *storage.DeleteData, *TimeRange, *msgpb.MsgPosition) {
	buffer, ok := wb.buffers[segmentID]
	if !ok {
		return nil, nil, nil, nil
	}

	// remove buffer and move it to sync manager
	delete(wb.buffers, segmentID)
	start := buffer.EarliestPosition()
	timeRange := buffer.GetTimeRange()
	insert, delta := buffer.Yield()

	return insert, delta, timeRange, start
}

// bufferInsert transform InsertMsg into bufferred InsertData and returns primary key field data for future usage.
func (wb *writeBufferBase) bufferInsert(insertMsgs []*msgstream.InsertMsg, startPos, endPos *msgpb.MsgPosition) (map[int64][]storage.FieldData, error) {
	insertGroups := lo.GroupBy(insertMsgs, func(msg *msgstream.InsertMsg) int64 { return msg.GetSegmentID() })
	segmentPKData := make(map[int64][]storage.FieldData)
	segmentPartition := lo.SliceToMap(insertMsgs, func(msg *msgstream.InsertMsg) (int64, int64) { return msg.GetSegmentID(), msg.GetPartitionID() })

	for segmentID, msgs := range insertGroups {
		_, ok := wb.metaCache.GetSegmentByID(segmentID)
		// new segment
		if !ok {
			wb.metaCache.AddSegment(&datapb.SegmentInfo{
				ID:            segmentID,
				PartitionID:   segmentPartition[segmentID],
				CollectionID:  wb.collectionID,
				InsertChannel: wb.channelName,
				StartPosition: startPos,
				State:         commonpb.SegmentState_Growing,
			}, func(_ *datapb.SegmentInfo) *metacache.BloomFilterSet {
				return metacache.NewBloomFilterSetWithBatchSize(wb.getEstBatchSize())
			}, metacache.SetStartPosRecorded(false))
		}

		segBuf := wb.getOrCreateBuffer(segmentID)

		pkData, totalMemSize, err := segBuf.insertBuffer.Buffer(msgs, startPos, endPos)
		if err != nil {
			log.Warn("failed to buffer insert data", zap.Int64("segmentID", segmentID), zap.Error(err))
			return nil, err
		}
		segmentPKData[segmentID] = pkData
		wb.metaCache.UpdateSegments(metacache.UpdateBufferedRows(segBuf.insertBuffer.rows),
			metacache.WithSegmentIDs(segmentID))

		metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), fmt.Sprint(wb.collectionID)).Add(float64(totalMemSize))
	}

	return segmentPKData, nil
}

// bufferDelete buffers DeleteMsg into DeleteData.
func (wb *writeBufferBase) bufferDelete(segmentID int64, pks []storage.PrimaryKey, tss []typeutil.Timestamp, startPos, endPos *msgpb.MsgPosition) error {
	segBuf := wb.getOrCreateBuffer(segmentID)
	bufSize := segBuf.deltaBuffer.Buffer(pks, tss, startPos, endPos)
	metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), fmt.Sprint(wb.collectionID)).Add(float64(bufSize))
	return nil
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

	insert, delta, timeRange, startPos := wb.yieldBuffer(segmentID)
	if timeRange != nil {
		tsFrom, tsTo = timeRange.timestampMin, timeRange.timestampMax
	}

	actions := []metacache.SegmentAction{}
	if insert != nil {
		batchSize = int64(insert.GetRowNum())
		totalMemSize += float64(insert.GetMemorySize())
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
		WithCheckpoint(wb.checkpoint).
		WithBatchSize(batchSize)

	if segmentInfo.State() == commonpb.SegmentState_Flushing ||
		segmentInfo.Level() == datapb.SegmentLevel_L0 { // Level zero segment will always be sync as flushed
		pack.WithFlush()
	}

	metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), fmt.Sprint(wb.collectionID)).Sub(totalMemSize)

	return wb.serializer.EncodeBuffer(ctx, pack)
}

// getEstBatchSize returns the batch size based on estimated size per record and FlushBufferSize configuration value.
func (wb *writeBufferBase) getEstBatchSize() uint {
	sizeLimit := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	return uint(sizeLimit / int64(wb.estSizePerRecord))
}

func (wb *writeBufferBase) Close(drop bool) {
	// sink all data and call Drop for meta writer
	wb.mut.Lock()
	defer wb.mut.Unlock()
	if !drop {
		return
	}

	var futures []*conc.Future[error]
	for id := range wb.buffers {
		syncTask, err := wb.getSyncTask(context.Background(), id)
		if err != nil {
			// TODO
			continue
		}
		switch t := syncTask.(type) {
		case *syncmgr.SyncTask:
			t.WithDrop()
		case *syncmgr.SyncTaskV2:
			t.WithDrop()
		}

		f := wb.syncMgr.SyncData(context.Background(), syncTask)
		futures = append(futures, f)
	}

	err := conc.AwaitAll(futures...)
	if err != nil {
		log.Error("failed to sink write buffer data", zap.String("channel", wb.channelName), zap.Error(err))
		// TODO change to remove channel in the future
		panic(err)
	}
	err = wb.metaWriter.DropChannel(wb.channelName)
	if err != nil {
		log.Error("failed to drop channel", zap.String("channel", wb.channelName), zap.Error(err))
		// TODO change to remove channel in the future
		panic(err)
	}
}
