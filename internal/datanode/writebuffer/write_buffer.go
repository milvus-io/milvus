package writebuffer

import (
	"context"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// WriteBuffer is the interface for channel write buffer.
// It provides abstraction for channel write buffer and pk bloom filter & L0 delta logic.
type WriteBuffer interface {
	// HasSegment checks whether certain segment exists in this buffer.
	HasSegment(segmentID int64) bool
	// BufferData is the method to buffer dml data msgs.
	BufferData(insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error
	// FlushSegments is the method to perform `Sync` operation with provided options.
	FlushSegments(ctx context.Context, segmentIDs []int64) error
	// Close is the method to close and sink current buffer data.
	Close()
}

func NewWriteBuffer(schema *schemapb.CollectionSchema, metacache metacache.MetaCache, syncMgr syncmgr.SyncManager, opts ...WriteBufferOption) (WriteBuffer, error) {
	option := defaultWBOption()
	option.syncPolicies = append(option.syncPolicies, GetFlushingSegmentsPolicy(metacache))
	for _, opt := range opts {
		opt(option)
	}

	switch option.deletePolicy {
	case DeletePolicyBFPkOracle:
		return NewBFWriteBuffer(schema, metacache, syncMgr, option)
	case DeletePolicyL0Delta:
		return NewL0WriteBuffer(schema, metacache, syncMgr, option)
	default:
		return nil, merr.WrapErrParameterInvalid("valid delete policy config", option.deletePolicy)
	}
}

// writeBufferBase is the common component for buffering data
type writeBufferBase struct {
	mut sync.RWMutex

	collectionID int64

	collSchema *schemapb.CollectionSchema
	metaCache  metacache.MetaCache
	syncMgr    syncmgr.SyncManager
	broker     broker.Broker
	buffers    map[int64]*segmentBuffer // segmentID => segmentBuffer

	syncPolicies []SyncPolicy
	checkpoint   *msgpb.MsgPosition
}

func newWriteBufferBase(sch *schemapb.CollectionSchema, metacache metacache.MetaCache, syncMgr syncmgr.SyncManager, option *writeBufferOption) *writeBufferBase {
	return &writeBufferBase{
		collSchema:   sch,
		syncMgr:      syncMgr,
		broker:       option.broker,
		buffers:      make(map[int64]*segmentBuffer),
		metaCache:    metacache,
		syncPolicies: option.syncPolicies,
	}
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

func (wb *writeBufferBase) triggerAutoSync() error {
	segmentsToSync := wb.getSegmentsToSync(wb.checkpoint.GetTimestamp())
	if len(segmentsToSync) > 0 {
		log.Info("write buffer get segments to sync", zap.Int64s("segmentIDs", segmentsToSync))
		err := wb.syncSegments(context.Background(), segmentsToSync)
		if err != nil {
			log.Warn("segment segments failed", zap.Int64s("segmentIDs", segmentsToSync), zap.Error(err))
			return err
		}
	}

	return nil
}

func (wb *writeBufferBase) flushSegments(ctx context.Context, segmentIDs []int64) error {
	// mark segment flushing if segment was growing
	wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing),
		metacache.WithSegmentIDs(segmentIDs...),
		metacache.WithSegmentState(commonpb.SegmentState_Growing))
	return nil
}

func (wb *writeBufferBase) syncSegments(ctx context.Context, segmentIDs []int64) error {
	log := log.Ctx(ctx)
	for _, segmentID := range segmentIDs {
		infos := wb.metaCache.GetSegmentsBy(metacache.WithSegmentIDs(segmentID))
		if len(infos) == 0 {
			log.Warn("segment info not found in meta cache", zap.Int64("segmentID", segmentID))
			continue
		}
		segmentInfo := infos[0]

		buffer, exist := wb.getBuffer(segmentID)

		var insert *storage.InsertData
		var delta *storage.DeleteData
		if exist {
			insert, delta = buffer.Renew()
		}

		wb.metaCache.UpdateSegments(metacache.RollStats(), metacache.WithSegmentIDs(segmentID))

		syncTask := syncmgr.NewSyncTask().
			WithInsertData(insert).
			WithDeleteData(delta).
			WithCollectionID(wb.collectionID).
			WithPartitionID(segmentInfo.PartitionID()).
			WithSegmentID(segmentID).
			WithCheckpoint(wb.checkpoint).
			WithSchema(wb.collSchema).
			WithMetaWriter(syncmgr.BrokerMetaWriter(wb.broker)).
			WithFailureCallback(func(err error) {
				// TODO could change to unsub channel in the future
				panic(err)
			})

		// update flush& drop state
		switch segmentInfo.State() {
		case commonpb.SegmentState_Flushing:
			syncTask.WithFlush()
		case commonpb.SegmentState_Dropped:
			syncTask.WithDrop()
		}

		err := wb.syncMgr.SyncData(ctx, syncTask)
		if err != nil {
			return err
		}
	}
	return nil
}

// getSegmentsToSync applies all policies to get segments list to sync.
// **NOTE** shall be invoked within mutex protection
func (wb *writeBufferBase) getSegmentsToSync(ts typeutil.Timestamp) []int64 {
	buffers := lo.Values(wb.buffers)
	segments := typeutil.NewSet[int64]()
	for _, policy := range wb.syncPolicies {
		segments.Insert(policy(buffers, ts)...)
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

func (wb *writeBufferBase) getBuffer(segmentID int64) (*segmentBuffer, bool) {
	buffer, ok := wb.buffers[segmentID]
	return buffer, ok
}

// bufferInsert transform InsertMsg into bufferred InsertData and returns primary key field data for future usage.
func (wb *writeBufferBase) bufferInsert(insertMsgs []*msgstream.InsertMsg, startPos, endPos *msgpb.MsgPosition) (map[int64][]storage.FieldData, error) {
	insertGroups := lo.GroupBy(insertMsgs, func(msg *msgstream.InsertMsg) int64 { return msg.GetSegmentID() })
	segmentPKData := make(map[int64][]storage.FieldData)

	for segmentID, msgs := range insertGroups {
		segBuf := wb.getOrCreateBuffer(segmentID)

		pkData, err := segBuf.insertBuffer.Buffer(msgs, startPos, endPos)
		if err != nil {
			log.Warn("failed to buffer insert data", zap.Int64("segmentID", segmentID), zap.Error(err))
			return nil, err
		}
		segmentPKData[segmentID] = pkData
	}

	return segmentPKData, nil
}

// bufferDelete buffers DeleteMsg into DeleteData.
func (wb *writeBufferBase) bufferDelete(segmentID int64, pks []storage.PrimaryKey, tss []typeutil.Timestamp, startPos, endPos *msgpb.MsgPosition) error {
	segBuf := wb.getOrCreateBuffer(segmentID)
	segBuf.deltaBuffer.Buffer(pks, tss, startPos, endPos)
	return nil
}

func (wb *writeBufferBase) Close() {
}
