package writebuffer

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

type l0WriteBuffer struct {
	*writeBufferBase

	l0Segments  map[int64]int64 // partitionID => l0 segment ID
	l0partition map[int64]int64 // l0 segment id => partition id

	syncMgr     syncmgr.SyncManager
	idAllocator allocator.Interface
}

func NewL0WriteBuffer(channel string, metacache metacache.MetaCache, storageV2Cache *metacache.StorageV2Cache, syncMgr syncmgr.SyncManager, option *writeBufferOption) (WriteBuffer, error) {
	if option.idAllocator == nil {
		return nil, merr.WrapErrServiceInternal("id allocator is nil when creating l0 write buffer")
	}
	return &l0WriteBuffer{
		l0Segments:      make(map[int64]int64),
		l0partition:     make(map[int64]int64),
		writeBufferBase: newWriteBufferBase(channel, metacache, storageV2Cache, syncMgr, option),
		syncMgr:         syncMgr,
		idAllocator:     option.idAllocator,
	}, nil
}

func (wb *l0WriteBuffer) BufferData(insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	// process insert msgs
	pkData, err := wb.bufferInsert(insertMsgs, startPos, endPos)
	if err != nil {
		log.Warn("failed to buffer insert data", zap.Error(err))
		return err
	}

	// update pk oracle
	for segmentID, dataList := range pkData {
		segments := wb.metaCache.GetSegmentsBy(metacache.WithSegmentIDs(segmentID))
		for _, segment := range segments {
			for _, fieldData := range dataList {
				err := segment.GetBloomFilterSet().UpdatePKRange(fieldData)
				if err != nil {
					return err
				}
			}
		}
	}

	for _, msg := range deleteMsgs {
		l0SegmentID := wb.getL0SegmentID(msg.GetPartitionID(), startPos)
		pks := storage.ParseIDs2PrimaryKeys(msg.GetPrimaryKeys())
		err := wb.bufferDelete(l0SegmentID, pks, msg.GetTimestamps(), startPos, endPos)
		if err != nil {
			log.Warn("failed to buffer delete data", zap.Error(err))
			return err
		}
	}

	// update buffer last checkpoint
	wb.checkpoint = endPos

	segmentsSync := wb.triggerSync()
	for _, segment := range segmentsSync {
		partition, ok := wb.l0partition[segment]
		if ok {
			delete(wb.l0partition, segment)
			delete(wb.l0Segments, partition)
		}
	}

	wb.cleanupCompactedSegments()
	return nil
}

func (wb *l0WriteBuffer) getL0SegmentID(partitionID int64, startPos *msgpb.MsgPosition) int64 {
	segmentID, ok := wb.l0Segments[partitionID]
	if !ok {
		err := retry.Do(context.Background(), func() error {
			var err error
			segmentID, err = wb.idAllocator.AllocOne()
			return err
		})
		if err != nil {
			log.Error("failed to allocate l0 segment ID", zap.Error(err))
			panic(err)
		}
		wb.l0Segments[partitionID] = segmentID
		wb.l0partition[segmentID] = partitionID
		wb.metaCache.AddSegment(&datapb.SegmentInfo{
			ID:            segmentID,
			PartitionID:   partitionID,
			CollectionID:  wb.collectionID,
			InsertChannel: wb.channelName,
			StartPosition: startPos,
			State:         commonpb.SegmentState_Growing,
			Level:         datapb.SegmentLevel_L0,
		}, func(_ *datapb.SegmentInfo) *metacache.BloomFilterSet { return metacache.NewBloomFilterSet() }, metacache.SetStartPosRecorded(false))
	}
	return segmentID
}
