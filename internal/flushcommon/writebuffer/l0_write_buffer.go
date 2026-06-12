package writebuffer

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type l0WriteBuffer struct {
	*writeBufferBase

	l0Segments  map[int64]int64 // partitionID => l0 segment ID
	l0partition map[int64]int64 // l0 segment id => partition id

	syncMgr     syncmgr.SyncManager
	idAllocator allocator.Interface
}

func NewL0WriteBuffer(channel string, metacache metacache.MetaCache, syncMgr syncmgr.SyncManager, option *writeBufferOption) (WriteBuffer, error) {
	if option.idAllocator == nil {
		return nil, merr.WrapErrServiceInternal("id allocator is nil when creating l0 write buffer")
	}
	base, err := newWriteBufferBase(channel, metacache, syncMgr, option)
	if err != nil {
		return nil, err
	}
	return &l0WriteBuffer{
		l0Segments:      make(map[int64]int64),
		l0partition:     make(map[int64]int64),
		writeBufferBase: base,
		syncMgr:         syncMgr,
		idAllocator:     option.idAllocator,
	}, nil
}

func (wb *l0WriteBuffer) dispatchDeleteMsgsWithoutFilter(deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) {
	for _, msg := range deleteMsgs {
		l0SegmentID := wb.getL0SegmentID(msg.GetPartitionID(), startPos)
		pks := storage.ParseIDs2PrimaryKeys(msg.GetPrimaryKeys())
		pkTss := msg.GetTimestamps()
		if len(pks) > 0 {
			wb.bufferDelete(l0SegmentID, pks, pkTss, startPos, endPos)
		}
	}
}

func (wb *l0WriteBuffer) BufferData(insertData []*InsertData, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition, schemaVersion int32) error {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	for _, inData := range insertData {
		if wb.useGrowingSourceFlush {
			targetOffset := wb.growingSourceTargetOffset(inData.segmentID, inData.rowNum)
			decision := wb.decideGrowingFlushSource(inData.segmentID, targetOffset, endPos)
			if decision.sourceType == metacache.FlushSourceGrowing {
				wb.recordGrowingSourceProgress(inData, startPos, endPos, schemaVersion, targetOffset)
				continue
			}
		}

		err := wb.bufferInsert(inData, startPos, endPos, schemaVersion)
		if err != nil {
			return err
		}
	}

	// In streaming service mode, flushed segments no longer maintain a bloom filter.
	// So, here we skip generating BF (growing segment's BF will be regenerated during the sync phase)
	// and also skip filtering delete entries by bf.
	wb.dispatchDeleteMsgsWithoutFilter(deleteMsgs, startPos, endPos)
	// update buffer last checkpoint
	wb.checkpoint = endPos
	wb.updateProcessedTsLocked(endPos.GetTimestamp())

	segmentsSync := wb.triggerSync()
	for _, segment := range segmentsSync {
		partition, ok := wb.l0partition[segment]
		if ok {
			delete(wb.l0partition, segment)
			delete(wb.l0Segments, partition)
		}
	}

	return nil
}

// bufferInsert function InsertMsg into bufferred InsertData and returns primary key field data for future usage.
func (wb *l0WriteBuffer) bufferInsert(inData *InsertData, startPos, endPos *msgpb.MsgPosition, schemaVersion int32) error {
	wb.CreateNewGrowingSegment(inData.partitionID, inData.segmentID, startPos, schemaVersion)
	segBuf := wb.getOrCreateBuffer(inData.segmentID, startPos.GetTimestamp())

	totalMemSize := segBuf.insertBuffer.Buffer(inData, startPos, endPos)
	wb.metaCache.UpdateSegments(metacache.SegmentActions(
		metacache.UpdateBufferedRows(segBuf.insertBuffer.rows),
		metacache.SetStartPositionIfNil(startPos),
	), metacache.WithSegmentIDs(inData.segmentID))

	metrics.DataNodeFlowGraphBufferDataSize.WithLabelValues(paramtable.GetStringNodeID(), fmt.Sprint(wb.collectionID)).Add(float64(totalMemSize))

	return nil
}

func (wb *l0WriteBuffer) getL0SegmentID(partitionID int64, startPos *msgpb.MsgPosition) int64 {
	log := wb.logger
	segmentID, ok := wb.l0Segments[partitionID]
	if !ok {
		err := retry.Do(context.Background(), func() error {
			var err error
			segmentID, err = wb.idAllocator.AllocOne()
			return err
		})
		if err != nil {
			log.Error(context.TODO(), "failed to allocate l0 segment ID", mlog.Err(err))
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
		}, func(_ *datapb.SegmentInfo) pkoracle.PkStat { return pkoracle.NewBloomFilterSet() }, metacache.NoneBm25StatsFactory, metacache.SetStartPosRecorded(false))
		log.Info(context.TODO(), "Add a new level zero segment",
			mlog.FieldSegmentID(segmentID),
			mlog.String("level", datapb.SegmentLevel_L0.String()),
			mlog.Any("start position", startPos),
		)
	}
	return segmentID
}
