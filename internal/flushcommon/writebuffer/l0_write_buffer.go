package writebuffer

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type l0WriteBuffer struct {
	*writeBufferBase
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
		writeBufferBase: base,
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
	// Every timetick that carries data is also the retry clock for this channel:
	// a flush that failed earlier gets its next attempt here, ahead of the new
	// data, so the segment's queue is always replayed from its oldest task.
	wb.driveRetries(wb.syncCtx)

	wb.mut.Lock()
	if wb.closed || wb.dropping {
		wb.mut.Unlock()
		return merr.WrapErrChannelNotFound(wb.channelName)
	}

	for _, inData := range insertData {
		// One call for both modes: the payload chosen at buffer creation
		// (owned vs ref) decides where the bytes live.
		if err := wb.bufferInsert(inData, startPos, endPos, schemaVersion); err != nil {
			wb.mut.Unlock()
			return err
		}
	}

	// In streaming service mode, flushed segments no longer maintain a bloom filter.
	// So, here we skip generating BF (growing segment's BF will be regenerated during the sync phase)
	// and also skip filtering delete entries by bf.
	wb.dispatchDeleteMsgsWithoutFilter(deleteMsgs, startPos, endPos)
	wb.checkpoint = endPos

	segmentsSync := wb.triggerSync()
	wb.mut.Unlock()

	if len(segmentsSync) > 0 {
		wb.syncSegments(wb.syncCtx, segmentsSync)
	}

	return wb.waitFlushCapacity()
}

func (wb *l0WriteBuffer) getL0SegmentID(partitionID int64, startPos *msgpb.MsgPosition) int64 {
	log := wb.logger
	segmentID, ok := wb.l0Segments[partitionID]
	if !ok {
		err := retry.Do(context.Background(), func() error {
			var err error
			segmentID, err = wb.allocator.AllocOne()
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
