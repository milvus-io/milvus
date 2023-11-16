package writebuffer

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type bfWriteBuffer struct {
	*writeBufferBase

	syncMgr   syncmgr.SyncManager
	metacache metacache.MetaCache
}

func NewBFWriteBuffer(channel string, metacache metacache.MetaCache, syncMgr syncmgr.SyncManager, option *writeBufferOption) (WriteBuffer, error) {
	return &bfWriteBuffer{
		writeBufferBase: newWriteBufferBase(channel, metacache, syncMgr, option),
		syncMgr:         syncMgr,
	}, nil
}

func (wb *bfWriteBuffer) BufferData(insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	// process insert msgs
	pkData, err := wb.bufferInsert(insertMsgs, startPos, endPos)
	if err != nil {
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

	// distribute delete msg
	for _, delMsg := range deleteMsgs {
		pks := storage.ParseIDs2PrimaryKeys(delMsg.GetPrimaryKeys())
		segments := wb.metaCache.GetSegmentsBy(metacache.WithPartitionID(delMsg.PartitionID),
			metacache.WithSegmentState(commonpb.SegmentState_Growing, commonpb.SegmentState_Flushing, commonpb.SegmentState_Flushed))
		for _, segment := range segments {
			var deletePks []storage.PrimaryKey
			var deleteTss []typeutil.Timestamp
			for idx, pk := range pks {
				if segment.GetBloomFilterSet().PkExists(pk) {
					deletePks = append(deletePks, pk)
					deleteTss = append(deleteTss, delMsg.GetTimestamps()[idx])
				}
			}
			if len(deletePks) > 0 {
				wb.bufferDelete(segment.SegmentID(), deletePks, deleteTss, startPos, endPos)
			}
		}
	}

	// update buffer last checkpoint
	wb.checkpoint = endPos

	_ = wb.triggerSync()
	return nil
}
