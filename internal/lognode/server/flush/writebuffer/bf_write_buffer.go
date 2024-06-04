package writebuffer

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/lognode/server/flush/meta"
	"github.com/milvus-io/milvus/internal/lognode/server/flush/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type bfWriteBuffer struct {
	*writeBufferBase

	syncMgr   syncmgr.SyncManager
	metacache meta.MetaCache
}

func NewBFWriteBuffer(channel string, metacache meta.MetaCache, syncMgr syncmgr.SyncManager, option *writeBufferOption) (WriteBuffer, error) {
	base, err := newWriteBufferBase(channel, metacache, syncMgr, option)
	if err != nil {
		return nil, err
	}
	return &bfWriteBuffer{
		writeBufferBase: base,
		syncMgr:         syncMgr,
	}, nil
}

func (wb *bfWriteBuffer) dispatchDeleteMsgs(groups []*inData, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) {
	// distribute delete msg for previous data
	for _, delMsg := range deleteMsgs {
		pks := storage.ParseIDs2PrimaryKeys(delMsg.GetPrimaryKeys())
		lcs := lo.Map(pks, func(pk storage.PrimaryKey, _ int) *storage.LocationsCache { return storage.NewLocationsCache(pk) })
		segments := wb.metaCache.GetSegmentsBy(meta.WithPartitionID(delMsg.PartitionID),
			meta.WithSegmentState(commonpb.SegmentState_Growing, commonpb.SegmentState_Flushing, commonpb.SegmentState_Flushed))
		for _, segment := range segments {
			if segment.CompactTo() != 0 {
				continue
			}
			var deletePks []storage.PrimaryKey
			var deleteTss []typeutil.Timestamp
			for idx, lc := range lcs {
				if segment.GetBloomFilterSet().PkExists(lc) {
					deletePks = append(deletePks, pks[idx])
					deleteTss = append(deleteTss, delMsg.GetTimestamps()[idx])
				}
			}
			if len(deletePks) > 0 {
				wb.bufferDelete(segment.SegmentID(), deletePks, deleteTss, startPos, endPos)
			}
		}

		for _, inData := range groups {
			if delMsg.GetPartitionID() == common.AllPartitionsID || delMsg.GetPartitionID() == inData.partitionID {
				var deletePks []storage.PrimaryKey
				var deleteTss []typeutil.Timestamp
				for idx, pk := range pks {
					ts := delMsg.GetTimestamps()[idx]
					if inData.pkExists(pk, ts) {
						deletePks = append(deletePks, pk)
						deleteTss = append(deleteTss, delMsg.GetTimestamps()[idx])
					}
				}
				if len(deletePks) > 0 {
					wb.bufferDelete(inData.segmentID, deletePks, deleteTss, startPos, endPos)
				}
			}
		}
	}
}

func (wb *bfWriteBuffer) BufferData(insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	groups, err := wb.prepareInsert(insertMsgs)
	if err != nil {
		return err
	}

	// buffer insert data and add segment if not exists
	for _, inData := range groups {
		err := wb.bufferInsert(inData, startPos, endPos)
		if err != nil {
			return err
		}
	}

	// distribute delete msg
	// bf write buffer check bloom filter of segment and current insert batch to decide which segment to write delete data
	wb.dispatchDeleteMsgs(groups, deleteMsgs, startPos, endPos)

	// update pk oracle
	for _, inData := range groups {
		// segment shall always exists after buffer insert
		segments := wb.metaCache.GetSegmentsBy(
			meta.WithSegmentIDs(inData.segmentID))
		for _, segment := range segments {
			for _, fieldData := range inData.pkField {
				err := segment.GetBloomFilterSet().UpdatePKRange(fieldData)
				if err != nil {
					return err
				}
			}
		}
	}

	// update buffer last checkpoint
	wb.checkpoint = endPos

	_ = wb.triggerSync()

	wb.cleanupCompactedSegments()
	return nil
}
