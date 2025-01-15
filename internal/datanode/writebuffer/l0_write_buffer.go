package writebuffer

import (
	"context"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	base, err := newWriteBufferBase(channel, metacache, storageV2Cache, syncMgr, option)
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

func (wb *l0WriteBuffer) dispatchDeleteMsgs(groups []*inData, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) {
	batchSize := paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt()
	split := func(pks []storage.PrimaryKey, pkTss []uint64, partitionSegments []*metacache.SegmentInfo, partitionGroups []*inData) []bool {
		lc := storage.NewBatchLocationsCache(pks)

		// use hits to cache result
		hits := make([]bool, len(pks))

		for _, segment := range partitionSegments {
			hits = segment.GetBloomFilterSet().BatchPkExistWithHits(lc, hits)
		}

		for _, inData := range partitionGroups {
			hits = inData.batchPkExists(pks, pkTss, hits)
		}

		return hits
	}

	type BatchApplyRet = struct {
		// represent the idx for delete msg in deleteMsgs
		DeleteDataIdx int
		// represent the start idx for the batch in each deleteMsg
		StartIdx int
		Hits     []bool
	}

	pksInDeleteMsgs := make([][]storage.PrimaryKey, len(deleteMsgs))
	retIdx := 0
	retMap := typeutil.NewConcurrentMap[int, *BatchApplyRet]()
	var futures []*conc.Future[any]
	pool := io.GetBFApplyPool()
	for didx, delMsg := range deleteMsgs {
		pks := storage.ParseIDs2PrimaryKeys(delMsg.GetPrimaryKeys())
		pksInDeleteMsgs[didx] = pks
		pkTss := delMsg.GetTimestamps()
		partitionSegments := wb.metaCache.GetSegmentsBy(metacache.WithPartitionID(delMsg.PartitionID),
			metacache.WithSegmentState(commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Flushed))
		partitionGroups := lo.Filter(groups, func(inData *inData, _ int) bool {
			return delMsg.GetPartitionID() == common.AllPartitionsID || delMsg.GetPartitionID() == inData.partitionID
		})

		for idx := 0; idx < len(pks); idx += batchSize {
			startIdx := idx
			endIdx := idx + batchSize
			if endIdx > len(pks) {
				endIdx = len(pks)
			}
			retIdx += 1
			tmpRetIdx := retIdx
			deleteMsgId := didx
			future := pool.Submit(func() (any, error) {
				hits := split(pks[startIdx:endIdx], pkTss[startIdx:endIdx], partitionSegments, partitionGroups)
				retMap.Insert(tmpRetIdx, &BatchApplyRet{
					DeleteDataIdx: deleteMsgId,
					StartIdx:      startIdx,
					Hits:          hits,
				})
				return nil, nil
			})
			futures = append(futures, future)
		}
	}
	conc.AwaitAll(futures...)

	retMap.Range(func(key int, value *BatchApplyRet) bool {
		l0SegmentID := wb.getL0SegmentID(deleteMsgs[value.DeleteDataIdx].GetPartitionID(), startPos)
		pks := pksInDeleteMsgs[value.DeleteDataIdx]
		pkTss := deleteMsgs[value.DeleteDataIdx].GetTimestamps()

		var deletePks []storage.PrimaryKey
		var deleteTss []typeutil.Timestamp
		for i, hit := range value.Hits {
			if hit {
				deletePks = append(deletePks, pks[value.StartIdx+i])
				deleteTss = append(deleteTss, pkTss[value.StartIdx+i])
			}
		}
		if len(deletePks) > 0 {
			wb.bufferDelete(l0SegmentID, deletePks, deleteTss, startPos, endPos)
		}
		return true
	})
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

func (wb *l0WriteBuffer) BufferData(insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error {
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

	if paramtable.Get().DataNodeCfg.SkipBFStatsLoad.GetAsBool() {
		// In Skip BF mode, datanode no longer maintains bloom filters.
		// So, here we skip filtering delete entries.
		wb.dispatchDeleteMsgsWithoutFilter(deleteMsgs, startPos, endPos)
	} else {
		// distribute delete msg
		// bf write buffer check bloom filter of segment and current insert batch to decide which segment to write delete data
		wb.dispatchDeleteMsgs(groups, deleteMsgs, startPos, endPos)
	}

	// update pk oracle
	for _, inData := range groups {
		// segment shall always exists after buffer insert
		segments := wb.metaCache.GetSegmentsBy(metacache.WithSegmentIDs(inData.segmentID))
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
		log.Info("Add a new level zero segment",
			zap.Int64("segmentID", segmentID),
			zap.String("level", datapb.SegmentLevel_L0.String()),
			zap.Any("start position", startPos),
		)
	}
	return segmentID
}
