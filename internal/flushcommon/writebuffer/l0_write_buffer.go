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

func (wb *l0WriteBuffer) dispatchDeleteMsgsWithoutFilter(deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error {
	for _, msg := range deleteMsgs {
		pks, pkTss, serializedExprPlan, isPredicateDelete, err := validateDeletePayload(msg)
		if err != nil {
			return err
		}
		if len(pks) == 0 && !isPredicateDelete {
			continue
		}

		l0SegmentID := wb.getL0SegmentID(msg.GetPartitionID(), startPos)
		if isPredicateDelete {
			wb.bufferPredicateDelete(l0SegmentID, serializedExprPlan, pkTss[0], startPos, endPos)
			continue
		}
		wb.bufferDelete(l0SegmentID, pks, pkTss, startPos, endPos)
	}
	return nil
}

// validateDeletePayload is the local WAL-shape guard before L0 buffering.
// storage.ParseIDs2PrimaryKeys only parses PK IDs and cannot express predicate
// deletes, so predicate payloads stay as serialized expr bytes instead of being
// wrapped in a fake PrimaryKey implementation.
func validateDeletePayload(msg *msgstream.DeleteMsg) ([]storage.PrimaryKey, []uint64, []byte, bool, error) {
	if msg == nil || msg.DeleteRequest == nil {
		return nil, nil, nil, false, merr.WrapErrServiceInternalMsg("delete message is nil")
	}

	serializedExprPlan := msg.GetSerializedExprPlan()
	pks := parseDeletePrimaryKeys(msg)
	tss := msg.GetTimestamps()
	if len(serializedExprPlan) > 0 {
		if len(pks) > 0 {
			return nil, nil, nil, false, merr.WrapErrServiceInternalMsg("predicate delete message must not contain primary keys")
		}
		if len(tss) != 1 {
			return nil, nil, nil, false, merr.WrapErrServiceInternalMsg("predicate delete message must contain exactly one timestamp, got %d", len(tss))
		}
		if msg.GetNumRows() != 0 {
			return nil, nil, nil, false, merr.WrapErrServiceInternalMsg("predicate delete message must have num_rows 0, got %d", msg.GetNumRows())
		}
		return nil, tss, serializedExprPlan, true, nil
	}

	if len(pks) == 0 {
		if len(tss) == 0 && msg.GetNumRows() == 0 {
			return nil, nil, nil, false, nil
		}
		return nil, nil, nil, false, merr.WrapErrServiceInternalMsg("primary-key delete message must contain primary keys")
	}
	if len(pks) != len(tss) {
		return nil, nil, nil, false, merr.WrapErrServiceInternalMsg("primary-key delete message primary key count %d does not match timestamp count %d", len(pks), len(tss))
	}
	if msg.GetNumRows() != 0 && msg.GetNumRows() != int64(len(pks)) {
		return nil, nil, nil, false, merr.WrapErrServiceInternalMsg("primary-key delete message num_rows %d does not match primary key count %d", msg.GetNumRows(), len(pks))
	}
	return pks, tss, nil, false, nil
}

func parseDeletePrimaryKeys(msg *msgstream.DeleteMsg) []storage.PrimaryKey {
	if ids := msg.GetPrimaryKeys(); ids != nil && ids.GetIdField() != nil {
		return storage.ParseIDs2PrimaryKeys(ids)
	}
	int64Pks := msg.GetInt64PrimaryKeys()
	if len(int64Pks) == 0 {
		return nil
	}
	pks := make([]storage.PrimaryKey, 0, len(int64Pks))
	for _, pk := range int64Pks {
		pks = append(pks, storage.NewInt64PrimaryKey(pk))
	}
	return pks
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
	if err := wb.dispatchDeleteMsgsWithoutFilter(deleteMsgs, startPos, endPos); err != nil {
		return err
	}
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
