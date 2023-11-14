package writebuffer

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

type l0WriteBuffer struct {
	*writeBufferBase

	l0Segments map[int64]int64 // partitionID => l0 segment ID

	syncMgr     syncmgr.SyncManager
	idAllocator allocator.Interface
}

func NewL0WriteBuffer(channel string, sch *schemapb.CollectionSchema, metacache metacache.MetaCache, syncMgr syncmgr.SyncManager, option *writeBufferOption) (WriteBuffer, error) {
	if option.idAllocator == nil {
		return nil, merr.WrapErrServiceInternal("id allocator is nil when creating l0 write buffer")
	}
	return &l0WriteBuffer{
		l0Segments:      make(map[int64]int64),
		writeBufferBase: newWriteBufferBase(channel, sch, metacache, syncMgr, option),
		syncMgr:         syncMgr,
		idAllocator:     option.idAllocator,
	}, nil
}

func (wb *l0WriteBuffer) BufferData(insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error {
	wb.mut.Lock()
	defer wb.mut.Unlock()

	// process insert msgs
	_, err := wb.bufferInsert(insertMsgs, startPos, endPos)
	if err != nil {
		log.Warn("failed to buffer insert data", zap.Error(err))
		return err
	}

	for _, msg := range deleteMsgs {
		l0SegmentID := wb.getL0SegmentID(msg.GetPartitionID())
		pks := storage.ParseIDs2PrimaryKeys(msg.GetPrimaryKeys())
		err := wb.bufferDelete(l0SegmentID, pks, msg.GetTimestamps(), startPos, endPos)
		if err != nil {
			log.Warn("failed to buffer delete data", zap.Error(err))
			return err
		}
	}

	// update buffer last checkpoint
	wb.checkpoint = endPos

	return wb.triggerAutoSync()
}

func (wb *l0WriteBuffer) getL0SegmentID(partitionID int64) int64 {
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
	}
	return segmentID
}
