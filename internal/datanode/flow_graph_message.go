package datanode

import (
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

type (
	Msg          = flowgraph.Msg
	MsgStreamMsg = flowgraph.MsgStreamMsg
)

type (
	key2SegMsg struct {
		tsMessages []msgstream.TsMsg
		timeRange  TimeRange
	}

	ddMsg struct {
		// TODO: use collection id
		collectionRecords map[string][]metaOperateRecord
		// TODO: use partition id
		partitionRecords map[string][]metaOperateRecord
		flushMessages    []*flushMsg
		gcRecord         *gcRecord
		timeRange        TimeRange
	}

	metaOperateRecord struct {
		createOrDrop bool // create: true, drop: false
		timestamp    Timestamp
	}

	insertMsg struct {
		insertMessages []*msgstream.InsertMsg
		flushMessages  []*flushMsg
		gcRecord       *gcRecord
		timeRange      TimeRange
		startPositions []*internalpb2.MsgPosition
	}

	deleteMsg struct {
		deleteMessages []*msgstream.DeleteMsg
		timeRange      TimeRange
	}

	gcMsg struct {
		gcRecord  *gcRecord
		timeRange TimeRange
	}

	gcRecord struct {
		collections []UniqueID
	}

	flushMsg struct {
		msgID        UniqueID
		timestamp    Timestamp
		segmentIDs   []UniqueID
		collectionID UniqueID
	}
)

func (ksMsg *key2SegMsg) TimeTick() Timestamp {
	return ksMsg.timeRange.timestampMax
}

func (suMsg *ddMsg) TimeTick() Timestamp {
	return suMsg.timeRange.timestampMax
}

func (iMsg *insertMsg) TimeTick() Timestamp {
	return iMsg.timeRange.timestampMax
}

func (dMsg *deleteMsg) TimeTick() Timestamp {
	return dMsg.timeRange.timestampMax
}

func (gcMsg *gcMsg) TimeTick() Timestamp {
	return gcMsg.timeRange.timestampMax
}
