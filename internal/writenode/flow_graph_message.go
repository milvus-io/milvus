package writenode

import (
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

type (
	Msg          = flowgraph.Msg
	MsgStreamMsg = flowgraph.MsgStreamMsg
	SegmentID    = UniqueID
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
		timeRange        TimeRange
	}

	metaOperateRecord struct {
		createOrDrop bool // create: true, drop: false
		timestamp    Timestamp
	}

	insertMsg struct {
		insertMessages []*msgstream.InsertMsg
		flushMessages  []*msgstream.FlushMsg
		timeRange      TimeRange
	}

	deleteMsg struct {
		deleteMessages []*msgstream.DeleteMsg
		timeRange      TimeRange
	}
)

func (ksMsg *key2SegMsg) TimeTick() Timestamp {
	return ksMsg.timeRange.timestampMax
}

func (ksMsg *key2SegMsg) DownStreamNodeIdx() int {
	return 0
}

func (suMsg *ddMsg) TimeTick() Timestamp {
	return suMsg.timeRange.timestampMax
}

func (suMsg *ddMsg) DownStreamNodeIdx() int {
	return 0
}

func (iMsg *insertMsg) TimeTick() Timestamp {
	return iMsg.timeRange.timestampMax
}

func (iMsg *insertMsg) DownStreamNodeIdx() int {
	return 0
}

func (dMsg *deleteMsg) TimeTick() Timestamp {
	return dMsg.timeRange.timestampMax
}

func (dMsg *deleteMsg) DownStreamNodeIdx() int {
	return 0
}
