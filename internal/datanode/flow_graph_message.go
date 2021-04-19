package datanode

import (
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

type (
	Msg          = flowgraph.Msg
	MsgStreamMsg = flowgraph.MsgStreamMsg
)

type key2SegMsg struct {
	tsMessages []msgstream.TsMsg
	timeRange  TimeRange
}

type ddMsg struct {
	collectionRecords map[UniqueID][]*metaOperateRecord
	partitionRecords  map[UniqueID][]*metaOperateRecord
	flushMessages     []*flushMsg
	gcRecord          *gcRecord
	timeRange         TimeRange
}

type metaOperateRecord struct {
	createOrDrop bool // create: true, drop: false
	timestamp    Timestamp
}

type insertMsg struct {
	insertMessages []*msgstream.InsertMsg
	flushMessages  []*flushMsg
	gcRecord       *gcRecord
	timeRange      TimeRange
	startPositions []*internalpb.MsgPosition
	endPositions   []*internalpb.MsgPosition
}

type deleteMsg struct {
	deleteMessages []*msgstream.DeleteMsg
	timeRange      TimeRange
}

type gcMsg struct {
	gcRecord  *gcRecord
	timeRange TimeRange
}

type gcRecord struct {
	collections []UniqueID
}

type flushMsg struct {
	msgID        UniqueID
	timestamp    Timestamp
	segmentIDs   []UniqueID
	collectionID UniqueID
}

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
