package querynode

import (
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

type Msg = flowgraph.Msg
type MsgStreamMsg = flowgraph.MsgStreamMsg

type key2SegMsg struct {
	tsMessages []msgstream.TsMsg
	timeRange  TimeRange
}

type ddMsg struct {
	collectionRecords map[string][]metaOperateRecord
	partitionRecords  map[string][]metaOperateRecord
	timeRange         TimeRange
}

type metaOperateRecord struct {
	createOrDrop bool // create: true, drop: false
	timestamp    Timestamp
}

type insertMsg struct {
	insertMessages []*msgstream.InsertMsg
	timeRange      TimeRange
}

type deleteMsg struct {
	deleteMessages []*msgstream.DeleteMsg
	timeRange      TimeRange
}

type serviceTimeMsg struct {
	timeRange TimeRange
}

type DeleteData struct {
	deleteIDs        map[UniqueID][]UniqueID
	deleteTimestamps map[UniqueID][]Timestamp
	deleteOffset     map[UniqueID]int64
}

type DeleteRecord struct {
	entityID  UniqueID
	timestamp Timestamp
	segmentID UniqueID
}

type DeletePreprocessData struct {
	deleteRecords []*DeleteRecord
	count         int32
}

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

func (stMsg *serviceTimeMsg) TimeTick() Timestamp {
	return stMsg.timeRange.timestampMax
}

func (stMsg *serviceTimeMsg) DownStreamNodeIdx() int {
	return 0
}
