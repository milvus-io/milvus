package querynodeimp

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
	gcRecord          *gcRecord
	timeRange         TimeRange
}

type metaOperateRecord struct {
	createOrDrop bool // create: true, drop: false
	timestamp    Timestamp
}

type insertMsg struct {
	insertMessages []*msgstream.InsertMsg
	gcRecord       *gcRecord
	timeRange      TimeRange
}

type deleteMsg struct {
	deleteMessages []*msgstream.DeleteMsg
	timeRange      TimeRange
}

type serviceTimeMsg struct {
	gcRecord  *gcRecord
	timeRange TimeRange
}

type gcMsg struct {
	gcRecord  *gcRecord
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

// TODO: replace partitionWithID by partition id
type partitionWithID struct {
	partitionTag string
	collectionID UniqueID
}

type gcRecord struct {
	// collections and partitions to be dropped
	collections []UniqueID
	// TODO: use partition id
	partitions []partitionWithID
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

func (stMsg *serviceTimeMsg) TimeTick() Timestamp {
	return stMsg.timeRange.timestampMax
}

func (gcMsg *gcMsg) TimeTick() Timestamp {
	return gcMsg.timeRange.timestampMax
}
