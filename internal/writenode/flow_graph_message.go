package writenode

import (
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
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

	schemaUpdateMsg struct {
		timeRange TimeRange
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

	serviceTimeMsg struct {
		timeRange TimeRange
	}

	InsertData struct {
		insertIDs        map[SegmentID][]UniqueID
		insertTimestamps map[SegmentID][]Timestamp
		insertRecords    map[SegmentID][]*commonpb.Blob
		insertOffset     map[SegmentID]int64
	}

	DeleteData struct {
		deleteIDs        map[SegmentID][]UniqueID
		deleteTimestamps map[SegmentID][]Timestamp
		deleteOffset     map[SegmentID]int64
	}

	DeleteRecord struct {
		entityID  UniqueID
		timestamp Timestamp
		segmentID UniqueID
	}

	DeletePreprocessData struct {
		deleteRecords []*DeleteRecord
		count         int32
	}
)

func (ksMsg *key2SegMsg) TimeTick() Timestamp {
	return ksMsg.timeRange.timestampMax
}

func (ksMsg *key2SegMsg) DownStreamNodeIdx() int {
	return 0
}

func (suMsg *schemaUpdateMsg) TimeTick() Timestamp {
	return suMsg.timeRange.timestampMax
}

func (suMsg *schemaUpdateMsg) DownStreamNodeIdx() int {
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
