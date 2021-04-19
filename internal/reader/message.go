package reader

import (
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

type Msg = flowgraph.Msg

type msgStreamMsg struct {
	tsMessages []*msgstream.TsMsg
	timeRange  TimeRange
}

type dmMsg struct {
	insertMessages []*msgstream.InsertTask
	// TODO: add delete message support
	// deleteMessages []*msgstream.DeleteTask
	timeRange TimeRange
}

type key2SegMsg struct {
	tsMessages []*msgstream.TsMsg
	timeRange  TimeRange
}

type schemaUpdateMsg struct {
	timeRange TimeRange
}

type filteredDmMsg struct {
	insertMessages []*msgstream.InsertTask
	// TODO: add delete message support
	// deleteMessages []*msgstream.DeleteTask
	timeRange TimeRange
}

type insertMsg struct {
	insertData InsertData
	timeRange  TimeRange
}

type deletePreprocessMsg struct {
	deletePreprocessData DeletePreprocessData
	timeRange            TimeRange
}

type deleteMsg struct {
	deleteData DeleteData
	timeRange  TimeRange
}

type serviceTimeMsg struct {
	timeRange TimeRange
}

type InsertData struct {
	insertIDs        map[UniqueID][]UniqueID
	insertTimestamps map[UniqueID][]Timestamp
	insertRecords    map[UniqueID][]*commonpb.Blob
	insertOffset     map[UniqueID]int64
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

func (msMsg *msgStreamMsg) TimeTick() Timestamp {
	return msMsg.timeRange.timestampMax
}

func (msMsg *msgStreamMsg) DownStreamNodeIdx() int {
	return 0
}

func (dmMsg *dmMsg) TimeTick() Timestamp {
	return dmMsg.timeRange.timestampMax
}

func (dmMsg *dmMsg) DownStreamNodeIdx() int {
	return 0
}

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

func (fdmMsg *filteredDmMsg) TimeTick() Timestamp {
	return fdmMsg.timeRange.timestampMax
}

func (fdmMsg *filteredDmMsg) DownStreamNodeIdx() int {
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

func (dpMsg *deletePreprocessMsg) TimeTick() Timestamp {
	return dpMsg.timeRange.timestampMax
}

func (dpMsg *deletePreprocessMsg) DownStreamNodeIdx() int {
	return 0
}

func (stMsg *serviceTimeMsg) TimeTick() Timestamp {
	return stMsg.timeRange.timestampMax
}

func (stMsg *serviceTimeMsg) DownStreamNodeIdx() int {
	return 0
}
