package msgstream

import (
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type MsgType = commonpb.MsgType
type MarshalType = interface{}

type TsMsg interface {
	ID() UniqueID
	BeginTs() Timestamp
	EndTs() Timestamp
	Type() MsgType
	HashKeys() []uint32
	Marshal(TsMsg) (MarshalType, error)
	Unmarshal(MarshalType) (TsMsg, error)
	Position() *MsgPosition
	SetPosition(*MsgPosition)
}

type BaseMsg struct {
	BeginTimestamp Timestamp
	EndTimestamp   Timestamp
	HashValues     []uint32
	MsgPosition    *MsgPosition
}

func (bm *BaseMsg) BeginTs() Timestamp {
	return bm.BeginTimestamp
}

func (bm *BaseMsg) EndTs() Timestamp {
	return bm.EndTimestamp
}

func (bm *BaseMsg) HashKeys() []uint32 {
	return bm.HashValues
}

func (bm *BaseMsg) Position() *MsgPosition {
	return bm.MsgPosition
}

func (bm *BaseMsg) SetPosition(position *MsgPosition) {
	bm.MsgPosition = position
}

func ConvertToByteArray(input interface{}) ([]byte, error) {
	switch output := input.(type) {
	case []byte:
		return output, nil
	default:
		return nil, errors.New("cannot convert interface{} to []byte")
	}
}

/////////////////////////////////////////Insert//////////////////////////////////////////
type InsertMsg struct {
	BaseMsg
	internalpb2.InsertRequest
}

func (it *InsertMsg) ID() UniqueID {
	return it.Base.MsgID
}

func (it *InsertMsg) Type() MsgType {
	return it.Base.MsgType
}

func (it *InsertMsg) Marshal(input TsMsg) (MarshalType, error) {
	insertMsg := input.(*InsertMsg)
	insertRequest := &insertMsg.InsertRequest
	mb, err := proto.Marshal(insertRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (it *InsertMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	insertRequest := internalpb2.InsertRequest{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &insertRequest)
	if err != nil {
		return nil, err
	}
	insertMsg := &InsertMsg{InsertRequest: insertRequest}
	for _, timestamp := range insertMsg.Timestamps {
		insertMsg.BeginTimestamp = timestamp
		insertMsg.EndTimestamp = timestamp
		break
	}
	for _, timestamp := range insertMsg.Timestamps {
		if timestamp > insertMsg.EndTimestamp {
			insertMsg.EndTimestamp = timestamp
		}
		if timestamp < insertMsg.BeginTimestamp {
			insertMsg.BeginTimestamp = timestamp
		}
	}

	return insertMsg, nil
}

/////////////////////////////////////////FlushCompletedMsg//////////////////////////////////////////
type FlushCompletedMsg struct {
	BaseMsg
	internalpb2.SegmentFlushCompletedMsg
}

func (fl *FlushCompletedMsg) ID() UniqueID {
	return fl.Base.MsgID
}

func (fl *FlushCompletedMsg) Type() MsgType {
	return fl.Base.MsgType
}

func (fl *FlushCompletedMsg) Marshal(input TsMsg) (MarshalType, error) {
	flushCompletedMsgTask := input.(*FlushCompletedMsg)
	flushCompletedMsg := &flushCompletedMsgTask.SegmentFlushCompletedMsg
	mb, err := proto.Marshal(flushCompletedMsg)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (fl *FlushCompletedMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	flushCompletedMsg := internalpb2.SegmentFlushCompletedMsg{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &flushCompletedMsg)
	if err != nil {
		return nil, err
	}
	flushCompletedMsgTask := &FlushCompletedMsg{SegmentFlushCompletedMsg: flushCompletedMsg}
	flushCompletedMsgTask.BeginTimestamp = flushCompletedMsgTask.Base.Timestamp
	flushCompletedMsgTask.EndTimestamp = flushCompletedMsgTask.Base.Timestamp

	return flushCompletedMsgTask, nil
}

/////////////////////////////////////////Flush//////////////////////////////////////////
// GOOSE TODO remove this
type FlushMsg struct {
	BaseMsg
	internalpb2.FlushMsg
}

func (fl *FlushMsg) ID() UniqueID {
	return fl.Base.MsgID
}

func (fl *FlushMsg) Type() MsgType {
	return fl.Base.MsgType
}

func (fl *FlushMsg) Marshal(input TsMsg) (MarshalType, error) {
	flushMsgTask := input.(*FlushMsg)
	flushMsg := &flushMsgTask.FlushMsg
	mb, err := proto.Marshal(flushMsg)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (fl *FlushMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	flushMsg := internalpb2.FlushMsg{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &flushMsg)
	if err != nil {
		return nil, err
	}
	flushMsgTask := &FlushMsg{FlushMsg: flushMsg}
	flushMsgTask.BeginTimestamp = flushMsgTask.Base.Timestamp
	flushMsgTask.EndTimestamp = flushMsgTask.Base.Timestamp

	return flushMsgTask, nil
}

/////////////////////////////////////////Delete//////////////////////////////////////////
type DeleteMsg struct {
	BaseMsg
	internalpb2.DeleteRequest
}

func (dt *DeleteMsg) ID() UniqueID {
	return dt.Base.MsgID
}

func (dt *DeleteMsg) Type() MsgType {
	return dt.Base.MsgType
}

func (dt *DeleteMsg) Marshal(input TsMsg) (MarshalType, error) {
	deleteMsg := input.(*DeleteMsg)
	deleteRequest := &deleteMsg.DeleteRequest
	mb, err := proto.Marshal(deleteRequest)
	if err != nil {
		return nil, err
	}

	return mb, nil
}

func (dt *DeleteMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	deleteRequest := internalpb2.DeleteRequest{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &deleteRequest)
	if err != nil {
		return nil, err
	}
	deleteMsg := &DeleteMsg{DeleteRequest: deleteRequest}
	for _, timestamp := range deleteMsg.Timestamps {
		deleteMsg.BeginTimestamp = timestamp
		deleteMsg.EndTimestamp = timestamp
		break
	}
	for _, timestamp := range deleteMsg.Timestamps {
		if timestamp > deleteMsg.EndTimestamp {
			deleteMsg.EndTimestamp = timestamp
		}
		if timestamp < deleteMsg.BeginTimestamp {
			deleteMsg.BeginTimestamp = timestamp
		}
	}

	return deleteMsg, nil
}

/////////////////////////////////////////Search//////////////////////////////////////////
type SearchMsg struct {
	BaseMsg
	internalpb2.SearchRequest
}

func (st *SearchMsg) ID() UniqueID {
	return st.Base.MsgID
}

func (st *SearchMsg) Type() MsgType {
	return st.Base.MsgType
}

func (st *SearchMsg) Marshal(input TsMsg) (MarshalType, error) {
	searchTask := input.(*SearchMsg)
	searchRequest := &searchTask.SearchRequest
	mb, err := proto.Marshal(searchRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (st *SearchMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	searchRequest := internalpb2.SearchRequest{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &searchRequest)
	if err != nil {
		return nil, err
	}
	searchMsg := &SearchMsg{SearchRequest: searchRequest}
	searchMsg.BeginTimestamp = searchMsg.Base.Timestamp
	searchMsg.EndTimestamp = searchMsg.Base.Timestamp

	return searchMsg, nil
}

/////////////////////////////////////////SearchResult//////////////////////////////////////////
type SearchResultMsg struct {
	BaseMsg
	internalpb2.SearchResults
}

func (srt *SearchResultMsg) ID() UniqueID {
	return srt.Base.MsgID
}

func (srt *SearchResultMsg) Type() MsgType {
	return srt.Base.MsgType
}

func (srt *SearchResultMsg) Marshal(input TsMsg) (MarshalType, error) {
	searchResultTask := input.(*SearchResultMsg)
	searchResultRequest := &searchResultTask.SearchResults
	mb, err := proto.Marshal(searchResultRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (srt *SearchResultMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	searchResultRequest := internalpb2.SearchResults{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &searchResultRequest)
	if err != nil {
		return nil, err
	}
	searchResultMsg := &SearchResultMsg{SearchResults: searchResultRequest}
	searchResultMsg.BeginTimestamp = searchResultMsg.Base.Timestamp
	searchResultMsg.EndTimestamp = searchResultMsg.Base.Timestamp

	return searchResultMsg, nil
}

/////////////////////////////////////////TimeTick//////////////////////////////////////////
type TimeTickMsg struct {
	BaseMsg
	internalpb2.TimeTickMsg
}

func (tst *TimeTickMsg) ID() UniqueID {
	return tst.Base.MsgID
}

func (tst *TimeTickMsg) Type() MsgType {
	return tst.Base.MsgType
}

func (tst *TimeTickMsg) Marshal(input TsMsg) (MarshalType, error) {
	timeTickTask := input.(*TimeTickMsg)
	timeTick := &timeTickTask.TimeTickMsg
	mb, err := proto.Marshal(timeTick)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (tst *TimeTickMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	timeTickMsg := internalpb2.TimeTickMsg{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &timeTickMsg)
	if err != nil {
		return nil, err
	}
	timeTick := &TimeTickMsg{TimeTickMsg: timeTickMsg}
	timeTick.BeginTimestamp = timeTick.Base.Timestamp
	timeTick.EndTimestamp = timeTick.Base.Timestamp

	return timeTick, nil
}

/////////////////////////////////////////QueryNodeStats//////////////////////////////////////////
// GOOSE TODO: remove QueryNodeStats
type QueryNodeStatsMsg struct {
	BaseMsg
	internalpb2.QueryNodeStats
}

func (qs *QueryNodeStatsMsg) ID() UniqueID {
	return qs.Base.MsgID
}

func (qs *QueryNodeStatsMsg) Type() MsgType {
	return qs.Base.MsgType
}

func (qs *QueryNodeStatsMsg) Marshal(input TsMsg) (MarshalType, error) {
	queryNodeSegStatsTask := input.(*QueryNodeStatsMsg)
	queryNodeSegStats := &queryNodeSegStatsTask.QueryNodeStats
	mb, err := proto.Marshal(queryNodeSegStats)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (qs *QueryNodeStatsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	queryNodeSegStats := internalpb2.QueryNodeStats{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &queryNodeSegStats)
	if err != nil {
		return nil, err
	}
	queryNodeSegStatsMsg := &QueryNodeStatsMsg{QueryNodeStats: queryNodeSegStats}

	return queryNodeSegStatsMsg, nil
}

/////////////////////////////////////////SegmentStatisticsMsg//////////////////////////////////////////
type SegmentStatisticsMsg struct {
	BaseMsg
	internalpb2.SegmentStatistics
}

func (ss *SegmentStatisticsMsg) ID() UniqueID {
	return ss.Base.MsgID
}

func (ss *SegmentStatisticsMsg) Type() MsgType {
	return ss.Base.MsgType
}

func (ss *SegmentStatisticsMsg) Marshal(input TsMsg) (MarshalType, error) {
	segStatsTask := input.(*SegmentStatisticsMsg)
	segStats := &segStatsTask.SegmentStatistics
	mb, err := proto.Marshal(segStats)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (ss *SegmentStatisticsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	segStats := internalpb2.SegmentStatistics{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &segStats)
	if err != nil {
		return nil, err
	}
	segStatsMsg := &SegmentStatisticsMsg{SegmentStatistics: segStats}

	return segStatsMsg, nil
}

///////////////////////////////////////////Key2Seg//////////////////////////////////////////
//type Key2SegMsg struct {
//	BaseMsg
//	internalpb2.Key2SegMsg
//}
//
//func (k2st *Key2SegMsg) Type() MsgType {
//	return
//}

/////////////////////////////////////////CreateCollection//////////////////////////////////////////
type CreateCollectionMsg struct {
	BaseMsg
	internalpb2.CreateCollectionRequest
}

func (cc *CreateCollectionMsg) ID() UniqueID {
	return cc.Base.MsgID
}

func (cc *CreateCollectionMsg) Type() MsgType {
	return cc.Base.MsgType
}

func (cc *CreateCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	createCollectionMsg := input.(*CreateCollectionMsg)
	createCollectionRequest := &createCollectionMsg.CreateCollectionRequest
	mb, err := proto.Marshal(createCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (cc *CreateCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createCollectionRequest := internalpb2.CreateCollectionRequest{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &createCollectionRequest)
	if err != nil {
		return nil, err
	}
	createCollectionMsg := &CreateCollectionMsg{CreateCollectionRequest: createCollectionRequest}
	createCollectionMsg.BeginTimestamp = createCollectionMsg.Base.Timestamp
	createCollectionMsg.EndTimestamp = createCollectionMsg.Base.Timestamp

	return createCollectionMsg, nil
}

/////////////////////////////////////////DropCollection//////////////////////////////////////////
type DropCollectionMsg struct {
	BaseMsg
	internalpb2.DropCollectionRequest
}

func (dc *DropCollectionMsg) ID() UniqueID {
	return dc.Base.MsgID
}

func (dc *DropCollectionMsg) Type() MsgType {
	return dc.Base.MsgType
}

func (dc *DropCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropCollectionMsg := input.(*DropCollectionMsg)
	dropCollectionRequest := &dropCollectionMsg.DropCollectionRequest
	mb, err := proto.Marshal(dropCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dc *DropCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropCollectionRequest := internalpb2.DropCollectionRequest{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &dropCollectionRequest)
	if err != nil {
		return nil, err
	}
	dropCollectionMsg := &DropCollectionMsg{DropCollectionRequest: dropCollectionRequest}
	dropCollectionMsg.BeginTimestamp = dropCollectionMsg.Base.Timestamp
	dropCollectionMsg.EndTimestamp = dropCollectionMsg.Base.Timestamp

	return dropCollectionMsg, nil
}

/////////////////////////////////////////CreatePartition//////////////////////////////////////////
type CreatePartitionMsg struct {
	BaseMsg
	internalpb2.CreatePartitionRequest
}

func (cc *CreatePartitionMsg) ID() UniqueID {
	return cc.Base.MsgID
}

func (cc *CreatePartitionMsg) Type() MsgType {
	return cc.Base.MsgType
}

func (cc *CreatePartitionMsg) Marshal(input TsMsg) (MarshalType, error) {
	createPartitionMsg := input.(*CreatePartitionMsg)
	createPartitionRequest := &createPartitionMsg.CreatePartitionRequest
	mb, err := proto.Marshal(createPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (cc *CreatePartitionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createPartitionRequest := internalpb2.CreatePartitionRequest{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &createPartitionRequest)
	if err != nil {
		return nil, err
	}
	createPartitionMsg := &CreatePartitionMsg{CreatePartitionRequest: createPartitionRequest}
	createPartitionMsg.BeginTimestamp = createPartitionMsg.Base.Timestamp
	createPartitionMsg.EndTimestamp = createPartitionMsg.Base.Timestamp

	return createPartitionMsg, nil
}

/////////////////////////////////////////DropPartition//////////////////////////////////////////
type DropPartitionMsg struct {
	BaseMsg
	internalpb2.DropPartitionRequest
}

func (dc *DropPartitionMsg) ID() UniqueID {
	return dc.Base.MsgID
}

func (dc *DropPartitionMsg) Type() MsgType {
	return dc.Base.MsgType
}

func (dc *DropPartitionMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropPartitionMsg := input.(*DropPartitionMsg)
	dropPartitionRequest := &dropPartitionMsg.DropPartitionRequest
	mb, err := proto.Marshal(dropPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dc *DropPartitionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropPartitionRequest := internalpb2.DropPartitionRequest{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &dropPartitionRequest)
	if err != nil {
		return nil, err
	}
	dropPartitionMsg := &DropPartitionMsg{DropPartitionRequest: dropPartitionRequest}
	dropPartitionMsg.BeginTimestamp = dropPartitionMsg.Base.Timestamp
	dropPartitionMsg.EndTimestamp = dropPartitionMsg.Base.Timestamp

	return dropPartitionMsg, nil
}

/////////////////////////////////////////LoadIndex//////////////////////////////////////////
type LoadIndexMsg struct {
	BaseMsg
	internalpb2.LoadIndex
}

func (lim *LoadIndexMsg) ID() UniqueID {
	return lim.Base.MsgID
}

func (lim *LoadIndexMsg) Type() MsgType {
	return lim.Base.MsgType
}

func (lim *LoadIndexMsg) Marshal(input TsMsg) (MarshalType, error) {
	loadIndexMsg := input.(*LoadIndexMsg)
	loadIndexRequest := &loadIndexMsg.LoadIndex
	mb, err := proto.Marshal(loadIndexRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (lim *LoadIndexMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	loadIndexRequest := internalpb2.LoadIndex{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &loadIndexRequest)
	if err != nil {
		return nil, err
	}
	loadIndexMsg := &LoadIndexMsg{LoadIndex: loadIndexRequest}

	return loadIndexMsg, nil
}

/////////////////////////////////////////SegmentInfoMsg//////////////////////////////////////////
type SegmentInfoMsg struct {
	BaseMsg
	datapb.SegmentMsg
}

func (sim *SegmentInfoMsg) ID() UniqueID {
	return sim.Base.MsgID
}

func (sim *SegmentInfoMsg) Type() MsgType {
	return sim.Base.MsgType
}

func (sim *SegmentInfoMsg) Marshal(input TsMsg) (MarshalType, error) {
	segInfoMsg := input.(*SegmentInfoMsg)
	mb, err := proto.Marshal(&segInfoMsg.SegmentMsg)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (sim *SegmentInfoMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	segMsg := datapb.SegmentMsg{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &segMsg)
	if err != nil {
		return nil, err
	}
	return &SegmentInfoMsg{
		SegmentMsg: segMsg,
	}, nil
}
