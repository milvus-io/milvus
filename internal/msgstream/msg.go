package msgstream

import (
	"github.com/golang/protobuf/proto"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type MsgType = internalPb.MsgType

type TsMsg interface {
	BeginTs() Timestamp
	EndTs() Timestamp
	Type() MsgType
	HashKeys() []int32
	Marshal(TsMsg) ([]byte, error)
	Unmarshal([]byte) (TsMsg, error)
}

type BaseMsg struct {
	BeginTimestamp Timestamp
	EndTimestamp   Timestamp
	HashValues     []int32
}

func (bm *BaseMsg) BeginTs() Timestamp {
	return bm.BeginTimestamp
}

func (bm *BaseMsg) EndTs() Timestamp {
	return bm.EndTimestamp
}

func (bm *BaseMsg) HashKeys() []int32 {
	return bm.HashValues
}

/////////////////////////////////////////Insert//////////////////////////////////////////
type InsertMsg struct {
	BaseMsg
	internalPb.InsertRequest
}

func (it *InsertMsg) Type() MsgType {
	return it.MsgType
}

func (it *InsertMsg) Marshal(input TsMsg) ([]byte, error) {
	insertMsg := input.(*InsertMsg)
	insertRequest := &insertMsg.InsertRequest
	mb, err := proto.Marshal(insertRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (it *InsertMsg) Unmarshal(input []byte) (TsMsg, error) {
	insertRequest := internalPb.InsertRequest{}
	err := proto.Unmarshal(input, &insertRequest)
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

/////////////////////////////////////////Delete//////////////////////////////////////////
type DeleteMsg struct {
	BaseMsg
	internalPb.DeleteRequest
}

func (dt *DeleteMsg) Type() MsgType {
	return dt.MsgType
}

func (dt *DeleteMsg) Marshal(input TsMsg) ([]byte, error) {
	deleteTask := input.(*DeleteMsg)
	deleteRequest := &deleteTask.DeleteRequest
	mb, err := proto.Marshal(deleteRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dt *DeleteMsg) Unmarshal(input []byte) (TsMsg, error) {
	deleteRequest := internalPb.DeleteRequest{}
	err := proto.Unmarshal(input, &deleteRequest)
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
	internalPb.SearchRequest
}

func (st *SearchMsg) Type() MsgType {
	return st.MsgType
}

func (st *SearchMsg) Marshal(input TsMsg) ([]byte, error) {
	searchTask := input.(*SearchMsg)
	searchRequest := &searchTask.SearchRequest
	mb, err := proto.Marshal(searchRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (st *SearchMsg) Unmarshal(input []byte) (TsMsg, error) {
	searchRequest := internalPb.SearchRequest{}
	err := proto.Unmarshal(input, &searchRequest)
	if err != nil {
		return nil, err
	}
	searchMsg := &SearchMsg{SearchRequest: searchRequest}
	searchMsg.BeginTimestamp = searchMsg.Timestamp
	searchMsg.EndTimestamp = searchMsg.Timestamp

	return searchMsg, nil
}

/////////////////////////////////////////SearchResult//////////////////////////////////////////
type SearchResultMsg struct {
	BaseMsg
	internalPb.SearchResult
}

func (srt *SearchResultMsg) Type() MsgType {
	return srt.MsgType
}

func (srt *SearchResultMsg) Marshal(input TsMsg) ([]byte, error) {
	searchResultTask := input.(*SearchResultMsg)
	searchResultRequest := &searchResultTask.SearchResult
	mb, err := proto.Marshal(searchResultRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (srt *SearchResultMsg) Unmarshal(input []byte) (TsMsg, error) {
	searchResultRequest := internalPb.SearchResult{}
	err := proto.Unmarshal(input, &searchResultRequest)
	if err != nil {
		return nil, err
	}
	searchResultMsg := &SearchResultMsg{SearchResult: searchResultRequest}
	searchResultMsg.BeginTimestamp = searchResultMsg.Timestamp
	searchResultMsg.EndTimestamp = searchResultMsg.Timestamp

	return searchResultMsg, nil
}

/////////////////////////////////////////TimeTick//////////////////////////////////////////
type TimeTickMsg struct {
	BaseMsg
	internalPb.TimeTickMsg
}

func (tst *TimeTickMsg) Type() MsgType {
	return tst.MsgType
}

func (tst *TimeTickMsg) Marshal(input TsMsg) ([]byte, error) {
	timeTickTask := input.(*TimeTickMsg)
	timeTick := &timeTickTask.TimeTickMsg
	mb, err := proto.Marshal(timeTick)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (tst *TimeTickMsg) Unmarshal(input []byte) (TsMsg, error) {
	timeTickMsg := internalPb.TimeTickMsg{}
	err := proto.Unmarshal(input, &timeTickMsg)
	if err != nil {
		return nil, err
	}
	timeTick := &TimeTickMsg{TimeTickMsg: timeTickMsg}
	timeTick.BeginTimestamp = timeTick.Timestamp
	timeTick.EndTimestamp = timeTick.Timestamp

	return timeTick, nil
}

/////////////////////////////////////////QueryNodeSegStats//////////////////////////////////////////
type QueryNodeSegStatsMsg struct {
	BaseMsg
	internalPb.QueryNodeSegStats
}

func (qs *QueryNodeSegStatsMsg) Type() MsgType {
	return qs.MsgType
}

func (qs *QueryNodeSegStatsMsg) Marshal(input TsMsg) ([]byte, error) {
	queryNodeSegStatsTask := input.(*QueryNodeSegStatsMsg)
	queryNodeSegStats := &queryNodeSegStatsTask.QueryNodeSegStats
	mb, err := proto.Marshal(queryNodeSegStats)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (qs *QueryNodeSegStatsMsg) Unmarshal(input []byte) (TsMsg, error) {
	queryNodeSegStats := internalPb.QueryNodeSegStats{}
	err := proto.Unmarshal(input, &queryNodeSegStats)
	if err != nil {
		return nil, err
	}
	queryNodeSegStatsMsg := &QueryNodeSegStatsMsg{QueryNodeSegStats: queryNodeSegStats}

	return queryNodeSegStatsMsg, nil
}

///////////////////////////////////////////Key2Seg//////////////////////////////////////////
//type Key2SegMsg struct {
//	BaseMsg
//	internalPb.Key2SegMsg
//}
//
//func (k2st *Key2SegMsg) Type() MsgType {
//	return
//}

/////////////////////////////////////////CreateCollection//////////////////////////////////////////
type CreateCollectionMsg struct {
	BaseMsg
	internalPb.CreateCollectionRequest
}

func (cc *CreateCollectionMsg) Type() MsgType {
	return cc.MsgType
}

func (cc *CreateCollectionMsg) Marshal(input TsMsg) ([]byte, error) {
	createCollectionMsg := input.(*CreateCollectionMsg)
	createCollectionRequest := &createCollectionMsg.CreateCollectionRequest
	mb, err := proto.Marshal(createCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (cc *CreateCollectionMsg) Unmarshal(input []byte) (TsMsg, error) {
	createCollectionRequest := internalPb.CreateCollectionRequest{}
	err := proto.Unmarshal(input, &createCollectionRequest)
	if err != nil {
		return nil, err
	}
	createCollectionMsg := &CreateCollectionMsg{CreateCollectionRequest: createCollectionRequest}
	createCollectionMsg.BeginTimestamp = createCollectionMsg.Timestamp
	createCollectionMsg.EndTimestamp = createCollectionMsg.Timestamp

	return createCollectionMsg, nil
}

/////////////////////////////////////////DropCollection//////////////////////////////////////////
type DropCollectionMsg struct {
	BaseMsg
	internalPb.DropCollectionRequest
}

func (dc *DropCollectionMsg) Type() MsgType {
	return dc.MsgType
}

func (dc *DropCollectionMsg) Marshal(input TsMsg) ([]byte, error) {
	dropCollectionMsg := input.(*DropCollectionMsg)
	dropCollectionRequest := &dropCollectionMsg.DropCollectionRequest
	mb, err := proto.Marshal(dropCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dc *DropCollectionMsg) Unmarshal(input []byte) (TsMsg, error) {
	dropCollectionRequest := internalPb.DropCollectionRequest{}
	err := proto.Unmarshal(input, &dropCollectionRequest)
	if err != nil {
		return nil, err
	}
	dropCollectionMsg := &DropCollectionMsg{DropCollectionRequest: dropCollectionRequest}
	dropCollectionMsg.BeginTimestamp = dropCollectionMsg.Timestamp
	dropCollectionMsg.EndTimestamp = dropCollectionMsg.Timestamp

	return dropCollectionMsg, nil
}

/////////////////////////////////////////HasCollection//////////////////////////////////////////
type HasCollectionMsg struct {
	BaseMsg
	internalPb.HasCollectionRequest
}

func (hc *HasCollectionMsg) Type() MsgType {
	return hc.MsgType
}

func (hc *HasCollectionMsg) Marshal(input TsMsg) ([]byte, error) {
	hasCollectionMsg := input.(*HasCollectionMsg)
	hasCollectionRequest := &hasCollectionMsg.HasCollectionRequest
	mb, err := proto.Marshal(hasCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (hc *HasCollectionMsg) Unmarshal(input []byte) (TsMsg, error) {
	hasCollectionRequest := internalPb.HasCollectionRequest{}
	err := proto.Unmarshal(input, &hasCollectionRequest)
	if err != nil {
		return nil, err
	}
	hasCollectionMsg := &HasCollectionMsg{HasCollectionRequest: hasCollectionRequest}
	hasCollectionMsg.BeginTimestamp = hasCollectionMsg.Timestamp
	hasCollectionMsg.EndTimestamp = hasCollectionMsg.Timestamp

	return hasCollectionMsg, nil
}

/////////////////////////////////////////DescribeCollection//////////////////////////////////////////
type DescribeCollectionMsg struct {
	BaseMsg
	internalPb.DescribeCollectionRequest
}

func (dc *DescribeCollectionMsg) Type() MsgType {
	return dc.MsgType
}

func (dc *DescribeCollectionMsg) Marshal(input TsMsg) ([]byte, error) {
	describeCollectionMsg := input.(*DescribeCollectionMsg)
	describeCollectionRequest := &describeCollectionMsg.DescribeCollectionRequest
	mb, err := proto.Marshal(describeCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dc *DescribeCollectionMsg) Unmarshal(input []byte) (TsMsg, error) {
	describeCollectionRequest := internalPb.DescribeCollectionRequest{}
	err := proto.Unmarshal(input, &describeCollectionRequest)
	if err != nil {
		return nil, err
	}
	describeCollectionMsg := &DescribeCollectionMsg{DescribeCollectionRequest: describeCollectionRequest}
	describeCollectionMsg.BeginTimestamp = describeCollectionMsg.Timestamp
	describeCollectionMsg.EndTimestamp = describeCollectionMsg.Timestamp

	return describeCollectionMsg, nil
}

/////////////////////////////////////////ShowCollection//////////////////////////////////////////
type ShowCollectionMsg struct {
	BaseMsg
	internalPb.ShowCollectionRequest
}

func (sc *ShowCollectionMsg) Type() MsgType {
	return sc.MsgType
}

func (sc *ShowCollectionMsg) Marshal(input TsMsg) ([]byte, error) {
	showCollectionMsg := input.(*ShowCollectionMsg)
	showCollectionRequest := &showCollectionMsg.ShowCollectionRequest
	mb, err := proto.Marshal(showCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (sc *ShowCollectionMsg) Unmarshal(input []byte) (TsMsg, error) {
	showCollectionRequest := internalPb.ShowCollectionRequest{}
	err := proto.Unmarshal(input, &showCollectionRequest)
	if err != nil {
		return nil, err
	}
	showCollectionMsg := &ShowCollectionMsg{ShowCollectionRequest: showCollectionRequest}
	showCollectionMsg.BeginTimestamp = showCollectionMsg.Timestamp
	showCollectionMsg.EndTimestamp = showCollectionMsg.Timestamp

	return showCollectionMsg, nil
}

/////////////////////////////////////////CreatePartition//////////////////////////////////////////
type CreatePartitionMsg struct {
	BaseMsg
	internalPb.CreatePartitionRequest
}

func (cc *CreatePartitionMsg) Type() MsgType {
	return cc.MsgType
}

func (cc *CreatePartitionMsg) Marshal(input TsMsg) ([]byte, error) {
	createPartitionMsg := input.(*CreatePartitionMsg)
	createPartitionRequest := &createPartitionMsg.CreatePartitionRequest
	mb, err := proto.Marshal(createPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (cc *CreatePartitionMsg) Unmarshal(input []byte) (TsMsg, error) {
	createPartitionRequest := internalPb.CreatePartitionRequest{}
	err := proto.Unmarshal(input, &createPartitionRequest)
	if err != nil {
		return nil, err
	}
	createPartitionMsg := &CreatePartitionMsg{CreatePartitionRequest: createPartitionRequest}
	createPartitionMsg.BeginTimestamp = createPartitionMsg.Timestamp
	createPartitionMsg.EndTimestamp = createPartitionMsg.Timestamp

	return createPartitionMsg, nil
}

/////////////////////////////////////////DropPartition//////////////////////////////////////////
type DropPartitionMsg struct {
	BaseMsg
	internalPb.DropPartitionRequest
}

func (dc *DropPartitionMsg) Type() MsgType {
	return dc.MsgType
}

func (dc *DropPartitionMsg) Marshal(input TsMsg) ([]byte, error) {
	dropPartitionMsg := input.(*DropPartitionMsg)
	dropPartitionRequest := &dropPartitionMsg.DropPartitionRequest
	mb, err := proto.Marshal(dropPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dc *DropPartitionMsg) Unmarshal(input []byte) (TsMsg, error) {
	dropPartitionRequest := internalPb.DropPartitionRequest{}
	err := proto.Unmarshal(input, &dropPartitionRequest)
	if err != nil {
		return nil, err
	}
	dropPartitionMsg := &DropPartitionMsg{DropPartitionRequest: dropPartitionRequest}
	dropPartitionMsg.BeginTimestamp = dropPartitionMsg.Timestamp
	dropPartitionMsg.EndTimestamp = dropPartitionMsg.Timestamp

	return dropPartitionMsg, nil
}

/////////////////////////////////////////HasPartition//////////////////////////////////////////
type HasPartitionMsg struct {
	BaseMsg
	internalPb.HasPartitionRequest
}

func (hc *HasPartitionMsg) Type() MsgType {
	return hc.MsgType
}

func (hc *HasPartitionMsg) Marshal(input TsMsg) ([]byte, error) {
	hasPartitionMsg := input.(*HasPartitionMsg)
	hasPartitionRequest := &hasPartitionMsg.HasPartitionRequest
	mb, err := proto.Marshal(hasPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (hc *HasPartitionMsg) Unmarshal(input []byte) (TsMsg, error) {
	hasPartitionRequest := internalPb.HasPartitionRequest{}
	err := proto.Unmarshal(input, &hasPartitionRequest)
	if err != nil {
		return nil, err
	}
	hasPartitionMsg := &HasPartitionMsg{HasPartitionRequest: hasPartitionRequest}
	hasPartitionMsg.BeginTimestamp = hasPartitionMsg.Timestamp
	hasPartitionMsg.EndTimestamp = hasPartitionMsg.Timestamp

	return hasPartitionMsg, nil
}

/////////////////////////////////////////DescribePartition//////////////////////////////////////////
type DescribePartitionMsg struct {
	BaseMsg
	internalPb.DescribePartitionRequest
}

func (dc *DescribePartitionMsg) Type() MsgType {
	return dc.MsgType
}

func (dc *DescribePartitionMsg) Marshal(input TsMsg) ([]byte, error) {
	describePartitionMsg := input.(*DescribePartitionMsg)
	describePartitionRequest := &describePartitionMsg.DescribePartitionRequest
	mb, err := proto.Marshal(describePartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dc *DescribePartitionMsg) Unmarshal(input []byte) (TsMsg, error) {
	describePartitionRequest := internalPb.DescribePartitionRequest{}
	err := proto.Unmarshal(input, &describePartitionRequest)
	if err != nil {
		return nil, err
	}
	describePartitionMsg := &DescribePartitionMsg{DescribePartitionRequest: describePartitionRequest}
	describePartitionMsg.BeginTimestamp = describePartitionMsg.Timestamp
	describePartitionMsg.EndTimestamp = describePartitionMsg.Timestamp

	return describePartitionMsg, nil
}

/////////////////////////////////////////ShowPartition//////////////////////////////////////////
type ShowPartitionMsg struct {
	BaseMsg
	internalPb.ShowPartitionRequest
}

func (sc *ShowPartitionMsg) Type() MsgType {
	return sc.MsgType
}

func (sc *ShowPartitionMsg) Marshal(input TsMsg) ([]byte, error) {
	showPartitionMsg := input.(*ShowPartitionMsg)
	showPartitionRequest := &showPartitionMsg.ShowPartitionRequest
	mb, err := proto.Marshal(showPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (sc *ShowPartitionMsg) Unmarshal(input []byte) (TsMsg, error) {
	showPartitionRequest := internalPb.ShowPartitionRequest{}
	err := proto.Unmarshal(input, &showPartitionRequest)
	if err != nil {
		return nil, err
	}
	showPartitionMsg := &ShowPartitionMsg{ShowPartitionRequest: showPartitionRequest}
	showPartitionMsg.BeginTimestamp = showPartitionMsg.Timestamp
	showPartitionMsg.EndTimestamp = showPartitionMsg.Timestamp

	return showPartitionMsg, nil
}
