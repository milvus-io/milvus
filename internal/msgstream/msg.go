package msgstream

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type MsgType = commonpb.MsgType

type TsMsg interface {
	GetMsgContext() context.Context
	SetMsgContext(context.Context)
	BeginTs() Timestamp
	EndTs() Timestamp
	Type() MsgType
	HashKeys() []uint32
	Marshal(TsMsg) ([]byte, error)
	Unmarshal([]byte) (TsMsg, error)
}

type BaseMsg struct {
	MsgCtx         context.Context
	BeginTimestamp Timestamp
	EndTimestamp   Timestamp
	HashValues     []uint32
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

/////////////////////////////////////////Insert//////////////////////////////////////////
type InsertMsg struct {
	BaseMsg
	internalpb2.InsertRequest
}

func (it *InsertMsg) Type() MsgType {
	return it.Base.MsgType
}

func (it *InsertMsg) GetMsgContext() context.Context {
	return it.MsgCtx
}

func (it *InsertMsg) SetMsgContext(ctx context.Context) {
	it.MsgCtx = ctx
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
	insertRequest := internalpb2.InsertRequest{}
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

/////////////////////////////////////////Flush//////////////////////////////////////////
type FlushMsg struct {
	BaseMsg
	internalpb2.FlushMsg
}

func (fl *FlushMsg) Type() MsgType {
	return fl.Base.MsgType
}

func (fl *FlushMsg) GetMsgContext() context.Context {
	return fl.MsgCtx
}
func (fl *FlushMsg) SetMsgContext(ctx context.Context) {
	fl.MsgCtx = ctx
}

func (fl *FlushMsg) Marshal(input TsMsg) ([]byte, error) {
	flushMsgTask := input.(*FlushMsg)
	flushMsg := &flushMsgTask.FlushMsg
	mb, err := proto.Marshal(flushMsg)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (fl *FlushMsg) Unmarshal(input []byte) (TsMsg, error) {
	flushMsg := internalpb2.FlushMsg{}
	err := proto.Unmarshal(input, &flushMsg)
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

func (dt *DeleteMsg) Type() MsgType {
	return dt.Base.MsgType
}

func (dt *DeleteMsg) GetMsgContext() context.Context {
	return dt.MsgCtx
}

func (dt *DeleteMsg) SetMsgContext(ctx context.Context) {
	dt.MsgCtx = ctx
}

func (dt *DeleteMsg) Marshal(input TsMsg) ([]byte, error) {
	deleteMsg := input.(*DeleteMsg)
	deleteRequest := &deleteMsg.DeleteRequest
	mb, err := proto.Marshal(deleteRequest)
	if err != nil {
		return nil, err
	}

	return mb, nil
}

func (dt *DeleteMsg) Unmarshal(input []byte) (TsMsg, error) {
	deleteRequest := internalpb2.DeleteRequest{}
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
	internalpb2.SearchRequest
}

func (st *SearchMsg) Type() MsgType {
	return st.Base.MsgType
}

func (st *SearchMsg) GetMsgContext() context.Context {
	return st.MsgCtx
}

func (st *SearchMsg) SetMsgContext(ctx context.Context) {
	st.MsgCtx = ctx
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
	searchRequest := internalpb2.SearchRequest{}
	err := proto.Unmarshal(input, &searchRequest)
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

func (srt *SearchResultMsg) Type() MsgType {
	return srt.Base.MsgType
}

func (srt *SearchResultMsg) GetMsgContext() context.Context {
	return srt.MsgCtx
}

func (srt *SearchResultMsg) SetMsgContext(ctx context.Context) {
	srt.MsgCtx = ctx
}

func (srt *SearchResultMsg) Marshal(input TsMsg) ([]byte, error) {
	searchResultTask := input.(*SearchResultMsg)
	searchResultRequest := &searchResultTask.SearchResults
	mb, err := proto.Marshal(searchResultRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (srt *SearchResultMsg) Unmarshal(input []byte) (TsMsg, error) {
	searchResultRequest := internalpb2.SearchResults{}
	err := proto.Unmarshal(input, &searchResultRequest)
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

func (tst *TimeTickMsg) Type() MsgType {
	return tst.Base.MsgType
}

func (tst *TimeTickMsg) GetMsgContext() context.Context {
	return tst.MsgCtx
}

func (tst *TimeTickMsg) SetMsgContext(ctx context.Context) {
	tst.MsgCtx = ctx
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
	timeTickMsg := internalpb2.TimeTickMsg{}
	err := proto.Unmarshal(input, &timeTickMsg)
	if err != nil {
		return nil, err
	}
	timeTick := &TimeTickMsg{TimeTickMsg: timeTickMsg}
	timeTick.BeginTimestamp = timeTick.Base.Timestamp
	timeTick.EndTimestamp = timeTick.Base.Timestamp

	return timeTick, nil
}

/////////////////////////////////////////QueryNodeStats//////////////////////////////////////////
type QueryNodeStatsMsg struct {
	BaseMsg
	internalpb2.QueryNodeStats
}

func (qs *QueryNodeStatsMsg) Type() MsgType {
	return qs.Base.MsgType
}

func (qs *QueryNodeStatsMsg) GetMsgContext() context.Context {
	return qs.MsgCtx
}

func (qs *QueryNodeStatsMsg) SetMsgContext(ctx context.Context) {
	qs.MsgCtx = ctx
}

func (qs *QueryNodeStatsMsg) Marshal(input TsMsg) ([]byte, error) {
	queryNodeSegStatsTask := input.(*QueryNodeStatsMsg)
	queryNodeSegStats := &queryNodeSegStatsTask.QueryNodeStats
	mb, err := proto.Marshal(queryNodeSegStats)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (qs *QueryNodeStatsMsg) Unmarshal(input []byte) (TsMsg, error) {
	queryNodeSegStats := internalpb2.QueryNodeStats{}
	err := proto.Unmarshal(input, &queryNodeSegStats)
	if err != nil {
		return nil, err
	}
	queryNodeSegStatsMsg := &QueryNodeStatsMsg{QueryNodeStats: queryNodeSegStats}

	return queryNodeSegStatsMsg, nil
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

func (cc *CreateCollectionMsg) Type() MsgType {
	return cc.Base.MsgType
}

func (cc *CreateCollectionMsg) GetMsgContext() context.Context {
	return cc.MsgCtx
}

func (cc *CreateCollectionMsg) SetMsgContext(ctx context.Context) {
	cc.MsgCtx = ctx
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
	createCollectionRequest := internalpb2.CreateCollectionRequest{}
	err := proto.Unmarshal(input, &createCollectionRequest)
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

func (dc *DropCollectionMsg) Type() MsgType {
	return dc.Base.MsgType
}
func (dc *DropCollectionMsg) GetMsgContext() context.Context {
	return dc.MsgCtx
}

func (dc *DropCollectionMsg) SetMsgContext(ctx context.Context) {
	dc.MsgCtx = ctx
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
	dropCollectionRequest := internalpb2.DropCollectionRequest{}
	err := proto.Unmarshal(input, &dropCollectionRequest)
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

func (cc *CreatePartitionMsg) GetMsgContext() context.Context {
	return cc.MsgCtx
}

func (cc *CreatePartitionMsg) SetMsgContext(ctx context.Context) {
	cc.MsgCtx = ctx
}

func (cc *CreatePartitionMsg) Type() MsgType {
	return cc.Base.MsgType
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
	createPartitionRequest := internalpb2.CreatePartitionRequest{}
	err := proto.Unmarshal(input, &createPartitionRequest)
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

func (dc *DropPartitionMsg) GetMsgContext() context.Context {
	return dc.MsgCtx
}

func (dc *DropPartitionMsg) SetMsgContext(ctx context.Context) {
	dc.MsgCtx = ctx
}

func (dc *DropPartitionMsg) Type() MsgType {
	return dc.Base.MsgType
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
	dropPartitionRequest := internalpb2.DropPartitionRequest{}
	err := proto.Unmarshal(input, &dropPartitionRequest)
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

func (lim *LoadIndexMsg) Type() MsgType {
	return lim.Base.MsgType
}

func (lim *LoadIndexMsg) GetMsgContext() context.Context {
	return lim.MsgCtx
}

func (lim *LoadIndexMsg) SetMsgContext(ctx context.Context) {
	lim.MsgCtx = ctx
}

func (lim *LoadIndexMsg) Marshal(input TsMsg) ([]byte, error) {
	loadIndexMsg := input.(*LoadIndexMsg)
	loadIndexRequest := &loadIndexMsg.LoadIndex
	mb, err := proto.Marshal(loadIndexRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (lim *LoadIndexMsg) Unmarshal(input []byte) (TsMsg, error) {
	loadIndexRequest := internalpb2.LoadIndex{}
	err := proto.Unmarshal(input, &loadIndexRequest)
	if err != nil {
		return nil, err
	}
	loadIndexMsg := &LoadIndexMsg{LoadIndex: loadIndexRequest}

	return loadIndexMsg, nil
}
