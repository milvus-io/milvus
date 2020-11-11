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
	Marshal(*TsMsg) ([]byte, error)
	Unmarshal([]byte) (*TsMsg, error)
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

func (it *InsertMsg) Marshal(input *TsMsg) ([]byte, error) {
	insertMsg := (*input).(*InsertMsg)
	insertRequest := &insertMsg.InsertRequest
	mb, err := proto.Marshal(insertRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (it *InsertMsg) Unmarshal(input []byte) (*TsMsg, error) {
	insertRequest := internalPb.InsertRequest{}
	err := proto.Unmarshal(input, &insertRequest)
	insertMsg := &InsertMsg{InsertRequest: insertRequest}

	if err != nil {
		return nil, err
	}
	for _, timestamp := range insertMsg.Timestamps {
		it.BeginTimestamp = timestamp
		it.EndTimestamp = timestamp
		break
	}
	for _, timestamp := range insertMsg.Timestamps {
		if timestamp > it.EndTimestamp {
			it.EndTimestamp = timestamp
		}
		if timestamp < it.BeginTimestamp {
			it.BeginTimestamp = timestamp
		}
	}
	var tsMsg TsMsg = insertMsg
	return &tsMsg, nil
}

/////////////////////////////////////////Delete//////////////////////////////////////////
type DeleteMsg struct {
	BaseMsg
	internalPb.DeleteRequest
}

func (dt *DeleteMsg) Type() MsgType {
	return dt.MsgType
}

func (dt *DeleteMsg) Marshal(input *TsMsg) ([]byte, error) {
	deleteTask := (*input).(*DeleteMsg)
	deleteRequest := &deleteTask.DeleteRequest
	mb, err := proto.Marshal(deleteRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dt *DeleteMsg) Unmarshal(input []byte) (*TsMsg, error) {
	deleteRequest := internalPb.DeleteRequest{}
	err := proto.Unmarshal(input, &deleteRequest)
	deleteMsg := &DeleteMsg{DeleteRequest: deleteRequest}

	if err != nil {
		return nil, err
	}
	for _, timestamp := range deleteMsg.Timestamps {
		dt.BeginTimestamp = timestamp
		dt.EndTimestamp = timestamp
		break
	}
	for _, timestamp := range deleteMsg.Timestamps {
		if timestamp > dt.EndTimestamp {
			dt.EndTimestamp = timestamp
		}
		if timestamp < dt.BeginTimestamp {
			dt.BeginTimestamp = timestamp
		}
	}
	var tsMsg TsMsg = deleteMsg
	return &tsMsg, nil
}

/////////////////////////////////////////Search//////////////////////////////////////////
type SearchMsg struct {
	BaseMsg
	internalPb.SearchRequest
}

func (st *SearchMsg) Type() MsgType {
	return st.MsgType
}

func (st *SearchMsg) Marshal(input *TsMsg) ([]byte, error) {
	searchTask := (*input).(*SearchMsg)
	searchRequest := &searchTask.SearchRequest
	mb, err := proto.Marshal(searchRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (st *SearchMsg) Unmarshal(input []byte) (*TsMsg, error) {
	searchRequest := internalPb.SearchRequest{}
	err := proto.Unmarshal(input, &searchRequest)
	searchMsg := &SearchMsg{SearchRequest: searchRequest}

	if err != nil {
		return nil, err
	}
	st.BeginTimestamp = searchMsg.Timestamp
	st.EndTimestamp = searchMsg.Timestamp
	var tsMsg TsMsg = searchMsg
	return &tsMsg, nil
}

/////////////////////////////////////////SearchResult//////////////////////////////////////////
type SearchResultMsg struct {
	BaseMsg
	internalPb.SearchResult
}

func (srt *SearchResultMsg) Type() MsgType {
	return srt.MsgType
}

func (srt *SearchResultMsg) Marshal(input *TsMsg) ([]byte, error) {
	searchResultTask := (*input).(*SearchResultMsg)
	searchResultRequest := &searchResultTask.SearchResult
	mb, err := proto.Marshal(searchResultRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (srt *SearchResultMsg) Unmarshal(input []byte) (*TsMsg, error) {
	searchResultRequest := internalPb.SearchResult{}
	err := proto.Unmarshal(input, &searchResultRequest)
	searchResultMsg := &SearchResultMsg{SearchResult: searchResultRequest}

	if err != nil {
		return nil, err
	}
	srt.BeginTimestamp = searchResultMsg.Timestamp
	srt.EndTimestamp = searchResultMsg.Timestamp
	var tsMsg TsMsg = searchResultMsg
	return &tsMsg, nil
}

/////////////////////////////////////////TimeTick//////////////////////////////////////////
type TimeTickMsg struct {
	BaseMsg
	internalPb.TimeTickMsg
}

func (tst *TimeTickMsg) Type() MsgType {
	return tst.MsgType
}

func (tst *TimeTickMsg) Marshal(input *TsMsg) ([]byte, error) {
	timeTickTask := (*input).(*TimeTickMsg)
	timeTick := &timeTickTask.TimeTickMsg
	mb, err := proto.Marshal(timeTick)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (tst *TimeTickMsg) Unmarshal(input []byte) (*TsMsg, error) {
	timeTickMsg := internalPb.TimeTickMsg{}
	err := proto.Unmarshal(input, &timeTickMsg)
	timeTick := &TimeTickMsg{TimeTickMsg: timeTickMsg}

	if err != nil {
		return nil, err
	}
	tst.BeginTimestamp = timeTick.Timestamp
	tst.EndTimestamp = timeTick.Timestamp
	var tsMsg TsMsg = timeTick
	return &tsMsg, nil
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
