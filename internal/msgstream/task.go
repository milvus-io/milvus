package msgstream

import (
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type MsgType = internalPb.MsgType

type TsMsg interface {
	BeginTs() Timestamp
	EndTs() Timestamp
	Type() MsgType
	HashKeys() []int32
}

type BaseMsg struct {
	BeginTimestamp Timestamp
	EndTimestamp Timestamp
	HashValues []int32
}

func (bm *BaseMsg) BeginTs() Timestamp{
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

/////////////////////////////////////////Delete//////////////////////////////////////////
type DeleteMsg struct {
	BaseMsg
	internalPb.DeleteRequest
}

func (dt *DeleteMsg) Type() MsgType {
	return dt.MsgType
}

/////////////////////////////////////////Search//////////////////////////////////////////
type SearchMsg struct {
	BaseMsg
	internalPb.SearchRequest
}

func (st *SearchMsg) Type() MsgType {
	return st.MsgType
}

/////////////////////////////////////////SearchResult//////////////////////////////////////////
type SearchResultMsg struct {
	BaseMsg
	internalPb.SearchResult
}

func (srt *SearchResultMsg) Type() MsgType {
	return srt.MsgType
}

/////////////////////////////////////////TimeTick//////////////////////////////////////////
type TimeTickMsg struct {
	BaseMsg
	internalPb.TimeTickMsg
}

func (tst *TimeTickMsg) Type() MsgType {
	return tst.MsgType
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
