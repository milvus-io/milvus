package msgstream

import (
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type MsgType uint32

const (
	kInsert         MsgType = 1
	kDelete         MsgType = 2
	kSearch         MsgType = 3
	kSearchResult   MsgType = 4
	kTimeTick       MsgType = 5
	kSegmentStatics MsgType = 6
	kTimeSync       MsgType = 7
)

type TsMsg interface {
	SetTs(ts TimeStamp)
	Ts() TimeStamp
	Type() MsgType
}

/////////////////////////////////////////Insert//////////////////////////////////////////
type InsertTask struct {
	internalPb.InsertRequest
}

func (it InsertTask) SetTs(ts TimeStamp) {
	it.Timestamp = uint64(ts)
}

func (it InsertTask) Ts() TimeStamp {
	return TimeStamp(it.Timestamp)
}

func (it InsertTask) Type() MsgType {
	if it.ReqType == internalPb.ReqType_kNone {
		return kTimeTick
	}
	return kInsert
}

/////////////////////////////////////////Delete//////////////////////////////////////////
type DeleteTask struct {
	internalPb.DeleteRequest
}

func (dt DeleteTask) SetTs(ts TimeStamp) {
	dt.Timestamp = uint64(ts)
}

func (dt DeleteTask) Ts() TimeStamp {
	return TimeStamp(dt.Timestamp)
}

func (dt DeleteTask) Type() MsgType {
	if dt.ReqType == internalPb.ReqType_kNone {
		return kTimeTick
	}
	return kDelete
}

/////////////////////////////////////////Search//////////////////////////////////////////
type SearchTask struct {
	internalPb.SearchRequest
}

func (st SearchTask) SetTs(ts TimeStamp) {
	st.Timestamp = uint64(ts)
}

func (st SearchTask) Ts() TimeStamp {
	return TimeStamp(st.Timestamp)
}

func (st SearchTask) Type() MsgType {
	if st.ReqType == internalPb.ReqType_kNone {
		return kTimeTick
	}
	return kSearch
}

/////////////////////////////////////////SearchResult//////////////////////////////////////////
type SearchResultTask struct {
	internalPb.SearchResult
}

func (srt SearchResultTask) SetTs(ts TimeStamp) {
	srt.Timestamp = uint64(ts)
}

func (srt SearchResultTask) Ts() TimeStamp {
	return TimeStamp(srt.Timestamp)
}

func (srt SearchResultTask) Type() MsgType {
	return kSearchResult
}

/////////////////////////////////////////TimeSync//////////////////////////////////////////
type TimeSyncTask struct {
	internalPb.TimeSyncMsg
}

func (tst TimeSyncTask) SetTs(ts TimeStamp) {
	tst.Timestamp = uint64(ts)
}

func (tst TimeSyncTask) Ts() TimeStamp {
	return TimeStamp(tst.Timestamp)
}

func (tst TimeSyncTask) Type() MsgType {
	return kTimeSync
}

///////////////////////////////////////////Key2Seg//////////////////////////////////////////
//type Key2SegTask struct {
//	internalPb.Key2SegMsg
//}
//
////TODO::Key2SegMsg don't have timestamp
//func (k2st Key2SegTask) SetTs(ts TimeStamp) {}
//
//func (k2st Key2SegTask) Ts() TimeStamp {
//	return TimeStamp(0)
//}
//
//func (k2st Key2SegTask) Type() MsgType {
//	return
//}
