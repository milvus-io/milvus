package msgstream

import (
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type MsgType uint32

const (
	kInsert         MsgType = 400
	kDelete         MsgType = 401
	kSearch         MsgType = 500
	kSearchResult   MsgType = 1000

	kSegmentStatics MsgType = 1100
	kTimeTick       MsgType = 1200
	kTimeSync       MsgType = 1201
)

type TsMsg interface {
	SetTs(ts Timestamp)
	BeginTs() Timestamp
	EndTs() Timestamp
	Type() MsgType
	HashKeys() []int32
}

/////////////////////////////////////////Insert//////////////////////////////////////////
type InsertTask struct {
	HashValues []int32
	internalPb.InsertRequest
}

func (it InsertTask) SetTs(ts Timestamp) {
	// TODO::
}

func (it InsertTask) BeginTs() Timestamp {
	timestamps := it.Timestamps
	var beginTs Timestamp
	for _, v := range timestamps {
		beginTs = Timestamp(v)
		break
	}
	for _, v := range timestamps {
		if beginTs > Timestamp(v) {
			beginTs = Timestamp(v)
		}
	}
	return beginTs
}

func (it InsertTask) EndTs() Timestamp {
	timestamps := it.Timestamps
	var endTs Timestamp
	for _, v := range timestamps {
		endTs = Timestamp(v)
		break
	}
	for _, v := range timestamps {
		if endTs < Timestamp(v) {
			endTs = Timestamp(v)
		}
	}
	return endTs
}

func (it InsertTask) Type() MsgType {
	if it.ReqType == internalPb.ReqType_kTimeTick {
		return kTimeSync
	}
	return kInsert
}

func (it InsertTask) HashKeys() []int32 {
	return it.HashValues
}

/////////////////////////////////////////Delete//////////////////////////////////////////
type DeleteTask struct {
	HashValues []int32
	internalPb.DeleteRequest
}

func (dt DeleteTask) SetTs(ts Timestamp) {
	// TODO::
}

func (dt DeleteTask) BeginTs() Timestamp {
	timestamps := dt.Timestamps
	var beginTs Timestamp
	for _, v := range timestamps {
		beginTs = Timestamp(v)
		break
	}
	for _, v := range timestamps {
		if beginTs > Timestamp(v) {
			beginTs = Timestamp(v)
		}
	}
	return beginTs
}

func (dt DeleteTask) EndTs() Timestamp {
	timestamps := dt.Timestamps
	var endTs Timestamp
	for _, v := range timestamps {
		endTs = Timestamp(v)
		break
	}
	for _, v := range timestamps {
		if endTs < Timestamp(v) {
			endTs = Timestamp(v)
		}
	}
	return endTs
}

func (dt DeleteTask) Type() MsgType {
	if dt.ReqType == internalPb.ReqType_kTimeTick {
		return kTimeSync
	}
	return kDelete
}

func (dt DeleteTask) HashKeys() []int32 {
	return dt.HashValues
}

/////////////////////////////////////////Search//////////////////////////////////////////
type SearchTask struct {
	HashValues []int32
	internalPb.SearchRequest
}

func (st SearchTask) SetTs(ts Timestamp) {
	st.Timestamp = uint64(ts)
}

func (st SearchTask) BeginTs() Timestamp {
	return Timestamp(st.Timestamp)
}

func (st SearchTask) EndTs() Timestamp {
	return Timestamp(st.Timestamp)
}

func (st SearchTask) Type() MsgType {
	if st.ReqType == internalPb.ReqType_kTimeTick {
		return kTimeSync
	}
	return kSearch
}

func (st SearchTask) HashKeys() []int32 {
	return st.HashValues
}

/////////////////////////////////////////SearchResult//////////////////////////////////////////
type SearchResultTask struct {
	HashValues []int32
	internalPb.SearchResult
}

func (srt SearchResultTask) SetTs(ts Timestamp) {
	srt.Timestamp = uint64(ts)
}

func (srt SearchResultTask)  BeginTs() Timestamp {
	return Timestamp(srt.Timestamp)
}

func (srt SearchResultTask)  EndTs() Timestamp {
	return Timestamp(srt.Timestamp)
}

func (srt SearchResultTask) Type() MsgType {
	return kSearchResult
}

func (srt SearchResultTask) HashKeys() []int32 {
	return srt.HashValues
}

/////////////////////////////////////////TimeSync//////////////////////////////////////////
type TimeSyncTask struct {
	HashValues []int32
	internalPb.TimeSyncMsg
}

func (tst TimeSyncTask) SetTs(ts Timestamp) {
	tst.Timestamp = uint64(ts)
}

func (tst TimeSyncTask) BeginTs() Timestamp {
	return Timestamp(tst.Timestamp)
}

func (tst TimeSyncTask)  EndTs() Timestamp {
	return Timestamp(tst.Timestamp)
}

func (tst TimeSyncTask) Type() MsgType {
	return kTimeSync
}

func (tst TimeSyncTask) HashKeys() []int32 {
	return tst.HashValues
}


///////////////////////////////////////////Key2Seg//////////////////////////////////////////
//type Key2SegTask struct {
//	internalPb.Key2SegMsg
//}
//
////TODO::Key2SegMsg don't have timestamp
//func (k2st Key2SegTask) SetTs(ts Timestamp) {}
//
//func (k2st Key2SegTask) Ts() Timestamp {
//	return Timestamp(0)
//}
//
//func (k2st Key2SegTask) Type() MsgType {
//	return
//}
