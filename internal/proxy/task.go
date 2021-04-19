package proxy

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

//type TimeStamp uint64

type task interface {
	Id() int64 // return ReqId
	Type() internalpb.ReqType
	GetTs() typeutil.Timestamp
	SetTs(ts typeutil.Timestamp)
	PreExecute() error
	Execute() error
	PostExecute() error
	WaitToFinish() error
	Notify() error
}

type baseTask struct {
	ReqType internalpb.ReqType
	ReqId   int64
	Ts      typeutil.Timestamp
	ProxyId int64
}

func (bt *baseTask) Id() int64 {
	return bt.ReqId
}

func (bt *baseTask) Type() internalpb.ReqType {
	return bt.ReqType
}

func (bt *baseTask) GetTs() typeutil.Timestamp {
	return bt.Ts
}

func (bt *baseTask) SetTs(ts typeutil.Timestamp) {
	bt.Ts = ts
}
