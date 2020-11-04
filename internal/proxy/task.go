package proxy

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type task interface {
	Id() UniqueID // return ReqId
	Type() internalpb.MsgType
	GetTs() Timestamp
	SetTs(ts Timestamp)
	PreExecute() error
	Execute() error
	PostExecute() error
	WaitToFinish() error
	Notify() error
}

type baseTask struct {
	ReqType internalpb.MsgType
	ReqId   UniqueID
	Ts      Timestamp
	ProxyId UniqueID
}

func (bt *baseTask) Id() UniqueID {
	return bt.ReqId
}

func (bt *baseTask) Type() internalpb.MsgType {
	return bt.ReqType
}

func (bt *baseTask) GetTs() Timestamp {
	return bt.Ts
}

func (bt *baseTask) SetTs(ts Timestamp) {
	bt.Ts = ts
}
