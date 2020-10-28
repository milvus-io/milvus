package master

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type ddRequestScheduler struct {
	reqQueue chan *task
}

func NewDDRequestScheduler() *ddRequestScheduler {
	const channelSize = 1024

	rs := ddRequestScheduler{
		reqQueue: make(chan *task, channelSize),
	}
	return &rs
}

func (rs *ddRequestScheduler) Enqueue(task *task) commonpb.Status {
	rs.reqQueue <- task
	return commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
}

func (rs *ddRequestScheduler) schedule() *task {
	t := <- rs.reqQueue
	return t
}
