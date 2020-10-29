package master

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

// TODO: get timestamp from timestampOracle
type Timestamp uint64

type baseTask struct {
	kvBase *kv.Base
	mt     *metaTable
	cv     chan int
}

type task interface {
	Type() internalpb.ReqType
	Ts() (Timestamp, error)
	Execute() commonpb.Status
	WaitToFinish(ctx context.Context) commonpb.Status
	Notify() commonpb.Status
}

func (bt *baseTask) Notify() commonpb.Status {
	bt.cv <- 0
	return commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
}

func (bt *baseTask) WaitToFinish(ctx context.Context) commonpb.Status {
	for {
		select {
		case <-ctx.Done():
			return commonpb.Status{
				// TODO: if to return unexpected error
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			}
		case <-bt.cv:
			return commonpb.Status{
				ErrorCode: commonpb.ErrorCode_SUCCESS,
			}
		}
	}
}
