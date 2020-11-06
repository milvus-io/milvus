package master

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

// TODO: get timestamp from timestampOracle

type baseTask struct {
	kvBase *kv.EtcdKV
	mt     *metaTable
	cv     chan int
}

type task interface {
	Type() internalpb.MsgType
	Ts() (Timestamp, error)
	Execute() error
	WaitToFinish(ctx context.Context) error
	Notify() error
	NotifyTimeout() error
}

func (bt *baseTask) Notify() error {
	bt.cv <- 0
	return nil
}

func (bt *baseTask) NotifyTimeout() error {
	bt.cv <- 0
	return errors.New("request timeout")
}

func (bt *baseTask) WaitToFinish(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-bt.cv:
			return nil
		}
	}
}
