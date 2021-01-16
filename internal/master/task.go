package master

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/errors"
)

// TODO: get timestamp from timestampOracle

type baseTask struct {
	sch *ddRequestScheduler
	mt  *metaTable
	cv  chan error
}

type task interface {
	Type() commonpb.MsgType
	Ts() (Timestamp, error)
	Execute() error
	WaitToFinish(ctx context.Context) error
	Notify(err error)
}

func (bt *baseTask) Notify(err error) {
	bt.cv <- err
}

func (bt *baseTask) WaitToFinish(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Errorf("context done")
		case err, ok := <-bt.cv:
			if !ok {
				return errors.Errorf("notify chan closed")
			}
			return err
		}
	}
}
