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
	cv     chan error
}

type task interface {
	Type() internalpb.MsgType
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
