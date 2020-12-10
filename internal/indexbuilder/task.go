package indexbuilder

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/errors"
)

type task interface {
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	PreExecute() error
	Execute() error
	PostExecute() error
	WaitToFinish() error
	Notify(err error)
}

type BaseTask struct {
	done chan error
	ctx  context.Context
	id   UniqueID
}

func (bt *BaseTask) ID() UniqueID {
	return bt.id
}

func (bt *BaseTask) setID(id UniqueID) {
	bt.id = id
}

func (bt *BaseTask) WaitToFinish() error {
	for {
		select {
		case <-bt.ctx.Done():
			return errors.New("timeout")
		case err := <-bt.done:
			return err
		}
	}
}

func (bt *BaseTask) Notify(err error) {
	bt.done <- err
}

type IndexBuildTask struct {
	BaseTask
	rowIDAllocator *allocator.IDAllocator
}

func (it *IndexBuildTask) PreExecute() error {

	return nil
}

func (it *IndexBuildTask) Execute() error {

	return nil
}

func (it *IndexBuildTask) PostExecute() error {
	return nil
}

type DescribeIndexTask struct {
	BaseTask
	ctx context.Context
}

func (dct *DescribeIndexTask) PreExecute() error {
	return nil
}

func (dct *DescribeIndexTask) Execute() error {
	return nil
}

func (dct *DescribeIndexTask) PostExecute() error {
	return nil
}
