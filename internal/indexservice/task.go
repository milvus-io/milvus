package indexservice

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/types"
)

const (
	IndexAddTaskName = "IndexAddTask"
)

type task interface {
	Ctx() context.Context
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Name() string
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
	OnEnqueue() error
}

type BaseTask struct {
	done  chan error
	ctx   context.Context
	id    UniqueID
	table *metaTable
}

func (bt *BaseTask) ID() UniqueID {
	return bt.id
}

func (bt *BaseTask) setID(id UniqueID) {
	bt.id = id
}

func (bt *BaseTask) WaitToFinish() error {
	select {
	case <-bt.ctx.Done():
		return errors.New("Task wait to finished timeout")
	case err := <-bt.done:
		return err
	}
}

func (bt *BaseTask) Notify(err error) {
	bt.done <- err
}

type IndexAddTask struct {
	BaseTask
	req               *indexpb.BuildIndexRequest
	indexBuildID      UniqueID
	idAllocator       *allocator.GlobalIDAllocator
	buildQueue        TaskQueue
	kv                kv.Base
	builderClient     types.IndexNode
	nodeClients       *PriorityQueue
	buildClientNodeID UniqueID
}

func (it *IndexAddTask) Ctx() context.Context {
	return it.ctx
}

func (it *IndexAddTask) ID() UniqueID {
	return it.id
}

func (it *IndexAddTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

func (it *IndexAddTask) Name() string {
	return IndexAddTaskName
}

func (it *IndexAddTask) OnEnqueue() error {
	var err error
	it.indexBuildID, err = it.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	return nil
}

func (it *IndexAddTask) PreExecute(ctx context.Context) error {
	log.Debug("pretend to check Index Req")
	nodeID, builderClient := it.nodeClients.PeekClient()
	if builderClient == nil {
		return errors.New("IndexAddTask Service not available")
	}
	it.builderClient = builderClient
	it.buildClientNodeID = nodeID
	err := it.table.AddIndex(it.indexBuildID, it.req)
	if err != nil {
		return err
	}
	return nil
}

func (it *IndexAddTask) Execute(ctx context.Context) error {
	cmd := &indexpb.BuildIndexCmd{
		IndexBuildID: it.indexBuildID,
		Req:          it.req,
	}
	log.Debug("before index ...")
	resp, err := it.builderClient.BuildIndex(ctx, cmd)
	if err != nil {
		return err
	}
	log.Debug("indexservice", zap.String("build index finish err", err.Error()))
	if resp.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(resp.Reason)
	}
	it.nodeClients.IncPriority(it.buildClientNodeID, 1)
	return nil
}

func (it *IndexAddTask) PostExecute(ctx context.Context) error {
	return nil
}
