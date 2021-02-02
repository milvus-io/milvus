package indexservice

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
)

type task interface {
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	PreExecute() error
	Execute() error
	PostExecute() error
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
	idAllocator       *GlobalIDAllocator
	buildQueue        TaskQueue
	kv                kv.Base
	builderClient     typeutil.IndexNodeInterface
	nodeClients       *PriorityQueue
	buildClientNodeID UniqueID
}

func (it *IndexAddTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

func (it *IndexAddTask) OnEnqueue() error {
	var err error
	it.indexBuildID, err = it.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	return nil
}

func (it *IndexAddTask) PreExecute() error {
	log.Println("pretend to check Index Req")
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

func (it *IndexAddTask) Execute() error {
	cmd := &indexpb.BuildIndexCmd{
		IndexBuildID: it.indexBuildID,
		Req:          it.req,
	}
	log.Println("before index ...")
	resp, err := it.builderClient.BuildIndex(cmd)
	if err != nil {
		return err
	}
	log.Println("build index finish, err = ", err)
	if resp.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return errors.New(resp.Reason)
	}
	it.nodeClients.IncPriority(it.buildClientNodeID, 1)
	return nil
}

func (it *IndexAddTask) PostExecute() error {
	return nil
}

func NewIndexAddTask() *IndexAddTask {
	return &IndexAddTask{
		BaseTask: BaseTask{
			done: make(chan error),
		},
	}
}
