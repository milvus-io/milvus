package indexbuilder

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
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
		return errors.New("timeout")
	case err := <-bt.done:
		return err
	}
}

func (bt *BaseTask) Notify(err error) {
	bt.done <- err
}

type IndexAddTask struct {
	BaseTask
	req         *indexbuilderpb.BuildIndexRequest
	indexID     UniqueID
	idAllocator *allocator.IDAllocator
	buildQueue  TaskQueue
}

func (it *IndexAddTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

func (it *IndexAddTask) OnEnqueue() error {
	var err error
	it.indexID, err = it.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	return nil
}

func (it *IndexAddTask) PreExecute() error {
	log.Println("pretend to check Index Req")
	err := it.table.AddIndex(it.indexID, it.req)
	if err != nil {
		return err
	}
	return nil
}

func (it *IndexAddTask) Execute() error {
	t := newIndexBuildTask()
	t.table = it.table
	t.indexID = it.indexID
	var cancel func()
	t.ctx, cancel = context.WithTimeout(it.ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-t.ctx.Done():
			return errors.New("index add timeout")
		default:
			return it.buildQueue.Enqueue(t)
		}
	}
	return fn()
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

type IndexBuildTask struct {
	BaseTask
	indexID   UniqueID
	indexMeta *indexbuilderpb.IndexMeta
}

func newIndexBuildTask() *IndexBuildTask {
	return &IndexBuildTask{
		BaseTask: BaseTask{
			done: make(chan error, 1), // intend to do this
		},
	}
}

func (it *IndexBuildTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

func (it *IndexBuildTask) OnEnqueue() error {
	return it.table.UpdateIndexEnqueTime(it.indexID, time.Now())
}

func (it *IndexBuildTask) PreExecute() error {
	return it.table.UpdateIndexScheduleTime(it.indexID, time.Now())
}

func (it *IndexBuildTask) Execute() error {
	err := it.table.UpdateIndexStatus(it.indexID, indexbuilderpb.IndexStatus_INPROGRESS)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	log.Println("Pretend to Execute for 1 second")
	return nil
}

func (it *IndexBuildTask) PostExecute() error {
	dataPaths := []string{"file1", "file2"}
	return it.table.CompleteIndex(it.indexID, dataPaths)
}
