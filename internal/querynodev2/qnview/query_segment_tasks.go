package qnview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var (
	_ viewquery.QuerySegmentTask  = (*QNQuerySegmentTask)(nil)
	_ viewquery.QuerySegmentTasks = (*QNQuerySegmentTasks)(nil)
)

type QNQuerySegmentTask struct {
	Handle  SealedSegmentHandle
	Request *internalpb.RetrieveRequest
	MVCC    *viewpb.QueryPlanMVCC
}

type QNQuerySegmentTasks struct {
	tasks []QNQuerySegmentTask
	once  sync.Once
}

func NewQNQuerySegmentTasks(tasks []QNQuerySegmentTask) *QNQuerySegmentTasks {
	return &QNQuerySegmentTasks{
		tasks: append([]QNQuerySegmentTask(nil), tasks...),
	}
}

func (t *QNQuerySegmentTasks) Tasks() []viewquery.QuerySegmentTask {
	out := make([]viewquery.QuerySegmentTask, 0, len(t.tasks))
	for i := range t.tasks {
		out = append(out, &t.tasks[i])
	}
	return out
}

func (t *QNQuerySegmentTasks) Release() {
	t.once.Do(func() {
		for i := range t.tasks {
			t.tasks[i].Handle.Release()
		}
	})
}
