package qnview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var (
	_ viewquery.SearchSegmentTask  = (*QNSearchSegmentTask)(nil)
	_ viewquery.SearchSegmentTasks = (*QNSearchSegmentTasks)(nil)
)

type QNSearchSegmentTask struct {
	Handle  SealedSegmentHandle
	Request *internalpb.SearchRequest
	MVCC    *viewpb.QueryPlanMVCC
}

type QNSearchSegmentTasks struct {
	tasks []QNSearchSegmentTask
	once  sync.Once
}

func NewQNSearchSegmentTasks(tasks []QNSearchSegmentTask) *QNSearchSegmentTasks {
	return &QNSearchSegmentTasks{
		tasks: append([]QNSearchSegmentTask(nil), tasks...),
	}
}

func (t *QNSearchSegmentTasks) Tasks() []viewquery.SearchSegmentTask {
	out := make([]viewquery.SearchSegmentTask, 0, len(t.tasks))
	for i := range t.tasks {
		out = append(out, &t.tasks[i])
	}
	return out
}

func (t *QNSearchSegmentTasks) Release() {
	t.once.Do(func() {
		for i := range t.tasks {
			t.tasks[i].Handle.Release()
		}
	})
}
