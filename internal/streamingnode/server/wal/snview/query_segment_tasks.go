package snview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var (
	_ viewquery.QuerySegmentTask  = (*SNQuerySegmentTask)(nil)
	_ viewquery.QuerySegmentTasks = (*SNQuerySegmentTasks)(nil)
)

type SNQuerySegmentTask struct {
	Handle   GrowingSegmentHandle
	Request  *internalpb.RetrieveRequest
	MVCC     *viewpb.QueryPlanMVCC
	VChannel string
}

type SNQuerySegmentTasks struct {
	tasks []SNQuerySegmentTask
	once  sync.Once
}

func NewSNQuerySegmentTasks(tasks []SNQuerySegmentTask) *SNQuerySegmentTasks {
	return &SNQuerySegmentTasks{tasks: append([]SNQuerySegmentTask(nil), tasks...)}
}

func (t *SNQuerySegmentTasks) Tasks() []viewquery.QuerySegmentTask {
	out := make([]viewquery.QuerySegmentTask, 0, len(t.tasks))
	for i := range t.tasks {
		out = append(out, &t.tasks[i])
	}
	return out
}

func (t *SNQuerySegmentTasks) Release() {
	t.once.Do(func() {
		for i := range t.tasks {
			t.tasks[i].Handle.Release()
		}
	})
}
