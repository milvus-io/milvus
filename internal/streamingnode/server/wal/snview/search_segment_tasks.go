package snview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var (
	_ viewquery.SearchSegmentTask  = (*SNSearchSegmentTask)(nil)
	_ viewquery.SearchSegmentTasks = (*SNSearchSegmentTasks)(nil)
)

type SNSearchSegmentTask struct {
	Handle   GrowingSegmentHandle
	Request  *internalpb.SearchRequest
	MVCC     *viewpb.QueryPlanMVCC
	VChannel string
}

type SNSearchSegmentTasks struct {
	tasks []SNSearchSegmentTask
	once  sync.Once
}

func NewSNSearchSegmentTasks(tasks []SNSearchSegmentTask) *SNSearchSegmentTasks {
	return &SNSearchSegmentTasks{tasks: append([]SNSearchSegmentTask(nil), tasks...)}
}

func (t *SNSearchSegmentTasks) Tasks() []viewquery.SearchSegmentTask {
	out := make([]viewquery.SearchSegmentTask, 0, len(t.tasks))
	for i := range t.tasks {
		out = append(out, &t.tasks[i])
	}
	return out
}

func (t *SNSearchSegmentTasks) Release() {
	t.once.Do(func() {
		for i := range t.tasks {
			t.tasks[i].Handle.Release()
		}
	})
}
