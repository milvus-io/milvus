package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	sharedviewquery "github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ sharedviewquery.Scheduler = (*Scheduler)(nil)

type SegmentTaskExecutor interface {
	Search(ctx context.Context, tasks []qnview.QNSearchSegmentTask) (*internalpb.SearchResults, error)
	Query(ctx context.Context, tasks []qnview.QNQuerySegmentTask) (*internalpb.RetrieveResults, error)
}

type Scheduler struct {
	executor SegmentTaskExecutor
}

func NewScheduler(executor SegmentTaskExecutor) *Scheduler {
	return &Scheduler{executor: executor}
}

func (s *Scheduler) Search(ctx context.Context, tasks sharedviewquery.SearchSegmentTasks) (*internalpb.SearchResults, error) {
	qnTasks, err := unwrapQNSearchSegmentTasks(tasks)
	if err != nil {
		return nil, err
	}
	return s.executor.Search(ctx, qnTasks)
}

func (s *Scheduler) Query(ctx context.Context, tasks sharedviewquery.QuerySegmentTasks) (*internalpb.RetrieveResults, error) {
	qnTasks, err := unwrapQNQuerySegmentTasks(tasks)
	if err != nil {
		return nil, err
	}
	return s.executor.Query(ctx, qnTasks)
}

func unwrapQNSearchSegmentTasks(tasks sharedviewquery.SearchSegmentTasks) ([]qnview.QNSearchSegmentTask, error) {
	rawTasks := tasks.Tasks()
	out := make([]qnview.QNSearchSegmentTask, 0, len(rawTasks))
	for _, task := range rawTasks {
		qnTask, ok := task.(*qnview.QNSearchSegmentTask)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("querynode view query scheduler received %T search segment task", task)
		}
		out = append(out, *qnTask)
	}
	return out, nil
}

func unwrapQNQuerySegmentTasks(tasks sharedviewquery.QuerySegmentTasks) ([]qnview.QNQuerySegmentTask, error) {
	rawTasks := tasks.Tasks()
	out := make([]qnview.QNQuerySegmentTask, 0, len(rawTasks))
	for _, task := range rawTasks {
		qnTask, ok := task.(*qnview.QNQuerySegmentTask)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("querynode view query scheduler received %T query segment task", task)
		}
		out = append(out, *qnTask)
	}
	return out, nil
}
