package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	sharedviewquery "github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ sharedviewquery.Scheduler = (*Scheduler)(nil)

type SegmentTaskExecutor interface {
	Search(ctx context.Context, tasks []snview.SNSearchSegmentTask) (*internalpb.SearchResults, error)
	Query(ctx context.Context, tasks []snview.SNQuerySegmentTask) (*internalpb.RetrieveResults, error)
}

type Scheduler struct {
	executor SegmentTaskExecutor
}

func NewScheduler(executor SegmentTaskExecutor) *Scheduler {
	return &Scheduler{executor: executor}
}

func (s *Scheduler) Search(ctx context.Context, tasks sharedviewquery.SearchSegmentTasks) (*internalpb.SearchResults, error) {
	snTasks, err := unwrapSNSearchSegmentTasks(tasks)
	if err != nil {
		return nil, err
	}
	return s.executor.Search(ctx, snTasks)
}

func (s *Scheduler) Query(ctx context.Context, tasks sharedviewquery.QuerySegmentTasks) (*internalpb.RetrieveResults, error) {
	snTasks, err := unwrapSNQuerySegmentTasks(tasks)
	if err != nil {
		return nil, err
	}
	return s.executor.Query(ctx, snTasks)
}

func unwrapSNSearchSegmentTasks(tasks sharedviewquery.SearchSegmentTasks) ([]snview.SNSearchSegmentTask, error) {
	rawTasks := tasks.Tasks()
	out := make([]snview.SNSearchSegmentTask, 0, len(rawTasks))
	for _, task := range rawTasks {
		snTask, ok := task.(*snview.SNSearchSegmentTask)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("streamingnode view query scheduler received %T search segment task", task)
		}
		out = append(out, *snTask)
	}
	return out, nil
}

func unwrapSNQuerySegmentTasks(tasks sharedviewquery.QuerySegmentTasks) ([]snview.SNQuerySegmentTask, error) {
	rawTasks := tasks.Tasks()
	out := make([]snview.SNQuerySegmentTask, 0, len(rawTasks))
	for _, task := range rawTasks {
		snTask, ok := task.(*snview.SNQuerySegmentTask)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("streamingnode view query scheduler received %T query segment task", task)
		}
		out = append(out, *snTask)
	}
	return out, nil
}
