package viewquery

import "github.com/milvus-io/milvus/pkg/v3/proto/viewpb"

// SearchSegmentTask is a node-local search task over a concrete segment.
type SearchSegmentTask interface{}

// QuerySegmentTask is a node-local query task over a concrete segment.
type QuerySegmentTask interface{}

// SearchSegmentTasks owns the search lifecycle refs for all selected segments.
type SearchSegmentTasks interface {
	Tasks() []SearchSegmentTask
	Release()
}

// QuerySegmentTasks owns the query lifecycle refs for all selected segments.
type QuerySegmentTasks interface {
	Tasks() []QuerySegmentTask
	Release()
}

// SearchTasksWithOptions adds request-local execution controls without changing
// the provider's ownership of the selected physical resources.
type SearchTasksWithOptions struct {
	SearchSegmentTasks
	Options *viewpb.SearchExecutionOptions
}

func SearchOptions(tasks SearchSegmentTasks) *viewpb.SearchExecutionOptions {
	if configured, ok := tasks.(*SearchTasksWithOptions); ok {
		return configured.Options
	}
	return nil
}
