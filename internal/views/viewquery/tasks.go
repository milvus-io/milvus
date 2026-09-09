package viewquery

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
