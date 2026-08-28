package handler

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// ApplyView represents a query view to be applied on a work node,
// along with an asynchronous report callback.
type ApplyView struct {
	// View is the query view to apply.
	View qviews.QueryViewAtWorkNode

	// OnReport is called asynchronously when the work node's local state
	// for this view changes after the initial ApplyViews call.
	// The report carries the node's latest state for this view.
	// OnReport may be called from any goroutine and must be non-blocking.
	OnReport func(report qviews.QueryViewAtWorkNode)
}

// QueryViewHandler defines the interface that SN/QN-specific handlers must implement
// to process query view sync requests from the coordinator.
type QueryViewHandler interface {
	// ApplyViews applies a batch of query views on the work node.
	// State updates (both immediate and asynchronous) are reported
	// exclusively through the OnReport callback in each ApplyView.
	ApplyViews(views []ApplyView)
}
