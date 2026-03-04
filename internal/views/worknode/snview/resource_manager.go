package snview

import "github.com/milvus-io/milvus/internal/views/qviews"

// AcquireResource describes a resource acquisition request for a new Preparing view.
type AcquireResource struct {
	// Key identifies the query view.
	Key qviews.QueryViewKey

	// OnReady is called when resource preparation completes successfully.
	// Must NOT be called synchronously during Acquire.
	OnReady func()

	// OnUnrecoverable is called when a fatal error prevents resource setup.
	// Must NOT be called synchronously during Acquire.
	OnUnrecoverable func()
}

// RecoverResource describes a resource recovery request for a persisted view
// after SN crash recovery.
type RecoverResource struct {
	// Key identifies the query view.
	Key qviews.QueryViewKey

	// OnRecoveringDone is called when WAL catch-up completes successfully.
	// Must NOT be called synchronously during Recover.
	OnRecoveringDone func()

	// OnUnrecoverable is called when a fatal error prevents recovery.
	// Must NOT be called synchronously during Recover.
	OnUnrecoverable func()
}

// ReleaseResource describes a resource release request when a query view
// is being dropped.
type ReleaseResource struct {
	// Key identifies the query view whose resources are being released.
	Key qviews.QueryViewKey

	// OnDropped is called when the release operation completes (growing segments
	// unsubscribed, BM25 IDF stats released, etc.).
	// Must NOT be called synchronously during Release.
	OnDropped func()
}

// StreamingNodeResourceManager manages streaming resources on a StreamingNode.
// Resources include growing segments, BM25 IDF statistics, and other
// shard-level query state required to serve a query view.
//
// # Liveness Contracts
//
// Implementations MUST guarantee the following callback obligations.
// Violating these contracts causes the corresponding query views to
// stall without ever producing a response to the Coordinator.
//
//   - Acquire: for every Acquire call, the implementation MUST eventually
//     invoke exactly one of OnReady or OnUnrecoverable.
//     Failure to do so leaves the view stuck in Preparing with no report.
//
//   - Recover: for every Recover call, the implementation MUST eventually
//     invoke exactly one of OnRecoveringDone or OnUnrecoverable.
//     Failure to do so leaves the view stuck in UpRecovering with no report.
//
//   - Release: for every Release call, the implementation MUST eventually
//     invoke OnDropped exactly once.
//     Failure to do so leaves the view stuck in Dropping with no report.
//
// All callbacks MUST be invoked asynchronously (not during the Acquire /
// Recover / Release call itself) to avoid deadlocking the caller's mutex.
type StreamingNodeResourceManager interface {
	// Acquire starts resource preparation for a new query view.
	Acquire(req AcquireResource)

	// Recover starts WAL catch-up for a recovered query view.
	Recover(req RecoverResource)

	// Release releases resources held by a query view being dropped.
	Release(req ReleaseResource)
}
