package syncer

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// SyncView pairs a query view with its response callback and QueryNode-loss handler.
type SyncView struct {
	// View is the query view state to push to the target work node.
	// The target node is determined by View.WorkNode().
	View qviews.QueryViewAtWorkNode

	// OnSyncResponse is invoked when the ReliableSyncer receives a real response
	// from the work node for this view.
	//
	// Return value:
	//
	//	true  — the caller no longer needs to monitor this view; the ReliableSyncer
	//	        removes it from the pending set and will not invoke the callback again.
	//	false — the view remains in the pending set; the ReliableSyncer continues
	//	        tracking it and may invoke the callback again on future responses.
	//
	// Thread-safety: must be safe for concurrent invocation from
	// multiple ReliableSyncer internal goroutines (per-node recv goroutines).
	// Must not block for long.
	OnSyncResponse func(resp qviews.QueryViewAtWorkNode) bool

	// OnQueryNodeLost is called when the target QueryNode is declared lost
	// (detected via service discovery). StreamingNode loss is not a per-view
	// event; SN availability is handled by the channel assignment layer.
	// Pure notification; the ReliableSyncer removes the view from the pending
	// set after calling this.
	OnQueryNodeLost func(qviews.QueryNode)
}

// SyncGroup represents a batch of views to sync, pre-grouped by target work node.
// Using a struct instead of a bare map allows future extension with
// group-level parameters (e.g., priority, deadline, metadata) without
// breaking the SyncViews signature.
type SyncGroup struct {
	// ViewsByNode maps each work node to the views targeting it.
	ViewsByNode map[qviews.WorkNodeKey][]SyncView
}

// ReliableSyncer manages reliable delivery of QueryView syncs from Coord to work nodes.
//
// Delivery guarantee (the core contract):
//
//	For every outstanding sync whose callback has not yet returned true,
//	the ReliableSyncer guarantees that eventually the callback will be invoked
//	with either:
//	  (a) The node's real response, OR
//	  (b) SyncView.OnQueryNodeLost if the target QueryNode is declared lost
//	      (detected via service discovery).
//
// The guarantee is achieved through two mechanisms:
//  1. Re-push on reconnection: when a stream breaks and is re-established,
//     all outstanding syncs are re-pushed automatically via ResumableSyncer.
//  2. QueryNode loss handling: when service discovery reports a QueryNode removal,
//     the node's ResumableSyncer is closed and OnQueryNodeLost() is invoked
//     for each outstanding entry targeting that QueryNode.
//
// The ReliableSyncer is stateless with respect to state machine semantics.
// It does not interpret view states or transitions. It simply:
//   - Tracks outstanding syncs keyed by viewKey in per-node pending sets.
//   - Delivers views to nodes via per-node ResumableSyncers and routes responses
//     back via OnSyncResponse callbacks.
//   - Removes an outstanding entry when OnSyncResponse returns true.
//   - On QueryNode loss (service discovery), invokes OnQueryNodeLost() for each entry.
//
// Thread-safety: All methods are thread-safe.
type ReliableSyncer interface {
	// SyncViews delivers a group of query views to work nodes with delivery guarantee.
	//
	// Each SyncView in the group contains a view and its callback. Views are
	// pre-grouped by target work node in SyncGroup.ViewsByNode and routed to
	// their respective per-node ResumableSyncers.
	//
	// The views are tracked internally as "outstanding syncs" keyed by
	// viewKey = (replicaID, vchannel, version).
	//
	// When SyncViews is called for a viewKey that already has an outstanding
	// entry, the old entry (including its callback) is replaced.
	//
	// Outstanding entry lifecycle:
	//   - Persists until OnSyncResponse is invoked and returns true.
	//   - Re-pushed to the node on stream reconnection.
	//   - On QueryNode loss (service discovery), OnQueryNodeLost() is invoked.
	//
	// Non-blocking: returns after enqueuing. Returns error only if the
	// ReliableSyncer is closed or ctx is canceled.
	SyncViews(ctx context.Context, group SyncGroup) error

	// Close gracefully closes all ResumableSyncers and releases resources.
	// Must only be called during Coordinator shutdown. After Close, the
	// ReliableSyncer cannot be reused — a new instance must be created
	// via Coordinator recovery.
	Close() error
}

// ViewSyncClient provides service discovery and gRPC stream creation for all work node types.
// Internally routes to the appropriate backend (StreamingNode via HandlerClient,
// QueryNode via etcd session) based on the node's NodeType.
type ViewSyncClient interface {
	// WatchNodeChanged returns a channel that signals node membership changes
	// across all node types. The channel receives a value whenever the set of
	// known nodes (StreamingNode or QueryNode) changes.
	WatchNodeChanged(ctx context.Context) (<-chan struct{}, error)

	// GetAllNodes returns all currently known nodes (both StreamingNode and QueryNode)
	// as a map from WorkNodeKey to the node identity.
	GetAllNodes(ctx context.Context) (map[qviews.WorkNodeKey]qviews.WorkNode, error)

	// IsNodeAlive checks whether the given node is currently alive.
	IsNodeAlive(ctx context.Context, node qviews.WorkNode) bool

	// OpenSyncStream opens a SyncQueryView bidirectional stream to the given node.
	OpenSyncStream(ctx context.Context, node qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error)

	// Close closes the client and releases resources.
	Close()
}
