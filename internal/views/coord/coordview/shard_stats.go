package coordview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ShardStats is an atomic snapshot of a shard's placement state.
// Returned by ShardViewManager.Stats for use by Balancer / ShardViewRegistry.
//
// The snapshot is taken under the ShardViewManager's mutex, so all fields are
// consistent with each other: segment placements reflect exactly the views whose
// state is reported.
type ShardStats struct {
	// UpVersion is the version of the current Up view, if any.
	// Nil when no view is currently Up.
	UpVersion *qviews.QueryViewVersion

	// UpSettings is the QueryViewSettings of the current Up view.
	// Nil when no view is currently Up.
	UpSettings *viewpb.QueryViewSettings

	// PreparingVersion is the version of the current Preparing or Ready view,
	// if any. Nil when there is no in-flight view.
	PreparingVersion *qviews.QueryViewVersion

	// Segments lists every segment currently placed for this shard, keyed by
	// segmentID. The value is node-level state: the same segment may appear on
	// multiple nodes while views overlap, but one node has at most one state.
	//
	// Down view placements are reported as Ready because QueryNodes do not
	// receive Down and the loaded segments are still more reusable than
	// Preparing placements. Dropping and Dropped views are excluded.
	Segments map[int64]*SegmentStats
}

// SegmentState is the per-node segment progress observed by Coord. Larger
// values are more reusable and override smaller values when multiple views
// mention the same segment on the same node.
type SegmentState int

const (
	// SegmentStateUnrecoverable means the previous load on this node failed
	// unrecoverably. Balancer should prefer other nodes when possible.
	SegmentStateUnrecoverable SegmentState = iota

	// SegmentStatePreparing means the segment is being loaded on this node.
	SegmentStatePreparing

	// SegmentStateReady means the segment has loaded and can participate in a
	// query view, but is not in the current Up view. This also covers Down
	// views: Down is sent only to StreamingNode, so QueryNode segments are still
	// loaded and should be treated as more reusable than Preparing placements.
	SegmentStateReady

	// SegmentStateUp means the segment is in the current Up view and serves
	// queries.
	SegmentStateUp
)

// SegmentStats describes all currently tracked node states for one segment.
type SegmentStats struct {
	SegmentID   int64
	PartitionID int64
	// Nodes maps nodeID to the segment state on that node. A segment may appear
	// on multiple nodes while views overlap, but one node only has one state for
	// a given segment.
	Nodes map[int64]SegmentState
}
