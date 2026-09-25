package coordview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// ShardStats is an atomic snapshot of a shard's placement state.
// Returned by ShardViewManager.Stats for use by Balancer / ShardViewRegistry.
//
// The snapshot is taken under the ShardViewManager's mutex, so all fields are
// consistent with each other: segment placements reflect exactly the views whose
// state is reported.
type ShardStats struct {
	// Exact view footprints, including empty partitions and retained Dropping
	// references. ResidentNodes disappear only after durable view removal.
	UpNodes        []int64
	PreparingNodes []int64
	ResidentNodes  []int64
	// Resources contains confirmed ready resources. Matching the full key is
	// conservative: a different DataVersion may still be physically reusable.
	Resources map[int64]map[ResourceKey]struct{}
	// UpVersion is the version of the current Up view, if any.
	// Nil when no view is currently Up.
	UpVersion *qviews.QueryViewVersion

	// UpLoadInfoVersion is the load-config snapshot version of the current Up view.
	// Zero when no view is currently Up.
	UpLoadInfoVersion uint64

	// PreparingVersion is the version of the current Preparing or Ready view,
	// if any. Nil when there is no in-flight view.
	PreparingVersion *qviews.QueryViewVersion

	// Segments lists every segment currently placed for this shard, keyed by
	// segmentID. The value is node-level state: the same segment may appear on
	// multiple nodes while views overlap, but one node has at most one state.
	// Each segment also carries the published RowNum footprint (see
	// SegmentStats.RowNum).
	//
	// Down view placements are reported as Ready because QueryNodes do not
	// receive Down and the loaded segments are still more reusable than
	// Preparing placements. Dropping and Dropped views are excluded.
	Segments map[int64]*SegmentStats
}

// ResourceKey identifies a compatible loading requirement within a collection.
// DataVersion covers membership/manifest revisions; LoadInfoVersion covers fields
// and indexes. Unknown load versions are not published as reusable resources.
type ResourceKey struct {
	PartitionID     int64
	SegmentID       int64
	DataVersion     qviews.DataVersion
	LoadInfoVersion uint64
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

	// RowNum is the published per-segment row-count footprint, read from a
	// resident QueryView's DataViewRef. HasRowNum distinguishes an unknown
	// footprint (for example after recovery) from a published zero row count.
	RowNum    int64
	HasRowNum bool
}
