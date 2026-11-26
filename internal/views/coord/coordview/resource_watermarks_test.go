package coordview

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/coord/resourcewatermark"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestFailureWatermarksAndMaterializationIdentity(t *testing.T) {
	view := buildTestView(1)
	view.Meta.LoadInfoVersion = 7
	view.Meta.SchemaVersion = 8
	view.QueryNode[0].Partitions[0].SegmentManifestVersions = []int64{42}
	sm := NewCoordQueryViewStateMachineWithRef(view, nil)
	report := qnReport(view, 1, qviews.QueryViewStateUnrecoverable).(*qviews.QueryViewAtQueryNode)
	report.ViewOfQueryNode().FailedSegments = []*viewpb.SegmentPerparingFailure{{SegmentId: 1000, Reason: viewpb.SegmentPerparingFailure_MemoryLimitExceeded}}
	sm.OnNodeStateReported(report)
	store := resourcewatermark.New()
	update := store.Start(1)
	update(&viewpb.NodeResourceWatermarks{Memory: 95, Disk: 20})
	failed := fillFailedResources(nil, sm, store)
	key := FailureKey(10, 1000, 42, 8, 7, sm.Version().DataVersion)
	require.Equal(t, 95.0, failed[1][key].Watermarks.Memory)
	update(&viewpb.NodeResourceWatermarks{Memory: 80, Disk: 10})
	repeated := fillFailedResources(cloneFailedResources(failed), sm, store)
	require.Equal(t, failed, repeated, "repeated reports must not move the recovery baseline")
	for _, change := range []string{"other_shard_flush", "manifest", "schema", "load_info", "removed", "expired"} {
		t.Run(change, func(t *testing.T) {
			next := proto.Clone(view).(*viewpb.QueryViewOfShard)
			next.Meta.Version.DataVersion.StreamingVersion++
			copy := cloneFailedResources(failed)
			switch change {
			case "manifest":
				next.QueryNode[0].Partitions[0].SegmentManifestVersions[0]++
			case "schema":
				next.Meta.SchemaVersion++
			case "load_info":
				next.Meta.LoadInfoVersion++
			case "removed":
				next.QueryNode[0].Partitions = nil
			case "expired":
				fact := copy[1][key]
				fact.ObservedAt = time.Now().Add(-11 * time.Minute)
				copy[1][key] = fact
			}
			retainFailedResources(copy, NewCoordQueryViewStateMachineWithRef(next, nil))
			if change == "other_shard_flush" {
				require.Equal(t, failed, copy)
			} else {
				require.Empty(t, copy)
			}
		})
	}
	other := proto.Clone(view).(*viewpb.QueryViewOfShard)
	other.QueryNode[0].NodeId = 2
	clearSuccessfulFailures(failed, NewCoordQueryViewStateMachineWithRef(other, nil))
	require.NotEmpty(t, failed[1], "success on another node is not capacity recovery")
	clearSuccessfulFailures(failed, sm)
	require.Empty(t, failed)
}

func TestRegistryResourceWatermarksBinding(t *testing.T) {
	r, err := RecoverShardViewRegistry(t.Context(), newMockCatalog(), newMockSyncer())
	require.NoError(t, err)
	defer r.Close()
	one := r.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"})
	store := resourcewatermark.New()
	r.SetResourceWatermarksReader(store)
	two := r.Ensure(qviews.ShardID{ReplicaID: 2, VChannel: "p_1v0"})
	require.Same(t, store, one.resourceWatermarks)
	require.Same(t, store, two.resourceWatermarks)
	before := one.Stats()
	var publications atomic.Int32
	r.RegisterStatsObserver(func(qviews.ShardID, *ShardStats) { publications.Add(1) })
	update := store.Start(1)
	for i := range 100 {
		update(&viewpb.NodeResourceWatermarks{Memory: float64(i), Disk: float64(100 - i)})
	}
	require.Zero(t, publications.Load(), "watermarks must not enqueue balance through stats observers")
	require.Equal(t, before, one.Stats(), "node feedback must not mutate or publish shard state")
}
