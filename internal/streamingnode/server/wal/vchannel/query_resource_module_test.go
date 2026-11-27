package vchannel

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestDeleteReplayStartIncludesLegacySegments(t *testing.T) {
	for _, tc := range []struct {
		name    string
		created []uint64
		want    uint64
	}{
		{"empty", nil, 0},
		{"ordered", []uint64{20, 10}, 9},
		{"legacy first", []uint64{0, 20}, 0},
		{"legacy last", []uint64{20, 0}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := walview.VisibleSegmentSnapshot{}
			for _, tt := range tc.created {
				snapshot.Segments = append(snapshot.Segments, walview.VisibleSegment{Assignment: &streamingpb.SegmentAssignmentMeta{Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: tt}}})
			}
			require.Equal(t, tc.want, deleteReplayStartAfter(snapshot))
		})
	}
}

func TestDroppedCollectionWaitsForQueryAndSummaryCleanup(t *testing.T) {
	module, err := NewModule(ModuleConfig{
		PChannel: "p1", VChannel: "v1", SummaryManager: &walsummary.Manager{},
		VChannelMeta: &streamingpb.VChannelMeta{
			Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CreateCollectionTimeTick: 1, CheckpointTimeTick: 10, TransformMaterializedTimeTick: 10,
		},
	})
	require.NoError(t, err)
	var released bool
	retention := mockey.Mock((*walsummary.Manager).SetDataViewRetention).To(func(_ *walsummary.Manager, vc string, tt uint64) {
		require.Equal(t, "v1", vc)
		require.Equal(t, uint64(walsummary.DroppedVChannelTimeTick), tt)
		released = true
	}).Build()
	defer retention.UnPatch()
	queryReferenced := true
	references := mockey.Mock((*queryresource.Manager).OldestDataVersion).To(func(*queryresource.Manager) (qviews.DataVersion, bool) {
		return qviews.DataVersion{}, queryReferenced
	}).Build()
	defer references.UnPatch()
	summaryRetired := false
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 11, SummaryRetired: func(string, uint64) bool {
		return summaryRetired
	}}
	require.Empty(t, module.ConsumeCleanupSnapshots(cleanup))
	require.False(t, released, "the old query still needs Summary history")
	queryReferenced = false
	require.Empty(t, module.ConsumeCleanupSnapshots(cleanup), "retain the tombstone until Summary GC finishes")
	require.True(t, released, "release the pin before waiting for Summary retirement")
	summaryRetired = true
	snapshots := module.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
}

func TestDataViewRetentionRestoresCollectionOrigins(t *testing.T) {
	for _, origin := range []uint64{0, 50} {
		var got uint64
		patch := mockey.Mock((*walsummary.Manager).SetDataViewRetention).To(func(_ *walsummary.Manager, vc string, tt uint64) {
			require.Equal(t, "v1", vc)
			got = tt
		}).Build()
		_, err := NewModule(ModuleConfig{PChannel: "p1", VChannel: "v1", SummaryManager: &walsummary.Manager{}, VChannelMeta: &streamingpb.VChannelMeta{Vchannel: "v1", CreateCollectionTimeTick: origin}})
		patch.UnPatch()
		require.NoError(t, err)
		require.Equal(t, origin, got)
	}
}

func TestDataViewRetentionCreateReplayDoesNotReset(t *testing.T) {
	var origins []uint64
	patch := mockey.Mock((*walsummary.Manager).SetDataViewRetention).To(func(_ *walsummary.Manager, vc string, tt uint64) {
		require.Equal(t, "v1", vc)
		origins = append(origins, tt)
	}).Build()
	defer patch.UnPatch()
	module, err := NewModule(ModuleConfig{PChannel: "p1", VChannel: "v1", SummaryManager: &walsummary.Manager{}})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, origins)
	schema, err := proto.Marshal(&schemapb.CollectionSchema{Name: "collection"})
	require.NoError(t, err)
	raw := message.NewCreateCollectionMessageBuilderV1().WithVChannel("v1").
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{Schema: schema}).MustBuildMutable().
		WithTimeTick(100).WithLastConfirmed(walimplstest.NewTestMessageID(99)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(100))
	msg := message.MustAsImmutableCreateCollectionMessageV1(raw)
	module.handleCreateCollectionMessage(msg)
	require.Equal(t, []uint64{0, 100}, origins)
	module.handleCreateCollectionMessage(msg)
	require.Equal(t, []uint64{0, 100}, origins, "replayed Create must not reset the refreshed retention")
	module.vchannelView = NewVChannelViewFromMeta(&streamingpb.VChannelMeta{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, CheckpointTimeTick: 90})
	module.handleCreateCollectionMessage(msg)
	require.Equal(t, []uint64{0, 100, 100}, origins, "replacement lifetime gets its own origin")
}
