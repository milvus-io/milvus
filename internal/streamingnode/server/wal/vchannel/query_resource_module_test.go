package vchannel

import (
	"math"
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

func TestQueryRetentionSurvivesLocalSegmentRelease(t *testing.T) {
	for _, origin := range []uint64{0, 100} {
		var retained uint64
		patch := mockey.Mock((*walsummary.Manager).SetQueryRetention).To(func(_ *walsummary.Manager, channel string, start uint64) {
			require.Equal(t, "v1", channel)
			retained = start
		}).Build()
		module := &VChannelRecoveryModule{
			vchannel:       "v1",
			vchannelView:   NewVChannelViewFromMeta(&streamingpb.VChannelMeta{Vchannel: "v1", CreateCollectionTimeTick: origin}),
			summaryManager: &walsummary.Manager{},
		}
		module.refreshQueryRetentionLocked()
		patch.UnPatch()
		require.Equal(t, origin, retained, "the shared View suffix remains pinned with no local Segment; legacy origin stays conservative")
	}
}

func TestQueryRetentionBeforeCreateCollection(t *testing.T) {
	var retained []uint64
	patch := mockey.Mock((*walsummary.Manager).SetQueryRetention).To(func(_ *walsummary.Manager, channel string, start uint64) {
		require.Equal(t, "v1", channel)
		retained = append(retained, start)
	}).Build()
	defer patch.UnPatch()
	module, err := NewModule(ModuleConfig{
		PChannel: "p1", VChannel: "v1", SummaryManager: &walsummary.Manager{},
	})
	require.NoError(t, err)
	require.Nil(t, module.vchannelView)
	require.Equal(t, []uint64{0}, retained)
	schema, err := proto.Marshal(&schemapb.CollectionSchema{Name: "collection"})
	require.NoError(t, err)
	raw := message.NewCreateCollectionMessageBuilderV1().WithVChannel("v1").
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{Schema: schema}).MustBuildMutable().
		WithTimeTick(100).WithLastConfirmed(walimplstest.NewTestMessageID(99)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(100))
	module.handleCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(raw))
	require.Equal(t, []uint64{0, 100}, retained)
}

func TestDroppedCollectionReleasesQueryRetentionBeforeSummaryCleanup(t *testing.T) {
	var retained uint64
	patch := mockey.Mock((*walsummary.Manager).SetQueryRetention).To(func(_ *walsummary.Manager, _ string, start uint64) {
		retained = start
	}).Build()
	defer patch.UnPatch()
	module, err := NewModule(ModuleConfig{
		PChannel: "p1", VChannel: "v1", SummaryManager: &walsummary.Manager{},
		VChannelMeta: &streamingpb.VChannelMeta{
			Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CreateCollectionTimeTick: 1, CheckpointTimeTick: 10, TransformMaterializedTimeTick: 10,
		},
	})
	require.NoError(t, err)
	queryReferenced := true
	references := mockey.Mock((*queryresource.Manager).OldestDataVersion).To(func(*queryresource.Manager) (qviews.DataVersion, bool) {
		return qviews.DataVersion{}, queryReferenced
	}).Build()
	defer references.UnPatch()
	summaryRetired := false
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 11, SummaryRetired: func(string, uint64) bool {
		require.Equal(t, uint64(math.MaxUint64), retained, "release the pin before waiting for Summary GC")
		return summaryRetired
	}}
	require.Empty(t, module.ConsumeCleanupSnapshots(cleanup))
	require.Equal(t, uint64(1), retained, "existing QueryViews still own the history")
	queryReferenced = false
	require.Empty(t, module.ConsumeCleanupSnapshots(cleanup), "retain the tombstone until Summary GC finishes")
	require.Equal(t, uint64(math.MaxUint64), retained)
	summaryRetired = true
	snapshots := module.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
}
