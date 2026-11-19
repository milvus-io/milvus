package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSealedReadTimestamp(t *testing.T) {
	require.Zero(t, SealedReadTimestamp(nil))
	// Without a read point the transform frontier is the read timestamp, as before.
	require.EqualValues(t, 90, SealedReadTimestamp(&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}))
	require.EqualValues(t, 135, SealedReadTimestamp(&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 100, ReadTimetick: 135}))
}

func TestPlanSnapshotTimestamp(t *testing.T) {
	require.Zero(t, PlanSnapshotTimestamp(nil))
	require.EqualValues(t, 100, PlanSnapshotTimestamp(&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}))
	require.EqualValues(t, 300, PlanSnapshotTimestamp(&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 300}))
	require.EqualValues(t, 135, PlanSnapshotTimestamp(&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 100, ReadTimetick: 135}))
}

func TestLineageMVCCMismatch(t *testing.T) {
	lineage := &viewpb.QueryViewLineage{Vchannels: []string{"t1", "t2"}, SwitchTimetick: 100}
	plain := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}
	full := &viewpb.QueryPlanMVCC{
		GrowingTimetick: 100, TransformingTimetick: 100, ReadTimetick: 135,
		LineageTransformingTimeticks: map[string]uint64{"t1": 110, "t2": 130},
	}

	require.Empty(t, LineageMVCCMismatch(nil, plain))
	require.Empty(t, LineageMVCCMismatch(nil, nil))
	require.Empty(t, LineageMVCCMismatch(lineage, full))

	require.NotEmpty(t, LineageMVCCMismatch(nil, full))
	require.NotEmpty(t, LineageMVCCMismatch(nil, &viewpb.QueryPlanMVCC{ReadTimetick: 1}))
	require.NotEmpty(t, LineageMVCCMismatch(lineage, plain))
	require.NotEmpty(t, LineageMVCCMismatch(lineage, &viewpb.QueryPlanMVCC{ReadTimetick: 135, LineageTransformingTimeticks: map[string]uint64{"t1": 110}}))
	require.NotEmpty(t, LineageMVCCMismatch(lineage, &viewpb.QueryPlanMVCC{ReadTimetick: 135, LineageTransformingTimeticks: map[string]uint64{"t1": 110, "t3": 1}}))
}

func TestValidLineage(t *testing.T) {
	require.True(t, ValidLineage("s", nil))
	require.True(t, ValidLineage("s", &viewpb.QueryViewLineage{Vchannels: []string{"t1", "t2"}, SwitchTimetick: 100}))
	require.False(t, ValidLineage("s", &viewpb.QueryViewLineage{SwitchTimetick: 100}))
	require.False(t, ValidLineage("s", &viewpb.QueryViewLineage{Vchannels: []string{"t1"}}))
	require.False(t, ValidLineage("s", &viewpb.QueryViewLineage{Vchannels: []string{"t1", "t1"}, SwitchTimetick: 100}))
	require.False(t, ValidLineage("s", &viewpb.QueryViewLineage{Vchannels: []string{"t1", ""}, SwitchTimetick: 100}))
	require.False(t, ValidLineage("s", &viewpb.QueryViewLineage{Vchannels: []string{"s", "t1"}, SwitchTimetick: 100}))
}

func TestQueryViewBuilderLineage(t *testing.T) {
	dataView := &viewpb.DataViewOfCollection{
		CollectionId: 1,
		Shards:       []*viewpb.DataViewOfShard{{Vchannel: "s", GrowingTransformStartAfterTimetick: 90, CompactedTransformStartAfterTimetick: 80}},
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1},
	}

	t.Run("absent lineage leaves the view as before", func(t *testing.T) {
		plain := NewQueryViewAtCoordBuilder(7, dataView, "s").Build()
		require.Nil(t, plain.GetMeta().GetLineage())
		cleared := NewQueryViewAtCoordBuilder(7, dataView, "s").SetLineage([]string{"t1"}, 100).SetLineage(nil, 100).Build()
		// Byte identical: a view without lineage is the view master builds.
		want, err := proto.MarshalOptions{Deterministic: true}.Marshal(plain)
		require.NoError(t, err)
		got, err := proto.MarshalOptions{Deterministic: true}.Marshal(cleared)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("lineage is built and survives a placement copy", func(t *testing.T) {
		targets := []string{"t1", "t2"}
		view := NewQueryViewAtCoordBuilder(7, dataView, "s").SetLineage(targets, 100).SetPreparationPriority(8).Build()
		targets[0] = "mutated"
		want := &viewpb.QueryViewLineage{Vchannels: []string{"t1", "t2"}, SwitchTimetick: 100}
		require.True(t, proto.Equal(want, view.GetMeta().GetLineage()))

		copyBuilder := NewQueryViewAtCoordBuilderFromView(view)
		view.Meta.Control.PreparationPriority = 12
		copied := copyBuilder.Build()
		require.EqualValues(t, 8, copied.GetMeta().GetControl().GetPreparationPriority())
		require.True(t, proto.Equal(want, copied.GetMeta().GetLineage()))
		require.NotSame(t, view.GetMeta().GetLineage(), copied.GetMeta().GetLineage())
	})
}
