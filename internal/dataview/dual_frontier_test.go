package dataview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDualFrontiersOutOfOrderFlushCompactionAndRecovery(t *testing.T) {
	m := newPublicationTestManager(t)
	m.frontierProjector = func(context.Context, int64) (map[string]uint64, error) {
		t.Fatal("native snapshots must not read legacy checkpoint/collection metadata")
		return nil, nil
	}
	_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{
		CollectionID: 1, VChannels: []string{"v1", "v2"}, TransformStarts: map[string]uint64{"v1": 10, "v2": 20},
	})
	require.NoError(t, err)
	check := func(w, a, f uint64) *viewpb.DataViewOfCollection {
		ref, err := m.Latest(t.Context(), 1)
		require.NoError(t, err)
		defer ref.Deref()
		view := ref.DataView()
		require.Equal(t, w, view.Shards[0].GetGrowingTransformStartAfterTimetick())
		require.Equal(t, a, view.Shards[0].GetCompactedTransformStartAfterTimetick())
		require.Equal(t, f, transformStart(view.Shards[0]))
		require.Equal(t, uint64(20), transformStart(view.Shards[1]), "empty shards retain their own origin")
		return view
	}
	origin := check(10, 10, 10)
	y := LoadableSegment{SegmentID: 20, VChannel: "v1", PartitionID: 2, ManifestVersion: 1, TransformStartAfterTimetick: 100}
	x := LoadableSegment{SegmentID: 10, VChannel: "v1", PartitionID: 1, ManifestVersion: 1, TransformStartAfterTimetick: 50}
	flush := func(segment LoadableSegment, w uint64) {
		_, commit, _, err := m.PrepareFlush(t.Context(), FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segment}, GrowingTransformStarts: map[string]uint64{"v1": w}})
		require.NoError(t, err)
		commit()
	}
	flush(y, 50)
	first := check(50, 100, 50)
	flush(x, 200)
	second := check(200, 50, 50)
	require.Equal(t, uint64(100), first.Shards[0].GetCompactedTransformStartAfterTimetick(), "published A is immutable")
	_, _, abort, err := m.PrepareFlush(t.Context(), FlushDataViewEvent{CollectionID: 1, GrowingTransformStarts: map[string]uint64{"v1": 400}})
	require.NoError(t, err)
	abort()
	check(200, 50, 50)
	output := LoadableSegment{SegmentID: 30, VChannel: "v1", PartitionID: 1, ManifestVersion: 2, TransformStartAfterTimetick: 150}
	_, err = m.RecomputeNow(t.Context(), 1, projectSegments(output))
	require.NoError(t, err)
	compacted := check(200, 150, 150)
	bad := LoadableSegment{SegmentID: 40, VChannel: "v1", PartitionID: 3, ManifestVersion: 1, TransformStartAfterTimetick: 40}
	_, _, _, err = m.PrepareFlush(t.Context(), FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{bad}, GrowingTransformStarts: map[string]uint64{"v1": 300}})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	check(200, 150, 150)

	recovered := newCollectionState(1)
	for _, view := range []*viewpb.DataViewOfCollection{origin, first, second, compacted} {
		persisted := proto.Clone(view).(*viewpb.DataViewOfCollection)
		entry := newVersionEntry(persisted)
		recovered.versions[protoVersionToStruct(persisted.DataVersion)] = entry
		recovered.latest = entry
	}
	require.NoError(t, m.restoreTransformFrontiers(t.Context(), recovered))
	for _, expected := range []*viewpb.DataViewOfCollection{origin, first, second, compacted} {
		actual := recovered.versions[protoVersionToStruct(expected.DataVersion)].view
		require.True(t, proto.Equal(expected, actual), "recovery must use this version's W/A")
	}
}

func TestDualFrontierRecoveryRejectsInconsistentCoverage(t *testing.T) {
	m := newManager(t.Context(), nil, nil)
	view := transformTestView(1, 50)
	view.Shards[0].GrowingTransformStartAfterTimetick = 100
	view.Shards[0].CompactedTransformStartAfterTimetick = 80
	state := newCollectionState(1)
	entry := newVersionEntry(view)
	state.versions[protoVersionToStruct(view.DataVersion)] = entry
	state.latest = entry
	require.ErrorIs(t, m.restoreTransformFrontiers(t.Context(), state), merr.ErrDataIntegrity)
}

func TestRecomputeDoesNotCreateInitialTransformView(t *testing.T) {
	m := newPublicationTestManager(t)
	m.frontierProjector = func(context.Context, int64) (map[string]uint64, error) {
		t.Fatal("unexpected legacy lookup")
		return nil, nil
	}
	version, err := m.RecomputeNow(t.Context(), 1, projectSegments())
	require.NoError(t, err)
	require.Nil(t, version)
	_, err = m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v1", "v2"}, TransformStarts: map[string]uint64{"v1": 10, "v2": 20}})
	require.NoError(t, err)
	ref, err := m.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer ref.Deref()
	require.Len(t, ref.DataView().GetShards(), 2)
}

func TestDualFrontierPendingPublicationFences(t *testing.T) {
	m := newManager(t.Context(), nil, nil, WithPendingTransformProjector(func(context.Context, *viewpb.DataViewOfCollection) (map[string]uint64, error) {
		return map[string]uint64{"v1": 40}, nil
	}))
	view := transformTestView(1, 100)
	view.Shards[0].GrowingTransformStartAfterTimetick = 10
	view.Shards[0].CompactedTransformStartAfterTimetick = 100
	certificate := map[string]uint64{"v1": 200}
	require.NoError(t, m.advanceTransformFrontiers(t.Context(), view, certificate))
	require.Equal(t, uint64(40), view.Shards[0].GetGrowingTransformStartAfterTimetick())
	require.Equal(t, uint64(200), certificate["v1"], "caller owns its proof map")
	view.Shards[0].GrowingTransformStartAfterTimetick = 50
	require.ErrorIs(t, m.advanceTransformFrontiers(t.Context(), view, nil), merr.ErrServiceNotReady)
	m.pendingTransformProjector = func(context.Context, *viewpb.DataViewOfCollection) (map[string]uint64, error) {
		return nil, context.Canceled
	}
	require.ErrorIs(t, m.advanceTransformFrontiers(t.Context(), view, nil), context.Canceled)
	m.pendingTransformProjector = nil
	m.frontierProjector = func(context.Context, int64) (map[string]uint64, error) {
		t.Fatal("unexpected checkpoint lookup")
		return nil, nil
	}
	require.ErrorIs(t, m.advanceTransformFrontiers(t.Context(), transformTestView(1), map[string]uint64{"v1": 0}), merr.ErrServiceNotReady)
}

func TestDualFrontierRecoveryRejectsVersionRegression(t *testing.T) {
	m := newManager(t.Context(), nil, nil)
	for _, newer := range []struct{ w, a uint64 }{{90, 80}, {100, 40}} {
		state := newCollectionState(1)
		for i, values := range []struct{ w, a uint64 }{{100, 50}, newer} {
			view := transformTestView(int64(i+1), values.a)
			view.Shards[0].GrowingTransformStartAfterTimetick = values.w
			view.Shards[0].CompactedTransformStartAfterTimetick = values.a
			state.versions[protoVersionToStruct(view.DataVersion)] = newVersionEntry(view)
		}
		require.ErrorIs(t, m.restoreTransformFrontiers(t.Context(), state), merr.ErrDataIntegrity)
	}
}

func TestDualFrontierPublishChangePreservesCursorAlignment(t *testing.T) {
	m := newPublicationTestManager(t)
	_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v1"}, TransformStarts: map[string]uint64{"v1": 10}})
	require.NoError(t, err)
	_, commit, _, err := m.PrepareFlush(t.Context(), FlushDataViewEvent{CollectionID: 1, GrowingTransformStarts: map[string]uint64{"v1": 100}, Segments: []LoadableSegment{
		{SegmentID: 10, VChannel: "v1", PartitionID: 1, ManifestVersion: 1, TransformStartAfterTimetick: 30},
		{SegmentID: 20, VChannel: "v1", PartitionID: 1, ManifestVersion: 1, TransformStartAfterTimetick: 60},
	}})
	require.NoError(t, err)
	commit()
	view, commit, _, err := m.PublishChange(t.Context(), ChangeGroupDataViewEvent{CollectionID: 1, SupersededSegmentIDs: []int64{10}, NewSegments: []LoadableSegment{{SegmentID: 30, VChannel: "v1", PartitionID: 1, ManifestVersion: 1, TransformStartAfterTimetick: 80}}})
	require.NoError(t, err)
	commit()
	require.Equal(t, []int64{20, 30}, view.Shards[0].Partitions[0].GetSegmentIds())
	require.Equal(t, []uint64{60, 80}, view.Shards[0].Partitions[0].GetSegmentTransformStartAfterTimeticks())
	require.Equal(t, uint64(60), transformStart(view.Shards[0]))
}
