package dataview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRefreshPublishesMembershipAndSchemaTogether(t *testing.T) {
	for _, version := range []int32{1, 2} {
		t.Run(string('0'+version), func(t *testing.T) {
			m, _ := newTestManager()
			_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
			require.NoError(t, err)
			_, err = m.RecomputeNow(t.Context(), 1, projectSegments(segment(10, "v1", 1)))
			require.NoError(t, err)
			old, err := m.Latest(t.Context(), 1)
			require.NoError(t, err)
			defer old.Deref()
			var publications []*api.CollectionDataView
			unsub := m.RegisterDataViewListener(func(_ int64, v *api.CollectionDataView) { publications = append(publications, v) })
			defer unsub()
			WithRecomputeGuard(func(context.Context, int64) (bool, error) { return true, nil })(m)
			next := projectSegments(segment(20, "v1", 1))
			_, err = m.RecomputeNow(t.Context(), 1, next)
			require.NoError(t, err)
			require.Len(t, publications, 1, "background cannot expose new membership under the old schema")
			v, err := m.PublishRefresh(t.Context(), 1, version, next)
			require.NoError(t, err)
			require.Len(t, publications, 2)
			require.Equal(t, version, publications[1].SchemaVersion)
			current, err := m.Latest(t.Context(), 1)
			require.NoError(t, err)
			defer current.Deref()
			require.Equal(t, []int64{20}, current.DataView().Shards[0].Partitions[0].SegmentIds)
			require.Equal(t, []int64{10}, old.DataView().Shards[0].Partitions[0].SegmentIds)
			require.EqualValues(t, 1, old.DataView().SchemaVersion)
			replay, err := m.PublishRefresh(t.Context(), 1, version, next)
			require.NoError(t, err)
			require.Equal(t, v, replay)
		})
	}
}

func TestRefreshRecomputeGuardFailure(t *testing.T) {
	m, _ := newTestManager()
	_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	WithRecomputeGuard(func(context.Context, int64) (bool, error) { return false, merr.ErrServiceNotReady })(m)
	_, err = m.RecomputeNow(t.Context(), 1, projectSegments(segment(20, "v1", 1)))
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	_, err = m.PublishRefresh(t.Context(), 1, 1, projectSegments(segment(20, "v1", 1)))
	require.NoError(t, err)
}
