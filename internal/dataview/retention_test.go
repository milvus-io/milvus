package dataview

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDataViewRetentionSkipsLockedCollections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newManager(t.Context(), &fakeDataViewCatalog{}, nil)
		for _, event := range []CreateCollectionDataViewEvent{
			{CollectionID: 1, VChannels: []string{"other_1v0"}, TransformStarts: map[string]uint64{"other_1v0": 10}},
			{CollectionID: 2, VChannels: []string{"p_2v0"}, TransformStarts: map[string]uint64{"p_2v0": 20}},
			{CollectionID: 3, VChannels: []string{"p_3v0"}, TransformStarts: map[string]uint64{"p_3v0": 30}},
		} {
			_, err := m.OnCreateCollection(t.Context(), event)
			require.NoError(t, err)
		}
		m.states[1].mu.Lock()
		m.states[2].mu.Lock()
		done := make(chan map[string]uint64, 1)
		go func() { done <- m.DataViewRetention("p") }()
		synctest.Wait()
		m.states[1].mu.Unlock()
		m.states[2].mu.Unlock()
		select {
		case result := <-done:
			require.Equal(t, map[string]uint64{"p_3v0": 30}, result)
		default:
			t.Fatal("retention RPC waited for a collection lock")
		}
		require.Equal(t, map[string]uint64{"p_2v0": 20, "p_3v0": 30}, m.DataViewRetention("p"))
	})
}

func TestDataViewRetentionOldestVersion(t *testing.T) {
	catalog := &fakeDataViewCatalog{}
	m := NewManager(catalog, nil).(*dataViewManager)
	add := func(id int64, streaming, compact, w, a uint64, channels ...string) *versionEntry {
		state := m.states[id]
		if state == nil {
			state = newCollectionState(id)
			m.states[id] = state
		}
		view := &viewpb.DataViewOfCollection{CollectionId: id, DataVersion: &viewpb.DataVersion{StreamingVersion: int64(streaming), CompactVersion: int64(compact)}}
		for _, channel := range channels {
			view.Shards = append(view.Shards, &viewpb.DataViewOfShard{Vchannel: channel, GrowingTransformStartAfterTimetick: w, CompactedTransformStartAfterTimetick: a})
		}
		entry := &versionEntry{view: view, refs: &versionRefCounter{}}
		state.versions[protoVersionToStruct(view.DataVersion)] = entry
		if state.latest == nil || compareDataVersion(view.DataVersion, state.latest.view.DataVersion) > 0 {
			state.latest = entry
		}
		return entry
	}
	old := add(1, 1, 9, 50, 30, "p_1v0", "other_1v1")
	add(1, 2, 0, 100, 80, "p_1v0", "other_1v1")
	add(2, 1, 0, 70, 60, "p_2v0")
	check := func(want uint64) {
		require.Equal(t, map[string]uint64{"p_1v0": want, "p_2v0": 60}, m.DataViewRetention("p"))
	}
	m.states[3] = newCollectionState(3)
	check(30)
	// Live QV references preserve the old snapshot even during version GC.
	old.refs.count = 1
	require.NoError(t, m.GarbageCollect(t.Context(), 1, 1))
	check(30)
	old.refs.count = 0
	// Inspect the actual mark-before-delete window, then fail the catalog I/O.
	catalog.dropHook = func() {
		require.True(t, old.isTombstone)
		check(30)
	}
	catalog.dropErr = merr.WrapErrServiceUnavailableMsg("catalog unavailable")
	require.Error(t, m.GarbageCollect(t.Context(), 1, 1))
	require.False(t, old.isTombstone)
	check(30)
	catalog.dropErr = nil
	require.NoError(t, m.GarbageCollect(t.Context(), 1, 1))
	check(80)
	floor := m.DataViewRetention("missing")
	require.Empty(t, floor)
	// Unknown legacy coverage pins the result to zero.
	add(4, 1, 0, 0, 80, "p_4v0")
	floor = m.DataViewRetention("p")
	require.Equal(t, map[string]uint64{"p_1v0": 80, "p_2v0": 60, "p_4v0": 0}, floor)
}

func TestDataViewRetentionFailedRecoveryBootstrap(t *testing.T) {
	catalog := &fakeDataViewCatalog{}
	m, err := RecoverManager(t.Context(), catalog, recoverAllCollections,
		func(context.Context, int64, *viewpb.DataViewOfCollection) ([]LoadableSegment, error) {
			return nil, merr.WrapErrServiceUnavailableMsg("projection unavailable")
		}, []int64{1}, map[int64][]string{1: {"p_1v0"}})
	require.NoError(t, err)
	require.Empty(t, m.DataViewRetention("p"))
	// Return known shards without inventing a frontier for the missing snapshot.
	_, err = m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 2, VChannels: []string{"p_2v0"}, TransformStarts: map[string]uint64{"p_2v0": 100}})
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"p_2v0": 100}, m.DataViewRetention("p"))
	// A successful retry installs the lower frontier and makes GC progress safe.
	_, err = m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"p_1v0"}, TransformStarts: map[string]uint64{"p_1v0": 10}})
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"p_1v0": 10, "p_2v0": 100}, m.DataViewRetention("p"))
}
