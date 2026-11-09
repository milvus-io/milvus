package loadmgr

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadConfigObserverOnlyReportsCommittedChanges(t *testing.T) {
	store, hooks := newTestStore(t)
	var events []bool
	store.RegisterObserver(func(id int64, released bool) { require.EqualValues(t, 100, id); events = append(events, released) })
	hooks.beforeSave = func(*viewpb.CollectionLoadConfig) error {
		return merr.WrapErrServiceUnavailableMsg("catalog unavailable")
	}
	cfg := sampleConfig()
	require.Error(t, store.Put(t.Context(), cfg))
	require.Empty(t, events)
	hooks.beforeSave = nil
	require.NoError(t, store.Put(t.Context(), cfg))
	require.NoError(t, store.Put(t.Context(), cfg))
	store.StopPlanning(100)
	require.NoError(t, store.Remove(t.Context(), 100))
	require.Equal(t, []bool{false, true}, events)
}
