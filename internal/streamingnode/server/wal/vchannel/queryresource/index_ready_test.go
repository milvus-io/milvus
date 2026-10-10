package queryresource

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/growingruntime"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestReadyRefreshesIndexesAcrossRetainedReplicaViews(t *testing.T) {
	growing, err := (growingruntime.SnapshotBuilder{}).NewRuntime()
	require.NoError(t, err)
	runtime := NewQueryRuntime(growing)
	runtime.state = queryRuntimeReady
	runtime.indexVersion = 19
	defer runtime.Close()
	barrier := mockey.Mock((*QueryRuntime).PrepareQueryView).Return(nil).Build()
	defer barrier.UnPatch()
	resolveErr := merr.WrapErrServiceNotReadyMsg("metadata temporarily unavailable")
	var applyErr error
	fetches, updates := 0, 0
	provider := &futureLoadInfoProvider{}
	resolve := mockey.Mock((*futureLoadInfoProvider).QueryViewLoadInfo).To(func(_ *futureLoadInfoProvider, _ context.Context, id int64, version uint64) (QueryViewLoadInfo, error) {
		fetches++
		require.Equal(t, int64(100), id)
		require.Equal(t, uint64(7), version) // hashes may decrease
		return QueryViewLoadInfo{IndexInfos: []*indexpb.IndexInfo{{FieldID: 102}}}, resolveErr
	}).Build()
	defer resolve.UnPatch()
	update := mockey.Mock((*growingruntime.Runtime).UpdateIndexMeta).To(func(_ *growingruntime.Runtime, _ context.Context, indexes []*indexpb.IndexInfo) error {
		updates++
		require.Equal(t, int64(102), indexes[0].GetFieldID())
		return applyErr
	}).Build()
	defer update.UnPatch()
	oldMeta, oldKey := testManagerQueryViewMetaAndKey(1)
	oldMeta.LoadInfoVersion = 19
	newMeta, newKey := testManagerQueryViewMetaAndKey(1)
	newKey.ShardID.ReplicaID++
	newMeta.CollectionId, newMeta.LoadInfoVersion = 100, 7
	manager := &Manager{runtime: runtime, loadInfoProvider: provider, refs: map[qviews.QueryViewKey]queryViewRef{
		oldKey: {meta: oldMeta}, newKey: {meta: newMeta},
	}}
	ready := 0
	callback := func() { ready++ }
	require.True(t, errors.Is(manager.prepareReady(t.Context(), newKey, callback), nodescheduler.ErrDelay))
	require.Zero(t, ready)
	require.Zero(t, updates)
	resolveErr = nil
	applyErr = merr.WrapErrServiceNotReadyMsg("native metadata not ready")
	require.True(t, errors.Is(manager.prepareReady(t.Context(), newKey, callback), nodescheduler.ErrDelay))
	require.Zero(t, ready)
	require.Equal(t, uint64(19), runtime.indexVersion)
	applyErr = nil
	require.NoError(t, manager.prepareReady(t.Context(), newKey, callback))
	require.Equal(t, 1, ready)
	require.Equal(t, uint64(7), runtime.indexVersion)
	require.Len(t, manager.refs, 2, "old leased view must remain retained")
	require.NoError(t, manager.prepareReady(t.Context(), newKey, callback))
	require.Equal(t, 3, fetches, "unchanged requirements reuse the prepared metadata")
	require.Equal(t, 2, updates)
}

func TestIndexRefreshSerializesResolutionAndPublication(t *testing.T) {
	growing, err := (growingruntime.SnapshotBuilder{}).NewRuntime()
	require.NoError(t, err)
	runtime := NewQueryRuntime(growing)
	runtime.state = queryRuntimeReady
	defer runtime.Close()
	provider := &futureLoadInfoProvider{}
	var current atomic.Uint64
	current.Store(1)
	firstFetch := make(chan struct{})
	resumeFetch := make(chan struct{})
	resolve := mockey.Mock((*futureLoadInfoProvider).QueryViewLoadInfo).To(func(_ *futureLoadInfoProvider, _ context.Context, _ int64, version uint64) (QueryViewLoadInfo, error) {
		if version != current.Load() {
			return QueryViewLoadInfo{}, merr.WrapErrServiceNotReadyMsg("load config changed")
		}
		if version == 1 {
			close(firstFetch)
			<-resumeFetch
		}
		return QueryViewLoadInfo{IndexInfos: []*indexpb.IndexInfo{{IndexID: int64(version)}}}, nil
	}).Build()
	defer resolve.UnPatch()
	var applied []int64
	update := mockey.Mock((*growingruntime.Runtime).UpdateIndexMeta).To(func(_ *growingruntime.Runtime, _ context.Context, indexes []*indexpb.IndexInfo) error {
		applied = append(applied, indexes[0].GetIndexID())
		return nil
	}).Build()
	defer update.UnPatch()
	firstDone := make(chan error, 1)
	go func() { firstDone <- runtime.PrepareIndexMeta(t.Context(), 100, 1, provider) }()
	<-firstFetch
	current.Store(2)
	secondDone := make(chan error, 1)
	go func() { secondDone <- runtime.PrepareIndexMeta(t.Context(), 100, 2, provider) }()
	close(resumeFetch)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.Equal(t, []int64{1, 2}, applied)
	require.True(t, errors.Is(runtime.PrepareIndexMeta(t.Context(), 100, 1, provider), merr.ErrServiceNotReady))
	require.Equal(t, []int64{1, 2}, applied, "stale preparation cannot roll back native metadata")
}

func TestIndexRefreshDoesNotPublishAfterClose(t *testing.T) {
	runtime := NewQueryRuntime()
	runtime.state = queryRuntimeReady
	provider := &futureLoadInfoProvider{}
	resolve := mockey.Mock((*futureLoadInfoProvider).QueryViewLoadInfo).To(func(*futureLoadInfoProvider, context.Context, int64, uint64) (QueryViewLoadInfo, error) {
		runtime.Close() // RPC must not hold the live-application/Close lock.
		return QueryViewLoadInfo{}, nil
	}).Build()
	defer resolve.UnPatch()
	require.ErrorIs(t, runtime.PrepareIndexMeta(t.Context(), 100, 7, provider), context.Canceled)
	require.Zero(t, runtime.indexVersion)
}
