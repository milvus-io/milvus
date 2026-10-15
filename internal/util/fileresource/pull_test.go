package fileresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPrepareFileResourcesPublishesFetchedSnapshot(t *testing.T) {
	t.Cleanup(func() { SetSnapshotFetcher(nil) })
	var applied *internalpb.SyncFileResourceRequest
	apply := mockey.Mock(Sync).To(func(ctx context.Context, version uint64, resources []*internalpb.FileResourceInfo) error {
		require.NoError(t, ctx.Err())
		applied = proto.Clone(&internalpb.SyncFileResourceRequest{Version: version, Resources: resources}).(*internalpb.SyncFileResourceRequest)
		return nil
	}).Build()
	defer apply.UnPatch()
	resource := &internalpb.FileResourceInfo{Id: 3, Name: "model", Path: "models/rank.ubj", StorageName: "store"}
	SetSnapshotFetcher(func(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
		_, bounded := ctx.Deadline()
		require.True(t, bounded)
		return []*internalpb.FileResourceInfo{resource}, 5, nil
	})
	require.NoError(t, Prepare(t.Context()))
	require.EqualValues(t, 5, applied.GetVersion())
	require.True(t, proto.Equal(resource, applied.GetResources()[0]))
	SetSnapshotFetcher(func(context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
		return nil, 6, nil
	})
	require.NoError(t, Prepare(t.Context()))
	require.EqualValues(t, 6, applied.GetVersion())
	require.Empty(t, applied.GetResources(), "removal publishes the empty newer inventory")
}

func TestPrepareFileResourcesFailureDoesNotPublish(t *testing.T) {
	t.Cleanup(func() { SetSnapshotFetcher(nil) })
	apply := mockey.Mock(Sync).To(func(context.Context, uint64, []*internalpb.FileResourceInfo) error {
		t.Fatal("failed fetch cannot replace the active resources")
		return nil
	}).Build()
	defer apply.UnPatch()
	SetSnapshotFetcher(func(context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
		return nil, 0, merr.ErrServiceUnavailable
	})
	require.ErrorIs(t, Prepare(t.Context()), merr.ErrServiceUnavailable)
	SetSnapshotFetcher(func(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
		<-ctx.Done()
		return nil, 0, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, Prepare(ctx), context.Canceled)
	SetSnapshotFetcher(nil)
	require.NoError(t, Prepare(t.Context()), "workers using the existing push path need no fetcher")
}
