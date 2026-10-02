package adaptor

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestWALOpenResourcesDrainsRecoveredViewsBeforeStorageClose(t *testing.T) {
	var storageClosed atomic.Bool
	var released atomic.Int32
	acquire := mockey.Mock((*vchannel.PChannelRecoveryManager).Acquire).Return().Build()
	defer acquire.UnPatch()
	release := mockey.Mock((*vchannel.PChannelRecoveryManager).Release).To(func(_ *vchannel.PChannelRecoveryManager, req snview.ReleaseResource) {
		require.False(t, storageClosed.Load(), "release callbacks need a live recovery scheduler")
		go func() {
			released.Add(1)
			req.OnDropped()
		}()
	}).Build()
	defer release.UnPatch()
	storageClose := mockey.Mock((*mock_recovery.MockRecoveryStorage).Close).To(func(*mock_recovery.MockRecoveryStorage) {
		require.EqualValues(t, 1, released.Load())
		storageClosed.Store(true)
	}).Build()
	defer storageClose.UnPatch()
	walClose := mockey.Mock((*roWALAdaptorImpl).Close).To(func(*roWALAdaptorImpl) {
		require.True(t, storageClosed.Load())
	}).Build()
	defer walClose.UnPatch()
	view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{
		CollectionId: 1, ReplicaId: 1, Vchannel: "p_1v0", State: viewpb.QueryViewState_QueryViewStateUp,
		Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1},
	}, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}
	h := snview.RecoverPChannelSNQueryViewHandler(context.Background(), "p", nil, &vchannel.PChannelRecoveryManager{}, []*viewpb.QueryViewOfShard{view})
	resources := &walOpenResources{queryViewHandler: h, param: &interceptors.InterceptorBuildParam{RecoveryStorage: &mock_recovery.MockRecoveryStorage{}}, roWAL: &roWALAdaptorImpl{}}
	resources.Close() // The explicit FLUSHING-stage close.
	resources.Close() // openRWWAL's deferred cleanup.
	require.EqualValues(t, 1, released.Load())
	require.Equal(t, 1, storageClose.Times())
	require.Equal(t, 1, walClose.Times())
}

func TestWALOpenResourcesTransfersHandlerOwnership(t *testing.T) {
	closeHandler := mockey.Mock((*snview.SNQueryViewHandler).CloseForHandoff).Return().Build()
	defer closeHandler.UnPatch()
	clear := mockey.Mock((*interceptors.InterceptorBuildParam).Clear).Return().Build()
	defer clear.UnPatch()
	resources := &walOpenResources{queryViewHandler: &snview.SNQueryViewHandler{}, param: &interceptors.InterceptorBuildParam{}}
	resources.Release()
	resources.Close()
	require.Zero(t, closeHandler.Times())
	require.Zero(t, clear.Times())
}
