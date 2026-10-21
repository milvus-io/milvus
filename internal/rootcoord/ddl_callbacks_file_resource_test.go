// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	metamocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func mockFileResourceBroadcast(t *testing.T, core *Core) *mock_broadcaster.MockBroadcastAPI {
	t.Helper()
	if core.meta == nil {
		core.meta = mockrootcoord.NewIMetaTable(t)
	}
	if meta, ok := core.meta.(*mockrootcoord.IMetaTable); ok {
		meta.EXPECT().ListFileResource(mock.Anything).Return(nil, uint64(0)).Maybe()
		meta.EXPECT().PrepareRemoveFileResource(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, name string) (*internalpb.FileResourceInfo, error) {
				return &internalpb.FileResourceInfo{Id: 100, Name: name}, nil
			}).Maybe()
	}
	if observer, ok := core.fileResourceObserver.(*MockFileResourceObserver); ok {
		observer.EXPECT().Sync().Return(nil).Maybe()
		observer.EXPECT().Notify().Return().Maybe()
	}
	registry.ResetRegistration()
	RegisterDDLCallbacks(core)
	api := mock_broadcaster.NewMockBroadcastAPI(t)
	api.EXPECT().Close().Return().Maybe()
	api.EXPECT().Broadcast(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			require.Empty(t, msg.BroadcastHeader().VChannels)
			require.False(t, msg.IsUnreplicable())
			return &types.BroadcastAppendResult{}, registry.CallMessageAckCallback(ctx, msg, nil)
		}).Maybe()
	patch := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).
		To(func(_ context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			require.Equal(t, []message.ResourceKey{message.NewExclusiveClusterResourceKey()}, keys)
			return api, nil
		}).Build()
	t.Cleanup(patch.UnPatch)
	return api
}

func fileResourceTestMeta(t *testing.T) (*MetaTable, *metamocks.RootCoordCatalog) {
	t.Helper()
	catalog := metamocks.NewRootCoordCatalog(t)
	return &MetaTable{
		catalog:               catalog,
		fileResourceName2Meta: make(map[string]*internalpb.FileResourceInfo),
		fileResourceID2Meta:   make(map[int64]*internalpb.FileResourceInfo),
		fileResourceRefCnt:    make(map[int64]int),
	}, catalog
}

func addFileResourceTestResult(resource *internalpb.FileResourceInfo) message.BroadcastResultAddFileResourceMessageV2 {
	msg := message.NewAddFileResourceMessageBuilderV2().
		WithHeader(&message.AddFileResourceMessageHeader{}).
		WithBody(&message.AddFileResourceMessageBody{Resource: resource}).
		WithControlChannelBroadcast().MustBuildBroadcast()
	return message.BroadcastResultAddFileResourceMessageV2{Message: message.MustAsBroadcastAddFileResourceMessageV2(msg)}
}

func removeFileResourceTestResult(id int64, name string) message.BroadcastResultRemoveFileResourceMessageV2 {
	msg := message.NewRemoveFileResourceMessageBuilderV2().
		WithHeader(&message.RemoveFileResourceMessageHeader{}).
		WithBody(&message.RemoveFileResourceMessageBody{ResourceId: id, Name: name}).
		WithControlChannelBroadcast().MustBuildBroadcast()
	return message.BroadcastResultRemoveFileResourceMessageV2{Message: message.MustAsBroadcastRemoveFileResourceMessageV2(msg)}
}

func TestFileResourceCallbacksNotifyAfterReplay(t *testing.T) {
	ctx := context.Background()
	meta, catalog := fileResourceTestMeta(t)
	resource := &internalpb.FileResourceInfo{Id: 123, Name: "dictionary", Path: "dict/custom.txt"}
	catalog.EXPECT().SaveFileResource(mock.Anything, mock.Anything, uint64(1)).Return(nil).Once()
	catalog.EXPECT().RemoveFileResource(mock.Anything, resource.Id, uint64(2)).Return(nil).Once()
	core := newTestCore(withMeta(meta))
	observer := NewMockFileResourceObserver(t)
	observer.EXPECT().Notify().Return().Times(4)
	core.SetFileResourceObserver(observer)
	callback := &DDLCallback{Core: core}
	for i := 0; i < 2; i++ {
		require.NoError(t, callback.addFileResourceV2AckCallback(ctx, addFileResourceTestResult(resource)))
		require.Equal(t, uint64(1), meta.fileResourceVersion)
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, callback.removeFileResourceV2AckCallback(ctx, removeFileResourceTestResult(resource.Id, resource.Name)))
		require.Equal(t, uint64(2), meta.fileResourceVersion)
	}
	require.Empty(t, meta.fileResourceID2Meta)
}

func TestFileResourceCallbacksPreserveIdentity(t *testing.T) {
	ctx := context.Background()
	meta, catalog := fileResourceTestMeta(t)
	callback := &DDLCallback{Core: newTestCore(withMeta(meta))}
	resource := &internalpb.FileResourceInfo{Id: 123, Name: "dictionary", Path: "dict/custom.txt"}
	catalog.EXPECT().SaveFileResource(mock.Anything, mock.Anything, uint64(1)).Return(nil).Once()
	require.NoError(t, callback.addFileResourceV2AckCallback(ctx, addFileResourceTestResult(resource)))
	require.NoError(t, callback.addFileResourceV2AckCallback(ctx, addFileResourceTestResult(resource)))
	require.True(t, proto.Equal(resource, meta.fileResourceID2Meta[123]))

	conflict := &internalpb.FileResourceInfo{Id: 456, Name: resource.Name, Path: resource.Path}
	require.ErrorIs(t, callback.addFileResourceV2AckCallback(ctx, addFileResourceTestResult(conflict)), merr.ErrServiceInternal)
	conflict = &internalpb.FileResourceInfo{Id: 123, Name: "other", Path: resource.Path}
	require.ErrorIs(t, callback.addFileResourceV2AckCallback(ctx, addFileResourceTestResult(conflict)), merr.ErrServiceInternal)

	// A delayed remove for an earlier registration cannot delete the current ID.
	require.NoError(t, callback.removeFileResourceV2AckCallback(ctx, removeFileResourceTestResult(122, resource.Name)))
	require.True(t, proto.Equal(resource, meta.fileResourceName2Meta[resource.Name]))
	require.Equal(t, uint64(1), meta.fileResourceVersion)
}

func TestFileResourceCallbackCatalogFailure(t *testing.T) {
	ctx := context.Background()
	meta, catalog := fileResourceTestMeta(t)
	callback := &DDLCallback{Core: newTestCore(withMeta(meta))}
	resource := &internalpb.FileResourceInfo{Id: 123, Name: "dictionary", Path: "dict/custom.txt"}
	failure := merr.Wrap(merr.ErrIoTooManyRequests, "etcd unavailable")
	catalog.EXPECT().SaveFileResource(mock.Anything, mock.Anything, uint64(1)).Return(failure).Once()
	catalog.EXPECT().SaveFileResource(mock.Anything, mock.Anything, uint64(1)).Return(nil).Once()
	require.ErrorIs(t, callback.addFileResourceV2AckCallback(ctx, addFileResourceTestResult(resource)), failure)
	require.Empty(t, meta.fileResourceID2Meta)
	require.Zero(t, meta.fileResourceVersion)
	require.NoError(t, callback.addFileResourceV2AckCallback(ctx, addFileResourceTestResult(resource)))
	catalog.EXPECT().RemoveFileResource(mock.Anything, resource.Id, uint64(2)).Return(failure).Once()
	catalog.EXPECT().RemoveFileResource(mock.Anything, resource.Id, uint64(2)).Return(nil).Once()
	require.ErrorIs(t, callback.removeFileResourceV2AckCallback(ctx, removeFileResourceTestResult(resource.Id, resource.Name)), failure)
	require.Contains(t, meta.fileResourceID2Meta, resource.Id)
	require.Equal(t, uint64(1), meta.fileResourceVersion)
	require.NoError(t, callback.removeFileResourceV2AckCallback(ctx, removeFileResourceTestResult(resource.Id, resource.Name)))
}

func TestFileResourceSyncFailureAfterBroadcast(t *testing.T) {
	ctx := context.Background()
	meta, catalog := fileResourceTestMeta(t)
	catalog.EXPECT().SaveFileResource(mock.Anything, mock.Anything, uint64(1)).Return(nil).Once()
	catalog.EXPECT().RemoveFileResource(mock.Anything, int64(123), uint64(2)).Return(nil).Once()
	storage := mock_storage.NewMockChunkManager(t)
	storage.EXPECT().Exist(mock.Anything, "dict/custom.txt").Return(true, nil)
	tso := newMockTsoAllocator()
	tso.GenerateTSOF = func(uint32) (uint64, error) { return 123, nil }
	core := newTestCore(withHealthyCode(), withMeta(meta), withStorage(storage), withTsoAllocator(tso))
	observer := NewMockFileResourceObserver(t)
	failure := merr.WrapErrServiceUnavailableMsg("node unavailable")
	observer.EXPECT().Notify().Return().Times(4)
	observer.EXPECT().Sync().Return(failure).Twice()
	core.SetFileResourceObserver(observer)
	mockFileResourceBroadcast(t, core)
	status, err := core.AddFileResource(ctx, &milvuspb.AddFileResourceRequest{Name: "dictionary", Path: "dict/custom.txt"})
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrServiceUnavailable)
	require.Contains(t, meta.fileResourceID2Meta, int64(123))
	status, err = core.RemoveFileResource(ctx, &milvuspb.RemoveFileResourceRequest{Name: "dictionary"})
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrServiceUnavailable)
	require.Empty(t, meta.fileResourceID2Meta)
}

func TestFileResourceRemoveChecksReferencesBeforeBroadcast(t *testing.T) {
	meta, _ := fileResourceTestMeta(t)
	resource := &internalpb.FileResourceInfo{Id: 123, Name: "dictionary", Path: "dict/custom.txt"}
	meta.fileResourceName2Meta[resource.Name] = resource
	meta.fileResourceID2Meta[resource.Id] = resource
	meta.fileResourceRefCnt[resource.Id] = 1
	core := newTestCore(withHealthyCode(), withMeta(meta))
	api := mockFileResourceBroadcast(t, core)
	status, err := core.RemoveFileResource(context.Background(), &milvuspb.RemoveFileResourceRequest{Name: resource.Name})
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrParameterInvalid)
	api.AssertNotCalled(t, "Broadcast", mock.Anything, mock.Anything)
	removeErr, removed := meta.RemoveFileResource(context.Background(), resource.Name, resource.Id)
	require.ErrorIs(t, removeErr, merr.ErrServiceUnavailable)
	require.False(t, removed)
}

func TestFileResourceAddKeepsMetadataUnchangedBeforeBroadcast(t *testing.T) {
	meta, _ := fileResourceTestMeta(t)
	storage := mock_storage.NewMockChunkManager(t)
	storage.EXPECT().Exist(mock.Anything, "dict/custom.txt").Return(true, nil)
	tso := newMockTsoAllocator()
	tso.GenerateTSOF = func(uint32) (uint64, error) { return 123, nil }
	core := newTestCore(withHealthyCode(), withMeta(meta), withStorage(storage), withTsoAllocator(tso))
	api := mockFileResourceBroadcast(t, core)
	api.ExpectedCalls = nil
	api.EXPECT().Close().Return().Once()
	api.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
		require.Empty(t, meta.fileResourceID2Meta)
		body := message.MustAsBroadcastAddFileResourceMessageV2(msg).MustBody()
		require.Equal(t, int64(123), body.GetResource().GetId())
		return nil, broadcaster.ErrBroadcastTaskNotCreated
	}).Once()
	status, err := core.AddFileResource(context.Background(), &milvuspb.AddFileResourceRequest{Name: "dictionary", Path: "dict/custom.txt"})
	require.Error(t, merr.CheckRPCCall(status, err))
	require.Empty(t, meta.fileResourceID2Meta)
}

func TestFileResourceRejectsMalformedMetadata(t *testing.T) {
	ctx := context.Background()
	core := newTestCore(withHealthyCode())
	status, err := core.AddFileResource(ctx, &milvuspb.AddFileResourceRequest{Name: "dictionary"})
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrParameterMissing)
	callback := &DDLCallback{Core: core}
	for _, resource := range []*internalpb.FileResourceInfo{
		nil,
		{Id: 0, Name: "dictionary", Path: "dict/custom.txt"},
		{Id: 123, Path: "dict/custom.txt"},
		{Id: 123, Name: "dictionary"},
	} {
		require.ErrorIs(t, callback.addFileResourceV2AckCallback(ctx, addFileResourceTestResult(resource)), merr.ErrServiceInternal)
	}
	require.ErrorIs(t, callback.removeFileResourceV2AckCallback(ctx, removeFileResourceTestResult(0, "dictionary")), merr.ErrServiceInternal)
}
