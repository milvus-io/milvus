package rootcoord

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestFileResourceSnapshotIncludesInventoryVersion(t *testing.T) {
	state := commonpb.StateCode_Healthy
	health := mockey.Mock((*Core).GetStateCode).To(func(*Core) commonpb.StateCode { return state }).Build()
	defer health.UnPatch()
	meta := &MetaTable{fileResourceVersion: 7, fileResourceID2Meta: map[int64]*internalpb.FileResourceInfo{
		10: {Id: 10, Name: "dictionary", Path: "files/dictionary.txt", StorageName: "store"},
	}}
	core := &Core{meta: meta}
	response, err := core.GetFileResourceSnapshot(t.Context(), &milvuspb.ListFileResourcesRequest{})
	require.NoError(t, err)
	require.NoError(t, merr.Error(response.GetStatus()))
	require.EqualValues(t, 7, response.GetVersion())
	require.Len(t, response.GetResources(), 1)
	require.Equal(t, "store", response.GetResources()[0].GetStorageName())
	meta.fileResourceVersion++
	delete(meta.fileResourceID2Meta, 10)
	response, err = core.GetFileResourceSnapshot(t.Context(), &milvuspb.ListFileResourcesRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 8, response.GetVersion())
	require.Empty(t, response.GetResources())
	state = commonpb.StateCode_Abnormal
	response, err = core.GetFileResourceSnapshot(t.Context(), &milvuspb.ListFileResourcesRequest{})
	require.NoError(t, err)
	require.Error(t, merr.Error(response.GetStatus()))
	require.Empty(t, response.GetResources())
}
