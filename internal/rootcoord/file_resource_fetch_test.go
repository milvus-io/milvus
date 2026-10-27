package rootcoord

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestFetchFileResourcesBatch(t *testing.T) {
	state := commonpb.StateCode_Healthy
	health := mockey.Mock((*Core).GetStateCode).To(func(*Core) commonpb.StateCode { return state }).Build()
	defer health.UnPatch()
	dictionary := &internalpb.FileResourceInfo{Id: 10, Name: "dictionary", Path: "files/stop.txt", StorageName: "store"}
	synonyms := &internalpb.FileResourceInfo{Id: 11, Name: "synonyms", Path: "files/synonyms.txt"}
	meta := &MetaTable{
		fileResourceID2Meta:   map[int64]*internalpb.FileResourceInfo{10: dictionary, 11: synonyms},
		fileResourceName2Meta: map[string]*internalpb.FileResourceInfo{"dictionary": dictionary, "synonyms": synonyms},
	}
	core := &Core{meta: meta}
	for _, req := range []*rootcoordpb.FetchFileResourcesRequest{
		{ResourceIds: []int64{11, 10}},
		{ResourceNames: []string{"synonyms", "dictionary"}},
	} {
		response, err := core.FetchFileResources(t.Context(), req)
		require.NoError(t, err)
		require.NoError(t, merr.Error(response.GetStatus()))
		require.Len(t, response.GetResources(), 2)
		require.EqualValues(t, 11, response.Resources[0].GetId())
		require.EqualValues(t, 10, response.Resources[1].GetId())
		require.Equal(t, "store", response.Resources[1].GetStorageName())
		response.Resources[1].Path = "changed"
		require.Equal(t, "files/stop.txt", dictionary.GetPath(), "metadata is cloned under the lock")
	}
	for _, req := range []*rootcoordpb.FetchFileResourcesRequest{
		{ResourceIds: []int64{10, 99}},
		{ResourceNames: []string{"dictionary", "missing"}},
		{ResourceIds: []int64{10}, ResourceNames: []string{"dictionary"}},
	} {
		response, err := core.FetchFileResources(t.Context(), req)
		require.NoError(t, err)
		require.Error(t, merr.Error(response.GetStatus()))
		require.Empty(t, response.GetResources(), "failed batches do not return partial results")
	}
	// A recreated name cannot redirect an already-bound ID.
	delete(meta.fileResourceID2Meta, 10)
	replacement := &internalpb.FileResourceInfo{Id: 12, Name: "dictionary", Path: "new/stop.txt"}
	meta.fileResourceID2Meta[12] = replacement
	meta.fileResourceName2Meta["dictionary"] = replacement
	response, err := core.FetchFileResources(t.Context(), &rootcoordpb.FetchFileResourcesRequest{ResourceIds: []int64{10}})
	require.NoError(t, err)
	require.Error(t, merr.Error(response.GetStatus()))
	response, err = core.FetchFileResources(t.Context(), &rootcoordpb.FetchFileResourcesRequest{ResourceNames: []string{"dictionary"}})
	require.NoError(t, err)
	require.NoError(t, merr.Error(response.GetStatus()))
	require.EqualValues(t, 12, response.Resources[0].GetId())
	state = commonpb.StateCode_Abnormal
	response, err = core.FetchFileResources(t.Context(), &rootcoordpb.FetchFileResourcesRequest{ResourceIds: []int64{11}})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrServiceNotReady)
}
