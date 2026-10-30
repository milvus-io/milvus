package coordinator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestMixCoordQueryViewLoadInfoPreservesResolvedScope(t *testing.T) {
	qn := &querycoordv2.Server{}
	s := &mixCoordImpl{queryCoordServer: qn}
	resolved := &querypb.GetQueryViewLoadInfoResponse{
		Status: merr.Success(), CollectionID: 1, Version: 9,
		IndexInfoList: []*indexpb.IndexInfo{{FieldID: 100, IndexID: 200}},
	}
	patch := mockey.Mock((*querycoordv2.Server).GetQueryViewLoadInfo).To(func(receiver *querycoordv2.Server, _ context.Context, req *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		require.Same(t, qn, receiver)
		require.EqualValues(t, 1, req.CollectionID)
		require.EqualValues(t, 9, req.Version)
		return resolved, nil
	}).Build()
	defer patch.UnPatch()
	resp, err := s.GetQueryViewLoadInfo(t.Context(), &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.Same(t, resolved, resp, "MixCoord must not replace the store's scoped indexes")
}

func TestMixCoordQueryViewCollectionIndexInfos(t *testing.T) {
	dc := &datacoord.Server{}
	indexes := []*indexpb.IndexInfo{{FieldID: 100, IndexID: 200}}
	patch := mockey.Mock((*datacoord.Server).GetQueryViewCollectionIndexInfos).To(func(receiver *datacoord.Server, id int64) []*indexpb.IndexInfo {
		require.Same(t, dc, receiver)
		require.EqualValues(t, 1, id)
		return indexes
	}).Build()
	defer patch.UnPatch()
	s := &mixCoordImpl{datacoordServer: dc}
	require.Equal(t, indexes, s.QueryViewCollectionIndexInfos(1))
}
