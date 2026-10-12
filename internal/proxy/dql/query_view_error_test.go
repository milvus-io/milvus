package dql

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewLoadError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *querypb.GetQueryViewLoadInfoResponse
		want     error
	}{
		{"released", &querypb.GetQueryViewLoadInfoResponse{Status: merr.Status(merr.WrapErrCollectionNotLoaded(1))}, merr.ErrCollectionNotLoaded},
		{"partition released", &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), PartitionIDs: []int64{10}}, merr.ErrPartitionNotLoaded},
		{"still loaded", &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), PartitionIDs: []int64{20}}, nil},
		{"coord unavailable", &querypb.GetQueryViewLoadInfoResponse{Status: merr.Status(merr.WrapErrServiceNotReadyMsg("recovering"))}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				mockey.Mock((*namespaceRequeryMockNode).MixCoord).Return(&mocks.MockMixCoordClient{}).Build()
				mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).Return(tc.response, nil).Build()
				original := viewerror.NewViewNotFound("released or recovering")
				err := queryViewLoadError(t.Context(), &namespaceRequeryMockNode{}, 1, []int64{20}, original)
				if tc.want != nil {
					require.ErrorIs(t, err, tc.want)
				} else {
					require.Same(t, original, err)
				}
			})
		})
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, merr.WrapErrServiceInternalMsg("unrelated")} {
		require.Equal(t, err, queryViewLoadError(t.Context(), nil, 1, nil, err))
	}
}
