package grpcmixcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type segmentLoadInfoTestCoord struct{ types.MixCoordComponent }

func (*segmentLoadInfoTestCoord) GetQueryViewSegmentLoadInfo(context.Context, *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
	panic("patch with mockey")
}

func TestGetQueryViewSegmentLoadInfoForwarding(t *testing.T) {
	mockey.PatchConvey("the distributed service forwards unary requests and application errors", t, func() {
		request := &querypb.GetQueryViewSegmentLoadInfoRequest{CollectionID: 1, SegmentIDs: []int64{2, 3}}
		response := &querypb.GetQueryViewSegmentLoadInfoResponse{Status: merr.Status(merr.WrapErrSegmentNotFound(2))}
		called := false
		mockey.Mock((*segmentLoadInfoTestCoord).GetQueryViewSegmentLoadInfo).To(func(_ *segmentLoadInfoTestCoord, ctx context.Context, req *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
			require.Same(t, request, req)
			require.Equal(t, t.Context(), ctx)
			called = true
			return response, nil
		}).Build()
		server := &Server{mixCoord: &segmentLoadInfoTestCoord{}}
		actual, err := server.GetQueryViewSegmentLoadInfo(t.Context(), request)
		require.NoError(t, err)
		require.Same(t, response, actual)
		require.True(t, called)
	})
}
