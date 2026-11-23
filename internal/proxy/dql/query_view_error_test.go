package dql

import (
	"context"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/mocks"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
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
				original := merr.Wrap(merr.ErrQueryViewNotReady, "released or recovering")
				err := QueryViewLoadError(t.Context(), &namespaceRequeryMockNode{}, 1, []int64{20}, original)
				if tc.want != nil {
					require.ErrorIs(t, err, tc.want)
				} else {
					require.Same(t, original, err)
				}
			})
		})
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, merr.WrapErrServiceInternalMsg("unrelated")} {
		got := QueryViewLoadError(t.Context(), nil, 1, nil, err)
		require.True(t, errors.Is(got, err))
		require.Equal(t, merr.Status(err).GetCode(), merr.Status(got).GetCode())
	}
}

func TestQueryViewLoadErrorNormalizesStreamErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *querypb.GetQueryViewLoadInfoResponse
		want     error
	}{
		{"released", &querypb.GetQueryViewLoadInfoResponse{Status: merr.Status(merr.ErrCollectionNotLoaded)}, merr.ErrCollectionNotLoaded},
		{"dropped", &querypb.GetQueryViewLoadInfoResponse{Status: merr.Status(merr.ErrCollectionNotFound)}, merr.ErrCollectionNotFound},
		{"partition released", &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), PartitionIDs: []int64{10}}, merr.ErrPartitionNotLoaded},
		{"loaded", &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), PartitionIDs: []int64{20}}, merr.ErrQueryViewNotReady},
		{"coord unavailable", &querypb.GetQueryViewLoadInfoResponse{Status: merr.Status(merr.ErrServiceNotReady)}, merr.ErrQueryViewNotReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				mockey.Mock((*namespaceRequeryMockNode).MixCoord).Return(&mocks.MockMixCoordClient{}).Build()
				lookup := mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).Return(tc.response, nil).Build()
				source := viewerror.NewViewNotFound("worker view missing")
				wire := viewerror.NewGRPCStatusFromError(source).Err()
				for _, input := range []error{source, wire, viewerror.ConvertViewError("QueryOnViewStream", wire)} {
					got := QueryViewLoadError(t.Context(), &namespaceRequeryMockNode{}, 1, []int64{20}, merr.Wrap(input, "child stream"))
					require.ErrorIs(t, got, tc.want)
					require.Equal(t, merr.Status(tc.want).GetCode(), merr.Status(got).GetCode())
					require.NotContains(t, got.Error(), "VIEW_CODE_")
				}
				require.EqualValues(t, 3, lookup.Times())
			})
		})
	}
}

func TestQueryViewStreamTerminalError(t *testing.T) {
	for _, mode := range []string{"Query", "Search", "Hybrid"} {
		for _, tc := range []struct {
			name     string
			failure  error
			want     error
			attempts int
			lookups  int
			closeErr error
		}{
			{"missing view", viewerror.NewGRPCStatusFromError(viewerror.NewViewNotFound("missing")).Err(), merr.ErrQueryViewNotReady, 2, 1, nil},
			{"invalidated view", viewerror.NewGRPCStatusFromError(viewerror.NewViewInvalidated("retired")).Err(), merr.ErrQueryViewNotReady, 2, 1, nil},
			{"shutdown", viewerror.NewGRPCStatusFromError(viewerror.NewOnShutdownError("closing")).Err(), merr.ErrServiceUnavailable, 2, 1, nil},
			{"not primary", viewerror.NewGRPCStatusFromError(viewerror.NewNotPrimaryError("moved")).Err(), merr.ErrServiceUnavailable, 2, 1, nil},
			{"unavailable", status.Error(codes.Unavailable, "disconnected"), merr.ErrServiceUnavailable, 2, 1, nil},
			{"business error", viewerror.NewGRPCStatusFromError(merr.SegcoreError(2042, "invalid ef")).Err(), merr.SegcoreError(2042, "invalid ef"), 1, 0, nil},
			{"unknown", viewerror.NewGRPCStatusFromError(viewerror.NewUnknownError("unexpected")).Err(), merr.ErrServiceInternal, 1, 0, nil},
			{"canceled", status.Error(codes.Canceled, "canceled"), context.Canceled, 1, 0, nil},
			{"deadline", status.Error(codes.DeadlineExceeded, "timeout"), context.DeadlineExceeded, 1, 0, nil},
			{"missing view and close failure", viewerror.NewGRPCStatusFromError(viewerror.NewViewNotFound("missing")).Err(), merr.ErrQueryViewNotReady, 2, 1, io.ErrClosedPipe},
			{"unavailable and close failure", status.Error(codes.Unavailable, "disconnected"), merr.ErrServiceUnavailable, 2, 1, io.ErrClosedPipe},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				mockey.PatchConvey("normalize only after retry decisions", t, func() {
					mockey.Mock((*namespaceRequeryMockNode).MixCoord).Return(&mocks.MockMixCoordClient{}).Build()
					lookup := mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).Return(
						&querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil).Build()
					failure := merr.Wrap(viewerror.ConvertViewError("stream Recv", tc.failure), "child stream")
					legacy := &fakeLegacyQueryClient{maxRetries: 2}
					var queryStreams []*fakeLegacyQueryStream
					var searchStreams []*fakeLegacySearchStream
					legacy.query = func(int) (*queryclient.LegacyQueryResult, error) {
						stream := &fakeLegacyQueryStream{closeErr: tc.closeErr, recv: []fakeLegacyQueryRecv{
							{chunk: newFinalQueryChunk([]int64{1}, 99)}, {err: failure},
						}}
						queryStreams = append(queryStreams, stream)
						return &queryclient.LegacyQueryResult{Stream: stream}, nil
					}
					legacy.search = func(int) (*queryclient.LegacySearchResult, error) {
						stream := &fakeLegacySearchStream{closeErr: tc.closeErr, recv: []fakeLegacySearchRecv{
							{chunk: newFinalSearchChunk([]int64{1}, []float32{1}, "failed")}, {err: failure},
						}}
						searchStreams = append(searchStreams, stream)
						if mode == "Hybrid" {
							sibling := &fakeLegacySearchStream{}
							searchStreams = append(searchStreams, sibling)
							return &queryclient.LegacySearchResult{SubStreams: []searchreducestream.ReduceStream{stream, sibling}}, nil
						}
						return &queryclient.LegacySearchResult{Stream: stream}, nil
					}
					var err error
					var retryErrors []error
					if mode == "Query" {
						task := newQueryViewQueryTask(t, legacy, 2)
						task.node = &namespaceRequeryMockNode{}
						err = task.Execute(t.Context())
						retryErrors = legacy.lastQueryRequest.RetryErrors
						require.Empty(t, task.resultBuf.Collect())
					} else {
						task := newQueryViewSearchTask(t, legacy, 2)
						task.node = &namespaceRequeryMockNode{}
						if mode == "Hybrid" {
							task.IsIterator, task.IsAdvanced = false, true
							task.rerankMeta = &legacyRerankMeta{}
							task.SubReqs = []*internalpb.SubSearchRequest{
								{Nq: 1, Topk: 2, MetricType: "IP", SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER},
								{Nq: 1, Topk: 2, MetricType: "IP", SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER},
							}
						}
						err = task.Execute(t.Context())
						retryErrors = legacy.lastSearchRequest.RetryErrors
						require.Nil(t, task.reducedResult)
						require.Empty(t, task.reducedSubResults)
						require.Empty(t, task.queryChannelsTs)
						require.Empty(t, task.queryViewPlans)
					}
					require.Error(t, err)
					require.Equal(t, merr.Status(tc.want).GetCode(), merr.Status(err).GetCode())
					require.Equal(t, merr.Status(tc.want).GetErrorCode(), merr.Status(err).GetErrorCode())
					require.Equal(t, merr.GetErrorType(tc.want), merr.GetErrorType(err))
					require.Equal(t, merr.IsRetryableErr(tc.want), merr.IsRetryableErr(err))
					if merr.IsCanceledOrTimeout(tc.want) {
						require.True(t, errors.Is(err, tc.want))
					}
					require.Equal(t, tc.attempts, legacy.queryCalled+legacy.searchCalled)
					require.EqualValues(t, tc.lookups, lookup.Times())
					if tc.attempts > 1 {
						require.NotEmpty(t, retryErrors)
						for _, retryErr := range retryErrors {
							require.Equal(t, status.Code(failure), status.Code(retryErr))
							require.Equal(t, viewerror.AsViewError(failure).Code, viewerror.AsViewError(retryErr).Code)
						}
					}
					for _, stream := range queryStreams {
						require.Equal(t, 1, stream.closeCalls)
					}
					for _, stream := range searchStreams {
						require.Equal(t, 1, stream.closeCalls)
					}
				})
			})
		}
	}
}
