package grpcquerynode

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type batchMetadataTestClient struct{ types.MixCoordClient }

func (*batchMetadataTestClient) GetQueryViewSegmentLoadInfo(context.Context, *querypb.GetQueryViewSegmentLoadInfoRequest, ...grpc.CallOption) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
	panic("patch with mockey")
}

func newBatchMetadataTestProvider(t *testing.T, ctx context.Context, call func(context.Context, *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error)) *lazyQueryViewLoadMetadataProvider {
	t.Helper()
	patch := mockey.Mock((*batchMetadataTestClient).GetQueryViewSegmentLoadInfo).To(func(_ *batchMetadataTestClient, ctx context.Context, req *querypb.GetQueryViewSegmentLoadInfoRequest, _ ...grpc.CallOption) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
		return call(ctx, req)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	client := syncutil.NewFuture[types.MixCoordClient]()
	client.Set(&batchMetadataTestClient{})
	return &lazyQueryViewLoadMetadataProvider{ctx: ctx, mixCoord: client}
}

func batchMetadataSuccess(req *querypb.GetQueryViewSegmentLoadInfoRequest) *querypb.GetQueryViewSegmentLoadInfoResponse {
	response := &querypb.GetQueryViewSegmentLoadInfoResponse{Status: merr.Success(), IndexInfoList: []*indexpb.IndexInfo{{IndexID: 99}}}
	for _, id := range req.SegmentIDs {
		response.Results = append(response.Results, &querypb.QueryViewSegmentLoadInfoResult{SegmentID: id, Status: merr.Success(), LoadInfo: &querypb.SegmentLoadInfo{CollectionID: req.CollectionID, SegmentID: id, ManifestPath: "segment/_metadata/manifest-1.avro", TransformStartAfterTimetick: 100}})
	}
	return response
}

func TestQueryViewMetadataBatchGrouping(t *testing.T) {
	paramtable.Init()
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var calls []*querypb.GetQueryViewSegmentLoadInfoRequest
		provider := newBatchMetadataTestProvider(t, t.Context(), func(_ context.Context, req *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
			mu.Lock()
			calls = append(calls, proto.Clone(req).(*querypb.GetQueryViewSegmentLoadInfoRequest))
			mu.Unlock()
			resp := batchMetadataSuccess(req)
			for _, result := range resp.Results {
				if result.SegmentID == 2 {
					result.Status = merr.Status(merr.WrapErrSegmentNotFound(2))
					result.LoadInfo = nil
				}
			}
			return resp, nil
		})
		var wg sync.WaitGroup
		for _, item := range [][2]int64{{1, 1}, {1, 1}, {1, 2}, {1, 3}, {2, 4}} {
			wg.Go(func() {
				snapshot, err := provider.GetQueryViewSegmentLoadInfo(t.Context(), item[0], item[1])
				if item[1] == 2 {
					require.ErrorIs(t, err, merr.ErrSegmentNotFound)
					return
				}
				require.NoError(t, err)
				require.Equal(t, item[0], snapshot.LoadInfo.CollectionID)
				require.Equal(t, item[1], snapshot.LoadInfo.SegmentID)
				require.EqualValues(t, 99, snapshot.IndexInfos[0].IndexID)
			})
		}
		synctest.Wait()
		time.Sleep(queryViewMetadataBatchWait)
		wg.Wait()
		require.Len(t, calls, 2)
		for _, req := range calls {
			if req.CollectionID == 1 {
				require.ElementsMatch(t, []int64{1, 2, 3}, req.SegmentIDs)
			} else {
				require.Equal(t, []int64{4}, req.SegmentIDs)
			}
		}
		require.Empty(t, provider.pending)
	})
}

func TestQueryViewMetadataBatchCancellation(t *testing.T) {
	paramtable.Init()
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 1)
		finish := make(chan struct{})
		provider := newBatchMetadataTestProvider(t, t.Context(), func(ctx context.Context, req *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
			started <- ctx
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-finish:
				return batchMetadataSuccess(req), nil
			}
		})
		first, cancelFirst := context.WithCancel(t.Context())
		firstDone, secondDone := make(chan error, 1), make(chan error, 1)
		go func() { _, err := provider.GetQueryViewSegmentLoadInfo(first, 1, 1); firstDone <- err }()
		go func() { _, err := provider.GetQueryViewSegmentLoadInfo(t.Context(), 1, 2); secondDone <- err }()
		synctest.Wait()
		rpcCtx := <-started
		cancelFirst()
		require.ErrorIs(t, <-firstDone, context.Canceled)
		require.NoError(t, rpcCtx.Err(), "one canceled resource must not cancel the shared RPC")
		close(finish)
		require.NoError(t, <-secondDone)
	})
}

func TestQueryViewMetadataBatchLastWaiterCancelsRPC(t *testing.T) {
	paramtable.Init()
	synctest.Test(t, func(t *testing.T) {
		started, canceled := make(chan struct{}), make(chan struct{})
		provider := newBatchMetadataTestProvider(t, t.Context(), func(ctx context.Context, _ *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { _, err := provider.GetQueryViewSegmentLoadInfo(ctx, 1, 1); done <- err }()
		<-started
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		<-canceled
		require.Empty(t, provider.pending)
	})
}

func TestQueryViewMetadataBatchPendingCancellation(t *testing.T) {
	paramtable.Init()
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		provider := newBatchMetadataTestProvider(t, t.Context(), func(_ context.Context, req *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
			calls++
			return batchMetadataSuccess(req), nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { _, err := provider.GetQueryViewSegmentLoadInfo(ctx, 1, 1); done <- err }()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		time.Sleep(2 * queryViewMetadataBatchWait)
		require.Zero(t, calls)
		require.Empty(t, provider.pending)
	})
}

func TestQueryViewMetadataBatchSizeLimit(t *testing.T) {
	paramtable.Init()
	synctest.Test(t, func(t *testing.T) {
		sizes := make(chan int, 2)
		provider := newBatchMetadataTestProvider(t, t.Context(), func(_ context.Context, req *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
			sizes <- len(req.SegmentIDs)
			return batchMetadataSuccess(req), nil
		})
		batches := make([]*queryViewMetadataBatch, 0, queryViewMetadataBatchSize+1)
		for i := 0; i <= queryViewMetadataBatchSize; i++ {
			batches = append(batches, provider.enqueueSegmentMetadata(t.Context(), 1, int64(i+1)))
		}
		require.Same(t, batches[0], batches[queryViewMetadataBatchSize-1])
		require.NotSame(t, batches[0], batches[queryViewMetadataBatchSize])
		require.Equal(t, queryViewMetadataBatchSize, <-sizes)
		require.Equal(t, 1, <-sizes)
		for _, b := range batches {
			<-b.done
			provider.releaseSegmentMetadataWaiter(b)
		}
		require.Empty(t, provider.pending)
	})
}

func TestQueryViewMetadataBatchInvalidResponses(t *testing.T) {
	paramtable.Init()
	for _, mode := range []string{"missing", "duplicate", "missing-status", "wrong-collection", "wrong-segment", "missing-load-info", "unexpected", "request-error", "transport-error"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				provider := newBatchMetadataTestProvider(t, t.Context(), func(_ context.Context, req *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
					resp := batchMetadataSuccess(req)
					switch mode {
					case "missing":
						resp.Results = nil
					case "duplicate":
						resp.Results = append(resp.Results, resp.Results[0])
					case "missing-status":
						resp.Results[0].Status = nil
					case "wrong-collection":
						resp.Results[0].LoadInfo.CollectionID++
					case "wrong-segment":
						resp.Results[0].LoadInfo.SegmentID++
					case "missing-load-info":
						resp.Results[0].LoadInfo = nil
					case "unexpected":
						resp.Results[0].SegmentID++
					case "request-error":
						resp.Status = merr.Status(merr.WrapErrServiceNotReadyMsg("initializing"))
					case "transport-error":
						return nil, context.DeadlineExceeded
					}
					return resp, nil
				})
				_, err := provider.GetQueryViewSegmentLoadInfo(t.Context(), 1, 1)
				switch mode {
				case "request-error":
					require.ErrorIs(t, err, merr.ErrServiceNotReady)
				case "transport-error":
					require.ErrorIs(t, err, context.DeadlineExceeded)
				default:
					require.ErrorIs(t, err, merr.ErrServiceInternal)
				}
			})
		})
	}
}

func TestQueryViewMetadataBatchNodeShutdown(t *testing.T) {
	paramtable.Init()
	synctest.Test(t, func(t *testing.T) {
		nodeCtx, stopNode := context.WithCancel(t.Context())
		defer stopNode()
		started := make(chan struct{})
		provider := newBatchMetadataTestProvider(t, nodeCtx, func(ctx context.Context, _ *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		done := make(chan error, 1)
		go func() { _, err := provider.GetQueryViewSegmentLoadInfo(t.Context(), 1, 1); done <- err }()
		<-started
		stopNode()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Empty(t, provider.pending)
	})
}

func TestQueryViewMetadataBatchSkipsCanceledRequests(t *testing.T) {
	paramtable.Init()
	synctest.Test(t, func(t *testing.T) {
		provider := newBatchMetadataTestProvider(t, t.Context(), func(_ context.Context, req *querypb.GetQueryViewSegmentLoadInfoRequest) (*querypb.GetQueryViewSegmentLoadInfoResponse, error) {
			require.Equal(t, []int64{2}, req.SegmentIDs)
			return batchMetadataSuccess(req), nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { _, err := provider.GetQueryViewSegmentLoadInfo(ctx, 1, 1); first <- err }()
		go func() { _, err := provider.GetQueryViewSegmentLoadInfo(t.Context(), 1, 2); second <- err }()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-first, context.Canceled)
		require.NoError(t, <-second)
		_, err := provider.GetQueryViewSegmentLoadInfo(ctx, 1, 3)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, provider.pending)
	})
}
