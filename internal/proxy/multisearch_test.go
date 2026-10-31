package proxy

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/multisearchpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func multiSearchTestLimits() MultiSearchLimits {
	return MultiSearchLimits{MaxItems: 128, Parallelism: 4, GlobalParallelism: 6, MaxBatches: 3,
		MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20, Timeout: time.Second}
}

func multiSearchTestRequest(n int) *multisearchpb.MultiSearchRequest {
	r := &multisearchpb.MultiSearchRequest{}
	for i := range n {
		r.Items = append(r.Items, &multisearchpb.SearchItem{Id: fmt.Sprint(i), Search: &milvuspb.SearchRequest{CollectionName: fmt.Sprint(i), Nq: 1}})
	}
	return r
}

func TestMultiSearchIndependentOutcomes(t *testing.T) {
	original := merr.Status(merr.WrapErrServiceRateLimit(1, "test quota"))
	if original.ExtraInfo == nil {
		original.ExtraInfo = map[string]string{}
	}
	original.ExtraInfo["test"] = "preserved"
	exec := NewMultiSearchExecutor(multiSearchTestLimits(), func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		require.Equal(t, "default", req.DbName)
		switch req.CollectionName {
		case "0":
			return &milvuspb.SearchResults{Status: original}, nil
		case "1":
			return nil, status.Error(codes.PermissionDenied, "collection denied")
		default:
			return &milvuspb.SearchResults{Status: merr.Success(), CollectionName: req.CollectionName}, nil
		}
	})
	req := multiSearchTestRequest(3)
	before := proto.Clone(req)
	resp, err := exec.Execute(context.Background(), req)
	require.NoError(t, err)
	require.True(t, merr.Ok(resp.Status))
	require.Len(t, resp.Items, 3)
	require.True(t, proto.Equal(original, resp.Items[0].Result.Status))
	require.Equal(t, int32(codes.PermissionDenied), resp.Items[1].GrpcCode)
	require.Nil(t, resp.Items[1].Result)
	require.Equal(t, "2", resp.Items[2].Id)
	require.Equal(t, "2", resp.Items[2].Result.CollectionName)
	require.True(t, proto.Equal(before, req), "caller-owned requests must not be mutated")
}

func TestMultiSearchEnvelopeValidation(t *testing.T) {
	var calls atomic.Int64
	exec := NewMultiSearchExecutor(multiSearchTestLimits(), func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		calls.Add(1)
		return &milvuspb.SearchResults{Status: merr.Success()}, nil
	})
	duplicate := multiSearchTestRequest(2)
	duplicate.Items[1].Id = duplicate.Items[0].Id
	crossDB := multiSearchTestRequest(1)
	crossDB.Items[0].Search.DbName = "other"
	large := multiSearchTestRequest(1)
	large.Items[0].Search.Dsl = strings.Repeat("x", 1<<20)
	for _, req := range []*multisearchpb.MultiSearchRequest{nil, {}, multiSearchTestRequest(129), duplicate, crossDB, large,
		{Items: []*multisearchpb.SearchItem{nil}}, {Items: []*multisearchpb.SearchItem{{Id: "missing"}}}} {
		resp, err := exec.Execute(context.Background(), req)
		require.NoError(t, err)
		require.False(t, merr.Ok(resp.Status))
		require.Empty(t, resp.Items)
	}
	require.Zero(t, calls.Load(), "validation must finish before any item dispatch")
}

func TestMultiSearchDatabaseAndPointerIsolation(t *testing.T) {
	shared := &milvuspb.SearchRequest{CollectionName: "same"}
	req := &multisearchpb.MultiSearchRequest{Items: []*multisearchpb.SearchItem{{Id: "a", Search: shared}, {Id: "b", Search: shared}}}
	exec := NewMultiSearchExecutor(multiSearchTestLimits(), func(ctx context.Context, r *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		require.Equal(t, "from_header", r.DbName)
		r.CollectionName = "mutated"
		return &milvuspb.SearchResults{Status: merr.Success()}, nil
	})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("dbname", "from_header"))
	resp, err := exec.Execute(ctx, req)
	require.NoError(t, err)
	require.True(t, merr.Ok(resp.Status))
	require.Equal(t, "same", shared.CollectionName)
	require.Empty(t, shared.DbName)
}

func TestMultiSearchConcurrencyAndAdmission(t *testing.T) {
	limits := multiSearchTestLimits()
	limits.Parallelism, limits.GlobalParallelism, limits.MaxBatches = 2, 3, 2
	started := make(chan struct{}, 10)
	release := make(chan struct{})
	var active, peak atomic.Int64
	exec := NewMultiSearchExecutor(limits, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &milvuspb.SearchResults{Status: merr.Success()}, nil
	})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = exec.Execute(context.Background(), multiSearchTestRequest(2)) }()
	}
	for range 3 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers did not start")
		}
	}
	resp, err := exec.Execute(context.Background(), multiSearchTestRequest(1))
	require.NoError(t, err)
	require.Equal(t, merr.Code(merr.ErrServiceTooManyRequests), resp.Status.Code)
	require.True(t, resp.Status.Retriable)
	require.Equal(t, int64(3), peak.Load())
	close(release)
	wg.Wait()
	require.Zero(t, active.Load())
	require.Empty(t, exec.active)
	require.Empty(t, exec.batches)
}

func TestMultiSearchDeadlineStopsDispatch(t *testing.T) {
	limits := multiSearchTestLimits()
	limits.Parallelism, limits.GlobalParallelism = 1, 1
	limits.Timeout = 20 * time.Millisecond
	var calls atomic.Int64
	exec := NewMultiSearchExecutor(limits, func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	resp, err := exec.Execute(context.Background(), multiSearchTestRequest(8))
	require.Nil(t, resp)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Equal(t, int64(1), calls.Load())
	require.Empty(t, exec.active)
	require.Empty(t, exec.batches)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = exec.Execute(ctx, multiSearchTestRequest(1))
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Equal(t, int64(1), calls.Load())
}

func TestMultiSearchResponseBudgetAndInvalidResult(t *testing.T) {
	limits := multiSearchTestLimits()
	limits.MaxResponseBytes = 1024
	exec := NewMultiSearchExecutor(limits, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		return &milvuspb.SearchResults{Status: &commonpb.Status{Reason: strings.Repeat("x", 2048)}}, nil
	})
	resp, err := exec.Execute(context.Background(), multiSearchTestRequest(2))
	require.NoError(t, err)
	require.Equal(t, merr.Code(merr.ErrParameterTooLarge), resp.Status.Code)
	require.Empty(t, resp.Items)
	require.Empty(t, exec.active)
	exec = NewMultiSearchExecutor(limits, func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) { return nil, nil })
	resp, err = exec.Execute(context.Background(), multiSearchTestRequest(1))
	require.NoError(t, err)
	require.Equal(t, merr.Code(merr.ErrServiceInternal), resp.Items[0].Result.Status.Code)
}
