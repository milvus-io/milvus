package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/multisearchpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MultiSearchLimits are fixed for the lifetime of a Proxy server.
type MultiSearchLimits struct {
	MaxItems, Parallelism, GlobalParallelism, MaxBatches int
	MaxRequestBytes, MaxResponseBytes                    int
	Timeout                                              time.Duration
}

// MultiSearchExecutor coordinates independent searches. It knows nothing about
// schemas, shards, score reduction, or retries; search must supply the ordinary
// Search entry checks as well as execution.
type MultiSearchExecutor struct {
	limits  MultiSearchLimits
	search  func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)
	batches chan struct{}
	active  chan struct{}
}

func NewMultiSearchExecutor(limits MultiSearchLimits, search func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)) *MultiSearchExecutor {
	return &MultiSearchExecutor{limits: limits, search: search,
		batches: make(chan struct{}, limits.MaxBatches), active: make(chan struct{}, limits.GlobalParallelism)}
}

func (e *MultiSearchExecutor) Execute(ctx context.Context, req *multisearchpb.MultiSearchRequest) (*multisearchpb.MultiSearchResponse, error) {
	fail := func(err error) (*multisearchpb.MultiSearchResponse, error) {
		return &multisearchpb.MultiSearchResponse{Status: merr.Status(err)}, nil
	}
	if req == nil || len(req.GetItems()) == 0 || len(req.GetItems()) > e.limits.MaxItems {
		return fail(merr.WrapErrParameterInvalidMsg("MultiSearch requires 1..%d items", e.limits.MaxItems))
	}
	if proto.Size(req) > e.limits.MaxRequestBytes {
		return fail(merr.WrapErrParameterTooLarge("MultiSearch request exceeds byte limit"))
	}
	db := GetCurDBNameFromRequestOrContext(ctx, req)
	ids := make(map[string]struct{}, len(req.Items))
	for _, item := range req.Items {
		if item == nil || item.Search == nil || item.Id == "" || len(item.Id) > 128 {
			return fail(merr.WrapErrParameterInvalidMsg("each MultiSearch item needs a search and a 1..128 byte id"))
		}
		if _, exists := ids[item.Id]; exists {
			return fail(merr.WrapErrParameterInvalidMsg("duplicate MultiSearch item id %q", item.Id))
		}
		ids[item.Id] = struct{}{}
		if item.Search.DbName != "" && item.Search.DbName != db {
			return fail(merr.WrapErrParameterInvalidMsg("MultiSearch items must use envelope database %q", db))
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	// Admission is fail-fast: waiting envelopes cannot grow without bound.
	select {
	case e.batches <- struct{}{}:
		defer func() { <-e.batches }()
	default:
		return fail(merr.ErrServiceTooManyRequests)
	}
	ctx, cancel := context.WithTimeout(ctx, e.limits.Timeout)
	defer cancel()
	out := &multisearchpb.MultiSearchResponse{Status: merr.Success(), Items: make([]*multisearchpb.SearchOutcome, len(req.Items))}
	var next atomic.Int64
	var resultBytes atomic.Int64
	var overflow atomic.Bool
	var wg sync.WaitGroup
	for range min(e.limits.Parallelism, len(req.Items)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= len(req.Items) {
					return
				}
				select {
				case e.active <- struct{}{}:
				case <-ctx.Done():
					return
				}
				if ctx.Err() != nil {
					<-e.active
					return
				}
				item := req.Items[i]
				// Search and entry interceptors may mutate the request. Never share
				// caller-owned request pointers between workers (or retry attempts).
				search := proto.Clone(item.Search).(*milvuspb.SearchRequest)
				search.DbName = db
				result, err := e.search(ctx, search)
				<-e.active
				outcome := &multisearchpb.SearchOutcome{Id: item.Id, Result: result}
				if err != nil {
					s := status.Convert(err)
					outcome.Result = nil
					outcome.GrpcCode, outcome.GrpcMessage = int32(s.Code()), s.Message()
				} else if result == nil || result.Status == nil {
					outcome.Result = &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrServiceInternalMsg("Search returned no result status"))}
				}
				// Reserve protobuf framing overhead as well as outcome bytes.
				if resultBytes.Add(int64(proto.Size(outcome)+16)) > int64(e.limits.MaxResponseBytes) {
					overflow.Store(true)
					cancel()
					return
				}
				out.Items[i] = outcome
			}
		}()
	}
	wg.Wait()
	if overflow.Load() || proto.Size(out) > e.limits.MaxResponseBytes {
		return fail(merr.WrapErrParameterTooLarge("MultiSearch response exceeds byte limit; reduce item count, topK, or output fields"))
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return out, nil
}
