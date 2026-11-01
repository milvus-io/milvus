package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// multiSearchLimits are fixed when the Proxy's Search entry is installed.
type multiSearchLimits struct {
	MaxItems, Parallelism, GlobalParallelism, MaxBatches int
	MaxRequestBytes, MaxResponseBytes                    int
	Timeout                                              time.Duration
}

type multiSearchEntry func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)

// SetMultiSearchEntry installs the ordinary Search entry checks once, before
// serving requests. The transport supplies the entry chain; its terminal handler
// calls this Proxy's Search method directly. Never fall back to unchecked Search.
func (node *Proxy) SetMultiSearchEntry(entry func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)) {
	p := &paramtable.Get().ProxyCfg
	node.initMultiSearch(multiSearchLimits{
		MaxItems:          max(1, p.MultiSearchMaxItems.GetAsInt()),
		Parallelism:       max(1, p.MultiSearchParallelism.GetAsInt()),
		GlobalParallelism: max(1, p.MultiSearchGlobalParallelism.GetAsInt()),
		MaxBatches:        max(1, p.MultiSearchMaxBatches.GetAsInt()),
		MaxRequestBytes:   max(1024, p.MultiSearchMaxRequestBytes.GetAsInt()),
		MaxResponseBytes:  max(1024, p.MultiSearchMaxResponseBytes.GetAsInt()),
		Timeout:           time.Duration(max(1, p.MultiSearchTimeoutSeconds.GetAsInt())) * time.Second,
	}, entry)
}

func (node *Proxy) initMultiSearch(limits multiSearchLimits, entry multiSearchEntry) {
	if entry == nil {
		return
	}
	node.multiSearchInit.Do(func() {
		node.multiSearchLimits = limits
		node.multiSearchBatches = make(chan struct{}, limits.MaxBatches)
		node.multiSearchActive = make(chan struct{}, limits.GlobalParallelism)
		// Publish the callback last: a handler that observes it also observes
		// the initialized limits and permits. Reconfiguration cannot reset
		// admission limits while requests are in flight.
		node.multiSearchEntry.Store(&entry)
	})
}

// MultiSearch coordinates a bounded set of ordinary Search calls. Only their
// existing SearchTasks enter node.sched.DqQueue; the parent waits outside it.
func (node *Proxy) MultiSearch(ctx context.Context, req *milvuspb.MultiSearchRequest) (*milvuspb.MultiSearchResponse, error) {
	fail := func(err error) (*milvuspb.MultiSearchResponse, error) {
		return &milvuspb.MultiSearchResponse{Status: merr.Status(err)}, nil
	}
	entry := node.multiSearchEntry.Load()
	if entry == nil {
		return fail(merr.Wrap(merr.ErrServiceNotReady, "MultiSearch Search entry is not initialized"))
	}
	if req == nil || len(req.GetItems()) == 0 || len(req.GetItems()) > node.multiSearchLimits.MaxItems {
		return fail(merr.WrapErrParameterInvalidMsg("MultiSearch requires 1..%d items", node.multiSearchLimits.MaxItems))
	}
	if proto.Size(req) > node.multiSearchLimits.MaxRequestBytes {
		return fail(merr.WrapErrParameterTooLarge("MultiSearch request exceeds byte limit"))
	}
	ids := make(map[string]struct{}, len(req.Items))
	for _, item := range req.Items {
		if item == nil || item.Search == nil || item.Id == "" || len(item.Id) > 128 {
			return fail(merr.WrapErrParameterInvalidMsg("each MultiSearch item needs a search and a 1..128 byte id"))
		}
		if _, exists := ids[item.Id]; exists {
			return fail(merr.WrapErrParameterInvalidMsg("duplicate MultiSearch item id %q", item.Id))
		}
		ids[item.Id] = struct{}{}

	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := node.ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	// Admission is fail-fast: waiting envelopes cannot grow without bound.
	select {
	case node.multiSearchBatches <- struct{}{}:
		defer func() { <-node.multiSearchBatches }()
	default:
		return fail(merr.ErrServiceTooManyRequests)
	}
	ctx, cancel := context.WithTimeout(ctx, node.multiSearchLimits.Timeout)
	defer cancel()
	stop := context.AfterFunc(node.ctx, cancel)
	defer stop()
	out := &milvuspb.MultiSearchResponse{Status: merr.Success(), Items: make([]*milvuspb.SearchOutcome, len(req.Items))}
	var next atomic.Int64
	var resultBytes atomic.Int64
	var overflow atomic.Bool
	var wg sync.WaitGroup
	parallelism := node.multiSearchLimits.Parallelism
	if requested := req.GetMaxConcurrentSearches(); requested > 0 {
		parallelism = min(parallelism, int(requested))
	}
	// The RPC handler and these bounded workers stay outside the DQL pool.
	for range min(parallelism, len(req.Items)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= len(req.Items) {
					return
				}
				select {
				case node.multiSearchActive <- struct{}{}:
				case <-ctx.Done():
					return
				}
				if ctx.Err() != nil {
					<-node.multiSearchActive
					return
				}
				item := req.Items[i]
				// Search and entry interceptors may mutate the request. Never share
				// caller-owned request pointers between workers (or retry attempts).
				search := proto.Clone(item.Search).(*milvuspb.SearchRequest)
				result, err := (*entry)(ctx, search)
				<-node.multiSearchActive
				outcome := &milvuspb.SearchOutcome{Id: item.Id, Result: result}
				if err != nil {
					s := status.Convert(err)
					outcome.Result = nil
					outcome.GrpcCode, outcome.GrpcMessage = int32(s.Code()), s.Message()
				} else if result == nil || result.Status == nil {
					outcome.Result = &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrServiceInternalMsg("Search returned no result status"))}
				}
				// Reserve protobuf framing overhead as well as outcome bytes.
				if resultBytes.Add(int64(proto.Size(outcome)+16)) > int64(node.multiSearchLimits.MaxResponseBytes) {
					overflow.Store(true)
					cancel()
					return
				}
				out.Items[i] = outcome
			}
		}()
	}
	wg.Wait()
	if overflow.Load() || proto.Size(out) > node.multiSearchLimits.MaxResponseBytes {
		return fail(merr.WrapErrParameterTooLarge("MultiSearch response exceeds byte limit; reduce item count, topK, or output fields"))
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return out, nil
}
