// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"container/list"
	"context"
	"math"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	multiSearchQueueCapacity = 64
	multiSearchWorkers       = 8
	multiSearchConcurrency   = 4
)

// MultiSearchOptions applies to the entire admitted batch. A zero deadline
// leaves execution bounded only by the parent call and scheduler lifetime.
type MultiSearchOptions struct {
	MaxConcurrency    uint32
	ExecutionDeadline time.Time
}

// MultiSearchExecutionDeadline validates the optional wire budget and anchors
// it at receipt. An explicitly supplied zero expires at receipt.
func MultiSearchExecutionDeadline(receivedAt time.Time, timeoutMs *int64) (time.Time, error) {
	if timeoutMs == nil {
		return time.Time{}, nil
	}
	const maxTimeoutMs = math.MaxInt64 / int64(time.Millisecond)
	if *timeoutMs < 0 || *timeoutMs > maxTimeoutMs {
		return time.Time{}, merr.WrapErrParameterInvalidMsg("MultiSearch execution timeout must be between 0 and %d milliseconds", maxTimeoutMs)
	}
	return receivedAt.Add(time.Duration(*timeoutMs) * time.Millisecond), nil
}

// MultiSearchPrepare builds one recognized Search request after batch admission.
type MultiSearchPrepare func(context.Context) (*milvuspb.SearchRequest, error)

// MultiSearchExecute runs a prepared request through ordinary Search entry checks and execution.
type MultiSearchExecute func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)

// multiSearchScheduler owns the queue and workers that orchestrate batches.
// The searches themselves run through the existing DQL scheduler.
type multiSearchScheduler struct {
	ctx      context.Context
	cancel   context.CancelFunc
	capacity int
	mu       sync.Mutex
	ready    *sync.Cond
	queue    list.List
	workers  sync.WaitGroup
}

func newMultiSearchScheduler(ctx context.Context, capacity int) *multiSearchScheduler {
	ctx, cancel := context.WithCancel(ctx)
	s := &multiSearchScheduler{
		ctx:      ctx,
		cancel:   cancel,
		capacity: capacity,
	}
	s.ready = sync.NewCond(&s.mu)
	return s
}

func (s *multiSearchScheduler) start(workers int) {
	nodeID := paramtable.GetStringNodeID()
	metrics.ProxyMultiSearchAttemptsTotal.WithLabelValues(nodeID)
	metrics.ProxyMultiSearchRetriesTotal.WithLabelValues(nodeID, multiSearchRetryRateLimit)
	metrics.ProxyMultiSearchRetriesTotal.WithLabelValues(nodeID, multiSearchRetryQueueFull)
	for range workers {
		s.workers.Go(func() {
			for run := s.dequeue(); run != nil; run = s.dequeue() {
				run()
			}
		})
	}
}

func (s *multiSearchScheduler) stop() {
	s.mu.Lock()
	s.cancel()
	s.ready.Broadcast()
	s.mu.Unlock()
	s.workers.Wait()
}

func (s *multiSearchScheduler) fullError() error {
	return merr.WrapErrTooManyRequests(int32(s.capacity), "MultiSearch queue is full")
}

func (s *multiSearchScheduler) isFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queue.Len() == s.capacity
}

func (s *multiSearchScheduler) enqueue(run func()) (*list.Element, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, merr.WrapErrServiceUnavailableMsg("MultiSearch scheduler is stopping")
	}
	if s.queue.Len() == s.capacity {
		return nil, s.fullError()
	}
	entry := s.queue.PushBack(run)
	s.ready.Signal()
	return entry, nil
}

func (s *multiSearchScheduler) dequeue() func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.queue.Len() == 0 && s.ctx.Err() == nil {
		s.ready.Wait()
	}
	if s.ctx.Err() != nil {
		return nil
	}
	return s.queue.Remove(s.queue.Front()).(func())
}

func (s *multiSearchScheduler) remove(entry *list.Element) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Removing an entry the worker already dequeued is a no-op.
	s.queue.Remove(entry)
}

// CheckMultiSearchAdmission is a pre-decode probe. RunMultiSearch makes the
// authoritative admission decision when it enqueues the batch.
func (node *Proxy) CheckMultiSearchAdmission() error {
	if s := node.multiSearch; s != nil && s.isFull() {
		return s.fullError()
	}
	return nil
}

// RunMultiSearch accepts a structurally validated list of Search operations.
// A failure of an individual search is returned in its native SearchResults.
func (node *Proxy) RunMultiSearch(ctx context.Context, options MultiSearchOptions, items []MultiSearchPrepare, execute MultiSearchExecute) (results []*milvuspb.SearchResults, err error) {
	startedAt := time.Now()
	nodeID := paramtable.GetStringNodeID()
	ctx, span := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-MultiSearch")
	defer span.End()
	defer func() { observeMultiSearchBatch(ctx, nodeID, startedAt, len(items), results, err) }()
	if len(items) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("MultiSearch requires at least one search")
	}
	for i, prepare := range items {
		if prepare == nil {
			return nil, merr.WrapErrServiceInternalMsg("MultiSearch requests[%d] has no preparer", i)
		}
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return nil, err
	}
	s := node.multiSearch
	if s == nil || execute == nil {
		return nil, merr.WrapErrServiceInternalMsg("MultiSearch scheduler or Search entry is unavailable")
	}
	if err := contextutil.ErrOrDeadlineExceeded(ctx); err != nil {
		return nil, err
	}

	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()

	executionCtx := batchCtx
	if !options.ExecutionDeadline.IsZero() {
		var cancelExecution context.CancelFunc
		executionCtx, cancelExecution = context.WithDeadline(batchCtx, options.ExecutionDeadline)
		defer cancelExecution()
	}
	batch := &multiSearchBatch{
		nodeID:    nodeID,
		startedAt: startedAt,
		results:   make([]*milvuspb.SearchResults, len(items)),
		progress:  make([]multiSearchItemProgress, len(items)),
		remaining: len(items),
		done:      make(chan struct{}),
	}
	entry, err := s.enqueue(func() {
		batch.run(executionCtx, options.MaxConcurrency, items, execute)
	})
	if err != nil {
		return nil, err
	}
	select {
	case <-batch.done:
	case <-executionCtx.Done():
	}
	s.remove(entry)
	batchResults := batch.finish(ctx, contextutil.ErrOrDeadlineExceeded(executionCtx))
	if err := contextutil.ErrOrDeadlineExceeded(ctx); err != nil {
		return nil, err
	}
	if s.ctx.Err() != nil {
		return nil, merr.WrapErrServiceUnavailableMsg("MultiSearch scheduler is stopping")
	}
	return batchResults, nil
}

type multiSearchBatch struct {
	nodeID    string
	startedAt time.Time

	mu        sync.Mutex
	results   []*milvuspb.SearchResults
	progress  []multiSearchItemProgress
	remaining int
	done      chan struct{}
}

func (b *multiSearchBatch) run(ctx context.Context, maxConcurrency uint32, items []MultiSearchPrepare, execute MultiSearchExecute) {
	concurrency := min(multiSearchConcurrency, len(items))
	if maxConcurrency != 0 {
		concurrency = int(min(uint64(maxConcurrency), uint64(len(items))))
	}
	var group errgroup.Group
	group.SetLimit(concurrency)
	for i, prepare := range items {
		if contextutil.ErrOrDeadlineExceeded(ctx) != nil {
			break
		}
		group.Go(func() error {
			if contextutil.ErrOrDeadlineExceeded(ctx) == nil {
				itemCtx := metrics.WithIndependentRPCStats(ctx)
				b.complete(ctx, i, b.runItem(itemCtx, i, prepare, execute))
			}
			return nil
		})
	}
	// Keep the scheduler worker occupied until canceled searches unwind so
	// slow cancellation cannot create unbounded detached work.
	_ = group.Wait()
}

func (b *multiSearchBatch) complete(ctx context.Context, i int, result *milvuspb.SearchResults) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if result == nil || b.results == nil || contextutil.ErrOrDeadlineExceeded(ctx) != nil {
		return
	}
	b.results[i] = result
	b.progress[i].completedAt = time.Now()
	b.remaining--
	if b.remaining == 0 {
		close(b.done)
	}
}

func (b *multiSearchBatch) finish(ctx context.Context, unfinished error) []*milvuspb.SearchResults {
	b.mu.Lock()
	// Transfer ownership to the caller. Late searches can no longer write
	// slots or retain completed Search data while they unwind.
	results, progress := b.results, b.progress
	b.results, b.progress = nil, nil
	finishedAt := time.Now()
	b.mu.Unlock()

	for i, result := range results {
		if result == nil {
			result = &milvuspb.SearchResults{Status: merr.Status(unfinished)}
			results[i] = result
			progress[i].completedAt = finishedAt
		}
		progress[i].finishBackoff(progress[i].completedAt)
		progress[i].observe(ctx, b.nodeID, i, b.startedAt, result.GetStatus())
	}
	return results
}

type multiSearchDQLAdmissionKey struct{}

type multiSearchDQLAdmission struct{ rejected bool }

func (b *multiSearchBatch) runItem(ctx context.Context, index int, prepare MultiSearchPrepare, execute MultiSearchExecute) *milvuspb.SearchResults {
	request, err := prepare(ctx)
	if err != nil {
		return &milvuspb.SearchResults{Status: multiSearchErrorStatus(err)}
	}
	if request == nil {
		return &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrServiceInternalMsg("MultiSearch prepared no Search request"))}
	}
	var result *milvuspb.SearchResults
	var reason string
	admission := multiSearchDQLAdmission{}
	ctx = context.WithValue(ctx, multiSearchDQLAdmissionKey{}, &admission)
	// Keep the native response. retry.Handle may return an earlier error
	// when a later attempt stops on a context error.
	_ = retry.Handle(ctx, func() (bool, error) {
		if !b.startAttempt(ctx, index, reason) {
			return false, nil
		}
		admission.rejected = false
		var err error
		result, err = execute(ctx, copySearchRequestForAttempt(request))
		if err != nil {
			result = &milvuspb.SearchResults{Status: multiSearchErrorStatus(err)}
		}
		st := result.GetStatus()
		if st == nil {
			result = &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrServiceInternalMsg("Search returned no status"))}
			return false, nil
		}
		if st.GetExtraInfo()[merr.InputErrorFlagKey] == "true" {
			return false, nil
		}
		switch merr.Code(merr.Error(st)) {
		case merr.Code(merr.ErrServiceRateLimit):
			reason = multiSearchRetryRateLimit
			b.recordRejection(index, reason)
			return true, merr.ErrServiceRateLimit
		case merr.Code(merr.ErrServiceTooManyRequests):
			if admission.rejected {
				reason = multiSearchRetryQueueFull
				b.recordRejection(index, reason)
				return true, merr.ErrServiceTooManyRequests
			}
		}
		return false, nil
	}, retry.OnBackoff(func() { b.startBackoff(ctx, index) }))
	if !merr.Ok(result.GetStatus()) && result.GetResults() != nil {
		return &milvuspb.SearchResults{
			Status:         result.GetStatus(),
			CollectionName: result.GetCollectionName(),
			SessionTs:      result.GetSessionTs(),
		}
	}
	return result
}

func multiSearchErrorStatus(err error) *commonpb.Status {
	grpcStatus := status.Convert(err)
	switch grpcStatus.Code() {
	case codes.PermissionDenied:
		err = merr.WrapErrPrivilegeNotPermitted("%s", grpcStatus.Message())
	case codes.Unauthenticated:
		err = merr.Wrap(merr.ErrNeedAuthenticate, grpcStatus.Message())
	case codes.InvalidArgument:
		err = merr.WrapErrParameterInvalidMsg("%s", grpcStatus.Message())
	case codes.Canceled:
		err = context.Canceled
	case codes.DeadlineExceeded:
		err = context.DeadlineExceeded
	}
	return merr.Status(err)
}
