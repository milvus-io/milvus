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
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	multiSearchQueueCapacity = 64
	multiSearchWorkers       = 8
	multiSearchConcurrency   = 4
)

// MultiSearchPrepare builds one recognized Search request after batch admission.
type MultiSearchPrepare func(context.Context) (*milvuspb.SearchRequest, error)

// MultiSearchExecute runs a prepared request through ordinary Search entry checks and execution.
type MultiSearchExecute func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)

// multiSearchScheduler owns the queue and workers that orchestrate batches.
// The searches themselves run through the existing DQL scheduler.
type multiSearchScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan func()

	mu      sync.Mutex
	stopped bool
	workers sync.WaitGroup
}

func newMultiSearchScheduler(ctx context.Context, capacity int) *multiSearchScheduler {
	ctx, cancel := context.WithCancel(ctx)
	return &multiSearchScheduler{
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan func(), capacity),
	}
}

func (s *multiSearchScheduler) start(workers int) {
	for range workers {
		s.workers.Go(func() {
			for run := range s.queue {
				if s.ctx.Err() == nil {
					run()
				}
			}
		})
	}
}

func (s *multiSearchScheduler) stop() {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		s.cancel()
		close(s.queue)
	}
	s.mu.Unlock()
	s.workers.Wait()
}

func (s *multiSearchScheduler) fullError() error {
	return merr.WrapErrTooManyRequests(int32(cap(s.queue)), "MultiSearch queue is full")
}

func (s *multiSearchScheduler) enqueue(run func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return merr.WrapErrServiceUnavailableMsg("MultiSearch scheduler is stopping")
	}
	select {
	case s.queue <- run:
		return nil
	default:
		return s.fullError()
	}
}

// CheckMultiSearchAdmission is a pre-decode probe. RunMultiSearch makes the
// authoritative admission decision when it enqueues the batch.
func (node *Proxy) CheckMultiSearchAdmission() error {
	if s := node.multiSearch; s != nil && len(s.queue) == cap(s.queue) {
		return s.fullError()
	}
	return nil
}

// RunMultiSearch accepts a structurally validated list of Search operations.
// A failure of an individual search is returned in its native SearchResults.
func (node *Proxy) RunMultiSearch(ctx context.Context, maxConcurrency uint32, items []MultiSearchPrepare, execute MultiSearchExecute) ([]*milvuspb.SearchResults, error) {
	if len(items) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("MultiSearch requires at least one search")
	}
	for i, prepare := range items {
		if prepare == nil {
			return nil, merr.WrapErrParameterInvalidMsg("requests[%d] requires a search", i)
		}
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return nil, err
	}
	s := node.multiSearch
	if s == nil || execute == nil {
		return nil, merr.WrapErrServiceInternalMsg("MultiSearch scheduler or Search entry is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()

	done := make(chan []*milvuspb.SearchResults, 1)
	if err := s.enqueue(func() {
		if batchCtx.Err() == nil {
			done <- runMultiSearchBatch(batchCtx, maxConcurrency, items, execute)
		}
	}); err != nil {
		return nil, err
	}
	select {
	case results := <-done:
		if batchCtx.Err() == nil {
			return results, nil
		}
	case <-batchCtx.Done():
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, merr.WrapErrServiceUnavailableMsg("MultiSearch scheduler is stopping")
}

func runMultiSearchBatch(ctx context.Context, maxConcurrency uint32, items []MultiSearchPrepare, execute MultiSearchExecute) []*milvuspb.SearchResults {
	concurrency := multiSearchConcurrency
	if maxConcurrency != 0 {
		concurrency = int(min(uint64(maxConcurrency), uint64(len(items))))
	}
	var group errgroup.Group
	group.SetLimit(concurrency)
	results := make([]*milvuspb.SearchResults, len(items))
	for i, prepare := range items {
		if ctx.Err() != nil {
			break
		}
		group.Go(func() error {
			if ctx.Err() == nil {
				itemCtx := metrics.WithIndependentRPCStats(ctx)
				results[i] = runMultiSearchItem(itemCtx, prepare, execute)
			}
			return nil
		})
	}
	_ = group.Wait()
	return results
}

func runMultiSearchItem(ctx context.Context, prepare MultiSearchPrepare, execute MultiSearchExecute) *milvuspb.SearchResults {
	request, err := prepare(ctx)
	if err != nil {
		return &milvuspb.SearchResults{Status: multiSearchErrorStatus(err)}
	}
	if request == nil {
		return &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrServiceInternalMsg("MultiSearch prepared no Search request"))}
	}
	if err := ctx.Err(); err != nil {
		return &milvuspb.SearchResults{Status: merr.Status(err)}
	}
	result, err := execute(ctx, request)
	if err != nil {
		return &milvuspb.SearchResults{Status: multiSearchErrorStatus(err)}
	}
	if result.GetStatus() == nil {
		return &milvuspb.SearchResults{Status: merr.Status(merr.WrapErrServiceInternalMsg("Search returned no status"))}
	}
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
