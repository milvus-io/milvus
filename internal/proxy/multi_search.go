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
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const multiSearchConcurrency = 4

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

// CheckMultiSearchAdmission is a pre-decode probe. RunMultiSearch makes the
// authoritative admission decision when it enqueues the batch.
func (node *Proxy) CheckMultiSearchAdmission() error {
	if node.sched != nil {
		return node.sched.MSearchQueue.CheckAdmission()
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
	s := node.sched
	if s == nil || execute == nil {
		return nil, merr.WrapErrServiceInternalMsg("MultiSearch scheduler or Search entry is unavailable")
	}
	if err := contextutil.ErrOrDeadlineExceeded(ctx); err != nil {
		return nil, err
	}

	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.Context(), cancel)
	defer stop()

	executionCtx := batchCtx
	if !options.ExecutionDeadline.IsZero() {
		var cancelExecution context.CancelFunc
		executionCtx, cancelExecution = context.WithDeadline(batchCtx, options.ExecutionDeadline)
		defer cancelExecution()
	}
	batch := &MultiSearchTask{
		BaseTask:       taskmodel.BaseTask{MetaCache: node.GetMetaCache()},
		TaskCondition:  taskmodel.NewTaskCondition(executionCtx),
		nodeID:         nodeID,
		startedAt:      startedAt,
		items:          items,
		execute:        execute,
		maxConcurrency: options.MaxConcurrency,
		results:        make([]*milvuspb.SearchResults, len(items)),
		progress:       make([]multiSearchItemProgress, len(items)),
	}
	err = s.MSearchQueue.Enqueue(batch)
	if err != nil && !errors.Is(err, executionCtx.Err()) {
		return nil, err
	}
	if err == nil {
		err = batch.WaitToFinish()
	}
	// PostExecute runs only on success. Queue expiry, an interrupted lifecycle,
	// and parent cancellation must finalize the same ordered result slots.
	unfinished := contextutil.ErrOrDeadlineExceeded(executionCtx)
	if unfinished == nil {
		unfinished = err
	}
	batch.finish(ctx, unfinished)
	batchResults := batch.response
	batch.response = nil // Do not retain delivered results while canceled work unwinds.

	if err := contextutil.ErrOrDeadlineExceeded(ctx); err != nil {
		return nil, err
	}
	if s.Context().Err() != nil {
		return nil, merr.WrapErrServiceUnavailableMsg("MultiSearch scheduler is stopping")
	}
	return batchResults, nil
}

// MultiSearchTask orchestrates a batch on TaskScheduler's independent MSearch queue.
// Item preparation stays inside the bounded fan-out because REST preparation can
// perform schema lookups and must share the execution budget and item limit.
type MultiSearchTask struct {
	taskmodel.BaseTask
	*taskmodel.TaskCondition
	id             taskmodel.UniqueID
	ts             taskmodel.Timestamp
	items          []MultiSearchPrepare
	execute        MultiSearchExecute
	maxConcurrency uint32
	concurrency    int
	finishOnce     sync.Once
	response       []*milvuspb.SearchResults
	nodeID         string
	startedAt      time.Time

	mu       sync.Mutex
	results  []*milvuspb.SearchResults
	progress []multiSearchItemProgress
}

var _ taskmodel.Task = (*MultiSearchTask)(nil)

func (b *MultiSearchTask) TraceCtx() context.Context   { return b.Ctx() }
func (b *MultiSearchTask) ID() taskmodel.UniqueID      { return b.id }
func (b *MultiSearchTask) SetID(id taskmodel.UniqueID) { b.id = id }
func (b *MultiSearchTask) Name() string                { return "MultiSearchTask" }

// The batch is a local orchestration task, without a corresponding wire MsgType.
func (b *MultiSearchTask) Type() commonpb.MsgType       { return commonpb.MsgType_Undefined }
func (b *MultiSearchTask) BeginTs() taskmodel.Timestamp { return b.ts }
func (b *MultiSearchTask) EndTs() taskmodel.Timestamp   { return b.ts }
func (b *MultiSearchTask) SetTs(ts taskmodel.Timestamp) { b.ts = ts }
func (b *MultiSearchTask) CanSkipAllocTimestamp() bool  { return true }
func (b *MultiSearchTask) OnEnqueue() error             { return nil }

func (b *MultiSearchTask) PreExecute(ctx context.Context) error {
	if err := contextutil.ErrOrDeadlineExceeded(ctx); err != nil {
		return err
	}
	b.concurrency = min(multiSearchConcurrency, len(b.items))
	if b.maxConcurrency != 0 {
		b.concurrency = int(min(uint64(b.maxConcurrency), uint64(len(b.items))))
	}
	return nil
}

func (b *MultiSearchTask) Execute(ctx context.Context) error {
	var group errgroup.Group
	group.SetLimit(b.concurrency)
	for i, prepare := range b.items {
		if contextutil.ErrOrDeadlineExceeded(ctx) != nil {
			break
		}
		group.Go(func() error {
			if contextutil.ErrOrDeadlineExceeded(ctx) == nil {
				itemCtx := metrics.WithIndependentRPCStats(ctx)
				b.complete(ctx, i, b.runItem(itemCtx, i, prepare, b.execute))
			}
			return nil
		})
	}
	// Keep the scheduler worker occupied until canceled searches unwind so
	// slow cancellation cannot create unbounded detached work.
	_ = group.Wait()
	return contextutil.ErrOrDeadlineExceeded(ctx)
}

func (b *MultiSearchTask) PostExecute(ctx context.Context) error {
	b.finish(b.TraceCtx(), contextutil.ErrOrDeadlineExceeded(ctx))
	return nil
}

func (b *MultiSearchTask) complete(ctx context.Context, i int, result *milvuspb.SearchResults) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if result == nil || b.results == nil || contextutil.ErrOrDeadlineExceeded(ctx) != nil {
		return
	}
	b.results[i] = result
	b.progress[i].completedAt = time.Now()
}

func (b *MultiSearchTask) finish(ctx context.Context, unfinished error) {
	b.finishOnce.Do(func() {
		b.mu.Lock()
		// Transfer ownership to the response. Late searches can no longer write
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
		b.response = results
	})
}

type multiSearchDQLAdmissionKey struct{}

type multiSearchDQLAdmission struct{ rejected bool }

func (b *MultiSearchTask) runItem(ctx context.Context, index int, prepare MultiSearchPrepare, execute MultiSearchExecute) *milvuspb.SearchResults {
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
