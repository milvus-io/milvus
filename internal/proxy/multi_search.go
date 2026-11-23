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
	"runtime/debug"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/reqregistry"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const defaultMultiSearchConcurrency = 4

var multiSearchSuccessStatusBytes = uint64(proto.Size(&milvuspb.MultiSearchResponse{Status: merr.Success()}))

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

// CheckMultiSearchBatchSize bounds item preparation on both transports.
func CheckMultiSearchBatchSize(count int) error {
	limit := paramtable.Get().ProxyCfg.MultiSearchMaxBatchSize.GetAsInt64()
	if int64(count) > limit {
		return merr.Wrapf(merr.ErrParameterTooLarge, "MultiSearch item count %d exceeds proxy.multiSearch.maxBatchSize %d", count, limit)
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
	if err := CheckMultiSearchBatchSize(len(items)); err != nil {
		return nil, err
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
	maxResultBytes := uint64(paramtable.Get().ProxyCfg.MultiSearchMaxResultBytes.GetAsInt64())
	if multiSearchSuccessStatusBytes > maxResultBytes {
		return nil, multiSearchResultLimitError(maxResultBytes, multiSearchSuccessStatusBytes, false)
	}
	// Register once for both transports, before queueing or preparing any item.
	// A batch has no single database, collection, NQ, topk or expression.
	ctx, entry := node.registerRequest(ctx, reqregistry.Info{Type: reqregistry.TypeMultiSearch})
	defer node.unregisterRequest(entry)
	defer func() {
		// Linearize the response against CancelRequests before releasing the
		// registration. An accepted operator cancel wins even if the last item
		// completed while the response was being finalized.
		entry.Finish()
		if cause := reqregistry.CancelCause(ctx); cause != nil {
			results, err = nil, cause
		}
	}()
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
		cancel:         cancel,
		maxResultBytes: maxResultBytes,
		resultBytes:    multiSearchSuccessStatusBytes,
		results:        make([]*milvuspb.SearchResults, len(items)),
	}
	err = s.MSearchQueue.Enqueue(batch)
	if err != nil && !errors.Is(err, executionCtx.Err()) {
		return nil, err
	}
	if err == nil {
		err = batch.WaitToFinish()
	}
	// Freeze the ordered results on success, queue expiry, or cancellation.
	unfinished := contextutil.ErrOrDeadlineExceeded(executionCtx)
	if cause := reqregistry.CancelCause(executionCtx); cause != nil {
		unfinished = cause
	}
	if unfinished == nil {
		unfinished = err
	}
	batchResults, limitErr := batch.finish(ctx, unfinished)
	if limitErr != nil {
		return nil, limitErr
	}

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
	nodeID         string
	startedAt      time.Time
	cancel         context.CancelFunc
	maxResultBytes uint64

	mu             sync.Mutex
	results        []*milvuspb.SearchResults
	resultBytes    uint64
	resultLimitErr error
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
	requested := b.maxConcurrency
	if requested == 0 {
		requested = defaultMultiSearchConcurrency
	}
	limit := max(1, paramtable.Get().ProxyCfg.MultiSearchMaxConcurrency.GetAsUint64())
	b.concurrency = int(min(uint64(requested), limit, uint64(len(b.items))))
	return nil
}

func (b *MultiSearchTask) Execute(ctx context.Context) error {
	var group sync.WaitGroup
	slots := semaphore.NewWeighted(int64(b.concurrency))
	for i, prepare := range b.items {
		if contextutil.ErrOrDeadlineExceeded(ctx) != nil || slots.Acquire(ctx, 1) != nil {
			break
		}
		group.Go(func() {
			defer slots.Release(1)
			if contextutil.ErrOrDeadlineExceeded(ctx) != nil {
				return
			}
			itemCtx := metrics.WithIndependentRPCStats(ctx)
			var result *milvuspb.SearchResults
			defer func() {
				if recover() != nil {
					err := merr.WrapErrServiceInternalMsg("MultiSearch requests[%d] panicked", i)
					mlog.Error(itemCtx, "MultiSearch item panicked", mlog.Int("index", i),
						mlog.FieldTaskID(b.ID()), mlog.Err(err), mlog.ByteString("panic_stack", debug.Stack()))
					result = &milvuspb.SearchResults{Status: merr.Status(err)}
				}
				b.complete(itemCtx, i, result)
			}()
			result = b.runItem(itemCtx, i, prepare, b.execute)
		})
	}
	// Keep the scheduler worker occupied until canceled searches unwind so
	// slow cancellation cannot create unbounded detached work.
	group.Wait()
	return contextutil.ErrOrDeadlineExceeded(ctx)
}

func (b *MultiSearchTask) PostExecute(context.Context) error { return nil }

func multiSearchResultSize(result *milvuspb.SearchResults) uint64 {
	const resultsField = 2 // milvuspb.MultiSearchResponse.results
	size := uint64(proto.Size(result))
	return uint64(protowire.SizeTag(resultsField)+protowire.SizeVarint(size)) + size
}

func multiSearchResultLimitError(limit, observed uint64, singleItem bool) error {
	message := "MultiSearch response has at least %d bytes, exceeding proxy.multiSearch.maxResultBytes (%d bytes)"
	if singleItem {
		message += ". One Search result exceeds the batch limit. Reduce nq, topK, or outputFields"
	}
	return merr.Wrapf(merr.ErrParameterTooLarge, message, observed, limit)
}

func (b *MultiSearchTask) complete(ctx context.Context, i int, result *milvuspb.SearchResults) {
	if result == nil || contextutil.ErrOrDeadlineExceeded(ctx) != nil {
		return
	}
	size := multiSearchResultSize(result)
	b.mu.Lock()
	if b.results == nil || b.results[i] != nil || contextutil.ErrOrDeadlineExceeded(ctx) != nil {
		b.mu.Unlock()
		return
	}
	b.resultBytes += size
	if b.resultBytes > b.maxResultBytes {
		// Publish the limit error before canceling execution and waking the
		// caller. Late completions cannot replace the error or retain results.
		b.resultLimitErr = multiSearchResultLimitError(b.maxResultBytes, b.resultBytes, multiSearchSuccessStatusBytes+size > b.maxResultBytes)
		b.results = nil
		b.cancel()
		b.mu.Unlock()
		return
	}
	b.results[i] = result
	label, code := multiSearchResultLabel(result.GetStatus())
	duration := time.Since(b.startedAt)
	// Serialize the event with finish so the caller cannot end the batch span
	// before an accepted completion has been recorded.
	traceMultiSearchCompletion(b.TraceCtx(), i, label, code, duration)
	metrics.ProxyMultiSearchSubrequestsTotal.WithLabelValues(b.nodeID, label).Inc()
	b.mu.Unlock()
	logMultiSearchCompletion(ctx, i, label, code, duration)
}

// finish is called once by RunMultiSearch, which owns final response accounting.
func (b *MultiSearchTask) finish(ctx context.Context, unfinished error) ([]*milvuspb.SearchResults, error) {
	b.mu.Lock()
	// Transfer ownership to the caller. Late searches can no longer write
	// slots or retain completed Search data while they unwind.
	results := b.results
	b.results = nil
	resultBytes, limitErr := b.resultBytes, b.resultLimitErr
	duration := time.Since(b.startedAt)
	b.mu.Unlock()
	if limitErr != nil {
		return nil, limitErr
	}

	// Check the full envelope before publishing or recording synthesized slots.
	var unfinishedResult *milvuspb.SearchResults
	var unfinishedBytes uint64
	for _, result := range results {
		if result != nil {
			continue
		}
		if unfinishedResult == nil {
			unfinishedResult = &milvuspb.SearchResults{Status: merr.Status(unfinished)}
			unfinishedBytes = multiSearchResultSize(unfinishedResult)
		}
		resultBytes += unfinishedBytes
		if resultBytes > b.maxResultBytes {
			return nil, multiSearchResultLimitError(b.maxResultBytes, resultBytes, false)
		}
	}
	for i, result := range results {
		if result != nil {
			continue
		}
		results[i] = proto.CloneOf(unfinishedResult)
		label, code := multiSearchResultLabel(unfinishedResult.GetStatus())
		traceMultiSearchCompletion(ctx, i, label, code, duration)
		logMultiSearchCompletion(ctx, i, label, code, duration)
		metrics.ProxyMultiSearchSubrequestsTotal.WithLabelValues(b.nodeID, label).Inc()
	}
	return results, nil
}

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
	// Keep the native response. retry.Handle may return an earlier error
	// when a later attempt stops on a context error.
	_ = retry.Handle(ctx, func() (bool, error) {
		if !b.startAttempt(ctx, index, reason) {
			return false, nil
		}
		attemptCtx, rejected := scheduler.WithDQLAdmissionTracking(ctx)
		var err error
		result, err = execute(attemptCtx, copySearchRequestForAttempt(request))
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
			logMultiSearchRejection(ctx, index, reason)
			return true, merr.ErrServiceRateLimit
		case merr.Code(merr.ErrServiceTooManyRequests):
			if rejected.Load() {
				reason = multiSearchRetryQueueFull
				logMultiSearchRejection(ctx, index, reason)
				return true, merr.ErrServiceTooManyRequests
			}
		}
		return false, nil
	})
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
