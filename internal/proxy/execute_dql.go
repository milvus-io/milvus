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

const defaultExecuteDQLConcurrency = 4

var executeDQLSuccessStatusBytes = uint64(proto.Size(&milvuspb.ExecuteDQLResponse{Status: merr.Success()}))

// ExecuteDQLOptions applies to the entire admitted batch. A zero deadline
// leaves execution bounded only by the parent call and scheduler lifetime.
type ExecuteDQLOptions struct {
	MaxConcurrency    uint32
	ExecutionDeadline time.Time
}

// ExecuteDQLExecutionDeadline validates the optional wire budget and anchors
// it at receipt. An explicitly supplied zero expires at receipt.
func ExecuteDQLExecutionDeadline(receivedAt time.Time, timeoutMs *int64) (time.Time, error) {
	if timeoutMs == nil {
		return time.Time{}, nil
	}
	const maxTimeoutMs = math.MaxInt64 / int64(time.Millisecond)
	if *timeoutMs < 0 || *timeoutMs > maxTimeoutMs {
		return time.Time{}, merr.WrapErrParameterInvalidMsg("ExecuteDQL execution timeout must be between 0 and %d milliseconds", maxTimeoutMs)
	}
	return receivedAt.Add(time.Duration(*timeoutMs) * time.Millisecond), nil
}

// ExecuteDQLPrepare builds one recognized Search request after batch admission.
type ExecuteDQLPrepare func(context.Context) (*milvuspb.SearchRequest, error)

// ExecuteDQLExecute runs a prepared request through ordinary Search entry checks and execution.
type ExecuteDQLExecute func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)

// CheckExecuteDQLAdmission is a pre-decode probe. RunExecuteDQL makes the
// authoritative admission decision when it enqueues the batch.
func (node *Proxy) CheckExecuteDQLAdmission() error {
	if node.sched != nil {
		return node.sched.ExecuteDQLQueue.CheckAdmission()
	}
	return nil
}

// CheckExecuteDQLBatchSize bounds item preparation on both transports.
func CheckExecuteDQLBatchSize(count int) error {
	limit := paramtable.Get().ProxyCfg.ExecuteDQLMaxBatchSize.GetAsInt64()
	if int64(count) > limit {
		return merr.Wrapf(merr.ErrParameterTooLarge, "ExecuteDQL item count %d exceeds proxy.executeDQL.maxBatchSize %d", count, limit)
	}
	return nil
}

// RunExecuteDQL accepts a structurally validated list of Search operations.
// Each DQLResult carries the outcome of one operation in input order.
func (node *Proxy) RunExecuteDQL(ctx context.Context, options ExecuteDQLOptions, items []ExecuteDQLPrepare, execute ExecuteDQLExecute) (results []*milvuspb.DQLResult, err error) {
	startedAt := time.Now()
	nodeID := paramtable.GetStringNodeID()
	ctx, span := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ExecuteDQL")
	defer span.End()
	defer func() { observeExecuteDQLBatch(ctx, nodeID, startedAt, len(items), results, err) }()
	if len(items) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("ExecuteDQL requires at least one operation")
	}
	if err := CheckExecuteDQLBatchSize(len(items)); err != nil {
		return nil, err
	}
	for i, prepare := range items {
		if prepare == nil {
			return nil, merr.WrapErrServiceInternalMsg("ExecuteDQL requests[%d] has no preparer", i)
		}
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return nil, err
	}
	s := node.sched
	if s == nil || execute == nil {
		return nil, merr.WrapErrServiceInternalMsg("ExecuteDQL scheduler or Search entry is unavailable")
	}
	maxResultBytes := uint64(paramtable.Get().ProxyCfg.ExecuteDQLMaxResultBytes.GetAsInt64())
	if executeDQLSuccessStatusBytes > maxResultBytes {
		return nil, executeDQLResultLimitError(maxResultBytes, executeDQLSuccessStatusBytes, false)
	}
	// Register once for both transports, before queueing or preparing any item.
	// A batch has no single database, collection, NQ, topk or expression.
	ctx, entry := node.registerRequest(ctx, reqregistry.Info{Type: reqregistry.TypeExecuteDQL})
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
	batch := &ExecuteDQLTask{
		BaseTask:       taskmodel.BaseTask{MetaCache: node.GetMetaCache()},
		TaskCondition:  taskmodel.NewTaskCondition(executionCtx),
		nodeID:         nodeID,
		startedAt:      startedAt,
		items:          items,
		execute:        execute,
		maxConcurrency: options.MaxConcurrency,
		cancel:         cancel,
		maxResultBytes: maxResultBytes,
		resultBytes:    executeDQLSuccessStatusBytes,
		results:        make([]*milvuspb.DQLResult, len(items)),
	}
	err = s.ExecuteDQLQueue.Enqueue(batch)
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
		return nil, merr.WrapErrServiceUnavailableMsg("ExecuteDQL scheduler is stopping")
	}
	return batchResults, nil
}

// ExecuteDQLTask orchestrates a batch on TaskScheduler's independent ExecuteDQL queue.
// Item preparation stays inside the bounded fan-out because REST preparation can
// perform schema lookups and must share the execution budget and item limit.
type ExecuteDQLTask struct {
	taskmodel.BaseTask
	*taskmodel.TaskCondition
	id             taskmodel.UniqueID
	ts             taskmodel.Timestamp
	items          []ExecuteDQLPrepare
	execute        ExecuteDQLExecute
	maxConcurrency uint32
	concurrency    int
	nodeID         string
	startedAt      time.Time
	cancel         context.CancelFunc
	maxResultBytes uint64

	mu             sync.Mutex
	results        []*milvuspb.DQLResult
	resultBytes    uint64
	resultLimitErr error
}

var _ taskmodel.Task = (*ExecuteDQLTask)(nil)

func (b *ExecuteDQLTask) TraceCtx() context.Context   { return b.Ctx() }
func (b *ExecuteDQLTask) ID() taskmodel.UniqueID      { return b.id }
func (b *ExecuteDQLTask) SetID(id taskmodel.UniqueID) { b.id = id }
func (b *ExecuteDQLTask) Name() string                { return "ExecuteDQLTask" }

// The batch is a local orchestration task, without a corresponding wire MsgType.
func (b *ExecuteDQLTask) Type() commonpb.MsgType       { return commonpb.MsgType_Undefined }
func (b *ExecuteDQLTask) BeginTs() taskmodel.Timestamp { return b.ts }
func (b *ExecuteDQLTask) EndTs() taskmodel.Timestamp   { return b.ts }
func (b *ExecuteDQLTask) SetTs(ts taskmodel.Timestamp) { b.ts = ts }
func (b *ExecuteDQLTask) CanSkipAllocTimestamp() bool  { return true }
func (b *ExecuteDQLTask) OnEnqueue() error             { return nil }

func (b *ExecuteDQLTask) PreExecute(ctx context.Context) error {
	if err := contextutil.ErrOrDeadlineExceeded(ctx); err != nil {
		return err
	}
	requested := b.maxConcurrency
	if requested == 0 {
		requested = defaultExecuteDQLConcurrency
	}
	limit := max(1, paramtable.Get().ProxyCfg.ExecuteDQLMaxConcurrency.GetAsUint64())
	b.concurrency = int(min(uint64(requested), limit, uint64(len(b.items))))
	return nil
}

func (b *ExecuteDQLTask) Execute(ctx context.Context) error {
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
			var result *milvuspb.DQLResult
			defer func() {
				if recover() != nil {
					err := merr.WrapErrServiceInternalMsg("ExecuteDQL requests[%d] panicked", i)
					mlog.Error(itemCtx, "ExecuteDQL item panicked", mlog.Int("index", i),
						mlog.FieldTaskID(b.ID()), mlog.Err(err), mlog.ByteString("panic_stack", debug.Stack()))
					result = &milvuspb.DQLResult{Status: merr.Status(err)}
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

func (b *ExecuteDQLTask) PostExecute(context.Context) error { return nil }

func executeDQLResultSize(result *milvuspb.DQLResult) uint64 {
	const resultsField = 2 // milvuspb.ExecuteDQLResponse.results
	size := uint64(proto.Size(result))
	return uint64(protowire.SizeTag(resultsField)+protowire.SizeVarint(size)) + size
}

func executeDQLResultLimitError(limit, observed uint64, singleItem bool) error {
	message := "ExecuteDQL response has at least %d bytes, exceeding proxy.executeDQL.maxResultBytes (%d bytes)"
	if singleItem {
		message += ". One Search result exceeds the batch limit. Reduce nq, topK, or outputFields"
	}
	return merr.Wrapf(merr.ErrParameterTooLarge, message, observed, limit)
}

func (b *ExecuteDQLTask) complete(ctx context.Context, i int, result *milvuspb.DQLResult) {
	if result == nil || contextutil.ErrOrDeadlineExceeded(ctx) != nil {
		return
	}
	size := executeDQLResultSize(result)
	b.mu.Lock()
	if b.results == nil || b.results[i] != nil || contextutil.ErrOrDeadlineExceeded(ctx) != nil {
		b.mu.Unlock()
		return
	}
	b.resultBytes += size
	if b.resultBytes > b.maxResultBytes {
		// Publish the limit error before canceling execution and waking the
		// caller. Late completions cannot replace the error or retain results.
		b.resultLimitErr = executeDQLResultLimitError(b.maxResultBytes, b.resultBytes, executeDQLSuccessStatusBytes+size > b.maxResultBytes)
		b.results = nil
		b.cancel()
		b.mu.Unlock()
		return
	}
	b.results[i] = result
	label, code := executeDQLResultLabel(result.GetStatus())
	duration := time.Since(b.startedAt)
	// Serialize the event with finish so the caller cannot end the batch span
	// before an accepted completion has been recorded.
	traceExecuteDQLCompletion(b.TraceCtx(), i, label, code, duration)
	metrics.ProxyExecuteDQLSubrequestsTotal.WithLabelValues(b.nodeID, label).Inc()
	b.mu.Unlock()
	logExecuteDQLCompletion(ctx, i, label, code, duration)
}

// finish is called once by RunExecuteDQL, which owns final response accounting.
func (b *ExecuteDQLTask) finish(ctx context.Context, unfinished error) ([]*milvuspb.DQLResult, error) {
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
	var unfinishedResult *milvuspb.DQLResult
	var unfinishedBytes uint64
	for _, result := range results {
		if result != nil {
			continue
		}
		if unfinishedResult == nil {
			if unfinished == nil {
				unfinished = merr.WrapErrServiceInternalMsg("ExecuteDQL item finished without a result")
			}
			unfinishedResult = &milvuspb.DQLResult{Status: merr.Status(unfinished)}
			unfinishedBytes = executeDQLResultSize(unfinishedResult)
		}
		resultBytes += unfinishedBytes
		if resultBytes > b.maxResultBytes {
			return nil, executeDQLResultLimitError(b.maxResultBytes, resultBytes, false)
		}
	}
	for i, result := range results {
		if result != nil {
			continue
		}
		results[i] = proto.CloneOf(unfinishedResult)
		label, code := executeDQLResultLabel(unfinishedResult.GetStatus())
		traceExecuteDQLCompletion(ctx, i, label, code, duration)
		logExecuteDQLCompletion(ctx, i, label, code, duration)
		metrics.ProxyExecuteDQLSubrequestsTotal.WithLabelValues(b.nodeID, label).Inc()
	}
	return results, nil
}

func (b *ExecuteDQLTask) runItem(ctx context.Context, index int, prepare ExecuteDQLPrepare, execute ExecuteDQLExecute) *milvuspb.DQLResult {
	request, err := prepare(ctx)
	if err != nil {
		return &milvuspb.DQLResult{Status: executeDQLErrorStatus(err)}
	}
	if request == nil {
		return &milvuspb.DQLResult{Status: merr.Status(merr.WrapErrServiceInternalMsg("ExecuteDQL prepared no Search request"))}
	}
	var search *milvuspb.SearchResults
	var itemStatus *commonpb.Status
	var reason string
	// Retain the last attempt's status. retry.Handle can return the previous
	// refusal when the next attempt stops at the execution deadline.
	_ = retry.Handle(ctx, func() (bool, error) {
		if !b.startAttempt(ctx, index, reason) {
			return false, nil
		}
		attemptCtx, rejected := scheduler.WithDQLAdmissionTracking(ctx)
		var err error
		search, err = execute(attemptCtx, copySearchRequestForAttempt(request))
		switch {
		case err != nil:
			itemStatus = executeDQLErrorStatus(err)
		case search.GetStatus() == nil:
			itemStatus = merr.Status(merr.WrapErrServiceInternalMsg("Search returned no status"))
			return false, nil
		default:
			itemStatus = search.GetStatus()
		}
		if itemStatus.GetExtraInfo()[merr.InputErrorFlagKey] == "true" {
			return false, nil
		}
		switch merr.Code(merr.Error(itemStatus)) {
		case merr.Code(merr.ErrServiceRateLimit):
			reason = executeDQLRetryRateLimit
			logExecuteDQLRejection(ctx, index, reason)
			return true, merr.ErrServiceRateLimit
		case merr.Code(merr.ErrServiceTooManyRequests):
			if rejected.Load() {
				reason = executeDQLRetryQueueFull
				logExecuteDQLRejection(ctx, index, reason)
				return true, merr.ErrServiceTooManyRequests
			}
		}
		return false, nil
	})
	if itemStatus == nil {
		return nil // The execution budget expired before the first attempt.
	}
	if !merr.Ok(itemStatus) {
		return &milvuspb.DQLResult{Status: itemStatus}
	}
	return &milvuspb.DQLResult{
		Status: merr.Success(),
		Result: &milvuspb.DQLResult_Search{Search: search},
	}
}

func executeDQLErrorStatus(err error) *commonpb.Status {
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
