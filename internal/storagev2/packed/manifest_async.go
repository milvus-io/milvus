// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
extern int32_t milvusManifestSubmit(void*, LoonAsyncTask, void*);
extern void milvusManifestOpenDone(uintptr_t, LoonFFIResult, LoonTransactionHandle);
extern void milvusManifestCommitDone(uintptr_t, LoonFFIResult, int32_t, int64_t);
static inline void milvusRunManifestTask(LoonAsyncTask task, void* data) { task(data); }
*/
import "C"

import (
	"context"
	"runtime/cgo"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type manifestTask struct {
	run  C.LoonAsyncTask
	data unsafe.Pointer
}

// ManifestIOContext belongs to a coordinator, not the process. Admission waits
// in Go; native tasks and callbacks use a bounded, caller-owned Go executor.
// Close drains callers, then native callbacks, then workers, in that order.
// Constructing a context is cheap: native resources are acquired on first use.
type ManifestIOContext struct {
	mu        sync.Mutex
	closeOnce sync.Once
	callers   sync.WaitGroup
	stopping  chan struct{}
	closed    bool
	once      sync.Once
	initErr   error
	native    C.LoonAsyncContextHandle
	token     cgo.Handle
	executor  unsafe.Pointer
	tasks     chan manifestTask
	workers   sync.WaitGroup
	slots     chan struct{}
	// taskMu protects demand accounting, including tasks executing native code.
	// Workers grow to meet demand and remain available until Close joins them.
	taskMu       sync.Mutex
	workerCount  int
	pendingTasks int
}

func NewManifestIOContext(concurrency int) *ManifestIOContext {
	return &ManifestIOContext{slots: make(chan struct{}, max(1, concurrency)), stopping: make(chan struct{})}
}

func (io *ManifestIOContext) init() {
	io.executor = C.malloc(C.size_t(unsafe.Sizeof(C.uintptr_t(0))))
	if io.executor == nil {
		io.initErr = merr.Wrap(merr.ErrServiceResourceInsufficient, "allocate manifest executor")
		return
	}
	io.token = cgo.NewHandle(io)
	*(*C.uintptr_t)(io.executor) = C.uintptr_t(io.token)
	io.tasks = make(chan manifestTask, 2*cap(io.slots))
	descriptor := C.LoonAsyncExecutor{context: io.executor, submit: (C.LoonAsyncSubmit)(C.milvusManifestSubmit)}
	io.initErr = HandleLoonFFIResult(C.loon_async_context_create(&descriptor, &io.native))
	if io.initErr != nil {
		io.initErr = merr.WrapErrStorage(io.initErr, "create manifest IO context")
		io.token.Delete()
		C.free(io.executor)
		io.executor = nil
		return
	}
}

func (io *ManifestIOContext) runWorker() {
	defer io.workers.Done()
	for task := range io.tasks {
		C.milvusRunManifestTask(task.run, task.data)
		io.taskMu.Lock()
		io.pendingTasks--
		io.taskMu.Unlock()
	}
}

// Admission registers callers before waiting. Closing wakes queued callers and
// waits for active transactions without holding a lock across native callbacks.
func (io *ManifestIOContext) acquire(ctx context.Context) error {
	io.mu.Lock()
	if io.closed {
		io.mu.Unlock()
		return merr.WrapErrServiceUnavailable("manifest IO context is closed")
	}
	io.callers.Add(1)
	io.mu.Unlock()
	select {
	case io.slots <- struct{}{}:
	case <-ctx.Done():
		io.callers.Done()
		return ctx.Err()
	case <-io.stopping:
		io.callers.Done()
		return merr.WrapErrServiceUnavailable("manifest IO context is closed")
	}
	if err := ctx.Err(); err != nil {
		io.release()
		return err
	}
	select {
	case <-io.stopping:
		io.release()
		return merr.WrapErrServiceUnavailable("manifest IO context is closed")
	default:
	}
	io.once.Do(io.init)
	if io.initErr != nil {
		io.release()
		return io.initErr
	}
	return nil
}

func (io *ManifestIOContext) release() {
	<-io.slots
	io.callers.Done()
}

func (io *ManifestIOContext) Close() {
	io.closeOnce.Do(func() {
		io.mu.Lock()
		io.closed = true
		close(io.stopping)
		io.mu.Unlock()
		io.callers.Wait()
		if io.native == nil {
			return
		}
		C.loon_async_context_destroy(io.native)
		close(io.tasks)
		io.workers.Wait()
		io.token.Delete()
		C.free(io.executor)
		io.native = nil
	})
}

//export milvusManifestSubmit
func milvusManifestSubmit(executor unsafe.Pointer, task C.LoonAsyncTask, data unsafe.Pointer) C.int32_t {
	io := cgo.Handle(*(*C.uintptr_t)(executor)).Value().(*ManifestIOContext)
	io.taskMu.Lock()
	defer io.taskMu.Unlock()
	select {
	case io.tasks <- manifestTask{task, data}:
		io.pendingTasks++
		if io.pendingTasks > io.workerCount && io.workerCount < cap(io.slots) {
			io.workerCount++
			io.workers.Add(1)
			go io.runWorker()
		}
		return 0
	default:
		return 1 // Native admission rolls back; never retain or execute this task.
	}
}

type manifestOpenResult struct {
	transaction C.LoonTransactionHandle
	err         error
}

//export milvusManifestOpenDone
func milvusManifestOpenDone(token C.uintptr_t, result C.LoonFFIResult, txn C.LoonTransactionHandle) {
	h := cgo.Handle(token)
	complete := h.Value().(func(manifestOpenResult))
	value := manifestOpenResult{txn, handleManifestAsyncResult(result)}
	h.Delete()
	complete(value)
}

// ManifestCommitOutcome is independent of the underlying error's retryability.
type ManifestCommitOutcome int32

const (
	ManifestNotCommitted  ManifestCommitOutcome = C.LOON_COMMIT_NOT_COMMITTED
	ManifestCommitted     ManifestCommitOutcome = C.LOON_COMMIT_COMMITTED
	ManifestCommitUnknown ManifestCommitOutcome = C.LOON_COMMIT_UNKNOWN
)

// ManifestCommitError preserves UNKNOWN through wrapping. Callers must not
// replay the consumed transaction; a coordinator may rebuild a new transaction
// from its still-published pointer under the segment lock.
type ManifestCommitError struct {
	Outcome ManifestCommitOutcome
	Err     error
}

func (e *ManifestCommitError) Error() string {
	if e.Outcome == ManifestCommitUnknown {
		return "manifest commit outcome unknown: " + e.Err.Error()
	}
	return e.Err.Error()
}
func (e *ManifestCommitError) Unwrap() error { return e.Err }

type manifestCommitResult struct {
	outcome ManifestCommitOutcome
	version int64
	err     error
}

//export milvusManifestCommitDone
func milvusManifestCommitDone(token C.uintptr_t, result C.LoonFFIResult, outcome C.int32_t, version C.int64_t) {
	h := cgo.Handle(token)
	complete := h.Value().(func(manifestCommitResult))
	value := manifestCommitResult{ManifestCommitOutcome(outcome), int64(version), handleManifestAsyncResult(result)}
	h.Delete()
	complete(value)
}

// Async queue exhaustion and deadline codes are outside the native ExtendStatus
// table used by HandleLoonFFIResult. Both are transient, and their producers
// guarantee that the operation has not started execution.
func handleManifestAsyncResult(result C.LoonFFIResult) error {
	if result.err_code == C.loon_errcode_async_overloaded || result.err_code == C.loon_errcode_async_deadline {
		defer C.loon_ffi_free_result(&result)
		return merr.Wrapf(ErrLoonTransient, "manifest async operation did not start (code=%d): %s", int(result.err_code), C.GoString(result.message))
	}
	return HandleLoonFFIResult(result)
}

func manifestAsyncHandle(ctx context.Context) C.LoonAsyncHandle {
	timeout := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(time.Until(deadline), 24*time.Hour)
	}
	return C.LoonAsyncHandle{timeout_ms: C.uint64_t(max(1, timeout.Milliseconds()))}
}

// submitOpen never waits for I/O or callback completion. The short mutex only
// serializes access to the caller-owned handle: callback/cancellation may race
// submission, but the executor's submit hook never runs a task inline or waits.
func (io *ManifestIOContext) submitOpen(ctx context.Context, base string, version int64, config *indexpb.StorageConfig, resolver C.int32_t, complete func(manifestOpenResult)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	properties, err := MakePropertiesFromStorageConfig(config, nil)
	if err != nil {
		return err
	}
	defer C.loon_properties_free(properties)
	cBase := C.CString(base)
	defer C.free(unsafe.Pointer(cBase))

	var mu sync.Mutex
	operation := manifestAsyncHandle(ctx)
	var stopCancel func() bool
	token := cgo.NewHandle(func(result manifestOpenResult) {
		mu.Lock()
		stopCancel()
		C.loon_async_release(&operation)
		mu.Unlock()
		if err := ctx.Err(); err != nil {
			if result.transaction != 0 {
				C.loon_transaction_destroy(result.transaction)
			}
			result = manifestOpenResult{err: err}
		} else if result.err != nil {
			result.err = merr.WrapErrStorage(result.err, "async manifest open")
		}
		complete(result)
	})
	mu.Lock()
	res := C.loon_transaction_open_async(io.native, cBase, properties, C.int64_t(version), resolver,
		getRetryLimit(), (C.LoonTransactionOpenCallback)(C.milvusManifestOpenDone), C.uintptr_t(token), &operation)
	if err := handleManifestAsyncResult(res); err != nil {
		mu.Unlock()
		token.Delete() // Rejected submissions never invoke a callback.
		return merr.WrapErrStorage(err, "async manifest open")
	}
	stopCancel = context.AfterFunc(ctx, func() {
		mu.Lock()
		C.loon_async_cancel(&operation)
		mu.Unlock()
	})
	mu.Unlock()
	return nil
}

func (io *ManifestIOContext) open(ctx context.Context, base string, version int64, config *indexpb.StorageConfig, resolver C.int32_t) (C.LoonTransactionHandle, error) {
	done := make(chan manifestOpenResult, 1)
	if err := io.submitOpen(ctx, base, version, config, resolver, func(result manifestOpenResult) { done <- result }); err != nil {
		return 0, err
	}
	result := <-done // Cancellation also drains the terminal callback.
	return result.transaction, result.err
}

// SubmitManifestIndexInfos waits only for admission, then loads and projects the
// manifest on the same executor that delivers the native callback. An accepted
// read calls complete exactly once, possibly before this function returns; a
// rejected read returns an error without calling complete. The callback must not
// block, submit more work, or close its own IO context. Close drains callbacks.
func SubmitManifestIndexInfos(ctx context.Context, io *ManifestIOContext, manifestPath string, config *indexpb.StorageConfig, complete func([]ManifestIndexInfo, error)) error {
	base, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return err
	}
	if err := io.acquire(ctx); err != nil {
		return err
	}
	err = io.submitOpen(ctx, base, version, config, C.LOON_TRANSACTION_RESOLVE_FAIL, func(result manifestOpenResult) {
		defer io.release()
		if result.err != nil {
			complete(nil, result.err)
			return
		}
		defer C.loon_transaction_destroy(result.transaction)
		entries, err := transactionIndexInfos(result.transaction, manifestPath)
		complete(entries, err)
	})
	if err != nil {
		io.release()
	}
	return err
}

// submitCommit transfers completion to the executor without waiting for the
// native callback. Like submitOpen, it serializes access to the caller-owned
// handle even when completion races the return from submission.
func (io *ManifestIOContext) submitCommit(ctx context.Context, txn C.LoonTransactionHandle, complete func(manifestCommitResult)) error {
	if err := ctx.Err(); err != nil {
		return &ManifestCommitError{ManifestNotCommitted, err}
	}
	var mu sync.Mutex
	operation := manifestAsyncHandle(ctx)
	var stopCancel func() bool
	token := cgo.NewHandle(func(result manifestCommitResult) {
		mu.Lock()
		stopCancel()
		C.loon_async_release(&operation)
		mu.Unlock()
		complete(result)
	})
	mu.Lock()
	res := C.loon_transaction_commit_async(io.native, txn,
		(C.LoonTransactionCommitCallback)(C.milvusManifestCommitDone), C.uintptr_t(token), &operation)
	if err := handleManifestAsyncResult(res); err != nil {
		mu.Unlock()
		token.Delete()
		return &ManifestCommitError{ManifestNotCommitted, merr.WrapErrStorage(err, "submit manifest commit")}
	}
	stopCancel = context.AfterFunc(ctx, func() {
		mu.Lock()
		C.loon_async_cancel(&operation)
		mu.Unlock()
	})
	mu.Unlock()
	return nil
}

func (result manifestCommitResult) finish(ctx context.Context) (int64, error) {
	if result.err != nil {
		err := merr.WrapErrStorage(result.err, "async manifest commit")
		if result.outcome == ManifestNotCommitted && ctx.Err() != nil {
			err = ctx.Err()
		}
		return -1, &ManifestCommitError{result.outcome, err}
	}
	if result.outcome != ManifestCommitted || result.version < 0 {
		return -1, &ManifestCommitError{result.outcome, merr.WrapErrServiceInternalMsg("invalid async manifest commit result")}
	}
	return result.version, nil
}

// GetManifestIndexInfosAsync waits in Go while OpenAsync loads the exact revision.
func GetManifestIndexInfosAsync(ctx context.Context, io *ManifestIOContext, manifestPath string, config *indexpb.StorageConfig) ([]ManifestIndexInfo, error) {
	manifest, err := getManifestAsync(ctx, io, manifestPath, config)
	if err != nil {
		return nil, err
	}
	defer C.loon_manifest_destroy(manifest)
	return manifestIndexInfos(manifest, manifestPath)
}

func GetManifestLobFilesAsync(ctx context.Context, io *ManifestIOContext, manifestPath string, config *indexpb.StorageConfig) ([]LobFileInfo, error) {
	manifest, err := getManifestAsync(ctx, io, manifestPath, config)
	if err != nil {
		return nil, err
	}
	defer C.loon_manifest_destroy(manifest)
	return manifestLobFiles(manifest), nil
}

func getManifestAsync(ctx context.Context, io *ManifestIOContext, manifestPath string, config *indexpb.StorageConfig) (*C.LoonManifest, error) {
	base, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, err
	}
	if err := io.acquire(ctx); err != nil {
		return nil, err
	}
	defer io.release()
	txn, err := io.open(ctx, base, version, config, C.LOON_TRANSACTION_RESOLVE_FAIL)
	if err != nil {
		return nil, err
	}
	defer C.loon_transaction_destroy(txn)
	var manifest *C.LoonManifest
	if err := HandleLoonFFIResult(C.loon_transaction_get_manifest(txn, &manifest)); err != nil {
		return nil, merr.WrapErrStorage(err, "get loaded manifest")
	}
	return manifest, nil
}

func transactionIndexInfos(txn C.LoonTransactionHandle, manifestPath string) ([]ManifestIndexInfo, error) {
	var manifest *C.LoonManifest
	if err := HandleLoonFFIResult(C.loon_transaction_get_manifest(txn, &manifest)); err != nil {
		return nil, merr.WrapErrStorage(err, "get loaded manifest")
	}
	defer C.loon_manifest_destroy(manifest)
	return manifestIndexInfos(manifest, manifestPath)
}

// ManifestUpdateResult carries the committed path and the index-marker change.
// A nil HasIndexes means the mutation preserves the caller's existing marker.
// Marker changes are delivered only after a successful commit (or a proven no-op).
type ManifestUpdateResult struct {
	ManifestPath string
	HasIndexes   *bool
}

// SubmitManifestUpdates waits only for admission, then chains native open and
// commit on io's executor. An accepted submission calls complete exactly once,
// possibly before returning; rejection returns an error without a callback.
// Updates must remain immutable until completion. The callback must not block,
// submit more work, or close its own IO context. Close drains accepted work.
func SubmitManifestUpdates(ctx context.Context, io *ManifestIOContext, base string, version int64, config *indexpb.StorageConfig, updates *ManifestUpdates, complete func(ManifestUpdateResult, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if updates.isEmpty() {
		complete(ManifestUpdateResult{ManifestPath: MarshalManifestPath(base, version)}, nil)
		return nil
	}
	if err := io.acquire(ctx); err != nil {
		return err
	}
	err := io.submitOpen(ctx, base, version, config, C.LOON_TRANSACTION_RESOLVE_OVERWRITE, func(result manifestOpenResult) {
		finish := func(updateResult ManifestUpdateResult, err error) {
			defer io.release()
			if result.transaction != 0 {
				defer C.loon_transaction_destroy(result.transaction)
			}
			complete(updateResult, err)
		}
		if result.err != nil {
			finish(ManifestUpdateResult{}, result.err)
			return
		}
		manifestPath := MarshalManifestPath(base, version)
		changed, hasIndexes, err := applyAsyncManifestUpdates(result.transaction, manifestPath, updates)
		if err != nil {
			finish(ManifestUpdateResult{}, err)
			return
		}
		if !changed {
			finish(ManifestUpdateResult{manifestPath, hasIndexes}, nil)
			return
		}
		// Keep the original admission slot and transaction until commit completes.
		// Submitting directly avoids re-entering admission from an executor worker.
		err = io.submitCommit(ctx, result.transaction, func(result manifestCommitResult) {
			committed, err := result.finish(ctx)
			if err != nil {
				finish(ManifestUpdateResult{}, err)
				return
			}
			finish(ManifestUpdateResult{MarshalManifestPath(base, committed), hasIndexes}, nil)
		})
		if err != nil {
			finish(ManifestUpdateResult{}, err)
		}
	})
	if err != nil {
		io.release()
	}
	return err
}

// OVERWRITE applies mutations to this transaction's exact read revision, even
// when conflict retries allocate a newer version. Reuse that in-memory snapshot
// both for drop validation and for the final index-presence projection.
func applyAsyncManifestUpdates(txn C.LoonTransactionHandle, manifestPath string, updates *ManifestUpdates) (bool, *bool, error) {
	var columns []string
	if updates.NewFiles != nil {
		columns = updates.NewFiles.invalidatedIndexColumns()
	}
	var indexes []ManifestIndexInfo
	if len(updates.DropIndexes) > 0 || (len(columns) > 0 && len(updates.Indexes) == 0) {
		var err error
		indexes, err = transactionIndexInfos(txn, manifestPath)
		if err != nil {
			return false, nil, err
		}
	}
	drops, err := resolveManifestIndexDrops(manifestPath, indexes, updates.DropIndexes)
	if err != nil {
		return false, nil, err
	}
	hasIndexes := manifestIndexesAfterUpdates(indexes, updates, drops, columns)
	if updates.NewFiles == nil && len(updates.ColumnGroups) == 0 && len(updates.DeltaLogs) == 0 &&
		len(updates.Stats) == 0 && len(updates.Indexes) == 0 && len(drops) == 0 {
		return false, hasIndexes, nil
	}
	return true, hasIndexes, applyManifestUpdates(txn, updates, drops)
}

func manifestIndexesAfterUpdates(indexes []ManifestIndexInfo, updates *ManifestUpdates, drops []int64, appendedColumns []string) *bool {
	// Storage applies file invalidation and drops before additions. At least one
	// addition therefore guarantees a nonempty index section after a valid commit.
	value := len(updates.Indexes) > 0
	if value {
		return &value
	}
	if len(updates.DropIndexes) == 0 && len(appendedColumns) == 0 {
		return nil
	}
	for _, index := range indexes {
		if !slices.Contains(drops, index.IndexID) && !slices.Contains(appendedColumns, index.ColumnName) {
			value = true
			break
		}
	}
	return &value
}

// CommitManifestUpdatesWithResultAsync also returns the index-marker change.
func CommitManifestUpdatesWithResultAsync(ctx context.Context, io *ManifestIOContext, base string, version int64, config *indexpb.StorageConfig, updates *ManifestUpdates) (ManifestUpdateResult, error) {
	type commitResult struct {
		result ManifestUpdateResult
		err    error
	}
	done := make(chan commitResult, 1)
	if err := SubmitManifestUpdates(ctx, io, base, version, config, updates, func(result ManifestUpdateResult, err error) {
		done <- commitResult{result, err}
	}); err != nil {
		return ManifestUpdateResult{}, err
	}
	result := <-done
	return result.result, result.err
}
