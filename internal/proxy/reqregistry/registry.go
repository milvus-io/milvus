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

// Package reqregistry keeps track of the client requests a proxy is currently
// serving, so that they can be listed and canceled.
//
// The unit of registration is one client RPC (a Search, HybridSearch or
// Query call), not one scheduler task: a single RPC can spawn several tasks
// (a requery, a search-by-primary-key vector fetch, a retried attempt) and
// those tasks share the request's context, so cancellation can only ever stop
// the whole tree. Registration therefore happens at the very top of the RPC
// method, before any task exists, and removal happens when the method returns.
//
// Cancellation is expressed purely through the request context: Register
// wraps the caller's ctx with context.WithCancelCause and stores the cancel
// function in the Entry; Cancel invokes it with merr.ErrRequestCanceled as
// the cause. Everything downstream -- the proxy scheduler, the QueryNode RPCs,
// the cgo futures and segcore -- already reacts to that ctx.
package reqregistry

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// State of a registered request on this proxy.
const (
	// StateQueued: registered, no task has started executing yet.
	StateQueued = "Queued"
	// StateRunning: at least one task of the request has started executing.
	StateRunning = "Running"
)

// Request types registered on the proxy.
const (
	TypeSearch       = "Search"
	TypeHybridSearch = "HybridSearch"
	TypeQuery        = "Query"
)

// ExprPrefixLimit bounds the expression text kept per request.
const ExprPrefixLimit = 256

// Info is the listable description of a request. Snapshot returns a copy with
// ElapsedMS computed at that moment.
type Info struct {
	RequestID      int64
	ProxyID        int64
	Type           string
	DBName         string
	CollectionName string
	User           string
	ClientAddr     string
	NQ             int64
	TopK           int64
	Expr           string
	StartTime      time.Time
	// QueuedMS is the scheduler queue wait of the request's first task, known
	// once that task starts executing; 0 before that.
	QueuedMS  int64
	ElapsedMS int64
	State     string
	TaskIDs   []int64
	TraceID   string
	// Cancellable is true for every type registered today; kept in the
	// snapshot so a future non-cancellable type can be listed without an
	// interface change.
	Cancellable bool
}

// Filter selects requests in List. Empty string / zero means "no constraint";
// the constraints are combined with AND.
type Filter struct {
	DBName         string
	CollectionName string
	User           string
	MinElapsed     time.Duration
}

func (f Filter) matches(info *Info, now time.Time) bool {
	if f.DBName != "" && info.DBName != f.DBName {
		return false
	}
	if f.CollectionName != "" && info.CollectionName != f.CollectionName {
		return false
	}
	if f.User != "" && info.User != f.User {
		return false
	}
	if f.MinElapsed > 0 && now.Sub(info.StartTime) < f.MinElapsed {
		return false
	}
	return true
}

// Entry is one registered request. It is safe for concurrent use: the
// registering goroutine, the scheduler (task linkage) and an operator's cancel
// all touch it.
type Entry struct {
	mu         sync.Mutex // guards info, canceled, canceledAt
	info       Info
	cancel     context.CancelCauseFunc
	canceled   bool
	canceledAt time.Time
}

// Snapshot returns a copy of the request description with ElapsedMS computed
// against now.
func (e *Entry) Snapshot(now time.Time) Info {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotLocked(now)
}

func (e *Entry) snapshotLocked(now time.Time) Info {
	info := e.info
	info.TaskIDs = append([]int64(nil), e.info.TaskIDs...)
	info.ElapsedMS = now.Sub(info.StartTime).Milliseconds()
	return info
}

// RequestID of the entry.
func (e *Entry) RequestID() int64 {
	return e.info.RequestID
}

// AddTask records a scheduler task id that belongs to this request. Called by
// the proxy task queue once the task has been assigned its id.
func (e *Entry) AddTask(taskID int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.info.TaskIDs = append(e.info.TaskIDs, taskID)
}

// MarkRunning records that a task of this request started executing after
// waiting queued in the scheduler. Only the first call sets QueuedMS: it is
// the wait of the request's first task, which is what an operator reads as
// "how long did this request sit in the queue".
func (e *Entry) MarkRunning(queued time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.info.State == StateRunning {
		return
	}
	e.info.State = StateRunning
	e.info.QueuedMS = queued.Milliseconds()
}

// Cancel cancels the request's context with merr.ErrRequestCanceled as the
// cause. It returns the snapshot taken at the moment of cancellation and true
// on the first call; later calls return false and do nothing, so an operator
// canceling twice is reported once.
func (e *Entry) Cancel(operator, reason string, now time.Time) (Info, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.canceled {
		return Info{}, false
	}
	e.canceled = true
	e.canceledAt = now
	snapshot := e.snapshotLocked(now)
	e.cancel(merr.WrapErrRequestCanceled(operator, reason))
	return snapshot, true
}

// CanceledAt returns when Cancel was first called and whether it was.
func (e *Entry) CanceledAt() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.canceledAt, e.canceled
}

type ctxKey struct{}

// FromContext returns the Entry registered on ctx, or nil. The pointer stays
// valid after the entry has been removed from the registry, so a task that
// outlives the RPC can still read it.
func FromContext(ctx context.Context) *Entry {
	e, _ := ctx.Value(ctxKey{}).(*Entry)
	return e
}

// CancelCause returns the operator cancellation that ended ctx, or nil when
// ctx is still live or ended for any other reason (client deadline, client
// disconnect). Callers use it at the outermost RPC layer to turn the generic
// ctx error into merr.ErrRequestCanceled for the client.
func CancelCause(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	cause := context.Cause(ctx)
	if errors.Is(cause, merr.ErrRequestCanceled) {
		return cause
	}
	return nil
}

// Registry holds the requests currently registered on one proxy.
type Registry struct {
	entries *typeutil.ConcurrentMap[int64, *Entry]
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{entries: typeutil.NewConcurrentMap[int64, *Entry]()}
}

// Register records a request and returns the derived context that the request
// must run under, plus the entry. The caller must call Unregister when the RPC
// returns (normally in a defer), which also releases the derived context.
// info.RequestID must be set by the caller; StartTime, State, ElapsedMS and
// Cancellable are filled here.
func (r *Registry) Register(ctx context.Context, info Info) (context.Context, *Entry) {
	ctx, cancel := context.WithCancelCause(ctx)
	info.StartTime = time.Now()
	info.State = StateQueued
	info.Cancellable = true
	if len(info.Expr) > ExprPrefixLimit {
		info.Expr = info.Expr[:ExprPrefixLimit]
	}
	e := &Entry{info: info, cancel: cancel}
	r.entries.Insert(info.RequestID, e)
	return context.WithValue(ctx, ctxKey{}, e), e
}

// Unregister removes the entry and releases its context. Safe to call with a
// nil entry and safe to call more than once.
func (r *Registry) Unregister(e *Entry) {
	if e == nil {
		return
	}
	r.entries.Remove(e.info.RequestID)
	// Release the derived ctx. A request that finished normally has never
	// been canceled; give context.Canceled so nothing downstream mistakes
	// this bookkeeping release for an operator cancel.
	e.cancel(context.Canceled)
}

// Get returns the entry for requestID, or nil.
func (r *Registry) Get(requestID int64) *Entry {
	e, _ := r.entries.Get(requestID)
	return e
}

// Len returns the number of registered requests.
func (r *Registry) Len() int {
	return r.entries.Len()
}

// List returns a snapshot of every registered request matching filter.
func (r *Registry) List(filter Filter, now time.Time) []Info {
	result := make([]Info, 0, r.entries.Len())
	r.entries.Range(func(_ int64, e *Entry) bool {
		info := e.Snapshot(now)
		if filter.matches(&info, now) {
			result = append(result, info)
		}
		return true
	})
	return result
}

// Cancel cancels every request in requestIDs that this registry holds and
// returns their snapshots taken at cancellation, plus the ids it does not
// hold. An id whose request was already canceled counts as canceled again
// but is not reported a second time.
func (r *Registry) Cancel(requestIDs []int64, operator, reason string, now time.Time) (canceled []Info, notFound []int64) {
	for _, id := range requestIDs {
		e, ok := r.entries.Get(id)
		if !ok {
			notFound = append(notFound, id)
			continue
		}
		if snapshot, first := e.Cancel(operator, reason, now); first {
			canceled = append(canceled, snapshot)
		}
	}
	return canceled, notFound
}
