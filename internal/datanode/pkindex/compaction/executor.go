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

package compaction

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/shirou/gopsutil/v4/disk"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

// Executor runs the compaction tasks of this node. It owns the task
// directories under Config.LocalRoot: it creates them, and it deletes them
// when a task is dropped, deleted after its TTL, or found at start. A task
// reports Finished only after every output is uploaded.
//
// Create returns at once. At most Config.Parallelism tasks run; the others
// wait for a free worker and stay InProgress. A waiting task holds a slot
// like a running one, which is what bounds the queue from the coordinator
// side.
//
// Call SetNodeID before the first Create, and Start, or the directories of
// finished tasks nobody queries are never deleted. Call Close after the RPC
// server stops.
type Executor struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	cfg      Config
	store    Store
	sem      chan struct{}
	nodeID   string
	freeDisk func(path string) (uint64, error)
	now      func() time.Time

	mu     sync.Mutex
	tasks  map[int64]*task
	closed bool
}

// New returns an executor with an empty Config.LocalRoot. Call Start to run
// the deletion of unqueried terminal tasks.
func New(ctx context.Context, cfg Config, store Store) (*Executor, error) {
	if err := os.RemoveAll(cfg.LocalRoot); err != nil {
		return nil, pkindex.MarkIO(err, "clear %s", cfg.LocalRoot)
	}
	if err := os.MkdirAll(cfg.LocalRoot, 0o755); err != nil {
		return nil, pkindex.MarkIO(err, "create %s", cfg.LocalRoot)
	}
	if cfg.TerminalTaskTTL <= 0 {
		mlog.Warn(ctx, "pkindex compaction terminal task ttl is not positive, using one minute", mlog.Duration("ttl", cfg.TerminalTaskTTL))
		cfg.TerminalTaskTTL = time.Minute
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Executor{
		ctx: ctx, cancel: cancel, cfg: cfg, store: store,
		sem:      make(chan struct{}, max(cfg.Parallelism, 1)),
		freeDisk: freeDisk,
		now:      time.Now,
		tasks:    map[int64]*task{},
	}, nil
}

func freeDisk(path string) (uint64, error) {
	usage, err := disk.Usage(path)
	if err != nil {
		return 0, err
	}
	return usage.Free, nil
}

// SetNodeID sets the node id label of the metrics.
func (e *Executor) SetNodeID(id int64) { e.nodeID = strconv.FormatInt(id, 10) }

// Start runs the deletion of unqueried terminal tasks until Close.
func (e *Executor) Start() {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		interval := max(e.cfg.TerminalTaskTTL/4, 10*time.Millisecond)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-e.ctx.Done():
				return
			case <-ticker.C:
				e.reap()
			}
		}
	}()
}

// Close cancels the tasks, waits for them and deletes every task directory.
// Create fails after Close. It is safe to call Close more than once.
func (e *Executor) Close() {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.cancel()
	e.wg.Wait()
	e.mu.Lock()
	ids := make([]int64, 0, len(e.tasks))
	for id := range e.tasks {
		ids = append(ids, id)
	}
	e.mu.Unlock()
	for _, id := range ids {
		e.Drop(id)
	}
}

// errInvalidRequest marks a malformed compaction request.
var errInvalidRequest = errors.New("invalid pkindex compaction request")

// Create starts a task. A request whose task id is already here replaces the
// earlier attempt: the coordinator sent it again with new output ids. It
// returns an error marked pkindex.ErrUnavailable when the local disk has no
// room for the task or the executor is closed, so that the scheduler tries
// another node later; a malformed request returns an unmarked error.
func (e *Executor) Create(ctx context.Context, req *pkindexpb.CompactionRequest) error {
	if len(req.GetInputs()) == 0 || req.GetOutputIdEnd() <= req.GetOutputIdBegin() || req.GetTargetFileSize() <= 0 || req.GetSstBasePath() == "" {
		return errors.Wrapf(errInvalidRequest, "task %d: %d inputs, output ids [%d, %d), target file size %d, base path %q",
			req.GetTaskId(), len(req.GetInputs()), req.GetOutputIdBegin(), req.GetOutputIdEnd(), req.GetTargetFileSize(), req.GetSstBasePath())
	}
	var inputBytes int64
	for _, s := range req.GetInputs() {
		inputBytes += s.GetSize()
	}
	if err := e.admitDisk(inputBytes); err != nil {
		return err
	}
	e.Drop(req.GetTaskId())

	tctx, cancel := context.WithCancel(e.ctx)
	// The output id begin in the name keeps two attempts of one task id apart.
	dir := filepath.Join(e.cfg.LocalRoot, strconv.FormatInt(req.GetTaskId(), 10)+"-"+strconv.FormatInt(req.GetOutputIdBegin(), 10))
	t := &task{req: req, dir: dir, cancel: cancel, done: make(chan struct{}), state: taskcommon.InProgress}
	t.lastQueried.Store(e.now().UnixNano())
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		cancel()
		return errors.Mark(errors.New("pkindex compaction executor is closed"), pkindex.ErrUnavailable)
	}
	e.tasks[req.GetTaskId()] = t
	e.mu.Unlock()
	e.updateRunning()

	mlog.Info(ctx, "pkindex compaction task accepted", mlog.FieldTaskID(req.GetTaskId()), mlog.FieldCollectionID(req.GetCollectionId()),
		mlog.FieldVChannel(req.GetVchannel()), mlog.Int("inputs", len(req.GetInputs())), mlog.Int64("inputBytes", inputBytes))
	go func() {
		defer close(t.done)
		start := time.Now()
		var outputs []*pkindexpb.SSTRef
		var err error
		select {
		case e.sem <- struct{}{}:
			outputs, err = t.run(tctx, e.store)
			<-e.sem
		case <-tctx.Done():
			err = tctx.Err()
		}
		if err != nil && tctx.Err() != nil {
			// A cancellation is never the fault of the request.
			err = errors.Mark(err, pkindex.ErrUnavailable)
		}
		t.setDone(outputs, err)
		e.updateRunning()
		if err != nil {
			mlog.Warn(tctx, "pkindex compaction task failed", mlog.FieldTaskID(req.GetTaskId()), mlog.Duration("elapsed", time.Since(start)), mlog.Err(err))
		} else {
			mlog.Info(tctx, "pkindex compaction task finished", mlog.FieldTaskID(req.GetTaskId()), mlog.Int("outputs", len(outputs)),
				mlog.Int64("entries", t.processed.Load()), mlog.Duration("elapsed", time.Since(start)))
		}
	}()
	return nil
}

// admitDisk returns an error marked ErrUnavailable when the free disk under
// LocalRoot is below twice inputBytes plus Config.DiskMargin: the inputs and
// the outputs are both on disk at the peak.
func (e *Executor) admitDisk(inputBytes int64) error {
	free, err := e.freeDisk(e.cfg.LocalRoot)
	if err != nil {
		return pkindex.MarkIO(err, "stat %s", e.cfg.LocalRoot)
	}
	need := uint64(inputBytes*2 + e.cfg.DiskMargin)
	if free < need {
		return errors.Mark(errors.Newf("free disk %d is below the %d a compaction of %d input bytes needs", free, need, inputBytes), pkindex.ErrUnavailable)
	}
	return nil
}

// Query returns the state of a task and refreshes its TTL. ok is false when
// the task is not here. A failed task's reason is its error translated for
// the coordinator, and retriable tells whether another attempt may succeed.
func (e *Executor) Query(taskID int64) (state taskcommon.State, reason string, result *pkindexpb.CompactionResult, ok bool) {
	e.mu.Lock()
	t, ok := e.tasks[taskID]
	e.mu.Unlock()
	if !ok {
		return taskcommon.None, "", nil, false
	}
	t.lastQueried.Store(e.now().UnixNano())
	state, outputs, err := t.getState()
	result = &pkindexpb.CompactionResult{EntriesProcessed: t.processed.Load()}
	switch state {
	case taskcommon.Finished:
		result.Outputs = outputs
	case taskcommon.Failed:
		reason = pkindex.ToMerr(err).Error()
		result.Retriable = retriable(err)
	}
	return state, reason, result, true
}

// Drop cancels a task, waits for it and deletes its directory. The outputs
// already uploaded stay in object storage for the GC.
func (e *Executor) Drop(taskID int64) {
	e.mu.Lock()
	t, ok := e.tasks[taskID]
	if ok {
		delete(e.tasks, taskID)
	}
	e.mu.Unlock()
	if !ok {
		return
	}
	t.cancel()
	<-t.done
	if err := os.RemoveAll(t.dir); err != nil {
		mlog.Warn(e.ctx, "pkindex compaction task directory is not deleted", mlog.FieldTaskID(taskID), mlog.String("dir", t.dir), mlog.Err(err))
	}
	e.updateRunning()
}

// Slots is what the running tasks take from the node slots.
func (e *Executor) Slots() int64 {
	return int64(e.running()) * e.cfg.TaskSlot
}

func (e *Executor) running() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, t := range e.tasks {
		if state, _, _ := t.getState(); state == taskcommon.InProgress {
			n++
		}
	}
	return n
}

func (e *Executor) updateRunning() {
	metrics.DataNodePKIndexCompactionRunning.WithLabelValues(e.nodeID).Set(float64(e.running()))
}

// reap deletes the finished and failed tasks that no query touched for
// Config.TerminalTaskTTL. A coordinator that restarted does not know them;
// without this they would keep their directories for ever.
func (e *Executor) reap() {
	cutoff := e.now().Add(-e.cfg.TerminalTaskTTL).UnixNano()
	e.mu.Lock()
	var stale []int64
	for id, t := range e.tasks {
		if state, _, _ := t.getState(); state != taskcommon.InProgress && t.lastQueried.Load() < cutoff {
			stale = append(stale, id)
		}
	}
	e.mu.Unlock()
	for _, id := range stale {
		mlog.Info(e.ctx, "pkindex compaction task deleted: nobody queried it", mlog.FieldTaskID(id), mlog.Duration("ttl", e.cfg.TerminalTaskTTL))
		e.Drop(id)
	}
}
