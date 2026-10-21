// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bulkimport

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const (
	// snapshotExpandConcurrency bounds how many SnapshotPreparing jobs may
	// perform source I/O concurrently on DataCoord. Kept small (2) to limit
	// simultaneous in-memory SegmentDescription peak on small MixCoord pods
	// while each job reads its segment manifests concurrently. Queued jobs
	// are admitted on subsequent checker ticks.
	snapshotExpandConcurrency = 2

	// snapshotExpansionTimeout caps one V3 expansion attempt when the job has
	// no earlier deadline, so a stalled external endpoint cannot pin a slot
	// indefinitely.
	snapshotExpansionTimeout = 10 * time.Minute
)

type snapshotExpandResult struct {
	files   []*internalpb.ImportFile
	options importutilv2.Options
	sources []*internalpb.SnapshotImportL0Source
	err     error
}

type snapshotExpandEntry struct {
	cancel context.CancelFunc
	done   bool
	result snapshotExpandResult
}

// SnapshotExpandManager runs SnapshotPreparing source I/O off the V3 checker
// goroutine with bounded concurrency and per-job cancellation. It owns no
// metadata, ID allocation, or job state transitions: snapshotPreparingHandler
// polls completed results and drives all persistence and error classification
// on the state-machine loop.
type SnapshotExpandManager struct {
	ctx context.Context
	cm  milvusstorage.ChunkManager

	mu      sync.Mutex
	entries map[int64]*snapshotExpandEntry
	running int
	wg      sync.WaitGroup
	closed  bool
}

func NewSnapshotExpandManager(ctx context.Context, cm milvusstorage.ChunkManager) *SnapshotExpandManager {
	return &SnapshotExpandManager{
		ctx:     ctx,
		cm:      cm,
		entries: make(map[int64]*snapshotExpandEntry),
	}
}

// snapshotExpansionContext bounds one expansion by the earlier of the job
// deadline and snapshotExpansionTimeout. capped reports that the cap (not the
// job's own deadline) applies; expired reports that the job's own deadline has
// already passed so the caller can skip starting work and let GC time out the
// job.
func snapshotExpansionContext(parent context.Context, job ImportJob) (context.Context, context.CancelFunc, bool, bool) {
	now := time.Now()
	deadline := now.Add(snapshotExpansionTimeout)
	capped := true
	if timeoutTs := job.GetTimeoutTs(); timeoutTs != 0 && timeoutTs != math.MaxUint64 {
		jobDeadline, _ := tsoutil.ParseTS(timeoutTs)
		if !jobDeadline.After(now) {
			return nil, nil, false, true
		}
		if jobDeadline.Before(deadline) {
			deadline, capped = jobDeadline, false
		}
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	return ctx, cancel, capped, false
}

// PollOrStart harvests a completed expansion result for job (removing it from
// the cache) or starts one bounded background expansion if none is running.
// Returning ready=false means the job is still expanding, waiting for a
// concurrency slot, or already past its own deadline.
func (m *SnapshotExpandManager) PollOrStart(job ImportJob) (snapshotExpandResult, bool) {
	jobID := job.GetJobID()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return snapshotExpandResult{}, false
	}
	if entry := m.entries[jobID]; entry != nil {
		if !entry.done {
			return snapshotExpandResult{}, false
		}
		delete(m.entries, jobID)
		return entry.result, true
	}
	if m.running >= snapshotExpandConcurrency {
		return snapshotExpandResult{}, false
	}
	ctx, cancel, capped, expired := snapshotExpansionContext(m.ctx, job)
	if expired {
		return snapshotExpandResult{}, false
	}
	if m.entries == nil {
		m.entries = make(map[int64]*snapshotExpandEntry)
	}
	entry := &snapshotExpandEntry{cancel: cancel}
	m.entries[jobID] = entry
	m.running++
	m.wg.Add(1)

	go func() {
		defer m.wg.Done()
		defer cancel()
		files, options, sources, err := expandSnapshotSource(ctx, m.cm, job)
		var res snapshotExpandResult
		drop := false
		if ctx.Err() != nil {
			if m.ctx.Err() == nil && capped && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				res.err = merr.WrapErrImportFailedMsg("snapshot expansion exceeded %s", snapshotExpansionTimeout)
			} else {
				// Leadership loss, prune, or job's own deadline: never publish
				// a canceled result; GC or the new leader owns the transition.
				drop = true
			}
		} else {
			res = snapshotExpandResult{files: files, options: options, sources: sources, err: err}
		}
		m.mu.Lock()
		m.running--
		if m.entries[jobID] == entry {
			if drop || m.closed {
				delete(m.entries, jobID)
			} else {
				entry.cancel = nil
				entry.done = true
				entry.result = res
			}
		}
		m.mu.Unlock()
	}()
	return snapshotExpandResult{}, false
}

// Prune cancels in-flight expansions and drops cached results for jobs that
// are no longer active SnapshotPreparing jobs. active is invoked without
// holding m.mu so callers may safely read importMeta.
func (m *SnapshotExpandManager) Prune(active func(jobID int64) bool) {
	m.mu.Lock()
	if len(m.entries) == 0 {
		m.mu.Unlock()
		return
	}
	ids := make([]int64, 0, len(m.entries))
	for id := range m.entries {
		ids = append(ids, id)
	}
	m.mu.Unlock()

	var inactive []int64
	for _, id := range ids {
		if !active(id) {
			inactive = append(inactive, id)
		}
	}
	if len(inactive) == 0 {
		return
	}
	m.mu.Lock()
	for _, id := range inactive {
		if entry := m.entries[id]; entry != nil {
			if entry.cancel != nil {
				entry.cancel()
			}
			delete(m.entries, id)
		}
	}
	m.mu.Unlock()
}

// Close cancels every in-flight attempt, drops cached results, and waits for
// all workers to exit so a leadership loss does not leave source I/O running.
func (m *SnapshotExpandManager) Close() {
	m.mu.Lock()
	m.closed = true
	for id, entry := range m.entries {
		if entry.cancel != nil {
			entry.cancel()
		}
		delete(m.entries, id)
	}
	m.mu.Unlock()
	m.wg.Wait()
}

func (m *SnapshotExpandManager) wait() {
	m.wg.Wait()
}
