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

package datacoord

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

// snapshotExportInspector is the sole producer of scheduler entries and owns
// cleanup retries. Persisted assignments recover cleanup after coordinator loss.
type snapshotExportInspector struct {
	manager   *snapshotExportManager
	startOnce sync.Once
	wakeCh    chan struct{}
	cleanups  map[int64]*snapshotExportCopyTask
	// Terminal jobs whose local callbacks and remote assignments have drained.
	// Rebuilt after restart; the checker removes entries after metadata GC.
	drained sync.Map
}

func (i *snapshotExportInspector) Start() {
	i.startOnce.Do(func() {
		i.manager.wg.Add(2)
		go i.run()
		go i.runCleanup()
	})
}

func (i *snapshotExportInspector) Wake() {
	select {
	case i.wakeCh <- struct{}{}:
	default:
	}
}

func (i *snapshotExportInspector) run() {
	defer i.manager.wg.Done()
	ticker := time.NewTicker(snapshotExportReconcileInterval)
	defer ticker.Stop()
	for {
		i.inspect()
		select {
		case <-i.manager.ctx.Done():
			return
		case <-i.wakeCh:
		case <-ticker.C:
		}
	}
}

// Admission never waits for worker removal or cleanup RPCs.
func (i *snapshotExportInspector) inspect() {
	m := i.manager
	if m.scheduler == nil {
		return
	}
	for _, job := range m.meta.GetJobs() {
		for _, record := range job.copyTasks {
			if m.ctx.Err() != nil {
				return
			}
			current, ok := m.meta.getJob(job.record.GetJobId(), record.GetTaskId())
			if !ok {
				continue
			}
			active := findExportTask(current, record.GetTaskId())
			if active == nil {
				continue
			}
			if current.record.GetCopyPlanComplete() && !exportTaskNeedsCleanup(current, active) {
				i.enqueue(current, active)
			}
		}
	}
}

func exportTaskNeedsCleanup(job *snapshotExportJobView, task *datapb.SnapshotExportCopyTask) bool {
	return isSnapshotExportTerminal(job.record.GetState()) || job.record.GetState() == datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing || isRetiredExportTask(job, task.GetTaskId()) || task.GetResult() != nil
}

func (i *snapshotExportInspector) runCleanup() {
	defer i.manager.wg.Done()
	ticker := time.NewTicker(snapshotExportReconcileInterval)
	defer ticker.Stop()
	for {
		i.inspectCleanup()
		select {
		case <-i.manager.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (i *snapshotExportInspector) inspectCleanup() {
	m := i.manager
	if m.scheduler == nil {
		return
	}
	jobs := m.meta.GetJobs()
	if len(i.cleanups) > 0 {
		// Another mutation may replay a pending deletion before this loop retries
		// it. Sweep absent records even when their job is drained or already GC'd.
		live := make(map[int64]struct{})
		for _, job := range jobs {
			for _, tasks := range [][]*datapb.SnapshotExportCopyTask{job.copyTasks, job.retiredTasks} {
				for _, task := range tasks {
					live[task.GetTaskId()] = struct{}{}
				}
			}
		}
		for id := range i.cleanups {
			if _, exists := live[id]; !exists {
				delete(i.cleanups, id)
			}
		}
	}
	for _, job := range jobs {
		if _, done := i.drained.Load(job.record.GetJobId()); done {
			continue
		}
		// Only a job already terminal at scan start can receive a drain confirmation:
		// otherwise a concurrently committed plan could add tasks outside this scan.
		terminal := isSnapshotExportTerminal(job.record.GetState())
		for _, tasks := range [][]*datapb.SnapshotExportCopyTask{job.copyTasks, job.retiredTasks} {
			for _, record := range tasks {
				if m.ctx.Err() != nil {
					return
				}
				current, ok := m.meta.getJob(job.record.GetJobId(), record.GetTaskId())
				if !ok {
					continue
				}
				active := findExportTask(current, record.GetTaskId())
				if active != nil && exportTaskNeedsCleanup(current, active) {
					i.cleanup(current, active)
				}
			}
		}
		if terminal {
			latest, ok := m.meta.GetJob(job.record.GetJobId())
			if !ok {
				continue
			}
			drained := len(latest.retiredTasks) == 0
			for _, record := range latest.copyTasks {
				drained = drained && record.GetNodeId() == 0
			}
			if drained {
				i.drained.Store(job.record.GetJobId(), struct{}{})
				// A job with no remaining task records may be GC'd without waiting for
				// this confirmation. Do not retain a marker if that GC won the race.
				if !m.meta.jobs.Contain(job.record.GetJobId()) {
					i.drained.Delete(job.record.GetJobId())
				}
			}
		}
	}
}

func (i *snapshotExportInspector) cleanup(job *snapshotExportJobView, active *datapb.SnapshotExportCopyTask) {
	m := i.manager
	id := active.GetTaskId()
	// Cannot drain an unknown in-flight dispatch.
	if m.scheduler == nil {
		return
	}
	// Removal waits for callbacks already executing and invalidates queued ones.
	// No worker RPC runs under the scheduler lock or needs a free slot.
	m.scheduler.RemoveTask(id)
	m.copyProgress.Delete(snapshotExportProgressKey{job.record.GetJobId(), id})
	pending := i.cleanups[id]
	if pending == nil {
		pending = &snapshotExportCopyTask{manager: m}
		i.cleanups[id] = pending
	}
	if !pending.ready() {
		return
	}
	// Refresh after draining Create/Query; an in-flight callback may have assigned
	// a worker before observing job termination.
	current, ok := m.meta.getJob(job.record.GetJobId(), id)
	if !ok {
		delete(i.cleanups, id)
		return
	}
	active = findExportTask(current, id)
	if active == nil || (active.GetNodeId() == 0 && !isRetiredExportTask(current, id)) || m.cleanupExportCopyTask(job.record.GetJobId(), active, m.cluster) {
		delete(i.cleanups, id)
	} else {
		pending.backoff()
	}
}

func (i *snapshotExportInspector) enqueue(job *snapshotExportJobView, task *datapb.SnapshotExportCopyTask) {
	m := i.manager
	// The scheduler retains the first task for this ID, including its hydrated
	// request. Retries must send identical input to the worker.
	req := snapshotExportExecutionRequest(job.record.GetCopyRequest())
	req.TaskId = task.GetTaskId()
	req.SegmentStart = task.GetSegmentStart()
	req.SegmentEnd = task.GetSegmentEnd()
	m.scheduler.Enqueue(&snapshotExportCopyTask{manager: m, request: req, times: make(map[taskcommon.TimeType]time.Time)})
}
