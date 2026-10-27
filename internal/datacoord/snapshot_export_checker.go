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
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// snapshotExportChecker advances jobs and reclaims metadata after writers drain.
// Object-store preparation and publication stay in the per-job executor.
type snapshotExportChecker struct{ manager *snapshotExportManager }

func (c *snapshotExportChecker) run() {
	m := c.manager
	defer m.wg.Done()
	ticker := time.NewTicker(snapshotExportReconcileInterval)
	defer ticker.Stop()
	c.check()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wakeCh:
			c.check()
		case <-ticker.C:
			c.check()
		}
	}
}

func (c *snapshotExportChecker) check() {
	m := c.manager
	jobs := m.meta.GetJobs()
	now := uint64(time.Now().UnixMilli())
	for _, job := range jobs {
		if isSnapshotExportTerminal(job.record.GetState()) {
			_, drained := m.inspector.drained.Load(job.record.GetJobId())
			if drained || len(job.copyTasks)+len(job.retiredTasks) == 0 {
				c.recycleJob(job, now)
			}
			continue
		}
		if job.record.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing &&
			job.record.GetDeadlineTime() != 0 && now >= job.record.GetDeadlineTime() {
			// Persist the timeout transition before canceling the worker. If the
			// worker has already entered Publishing, tryFailJob observes that state
			// under the job lock and leaves publication running.
			if m.tryFailJob(job.record.GetJobId(), "snapshot export job timed out") {
				m.cancelRunningJob(job.record.GetJobId())
			}
		}
	}

	for _, job := range m.meta.GetJobs() {
		if isSnapshotExportTerminal(job.record.GetState()) {
			continue
		}
		if job.record.GetState() != datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing &&
			job.record.GetDeadlineTime() != 0 && now >= job.record.GetDeadlineTime() {
			continue
		}
		m.tryStartJob(job.record.GetJobId())
	}
}

func (c *snapshotExportChecker) recycleJob(job *snapshotExportJobView, now uint64) {
	m := c.manager
	// Worker assignments are durable cleanup obligations, independent of slots.
	for _, tasks := range [][]*datapb.SnapshotExportCopyTask{job.copyTasks, job.retiredTasks} {
		for _, task := range tasks {
			if task.GetNodeId() != 0 {
				return
			}
		}
	}
	if job.record.GetExternalSpec() != "" {
		ctx, cancel := context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
		_, _, err := m.meta.UpdateJob(ctx, job.record.GetJobId(), func(latest *snapshotExportJobView) (bool, error) {
			if !isSnapshotExportTerminal(latest.record.GetState()) || latest.record.GetExternalSpec() == "" {
				return true, nil
			}
			latest.record.ExternalSpec = ""
			latest.record.CopyRequest = nil
			return false, nil
		})
		cancel()
		if err != nil {
			mlog.RatedWarn(m.ctx, 1, "failed to clear terminal snapshot export credentials",
				mlog.FieldJobID(job.record.GetJobId()),
				mlog.Err(err))
		}
		return
	}
	if job.record.GetPinId() != 0 {
		ctx, cancel := context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
		collID, snapshotName, remaining, err := m.snapshotManager.snapshotMeta.UnpinSnapshot(ctx, job.record.GetPinId())
		cancel()
		if err != nil {
			mlog.RatedWarn(m.ctx, 1, "failed to release terminal snapshot export pin",
				mlog.FieldJobID(job.record.GetJobId()),
				mlog.Int64("pinID", job.record.GetPinId()),
				mlog.Err(err))
			return
		}
		if snapshotName != "" {
			setSnapshotActivePinsGauge(collID, snapshotName, remaining)
		}
		ctx, cancel = context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
		_, _, err = m.meta.UpdateJob(ctx, job.record.GetJobId(), func(latest *snapshotExportJobView) (bool, error) {
			if latest.record.GetPinId() == 0 {
				return true, nil
			}
			latest.record.PinId = 0
			return false, nil
		})
		cancel()
		if err != nil {
			mlog.RatedWarn(m.ctx, 1, "failed to clear terminal snapshot export pin",
				mlog.FieldJobID(job.record.GetJobId()),
				mlog.Err(err))
		}
		return
	}

	retention := Params.DataCoordCfg.SnapshotExportJobRetention.GetAsDuration(time.Second)
	if job.record.GetEndTime() == 0 || now < job.record.GetEndTime()+uint64(retention.Milliseconds()) {
		return
	}
	ctx, cancel := context.WithTimeout(m.ctx, snapshotPinCleanupTimeout)
	defer cancel()
	if err := m.meta.DropJob(ctx, job.record.GetJobId()); err != nil {
		mlog.RatedWarn(m.ctx, 1, "failed to remove expired snapshot export job",
			mlog.FieldJobID(job.record.GetJobId()),
			mlog.Err(err))
	} else {
		m.inspector.drained.Delete(job.record.GetJobId())
	}
}
