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

// importCheckerV3 is only the orchestrator: it owns the two checker loops and
// the per-tick dispatch. Everything a tick does to one job lives elsewhere:
// the state machine in import_v3_state.go (state handlers, preimport stage,
// GC pipeline), the two planning stages in import_v3_planner.go, and the
// job-to-plan derivation in import_v3_plan_factory.go.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// importCheckerV3 is the ImportTaskV3 state machine. It only owns V3 jobs
// (ImportJob.version == ImportJobVersionV3) and never touches the legacy
// PreImportTask/ImportTaskV2 path, which remains fully owned by importChecker.
type importCheckerV3 struct {
	ctx        context.Context
	meta       *meta
	broker     broker.Broker
	alloc      allocator.Allocator
	importMeta ImportMeta
	cluster    session.Cluster

	hooks importCheckerHooks

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportCheckerV3(ctx context.Context,
	meta *meta,
	broker broker.Broker,
	alloc allocator.Allocator,
	importMeta ImportMeta,
	cluster session.Cluster,
	hooks importCheckerHooks,
) ImportChecker {
	return &importCheckerV3{
		ctx:        ctx,
		meta:       meta,
		broker:     broker,
		alloc:      alloc,
		importMeta: importMeta,
		cluster:    cluster,
		hooks:      hooks,
		closeChan:  make(chan struct{}),
	}
}

// isV3Job reports whether a job belongs to the Import V3 path. V3 and the
// legacy V1/V2 path are driven by two separate checkers that never touch each
// other's jobs.
func isV3Job(job ImportJob) bool {
	return job.GetVersion() == datapb.ImportJobVersion_ImportJobVersionV3
}

// v3Jobs returns every V3 job. It is the single version filter both checker
// loops use, so a new loop cannot forget it or write it the other way around.
func (c *importCheckerV3) v3Jobs() []ImportJob {
	return lo.Filter(c.importMeta.GetJobBy(c.ctx), func(job ImportJob, _ int) bool {
		return isV3Job(job)
	})
}

// Start runs the checker loops until Close. The state-machine loop and the
// timeout/GC loop deliberately run on separate goroutines: checkGC's rollback
// broadcast can park on the ctx-insensitive resource-key lock (see checkGC), and
// isolating it guarantees the state machine keeps making progress no matter how
// long GC blocks. All state shared by the two loops lives behind importMeta's
// mutex (which already serves concurrent RPC and ack-callback goroutines), and
// UpdateJob refuses transitions out of Completed/Failed, so the loops cannot
// resurrect or regress each other's terminal states.
func (c *importCheckerV3) Start() {
	mlog.Info(c.ctx, "start import checker v3")
	go c.runGCLoop()
	c.runStateMachineLoop()
}

func (c *importCheckerV3) runStateMachineLoop() {
	ticker := time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalHigh.GetAsDuration(time.Second)) // 2s
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			mlog.Info(c.ctx, "import checker v3 state-machine loop exited")
			return
		case <-ticker.C:
			for _, job := range c.v3Jobs() {
				if !funcutil.SliceSetEqual[string](job.GetVchannels(), job.GetReadyVchannels()) {
					// wait for all channels to send signals
					mlog.Info(c.ctx, "waiting for all channels to send signals",
						mlog.Strings("vchannels", job.GetVchannels()),
						mlog.Strings("readyVchannels", job.GetReadyVchannels()),
						mlog.FieldJobID(job.GetJobID()))
					continue
				}
				c.checkJob(job)
			}
		}
	}
}

// checkJob runs one state-machine tick for a single job: it resolves the
// state handler, runs it, and applies the shared error policy. A terminal
// error fails the job with the error as the reason; anything else is logged
// and retried on the next tick.
func (c *importCheckerV3) checkJob(job ImportJob) {
	jc := newImportV3JobContext(c, job)
	handler := handlerFor(job.GetState())
	if handler == nil {
		jc.log.Warn(jc.ctx, "no state handler registered for import v3 job state, skipping",
			mlog.String("state", job.GetState().String()))
		return
	}
	if err := handler.Handle(jc); err != nil {
		jc.log.Warn(jc.ctx, "import v3 job state handle failed", mlog.String("state", job.GetState().String()), mlog.Err(err))
		if isTerminalImportV3JobErr(err) {
			jc.failJob(err.Error())
		}
	}
}

// isTerminalImportV3JobErr decides whether a state-handler error fails the job
// immediately instead of retrying on the next tick. It is the shared denylist
// plus disk-quota exhaustion, matching the V2 checker (import_checker.go): the
// quota verdict is not input the user can fix by resubmitting the same
// request, and a silent retry loop would re-read every reshard manifest on
// each 2s tick while the job reports a fixed 40% with an empty reason. The
// Planning state is the only quota consumer, so folding it into the shared
// policy keeps every state's rule identical.
func isTerminalImportV3JobErr(err error) bool {
	return importcommon.IsTerminalImportV3Err(err) || errors.Is(err, merr.ErrServiceQuotaExceeded)
}

func (c *importCheckerV3) runGCLoop() {
	ticker := time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalLow.GetAsDuration(time.Second)) // 2min
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			mlog.Info(c.ctx, "import checker v3 gc loop exited")
			return
		case <-ticker.C:
			allJobs := c.importMeta.GetJobBy(c.ctx)
			jobs := lo.Filter(allJobs, func(job ImportJob, _ int) bool { return isV3Job(job) })
			for _, job := range jobs {
				c.tryTimeoutJob(job)
				c.checkGC(job)
			}
			jobsByColl := lo.GroupBy(jobs, func(job ImportJob) int64 {
				return job.GetCollectionID()
			})
			for collID, collJobs := range jobsByColl {
				c.checkCollection(collID, collJobs)
			}
			c.LogJobStats(allJobs)
			c.LogTaskStats()
		}
	}
}

func (c *importCheckerV3) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

// checkGC runs one GC tick for a job. The pipeline itself lives in importV3JobGC.
func (c *importCheckerV3) checkGC(job ImportJob) {
	newImportV3JobGC(newImportV3JobContext(c, job)).collect()
}

func (c *importCheckerV3) LogJobStats(jobs []ImportJob) {
	stateNum := make(map[string]int)
	for _, version := range []datapb.ImportJobVersion{
		datapb.ImportJobVersion_ImportJobVersionV1,
		datapb.ImportJobVersion_ImportJobVersionV3,
	} {
		versionJobs := lo.Filter(jobs, func(job ImportJob, _ int) bool { return job.GetVersion() == version })
		byState := lo.GroupBy(versionJobs, func(job ImportJob) string { return job.GetState().String() })
		for state := range internalpb.ImportJobState_value {
			if state == internalpb.ImportJobState_None.String() {
				continue
			}
			num := len(byState[state])
			stateNum[state] += num
			importV3Stats{}.setJobState(state, version, num)
		}
	}
	mlog.Info(c.ctx, "import job stats", mlog.Any("stateNum", stateNum))
}

func (c *importCheckerV3) LogTaskStats() {
	stats := importV3Stats{}
	logFunc := func(tasks []ImportTask, taskType TaskType) {
		byState := lo.GroupBy(tasks, func(t ImportTask) datapb.ImportTaskStateV2 {
			return t.GetState()
		})
		pending := len(byState[datapb.ImportTaskStateV2_Pending])
		inProgress := len(byState[datapb.ImportTaskStateV2_InProgress])
		completed := len(byState[datapb.ImportTaskStateV2_Completed])
		failed := len(byState[datapb.ImportTaskStateV2_Failed])
		mlog.Info(c.ctx, "import task stats", mlog.String("type", taskType.String()),
			mlog.Int("pending", pending), mlog.Int("inProgress", inProgress),
			mlog.Int("completed", completed), mlog.Int("failed", failed))
		stats.setTaskCount(taskType, datapb.ImportTaskStateV2_Pending, pending)
		stats.setTaskCount(taskType, datapb.ImportTaskStateV2_InProgress, inProgress)
		stats.setTaskCount(taskType, datapb.ImportTaskStateV2_Completed, completed)
		stats.setTaskCount(taskType, datapb.ImportTaskStateV2_Failed, failed)
	}
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(PreImportV2TaskType))
	logFunc(tasks, PreImportV2TaskType)
	tasks = c.importMeta.GetTaskBy(c.ctx, WithType(ReshardTaskType))
	logFunc(tasks, ReshardTaskType)
	tasks = c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskV3Type))
	logFunc(tasks, ImportTaskV3Type)
}

func (c *importCheckerV3) tryTimeoutJob(job ImportJob) {
	if job.GetState() == internalpb.ImportJobState_Failed ||
		job.GetState() == internalpb.ImportJobState_Completed ||
		job.GetState() == internalpb.ImportJobState_Committing {
		return
	}
	timeoutTime := tsoutil.PhysicalTime(job.GetTimeoutTs())
	if time.Now().After(timeoutTime) {
		mlog.Warn(c.ctx, "Import timeout, expired the specified time limit",
			mlog.FieldJobID(job.GetJobID()), mlog.Time("timeoutTime", timeoutTime))
		err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
			UpdateJobReason("import timeout"))
		if err != nil {
			mlog.Warn(c.ctx, "failed to update job state to Failed", mlog.FieldJobID(job.GetJobID()), mlog.Err(err))
		}
	}
}

func (c *importCheckerV3) checkCollection(collectionID int64, jobs []ImportJob) {
	if len(jobs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	has, err := c.broker.HasCollection(ctx, collectionID)
	if err != nil {
		mlog.Warn(c.ctx, "verify existence of collection failed", mlog.Int64("collection", collectionID), mlog.Err(err))
		return
	}
	if !has {
		jobs = lo.Filter(jobs, func(job ImportJob, _ int) bool {
			return job.GetState() != internalpb.ImportJobState_Failed &&
				job.GetState() != internalpb.ImportJobState_Completed &&
				job.GetState() != internalpb.ImportJobState_Committing
		})
		for _, job := range jobs {
			err = c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
				UpdateJobReason(fmt.Sprintf("collection %d dropped", collectionID)))
			if err != nil {
				mlog.Warn(c.ctx, "failed to update job state to Failed", mlog.FieldJobID(job.GetJobID()), mlog.Err(err))
			}
		}
	}
}
