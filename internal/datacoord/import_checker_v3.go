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
// the state machine in import_v3_handler.go and the import_v3_handler_<state>.go
// files (with the generic helpers in import_v3_job_context.go), the preimport
// stage in import_v3_preimport.go, the reshard and planning stages in
// import_v3_reshard.go and import_v3_planning.go, the terminal-job GC in
// import_v3_gc.go, and the job-to-plan derivation in import_v3_plan_factory.go.

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
// (ImportJob.version == ImportVersionV3) and never touches the legacy
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
	return job.GetVersion() == internalpb.ImportVersion_ImportVersionV3
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
			for _, job := range c.importMeta.GetJobBy(c.ctx, WithJobVersions(internalpb.ImportVersion_ImportVersionV3)) {
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
	handler := handlerFor(job.GetState())
	if handler == nil {
		// Completed and None have no per-tick state work; the GC loop owns
		// timeout and cleanup. Return silently instead of warning every tick
		// for the whole retention window.
		return
	}
	jc := newImportV3JobContext(c, job)
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
			jobs := c.importMeta.GetJobBy(c.ctx, WithJobVersions(internalpb.ImportVersion_ImportVersionV3))
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
			c.LogJobStats(jobs)
			c.LogTaskStats()
		}
	}
}

func (c *importCheckerV3) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

// checkGC runs one GC tick for a job. The pipeline itself lives in import_v3_gc.go.
func (c *importCheckerV3) checkGC(job ImportJob) {
	collectImportV3JobGC(newImportV3JobContext(c, job))
}

// LogJobStats reports the V3 job counts by state. The checker loads only V3
// jobs (see WithJobVersions), so it only sets the V3 gauge; the legacy checker
// reports V1.
func (c *importCheckerV3) LogJobStats(jobs []ImportJob) {
	stateNum := make(map[string]int)
	byState := lo.GroupBy(jobs, func(job ImportJob) string { return job.GetState().String() })
	for state := range internalpb.ImportJobState_value {
		if state == internalpb.ImportJobState_None.String() {
			continue
		}
		num := len(byState[state])
		stateNum[state] = num
		importV3Stats{}.setJobState(state, internalpb.ImportVersion_ImportVersionV3, num)
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
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(PreImportTaskV3Type))
	logFunc(tasks, PreImportTaskV3Type)
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
		_ = ImportV3JobTransitTo(c.ctx, c.importMeta, job, internalpb.ImportJobState_Failed, UpdateJobReason("import timeout"))
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
			_ = ImportV3JobTransitTo(c.ctx, c.importMeta, job, internalpb.ImportJobState_Failed, UpdateJobReason(fmt.Sprintf("collection %d dropped", collectionID)))
		}
	}
}
