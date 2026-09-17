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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

const (
	NullNodeID = -1
)

type ImportInspector interface {
	Start()
	Close()
}

type importInspector struct {
	ctx        context.Context
	meta       *meta
	importMeta ImportMeta
	scheduler  task.GlobalScheduler

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportInspector(ctx context.Context, meta *meta, importMeta ImportMeta, scheduler task.GlobalScheduler) ImportInspector {
	return &importInspector{
		ctx:        ctx,
		meta:       meta,
		importMeta: importMeta,
		scheduler:  scheduler,
		closeChan:  make(chan struct{}),
	}
}

func (s *importInspector) Start() {
	s.reloadFromMeta()
	mlog.Info(s.ctx, "start import inspector")
	ticker := time.NewTicker(Params.DataCoordCfg.ImportScheduleInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-s.closeChan:
			mlog.Info(s.ctx, "import inspector exited")
			return
		case <-ticker.C:
			s.inspect()
		}
	}
}

func (s *importInspector) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}

func (s *importInspector) reloadFromMeta() {
	tasks := s.importMeta.GetTaskBy(s.ctx, WithStates(
		datapb.ImportTaskStateV2_Pending,
		datapb.ImportTaskStateV2_InProgress,
	))
	sortImportTasks(tasks)
	for _, task := range tasks {
		job := s.importMeta.GetJob(s.ctx, task.GetJobID())
		if job == nil ||
			job.GetState() == internalpb.ImportJobState_Failed ||
			job.GetState() == internalpb.ImportJobState_Completed {
			continue
		}
		if task.GetState() == datapb.ImportTaskStateV2_Pending ||
			(task.GetState() == datapb.ImportTaskStateV2_InProgress && task.GetNodeID() != NullNodeID) {
			s.scheduler.Enqueue(task)
		}
	}
}

func (s *importInspector) inspect() {
	tasks := s.importMeta.GetTaskBy(s.ctx, WithStates(
		datapb.ImportTaskStateV2_Pending,
		datapb.ImportTaskStateV2_Failed,
		datapb.ImportTaskStateV2_Retry,
		datapb.ImportTaskStateV2_InProgress,
	))
	sortImportTasks(tasks)
	for _, task := range tasks {
		job := s.importMeta.GetJob(s.ctx, task.GetJobID())
		if job == nil {
			continue
		}
		if job.GetState() == internalpb.ImportJobState_Failed ||
			job.GetState() == internalpb.ImportJobState_Completed {
			if task.GetState() == datapb.ImportTaskStateV2_Failed {
				s.processFailed(task)
			}
			continue
		}
		switch task.GetState() {
		case datapb.ImportTaskStateV2_Pending:
			s.scheduler.Enqueue(task)
		case datapb.ImportTaskStateV2_Failed:
			s.processFailed(task)
		case datapb.ImportTaskStateV2_Retry:
			s.processRetry(task, job)
		case datapb.ImportTaskStateV2_InProgress:
			// Normally already owned by the scheduler. Re-offering it is
			// idempotent and recovers a wrapper that was released locally after
			// a failed catalog write.
			if task.GetNodeID() == NullNodeID {
				// Compatibility recovery for records written by an older binary,
				// which persisted worker release before Retry in two catalog writes.
				if err := s.importMeta.UpdateTask(s.ctx, task.GetTaskID(),
					UpdateState(datapb.ImportTaskStateV2_Retry)); err != nil {
					mlog.Warn(s.ctx, "failed to recover unassigned import retry",
						WrapTaskLog(task, mlog.Err(err))...)
				}
				continue
			}
			s.scheduler.Enqueue(task)
		}
	}
}

func sortImportTasks(tasks []ImportTask) {
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].GetJobID() == tasks[j].GetJobID() {
			return tasks[i].GetTaskID() < tasks[j].GetTaskID()
		}
		return tasks[i].GetJobID() < tasks[j].GetJobID()
	})
}

// processRetry owns both the retry interval and the transition back to a
// schedulable attempt. Both task types rotate task identity before enqueueing a
// replacement; Import additionally rotates its output segment identities.
func (s *importInspector) processRetry(task ImportTask, job ImportJob) {
	if s.failJobIfAttemptLimitReached(task, job) {
		return
	}
	switch task.GetType() {
	case PreImportTaskType:
		concrete, ok := task.(*preImportTask)
		if !ok {
			mlog.Warn(s.ctx, "cannot replace preimport retry task with unknown implementation",
				WrapTaskLog(task)...)
			return
		}
		replacement, err := replacePreImportTaskForRetry(s.ctx, task, concrete.alloc, s.importMeta)
		if err != nil {
			mlog.Warn(s.ctx, "failed to replace preimport retry task", WrapTaskLog(task, mlog.Err(err))...)
			return
		}
		if replacement != nil {
			s.scheduler.Enqueue(replacement)
		}
	case ImportTaskType:
		concrete, ok := task.(*importTask)
		if !ok {
			mlog.Warn(s.ctx, "cannot replace import retry task with unknown implementation",
				WrapTaskLog(task)...)
			return
		}
		replacement, err := replaceImportTaskForRetry(s.ctx, task, job,
			concrete.alloc, s.meta, s.importMeta)
		if err != nil {
			mlog.Warn(s.ctx, "failed to replace import retry task", WrapTaskLog(task, mlog.Err(err))...)
			return
		}
		if replacement != nil {
			s.scheduler.Enqueue(replacement)
		}
	}
}

// failJobIfAttemptLimitReached persists the terminal job decision once the
// logical task lineage has consumed its configured attempt budget. The checker
// owns the subsequent task failure/cleanup; a failed catalog write leaves the
// Retry record intact, and the next inspector tick tries this decision again.
func (s *importInspector) failJobIfAttemptLimitReached(task ImportTask, job ImportJob) bool {
	maxAttempts := Params.DataCoordCfg.ImportMaxAttempts.GetAsInt64()
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if task.GetTaskVersion()+1 < maxAttempts {
		return false
	}

	reason := fmt.Sprintf("import task reached attempt limit (%d)", maxAttempts)
	if task.GetReason() != "" {
		reason = fmt.Sprintf("%s; %s", task.GetReason(), reason)
	}
	if err := s.importMeta.UpdateJob(s.ctx, job.GetJobID(),
		UpdateJobState(internalpb.ImportJobState_Failed),
		UpdateJobReason(reason)); err != nil {
		mlog.Warn(s.ctx, "failed to settle import job after attempt cap", WrapTaskLog(task, mlog.Err(err))...)
	}
	return true
}

func (s *importInspector) processFailed(task ImportTask) {
	if task.GetType() == ImportTaskType {
		originSegmentIDs := task.(*importTask).GetSegmentIDs()
		legacySortedSegmentIDs := task.(*importTask).GetSortedSegmentIDs()
		for _, segment := range originSegmentIDs {
			// is_importing is cleared with the drop: the garbage collector skips
			// is_importing segments to protect the in-flight commit marker, so a
			// failed task's dropped inventory would otherwise leak forever. The
			// sort-skip marker is not at risk here -- it only matters to a live
			// job's validation, and this task has already terminated.
			op := UpdateStatusOperator(segment, commonpb.SegmentState_Dropped)
			err := s.meta.UpdateSegmentsInfo(s.ctx, op, UpdateIsImporting(segment, false))
			if err != nil {
				mlog.Warn(s.ctx, "drop import segment failed", WrapTaskLog(task, mlog.Int64("segment", segment), mlog.Err(err))...)
				return
			}
		}

		// Re-read sorted outputs only after every origin is dropped. Sort
		// completion checks the origin under the same segment lock: it either
		// finished first and is visible here, or it now rejects the dropped
		// origin and cannot publish a new output after this read.
		for _, originID := range originSegmentIDs {
			outputs, _ := s.meta.GetCompactionTo(originID)
			for _, output := range outputs {
				segment := output.GetID()
				op := UpdateStatusOperator(segment, commonpb.SegmentState_Dropped)
				err := s.meta.UpdateSegmentsInfo(s.ctx, op, UpdateIsImporting(segment, false))
				if err != nil {
					mlog.Warn(s.ctx, "drop sorted import segment failed", WrapTaskLog(task, mlog.Int64("segment", segment), mlog.Err(err))...)
					return
				}
			}
		}
		if len(originSegmentIDs) > 0 || len(legacySortedSegmentIDs) > 0 {
			err := s.importMeta.UpdateTask(s.ctx, task.GetTaskID(),
				UpdateSegmentIDs(nil), UpdateStatsSegmentIDs(nil))
			if err != nil {
				mlog.Warn(s.ctx, "update import task segments failed", WrapTaskLog(task, mlog.Err(err))...)
			}
		}
	}
}
