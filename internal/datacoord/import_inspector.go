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
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

const (
	NullNodeID = -1
)

type ImportInspector interface {
	Reload()
	Start()
	Close()
}

type importInspector struct {
	ctx        context.Context
	meta       *meta
	alloc      allocator.Allocator
	importMeta ImportMeta
	scheduler  task.GlobalScheduler
	copyMeta   CopySegmentMeta

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportInspector(ctx context.Context, meta *meta, importMeta ImportMeta, scheduler task.GlobalScheduler, copyMeta CopySegmentMeta) ImportInspector {
	return &importInspector{
		ctx:        ctx,
		meta:       meta,
		importMeta: importMeta,
		scheduler:  scheduler,
		copyMeta:   copyMeta,
		closeChan:  make(chan struct{}),
	}
}

func (s *importInspector) Start() {
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

// Reload performs the one-shot startup reconciliation and re-enqueue. The
// server runs it synchronously before the checkers start and before the state
// turns Healthy, so the orphan scan cannot race producers that persist a
// segment before its owner record (snapshot restore pre-registration and V2
// import segment allocation).
func (s *importInspector) Reload() {
	// Reconcile before enqueuing in-progress tasks: a queued task can race
	// into prepareRetry and allocate a new segment while the reconciler is
	// still scanning; cleaning first keeps the startup scan consistent with
	// the task table it observes.
	s.reconcileOrphanImportSegments()
	tasks := s.importMeta.GetTaskBy(s.ctx, WithStates(datapb.ImportTaskStateV2_InProgress))
	for _, task := range tasks {
		if s.importMeta.GetJob(s.ctx, task.GetJobID()) != nil {
			s.scheduler.Enqueue(task)
		}
	}
}

// reconcileOrphanImportSegments drops Importing segments that no import task
// references. A V2 import segment is persisted before the task that owns it, so
// a DataCoord crash between the two catalog writes leaves an IsImporting segment
// with no owner; the import terminal GC only drops task.SegmentId and the
// generic GC only collects Dropped segments, so without this pass such segments
// would leak forever. Every legitimate Importing segment is referenced by some
// task, so an unreferenced one is a leak. Import V3 no longer provisions an
// unowned segment (it creates its segment at dispatch, for a task that already
// carries its id), but its current segment is still counted as referenced so
// this pass can never sweep an in-flight V3 segment.
func (s *importInspector) reconcileOrphanImportSegments() {
	referenced := make(map[int64]struct{})
	if s.copyMeta != nil {
		// A persisted copy job is the durable owner of its pre-registered
		// target segments, even before copySegmentChecker creates the first
		// copy task. Include job-level mappings so a restart in that window
		// does not sweep them as import orphans.
		for _, job := range s.copyMeta.GetJobBy(s.ctx) {
			for _, mapping := range job.GetIdMappings() {
				referenced[mapping.GetTargetSegmentId()] = struct{}{}
			}
		}
		for _, task := range s.copyMeta.GetTaskBy(s.ctx) {
			for _, mapping := range task.GetIdMappings() {
				referenced[mapping.GetTargetSegmentId()] = struct{}{}
			}
		}
	}
	for _, task := range s.importMeta.GetTaskBy(s.ctx) {
		switch t := task.(type) {
		case *importTask:
			// V2 provisions preallocated segments before the range/regrouping
			// task references them, so this pass is what reclaims an orphan.
			for _, id := range t.GetSegmentIDs() {
				referenced[id] = struct{}{}
			}
			for _, id := range t.GetSortedSegmentIDs() {
				referenced[id] = struct{}{}
			}
		case *importTaskV3:
			// V3 creates its segment at dispatch, for a task that already
			// references it, so it never orphans one. It is still listed here so
			// this pass can never sweep an in-flight V3 segment. A superseded
			// segment lives in old_segment_ids (deliberately not referenced) and
			// is dropped by the next dispatch -- or by this pass if DataCoord
			// restarted first.
			if segmentID := t.task.Load().GetSegmentId(); segmentID != 0 {
				referenced[segmentID] = struct{}{}
			}
		}
	}
	var orphanIDs []int64
	for _, segment := range s.meta.GetAllSegmentsUnsafe() {
		if !segment.GetIsImporting() {
			continue
		}
		// Dropped segments are already owned by the generic GC; re-dropping them
		// here would reset DroppedAt and keep them out of GC indefinitely.
		if segment.GetState() == commonpb.SegmentState_Dropped {
			continue
		}
		if _, ok := referenced[segment.GetID()]; ok {
			continue
		}
		orphanIDs = append(orphanIDs, segment.GetID())
	}
	if len(orphanIDs) == 0 {
		return
	}
	mlog.Warn(s.ctx, "reconcile orphan import segments on restart", mlog.Int64s("segmentIDs", orphanIDs))
	if err := s.meta.UpdateSegmentsInfo(s.ctx, dropImportV3Segments(orphanIDs)); err != nil {
		mlog.Warn(s.ctx, "failed to drop orphan import segments", mlog.Int64s("segmentIDs", orphanIDs), mlog.Err(err))
	}
}

func (s *importInspector) inspect() {
	jobs := s.importMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobID() < jobs[j].GetJobID()
	})
	for _, job := range jobs {
		tasks := s.importMeta.GetTaskByJob(s.ctx, job.GetJobID())
		for _, task := range tasks {
			switch task.GetState() {
			case datapb.ImportTaskStateV2_Pending:
				switch task.GetType() {
				case ReshardTaskType:
					// A ReshardTask is persisted just before the job advances to
					// Resharding (createReshardTasks). Enqueueing it
					// earlier would race that transition; skip it until the job
					// can accept the dispatch.
					if reshardDispatchable(job.GetState()) {
						s.processPendingTask(task)
					}
				case PreImportTaskType, ImportTaskType, ImportTaskV3Type, PreImportTaskV3Type:
					s.processPendingTask(task)
				}
			case datapb.ImportTaskStateV2_Failed:
				s.processFailed(task)
			}
		}
	}
}

func (s *importInspector) processPendingTask(task ImportTask) {
	s.scheduler.Enqueue(task)
}

func (s *importInspector) processFailed(task ImportTask) {
	if task.GetType() == ImportTaskType {
		originSegmentIDs := task.(*importTask).GetSegmentIDs()
		statsSegmentIDs := task.(*importTask).GetSortedSegmentIDs()
		segments := append(originSegmentIDs, statsSegmentIDs...)
		for _, segment := range segments {
			op := UpdateStatusOperator(segment, commonpb.SegmentState_Dropped)
			err := s.meta.UpdateSegmentsInfo(s.ctx, op)
			if err != nil {
				mlog.Warn(s.ctx, "drop import segment failed", WrapTaskLog(task, mlog.Int64("segment", segment), mlog.Err(err))...)
				return
			}
		}
		if len(segments) > 0 {
			err := s.importMeta.UpdateTask(s.ctx, task.GetTaskID(), UpdateSegmentIDs(nil), UpdateStatsSegmentIDs(nil))
			if err != nil {
				mlog.Warn(s.ctx, "update import task segments failed", WrapTaskLog(task, mlog.Err(err))...)
			}
		}
	}
}
