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
	Start()
	Close()
}

type importInspector struct {
	ctx        context.Context
	meta       *meta
	alloc      allocator.Allocator
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
	jobs := s.importMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobID() < jobs[j].GetJobID()
	})
	for _, job := range jobs {
		tasks := s.importMeta.GetTaskBy(s.ctx, WithJob(job.GetJobID()))
		for _, task := range tasks {
			if task.GetState() == datapb.ImportTaskStateV2_InProgress {
				s.scheduler.Enqueue(task)
			}
		}
	}
}

func (s *importInspector) inspect() {
	jobs := s.importMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobID() < jobs[j].GetJobID()
	})
	for _, job := range jobs {
		tasks := s.importMeta.GetTaskBy(s.ctx, WithJob(job.GetJobID()))
		for _, task := range tasks {
			switch task.GetState() {
			case datapb.ImportTaskStateV2_Pending:
				switch task.GetType() {
				case PreImportTaskType:
					s.processPendingPreImport(task)
				case ImportTaskType:
					s.processPendingImport(task)
				}
			case datapb.ImportTaskStateV2_Failed:
				s.processFailed(task)
			}
		}
	}
}

func (s *importInspector) processPendingPreImport(task ImportTask) {
	s.scheduler.Enqueue(task)
}

func (s *importInspector) processPendingImport(task ImportTask) {
	s.scheduler.Enqueue(task)
}

func (s *importInspector) processFailed(task ImportTask) {
	if task.GetType() == ImportTaskType {
		originSegmentIDs := task.(*importTask).GetSegmentIDs()
		statsSegmentIDs := task.(*importTask).GetSortedSegmentIDs()
		segments := append(originSegmentIDs, statsSegmentIDs...)
		for _, segment := range segments {
			mutations := map[int64][]MutateFunc{
				segment: {func(seg *datapb.SegmentInfo) bool {
					seg.State = commonpb.SegmentState_Dropped
					seg.DroppedAt = uint64(time.Now().UnixNano())
					return true
				}},
			}
			err := s.meta.UpdateSegmentsInfo(s.ctx, mutations)
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
