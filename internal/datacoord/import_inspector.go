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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

const (
	NullNodeID = -1
)

type ImportInspector interface {
	Start()
	Close()
}

type importInspector struct {
	meta      *meta
	alloc     allocator.Allocator
	imeta     ImportMeta
	scheduler task.GlobalScheduler

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportInspector(meta *meta, imeta ImportMeta, scheduler task.GlobalScheduler) ImportInspector {
	return &importInspector{
		meta:      meta,
		imeta:     imeta,
		scheduler: scheduler,
		closeChan: make(chan struct{}),
	}
}

func (s *importInspector) Start() {
	log.Ctx(context.TODO()).Info("start import inspector")
	ticker := time.NewTicker(Params.DataCoordCfg.ImportScheduleInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-s.closeChan:
			log.Ctx(context.TODO()).Info("import inspector exited")
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

func (s *importInspector) inspect() {
	jobs := s.imeta.GetJobBy(context.TODO())
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobID() < jobs[j].GetJobID()
	})
	for _, job := range jobs {
		tasks := s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()))
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
		statsSegmentIDs := task.(*importTask).GetStatsSegmentIDs()
		segments := append(originSegmentIDs, statsSegmentIDs...)
		for _, segment := range segments {
			op := UpdateStatusOperator(segment, commonpb.SegmentState_Dropped)
			err := s.meta.UpdateSegmentsInfo(context.TODO(), op)
			if err != nil {
				log.Ctx(context.TODO()).Warn("drop import segment failed", WrapTaskLog(task, zap.Int64("segment", segment), zap.Error(err))...)
				return
			}
		}
		if len(segments) > 0 {
			err := s.imeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateSegmentIDs(nil), UpdateStatsSegmentIDs(nil))
			if err != nil {
				log.Ctx(context.TODO()).Warn("update import task segments failed", WrapTaskLog(task, zap.Error(err))...)
			}
		}
	}
}
