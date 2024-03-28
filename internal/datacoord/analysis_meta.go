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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type analysisMeta struct {
	ctx     context.Context
	lock    sync.RWMutex
	catalog metastore.DataCoordCatalog

	// taskID -> analyzeStats
	// TODO: when to mark as dropped?
	tasks map[int64]*model.AnalysisTask
}

func newAnalysisMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*analysisMeta, error) {
	mt := &analysisMeta{
		ctx:     ctx,
		lock:    sync.RWMutex{},
		catalog: catalog,
		tasks:   make(map[int64]*model.AnalysisTask),
	}

	if err := mt.reloadFromKV(); err != nil {
		return nil, err
	}
	return mt, nil
}

func (m *analysisMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("analysisMeta-reloadFromKV")

	// load analysis stats
	analysisTasks, err := m.catalog.ListAnalysisTasks(m.ctx)
	if err != nil {
		log.Warn("analysisMeta reloadFromKV load analysis tasks failed", zap.Error(err))
		return err
	}

	for _, analysisTask := range analysisTasks {
		m.tasks[analysisTask.TaskID] = analysisTask
	}
	log.Info("analysisMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *analysisMeta) saveTask(newTask *model.AnalysisTask) error {
	if err := m.catalog.SaveAnalysisTask(m.ctx, newTask); err != nil {
		return err
	}
	m.tasks[newTask.TaskID] = newTask
	return nil
}

// checkTask is checking and prompting only when creating tasks.
// Please don't use it.
func (m *analysisMeta) checkTask(task *model.AnalysisTask) {
	if t := m.tasks[task.TaskID]; t != nil {
		log.Warn("task already exist with taskID", zap.Int64("taskID", task.TaskID),
			zap.Int64("collectionID", task.CollectionID), zap.Int64("partitionID", task.PartitionID))
	}

	for _, t := range m.tasks {
		if t.CollectionID == task.CollectionID && t.PartitionID == task.PartitionID &&
			t.State != commonpb.IndexState_Finished && t.State != commonpb.IndexState_Failed {
			log.Warn("there is already exist task with partition and it not finished",
				zap.Int64("taskID", task.TaskID),
				zap.Int64("collectionID", task.CollectionID), zap.Int64("partitionID", task.PartitionID))
			break
		}
	}
}

func (m *analysisMeta) GetTask(taskID int64) *model.AnalysisTask {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return model.CloneAnalysisTask(m.tasks[taskID])
}

func (m *analysisMeta) AddAnalysisTask(task *model.AnalysisTask) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.checkTask(task)
	log.Info("add analysis task", zap.Int64("taskID", task.TaskID),
		zap.Int64("collectionID", task.CollectionID), zap.Int64("partitionID", task.PartitionID))
	return m.saveTask(task)
}

func (m *analysisMeta) DropAnalysisTask(taskID int64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	log.Info("drop analysis task", zap.Int64("taskID", taskID))
	if err := m.catalog.DropAnalysisTask(m.ctx, taskID); err != nil {
		log.Warn("drop analysis task by catalog failed", zap.Int64("taskID", taskID),
			zap.Error(err))
		return err
	}

	delete(m.tasks, taskID)
	return nil
}

func (m *analysisMeta) UpdateVersion(taskID int64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("there is no task with taskID: %d", taskID)
	}

	cloneT := model.CloneAnalysisTask(t)
	cloneT.Version++
	log.Info("update task version", zap.Int64("taskID", taskID), zap.Int64("newVersion", cloneT.Version))
	return m.saveTask(cloneT)
}

func (m *analysisMeta) BuildingTask(taskID, nodeID int64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("there is no task with taskID: %d", taskID)
	}

	cloneT := model.CloneAnalysisTask(t)
	cloneT.NodeID = nodeID
	cloneT.State = commonpb.IndexState_InProgress
	log.Info("task will be building", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))

	return m.saveTask(cloneT)
}

func (m *analysisMeta) FinishTask(taskID int64, result *indexpb.AnalysisResult) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("there is no task with taskID: %d", taskID)
	}

	log.Info("finish task meta...", zap.Int64("taskID", taskID), zap.String("state", result.GetState().String()),
		zap.String("centroidsFile", result.GetCentroidsFile()),
		zap.Any("segmentOffsetMapping", result.GetSegmentOffsetMappingFiles()),
		zap.String("failReason", result.GetFailReason()))

	cloneT := model.CloneAnalysisTask(t)
	cloneT.State = result.GetState()
	cloneT.FailReason = result.GetFailReason()
	cloneT.CentroidsFile = result.GetCentroidsFile()
	cloneT.SegmentOffsetMappingFiles = result.GetSegmentOffsetMappingFiles()
	return m.saveTask(cloneT)
}

func (m *analysisMeta) GetAllTasks() map[int64]*model.AnalysisTask {
	m.lock.RLock()
	defer m.lock.RUnlock()

	tasks := make(map[int64]*model.AnalysisTask)
	for taskID, t := range m.tasks {
		tasks[taskID] = model.CloneAnalysisTask(t)
	}
	return tasks
}
