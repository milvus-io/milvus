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

package indexcoord

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/tso"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type task interface {
	Ctx() context.Context
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Name() string       // set task name
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error) // notify if task is terminated
	OnEnqueue() error
}

// BaseTask is an basic instance of task.
type BaseTask struct {
	done  chan error
	ctx   context.Context
	id    UniqueID
	table *metaTable
}

// ID returns the id of index task.
func (bt *BaseTask) ID() UniqueID {
	return bt.id
}

func (bt *BaseTask) setID(id UniqueID) {
	bt.id = id
}

// WaitToFinish will wait for the task to complete, if the context is done,
// it means that the execution of the task has timed out.
func (bt *BaseTask) WaitToFinish() error {
	select {
	case <-bt.ctx.Done():
		return errors.New("Task wait to finished timeout")
	case err := <-bt.done:
		return err
	}
}

// Notify will notify WaitToFinish that the task is completed or failed.
func (bt *BaseTask) Notify(err error) {
	bt.done <- err
}

// CreateIndexTask is used to create an index on field.
type CreateIndexTask struct {
	BaseTask
	dataCoordClient  types.DataCoord
	indexCoordClient *IndexCoord
	req              *indexpb.BuildIndexRequest
	indexID          UniqueID
	createTs         uint64
	idAllocator      *allocator.GlobalIDAllocator
	tsoAllocator     *tso.GlobalTSOAllocator
}

// Ctx returns the context of the index task.
func (cit *CreateIndexTask) Ctx() context.Context {
	return cit.ctx
}

// ID returns the id of the index task.
func (cit *CreateIndexTask) ID() UniqueID {
	return cit.id
}

// SetID sets the id for index tasks.
func (cit *CreateIndexTask) SetID(ID UniqueID) {
	cit.BaseTask.setID(ID)
}

// Name returns the task name.
func (cit *CreateIndexTask) Name() string {
	return CreateIndexTaskName
}

// OnEnqueue assigns the indexBuildID to index task.
func (cit *CreateIndexTask) OnEnqueue() error {
	var err error
	cit.indexID, err = cit.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	cit.createTs, err = cit.tsoAllocator.AllocOne()
	if err != nil {
		return err
	}
	return nil
}

// PreExecute do nothing.
func (cit *CreateIndexTask) PreExecute(ctx context.Context) error {
	log.Info("IndexCoord CreateIndexTask PreExecute", zap.Int64("collectionID", cit.req.CollectionID),
		zap.Int64("fieldID", cit.req.FieldID), zap.String("indexName", cit.req.IndexName))
	return nil
}

// Execute adds the index task to meta table.
func (cit *CreateIndexTask) Execute(ctx context.Context) error {
	log.Info("IndexCoord CreateIndexTask Execute", zap.Int64("collectionID", cit.req.CollectionID),
		zap.Int64("fieldID", cit.req.FieldID), zap.String("indexName", cit.req.IndexName))
	cit.table.SetIndex(cit.indexID, cit.req, cit.createTs)
	// Get flushed segments
	flushedSegments, err := cit.dataCoordClient.GetFlushedSegments(cit.ctx, &datapb.GetFlushedSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   0,
			MsgID:     cit.indexID,
			Timestamp: cit.createTs,
			SourceID:  cit.indexCoordClient.serverID,
		},
		CollectionID: cit.req.CollectionID,
		PartitionID:  -1,
	})
	if err != nil {
		log.Error("IndexCoord get flushed segments from datacoord fail", zap.Int64("collectionID", cit.req.CollectionID),
			zap.Int64("fieldID", cit.req.FieldID), zap.String("indexName", cit.req.IndexName), zap.Error(err))
		return err
	}

	segmentsInfo, err := cit.dataCoordClient.GetSegmentInfo(cit.ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs:       flushedSegments.Segments,
		IncludeUnHealthy: true,
	})
	for _, segmentInfo := range segmentsInfo.Infos {
		binLogs := make([]string, 0)
		for _, fieldBinLog := range segmentInfo.GetBinlogs() {
			if fieldBinLog.GetFieldID() == cit.req.FieldID {
				for _, binLog := range fieldBinLog.GetBinlogs() {
					binLogs = append(binLogs, binLog.LogPath)
				}
				break
			}
		}

		segIdx := &model.SegmentIndex{
			Segment: model.Segment{
				SegmentID:    segmentInfo.ID,
				CollectionID: segmentInfo.CollectionID,
				PartitionID:  segmentInfo.PartitionID,
				NumRows:      segmentInfo.NumOfRows,
				BinLogs:      binLogs,
			},
			IndexID:    cit.indexID,
			CreateTime: cit.createTs,
		}
		if err = cit.indexCoordClient.createIndex(segIdx); err != nil {
			log.Error("IndexCoord create index on segment fail", zap.Int64("collectionID", cit.req.CollectionID),
				zap.Int64("fieldID", cit.req.FieldID), zap.String("indexName", cit.req.IndexName),
				zap.Int64("segmentID", segIdx.SegmentID), zap.Error(err))
			return err
		}
	}

	err = cit.table.CreateIndex(cit.indexID, cit.req, cit.createTs)
	if err != nil {
		log.Error("IndexCoord create index fail", zap.Int64("collectionID", cit.req.CollectionID),
			zap.Int64("fieldID", cit.req.FieldID), zap.String("indexName", cit.req.IndexName), zap.Error(err))
		return err
	}
	return nil
}

// PostExecute does nothing here.
func (cit *CreateIndexTask) PostExecute(ctx context.Context) error {
	log.Info("IndexCoord CreateIndexTask PostExecute", zap.Int64("collectionID", cit.req.CollectionID),
		zap.Int64("fieldID", cit.req.FieldID), zap.String("indexName", cit.req.IndexName))
	return nil
}

// IndexAddTask is used to record index task on segment.
type IndexAddTask struct {
	BaseTask
	segmentIndex *model.SegmentIndex
	idAllocator  *allocator.GlobalIDAllocator
}

// Ctx returns the context of the index task.
func (it *IndexAddTask) Ctx() context.Context {
	return it.ctx
}

// ID returns the id of the index task.
func (it *IndexAddTask) ID() UniqueID {
	return it.id
}

// SetID sets the id for index tasks.
func (it *IndexAddTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

// Name returns the task name.
func (it *IndexAddTask) Name() string {
	return IndexAddTaskName
}

// OnEnqueue assigns the indexBuildID to index task.
func (it *IndexAddTask) OnEnqueue() error {
	var err error
	it.segmentIndex.BuildID, err = it.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	return nil
}

// PreExecute sets the indexBuildID to index task request.
func (it *IndexAddTask) PreExecute(ctx context.Context) error {
	log.Info("IndexCoord IndexAddTask PreExecute", zap.Int64("IndexBuildID", it.segmentIndex.BuildID))
	return nil
}

// Execute adds the index task to meta table.
func (it *IndexAddTask) Execute(ctx context.Context) error {
	log.Info("IndexCoord IndexAddTask Execute", zap.Int64("IndexBuildID", it.segmentIndex.BuildID))
	err := it.table.AddIndex(it.segmentIndex)
	if err != nil {
		return err
	}
	return nil
}

// PostExecute does nothing here.
func (it *IndexAddTask) PostExecute(ctx context.Context) error {
	log.Info("IndexCoord IndexAddTask PostExecute", zap.Int64("IndexBuildID", it.segmentIndex.BuildID))
	return nil
}
