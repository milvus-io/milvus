// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexcoord

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

const (
	// IndexAddTaskName is the name of the operation to add index task.
	IndexAddTaskName = "IndexAddTask"
)

type task interface {
	Ctx() context.Context
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Name() string
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
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

// IndexAddTask is used to record the information of the index tasks.
type IndexAddTask struct {
	BaseTask
	req          *indexpb.BuildIndexRequest
	indexBuildID UniqueID
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
	it.indexBuildID, err = it.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	return nil
}

// PreExecute sets the indexBuildID to index task request.
func (it *IndexAddTask) PreExecute(ctx context.Context) error {
	log.Debug("IndexCoord IndexAddTask PreExecute", zap.Any("IndexBuildID", it.indexBuildID))
	it.req.IndexBuildID = it.indexBuildID
	return nil
}

// Execute adds the index task to meta table.
func (it *IndexAddTask) Execute(ctx context.Context) error {
	log.Debug("IndexCoord IndexAddTask Execute", zap.Any("IndexBuildID", it.indexBuildID))
	err := it.table.AddIndex(it.indexBuildID, it.req)
	if err != nil {
		return err
	}
	return nil
}

// PostExecute does nothing here.
func (it *IndexAddTask) PostExecute(ctx context.Context) error {
	log.Debug("IndexCoord IndexAddTask PostExecute", zap.Any("IndexBuildID", it.indexBuildID))
	return nil
}
