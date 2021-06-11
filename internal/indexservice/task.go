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

package indexservice

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/types"
)

const (
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

type BaseTask struct {
	done  chan error
	ctx   context.Context
	id    UniqueID
	table *metaTable
}

func (bt *BaseTask) ID() UniqueID {
	return bt.id
}

func (bt *BaseTask) setID(id UniqueID) {
	bt.id = id
}

func (bt *BaseTask) WaitToFinish() error {
	select {
	case <-bt.ctx.Done():
		return errors.New("Task wait to finished timeout")
	case err := <-bt.done:
		return err
	}
}

func (bt *BaseTask) Notify(err error) {
	bt.done <- err
}

type IndexAddTask struct {
	BaseTask
	req               *indexpb.BuildIndexRequest
	indexBuildID      UniqueID
	idAllocator       *allocator.GlobalIDAllocator
	buildQueue        TaskQueue
	kv                kv.BaseKV
	builderClient     types.IndexNode
	nodeClients       *PriorityQueue
	buildClientNodeID UniqueID
}

func (it *IndexAddTask) Ctx() context.Context {
	return it.ctx
}

func (it *IndexAddTask) ID() UniqueID {
	return it.id
}

func (it *IndexAddTask) SetID(ID UniqueID) {
	it.BaseTask.setID(ID)
}

func (it *IndexAddTask) Name() string {
	return IndexAddTaskName
}

func (it *IndexAddTask) OnEnqueue() error {
	var err error
	it.indexBuildID, err = it.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	return nil
}

func (it *IndexAddTask) PreExecute(ctx context.Context) error {
	log.Debug("IndexService IndexAddTask PreExecute", zap.Any("IndexBuildID", it.indexBuildID))
	it.req.IndexBuildID = it.indexBuildID
	return nil
}

func (it *IndexAddTask) Execute(ctx context.Context) error {
	log.Debug("IndexService IndexAddTask Execute", zap.Any("IndexBuildID", it.indexBuildID))
	err := it.table.AddIndex(it.indexBuildID, it.req)
	if err != nil {
		return err
	}
	return nil
}

func (it *IndexAddTask) PostExecute(ctx context.Context) error {
	log.Debug("IndexService IndexAddTask PostExecute", zap.Any("IndexBuildID", it.indexBuildID))
	return nil
}
