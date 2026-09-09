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

package taskmodel

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
)

const (
	SearchTaskName = "SearchTask"
	QueryTaskName  = "QueryTask"
)

// Task is the interface implemented by every proxy task. It is consumed by the
// scheduler and implemented by the concrete task structs in the proxy package.
type Task interface {
	TraceCtx() context.Context
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Name() string
	Type() commonpb.MsgType
	BeginTs() Timestamp
	EndTs() Timestamp
	SetTs(ts Timestamp)
	OnEnqueue() error
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
	CanSkipAllocTimestamp() bool
	GetMetaCache() metacache.Cache
	SetOnEnqueueTime()
	GetDurationInQueue() time.Duration
	IsSubTask() bool
	SetExecutingTime()
	GetDurationInExecuting() time.Duration
}

// BaseTask is embedded by concrete task structs to provide the common
// meta-cache accessor and queue/execute timing bookkeeping.
type BaseTask struct {
	onEnqueueTime time.Time
	executingTime time.Time
	MetaCache     metacache.Cache
}

func (bt *BaseTask) GetMetaCache() metacache.Cache {
	return bt.MetaCache
}

func (bt *BaseTask) CanSkipAllocTimestamp() bool {
	return false
}

func (bt *BaseTask) SetOnEnqueueTime() {
	bt.onEnqueueTime = time.Now()
}

func (bt *BaseTask) GetDurationInQueue() time.Duration {
	return time.Since(bt.onEnqueueTime)
}

func (bt *BaseTask) IsSubTask() bool {
	return false
}

func (bt *BaseTask) SetExecutingTime() {
	bt.executingTime = time.Now()
}

func (bt *BaseTask) GetDurationInExecuting() time.Duration {
	return time.Since(bt.executingTime)
}

// DMLTask is the interface implemented by DML tasks (insert/delete/upsert)
// whose physical channels are resolved before enqueueing.
type DMLTask interface {
	Task
	SetChannels() error
	GetChannels() []PChan
}

// BaseInsertTask is an alias of msgstream.InsertMsg.
type BaseInsertTask = msgstream.InsertMsg
