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

package rootcoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type task interface {
	GetCtx() context.Context
	SetCtx(context.Context)
	SetTs(ts Timestamp)
	GetTs() Timestamp
	SetID(id UniqueID)
	GetID() UniqueID
	Prepare(ctx context.Context) error
	Execute(ctx context.Context) error
	WaitToFinish() error
	NotifyDone(err error)
	SetInQueueDuration()
}

type baseTask struct {
	ctx  context.Context
	core *Core
	done chan error
	ts   Timestamp
	id   UniqueID

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
}

func newBaseTask(ctx context.Context, core *Core) baseTask {
	b := baseTask{
		core: core,
		done: make(chan error, 1),
		tr:   timerecord.NewTimeRecorderWithTrace(ctx, "new task"),
	}
	b.SetCtx(ctx)
	return b
}

func (b *baseTask) SetCtx(ctx context.Context) {
	b.ctx = ctx
}

func (b *baseTask) GetCtx() context.Context {
	return b.ctx
}

func (b *baseTask) SetTs(ts Timestamp) {
	b.ts = ts
}

func (b *baseTask) GetTs() Timestamp {
	return b.ts
}

func (b *baseTask) SetID(id UniqueID) {
	b.id = id
}

func (b *baseTask) GetID() UniqueID {
	return b.id
}

func (b *baseTask) Prepare(ctx context.Context) error {
	return nil
}

func (b *baseTask) Execute(ctx context.Context) error {
	return nil
}

func (b *baseTask) WaitToFinish() error {
	return <-b.done
}

func (b *baseTask) NotifyDone(err error) {
	b.done <- err
}

func (b *baseTask) SetInQueueDuration() {
	b.queueDur = b.tr.ElapseSpan()
}
