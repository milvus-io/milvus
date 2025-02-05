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

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type LockLevel int

const (
	ClusterLock LockLevel = iota
	DatabaseLock
	CollectionLock
)

type LockerKey interface {
	LockKey() string
	Level() LockLevel
	IsWLock() bool
	Next() LockerKey
}

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
	IsFinished() bool
	SetInQueueDuration()
	GetLockerKey() LockerKey
}

type baseTask struct {
	ctx        context.Context
	core       *Core
	done       chan error
	isFinished *atomic.Bool
	ts         Timestamp
	id         UniqueID

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
}

func newBaseTask(ctx context.Context, core *Core) baseTask {
	b := baseTask{
		core:       core,
		done:       make(chan error, 1),
		tr:         timerecord.NewTimeRecorderWithTrace(ctx, "new task"),
		isFinished: atomic.NewBool(false),
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
	b.isFinished.Store(true)
}

func (b *baseTask) SetInQueueDuration() {
	b.queueDur = b.tr.ElapseSpan()
}

func (b *baseTask) IsFinished() bool {
	return b.isFinished.Load()
}

func (b *baseTask) GetLockerKey() LockerKey {
	return nil
}

type taskLockerKey struct {
	key   string
	rw    bool
	level LockLevel
	next  LockerKey
}

func (t *taskLockerKey) LockKey() string {
	return t.key
}

func (t *taskLockerKey) Level() LockLevel {
	return t.level
}

func (t *taskLockerKey) IsWLock() bool {
	return t.rw
}

func (t *taskLockerKey) Next() LockerKey {
	return t.next
}

func NewClusterLockerKey(rw bool) LockerKey {
	return &taskLockerKey{
		key:   "$",
		rw:    rw,
		level: ClusterLock,
	}
}

func NewDatabaseLockerKey(db string, rw bool) LockerKey {
	return &taskLockerKey{
		key:   db,
		rw:    rw,
		level: DatabaseLock,
	}
}

func NewCollectionLockerKey(collection string, rw bool) LockerKey {
	return &taskLockerKey{
		key:   collection,
		rw:    rw,
		level: CollectionLock,
	}
}

func NewLockerKeyChain(lockerKeys ...LockerKey) LockerKey {
	if len(lockerKeys) == 0 {
		return nil
	}
	if lockerKeys[0] == nil || lockerKeys[0].Level() != ClusterLock {
		log.Warn("Invalid locker key chain", zap.Stack("stack"))
		return nil
	}

	for i := 0; i < len(lockerKeys)-1; i++ {
		if lockerKeys[i] == nil || lockerKeys[i].Level() >= lockerKeys[i+1].Level() {
			log.Warn("Invalid locker key chain", zap.Stack("stack"))
			return nil
		}
		lockerKeys[i].(*taskLockerKey).next = lockerKeys[i+1]
	}
	return lockerKeys[0]
}
