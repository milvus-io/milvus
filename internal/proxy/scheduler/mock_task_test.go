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

package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/uniquegenerator"
)

type mockTsoAllocator struct {
	mu        sync.Mutex
	logicPart uint32
}

func (tso *mockTsoAllocator) AllocOne(ctx context.Context) (taskmodel.Timestamp, error) {
	tso.mu.Lock()
	defer tso.mu.Unlock()
	tso.logicPart++
	physical := uint64(time.Now().UnixMilli())
	return (physical << 18) + uint64(tso.logicPart), nil
}

func newMockTsoAllocator() taskmodel.TsoAllocator {
	return &mockTsoAllocator{}
}

type mockTask struct {
	taskmodel.BaseTask
	*taskmodel.TaskCondition
	id          taskmodel.UniqueID
	name        string
	tType       commonpb.MsgType
	ts          taskmodel.Timestamp
	skipAllocTS bool
}

func (m *mockTask) CanSkipAllocTimestamp() bool {
	return m.skipAllocTS
}

func (m *mockTask) TraceCtx() context.Context {
	return m.Ctx()
}

func (m *mockTask) ID() taskmodel.UniqueID {
	return m.id
}

func (m *mockTask) SetID(uid taskmodel.UniqueID) {
	m.id = uid
}

func (m *mockTask) Name() string {
	return m.name
}

func (m *mockTask) Type() commonpb.MsgType {
	return m.tType
}

func (m *mockTask) BeginTs() taskmodel.Timestamp {
	return m.ts
}

func (m *mockTask) EndTs() taskmodel.Timestamp {
	return m.ts
}

func (m *mockTask) SetTs(ts taskmodel.Timestamp) {
	m.ts = ts
}

func (m *mockTask) OnEnqueue() error {
	return nil
}

func (m *mockTask) PreExecute(ctx context.Context) error {
	return nil
}

func (m *mockTask) Execute(ctx context.Context) error {
	return nil
}

func (m *mockTask) PostExecute(ctx context.Context) error {
	return nil
}

func newMockTask(ctx context.Context) *mockTask {
	return &mockTask{
		TaskCondition: taskmodel.NewTaskCondition(ctx),
		id:            taskmodel.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		name:          funcutil.GenRandomStr(),
		tType:         commonpb.MsgType_Undefined,
		ts:            taskmodel.Timestamp(time.Now().Nanosecond()),
	}
}

func newDefaultMockTask() *mockTask {
	return newMockTask(context.Background())
}

func newSkipAllocMockTask(metaCache metacache.Cache) *mockTask {
	t := newMockTask(context.Background())
	t.skipAllocTS = true
	t.MetaCache = metaCache
	return t
}

type mockDdlTask struct {
	*mockTask
}

func newMockDdlTask(ctx context.Context) *mockDdlTask {
	return &mockDdlTask{
		mockTask: newMockTask(ctx),
	}
}

func newDefaultMockDdlTask() *mockDdlTask {
	return newMockDdlTask(context.Background())
}

type mockDmlTask struct {
	*mockTask
	vchans []taskmodel.VChan
	pchans []taskmodel.PChan
}

func (m *mockDmlTask) SetChannels() error {
	return nil
}

func (m *mockDmlTask) GetChannels() []taskmodel.PChan {
	return m.pchans
}

func newMockDmlTask(ctx context.Context) *mockDmlTask {
	shardNum := 2

	vchans := make([]taskmodel.VChan, 0, shardNum)
	pchans := make([]taskmodel.PChan, 0, shardNum)

	for i := 0; i < shardNum; i++ {
		vchans = append(vchans, funcutil.GenRandomStr())
		pchans = append(pchans, funcutil.GenRandomStr())
	}

	return &mockDmlTask{
		mockTask: newMockTask(ctx),
		vchans:   vchans,
		pchans:   pchans,
	}
}

func newDefaultMockDmlTask() *mockDmlTask {
	return newMockDmlTask(context.Background())
}

type mockDqlTask struct {
	*mockTask
}

func newMockDqlTask(ctx context.Context) *mockDqlTask {
	return &mockDqlTask{
		mockTask: newMockTask(ctx),
	}
}

func newDefaultMockDqlTask() *mockDqlTask {
	return newMockDqlTask(context.Background())
}
