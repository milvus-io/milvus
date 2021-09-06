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

package proxy

import (
	"context"
	"math/rand"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

type mockTimestampAllocatorInterface struct {
}

func (tso *mockTimestampAllocatorInterface) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	return &rootcoordpb.AllocTimestampResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Timestamp: uint64(time.Now().UnixNano()),
		Count:     req.Count,
	}, nil
}

func newMockTimestampAllocatorInterface() timestampAllocatorInterface {
	return &mockTimestampAllocatorInterface{}
}

type mockTsoAllocator struct {
}

func (tso *mockTsoAllocator) AllocOne() (Timestamp, error) {
	return Timestamp(time.Now().UnixNano()), nil
}

func newMockTsoAllocator() tsoAllocator {
	return &mockTsoAllocator{}
}

type mockIDAllocatorInterface struct {
}

func (m *mockIDAllocatorInterface) AllocOne() (UniqueID, error) {
	return UniqueID(getUniqueIntGeneratorIns().get()), nil
}

func newMockIDAllocatorInterface() idAllocatorInterface {
	return &mockIDAllocatorInterface{}
}

type mockGetChannelsService struct {
	collectionID2Channels map[UniqueID]map[vChan]pChan
}

func newMockGetChannelsService() *mockGetChannelsService {
	return &mockGetChannelsService{
		collectionID2Channels: make(map[UniqueID]map[vChan]pChan),
	}
}

func (m *mockGetChannelsService) GetChannels(collectionID UniqueID) (map[vChan]pChan, error) {
	channels, ok := m.collectionID2Channels[collectionID]
	if ok {
		return channels, nil
	}

	channels = make(map[vChan]pChan)
	l := rand.Uint64()%10 + 1
	for i := 0; uint64(i) < l; i++ {
		channels[genUniqueStr()] = genUniqueStr()
	}

	m.collectionID2Channels[collectionID] = channels
	return channels, nil
}

type mockTask struct {
	*TaskCondition
	id    UniqueID
	name  string
	tType commonpb.MsgType
	ts    Timestamp
}

func (m *mockTask) TraceCtx() context.Context {
	return m.TaskCondition.ctx
}

func (m *mockTask) ID() UniqueID {
	return m.id
}

func (m *mockTask) SetID(uid UniqueID) {
	m.id = uid
}

func (m *mockTask) Name() string {
	return m.name
}

func (m *mockTask) Type() commonpb.MsgType {
	return m.tType
}

func (m *mockTask) BeginTs() Timestamp {
	return m.ts
}

func (m *mockTask) EndTs() Timestamp {
	return m.ts
}

func (m *mockTask) SetTs(ts Timestamp) {
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
		TaskCondition: NewTaskCondition(ctx),
		id:            UniqueID(getUniqueIntGeneratorIns().get()),
		name:          genUniqueStr(),
		tType:         commonpb.MsgType_Undefined,
		ts:            Timestamp(time.Now().Nanosecond()),
	}
}

func newDefaultMockTask() *mockTask {
	return newMockTask(context.Background())
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
	vchans []vChan
	pchans []pChan
}

func (m *mockDmlTask) getChannels() ([]vChan, error) {
	return m.vchans, nil
}

func (m *mockDmlTask) getPChanStats() (map[pChan]pChanStatistics, error) {
	ret := make(map[pChan]pChanStatistics)
	for _, pchan := range m.pchans {
		ret[pchan] = pChanStatistics{
			minTs: m.ts,
			maxTs: m.ts,
		}
	}
	return ret, nil
}

func newMockDmlTask(ctx context.Context) *mockDmlTask {
	shardNum := 2

	vchans := make([]vChan, 0, shardNum)
	pchans := make([]pChan, 0, shardNum)

	for i := 0; i < shardNum; i++ {
		vchans = append(vchans, genUniqueStr())
		pchans = append(pchans, genUniqueStr())
	}

	return &mockDmlTask{
		mockTask: newMockTask(ctx),
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
