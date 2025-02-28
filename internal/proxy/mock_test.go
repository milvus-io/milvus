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

package proxy

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
)

type mockTimestampAllocatorInterface struct {
	lastTs Timestamp
	mtx    sync.Mutex
}

func (tso *mockTimestampAllocatorInterface) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	tso.mtx.Lock()
	defer tso.mtx.Unlock()

	ts := uint64(time.Now().UnixNano())
	if ts < tso.lastTs+Timestamp(req.Count) {
		ts = tso.lastTs + Timestamp(req.Count)
	}

	tso.lastTs = ts
	return &rootcoordpb.AllocTimestampResponse{
		Status:    merr.Success(),
		Timestamp: ts,
		Count:     req.Count,
	}, nil
}

func newMockTimestampAllocatorInterface() timestampAllocatorInterface {
	return &mockTimestampAllocatorInterface{
		lastTs: Timestamp(time.Now().UnixNano()),
	}
}

type mockTsoAllocator struct {
	mu        sync.Mutex
	logicPart uint32
}

func (tso *mockTsoAllocator) AllocOne(ctx context.Context) (Timestamp, error) {
	tso.mu.Lock()
	defer tso.mu.Unlock()
	tso.logicPart++
	physical := uint64(time.Now().UnixMilli())
	return (physical << 18) + uint64(tso.logicPart), nil
}

func newMockTsoAllocator() tsoAllocator {
	return &mockTsoAllocator{}
}

type mockIDAllocatorInterface struct{}

func (m *mockIDAllocatorInterface) AllocOne() (UniqueID, error) {
	return UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()), nil
}

func (m *mockIDAllocatorInterface) Alloc(count uint32) (UniqueID, UniqueID, error) {
	return UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()), UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt() + int(count)), nil
}

func newMockIDAllocatorInterface() allocator.Interface {
	return &mockIDAllocatorInterface{}
}

type mockTask struct {
	baseTask
	*TaskCondition
	id    UniqueID
	name  string
	tType commonpb.MsgType
	ts    Timestamp
}

func (m *mockTask) CanSkipAllocTimestamp() bool {
	return false
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
		id:            UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		name:          funcutil.GenRandomStr(),
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

func (m *mockDmlTask) setChannels() error {
	return nil
}

func (m *mockDmlTask) getChannels() []vChan {
	return m.vchans
}

func newMockDmlTask(ctx context.Context) *mockDmlTask {
	shardNum := 2

	vchans := make([]vChan, 0, shardNum)
	pchans := make([]pChan, 0, shardNum)

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

type simpleMockMsgStream struct {
	msgChan chan *msgstream.ConsumeMsgPack

	msgCount    int
	msgCountMtx sync.RWMutex
}

func (ms *simpleMockMsgStream) Close() {
}

func (ms *simpleMockMsgStream) Chan() <-chan *msgstream.ConsumeMsgPack {
	if ms.getMsgCount() <= 0 {
		ms.msgChan <- nil
		return ms.msgChan
	}

	defer ms.decreaseMsgCount(1)

	return ms.msgChan
}

func (ms *simpleMockMsgStream) GetUnmarshalDispatcher() msgstream.UnmarshalDispatcher {
	return nil
}

func (ms *simpleMockMsgStream) AsProducer(ctx context.Context, channels []string) {
}

func (ms *simpleMockMsgStream) AsConsumer(ctx context.Context, channels []string, subName string, position common.SubscriptionInitialPosition) error {
	return nil
}

func (ms *simpleMockMsgStream) SetRepackFunc(repackFunc msgstream.RepackFunc) {
}

func (ms *simpleMockMsgStream) getMsgCount() int {
	ms.msgCountMtx.RLock()
	defer ms.msgCountMtx.RUnlock()

	return ms.msgCount
}

func (ms *simpleMockMsgStream) increaseMsgCount(delta int) {
	ms.msgCountMtx.Lock()
	defer ms.msgCountMtx.Unlock()

	ms.msgCount += delta
}

func (ms *simpleMockMsgStream) decreaseMsgCount(delta int) {
	ms.increaseMsgCount(-delta)
}

func (ms *simpleMockMsgStream) Produce(ctx context.Context, pack *msgstream.MsgPack) error {
	defer ms.increaseMsgCount(1)

	ms.msgChan <- msgstream.BuildConsumeMsgPack(pack)
	return nil
}

func (ms *simpleMockMsgStream) Broadcast(ctx context.Context, pack *msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
	return map[string][]msgstream.MessageID{}, nil
}

func (ms *simpleMockMsgStream) GetProduceChannels() []string {
	return nil
}

func (ms *simpleMockMsgStream) Seek(ctx context.Context, msgPositions []*msgstream.MsgPosition, includeCurrentMsg bool) error {
	return nil
}

func (ms *simpleMockMsgStream) GetLatestMsgID(channel string) (msgstream.MessageID, error) {
	return nil, nil
}

func (ms *simpleMockMsgStream) CheckTopicValid(topic string) error {
	return nil
}

func (ms *simpleMockMsgStream) ForceEnableProduce(enabled bool) {
}

func (ms *simpleMockMsgStream) SetReplicate(config *msgstream.ReplicateConfig) {
}

func newSimpleMockMsgStream() *simpleMockMsgStream {
	return &simpleMockMsgStream{
		msgChan:  make(chan *msgstream.ConsumeMsgPack, 1024),
		msgCount: 0,
	}
}

type simpleMockMsgStreamFactory struct{}

func (factory *simpleMockMsgStreamFactory) Init(param *paramtable.ComponentParam) error {
	return nil
}

func (factory *simpleMockMsgStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newSimpleMockMsgStream(), nil
}

func (factory *simpleMockMsgStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newSimpleMockMsgStream(), nil
}

func (factory *simpleMockMsgStreamFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return nil
}

func newSimpleMockMsgStreamFactory() *simpleMockMsgStreamFactory {
	return &simpleMockMsgStreamFactory{}
}

func generateFieldData(dataType schemapb.DataType, fieldName string, numRows int) *schemapb.FieldData {
	if dataType < 100 {
		return testutils.GenerateScalarFieldData(dataType, fieldName, numRows)
	}
	return testutils.GenerateVectorFieldData(dataType, fieldName, numRows, testVecDim)
}

func newScalarFieldData(fieldSchema *schemapb.FieldSchema, fieldName string, numRows int) *schemapb.FieldData {
	return testutils.GenerateScalarFieldData(fieldSchema.GetDataType(), fieldName, numRows)
}

func newFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewFloatVectorFieldData(fieldName, numRows, dim)
}

func newBinaryVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewBinaryVectorFieldData(fieldName, numRows, dim)
}

func newFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewFloat16VectorFieldData(fieldName, numRows, dim)
}

func newBFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewBFloat16VectorFieldData(fieldName, numRows, dim)
}
