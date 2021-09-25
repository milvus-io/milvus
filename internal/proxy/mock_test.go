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
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/msgstream"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

type mockTimestampAllocatorInterface struct {
	lastTs Timestamp
	mtx    sync.Mutex
}

func (tso *mockTimestampAllocatorInterface) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	tso.mtx.Lock()
	defer tso.mtx.Unlock()

	ts := uint64(time.Now().UnixNano())
	if ts < tso.lastTs+Timestamp(req.Count) {
		ts = tso.lastTs + Timestamp(req.Count)
	}

	tso.lastTs = ts
	return &rootcoordpb.AllocTimestampResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
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
	return UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()), nil
}

func newMockIDAllocatorInterface() idAllocatorInterface {
	return &mockIDAllocatorInterface{}
}

type mockGetChannelsService struct {
	collectionID2Channels map[UniqueID]map[vChan]pChan
	f                     getChannelsFuncType
}

func newMockGetChannelsService() *mockGetChannelsService {
	return &mockGetChannelsService{
		collectionID2Channels: make(map[UniqueID]map[vChan]pChan),
	}
}

func (m *mockGetChannelsService) GetChannels(collectionID UniqueID) (map[vChan]pChan, error) {
	if m.f != nil {
		return m.f(collectionID)
	}

	channels, ok := m.collectionID2Channels[collectionID]
	if ok {
		return channels, nil
	}

	channels = make(map[vChan]pChan)
	l := rand.Uint64()%10 + 1
	for i := 0; uint64(i) < l; i++ {
		channels[funcutil.GenRandomStr()] = funcutil.GenRandomStr()
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
		vchans = append(vchans, funcutil.GenRandomStr())
		pchans = append(pchans, funcutil.GenRandomStr())
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

type simpleMockMsgStream struct {
	msgChan chan *msgstream.MsgPack

	msgCount    int
	msgCountMtx sync.RWMutex
}

func (ms *simpleMockMsgStream) Start() {
}

func (ms *simpleMockMsgStream) Close() {
}

func (ms *simpleMockMsgStream) Chan() <-chan *msgstream.MsgPack {
	return ms.msgChan
}

func (ms *simpleMockMsgStream) AsProducer(channels []string) {
}

func (ms *simpleMockMsgStream) AsConsumer(channels []string, subName string) {
}

func (ms *simpleMockMsgStream) ComputeProduceChannelIndexes(tsMsgs []msgstream.TsMsg) [][]int32 {
	if len(tsMsgs) <= 0 {
		return nil
	}
	reBucketValues := make([][]int32, len(tsMsgs))
	channelNum := uint32(1)
	if channelNum == 0 {
		return nil
	}
	for idx, tsMsg := range tsMsgs {
		hashValues := tsMsg.HashKeys()
		bucketValues := make([]int32, len(hashValues))
		for index, hashValue := range hashValues {
			bucketValues[index] = int32(hashValue % channelNum)
		}
		reBucketValues[idx] = bucketValues
	}
	return reBucketValues
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

func (ms *simpleMockMsgStream) Produce(pack *msgstream.MsgPack) error {
	defer ms.increaseMsgCount(1)

	ms.msgChan <- pack

	return nil
}

func (ms *simpleMockMsgStream) Broadcast(pack *msgstream.MsgPack) error {
	return nil
}

func (ms *simpleMockMsgStream) GetProduceChannels() []string {
	return nil
}

func (ms *simpleMockMsgStream) Consume() *msgstream.MsgPack {
	if ms.getMsgCount() <= 0 {
		return nil
	}

	defer ms.decreaseMsgCount(1)

	return <-ms.msgChan
}

func (ms *simpleMockMsgStream) Seek(offset []*msgstream.MsgPosition) error {
	return nil
}

func newSimpleMockMsgStream() *simpleMockMsgStream {
	return &simpleMockMsgStream{
		msgChan:  make(chan *msgstream.MsgPack, 1024),
		msgCount: 0,
	}
}

type simpleMockMsgStreamFactory struct {
}

func (factory *simpleMockMsgStreamFactory) SetParams(params map[string]interface{}) error {
	return nil
}

func (factory *simpleMockMsgStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newSimpleMockMsgStream(), nil
}

func (factory *simpleMockMsgStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newSimpleMockMsgStream(), nil
}

func (factory *simpleMockMsgStreamFactory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return newSimpleMockMsgStream(), nil
}

func newSimpleMockMsgStreamFactory() *simpleMockMsgStreamFactory {
	return &simpleMockMsgStreamFactory{}
}

func generateBoolArray(numRows int) []bool {
	ret := make([]bool, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Int()%2 == 0)
	}
	return ret
}

func generateInt8Array(numRows int) []int8 {
	ret := make([]int8, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int8(rand.Int()))
	}
	return ret
}

func generateInt16Array(numRows int) []int16 {
	ret := make([]int16, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int16(rand.Int()))
	}
	return ret
}

func generateInt32Array(numRows int) []int32 {
	ret := make([]int32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int32(rand.Int()))
	}
	return ret
}

func generateInt64Array(numRows int) []int64 {
	ret := make([]int64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int64(rand.Int()))
	}
	return ret
}

func generateFloat32Array(numRows int) []float32 {
	ret := make([]float32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func generateFloat64Array(numRows int) []float64 {
	ret := make([]float64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Float64())
	}
	return ret
}

func generateFloatVectors(numRows, dim int) []float32 {
	total := numRows * dim
	ret := make([]float32, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func generateBinaryVectors(numRows, dim int) []byte {
	total := (numRows * dim) / 8
	ret := make([]byte, total)
	_, err := rand.Read(ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func newScalarFieldData(dType schemapb.DataType, fieldName string, numRows int) *schemapb.FieldData {
	ret := &schemapb.FieldData{
		Type:      dType,
		FieldName: fieldName,
		Field:     nil,
	}

	switch dType {
	case schemapb.DataType_Bool:
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: generateBoolArray(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int8:
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: generateInt32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int16:
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: generateInt32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int32:
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: generateInt32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int64:
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: generateInt64Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Float:
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: generateFloat32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Double:
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: generateFloat64Array(numRows),
					},
				},
			},
		}
	}

	return ret
}

func newFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: generateFloatVectors(numRows, dim),
					},
				},
			},
		},
	}
}

func newBinaryVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_BinaryVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: generateBinaryVectors(numRows, dim),
				},
			},
		},
	}
}

func generateHashKeys(numRows int) []uint32 {
	ret := make([]uint32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Uint32())
	}
	return ret
}
