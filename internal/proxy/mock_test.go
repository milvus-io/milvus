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
	"math/rand"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/uniquegenerator"
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

type mockIDAllocatorInterface struct {
}

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

func (ms *simpleMockMsgStream) Close() {
}

func (ms *simpleMockMsgStream) Chan() <-chan *msgstream.MsgPack {
	if ms.getMsgCount() <= 0 {
		ms.msgChan <- nil
		return ms.msgChan
	}

	defer ms.decreaseMsgCount(1)

	return ms.msgChan
}

func (ms *simpleMockMsgStream) AsProducer(channels []string) {
}

func (ms *simpleMockMsgStream) AsConsumer(channels []string, subName string, position mqwrapper.SubscriptionInitialPosition) {
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

func (ms *simpleMockMsgStream) Broadcast(pack *msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
	return map[string][]msgstream.MessageID{}, nil
}

func (ms *simpleMockMsgStream) GetProduceChannels() []string {
	return nil
}

func (ms *simpleMockMsgStream) Seek(offset []*msgstream.MsgPosition) error {
	return nil
}

func (ms *simpleMockMsgStream) GetLatestMsgID(channel string) (msgstream.MessageID, error) {
	return nil, nil
}

func (ms *simpleMockMsgStream) CheckTopicValid(topic string) error {
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

func (factory *simpleMockMsgStreamFactory) Init(param *paramtable.ComponentParam) error {
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

func (factory *simpleMockMsgStreamFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return nil
}

func newSimpleMockMsgStreamFactory() *simpleMockMsgStreamFactory {
	return &simpleMockMsgStreamFactory{}
}

func generateFieldData(dataType schemapb.DataType, fieldName string, numRows int) *schemapb.FieldData {
	fieldData := &schemapb.FieldData{
		Type:      dataType,
		FieldName: fieldName,
	}
	switch dataType {
	case schemapb.DataType_Bool:
		fieldData.FieldName = fieldName
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: generateBoolArray(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int32:
		fieldData.FieldName = fieldName
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: generateInt32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int64:
		fieldData.FieldName = fieldName
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: generateInt64Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Float:
		fieldData.FieldName = fieldName
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: generateFloat32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Double:
		fieldData.FieldName = fieldName
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: generateFloat64Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_VarChar:
		fieldData.FieldName = fieldName
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: generateVarCharArray(numRows, maxTestStringLen),
					},
				},
			},
		}
	case schemapb.DataType_FloatVector:
		fieldData.FieldName = fieldName
		fieldData.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(testVecDim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: generateFloatVectors(numRows, testVecDim),
					},
				},
			},
		}
	case schemapb.DataType_BinaryVector:
		fieldData.FieldName = fieldName
		fieldData.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(testVecDim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: generateBinaryVectors(numRows, testVecDim),
				},
			},
		}
	default:
		//TODO::
	}

	return fieldData
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

func generateUint64Array(numRows int) []uint64 {
	ret := make([]uint64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Uint64())
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

func generateVarCharArray(numRows int, maxLen int) []string {
	ret := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = funcutil.RandomString(rand.Intn(maxLen))
	}

	return ret
}

func newScalarFieldData(fieldSchema *schemapb.FieldSchema, fieldName string, numRows int) *schemapb.FieldData {
	ret := &schemapb.FieldData{
		Type:      fieldSchema.DataType,
		FieldName: fieldName,
		Field:     nil,
	}

	switch fieldSchema.DataType {
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
	case schemapb.DataType_VarChar:
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: generateVarCharArray(numRows, testMaxVarCharLength),
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
