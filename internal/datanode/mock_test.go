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

package datanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	s "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

const ctxTimeInMillisecond = 5000
const debug = false

var emptyFlushAndDropFunc flushAndDropFunc = func(_ []*segmentFlushPack) {}

func newIDLEDataNodeMock(ctx context.Context) *DataNode {
	msFactory := msgstream.NewPmsFactory()
	node := NewDataNode(ctx, msFactory)

	rc := &RootCoordFactory{
		ID:             0,
		collectionID:   1,
		collectionName: "collection-1",
	}
	node.rootCoord = rc

	ds := &DataCoordFactory{}
	node.dataCoord = ds

	return node
}

func newHEALTHDataNodeMock(dmChannelName string) *DataNode {
	var ctx context.Context

	if debug {
		ctx = context.Background()
	} else {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		go func() {
			<-ctx.Done()
			cancel()
		}()
	}

	msFactory := msgstream.NewPmsFactory()
	node := NewDataNode(ctx, msFactory)

	ms := &RootCoordFactory{
		ID:             0,
		collectionID:   1,
		collectionName: "collection-1",
	}
	node.rootCoord = ms

	ds := &DataCoordFactory{}
	node.dataCoord = ds

	return node
}

func makeNewChannelNames(names []string, suffix string) []string {
	var ret []string
	for _, name := range names {
		ret = append(ret, name+suffix)
	}
	return ret
}

func clearEtcd(rootPath string) error {
	client, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		return err
	}
	etcdKV := etcdkv.NewEtcdKV(client, rootPath)

	err = etcdKV.RemoveWithPrefix("writer/segment")
	if err != nil {
		return err
	}
	_, _, err = etcdKV.LoadWithPrefix("writer/segment")
	if err != nil {
		return err
	}
	log.Debug("Clear ETCD with prefix writer/segment ")

	err = etcdKV.RemoveWithPrefix("writer/ddl")
	if err != nil {
		return err
	}
	_, _, err = etcdKV.LoadWithPrefix("writer/ddl")
	if err != nil {
		return err
	}
	log.Debug("Clear ETCD with prefix writer/ddl")
	return nil

}

type MetaFactory struct {
}

func NewMetaFactory() *MetaFactory {
	return &MetaFactory{}
}

type DataFactory struct {
	rawData []byte
}

type RootCoordFactory struct {
	types.RootCoord
	ID             UniqueID
	collectionName string
	collectionID   UniqueID
}

type DataCoordFactory struct {
	types.DataCoord

	SaveBinlogPathError      bool
	SaveBinlogPathNotSuccess bool

	CompleteCompactionError      bool
	CompleteCompactionNotSuccess bool

	DropVirtualChannelError      bool
	DropVirtualChannelNotSuccess bool
}

func (ds *DataCoordFactory) CompleteCompaction(ctx context.Context, req *datapb.CompactionResult) (*commonpb.Status, error) {
	if ds.CompleteCompactionError {
		return nil, errors.New("Error")
	}
	if ds.CompleteCompactionNotSuccess {
		return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}, nil
	}

	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (ds *DataCoordFactory) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	if ds.SaveBinlogPathError {
		return nil, errors.New("Error")
	}
	if ds.SaveBinlogPathNotSuccess {
		return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}, nil
	}

	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (ds *DataCoordFactory) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	if ds.DropVirtualChannelError {
		return nil, errors.New("error")
	}
	if ds.DropVirtualChannelNotSuccess {
		return &datapb.DropVirtualChannelResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil
	}
	return &datapb.DropVirtualChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (mf *MetaFactory) GetCollectionMeta(collectionID UniqueID, collectionName string) *etcdpb.CollectionMeta {
	sch := schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "test collection by meta factory",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:     0,
				Name:        "RowID",
				Description: "RowID field",
				DataType:    schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "f0_tk1",
						Value: "f0_tv1",
					},
				},
			},
			{
				FieldID:     1,
				Name:        "Timestamp",
				Description: "Timestamp field",
				DataType:    schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "f1_tk1",
						Value: "f1_tv1",
					},
				},
			},
			{
				FieldID:     100,
				Name:        "float_vector_field",
				Description: "field 100",
				DataType:    schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "indexkey",
						Value: "indexvalue",
					},
				},
			},
			{
				FieldID:     101,
				Name:        "binary_vector_field",
				Description: "field 101",
				DataType:    schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "32",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "indexkey",
						Value: "indexvalue",
					},
				},
			},
			{
				FieldID:     102,
				Name:        "bool_field",
				Description: "field 102",
				DataType:    schemapb.DataType_Bool,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     103,
				Name:        "int8_field",
				Description: "field 103",
				DataType:    schemapb.DataType_Int8,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     104,
				Name:        "int16_field",
				Description: "field 104",
				DataType:    schemapb.DataType_Int16,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     105,
				Name:        "int32_field",
				Description: "field 105",
				DataType:    schemapb.DataType_Int32,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:      106,
				Name:         "int64_field",
				Description:  "field 106",
				DataType:     schemapb.DataType_Int64,
				TypeParams:   []*commonpb.KeyValuePair{},
				IndexParams:  []*commonpb.KeyValuePair{},
				IsPrimaryKey: true,
			},
			{
				FieldID:     107,
				Name:        "float32_field",
				Description: "field 107",
				DataType:    schemapb.DataType_Float,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
			{
				FieldID:     108,
				Name:        "float64_field",
				Description: "field 108",
				DataType:    schemapb.DataType_Double,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
		},
	}

	collection := etcdpb.CollectionMeta{
		ID:           collectionID,
		Schema:       &sch,
		CreateTime:   Timestamp(1),
		SegmentIDs:   make([]UniqueID, 0),
		PartitionIDs: []UniqueID{0},
	}
	return &collection
}

func NewDataFactory() *DataFactory {
	return &DataFactory{rawData: GenRowData()}
}

func GenRowData() (rawData []byte) {
	const DIM = 2
	const N = 1

	// Float vector
	var fvector = [DIM]float32{1, 2}
	for _, ele := range fvector {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}

	// Binary vector
	// Dimension of binary vector is 32
	// size := 4,  = 32 / 8
	var bvector = []byte{255, 255, 255, 0}
	rawData = append(rawData, bvector...)

	// Bool
	var fieldBool = true
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, common.Endian, fieldBool); err != nil {
		panic(err)
	}

	rawData = append(rawData, buf.Bytes()...)

	// int8
	var dataInt8 int8 = 100
	bint8 := new(bytes.Buffer)
	if err := binary.Write(bint8, common.Endian, dataInt8); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint8.Bytes()...)

	// int16
	var dataInt16 int16 = 200
	bint16 := new(bytes.Buffer)
	if err := binary.Write(bint16, common.Endian, dataInt16); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint16.Bytes()...)

	// int32
	var dataInt32 int32 = 300
	bint32 := new(bytes.Buffer)
	if err := binary.Write(bint32, common.Endian, dataInt32); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint32.Bytes()...)

	// int64
	var dataInt64 int64 = 400
	bint64 := new(bytes.Buffer)
	if err := binary.Write(bint64, common.Endian, dataInt64); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint64.Bytes()...)

	// float32
	var datafloat float32 = 1.1
	bfloat32 := new(bytes.Buffer)
	if err := binary.Write(bfloat32, common.Endian, datafloat); err != nil {
		panic(err)
	}
	rawData = append(rawData, bfloat32.Bytes()...)

	// float64
	var datafloat64 = 2.2
	bfloat64 := new(bytes.Buffer)
	if err := binary.Write(bfloat64, common.Endian, datafloat64); err != nil {
		panic(err)
	}
	rawData = append(rawData, bfloat64.Bytes()...)
	log.Debug("Rawdata length:", zap.Int("Length of rawData", len(rawData)))
	return
}

func (df *DataFactory) GenMsgStreamInsertMsg(idx int, chanName string) *msgstream.InsertMsg {
	var msg = &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{uint32(idx)},
		},
		InsertRequest: internalpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     0,
				Timestamp: Timestamp(idx + 1000),
				SourceID:  0,
			},
			CollectionName: "col1",
			PartitionName:  "default",
			SegmentID:      1,
			CollectionID:   UniqueID(0),
			ShardName:      chanName,
			Timestamps:     []Timestamp{Timestamp(idx + 1000)},
			RowIDs:         []UniqueID{UniqueID(idx)},
			RowData:        []*commonpb.Blob{{Value: df.rawData}},
		},
	}
	return msg
}

func (df *DataFactory) GetMsgStreamTsInsertMsgs(n int, chanName string) (inMsgs []msgstream.TsMsg) {
	for i := 0; i < n; i++ {
		var msg = df.GenMsgStreamInsertMsg(i, chanName)
		var tsMsg msgstream.TsMsg = msg
		inMsgs = append(inMsgs, tsMsg)
	}
	return
}

func (df *DataFactory) GetMsgStreamInsertMsgs(n int) (msgs []*msgstream.InsertMsg) {
	for i := 0; i < n; i++ {
		var msg = df.GenMsgStreamInsertMsg(i, "")
		msgs = append(msgs, msg)
	}
	return
}

func (df *DataFactory) GenMsgStreamDeleteMsg(pks []int64, chanName string) *msgstream.DeleteMsg {
	idx := 100
	timestamps := make([]Timestamp, len(pks))
	for i := 0; i < len(pks); i++ {
		timestamps[i] = Timestamp(i) + 1000
	}
	var msg = &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{uint32(idx)},
		},
		DeleteRequest: internalpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Delete,
				MsgID:     0,
				Timestamp: Timestamp(idx + 1000),
				SourceID:  0,
			},
			CollectionName: "col1",
			PartitionName:  "default",
			ShardName:      chanName,
			PrimaryKeys:    pks,
			Timestamps:     timestamps,
		},
	}
	return msg
}

func genFlowGraphInsertMsg(chanName string) flowGraphMsg {
	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	startPos := []*internalpb.MsgPosition{
		{
			ChannelName: chanName,
			MsgID:       make([]byte, 0),
			Timestamp:   0,
		},
	}

	var fgMsg = &flowGraphMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		timeRange: TimeRange{
			timestampMin: timeRange.timestampMin,
			timestampMax: timeRange.timestampMax,
		},
		startPositions: startPos,
		endPositions:   startPos,
	}

	dataFactory := NewDataFactory()
	fgMsg.insertMessages = append(fgMsg.insertMessages, dataFactory.GetMsgStreamInsertMsgs(2)...)

	return *fgMsg
}

func genFlowGraphDeleteMsg(pks []int64, chanName string) flowGraphMsg {
	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	startPos := []*internalpb.MsgPosition{
		{
			ChannelName: chanName,
			MsgID:       make([]byte, 0),
			Timestamp:   0,
		},
	}

	var fgMsg = &flowGraphMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		timeRange: TimeRange{
			timestampMin: timeRange.timestampMin,
			timestampMax: timeRange.timestampMax,
		},
		startPositions: startPos,
		endPositions:   startPos,
	}

	dataFactory := NewDataFactory()
	fgMsg.deleteMessages = append(fgMsg.deleteMessages, dataFactory.GenMsgStreamDeleteMsg(pks, chanName))

	return *fgMsg
}

type AllocatorFactory struct {
	sync.Mutex
	r             *rand.Rand
	isvalid       bool
	random        bool
	errAllocBatch bool
}

var _ allocatorInterface = &AllocatorFactory{}

func NewAllocatorFactory(id ...UniqueID) *AllocatorFactory {
	f := &AllocatorFactory{
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
		isvalid: len(id) == 0 || (len(id) > 0 && id[0] > 0),
	}
	return f
}

func (alloc *AllocatorFactory) allocID() (UniqueID, error) {
	alloc.Lock()
	defer alloc.Unlock()

	if !alloc.isvalid {
		return -1, errors.New("allocID error")
	}

	if alloc.random {
		return alloc.r.Int63n(10000), nil
	}

	return 19530, nil
}

func (alloc *AllocatorFactory) allocIDBatch(count uint32) (UniqueID, uint32, error) {
	if count == 0 || alloc.errAllocBatch {
		return 0, 0, errors.New("count should be greater than zero")
	}

	start, err := alloc.allocID()
	return start, count, err
}

func (alloc *AllocatorFactory) genKey(ids ...UniqueID) (string, error) {
	idx, err := alloc.allocID()
	if err != nil {
		return "", err
	}
	ids = append(ids, idx)
	return JoinIDPath(ids...), nil
}

// If id == 0, AllocID will return not successful status
// If id == -1, AllocID will return err
func (m *RootCoordFactory) setID(id UniqueID) {
	m.ID = id // GOOSE TODO: random ID generator
}

func (m *RootCoordFactory) setCollectionID(id UniqueID) {
	m.collectionID = id
}

func (m *RootCoordFactory) setCollectionName(name string) {
	m.collectionName = name
}

func (m *RootCoordFactory) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	resp := &rootcoordpb.AllocIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}}

	if m.ID == 0 {
		resp.Status.Reason = "Zero ID"
		return resp, nil
	}

	if m.ID == -1 {
		resp.Status.ErrorCode = commonpb.ErrorCode_Success
		return resp, errors.New(resp.Status.GetReason())
	}

	resp.ID = m.ID
	resp.Count = in.GetCount()
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

func (m *RootCoordFactory) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	resp := &rootcoordpb.AllocTimestampResponse{
		Status:    &commonpb.Status{},
		Timestamp: 1000,
	}
	return resp, nil
}

func (m *RootCoordFactory) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	resp := &milvuspb.ShowCollectionsResponse{
		Status:          &commonpb.Status{},
		CollectionNames: []string{m.collectionName},
	}
	return resp, nil

}

func (m *RootCoordFactory) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	f := MetaFactory{}
	meta := f.GetCollectionMeta(m.collectionID, m.collectionName)
	resp := &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if m.collectionID == -2 {
		resp.Status.Reason = "Status not success"
		return resp, nil
	}

	if m.collectionID == -1 {
		resp.Status.ErrorCode = commonpb.ErrorCode_Success
		return resp, errors.New(resp.Status.GetReason())
	}

	resp.CollectionID = m.collectionID
	resp.Schema = meta.Schema
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

func (m *RootCoordFactory) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State:              &internalpb.ComponentInfo{},
		SubcomponentStates: make([]*internalpb.ComponentInfo, 0),
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

// FailMessageStreamFactory mock MessageStreamFactory failure
type FailMessageStreamFactory struct {
	msgstream.Factory
}

func (f *FailMessageStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return nil, errors.New("mocked failure")
}

func (f *FailMessageStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return nil, errors.New("mocked failure")
}

func genInsertDataWithPKs(PKs [2]int64) *InsertData {
	iD := genInsertData()
	iD.Data[106].(*s.Int64FieldData).Data = PKs[:]

	return iD
}

func genInsertData() *InsertData {
	return &InsertData{
		Data: map[int64]s.FieldData{
			0: &s.Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{11, 22},
			},
			1: &s.Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{3, 4},
			},
			100: &s.FloatVectorFieldData{
				NumRows: []int64{2},
				Data:    []float32{1.0, 6.0, 7.0, 8.0},
				Dim:     2,
			},
			101: &s.BinaryVectorFieldData{
				NumRows: []int64{2},
				Data:    []byte{0, 255, 255, 255, 128, 128, 128, 0},
				Dim:     32,
			},
			102: &s.BoolFieldData{
				NumRows: []int64{2},
				Data:    []bool{true, false},
			},
			103: &s.Int8FieldData{
				NumRows: []int64{2},
				Data:    []int8{5, 6},
			},
			104: &s.Int16FieldData{
				NumRows: []int64{2},
				Data:    []int16{7, 8},
			},
			105: &s.Int32FieldData{
				NumRows: []int64{2},
				Data:    []int32{9, 10},
			},
			106: &s.Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{1, 2},
			},
			107: &s.FloatFieldData{
				NumRows: []int64{2},
				Data:    []float32{2.333, 2.334},
			},
			108: &s.DoubleFieldData{
				NumRows: []int64{2},
				Data:    []float64{3.333, 3.334},
			},
		}}
}

func genEmptyInsertData() *InsertData {
	return &InsertData{
		Data: map[int64]s.FieldData{
			0: &s.Int64FieldData{
				NumRows: []int64{0},
				Data:    []int64{},
			},
			1: &s.Int64FieldData{
				NumRows: []int64{0},
				Data:    []int64{},
			},
			100: &s.FloatVectorFieldData{
				NumRows: []int64{0},
				Data:    []float32{},
				Dim:     2,
			},
			101: &s.BinaryVectorFieldData{
				NumRows: []int64{0},
				Data:    []byte{},
				Dim:     32,
			},
			102: &s.BoolFieldData{
				NumRows: []int64{0},
				Data:    []bool{},
			},
			103: &s.Int8FieldData{
				NumRows: []int64{0},
				Data:    []int8{},
			},
			104: &s.Int16FieldData{
				NumRows: []int64{0},
				Data:    []int16{},
			},
			105: &s.Int32FieldData{
				NumRows: []int64{0},
				Data:    []int32{},
			},
			106: &s.Int64FieldData{
				NumRows: []int64{0},
				Data:    []int64{},
			},
			107: &s.FloatFieldData{
				NumRows: []int64{0},
				Data:    []float32{},
			},
			108: &s.DoubleFieldData{
				NumRows: []int64{0},
				Data:    []float64{},
			},
		}}
}

func genInsertDataWithExpiredTS() *InsertData {
	return &InsertData{
		Data: map[int64]s.FieldData{
			0: &s.Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{11, 22},
			},
			1: &s.Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{329749364736000000, 329500223078400000}, // 2009-11-10 23:00:00 +0000 UTC, 2009-10-31 23:00:00 +0000 UTC
			},
			100: &s.FloatVectorFieldData{
				NumRows: []int64{2},
				Data:    []float32{1.0, 6.0, 7.0, 8.0},
				Dim:     2,
			},
			101: &s.BinaryVectorFieldData{
				NumRows: []int64{2},
				Data:    []byte{0, 255, 255, 255, 128, 128, 128, 0},
				Dim:     32,
			},
			102: &s.BoolFieldData{
				NumRows: []int64{2},
				Data:    []bool{true, false},
			},
			103: &s.Int8FieldData{
				NumRows: []int64{2},
				Data:    []int8{5, 6},
			},
			104: &s.Int16FieldData{
				NumRows: []int64{2},
				Data:    []int16{7, 8},
			},
			105: &s.Int32FieldData{
				NumRows: []int64{2},
				Data:    []int32{9, 10},
			},
			106: &s.Int64FieldData{
				NumRows: []int64{2},
				Data:    []int64{1, 2},
			},
			107: &s.FloatFieldData{
				NumRows: []int64{2},
				Data:    []float32{2.333, 2.334},
			},
			108: &s.DoubleFieldData{
				NumRows: []int64{2},
				Data:    []float64{3.333, 3.334},
			},
		}}
}

func genTimestamp() typeutil.Timestamp {
	// Generate birthday of Golang
	gb := time.Date(2009, time.Month(11), 10, 23, 0, 0, 0, time.UTC)
	return tsoutil.ComposeTSByTime(gb, 0)
}
