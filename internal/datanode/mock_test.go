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
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/datanode/writebuffer"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const ctxTimeInMillisecond = 5000

// As used in data_sync_service_test.go
var segID2SegInfo = map[int64]*datapb.SegmentInfo{
	1: {
		ID:            1,
		CollectionID:  1,
		PartitionID:   1,
		InsertChannel: "by-dev-rootcoord-dml-test_v1",
	},
	2: {
		ID:            2,
		CollectionID:  1,
		InsertChannel: "by-dev-rootcoord-dml-test_v1",
	},
	3: {
		ID:            3,
		CollectionID:  1,
		InsertChannel: "by-dev-rootcoord-dml-test_v1",
	},
}

func newIDLEDataNodeMock(ctx context.Context, pkType schemapb.DataType) *DataNode {
	factory := dependency.NewDefaultFactory(true)
	node := NewDataNode(ctx, factory)
	node.SetSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}})
	node.dispClient = msgdispatcher.NewClient(factory, typeutil.DataNodeRole, paramtable.GetNodeID())

	broker := &broker.MockBroker{}
	broker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).Return(nil).Maybe()
	broker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{}, nil).Maybe()

	node.broker = broker
	node.timeTickSender = newTimeTickSender(node.broker, 0)

	syncMgr, _ := syncmgr.NewSyncManager(node.chunkManager, node.allocator)

	node.syncMgr = syncMgr
	node.writeBufferManager = writebuffer.NewManager(node.syncMgr)

	return node
}

func newTestEtcdKV() (kv.WatchKV, error) {
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	if err != nil {
		return nil, err
	}

	return etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath.GetValue()), nil
}

func clearEtcd(rootPath string) error {
	client, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
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

type MetaFactory struct{}

func NewMetaFactory() *MetaFactory {
	return &MetaFactory{}
}

type DataFactory struct {
	rawData    []byte
	columnData []*schemapb.FieldData
}

type RootCoordFactory struct {
	types.RootCoordClient
	ID             UniqueID
	collectionName string
	collectionID   UniqueID
	pkType         schemapb.DataType

	ReportImportErr        bool
	ReportImportNotSuccess bool

	ShowPartitionsErr        bool
	ShowPartitionsNotSuccess bool
	ShowPartitionsNames      []string
	ShowPartitionsIDs        []int64
}

type DataCoordFactory struct {
	types.DataCoordClient

	SaveBinlogPathError  bool
	SaveBinlogPathStatus *commonpb.Status

	CompleteCompactionError      bool
	CompleteCompactionNotSuccess bool
	DropVirtualChannelError      bool

	DropVirtualChannelStatus commonpb.ErrorCode

	GetSegmentInfosError      bool
	GetSegmentInfosNotSuccess bool
	UserSegmentInfo           map[int64]*datapb.SegmentInfo

	AddSegmentError      bool
	AddSegmentNotSuccess bool
	AddSegmentEmpty      bool

	ReportDataNodeTtMsgsError      bool
	ReportDataNodeTtMsgsNotSuccess bool
}

func (ds *DataCoordFactory) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	if ds.AddSegmentError {
		return nil, errors.New("Error")
	}
	res := &datapb.AssignSegmentIDResponse{
		Status: merr.Success(),
		SegIDAssignments: []*datapb.SegmentIDAssignment{
			{
				SegID: 666,
			},
		},
	}
	if ds.AddSegmentNotSuccess {
		res.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}
	} else if ds.AddSegmentEmpty {
		res.SegIDAssignments = []*datapb.SegmentIDAssignment{}
	}
	return res, nil
}

func (ds *DataCoordFactory) CompleteCompaction(ctx context.Context, req *datapb.CompactionPlanResult, opts ...grpc.CallOption) (*commonpb.Status, error) {
	if ds.CompleteCompactionError {
		return nil, errors.New("Error")
	}
	if ds.CompleteCompactionNotSuccess {
		return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}, nil
	}

	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (ds *DataCoordFactory) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	if ds.SaveBinlogPathError {
		return nil, errors.New("Error")
	}
	return ds.SaveBinlogPathStatus, nil
}

func (ds *DataCoordFactory) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
	if ds.DropVirtualChannelError {
		return nil, errors.New("error")
	}
	return &datapb.DropVirtualChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: ds.DropVirtualChannelStatus,
		},
	}, nil
}

func (ds *DataCoordFactory) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (ds *DataCoordFactory) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (ds *DataCoordFactory) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	if ds.ReportDataNodeTtMsgsError {
		return nil, errors.New("mock ReportDataNodeTtMsgs error")
	}
	if ds.ReportDataNodeTtMsgsNotSuccess {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, nil
	}
	return merr.Success(), nil
}

func (ds *DataCoordFactory) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (ds *DataCoordFactory) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (ds *DataCoordFactory) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return &milvuspb.CheckHealthResponse{
		IsHealthy: true,
	}, nil
}

func (ds *DataCoordFactory) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	if ds.GetSegmentInfosError {
		return nil, errors.New("mock get segment info error")
	}
	if ds.GetSegmentInfosNotSuccess {
		return &datapb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mock GetSegmentInfo failed",
			},
		}, nil
	}
	var segmentInfos []*datapb.SegmentInfo
	for _, segmentID := range req.SegmentIDs {
		if segInfo, ok := ds.UserSegmentInfo[segmentID]; ok {
			segmentInfos = append(segmentInfos, segInfo)
		} else if segInfo, ok := segID2SegInfo[segmentID]; ok {
			segmentInfos = append(segmentInfos, segInfo)
		} else {
			segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
				ID:           segmentID,
				CollectionID: 1,
			})
		}
	}
	return &datapb.GetSegmentInfoResponse{
		Status: merr.Success(),
		Infos:  segmentInfos,
	}, nil
}

func (mf *MetaFactory) GetCollectionMeta(collectionID UniqueID, collectionName string, pkDataType schemapb.DataType) *etcdpb.CollectionMeta {
	sch := schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "test collection by meta factory",
		AutoID:      true,
	}
	sch.Fields = mf.GetFieldSchema()
	for _, field := range sch.Fields {
		if field.GetDataType() == pkDataType && field.FieldID >= 100 {
			field.IsPrimaryKey = true
		}
	}

	return &etcdpb.CollectionMeta{
		ID:           collectionID,
		Schema:       &sch,
		CreateTime:   Timestamp(1),
		SegmentIDs:   make([]UniqueID, 0),
		PartitionIDs: []UniqueID{0},
	}
}

func (mf *MetaFactory) GetFieldSchema() []*schemapb.FieldSchema {
	fields := []*schemapb.FieldSchema{
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
					Key:   common.DimKey,
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
					Key:   common.DimKey,
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
			FieldID:     106,
			Name:        "int64_field",
			Description: "field 106",
			DataType:    schemapb.DataType_Int64,
			TypeParams:  []*commonpb.KeyValuePair{},
			IndexParams: []*commonpb.KeyValuePair{},
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
		{
			FieldID:     109,
			Name:        "varChar_field",
			Description: "field 109",
			DataType:    schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "100",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{},
		},
	}

	return fields
}

func NewDataFactory() *DataFactory {
	return &DataFactory{rawData: GenRowData(), columnData: GenColumnData()}
}

func GenRowData() (rawData []byte) {
	const DIM = 2

	// Float vector
	fvector := [DIM]float32{1, 2}
	for _, ele := range fvector {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}

	// Binary vector
	// Dimension of binary vector is 32
	// size := 4,  = 32 / 8
	bvector := []byte{255, 255, 255, 0}
	rawData = append(rawData, bvector...)

	// Bool
	fieldBool := true
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
	datafloat64 := 2.2
	bfloat64 := new(bytes.Buffer)
	if err := binary.Write(bfloat64, common.Endian, datafloat64); err != nil {
		panic(err)
	}
	rawData = append(rawData, bfloat64.Bytes()...)
	log.Debug("Rawdata length:", zap.Int("Length of rawData", len(rawData)))
	return
}

func GenColumnData() (fieldsData []*schemapb.FieldData) {
	// Float vector
	fVector := []float32{1, 2}
	floatVectorData := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: "float_vector_field",
		FieldId:   100,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 2,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: fVector,
					},
				},
			},
		},
	}
	fieldsData = append(fieldsData, floatVectorData)

	// Binary vector
	// Dimension of binary vector is 32
	// size := 4,  = 32 / 8
	binaryVector := []byte{255, 255, 255, 0}
	binaryVectorData := &schemapb.FieldData{
		Type:      schemapb.DataType_BinaryVector,
		FieldName: "binary_vector_field",
		FieldId:   101,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 32,
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: binaryVector,
				},
			},
		},
	}
	fieldsData = append(fieldsData, binaryVectorData)

	// bool
	boolData := []bool{true}
	boolFieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Bool,
		FieldName: "bool_field",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: boolData,
					},
				},
			},
		},
	}
	fieldsData = append(fieldsData, boolFieldData)

	// int8
	int8Data := []int32{100}
	int8FieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Int8,
		FieldName: "int8_field",
		FieldId:   103,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: int8Data,
					},
				},
			},
		},
	}
	fieldsData = append(fieldsData, int8FieldData)

	// int16
	int16Data := []int32{200}
	int16FieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Int16,
		FieldName: "int16_field",
		FieldId:   104,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: int16Data,
					},
				},
			},
		},
	}
	fieldsData = append(fieldsData, int16FieldData)

	// int32
	int32Data := []int32{300}
	int32FieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: "int32_field",
		FieldId:   105,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: int32Data,
					},
				},
			},
		},
	}
	fieldsData = append(fieldsData, int32FieldData)

	// int64
	int64Data := []int64{400}
	int64FieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "int64_field",
		FieldId:   106,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: int64Data,
					},
				},
			},
		},
	}
	fieldsData = append(fieldsData, int64FieldData)

	// float
	floatData := []float32{1.1}
	floatFieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: "float32_field",
		FieldId:   107,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: floatData,
					},
				},
			},
		},
	}
	fieldsData = append(fieldsData, floatFieldData)

	// double
	doubleData := []float64{2.2}
	doubleFieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Double,
		FieldName: "float64_field",
		FieldId:   108,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: doubleData,
					},
				},
			},
		},
	}
	fieldsData = append(fieldsData, doubleFieldData)

	// var char
	varCharData := []string{"test"}
	varCharFieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "varChar_field",
		FieldId:   109,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: varCharData,
					},
				},
			},
		},
	}
	fieldsData = append(fieldsData, varCharFieldData)

	return
}

func (df *DataFactory) GenMsgStreamInsertMsg(idx int, chanName string) *msgstream.InsertMsg {
	msg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{uint32(idx)},
		},
		InsertRequest: &msgpb.InsertRequest{
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
			// RowData:        []*commonpb.Blob{{Value: df.rawData}},
			FieldsData: df.columnData,
			Version:    msgpb.InsertDataVersion_ColumnBased,
			NumRows:    1,
		},
	}
	return msg
}

func (df *DataFactory) GenMsgStreamInsertMsgWithTs(idx int, chanName string, ts Timestamp) *msgstream.InsertMsg {
	msg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues:     []uint32{uint32(idx)},
			BeginTimestamp: ts,
			EndTimestamp:   ts,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     0,
				Timestamp: ts,
				SourceID:  0,
			},
			CollectionName: "col1",
			PartitionName:  "default",
			SegmentID:      1,
			CollectionID:   UniqueID(0),
			ShardName:      chanName,
			Timestamps:     []Timestamp{ts},
			RowIDs:         []UniqueID{UniqueID(idx)},
			// RowData:        []*commonpb.Blob{{Value: df.rawData}},
			FieldsData: df.columnData,
			Version:    msgpb.InsertDataVersion_ColumnBased,
			NumRows:    1,
		},
	}
	return msg
}

func (df *DataFactory) GetMsgStreamTsInsertMsgs(n int, chanName string, ts Timestamp) (inMsgs []msgstream.TsMsg) {
	for i := 0; i < n; i++ {
		msg := df.GenMsgStreamInsertMsgWithTs(i, chanName, ts)
		var tsMsg msgstream.TsMsg = msg
		inMsgs = append(inMsgs, tsMsg)
	}
	return
}

func (df *DataFactory) GetMsgStreamInsertMsgs(n int) (msgs []*msgstream.InsertMsg) {
	for i := 0; i < n; i++ {
		msg := df.GenMsgStreamInsertMsg(i, "")
		msgs = append(msgs, msg)
	}
	return
}

func (df *DataFactory) GenMsgStreamDeleteMsg(pks []storage.PrimaryKey, chanName string) *msgstream.DeleteMsg {
	idx := 100
	timestamps := make([]Timestamp, len(pks))
	for i := 0; i < len(pks); i++ {
		timestamps[i] = Timestamp(i) + 1000
	}
	msg := &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{uint32(idx)},
		},
		DeleteRequest: &msgpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Delete,
				MsgID:     0,
				Timestamp: Timestamp(idx + 1000),
				SourceID:  0,
			},
			CollectionName: "col1",
			PartitionName:  "default",
			PartitionID:    1,
			ShardName:      chanName,
			PrimaryKeys:    storage.ParsePrimaryKeys2IDs(pks),
			Timestamps:     timestamps,
			NumRows:        int64(len(pks)),
		},
	}
	return msg
}

func (df *DataFactory) GenMsgStreamDeleteMsgWithTs(idx int, pks []storage.PrimaryKey, chanName string, ts Timestamp) *msgstream.DeleteMsg {
	msg := &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues:     []uint32{uint32(idx)},
			BeginTimestamp: ts,
			EndTimestamp:   ts,
		},
		DeleteRequest: &msgpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Delete,
				MsgID:     1,
				Timestamp: ts,
				SourceID:  0,
			},
			CollectionName: "col1",
			PartitionName:  "default",
			PartitionID:    1,
			CollectionID:   UniqueID(0),
			ShardName:      chanName,
			PrimaryKeys:    storage.ParsePrimaryKeys2IDs(pks),
			Timestamps:     []Timestamp{ts},
			NumRows:        int64(len(pks)),
		},
	}
	return msg
}

func genFlowGraphInsertMsg(chanName string) flowGraphMsg {
	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	startPos := []*msgpb.MsgPosition{
		{
			ChannelName: chanName,
			MsgID:       make([]byte, 0),
			Timestamp:   tsoutil.ComposeTSByTime(time.Now(), 0),
		},
	}

	fgMsg := &flowGraphMsg{
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

func genFlowGraphDeleteMsg(pks []storage.PrimaryKey, chanName string) flowGraphMsg {
	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	startPos := []*msgpb.MsgPosition{
		{
			ChannelName: chanName,
			MsgID:       make([]byte, 0),
			Timestamp:   0,
		},
	}

	fgMsg := &flowGraphMsg{
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

func (m *RootCoordFactory) setCollectionID(id UniqueID) {
	m.collectionID = id
}

func (m *RootCoordFactory) setCollectionName(name string) {
	m.collectionName = name
}

func (m *RootCoordFactory) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	resp := &rootcoordpb.AllocIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if in.Count == 12 {
		resp.Status.ErrorCode = commonpb.ErrorCode_Success
		resp.ID = 1
		resp.Count = 12
	}

	if m.ID == 0 {
		resp.Status.Reason = "Zero ID"
		return resp, nil
	}

	if m.ID == -1 {
		return nil, merr.Error(resp.Status)
	}

	resp.ID = m.ID
	resp.Count = in.GetCount()
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

func (m *RootCoordFactory) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	resp := &rootcoordpb.AllocTimestampResponse{
		Status:    &commonpb.Status{},
		Timestamp: 1000,
	}

	v := ctx.Value(ctxKey{})
	if v != nil && v.(string) == returnError {
		resp.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		return resp, fmt.Errorf("injected error")
	}

	return resp, nil
}

func (m *RootCoordFactory) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	resp := &milvuspb.ShowCollectionsResponse{
		Status:          &commonpb.Status{},
		CollectionNames: []string{m.collectionName},
	}
	return resp, nil
}

func (m *RootCoordFactory) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	f := MetaFactory{}
	meta := f.GetCollectionMeta(m.collectionID, m.collectionName, m.pkType)
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
		return nil, merr.Error(resp.Status)
	}

	resp.CollectionID = m.collectionID
	resp.Schema = meta.Schema
	resp.ShardsNum = common.DefaultShardsNum
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

func (m *RootCoordFactory) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	if m.ShowPartitionsErr {
		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Success(),
		}, fmt.Errorf("mock show partitions error")
	}

	if m.ShowPartitionsNotSuccess {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "not success",
			},
		}, nil
	}

	return &milvuspb.ShowPartitionsResponse{
		Status:         merr.Success(),
		PartitionNames: m.ShowPartitionsNames,
		PartitionIDs:   m.ShowPartitionsIDs,
	}, nil
}

func (m *RootCoordFactory) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State:              &milvuspb.ComponentInfo{},
		SubcomponentStates: make([]*milvuspb.ComponentInfo, 0),
		Status:             merr.Success(),
	}, nil
}

// FailMessageStreamFactory mock MessageStreamFactory failure
type FailMessageStreamFactory struct {
	dependency.Factory
}

func (f *FailMessageStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return nil, errors.New("mocked failure")
}

func (f *FailMessageStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return nil, errors.New("mocked failure")
}

func genInsertDataWithPKs(PKs [2]storage.PrimaryKey, dataType schemapb.DataType) *InsertData {
	iD := genInsertData(2)
	switch dataType {
	case schemapb.DataType_Int64:
		values := make([]int64, len(PKs))
		for index, pk := range PKs {
			values[index] = pk.(*storage.Int64PrimaryKey).Value
		}
		iD.Data[106].(*storage.Int64FieldData).Data = values
	case schemapb.DataType_VarChar:
		values := make([]string, len(PKs))
		for index, pk := range PKs {
			values[index] = pk.(*storage.VarCharPrimaryKey).Value
		}
		iD.Data[109].(*storage.StringFieldData).Data = values
	default:
		// TODO::
	}
	return iD
}

func genTestStat(meta *etcdpb.CollectionMeta) *storage.PrimaryKeyStats {
	var pkFieldID, pkFieldType int64
	for _, field := range meta.Schema.Fields {
		if field.IsPrimaryKey {
			pkFieldID = field.FieldID
			pkFieldType = int64(field.DataType)
		}
	}
	stats, _ := storage.NewPrimaryKeyStats(pkFieldID, pkFieldType, 100)
	return stats
}

func genInsertData(rowNum int) *InsertData {
	return &InsertData{
		Data: map[int64]storage.FieldData{
			0: &storage.Int64FieldData{
				Data: lo.RepeatBy(rowNum, func(i int) int64 { return int64(i + 1) }),
			},
			1: &storage.Int64FieldData{
				Data: lo.RepeatBy(rowNum, func(i int) int64 { return int64(i + 3) }),
			},
			100: &storage.FloatVectorFieldData{
				Data: lo.RepeatBy(rowNum*2, func(i int) float32 { return rand.Float32() }),
				Dim:  2,
			},
			101: &storage.BinaryVectorFieldData{
				Data: lo.RepeatBy(rowNum*4, func(i int) byte { return byte(rand.Intn(256)) }),
				Dim:  32,
			},
			102: &storage.BoolFieldData{
				Data: lo.RepeatBy(rowNum, func(i int) bool { return i%2 == 0 }),
			},
			103: &storage.Int8FieldData{
				Data: lo.RepeatBy(rowNum, func(i int) int8 { return int8(i) }),
			},
			104: &storage.Int16FieldData{
				Data: lo.RepeatBy(rowNum, func(i int) int16 { return int16(i) }),
			},
			105: &storage.Int32FieldData{
				Data: lo.RepeatBy(rowNum, func(i int) int32 { return int32(i) }),
			},
			106: &storage.Int64FieldData{
				Data: lo.RepeatBy(rowNum, func(i int) int64 { return int64(i) }),
			},
			107: &storage.FloatFieldData{
				Data: lo.RepeatBy(rowNum, func(i int) float32 { return rand.Float32() }),
			},
			108: &storage.DoubleFieldData{
				Data: lo.RepeatBy(rowNum, func(i int) float64 { return rand.Float64() }),
			},
			109: &storage.StringFieldData{
				Data: lo.RepeatBy(rowNum, func(i int) string { return fmt.Sprintf("test%d", i) }),
			},
		},
	}
}

func genEmptyInsertData() *InsertData {
	return &InsertData{
		Data: map[int64]storage.FieldData{
			0: &storage.Int64FieldData{
				Data: []int64{},
			},
			1: &storage.Int64FieldData{
				Data: []int64{},
			},
			100: &storage.FloatVectorFieldData{
				Data: []float32{},
				Dim:  2,
			},
			101: &storage.BinaryVectorFieldData{
				Data: []byte{},
				Dim:  32,
			},
			102: &storage.BoolFieldData{
				Data: []bool{},
			},
			103: &storage.Int8FieldData{
				Data: []int8{},
			},
			104: &storage.Int16FieldData{
				Data: []int16{},
			},
			105: &storage.Int32FieldData{
				Data: []int32{},
			},
			106: &storage.Int64FieldData{
				Data: []int64{},
			},
			107: &storage.FloatFieldData{
				Data: []float32{},
			},
			108: &storage.DoubleFieldData{
				Data: []float64{},
			},
			109: &storage.StringFieldData{
				Data: []string{},
			},
		},
	}
}

func genTestTickler() *etcdTickler {
	return newEtcdTickler(0, "", nil, nil, 0)
}

// MockDataSuiteBase compose some mock dependency to generate test dataset
type MockDataSuiteBase struct {
	schema *schemapb.CollectionSchema
}

func (s *MockDataSuiteBase) prepareData() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.StartOfUserFieldID, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
			{FieldID: common.StartOfUserFieldID + 1, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			}},
		},
	}
}

func EmptyBfsFactory(info *datapb.SegmentInfo) *metacache.BloomFilterSet {
	return metacache.NewBloomFilterSet()
}
