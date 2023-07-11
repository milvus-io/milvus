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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	s "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const ctxTimeInMillisecond = 5000

const debug = false

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

var emptyFlushAndDropFunc flushAndDropFunc = func(_ []*segmentFlushPack) {}

func newIDLEDataNodeMock(ctx context.Context, pkType schemapb.DataType) *DataNode {
	factory := dependency.NewDefaultFactory(true)
	node := NewDataNode(ctx, factory)
	node.SetSession(&sessionutil.Session{ServerID: 1})
	node.dispClient = msgdispatcher.NewClient(factory, typeutil.DataNodeRole, paramtable.GetNodeID())

	rc := &RootCoordFactory{
		ID:             0,
		collectionID:   1,
		collectionName: "collection-1",
		pkType:         pkType,
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

	factory := dependency.NewDefaultFactory(true)
	node := NewDataNode(ctx, factory)

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

type MetaFactory struct {
}

func NewMetaFactory() *MetaFactory {
	return &MetaFactory{}
}

type DataFactory struct {
	rawData    []byte
	columnData []*schemapb.FieldData
}

type RootCoordFactory struct {
	types.RootCoord
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
	types.DataCoord

	SaveBinlogPathError  bool
	SaveBinlogPathStatus commonpb.ErrorCode

	CompleteCompactionError      bool
	CompleteCompactionNotSuccess bool

	DropVirtualChannelError  bool
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

func (ds *DataCoordFactory) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	if ds.AddSegmentError {
		return nil, errors.New("Error")
	}
	res := &datapb.AssignSegmentIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
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
	return &commonpb.Status{ErrorCode: ds.SaveBinlogPathStatus}, nil
}

func (ds *DataCoordFactory) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	if ds.DropVirtualChannelError {
		return nil, errors.New("error")
	}
	return &datapb.DropVirtualChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: ds.DropVirtualChannelStatus,
		},
	}, nil
}

func (ds *DataCoordFactory) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (ds *DataCoordFactory) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (ds *DataCoordFactory) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	if ds.ReportDataNodeTtMsgsError {
		return nil, errors.New("mock ReportDataNodeTtMsgs error")
	}
	if ds.ReportDataNodeTtMsgsNotSuccess {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (ds *DataCoordFactory) SaveImportSegment(ctx context.Context, req *datapb.SaveImportSegmentRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (ds *DataCoordFactory) UnsetIsImportingState(context.Context, *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (ds *DataCoordFactory) MarkSegmentsDropped(context.Context, *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (ds *DataCoordFactory) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (ds *DataCoordFactory) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return &milvuspb.CheckHealthResponse{
		IsHealthy: true,
	}, nil
}

func (ds *DataCoordFactory) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
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
				ID: segmentID,
			})
		}
	}
	return &datapb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Infos: segmentInfos,
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

func GenColumnData() (fieldsData []*schemapb.FieldData) {
	// Float vector
	var fVector = []float32{1, 2}
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

	//double
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

	//var char
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
	var msg = &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{uint32(idx)},
		},
		InsertRequest: msgpb.InsertRequest{
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
	var msg = &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues:     []uint32{uint32(idx)},
			BeginTimestamp: ts,
			EndTimestamp:   ts,
		},
		InsertRequest: msgpb.InsertRequest{
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
		var msg = df.GenMsgStreamInsertMsgWithTs(i, chanName, ts)
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

func (df *DataFactory) GenMsgStreamDeleteMsg(pks []primaryKey, chanName string) *msgstream.DeleteMsg {
	idx := 100
	timestamps := make([]Timestamp, len(pks))
	for i := 0; i < len(pks); i++ {
		timestamps[i] = Timestamp(i) + 1000
	}
	var msg = &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{uint32(idx)},
		},
		DeleteRequest: msgpb.DeleteRequest{
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
			PrimaryKeys:    s.ParsePrimaryKeys2IDs(pks),
			Timestamps:     timestamps,
			NumRows:        int64(len(pks)),
		},
	}
	return msg
}

func (df *DataFactory) GenMsgStreamDeleteMsgWithTs(idx int, pks []primaryKey, chanName string, ts Timestamp) *msgstream.DeleteMsg {
	var msg = &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues:     []uint32{uint32(idx)},
			BeginTimestamp: ts,
			EndTimestamp:   ts,
		},
		DeleteRequest: msgpb.DeleteRequest{
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
			PrimaryKeys:    s.ParsePrimaryKeys2IDs(pks),
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

func genFlowGraphDeleteMsg(pks []primaryKey, chanName string) flowGraphMsg {
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
		return nil, errors.New(resp.Status.GetReason())
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

	v := ctx.Value(ctxKey{})
	if v != nil && v.(string) == returnError {
		resp.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		return resp, fmt.Errorf("injected error")
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

func (m *RootCoordFactory) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
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
		resp.Status.ErrorCode = commonpb.ErrorCode_Success
		return resp, errors.New(resp.Status.GetReason())
	}

	resp.CollectionID = m.collectionID
	resp.Schema = meta.Schema
	resp.ShardsNum = common.DefaultShardsNum
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

func (m *RootCoordFactory) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if m.ShowPartitionsErr {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
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
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionNames: m.ShowPartitionsNames,
		PartitionIDs:   m.ShowPartitionsIDs,
	}, nil
}

func (m *RootCoordFactory) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State:              &milvuspb.ComponentInfo{},
		SubcomponentStates: make([]*milvuspb.ComponentInfo, 0),
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (m *RootCoordFactory) ReportImport(ctx context.Context, req *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	if ctx != nil && ctx.Value(ctxKey{}) != nil {
		if v := ctx.Value(ctxKey{}).(string); v == returnError {
			return nil, fmt.Errorf("injected error")
		}
	}
	if m.ReportImportErr {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, fmt.Errorf("mock report import error")
	}
	if m.ReportImportNotSuccess {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
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

func genInsertDataWithPKs(PKs [2]primaryKey, dataType schemapb.DataType) *InsertData {
	iD := genInsertData()
	switch dataType {
	case schemapb.DataType_Int64:
		values := make([]int64, len(PKs))
		for index, pk := range PKs {
			values[index] = pk.(*int64PrimaryKey).Value
		}
		iD.Data[106].(*s.Int64FieldData).Data = values
	case schemapb.DataType_VarChar:
		values := make([]string, len(PKs))
		for index, pk := range PKs {
			values[index] = pk.(*varCharPrimaryKey).Value
		}
		iD.Data[109].(*s.StringFieldData).Data = values
	default:
		//TODO::
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
	return storage.NewPrimaryKeyStats(pkFieldID, pkFieldType, 0)
}

func genInsertData() *InsertData {
	return &InsertData{
		Data: map[int64]s.FieldData{
			0: &s.Int64FieldData{
				Data: []int64{1, 2},
			},
			1: &s.Int64FieldData{
				Data: []int64{3, 4},
			},
			100: &s.FloatVectorFieldData{
				Data: []float32{1.0, 6.0, 7.0, 8.0},
				Dim:  2,
			},
			101: &s.BinaryVectorFieldData{
				Data: []byte{0, 255, 255, 255, 128, 128, 128, 0},
				Dim:  32,
			},
			102: &s.BoolFieldData{
				Data: []bool{true, false},
			},
			103: &s.Int8FieldData{
				Data: []int8{5, 6},
			},
			104: &s.Int16FieldData{
				Data: []int16{7, 8},
			},
			105: &s.Int32FieldData{
				Data: []int32{9, 10},
			},
			106: &s.Int64FieldData{
				Data: []int64{1, 2},
			},
			107: &s.FloatFieldData{
				Data: []float32{2.333, 2.334},
			},
			108: &s.DoubleFieldData{
				Data: []float64{3.333, 3.334},
			},
			109: &s.StringFieldData{
				Data: []string{"test1", "test2"},
			},
		}}
}

func genEmptyInsertData() *InsertData {
	return &InsertData{
		Data: map[int64]s.FieldData{
			0: &s.Int64FieldData{
				Data: []int64{},
			},
			1: &s.Int64FieldData{
				Data: []int64{},
			},
			100: &s.FloatVectorFieldData{
				Data: []float32{},
				Dim:  2,
			},
			101: &s.BinaryVectorFieldData{
				Data: []byte{},
				Dim:  32,
			},
			102: &s.BoolFieldData{
				Data: []bool{},
			},
			103: &s.Int8FieldData{
				Data: []int8{},
			},
			104: &s.Int16FieldData{
				Data: []int16{},
			},
			105: &s.Int32FieldData{
				Data: []int32{},
			},
			106: &s.Int64FieldData{
				Data: []int64{},
			},
			107: &s.FloatFieldData{
				Data: []float32{},
			},
			108: &s.DoubleFieldData{
				Data: []float64{},
			},
			109: &s.StringFieldData{
				Data: []string{},
			},
		}}
}

func genInsertDataWithExpiredTS() *InsertData {
	return &InsertData{
		Data: map[int64]s.FieldData{
			0: &s.Int64FieldData{
				Data: []int64{11, 22},
			},
			1: &s.Int64FieldData{
				Data: []int64{329749364736000000, 329500223078400000}, // 2009-11-10 23:00:00 +0000 UTC, 2009-10-31 23:00:00 +0000 UTC
			},
			100: &s.FloatVectorFieldData{
				Data: []float32{1.0, 6.0, 7.0, 8.0},
				Dim:  2,
			},
			101: &s.BinaryVectorFieldData{
				Data: []byte{0, 255, 255, 255, 128, 128, 128, 0},
				Dim:  32,
			},
			102: &s.BoolFieldData{
				Data: []bool{true, false},
			},
			103: &s.Int8FieldData{
				Data: []int8{5, 6},
			},
			104: &s.Int16FieldData{
				Data: []int16{7, 8},
			},
			105: &s.Int32FieldData{
				Data: []int32{9, 10},
			},
			106: &s.Int64FieldData{
				Data: []int64{1, 2},
			},
			107: &s.FloatFieldData{
				Data: []float32{2.333, 2.334},
			},
			108: &s.DoubleFieldData{
				Data: []float64{3.333, 3.334},
			},
			109: &s.StringFieldData{
				Data: []string{"test1", "test2"},
			},
		}}
}

func genTimestamp() typeutil.Timestamp {
	// Generate birthday of Golang
	gb := time.Date(2009, time.Month(11), 10, 23, 0, 0, 0, time.UTC)
	return tsoutil.ComposeTSByTime(gb, 0)
}

func genTestTickler() *tickler {
	return newTickler(0, "", nil, nil, 0)
}
