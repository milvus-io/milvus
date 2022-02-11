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

package querynode

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"strconv"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/indexnode"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

// ---------- unittest util functions ----------
// common definitions
const ctxTimeInMillisecond = 500
const debugUT = false

const (
	dimKey        = "dim"
	metricTypeKey = "metric_type"

	defaultVecFieldName   = "vec"
	defaultConstFieldName = "const"
	defaultPKFieldName    = "pk"
	defaultTopK           = int64(10)
	defaultRoundDecimal   = int64(6)
	defaultDim            = 128
	defaultNProb          = 10
	defaultMetricType     = "JACCARD"

	defaultDMLChannel   = "query-node-unittest-DML-0"
	defaultDeltaChannel = "query-node-unittest-delta-channel-0"
	defaultSubName      = "query-node-unittest-sub-name-0"
)

const (
	defaultCollectionID = UniqueID(0)
	defaultPartitionID  = UniqueID(1)
	defaultSegmentID    = UniqueID(2)

	defaultCollectionName = "query-node-unittest-default-collection"
	defaultPartitionName  = "query-node-unittest-default-partition"
)

const (
	defaultMsgLength = 100
	defaultDelLength = 10
)

const (
	buildID   = UniqueID(0)
	indexID   = UniqueID(0)
	fieldID   = UniqueID(100)
	indexName = "query-node-index-0"
)

// ---------- unittest util functions ----------
// functions of init meta and generate meta
type vecFieldParam struct {
	id         int64
	dim        int
	metricType string
	vecType    schemapb.DataType
}

type constFieldParam struct {
	id       int64
	dataType schemapb.DataType
}

var simpleVecField = vecFieldParam{
	id:         100,
	dim:        defaultDim,
	metricType: defaultMetricType,
	vecType:    schemapb.DataType_FloatVector,
}

var simpleConstField = constFieldParam{
	id:       101,
	dataType: schemapb.DataType_Int32,
}

var simplePKField = constFieldParam{
	id:       102,
	dataType: schemapb.DataType_Int64,
}

var uidField = constFieldParam{
	id:       rowIDFieldID,
	dataType: schemapb.DataType_Int64,
}

var timestampField = constFieldParam{
	id:       timestampFieldID,
	dataType: schemapb.DataType_Int64,
}

func genConstantField(param constFieldParam) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         defaultConstFieldName,
		IsPrimaryKey: false,
		DataType:     param.dataType,
	}
	return field
}

func genPKField(param constFieldParam) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         defaultPKFieldName,
		IsPrimaryKey: true,
		DataType:     param.dataType,
	}
	return field
}

func genFloatVectorField(param vecFieldParam) *schemapb.FieldSchema {
	fieldVec := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         defaultVecFieldName,
		IsPrimaryKey: false,
		DataType:     param.vecType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   dimKey,
				Value: strconv.Itoa(param.dim),
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   metricTypeKey,
				Value: param.metricType,
			},
		},
	}
	return fieldVec
}

func genSimpleIndexParams() map[string]string {
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_PQ"
	indexParams["index_mode"] = "cpu"
	indexParams["dim"] = strconv.FormatInt(defaultDim, 10)
	indexParams["k"] = "10"
	indexParams["nlist"] = "100"
	indexParams["nprobe"] = "10"
	indexParams["m"] = "4"
	indexParams["nbits"] = "8"
	indexParams["metric_type"] = "L2"
	indexParams["SLICE_SIZE"] = "400"
	return indexParams
}

func genIndexBinarySet() ([][]byte, error) {
	indexParams := genSimpleIndexParams()

	typeParams := make(map[string]string)
	typeParams["dim"] = strconv.Itoa(defaultDim)
	var indexRowData []float32
	for n := 0; n < defaultMsgLength; n++ {
		for i := 0; i < defaultDim; i++ {
			indexRowData = append(indexRowData, float32(n*i))
		}
	}

	index, err := indexnode.NewCIndex(typeParams, indexParams)
	if err != nil {
		return nil, err
	}

	err = index.BuildFloatVecIndexWithoutIds(indexRowData)
	if err != nil {
		return nil, err
	}

	// save index to minio
	binarySet, err := index.Serialize()
	if err != nil {
		return nil, err
	}

	bytesSet := make([][]byte, 0)
	for i := range binarySet {
		bytesSet = append(bytesSet, binarySet[i].Value)
	}
	return bytesSet, nil
}

func generateIndex(segmentID UniqueID) ([]string, error) {
	indexParams := genSimpleIndexParams()

	var indexParamsKV []*commonpb.KeyValuePair
	for key, value := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	typeParams := make(map[string]string)
	typeParams["dim"] = strconv.Itoa(defaultDim)
	var indexRowData []float32
	for n := 0; n < defaultMsgLength; n++ {
		for i := 0; i < defaultDim; i++ {
			indexRowData = append(indexRowData, float32(n*i))
		}
	}

	index, err := indexnode.NewCIndex(typeParams, indexParams)
	if err != nil {
		return nil, err
	}

	err = index.BuildFloatVecIndexWithoutIds(indexRowData)
	if err != nil {
		return nil, err
	}

	option := &minioKV.Option{
		Address:           Params.MinioCfg.Address,
		AccessKeyID:       Params.MinioCfg.AccessKeyID,
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey,
		UseSSL:            Params.MinioCfg.UseSSL,
		BucketName:        Params.MinioCfg.BucketName,
		CreateBucket:      true,
	}

	kv, err := minioKV.NewMinIOKV(context.Background(), option)
	if err != nil {
		return nil, err
	}

	// save index to minio
	binarySet, err := index.Serialize()
	if err != nil {
		return nil, err
	}

	// serialize index params
	indexCodec := storage.NewIndexFileBinlogCodec()
	serializedIndexBlobs, err := indexCodec.Serialize(
		0,
		0,
		0,
		0,
		0,
		0,
		indexParams,
		indexName,
		indexID,
		binarySet,
	)
	if err != nil {
		return nil, err
	}

	indexPaths := make([]string, 0)
	for _, index := range serializedIndexBlobs {
		p := strconv.Itoa(int(segmentID)) + "/" + index.Key
		indexPaths = append(indexPaths, p)
		err := kv.Save(p, string(index.Value))
		if err != nil {
			return nil, err
		}
	}

	return indexPaths, nil
}

func genSimpleSegCoreSchema() *schemapb.CollectionSchema {
	fieldVec := genFloatVectorField(simpleVecField)
	fieldInt := genConstantField(simpleConstField)
	fieldPK := genPKField(simplePKField)

	schema := schemapb.CollectionSchema{ // schema for segCore
		Name:   defaultCollectionName,
		AutoID: false,
		Fields: []*schemapb.FieldSchema{
			fieldVec,
			fieldInt,
			fieldPK,
		},
	}
	return &schema
}

func genSimpleInsertDataSchema() *schemapb.CollectionSchema {
	fieldUID := genConstantField(uidField)
	fieldTimestamp := genConstantField(timestampField)
	fieldVec := genFloatVectorField(simpleVecField)
	fieldInt := genConstantField(simpleConstField)

	schema := schemapb.CollectionSchema{ // schema for insertData
		Name:   defaultCollectionName,
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			fieldUID,
			fieldTimestamp,
			fieldVec,
			fieldInt,
		},
	}
	return &schema
}

func genCollectionMeta(collectionID UniqueID, schema *schemapb.CollectionSchema) *etcdpb.CollectionMeta {
	colInfo := &etcdpb.CollectionMeta{
		ID:           collectionID,
		Schema:       schema,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}
	return colInfo
}

func genSimpleCollectionMeta() *etcdpb.CollectionMeta {
	simpleSchema := genSimpleInsertDataSchema()
	return genCollectionMeta(defaultCollectionID, simpleSchema)
}

// ---------- unittest util functions ----------
// functions of third-party
func genMinioKV(ctx context.Context) (*minioKV.MinIOKV, error) {
	option := &minioKV.Option{
		Address:           Params.MinioCfg.Address,
		AccessKeyID:       Params.MinioCfg.AccessKeyID,
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey,
		UseSSL:            Params.MinioCfg.UseSSL,
		BucketName:        Params.MinioCfg.BucketName,
		CreateBucket:      true,
	}
	kv, err := minioKV.NewMinIOKV(ctx, option)
	return kv, err
}

func genEtcdKV() (*etcdkv.EtcdKV, error) {
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		return nil, err
	}
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	return etcdKV, nil
}

func genFactory() (msgstream.Factory, error) {
	const receiveBufSize = 1024

	pulsarURL := Params.PulsarCfg.Address
	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  pulsarURL,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	if err != nil {
		return nil, err
	}
	return msFactory, nil
}

func genQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	fac, err := genFactory()
	if err != nil {
		return nil, err
	}
	stream, err := fac.NewQueryMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func genLocalChunkManager() (storage.ChunkManager, error) {
	p, err := Params.Load("storage.path")
	if err != nil {
		return nil, err
	}
	lcm := storage.NewLocalChunkManager(storage.RootPath(p))

	return lcm, nil
}

func genRemoteChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return storage.NewMinioChunkManager(
		ctx,
		storage.Address(Params.MinioCfg.Address),
		storage.AccessKeyID(Params.MinioCfg.AccessKeyID),
		storage.SecretAccessKeyID(Params.MinioCfg.SecretAccessKey),
		storage.UseSSL(Params.MinioCfg.UseSSL),
		storage.BucketName(Params.MinioCfg.BucketName),
		storage.CreateBucket(true))
}

func genVectorChunkManager(ctx context.Context) (*storage.VectorChunkManager, error) {
	p, err := Params.Load("storage.path")
	if err != nil {
		return nil, err
	}
	lcm := storage.NewLocalChunkManager(storage.RootPath(p))

	rcm, err := storage.NewMinioChunkManager(
		ctx,
		storage.Address(Params.MinioCfg.Address),
		storage.AccessKeyID(Params.MinioCfg.AccessKeyID),
		storage.SecretAccessKeyID(Params.MinioCfg.SecretAccessKey),
		storage.UseSSL(Params.MinioCfg.UseSSL),
		storage.BucketName(Params.MinioCfg.BucketName),
		storage.CreateBucket(true))

	if err != nil {
		return nil, err
	}

	schema := genSimpleInsertDataSchema()
	vcm := storage.NewVectorChunkManager(lcm, rcm, &etcdpb.CollectionMeta{
		ID:     defaultCollectionID,
		Schema: schema,
	}, false)
	return vcm, nil
}

// ---------- unittest util functions ----------
// functions of inserting data init
func genInsertData(msgLength int, schema *schemapb.CollectionSchema) (*storage.InsertData, error) {
	insertData := &storage.InsertData{
		Data: make(map[int64]storage.FieldData),
	}

	for _, f := range schema.Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			data := make([]bool, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = true
			}
			insertData.Data[f.FieldID] = &storage.BoolFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Int8:
			data := make([]int8, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int8(i)
			}
			insertData.Data[f.FieldID] = &storage.Int8FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Int16:
			data := make([]int16, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int16(i)
			}
			insertData.Data[f.FieldID] = &storage.Int16FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Int32:
			data := make([]int32, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int32(i)
			}
			insertData.Data[f.FieldID] = &storage.Int32FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Int64:
			data := make([]int64, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int64(i)
			}
			insertData.Data[f.FieldID] = &storage.Int64FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Float:
			data := make([]float32, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = float32(i)
			}
			insertData.Data[f.FieldID] = &storage.FloatFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_Double:
			data := make([]float64, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = float64(i)
			}
			insertData.Data[f.FieldID] = &storage.DoubleFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
			}
		case schemapb.DataType_FloatVector:
			dim := simpleVecField.dim // if no dim specified, use simpleVecField's dim
			for _, p := range f.TypeParams {
				if p.Key == dimKey {
					var err error
					dim, err = strconv.Atoi(p.Value)
					if err != nil {
						return nil, err
					}
				}
			}
			data := make([]float32, 0)
			for i := 0; i < msgLength; i++ {
				for j := 0; j < dim; j++ {
					data = append(data, float32(i*j)*0.1)
				}
			}
			insertData.Data[f.FieldID] = &storage.FloatVectorFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    data,
				Dim:     dim,
			}
		default:
			err := errors.New("data type not supported")
			return nil, err
		}
	}

	return insertData, nil
}

func genSimpleInsertData() (*storage.InsertData, error) {
	schema := genSimpleInsertDataSchema()
	return genInsertData(defaultMsgLength, schema)
}

func genStorageBlob(collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	msgLength int,
	schema *schemapb.CollectionSchema) ([]*storage.Blob, error) {
	collMeta := genCollectionMeta(collectionID, schema)
	inCodec := storage.NewInsertCodec(collMeta)
	insertData, err := genInsertData(msgLength, schema)
	if err != nil {
		return nil, err
	}
	// timestamp field not allowed 0 timestamp
	if _, ok := insertData.Data[timestampFieldID]; ok {
		insertData.Data[timestampFieldID].(*storage.Int64FieldData).Data[0] = 1
	}
	binLogs, _, err := inCodec.Serialize(partitionID, segmentID, insertData)

	return binLogs, err
}

func genSimpleStorageBlob() ([]*storage.Blob, error) {
	schema := genSimpleInsertDataSchema()
	return genStorageBlob(defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
}

func genSimpleFloatVectors() []float32 {
	vec := make([]float32, defaultDim)
	for i := 0; i < defaultDim; i++ {
		vec[i] = rand.Float32()
	}
	return vec
}

func genCommonBlob(msgLength int, schema *schemapb.CollectionSchema) ([]*commonpb.Blob, error) {
	genRawData := func(i int) ([]byte, error) {
		var rawData []byte
		for _, f := range schema.Fields {
			switch f.DataType {
			case schemapb.DataType_Int32:
				bs := make([]byte, 4)
				common.Endian.PutUint32(bs, uint32(i))
				rawData = append(rawData, bs...)
			case schemapb.DataType_Int64:
				bs := make([]byte, 8)
				common.Endian.PutUint32(bs, uint32(i))
				rawData = append(rawData, bs...)
			case schemapb.DataType_FloatVector:
				dim := simpleVecField.dim // if no dim specified, use simpleVecField's dim
				for _, p := range f.TypeParams {
					if p.Key == dimKey {
						var err error
						dim, err = strconv.Atoi(p.Value)
						if err != nil {
							return nil, err
						}
					}
				}
				for j := 0; j < dim; j++ {
					f := float32(i*j) * 0.1
					buf := make([]byte, 4)
					common.Endian.PutUint32(buf, math.Float32bits(f))
					rawData = append(rawData, buf...)
				}
			default:
				err := errors.New("data type not supported")
				return nil, err
			}
		}
		return rawData, nil
	}

	var records []*commonpb.Blob
	for i := 0; i < msgLength; i++ {
		data, err := genRawData(i)
		if err != nil {
			return nil, err
		}
		blob := &commonpb.Blob{
			Value: data,
		}
		records = append(records, blob)
	}

	return records, nil
}

func genSimpleCommonBlob() ([]*commonpb.Blob, error) {
	schema := genSimpleSegCoreSchema()
	return genCommonBlob(defaultMsgLength, schema)
}

func saveBinLog(ctx context.Context,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	msgLength int,
	schema *schemapb.CollectionSchema) ([]*datapb.FieldBinlog, error) {
	binLogs, err := genStorageBlob(collectionID,
		partitionID,
		segmentID,
		msgLength,
		schema)
	if err != nil {
		return nil, err
	}

	log.Debug(".. [query node unittest] Saving bin logs to MinIO ..", zap.Int("number", len(binLogs)))
	kvs := make(map[string]string, len(binLogs))

	// write insert binlog
	fieldBinlog := make([]*datapb.FieldBinlog, 0)
	for _, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		log.Debug("[query node unittest] save binlog", zap.Int64("fieldID", fieldID))
		if err != nil {
			return nil, err
		}

		key := JoinIDPath(collectionID, partitionID, segmentID, fieldID)
		kvs[key] = string(blob.Value[:])
		fieldBinlog = append(fieldBinlog, &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{{LogPath: key}},
		})
	}
	log.Debug("[query node unittest] save binlog file to MinIO/S3")

	kv, err := genMinioKV(ctx)
	if err != nil {
		return nil, err
	}
	err = kv.MultiSave(kvs)
	return fieldBinlog, err
}

func saveSimpleBinLog(ctx context.Context) ([]*datapb.FieldBinlog, error) {
	schema := genSimpleInsertDataSchema()
	return saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, defaultMsgLength, schema)
}

func genSimpleTimestampFieldData() []Timestamp {
	times := make([]Timestamp, defaultMsgLength)
	for i := 0; i < defaultMsgLength; i++ {
		times[i] = Timestamp(i)
	}
	// timestamp 0 is not allowed
	times[0] = 1
	return times
}

func genSimpleTimestampDeletedPK() []Timestamp {
	times := make([]Timestamp, defaultDelLength)
	for i := 0; i < defaultDelLength; i++ {
		times[i] = Timestamp(i)
	}
	times[0] = 1
	return times
}

func genSimpleRowIDField() []IntPrimaryKey {
	ids := make([]IntPrimaryKey, defaultMsgLength)
	for i := 0; i < defaultMsgLength; i++ {
		ids[i] = IntPrimaryKey(i)
	}
	return ids
}

func genSimpleDeleteID() []IntPrimaryKey {
	ids := make([]IntPrimaryKey, defaultDelLength)
	for i := 0; i < defaultDelLength; i++ {
		ids[0] = IntPrimaryKey(i)
	}
	return ids
}

func genMsgStreamBaseMsg() msgstream.BaseMsg {
	return msgstream.BaseMsg{
		HashValues: []uint32{0},
	}
}

func genCommonMsgBase(msgType commonpb.MsgType) *commonpb.MsgBase {
	return &commonpb.MsgBase{
		MsgType: msgType,
		MsgID:   rand.Int63(),
	}
}

func genSimpleInsertMsg() (*msgstream.InsertMsg, error) {
	rowData, err := genSimpleCommonBlob()
	if err != nil {
		return nil, err
	}

	return &msgstream.InsertMsg{
		BaseMsg: genMsgStreamBaseMsg(),
		InsertRequest: internalpb.InsertRequest{
			Base:           genCommonMsgBase(commonpb.MsgType_Retrieve),
			CollectionName: defaultCollectionName,
			PartitionName:  defaultPartitionName,
			CollectionID:   defaultCollectionID,
			PartitionID:    defaultPartitionID,
			SegmentID:      defaultSegmentID,
			ShardName:      defaultDMLChannel,
			Timestamps:     genSimpleTimestampFieldData(),
			RowIDs:         genSimpleRowIDField(),
			RowData:        rowData,
		},
	}, nil
}

func genSimpleDeleteMsg() (*msgstream.DeleteMsg, error) {
	return &msgstream.DeleteMsg{
		BaseMsg: genMsgStreamBaseMsg(),
		DeleteRequest: internalpb.DeleteRequest{
			Base:           genCommonMsgBase(commonpb.MsgType_Delete),
			CollectionName: defaultCollectionName,
			PartitionName:  defaultPartitionName,
			CollectionID:   defaultCollectionID,
			PartitionID:    defaultPartitionID,
			PrimaryKeys:    genSimpleDeleteID(),
			Timestamps:     genSimpleTimestampDeletedPK(),
		},
	}, nil
}

// ---------- unittest util functions ----------
// functions of replica
func genSealedSegment(schemaForCreate *schemapb.CollectionSchema,
	schemaForLoad *schemapb.CollectionSchema,
	collectionID,
	partitionID,
	segmentID UniqueID,
	vChannel Channel,
	msgLength int) (*Segment, error) {
	col := newCollection(collectionID, schemaForCreate)
	seg, err := newSegment(col,
		segmentID,
		partitionID,
		collectionID,
		vChannel,
		segmentTypeSealed,
		true)
	if err != nil {
		return nil, err
	}
	insertData, err := genInsertData(msgLength, schemaForLoad)
	if err != nil {
		return nil, err
	}
	for k, v := range insertData.Data {
		var numRows []int64
		var data interface{}
		switch fieldData := v.(type) {
		case *storage.BoolFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.Int8FieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.Int16FieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.Int32FieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.Int64FieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.FloatFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.DoubleFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.StringFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.FloatVectorFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.BinaryVectorFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		default:
			return nil, errors.New("unexpected field data type")
		}
		totalNumRows := int64(0)
		for _, numRow := range numRows {
			totalNumRows += numRow
		}
		err := seg.segmentLoadFieldData(k, int(totalNumRows), data)
		if err != nil {
			return nil, err
		}
	}
	return seg, nil
}

func genSimpleSealedSegment() (*Segment, error) {
	schema := genSimpleSegCoreSchema()
	schema2 := genSimpleInsertDataSchema()
	return genSealedSegment(schema,
		schema2,
		defaultCollectionID,
		defaultPartitionID,
		defaultSegmentID,
		defaultDMLChannel,
		defaultMsgLength)
}

func genSimpleReplica() (ReplicaInterface, error) {
	kv, err := genEtcdKV()
	if err != nil {
		return nil, err
	}
	r := newCollectionReplica(kv)
	schema := genSimpleSegCoreSchema()
	r.addCollection(defaultCollectionID, schema)
	err = r.addPartition(defaultCollectionID, defaultPartitionID)
	return r, err
}

func genSimpleSegmentLoader(ctx context.Context, historicalReplica ReplicaInterface, streamingReplica ReplicaInterface) (*segmentLoader, error) {
	kv, err := genEtcdKV()
	if err != nil {
		return nil, err
	}
	return newSegmentLoader(ctx, historicalReplica, streamingReplica, kv, msgstream.NewPmsFactory()), nil
}

func genSimpleHistorical(ctx context.Context, tSafeReplica TSafeReplicaInterface) (*historical, error) {
	replica, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}
	h := newHistorical(ctx, replica, tSafeReplica)
	r, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}
	seg, err := genSimpleSealedSegment()
	if err != nil {
		return nil, err
	}
	err = r.setSegment(seg)
	if err != nil {
		return nil, err
	}
	h.replica = r
	col, err := h.replica.getCollectionByID(defaultCollectionID)
	if err != nil {
		return nil, err
	}
	col.addVChannels([]Channel{
		defaultDeltaChannel,
	})
	h.tSafeReplica.addTSafe(defaultDeltaChannel)
	return h, nil
}

func genSimpleStreaming(ctx context.Context, tSafeReplica TSafeReplicaInterface) (*streaming, error) {
	kv, err := genEtcdKV()
	if err != nil {
		return nil, err
	}
	fac, err := genFactory()
	if err != nil {
		return nil, err
	}
	replica, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}
	s := newStreaming(ctx, replica, fac, kv, tSafeReplica)
	r, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}
	err = r.addSegment(defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultDMLChannel,
		segmentTypeGrowing,
		true)
	if err != nil {
		return nil, err
	}
	s.replica = r
	col, err := s.replica.getCollectionByID(defaultCollectionID)
	if err != nil {
		return nil, err
	}
	col.addVChannels([]Channel{
		defaultDMLChannel,
	})
	s.tSafeReplica.addTSafe(defaultDMLChannel)
	return s, nil
}

// ---------- unittest util functions ----------
// functions of messages and requests
func genDSL(schema *schemapb.CollectionSchema, nProb int, topK int64, roundDecimal int64) (string, error) {
	var vecFieldName string
	var metricType string
	nProbStr := strconv.Itoa(nProb)
	topKStr := strconv.FormatInt(topK, 10)
	roundDecimalStr := strconv.FormatInt(roundDecimal, 10)
	for _, f := range schema.Fields {
		if f.DataType == schemapb.DataType_FloatVector {
			vecFieldName = f.Name
			for _, p := range f.IndexParams {
				if p.Key == metricTypeKey {
					metricType = p.Value
				}
			}
		}
	}
	if vecFieldName == "" || metricType == "" {
		err := errors.New("invalid vector field name or metric type")
		return "", err
	}

	return "{\"bool\": { \n\"vector\": {\n \"" + vecFieldName +
		"\": {\n \"metric_type\": \"" + metricType +
		"\", \n \"params\": {\n \"nprobe\": " + nProbStr + " \n},\n \"query\": \"$0\",\n \"topk\": " + topKStr +
		" \n,\"round_decimal\": " + roundDecimalStr +
		"\n } \n } \n } \n }", nil
}

func genSimpleDSL() (string, error) {
	schema := genSimpleSegCoreSchema()
	return genDSL(schema, defaultNProb, defaultTopK, defaultRoundDecimal)
}

func genSimplePlaceHolderGroup() ([]byte, error) {
	placeholderValue := &milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_FloatVector,
		Values: make([][]byte, 0),
	}
	for i := 0; i < int(defaultTopK); i++ {
		var vec = make([]float32, defaultDim)
		for j := 0; j < defaultDim; j++ {
			vec[j] = rand.Float32()
		}
		var rawData []byte
		for k, ele := range vec {
			buf := make([]byte, 4)
			common.Endian.PutUint32(buf, math.Float32bits(ele+float32(k*2)))
			rawData = append(rawData, buf...)
		}
		placeholderValue.Values = append(placeholderValue.Values, rawData)
	}

	// generate placeholder
	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{placeholderValue},
	}
	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		return nil, err
	}
	return placeGroupByte, nil
}

func genSimpleSearchPlanAndRequests() (*SearchPlan, []*searchRequest, error) {
	schema := genSimpleSegCoreSchema()
	collection := newCollection(defaultCollectionID, schema)

	var plan *SearchPlan
	var err error
	sm, err := genSimpleSearchMsg()
	if err != nil {
		return nil, nil, err
	}
	if sm.GetDslType() == commonpb.DslType_BoolExprV1 {
		expr := sm.SerializedExprPlan
		plan, err = createSearchPlanByExpr(collection, expr)
		if err != nil {
			return nil, nil, err
		}
	} else {
		dsl := sm.Dsl
		plan, err = createSearchPlan(collection, dsl)
		if err != nil {
			return nil, nil, err
		}
	}
	searchRequestBlob := sm.PlaceholderGroup
	searchReq, err := parseSearchRequest(plan, searchRequestBlob)
	if err != nil {
		return nil, nil, err
	}
	searchRequests := make([]*searchRequest, 0)
	searchRequests = append(searchRequests, searchReq)

	return plan, searchRequests, nil
}

func genSimpleRetrievePlanExpr() ([]byte, error) {
	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: &planpb.ColumnInfo{
							FieldId:  simpleConstField.id,
							DataType: simpleConstField.dataType,
						},
						Values: []*planpb.GenericValue{
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 1,
								},
							},
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 2,
								},
							},
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 3,
								},
							},
						},
					},
				},
			},
		},
		OutputFieldIds: []int64{simpleConstField.id},
	}
	planExpr, err := proto.Marshal(planNode)
	return planExpr, err
}

func genSimpleRetrievePlan() (*RetrievePlan, error) {
	retrieveMsg, err := genSimpleRetrieveMsg()
	if err != nil {
		return nil, err
	}
	timestamp := retrieveMsg.RetrieveRequest.TravelTimestamp

	schema := genSimpleSegCoreSchema()
	collection := newCollection(defaultCollectionID, schema)

	planExpr, err := genSimpleRetrievePlanExpr()
	if err != nil {
		return nil, err
	}

	plan, err := createRetrievePlanByExpr(collection, planExpr, timestamp)
	return plan, err
}

func genSimpleSearchRequest() (*internalpb.SearchRequest, error) {
	placeHolder, err := genSimplePlaceHolderGroup()
	if err != nil {
		return nil, err
	}
	simpleDSL, err := genSimpleDSL()
	if err != nil {
		return nil, err
	}
	return &internalpb.SearchRequest{
		Base:             genCommonMsgBase(commonpb.MsgType_Search),
		CollectionID:     defaultCollectionID,
		PartitionIDs:     []UniqueID{defaultPartitionID},
		Dsl:              simpleDSL,
		PlaceholderGroup: placeHolder,
		DslType:          commonpb.DslType_Dsl,
	}, nil
}

func genSimpleRetrieveRequest() (*internalpb.RetrieveRequest, error) {
	expr, err := genSimpleRetrievePlanExpr()
	if err != nil {
		return nil, err
	}

	return &internalpb.RetrieveRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_Retrieve,
			MsgID:   rand.Int63(), // TODO: random msgID?
		},
		CollectionID:       defaultCollectionID,
		PartitionIDs:       []UniqueID{defaultPartitionID},
		OutputFieldsId:     []int64{1, 2, 3},
		TravelTimestamp:    Timestamp(1000),
		SerializedExprPlan: expr,
	}, nil
}

func genSimpleSearchMsg() (*msgstream.SearchMsg, error) {
	req, err := genSimpleSearchRequest()
	if err != nil {
		return nil, err
	}
	return &msgstream.SearchMsg{
		BaseMsg:       genMsgStreamBaseMsg(),
		SearchRequest: *req,
	}, nil
}

func genSimpleRetrieveMsg() (*msgstream.RetrieveMsg, error) {
	req, err := genSimpleRetrieveRequest()
	if err != nil {
		return nil, err
	}
	return &msgstream.RetrieveMsg{
		BaseMsg:         genMsgStreamBaseMsg(),
		RetrieveRequest: *req,
	}, nil
}

func genQueryChannel() Channel {
	const queryChannelPrefix = "query-node-unittest-query-channel-"
	return queryChannelPrefix + strconv.Itoa(rand.Int())
}

func genQueryResultChannel() Channel {
	const queryResultChannelPrefix = "query-node-unittest-query-result-channel-"
	return queryResultChannelPrefix + strconv.Itoa(rand.Int())
}

func produceSimpleSearchMsg(ctx context.Context, queryChannel Channel) error {
	stream, err := genQueryMsgStream(ctx)
	if err != nil {
		return err
	}
	stream.AsProducer([]string{queryChannel})
	stream.Start()
	defer stream.Close()
	msg, err := genSimpleSearchMsg()
	if err != nil {
		return err
	}
	msgPack := &msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
	err = stream.Produce(msgPack)
	if err != nil {
		return err
	}
	log.Debug("[query node unittest] produce search message done")
	return nil
}

func produceSimpleRetrieveMsg(ctx context.Context, queryChannel Channel) error {
	stream, err := genQueryMsgStream(ctx)
	if err != nil {
		return err
	}
	stream.AsProducer([]string{queryChannel})
	stream.Start()
	defer stream.Close()
	msg, err := genSimpleRetrieveMsg()
	if err != nil {
		return err
	}
	msgPack := &msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
	err = stream.Produce(msgPack)
	if err != nil {
		return err
	}
	log.Debug("[query node unittest] produce retrieve message done")
	return nil
}

func initConsumer(ctx context.Context, queryResultChannel Channel) (msgstream.MsgStream, error) {
	stream, err := genQueryMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	stream.AsConsumer([]string{queryResultChannel}, defaultSubName)
	stream.Start()
	return stream, nil
}

func consumeSimpleSearchResult(stream msgstream.MsgStream) (*msgstream.SearchResultMsg, error) {
	res := stream.Consume()
	if len(res.Msgs) != 1 {
		err := errors.New("unexpected message length")
		return nil, err
	}
	return res.Msgs[0].(*msgstream.SearchResultMsg), nil
}

func consumeSimpleRetrieveResult(stream msgstream.MsgStream) (*msgstream.RetrieveResultMsg, error) {
	res := stream.Consume()
	if len(res.Msgs) != 1 {
		err := errors.New("unexpected message length")
		return nil, err
	}
	return res.Msgs[0].(*msgstream.RetrieveResultMsg), nil
}

func genSimpleChangeInfo() *querypb.SealedSegmentsChangeInfo {
	changeInfo := &querypb.SegmentChangeInfo{
		OnlineNodeID: Params.QueryNodeCfg.QueryNodeID,
		OnlineSegments: []*querypb.SegmentInfo{
			genSimpleSegmentInfo(),
		},
		OfflineNodeID: Params.QueryNodeCfg.QueryNodeID + 1,
		OfflineSegments: []*querypb.SegmentInfo{
			genSimpleSegmentInfo(),
		},
	}

	return &querypb.SealedSegmentsChangeInfo{
		Base:  genCommonMsgBase(commonpb.MsgType_LoadBalanceSegments),
		Infos: []*querypb.SegmentChangeInfo{changeInfo},
	}
}

func saveChangeInfo(key string, value string) error {
	log.Debug(".. [query node unittest] Saving change info")
	kv, err := genEtcdKV()
	if err != nil {
		return err
	}

	key = util.ChangeInfoMetaPrefix + "/" + key
	return kv.Save(key, value)
}

// node
func genSimpleQueryNode(ctx context.Context) (*QueryNode, error) {
	fac, err := genFactory()
	if err != nil {
		return nil, err
	}
	node := NewQueryNode(ctx, fac)
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		return nil, err
	}
	node.etcdCli = etcdCli
	node.initSession()

	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	node.etcdKV = etcdKV

	node.tSafeReplica = newTSafeReplica()

	streaming, err := genSimpleStreaming(ctx, node.tSafeReplica)
	if err != nil {
		return nil, err
	}

	historical, err := genSimpleHistorical(ctx, node.tSafeReplica)
	if err != nil {
		return nil, err
	}
	node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, streaming.replica, historical.replica, node.tSafeReplica, node.msFactory)

	node.streaming = streaming
	node.historical = historical

	loader, err := genSimpleSegmentLoader(node.queryNodeLoopCtx, historical.replica, streaming.replica)
	if err != nil {
		return nil, err
	}
	node.loader = loader

	// start task scheduler
	go node.scheduler.Start()

	qs := newQueryService(ctx, node.historical, node.streaming, node.msFactory)
	defer qs.close()
	node.queryService = qs

	node.UpdateStateCode(internalpb.StateCode_Healthy)

	return node, nil
}

func genFieldData(fieldName string, fieldID int64, fieldType schemapb.DataType, fieldValue interface{}, dim int64) *schemapb.FieldData {
	var fieldData *schemapb.FieldData
	switch fieldType {
	case schemapb.DataType_Bool:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Bool,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: fieldValue.([]bool),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int32:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int32,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: fieldValue.([]int32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int64:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: fieldValue.([]int64),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Float:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Float,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: fieldValue.([]float32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Double:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Double,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: fieldValue.([]float64),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_BinaryVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_BinaryVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: fieldValue.([]byte),
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_FloatVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_FloatVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: fieldValue.([]float32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	default:
		log.Error("not supported field type", zap.String("field type", fieldType.String()))
	}

	return fieldData
}
