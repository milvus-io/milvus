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

package querynode

import (
	"context"
	"fmt"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"math/rand"
	"path"
	"strconv"
)

// ---------- unittest util functions ----------
// common definitions
const ctxTimeInMillisecond = 5000
const debug = false

const (
	dimKey = "dim"
	metricTypeKey = "metric_type"
	topK = 10
	defaultMetricType = "JACCARD"
	defaultNProb = 10

	defaultVChannel = "query-node-unittest-channel-0"

	utKVRootPath = "query-node-unittest"
	utQueryChannel = "query-node-unittest-query-channel-0"
	utQueryResultChannel = "query-node-unittest-query-result-channel-0"
)

const (
	defaultCollectionID = UniqueID(0)
	defaultPartitionID = UniqueID(1)
	defaultSegmentID = UniqueID(2)
)

const defaultMsgLength = 1000

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
	dim:        128,
	metricType: defaultMetricType,
	vecType:    schemapb.DataType_FloatVector,
}

var simpleConstField = constFieldParam{
	id:       101,
	dataType: schemapb.DataType_Int32,
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
		Name:         "field-constant" + fmt.Sprintln(rand.Int()),
		IsPrimaryKey: false,
		DataType:     param.dataType,
	}
	return field
}

func genFloatVectorField(param vecFieldParam) *schemapb.FieldSchema {
	fieldVec := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         "field-vector" + fmt.Sprintln(rand.Int()),
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

func genSimpleSchema() *schemapb.CollectionSchema {
	fieldUID := genConstantField(uidField)
	fieldTimestamp := genConstantField(timestampField)
	fieldVec := genFloatVectorField(simpleVecField)
	fieldInt := genConstantField(simpleConstField)

	schema := schemapb.CollectionSchema{
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
	simpleSchema := genSimpleSchema()
	return genCollectionMeta(defaultCollectionID, simpleSchema)
}

// ---------- unittest util functions ----------
// functions of third-party
func genMinioKV(ctx context.Context) *minioKV.MinIOKV {
	bucketName := Params.MinioBucketName
	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		BucketName:        bucketName,
		CreateBucket:      true,
	}
	kv, err := minioKV.NewMinIOKV(ctx, option)
	if err != nil {
		panic(err)
	}
	return kv
}

func genEtcdKV() *etcdkv.EtcdKV {
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints})
	if err != nil {
		panic(err)
	}
	etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
	return etcdKV
}

func genFactory() msgstream.Factory {
	const receiveBufSize = 1024

	pulsarURL := Params.PulsarAddress
	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  pulsarURL,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	if err != nil {
		panic(err)
	}
	return msFactory
}

// ---------- unittest util functions ----------
// functions of inserting data init
func genInsertData(msgLength int, schema *schemapb.CollectionSchema) *storage.InsertData {
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
				NumRows: msgLength,
				Data: data,
			}
		case schemapb.DataType_Int8:
			data := make([]int8, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int8(i)
			}
			insertData.Data[f.FieldID] = &storage.Int8FieldData{
				NumRows: msgLength,
				Data: data,
			}
		case schemapb.DataType_Int16:
			data := make([]int16, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int16(i)
			}
			insertData.Data[f.FieldID] = &storage.Int16FieldData{
				NumRows: msgLength,
				Data: data,
			}
		case schemapb.DataType_Int32:
			data := make([]int32, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int32(i)
			}
			insertData.Data[f.FieldID] = &storage.Int32FieldData{
				NumRows: msgLength,
				Data: data,
			}
		case schemapb.DataType_Int64:
			data := make([]int64, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = int64(i)
			}
			insertData.Data[f.FieldID] = &storage.Int64FieldData{
				NumRows: msgLength,
				Data: data,
			}
		case schemapb.DataType_Float:
			data := make([]float32, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = float32(i)
			}
			insertData.Data[f.FieldID] = &storage.FloatFieldData{
				NumRows: msgLength,
				Data: data,
			}
		case schemapb.DataType_Double:
			data := make([]float64, msgLength)
			for i := 0; i < msgLength; i++ {
				data[i] = float64(i)
			}
			insertData.Data[f.FieldID] = &storage.DoubleFieldData{
				NumRows: msgLength,
				Data: data,
			}
		case schemapb.DataType_FloatVector:
			dim := simpleVecField.dim // if no dim specified, use simpleVecField's dim
			for _, p := range f.TypeParams {
				if p.Key == dimKey {
					var err error
					dim, err = strconv.Atoi(p.Value)
					if err != nil {
						panic(err)
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
				NumRows: msgLength,
				Data:    data,
				Dim:     dim,
			}
		default:
			panic("data type not supported!")
		}
	}

	return insertData
}

func genSimpleInsertData() *storage.InsertData {
	schema := genSimpleSchema()
	return genInsertData(defaultMsgLength, schema)
}

func genKey(collectionID, partitionID, segmentID UniqueID, fieldID int64) string {
	ids := []string {
		utKVRootPath,
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(partitionID, 10),
		strconv.FormatInt(segmentID, 10),
		strconv.FormatInt(fieldID, 10),
	}
	return path.Join(ids...)
}

func saveSimpleBinLog(ctx context.Context) {
	collMeta := genSimpleCollectionMeta()
	inCodec := storage.NewInsertCodec(collMeta)
	insertData := genSimpleInsertData()
	binLogs, _, err := inCodec.Serialize(defaultPartitionID, defaultSegmentID, insertData)
	if err != nil {
		panic(err)
	}

	log.Debug(".. [query node unittest] Saving bin logs to MinIO ..", zap.Int("number", len(binLogs)))
	kvs := make(map[string]string, len(binLogs))

	// write insert binlog
	for _, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		log.Debug("[query node unittest] save binlog", zap.Int64("fieldID", fieldID))
		if err != nil {
			panic(err)
		}

		key := genKey(defaultCollectionID, defaultPartitionID, defaultSegmentID, fieldID)
		kvs[key] = string(blob.Value[:])
	}
	log.Debug("[query node unittest] save binlog file to MinIO/S3")

	kv := genMinioKV(ctx)
	err = kv.MultiSave(kvs)
	if err != nil {
		panic(err)
	}
}

// ---------- unittest util functions ----------
// functions of replica
func genSimpleSealedSegment() *Segment {
	schema := genSimpleSchema()
	col := newCollection(defaultCollectionID, schema)
	seg := newSegment(col,
		defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultVChannel,
		segmentTypeSealed,
		true)
	insertData := genSimpleInsertData()
	for k, v := range insertData.Data {
		err := seg.segmentLoadFieldData(k, defaultMsgLength, v)
		if err != nil {
			panic(err)
		}
	}
	return seg
}

func genSimpleReplica() ReplicaInterface {
	kv := genEtcdKV()
	r := newCollectionReplica(kv)
	schema := genSimpleSchema()
	err := r.addCollection(defaultCollectionID, schema)
	if err != nil {
		panic(err)
	}
	err = r.addPartition(defaultCollectionID, defaultPartitionID)
	if err != nil {
		panic(err)
	}
	return r
}

func genSimpleHistorical(ctx context.Context) *historical {
	fac := genFactory()
	kv := genEtcdKV()
	h := newHistorical(ctx, nil, nil, fac, kv)
	r := genSimpleReplica()
	seg := genSimpleSealedSegment()
	err := r.setSegment(seg)
	if err != nil {
		panic(err)
	}
	h.replica = r
	return h
}

func genSimpleStreaming(ctx context.Context) *streaming {
	fac := genFactory()
	kv := genEtcdKV()
	s := newStreaming(ctx, fac, kv)
	r := genSimpleReplica()
	err := r.addSegment(defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultVChannel,
		segmentTypeGrowing,
		true)
	if err != nil {
		panic(err)
	}
	s.replica = r
	return s
}

// ---------- unittest util functions ----------
// functions of messages and requests
var simpleDSL = "{\"bool\": { " +
	"\"vector\": {" +
	" \"vec\": {" +
	" \"metric_type\": \"" + defaultMetricType + "\", " +
	" \"params\": {" +
	" \"nprobe\": " + strconv.Itoa(defaultNProb) + " " +
	"}, \"query\": \"$0\",\"topk\": " + strconv.Itoa(topK) + " \n } \n } \n } \n }"

func genSimpleSearchRequest() *internalpb.SearchRequest {
	return &internalpb.SearchRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_Search,
			MsgID: rand.Int63(), // TODO: random msgID?
		},
		ResultChannelID: utQueryResultChannel,
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		Dsl: simpleDSL,

	}
}
