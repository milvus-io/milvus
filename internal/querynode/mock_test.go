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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path"
	"runtime"
	"strconv"

	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

// ---------- unittest util functions ----------
// common definitions
const ctxTimeInMillisecond = 500
const debugUT = false

const (
	dimKey        = "dim"
	metricTypeKey = "metric_type"

	defaultPKFieldName  = "pk"
	defaultTopK         = int64(10)
	defaultRoundDecimal = int64(6)
	defaultDim          = 128
	defaultNProb        = 10
	defaultEf           = 10
	defaultMetricType   = L2
	defaultNQ           = 10

	defaultDMLChannel   = "query-node-unittest-DML-0"
	defaultDeltaChannel = "query-node-unittest-delta-channel-0"
	defaultSubName      = "query-node-unittest-sub-name-0"

	defaultLocalStorage = "/tmp/milvus_test/querynode"

	defaultSegmentVersion = int64(1001)
)

const (
	defaultCollectionID = UniqueID(0)
	defaultPartitionID  = UniqueID(1)
	defaultSegmentID    = UniqueID(2)
	defaultReplicaID    = UniqueID(10)

	defaultCollectionName = "query-node-unittest-default-collection"
	defaultPartitionName  = "query-node-unittest-default-partition"

	defaultChannelName = "default-channel"
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

const (
	// index type
	IndexFaissIDMap      = "FLAT"
	IndexFaissIVFFlat    = "IVF_FLAT"
	IndexFaissIVFPQ      = "IVF_PQ"
	IndexFaissIVFSQ8     = "IVF_SQ8"
	IndexFaissIVFSQ8H    = "IVF_SQ8_HYBRID"
	IndexFaissBinIDMap   = "BIN_FLAT"
	IndexFaissBinIVFFlat = "BIN_IVF_FLAT"
	IndexNsg             = "NSG"

	IndexHNSW      = "HNSW"
	IndexRHNSWFlat = "RHNSW_FLAT"
	IndexRHNSWPQ   = "RHNSW_PQ"
	IndexRHNSWSQ   = "RHNSW_SQ"
	IndexANNOY     = "ANNOY"
	IndexNGTPANNG  = "NGT_PANNG"
	IndexNGTONNG   = "NGT_ONNG"

	// metric type
	L2       = "L2"
	IP       = "IP"
	hamming  = "HAMMING"
	Jaccard  = "JACCARD"
	tanimoto = "TANIMOTO"

	nlist          = 100
	m              = 4
	nbits          = 8
	nprobe         = 8
	sliceSize      = 4
	efConstruction = 200
	ef             = 200
	edgeSize       = 10
	epsilon        = 0.1
	maxSearchEdges = 50
)

// ---------- unittest util functions ----------
// functions of init meta and generate meta
type vecFieldParam struct {
	id         int64
	dim        int
	metricType string
	vecType    schemapb.DataType
	fieldName  string
}

type constFieldParam struct {
	id        int64
	dataType  schemapb.DataType
	fieldName string
}

var simpleFloatVecField = vecFieldParam{
	id:         100,
	dim:        defaultDim,
	metricType: defaultMetricType,
	vecType:    schemapb.DataType_FloatVector,
	fieldName:  "floatVectorField",
}

var simpleBinVecField = vecFieldParam{
	id:         101,
	dim:        defaultDim,
	metricType: Jaccard,
	vecType:    schemapb.DataType_BinaryVector,
	fieldName:  "binVectorField",
}

var simpleBoolField = constFieldParam{
	id:        102,
	dataType:  schemapb.DataType_Bool,
	fieldName: "boolField",
}

var simpleInt8Field = constFieldParam{
	id:        103,
	dataType:  schemapb.DataType_Int8,
	fieldName: "int8Field",
}

var simpleInt16Field = constFieldParam{
	id:        104,
	dataType:  schemapb.DataType_Int16,
	fieldName: "int16Field",
}

var simpleInt32Field = constFieldParam{
	id:        105,
	dataType:  schemapb.DataType_Int32,
	fieldName: "int32Field",
}

var simpleInt64Field = constFieldParam{
	id:        106,
	dataType:  schemapb.DataType_Int64,
	fieldName: "int64Field",
}

var simpleFloatField = constFieldParam{
	id:        107,
	dataType:  schemapb.DataType_Float,
	fieldName: "floatField",
}

var simpleDoubleField = constFieldParam{
	id:        108,
	dataType:  schemapb.DataType_Double,
	fieldName: "doubleField",
}

var simpleVarCharField = constFieldParam{
	id:        109,
	dataType:  schemapb.DataType_VarChar,
	fieldName: "varCharField",
}

var uidField = constFieldParam{
	id:        rowIDFieldID,
	dataType:  schemapb.DataType_Int64,
	fieldName: "RowID",
}

var timestampField = constFieldParam{
	id:        timestampFieldID,
	dataType:  schemapb.DataType_Int64,
	fieldName: "Timestamp",
}

func genConstantFieldSchema(param constFieldParam) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         param.fieldName,
		IsPrimaryKey: false,
		DataType:     param.dataType,
	}
	return field
}

func genPKFieldSchema(param constFieldParam) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         param.fieldName,
		IsPrimaryKey: true,
		DataType:     param.dataType,
	}
	return field
}

func genVectorFieldSchema(param vecFieldParam) *schemapb.FieldSchema {
	fieldVec := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         param.fieldName,
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

func genIndexBinarySet() ([][]byte, error) {
	typeParams, indexParams := genIndexParams(IndexFaissIVFPQ, L2)

	index, err := indexcgowrapper.NewCgoIndex(schemapb.DataType_FloatVector, typeParams, indexParams)
	if err != nil {
		return nil, err
	}

	err = index.Build(indexcgowrapper.GenFloatVecDataset(generateFloatVectors(defaultDelLength, defaultDim)))
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

func loadIndexForSegment(ctx context.Context, node *QueryNode, segmentID UniqueID, msgLength int, indexType string, metricType string, pkType schemapb.DataType) error {
	schema := genTestCollectionSchema(pkType)

	// generate insert binlog
	fieldBinlog, _, err := saveBinLog(ctx, defaultCollectionID, defaultPartitionID, defaultSegmentID, msgLength, schema)
	if err != nil {
		return err
	}

	// generate index file for segment
	indexPaths, err := generateAndSaveIndex(segmentID, msgLength, indexType, metricType)
	if err != nil {
		return err
	}
	_, indexParams := genIndexParams(indexType, metricType)
	indexInfo := &querypb.FieldIndexInfo{
		FieldID:        simpleFloatVecField.id,
		EnableIndex:    true,
		IndexName:      indexName,
		IndexID:        indexID,
		BuildID:        buildID,
		IndexParams:    funcutil.Map2KeyValuePair(indexParams),
		IndexFilePaths: indexPaths,
	}

	loader := node.loader
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadSegments,
			MsgID:   rand.Int63(),
		},
		DstNodeID: 0,
		Schema:    schema,
		Infos: []*querypb.SegmentLoadInfo{
			{
				SegmentID:    segmentID,
				PartitionID:  defaultPartitionID,
				CollectionID: defaultCollectionID,
				BinlogPaths:  fieldBinlog,
				IndexInfos:   []*querypb.FieldIndexInfo{indexInfo},
			},
		},
	}

	err = loader.LoadSegment(req, segmentTypeSealed)
	if err != nil {
		return err
	}

	segment, err := node.loader.metaReplica.getSegmentByID(segmentID, segmentTypeSealed)
	if err != nil {
		return err
	}
	vecFieldInfo, err := segment.getIndexedFieldInfo(simpleFloatVecField.id)
	if err != nil {
		return err
	}
	if vecFieldInfo == nil {
		return fmt.Errorf("nil vecFieldInfo, load index failed")
	}
	return nil
}

func generateAndSaveIndex(segmentID UniqueID, msgLength int, indexType, metricType string) ([]string, error) {
	typeParams, indexParams := genIndexParams(indexType, metricType)

	var indexParamsKV []*commonpb.KeyValuePair
	for key, value := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	index, err := indexcgowrapper.NewCgoIndex(schemapb.DataType_FloatVector, typeParams, indexParams)
	if err != nil {
		return nil, err
	}

	err = index.Build(indexcgowrapper.GenFloatVecDataset(generateFloatVectors(msgLength, defaultDim)))
	if err != nil {
		return nil, err
	}

	cm := storage.NewLocalChunkManager(storage.RootPath(defaultLocalStorage))

	// save index to minio
	binarySet, err := index.Serialize()
	if err != nil {
		return nil, err
	}

	// serialize index params
	indexCodec := storage.NewIndexFileBinlogCodec()
	serializedIndexBlobs, err := indexCodec.Serialize(
		buildID,
		0,
		defaultCollectionID,
		defaultPartitionID,
		defaultSegmentID,
		simpleFloatVecField.id,
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
		err := cm.Write(p, index.Value)
		if err != nil {
			return nil, err
		}
	}

	return indexPaths, nil
}

func genIndexParams(indexType, metricType string) (map[string]string, map[string]string) {
	typeParams := make(map[string]string)
	typeParams["dim"] = strconv.Itoa(defaultDim)

	indexParams := make(map[string]string)
	indexParams["index_type"] = indexType
	indexParams["metric_type"] = metricType
	indexParams["index_mode"] = "cpu"
	if indexType == IndexFaissIDMap { // float vector
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexFaissIVFFlat {
		indexParams["nlist"] = strconv.Itoa(nlist)
	} else if indexType == IndexFaissIVFPQ {
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexFaissIVFSQ8 {
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["nbits"] = strconv.Itoa(nbits)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexFaissIVFSQ8H {
		// TODO: enable gpu
	} else if indexType == IndexNsg {
		indexParams["nlist"] = strconv.Itoa(163)
		indexParams["nprobe"] = strconv.Itoa(nprobe)
		indexParams["knng"] = strconv.Itoa(20)
		indexParams["search_length"] = strconv.Itoa(40)
		indexParams["out_degree"] = strconv.Itoa(30)
		indexParams["candidate_pool_size"] = strconv.Itoa(100)
	} else if indexType == IndexHNSW {
		indexParams["M"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		//indexParams["ef"] = strconv.Itoa(ef)
	} else if indexType == IndexRHNSWFlat {
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexRHNSWPQ {
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
		indexParams["PQM"] = strconv.Itoa(8)
	} else if indexType == IndexRHNSWSQ {
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexANNOY {
		indexParams["n_trees"] = strconv.Itoa(4)
		indexParams["search_k"] = strconv.Itoa(100)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexNGTPANNG {
		indexParams["edge_size"] = strconv.Itoa(edgeSize)
		indexParams["epsilon"] = fmt.Sprint(epsilon)
		indexParams["max_search_edges"] = strconv.Itoa(maxSearchEdges)
		indexParams["forcedly_pruned_edge_size"] = strconv.Itoa(60)
		indexParams["selectively_pruned_edge_size"] = strconv.Itoa(30)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexNGTONNG {
		indexParams["edge_size"] = strconv.Itoa(edgeSize)
		indexParams["epsilon"] = fmt.Sprint(epsilon)
		indexParams["max_search_edges"] = strconv.Itoa(maxSearchEdges)
		indexParams["outgoing_edge_size"] = strconv.Itoa(5)
		indexParams["incoming_edge_size"] = strconv.Itoa(40)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexFaissBinIVFFlat { // binary vector
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexFaissBinIDMap {
		//indexParams["dim"] = strconv.Itoa(defaultDim)
	} else {
		panic("")
	}

	return typeParams, indexParams
}

func genTestCollectionSchema(pkTypes ...schemapb.DataType) *schemapb.CollectionSchema {
	fieldBool := genConstantFieldSchema(simpleBoolField)
	fieldInt8 := genConstantFieldSchema(simpleInt8Field)
	fieldInt16 := genConstantFieldSchema(simpleInt16Field)
	fieldInt32 := genConstantFieldSchema(simpleInt32Field)
	fieldFloat := genConstantFieldSchema(simpleFloatField)
	fieldDouble := genConstantFieldSchema(simpleDoubleField)
	floatVecFieldSchema := genVectorFieldSchema(simpleFloatVecField)
	binVecFieldSchema := genVectorFieldSchema(simpleBinVecField)
	var pkFieldSchema *schemapb.FieldSchema
	var pkType schemapb.DataType
	if len(pkTypes) == 0 {
		pkType = schemapb.DataType_Int64
	} else {
		pkType = pkTypes[0]
	}
	switch pkType {
	case schemapb.DataType_Int64:
		pkFieldSchema = genPKFieldSchema(simpleInt64Field)
	case schemapb.DataType_VarChar:
		pkFieldSchema = genPKFieldSchema(simpleVarCharField)
	}

	schema := schemapb.CollectionSchema{ // schema for segCore
		Name:   defaultCollectionName,
		AutoID: false,
		Fields: []*schemapb.FieldSchema{
			fieldBool,
			fieldInt8,
			fieldInt16,
			fieldInt32,
			fieldFloat,
			fieldDouble,
			floatVecFieldSchema,
			binVecFieldSchema,
			pkFieldSchema,
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

// ---------- unittest util functions ----------
// functions of third-party
func genEtcdKV() (*etcdkv.EtcdKV, error) {
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		return nil, err
	}
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	return etcdKV, nil
}

func genFactory() dependency.Factory {
	factory := dependency.NewDefaultFactory(true)
	return factory
}

func genQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	fac := genFactory()
	stream, err := fac.NewQueryMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func genLocalChunkManager() (storage.ChunkManager, error) {
	p := Params.LoadWithDefault("storage.path", "/tmp/milvus/data")
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

func genVectorChunkManager(ctx context.Context, col *Collection) (*storage.VectorChunkManager, error) {
	p := Params.LoadWithDefault("storage.path", "/tmp/milvus/data")
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

	vcm, err := storage.NewVectorChunkManager(lcm, rcm, &etcdpb.CollectionMeta{
		ID:     col.id,
		Schema: col.schema,
	}, Params.QueryNodeCfg.CacheMemoryLimit, false)
	if err != nil {
		return nil, err
	}
	return vcm, nil
}

// ---------- unittest util functions ----------
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
		ret = append(ret, int32(i))
	}
	return ret
}

func generateInt64Array(numRows int) []int64 {
	ret := make([]int64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int64(i))
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

func generateStringArray(numRows int) []string {
	ret := make([]string, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, strconv.Itoa(i))
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
		ret.FieldId = simpleBoolField.id
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
		ret.FieldId = simpleInt8Field.id
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
		ret.FieldId = simpleInt16Field.id
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
		ret.FieldId = simpleInt32Field.id
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
		ret.FieldId = simpleInt64Field.id
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
		ret.FieldId = simpleFloatField.id
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
		ret.FieldId = simpleDoubleField.id
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
		ret.FieldId = simpleVarCharField.id
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: generateStringArray(numRows),
					},
				},
			},
		}
	default:
		panic("data type not supported")
	}

	return ret
}

func newFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   simpleFloatVecField.id,
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
		FieldId:   simpleBinVecField.id,
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

// functions of inserting data init
func genInsertData(msgLength int, schema *schemapb.CollectionSchema) (*storage.InsertData, error) {
	insertData := &storage.InsertData{
		Data: make(map[int64]storage.FieldData),
	}

	// set data for rowID field
	insertData.Data[rowIDFieldID] = &storage.Int64FieldData{
		NumRows: []int64{int64(msgLength)},
		Data:    generateInt64Array(msgLength),
	}
	// set data for ts field
	insertData.Data[timestampFieldID] = &storage.Int64FieldData{
		NumRows: []int64{int64(msgLength)},
		Data:    genTimestampFieldData(msgLength),
	}

	for _, f := range schema.Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			insertData.Data[f.FieldID] = &storage.BoolFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateBoolArray(msgLength),
			}
		case schemapb.DataType_Int8:
			insertData.Data[f.FieldID] = &storage.Int8FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateInt8Array(msgLength),
			}
		case schemapb.DataType_Int16:
			insertData.Data[f.FieldID] = &storage.Int16FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateInt16Array(msgLength),
			}
		case schemapb.DataType_Int32:
			insertData.Data[f.FieldID] = &storage.Int32FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateInt32Array(msgLength),
			}
		case schemapb.DataType_Int64:
			insertData.Data[f.FieldID] = &storage.Int64FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateInt64Array(msgLength),
			}
		case schemapb.DataType_Float:
			insertData.Data[f.FieldID] = &storage.FloatFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateFloat32Array(msgLength),
			}
		case schemapb.DataType_Double:
			insertData.Data[f.FieldID] = &storage.DoubleFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateFloat64Array(msgLength),
			}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			insertData.Data[f.FieldID] = &storage.StringFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateStringArray(msgLength),
			}
		case schemapb.DataType_FloatVector:
			dim := simpleFloatVecField.dim // if no dim specified, use simpleFloatVecField's dim
			insertData.Data[f.FieldID] = &storage.FloatVectorFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateFloatVectors(msgLength, dim),
				Dim:     dim,
			}
		case schemapb.DataType_BinaryVector:
			dim := simpleBinVecField.dim
			insertData.Data[f.FieldID] = &storage.BinaryVectorFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateBinaryVectors(msgLength, dim),
				Dim:     dim,
			}
		default:
			err := errors.New("data type not supported")
			return nil, err
		}
	}

	return insertData, nil
}

func genStorageBlob(collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	msgLength int,
	schema *schemapb.CollectionSchema) ([]*storage.Blob, []*storage.Blob, error) {
	tmpSchema := &schemapb.CollectionSchema{
		Name:   schema.Name,
		AutoID: schema.AutoID,
		Fields: []*schemapb.FieldSchema{genConstantFieldSchema(uidField), genConstantFieldSchema(timestampField)},
	}
	tmpSchema.Fields = append(tmpSchema.Fields, schema.Fields...)
	collMeta := genCollectionMeta(collectionID, tmpSchema)
	inCodec := storage.NewInsertCodec(collMeta)
	insertData, err := genInsertData(msgLength, schema)
	if err != nil {
		return nil, nil, err
	}
	binLogs, statsLogs, err := inCodec.Serialize(partitionID, segmentID, insertData)

	return binLogs, statsLogs, err
}

func genSimpleInsertMsg(schema *schemapb.CollectionSchema, numRows int) (*msgstream.InsertMsg, error) {
	fieldsData := make([]*schemapb.FieldData, 0)

	for _, f := range schema.Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleBoolField.fieldName, numRows))
		case schemapb.DataType_Int8:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleInt8Field.fieldName, numRows))
		case schemapb.DataType_Int16:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleInt16Field.fieldName, numRows))
		case schemapb.DataType_Int32:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleInt32Field.fieldName, numRows))
		case schemapb.DataType_Int64:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleInt64Field.fieldName, numRows))
		case schemapb.DataType_Float:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleFloatField.fieldName, numRows))
		case schemapb.DataType_Double:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleDoubleField.fieldName, numRows))
		case schemapb.DataType_VarChar:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleVarCharField.fieldName, numRows))
		case schemapb.DataType_FloatVector:
			dim := simpleFloatVecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, newFloatVectorFieldData(simpleFloatVecField.fieldName, numRows, dim))
		case schemapb.DataType_BinaryVector:
			dim := simpleBinVecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, newBinaryVectorFieldData(simpleBinVecField.fieldName, numRows, dim))
		default:
			err := errors.New("data type not supported")
			return nil, err
		}
	}

	return &msgstream.InsertMsg{
		BaseMsg: genMsgStreamBaseMsg(),
		InsertRequest: internalpb.InsertRequest{
			Base:           genCommonMsgBase(commonpb.MsgType_Insert),
			CollectionName: defaultCollectionName,
			PartitionName:  defaultPartitionName,
			CollectionID:   defaultCollectionID,
			PartitionID:    defaultPartitionID,
			SegmentID:      defaultSegmentID,
			ShardName:      defaultDMLChannel,
			Timestamps:     genSimpleTimestampFieldData(numRows),
			RowIDs:         genSimpleRowIDField(numRows),
			FieldsData:     fieldsData,
			NumRows:        uint64(numRows),
			Version:        internalpb.InsertDataVersion_ColumnBased,
		},
	}, nil
}

func saveBinLog(ctx context.Context,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	msgLength int,
	schema *schemapb.CollectionSchema) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	binLogs, statsLogs, err := genStorageBlob(collectionID,
		partitionID,
		segmentID,
		msgLength,
		schema)
	if err != nil {
		return nil, nil, err
	}

	log.Debug(".. [query node unittest] Saving bin logs to MinIO ..", zap.Int("number", len(binLogs)))
	kvs := make(map[string][]byte, len(binLogs))

	// write insert binlog
	fieldBinlog := make([]*datapb.FieldBinlog, 0)
	for _, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		log.Debug("[query node unittest] save binlog", zap.Int64("fieldID", fieldID))
		if err != nil {
			return nil, nil, err
		}

		k := JoinIDPath(collectionID, partitionID, segmentID, fieldID)
		key := path.Join("insert-log", k)
		kvs[key] = blob.Value[:]
		fieldBinlog = append(fieldBinlog, &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{{LogPath: key}},
		})
	}
	log.Debug("[query node unittest] save binlog file to MinIO/S3")

	// write insert binlog
	statsBinlog := make([]*datapb.FieldBinlog, 0)
	for _, blob := range statsLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		log.Debug("[query node unittest] save statLog", zap.Int64("fieldID", fieldID))
		if err != nil {
			return nil, nil, err
		}

		k := JoinIDPath(collectionID, partitionID, segmentID, fieldID)
		key := path.Join("delta-log", k)
		kvs[key] = blob.Value[:]
		statsBinlog = append(statsBinlog, &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{{LogPath: key}},
		})
	}
	log.Debug("[query node unittest] save statsLog file to MinIO/S3")

	cm := storage.NewLocalChunkManager(storage.RootPath(defaultLocalStorage))
	err = cm.MultiWrite(kvs)
	return fieldBinlog, statsBinlog, err
}

// saveDeltaLog saves delta logs into MinIO for testing purpose.
func saveDeltaLog(collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID) ([]*datapb.FieldBinlog, error) {

	binlogWriter := storage.NewDeleteBinlogWriter(schemapb.DataType_String, collectionID, partitionID, segmentID)
	eventWriter, _ := binlogWriter.NextDeleteEventWriter()
	dData := &storage.DeleteData{
		Pks:      []storage.PrimaryKey{&storage.Int64PrimaryKey{Value: 1}, &storage.Int64PrimaryKey{Value: 2}},
		Tss:      []Timestamp{100, 200},
		RowCount: 2,
	}

	sizeTotal := 0
	for i := int64(0); i < dData.RowCount; i++ {
		int64PkValue := dData.Pks[i].(*storage.Int64PrimaryKey).Value
		ts := dData.Tss[i]
		eventWriter.AddOneStringToPayload(fmt.Sprintf("%d,%d", int64PkValue, ts))
		sizeTotal += binary.Size(int64PkValue)
		sizeTotal += binary.Size(ts)
	}
	eventWriter.SetEventTimestamp(100, 200)
	binlogWriter.SetEventTimeStamp(100, 200)
	binlogWriter.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))

	binlogWriter.Finish()
	buffer, _ := binlogWriter.GetBuffer()
	blob := &storage.Blob{Key: "deltaLogPath1", Value: buffer}

	kvs := make(map[string][]byte, 1)

	// write delta log
	pkFieldID := UniqueID(106)
	fieldBinlog := make([]*datapb.FieldBinlog, 0)
	log.Debug("[query node unittest] save delta log", zap.Int64("fieldID", pkFieldID))
	key := JoinIDPath(collectionID, partitionID, segmentID, pkFieldID)
	key += "delta" // append suffix 'delta' to avoid conflicts against binlog
	kvs[key] = blob.Value[:]
	fieldBinlog = append(fieldBinlog, &datapb.FieldBinlog{
		FieldID: pkFieldID,
		Binlogs: []*datapb.Binlog{{LogPath: key}},
	})
	log.Debug("[query node unittest] save delta log file to MinIO/S3")

	return fieldBinlog, storage.NewLocalChunkManager(storage.RootPath(defaultLocalStorage)).MultiWrite(kvs)
}

func genSimpleTimestampFieldData(numRows int) []Timestamp {
	times := make([]Timestamp, numRows)
	for i := 0; i < numRows; i++ {
		times[i] = Timestamp(i)
	}
	// timestamp 0 is not allowed
	times[0] = 1
	return times
}

func genTimestampFieldData(numRows int) []int64 {
	times := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		times[i] = int64(i)
	}
	// timestamp 0 is not allowed
	times[0] = 1
	return times
}

func genSimpleRowIDField(numRows int) []IntPrimaryKey {
	ids := make([]IntPrimaryKey, numRows)
	for i := 0; i < numRows; i++ {
		ids[i] = IntPrimaryKey(i)
	}
	return ids
}

func genSimpleDeleteID(dataType schemapb.DataType, numRows int) *schemapb.IDs {
	ret := &schemapb.IDs{}
	switch dataType {
	case schemapb.DataType_Int64:
		ids := make([]IntPrimaryKey, numRows)
		for i := 0; i < numRows; i++ {
			ids[i] = IntPrimaryKey(i)
		}
		ret.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: ids,
			},
		}
	case schemapb.DataType_VarChar:
		ids := make([]string, numRows)
		for i := 0; i < numRows; i++ {
			ids[i] = strconv.Itoa(i)
		}
		ret.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: ids,
			},
		}
	default:
		//TODO
	}

	return ret
}

func genMsgStreamBaseMsg() msgstream.BaseMsg {
	return msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
		MsgPosition: &internalpb.MsgPosition{
			ChannelName: "",
			MsgID:       []byte{},
			MsgGroup:    "",
			Timestamp:   10,
		},
	}
}

func genCommonMsgBase(msgType commonpb.MsgType) *commonpb.MsgBase {
	return &commonpb.MsgBase{
		MsgType: msgType,
		MsgID:   rand.Int63(),
	}
}

func genDeleteMsg(collectionID int64, pkType schemapb.DataType, numRows int) *msgstream.DeleteMsg {
	return &msgstream.DeleteMsg{
		BaseMsg: genMsgStreamBaseMsg(),
		DeleteRequest: internalpb.DeleteRequest{
			Base:           genCommonMsgBase(commonpb.MsgType_Delete),
			CollectionName: defaultCollectionName,
			PartitionName:  defaultPartitionName,
			CollectionID:   collectionID,
			PartitionID:    defaultPartitionID,
			PrimaryKeys:    genSimpleDeleteID(pkType, numRows),
			Timestamps:     genSimpleTimestampFieldData(numRows),
			NumRows:        int64(numRows),
		},
	}
}

// ---------- unittest util functions ----------
// functions of replica
func genSealedSegment(schema *schemapb.CollectionSchema,
	collectionID,
	partitionID,
	segmentID UniqueID,
	vChannel Channel,
	msgLength int) (*Segment, error) {
	col := newCollection(collectionID, schema)
	pool, err := concurrency.NewPool(runtime.GOMAXPROCS(0))
	if err != nil {
		return nil, err
	}

	seg, err := newSegment(col,
		segmentID,
		partitionID,
		collectionID,
		vChannel,
		segmentTypeSealed,
		defaultSegmentVersion,
		pool)
	if err != nil {
		return nil, err
	}
	insertData, err := genInsertData(msgLength, schema)
	if err != nil {
		return nil, err
	}

	insertRecord, err := storage.TransferInsertDataToInsertRecord(insertData)
	if err != nil {
		return nil, err
	}
	numRows := insertRecord.NumRows
	for _, fieldData := range insertRecord.FieldsData {
		fieldID := fieldData.FieldId
		err := seg.segmentLoadFieldData(fieldID, numRows, fieldData)
		if err != nil {
			// TODO: return or continue?
			return nil, err
		}
	}

	return seg, nil
}

func genSimpleSealedSegment(msgLength int) (*Segment, error) {
	schema := genTestCollectionSchema()
	return genSealedSegment(schema,
		defaultCollectionID,
		defaultPartitionID,
		defaultSegmentID,
		defaultDMLChannel,
		msgLength)
}

func genSimpleReplica() (ReplicaInterface, error) {
	pool, err := concurrency.NewPool(runtime.GOMAXPROCS(0))
	if err != nil {
		return nil, err
	}
	r := newCollectionReplica(pool)
	schema := genTestCollectionSchema()
	r.addCollection(defaultCollectionID, schema)
	err = r.addPartition(defaultCollectionID, defaultPartitionID)
	return r, err
}

func genSimpleSegmentLoaderWithMqFactory(metaReplica ReplicaInterface, factory msgstream.Factory) (*segmentLoader, error) {
	pool, err := concurrency.NewPool(runtime.GOMAXPROCS(1))
	if err != nil {
		return nil, err
	}
	kv, err := genEtcdKV()
	if err != nil {
		return nil, err
	}
	cm := storage.NewLocalChunkManager(storage.RootPath(defaultLocalStorage))
	return newSegmentLoader(metaReplica, kv, cm, factory, pool), nil
}

func genSimpleReplicaWithSealSegment(ctx context.Context) (ReplicaInterface, error) {
	r, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}
	seg, err := genSimpleSealedSegment(defaultMsgLength)
	if err != nil {
		return nil, err
	}
	err = r.setSegment(seg)
	if err != nil {
		return nil, err
	}
	col, err := r.getCollectionByID(defaultCollectionID)
	if err != nil {
		return nil, err
	}
	col.addVChannels([]Channel{
		defaultDeltaChannel,
	})
	return r, nil
}

func genSimpleReplicaWithGrowingSegment() (ReplicaInterface, error) {
	r, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}
	err = r.addSegment(defaultSegmentID,
		defaultPartitionID,
		defaultCollectionID,
		defaultDMLChannel,
		defaultSegmentVersion,
		segmentTypeGrowing)
	if err != nil {
		return nil, err
	}
	col, err := r.getCollectionByID(defaultCollectionID)
	if err != nil {
		return nil, err
	}
	col.addVChannels([]Channel{
		defaultDMLChannel,
	})
	return r, nil
}

// ---------- unittest util functions ----------
// functions of messages and requests
func genIVFFlatDSL(schema *schemapb.CollectionSchema, nProb int, topK int64, roundDecimal int64) (string, error) {
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

func genHNSWDSL(schema *schemapb.CollectionSchema, ef int, topK int64, roundDecimal int64) (string, error) {
	var vecFieldName string
	var metricType string
	efStr := strconv.Itoa(ef)
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
		"\", \n \"params\": {\n \"ef\": " + efStr + " \n},\n \"query\": \"$0\",\n \"topk\": " + topKStr +
		" \n,\"round_decimal\": " + roundDecimalStr +
		"\n } \n } \n } \n }", nil
}

func genBruteForceDSL(schema *schemapb.CollectionSchema, topK int64, roundDecimal int64) (string, error) {
	var vecFieldName string
	var metricType string
	topKStr := strconv.FormatInt(topK, 10)
	nProbStr := strconv.Itoa(defaultNProb)
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

func genDSLByIndexType(schema *schemapb.CollectionSchema, indexType string) (string, error) {
	if indexType == IndexFaissIDMap { // float vector
		return genBruteForceDSL(schema, defaultTopK, defaultRoundDecimal)
	} else if indexType == IndexFaissBinIDMap {
		return genBruteForceDSL(schema, defaultTopK, defaultRoundDecimal)
	} else if indexType == IndexFaissIVFFlat {
		return genIVFFlatDSL(schema, defaultNProb, defaultTopK, defaultRoundDecimal)
	} else if indexType == IndexFaissBinIVFFlat { // binary vector
		return genIVFFlatDSL(schema, defaultNProb, defaultTopK, defaultRoundDecimal)
	} else if indexType == IndexHNSW {
		return genHNSWDSL(schema, defaultEf, defaultTopK, defaultRoundDecimal)
	}
	return "", fmt.Errorf("Invalid indexType")
}

func genPlaceHolderGroup(nq int64) ([]byte, error) {
	placeholderValue := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: make([][]byte, 0),
	}
	for i := int64(0); i < nq; i++ {
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
	placeholderGroup := commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{placeholderValue},
	}
	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		return nil, err
	}
	return placeGroupByte, nil
}

func genSearchPlanAndRequests(collection *Collection, indexType string, nq int64) (*searchRequest, error) {

	iReq, _ := genSearchRequest(nq, indexType, collection.schema)
	queryReq := &querypb.SearchRequest{
		Req:             iReq,
		DmlChannels:     []string{defaultDMLChannel},
		SegmentIDs:      []UniqueID{defaultSegmentID},
		FromShardLeader: true,
		Scope:           querypb.DataScope_Historical,
	}
	return newSearchRequest(collection, queryReq, queryReq.Req.GetPlaceholderGroup())
}

func genSimpleRetrievePlanExpr(schema *schemapb.CollectionSchema) ([]byte, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: &planpb.ColumnInfo{
							FieldId:  pkField.FieldID,
							DataType: pkField.DataType,
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
		OutputFieldIds: []int64{pkField.FieldID},
	}
	planExpr, err := proto.Marshal(planNode)
	return planExpr, err
}

func genSimpleRetrievePlan(collection *Collection) (*RetrievePlan, error) {
	retrieveMsg, err := genRetrieveMsg(collection.schema)
	if err != nil {
		return nil, err
	}
	timestamp := retrieveMsg.RetrieveRequest.TravelTimestamp

	plan, err2 := createRetrievePlanByExpr(collection, retrieveMsg.SerializedExprPlan, timestamp, 100)
	return plan, err2
}

func genGetCollectionStatisticRequest() (*internalpb.GetStatisticsRequest, error) {
	return &internalpb.GetStatisticsRequest{
		Base:         genCommonMsgBase(commonpb.MsgType_GetCollectionStatistics),
		DbID:         0,
		CollectionID: defaultCollectionID,
	}, nil
}

func genGetPartitionStatisticRequest() (*internalpb.GetStatisticsRequest, error) {
	return &internalpb.GetStatisticsRequest{
		Base:         genCommonMsgBase(commonpb.MsgType_GetPartitionStatistics),
		DbID:         0,
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}, nil
}

func genSearchRequest(nq int64, indexType string, schema *schemapb.CollectionSchema) (*internalpb.SearchRequest, error) {
	placeHolder, err := genPlaceHolderGroup(nq)
	if err != nil {
		return nil, err
	}
	simpleDSL, err2 := genDSLByIndexType(schema, indexType)
	if err2 != nil {
		return nil, err2
	}
	return &internalpb.SearchRequest{
		Base:             genCommonMsgBase(commonpb.MsgType_Search),
		CollectionID:     defaultCollectionID,
		PartitionIDs:     []UniqueID{defaultPartitionID},
		Dsl:              simpleDSL,
		PlaceholderGroup: placeHolder,
		DslType:          commonpb.DslType_Dsl,
		Nq:               nq,
	}, nil
}

func genRetrieveRequest(schema *schemapb.CollectionSchema) (*internalpb.RetrieveRequest, error) {
	expr, err := genSimpleRetrievePlanExpr(schema)
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
		OutputFieldsId:     []int64{100, 105, 106},
		TravelTimestamp:    Timestamp(1000),
		SerializedExprPlan: expr,
	}, nil
}

func genRetrieveMsg(schema *schemapb.CollectionSchema) (*msgstream.RetrieveMsg, error) {
	req, err := genRetrieveRequest(schema)
	if err != nil {
		return nil, err
	}

	msg := &msgstream.RetrieveMsg{
		BaseMsg:         genMsgStreamBaseMsg(),
		RetrieveRequest: *req,
	}
	msg.SetTimeRecorder()
	return msg, nil
}

func genQueryResultChannel() Channel {
	const queryResultChannelPrefix = "query-node-unittest-query-result-channel-"
	return queryResultChannelPrefix + strconv.Itoa(rand.Int())
}

func checkSearchResult(nq int64, plan *SearchPlan, searchResult *SearchResult) error {
	searchResults := make([]*SearchResult, 0)
	searchResults = append(searchResults, searchResult)

	topK := plan.getTopK()
	sliceNQs := []int64{nq / 5, nq / 5, nq / 5, nq / 5, nq / 5}
	sliceTopKs := []int64{topK, topK / 2, topK, topK, topK / 2}
	sInfo := parseSliceInfo(sliceNQs, sliceTopKs, nq)

	res, err := reduceSearchResultsAndFillData(plan, searchResults, 1, sInfo.sliceNQs, sInfo.sliceTopKs)
	if err != nil {
		return err
	}

	for i := 0; i < len(sInfo.sliceNQs); i++ {
		blob, err := getSearchResultDataBlob(res, i)
		if err != nil {
			return err
		}
		if len(blob) == 0 {
			return fmt.Errorf("wrong search result data blobs when checkSearchResult")
		}

		result := &schemapb.SearchResultData{}
		err = proto.Unmarshal(blob, result)
		if err != nil {
			return err
		}

		if result.TopK != sliceTopKs[i] {
			return fmt.Errorf("unexpected topK when checkSearchResult")
		}
		if result.NumQueries != sInfo.sliceNQs[i] {
			return fmt.Errorf("unexpected nq when checkSearchResult")
		}
		// search empty segment, return empty result.IDs
		if len(result.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data) != 0 {
			return fmt.Errorf("unexpected Ids when checkSearchResult")
		}
		if len(result.Scores) != 0 {
			return fmt.Errorf("unexpected Scores when checkSearchResult")
		}
	}

	deleteSearchResults(searchResults)
	deleteSearchResultDataBlobs(res)
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

func genSimpleSegmentInfo() *querypb.SegmentInfo {
	return &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
	}
}

func genSimpleChangeInfo() *querypb.SealedSegmentsChangeInfo {
	changeInfo := &querypb.SegmentChangeInfo{
		OnlineNodeID: Params.QueryNodeCfg.GetNodeID(),
		OnlineSegments: []*querypb.SegmentInfo{
			genSimpleSegmentInfo(),
		},
		OfflineNodeID: Params.QueryNodeCfg.GetNodeID() + 1,
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

func genSimpleQueryNodeWithMQFactory(ctx context.Context, fac dependency.Factory) (*QueryNode, error) {
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

	replica, err := genSimpleReplicaWithSealSegment(ctx)
	if err != nil {
		return nil, err
	}
	node.tSafeReplica.addTSafe(defaultDMLChannel)

	node.tSafeReplica.addTSafe(defaultDeltaChannel)
	node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, replica, node.tSafeReplica, node.factory)

	node.metaReplica = replica

	loader, err := genSimpleSegmentLoaderWithMqFactory(replica, fac)
	if err != nil {
		return nil, err
	}
	node.loader = loader

	// start task scheduler
	go node.scheduler.Start()
	err = node.initRateCollector()
	if err != nil {
		return nil, err
	}

	// init shard cluster service
	node.ShardClusterService = newShardClusterService(node.etcdCli, node.session, node)

	node.queryShardService = newQueryShardService(node.queryNodeLoopCtx,
		node.metaReplica, node.tSafeReplica,
		node.ShardClusterService, node.factory, node.scheduler)

	node.UpdateStateCode(internalpb.StateCode_Healthy)

	return node, nil
}

// node
func genSimpleQueryNode(ctx context.Context) (*QueryNode, error) {
	fac := genFactory()
	return genSimpleQueryNodeWithMQFactory(ctx, fac)
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
	case schemapb.DataType_VarChar:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: fieldValue.([]string),
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

type mockMsgStreamFactory struct {
	dependency.Factory
	mockMqStream msgstream.MsgStream
}

var _ dependency.Factory = &mockMsgStreamFactory{}

func (mm *mockMsgStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return mm.mockMqStream, nil
}

func (mm *mockMsgStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return nil, nil
}

func (mm *mockMsgStreamFactory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return nil, nil
}
func (mm *mockMsgStreamFactory) NewCacheStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return nil, nil
}
func (mm *mockMsgStreamFactory) NewVectorStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return nil, nil
}

type readAtFunc func(path string, offset int64, length int64) ([]byte, error)
type readFunc func(path string) ([]byte, error)

type mockChunkManager struct {
	storage.ChunkManager
	readAt readAtFunc
	read   readFunc
}

type mockChunkManagerOpt func(*mockChunkManager)

func defaultReadAt(path string, offset int64, length int64) ([]byte, error) {
	return funcutil.RandomBytes(int(length)), nil
}

func defaultRead(path string) ([]byte, error) {
	return []byte(path), nil
}

func readBool(maxOffset int64) ([]byte, error) {
	var arr schemapb.BoolArray
	for i := int64(0); i <= maxOffset; i++ {
		arr.Data = append(arr.Data, i%2 == 0)
	}
	return proto.Marshal(&arr)
}

func readIllegalBool() ([]byte, error) {
	return []byte("can convert to bool array"), nil
}

func readString(maxOffset int64) ([]byte, error) {
	var arr schemapb.StringArray
	for i := int64(0); i <= maxOffset; i++ {
		arr.Data = append(arr.Data, funcutil.GenRandomStr())
	}
	return proto.Marshal(&arr)
}

func readIllegalString() ([]byte, error) {
	return []byte("can convert to string array"), nil
}

func readAtEmptyContent() ([]byte, error) {
	return []byte{}, nil
}

func withReadAt(f readAtFunc) mockChunkManagerOpt {
	return func(manager *mockChunkManager) {
		manager.readAt = f
	}
}

func withRead(f readFunc) mockChunkManagerOpt {
	return func(manager *mockChunkManager) {
		manager.read = f
	}
}

func withDefaultReadAt() mockChunkManagerOpt {
	return withReadAt(defaultReadAt)
}

func withDefaultRead() mockChunkManagerOpt {
	return withRead(defaultRead)
}

func withReadErr() mockChunkManagerOpt {
	return withRead(func(path string) ([]byte, error) {
		return nil, errors.New("mock")
	})
}

func withReadAtErr() mockChunkManagerOpt {
	return withReadAt(func(path string, offset int64, length int64) ([]byte, error) {
		return nil, errors.New("mock")
	})
}

func withReadBool(maxOffset int64) mockChunkManagerOpt {
	return withRead(func(path string) ([]byte, error) {
		return readBool(maxOffset)
	})
}

func withReadIllegalBool() mockChunkManagerOpt {
	return withRead(func(path string) ([]byte, error) {
		return readIllegalBool()
	})
}

func withReadString(maxOffset int64) mockChunkManagerOpt {
	return withRead(func(path string) ([]byte, error) {
		return readString(maxOffset)
	})
}

func withReadIllegalString() mockChunkManagerOpt {
	return withRead(func(path string) ([]byte, error) {
		return readIllegalString()
	})
}

func withReadAtEmptyContent() mockChunkManagerOpt {
	return withReadAt(func(path string, offset int64, length int64) ([]byte, error) {
		return readAtEmptyContent()
	})
}

func newMockChunkManager(opts ...mockChunkManagerOpt) storage.ChunkManager {
	ret := &mockChunkManager{}
	for _, opt := range opts {
		if opt != nil {
			opt(ret)
		}
	}
	return ret
}

func (m *mockChunkManager) ReadAt(path string, offset int64, length int64) ([]byte, error) {
	if m.readAt != nil {
		return m.readAt(path, offset, length)
	}
	return defaultReadAt(path, offset, length)
}

func (m *mockChunkManager) Read(path string) ([]byte, error) {
	if m.read != nil {
		return m.read(path)
	}
	return defaultRead(path)
}
