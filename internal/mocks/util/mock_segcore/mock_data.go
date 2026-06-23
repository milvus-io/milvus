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

package mock_segcore

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"path"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	IndexFaissIDMap      = "FLAT"
	IndexFaissIVFFlat    = "IVF_FLAT"
	IndexFaissIVFPQ      = "IVF_PQ"
	IndexFaissIVFSQ8     = "IVF_SQ8"
	IndexScaNN           = "SCANN"
	IndexFaissBinIDMap   = "BIN_FLAT"
	IndexFaissBinIVFFlat = "BIN_IVF_FLAT"
	IndexHNSW            = "HNSW"
	IndexSparseWand      = "SPARSE_WAND"

	nlist               = 100
	m                   = 4
	nbits               = 8
	nprobe              = 8
	efConstruction      = 200
	ef                  = 200
	defaultTopK         = 10
	defaultRoundDecimal = 6
	defaultNProb        = 10

	rowIDFieldID      = 0
	timestampFieldID  = 1
	metricTypeKey     = common.MetricTypeKey
	DefaultDim        = 128
	defaultMetricType = metric.L2

	dimKey = common.DimKey

	defaultLocalStorage = "/tmp/milvus_test/querynode"
)

// ---------- unittest util functions ----------
// gen collection schema for
type vecFieldParam struct {
	ID         int64
	dim        int
	metricType string
	vecType    schemapb.DataType
	fieldName  string
}

type constFieldParam struct {
	ID        int64
	dataType  schemapb.DataType
	fieldName string
}

var SimpleFloatVecField = vecFieldParam{
	ID:         100,
	dim:        DefaultDim,
	metricType: defaultMetricType,
	vecType:    schemapb.DataType_FloatVector,
	fieldName:  "floatVectorField",
}

var simpleBinVecField = vecFieldParam{
	ID:         101,
	dim:        DefaultDim,
	metricType: metric.JACCARD,
	vecType:    schemapb.DataType_BinaryVector,
	fieldName:  "binVectorField",
}

var simpleFloat16VecField = vecFieldParam{
	ID:         112,
	dim:        DefaultDim,
	metricType: defaultMetricType,
	vecType:    schemapb.DataType_Float16Vector,
	fieldName:  "float16VectorField",
}

var simpleBFloat16VecField = vecFieldParam{
	ID:         113,
	dim:        DefaultDim,
	metricType: defaultMetricType,
	vecType:    schemapb.DataType_BFloat16Vector,
	fieldName:  "bfloat16VectorField",
}

var SimpleSparseFloatVectorField = vecFieldParam{
	ID:         114,
	metricType: metric.IP,
	vecType:    schemapb.DataType_SparseFloatVector,
	fieldName:  "sparseFloatVectorField",
}

var simpleBoolField = constFieldParam{
	ID:        102,
	dataType:  schemapb.DataType_Bool,
	fieldName: "boolField",
}

var simpleInt8Field = constFieldParam{
	ID:        103,
	dataType:  schemapb.DataType_Int8,
	fieldName: "int8Field",
}

var simpleInt16Field = constFieldParam{
	ID:        104,
	dataType:  schemapb.DataType_Int16,
	fieldName: "int16Field",
}

var simpleInt32Field = constFieldParam{
	ID:        105,
	dataType:  schemapb.DataType_Int32,
	fieldName: "int32Field",
}

var simpleInt64Field = constFieldParam{
	ID:        106,
	dataType:  schemapb.DataType_Int64,
	fieldName: "int64Field",
}

var simpleFloatField = constFieldParam{
	ID:        107,
	dataType:  schemapb.DataType_Float,
	fieldName: "floatField",
}

var simpleDoubleField = constFieldParam{
	ID:        108,
	dataType:  schemapb.DataType_Double,
	fieldName: "doubleField",
}

var simpleJSONField = constFieldParam{
	ID:        109,
	dataType:  schemapb.DataType_JSON,
	fieldName: "jsonField",
}

var simpleArrayField = constFieldParam{
	ID:        110,
	dataType:  schemapb.DataType_Array,
	fieldName: "arrayField",
}

var simpleVarCharField = constFieldParam{
	ID:        111,
	dataType:  schemapb.DataType_VarChar,
	fieldName: "varCharField",
}

var RowIDField = constFieldParam{
	ID:        rowIDFieldID,
	dataType:  schemapb.DataType_Int64,
	fieldName: "RowID",
}

var timestampField = constFieldParam{
	ID:        timestampFieldID,
	dataType:  schemapb.DataType_Int64,
	fieldName: "Timestamp",
}

func genConstantFieldSchema(param constFieldParam) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:      param.ID,
		Name:         param.fieldName,
		IsPrimaryKey: false,
		DataType:     param.dataType,
		ElementType:  schemapb.DataType_Int32,
	}
	if param.dataType == schemapb.DataType_VarChar {
		field.TypeParams = []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "128"},
		}
	}
	return field
}

func genPKFieldSchema(param constFieldParam) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:      param.ID,
		Name:         param.fieldName,
		IsPrimaryKey: true,
		DataType:     param.dataType,
	}

	if param.dataType == schemapb.DataType_VarChar {
		field.TypeParams = []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "12"},
		}
	}
	return field
}

func genVectorFieldSchema(param vecFieldParam) *schemapb.FieldSchema {
	fieldVec := &schemapb.FieldSchema{
		FieldID:      param.ID,
		Name:         param.fieldName,
		IsPrimaryKey: false,
		DataType:     param.vecType,
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   metricTypeKey,
				Value: param.metricType,
			},
		},
	}
	if fieldVec.DataType != schemapb.DataType_SparseFloatVector {
		fieldVec.TypeParams = []*commonpb.KeyValuePair{
			{
				Key:   dimKey,
				Value: strconv.Itoa(param.dim),
			},
		}
	}
	return fieldVec
}

func GenTestBM25CollectionSchema(collectionName string) *schemapb.CollectionSchema {
	fieldRowID := genConstantFieldSchema(RowIDField)
	fieldTimestamp := genConstantFieldSchema(timestampField)
	pkFieldSchema := genPKFieldSchema(simpleInt64Field)
	textFieldSchema := genConstantFieldSchema(simpleVarCharField)
	sparseFieldSchema := genVectorFieldSchema(SimpleSparseFloatVectorField)
	sparseFieldSchema.IsFunctionOutput = true

	schema := &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			fieldRowID,
			fieldTimestamp,
			pkFieldSchema,
			textFieldSchema,
			sparseFieldSchema,
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "BM25",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{textFieldSchema.GetName()},
			InputFieldIds:    []int64{textFieldSchema.GetFieldID()},
			OutputFieldNames: []string{sparseFieldSchema.GetName()},
			OutputFieldIds:   []int64{sparseFieldSchema.GetFieldID()},
		}},
	}
	return schema
}

// some tests do not yet support sparse float vector, see comments of
// GenSparseFloatVecDataset in indexcgowrapper/dataset.go
func GenTestCollectionSchema(collectionName string, pkType schemapb.DataType, withSparse bool) *schemapb.CollectionSchema {
	fieldRowID := genConstantFieldSchema(RowIDField)
	fieldTimestamp := genConstantFieldSchema(timestampField)
	fieldBool := genConstantFieldSchema(simpleBoolField)
	fieldInt8 := genConstantFieldSchema(simpleInt8Field)
	fieldInt16 := genConstantFieldSchema(simpleInt16Field)
	fieldInt32 := genConstantFieldSchema(simpleInt32Field)
	fieldFloat := genConstantFieldSchema(simpleFloatField)
	fieldDouble := genConstantFieldSchema(simpleDoubleField)
	// fieldArray := genConstantFieldSchema(simpleArrayField)
	fieldJSON := genConstantFieldSchema(simpleJSONField)
	fieldArray := genConstantFieldSchema(simpleArrayField)
	floatVecFieldSchema := genVectorFieldSchema(SimpleFloatVecField)
	binVecFieldSchema := genVectorFieldSchema(simpleBinVecField)
	float16VecFieldSchema := genVectorFieldSchema(simpleFloat16VecField)
	bfloat16VecFieldSchema := genVectorFieldSchema(simpleBFloat16VecField)
	var pkFieldSchema *schemapb.FieldSchema

	switch pkType {
	case schemapb.DataType_Int64:
		pkFieldSchema = genPKFieldSchema(simpleInt64Field)
	case schemapb.DataType_VarChar:
		pkFieldSchema = genPKFieldSchema(simpleVarCharField)
	}

	schema := schemapb.CollectionSchema{ // schema for segCore
		Name:   collectionName,
		AutoID: false,
		Fields: []*schemapb.FieldSchema{
			fieldBool,
			fieldInt8,
			fieldInt16,
			fieldInt32,
			fieldFloat,
			fieldDouble,
			fieldJSON,
			floatVecFieldSchema,
			binVecFieldSchema,
			pkFieldSchema,
			fieldArray,
			float16VecFieldSchema,
			bfloat16VecFieldSchema,
		},
	}

	if withSparse {
		schema.Fields = append(schema.Fields, genVectorFieldSchema(SimpleSparseFloatVectorField))
	}

	for i, field := range schema.GetFields() {
		field.FieldID = 100 + int64(i)
	}
	schema.Fields = append(schema.Fields, fieldRowID, fieldTimestamp)
	return &schema
}

// GenTestCollectionSchemaWithNullableVec returns the standard test schema with
// the float_vector field marked Nullable. Paired with GenInsertData's ValidData
// population, this enables end-to-end tests against nullable vector fields
// without per-test schema scaffolding.
func GenTestCollectionSchemaWithNullableVec(collectionName string, pkType schemapb.DataType) *schemapb.CollectionSchema {
	schema := GenTestCollectionSchema(collectionName, pkType, false)
	for _, f := range schema.Fields {
		if f.DataType == schemapb.DataType_FloatVector {
			f.Nullable = true
			break
		}
	}
	return schema
}

func GenTestIndexInfoList(collectionID int64, schema *schemapb.CollectionSchema) []*indexpb.IndexInfo {
	res := make([]*indexpb.IndexInfo, 0)
	vectorFieldSchemas := typeutil.GetVectorFieldSchemas(schema)
	for _, field := range vectorFieldSchemas {
		index := &indexpb.IndexInfo{
			CollectionID: collectionID,
			FieldID:      field.GetFieldID(),
			// For now, a field can only have one index
			// using fieldID and fieldName as indexID and indexName, just make sure not repeated.
			IndexID:    field.GetFieldID(),
			IndexName:  field.GetName(),
			TypeParams: field.GetTypeParams(),
		}
		switch field.GetDataType() {
		case schemapb.DataType_FloatVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
			{
				index.IndexParams = []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: metric.L2},
					{Key: common.IndexTypeKey, Value: IndexFaissIVFFlat},
					{Key: "nlist", Value: "128"},
					{Key: common.LoadPriorityKey, Value: "high"},
				}
			}
		case schemapb.DataType_BinaryVector:
			{
				index.IndexParams = []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: metric.JACCARD},
					{Key: common.IndexTypeKey, Value: IndexFaissBinIVFFlat},
					{Key: "nlist", Value: "128"},
					{Key: common.LoadPriorityKey, Value: "high"},
				}
			}
		case schemapb.DataType_SparseFloatVector:
			{
				index.IndexParams = []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: metric.IP},
					{Key: common.IndexTypeKey, Value: IndexSparseWand},
					{Key: "M", Value: "16"},
					{Key: common.LoadPriorityKey, Value: "high"},
				}
			}
		}
		res = append(res, index)
	}
	return res
}

func GenTestIndexMeta(collectionID int64, schema *schemapb.CollectionSchema) *segcorepb.CollectionIndexMeta {
	indexInfos := GenTestIndexInfoList(collectionID, schema)
	fieldIndexMetas := make([]*segcorepb.FieldIndexMeta, 0)
	for _, info := range indexInfos {
		fieldIndexMetas = append(fieldIndexMetas, &segcorepb.FieldIndexMeta{
			CollectionID:    info.GetCollectionID(),
			FieldID:         info.GetFieldID(),
			IndexName:       info.GetIndexName(),
			TypeParams:      info.GetTypeParams(),
			IndexParams:     info.GetIndexParams(),
			IsAutoIndex:     info.GetIsAutoIndex(),
			UserIndexParams: info.GetUserIndexParams(),
		})
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	maxIndexRecordPerSegment := int64(0)
	if err != nil || sizePerRecord == 0 {
		log.Warn("failed to transfer segment size to collection, because failed to estimate size per record", zap.Error(err))
	} else {
		threshold := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024
		proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
		maxIndexRecordPerSegment = int64(threshold * proportion / float64(sizePerRecord))
	}

	indexMeta := segcorepb.CollectionIndexMeta{
		MaxIndexRowCount: maxIndexRecordPerSegment,
		IndexMetas:       fieldIndexMetas,
	}

	return &indexMeta
}

func NewTestChunkManagerFactory(params *paramtable.ComponentParam, rootPath string) *storage.ChunkManagerFactory {
	return storage.NewChunkManagerFactory("minio",
		objectstorage.RootPath(rootPath),
		objectstorage.Address(params.MinioCfg.Address.GetValue()),
		objectstorage.AccessKeyID(params.MinioCfg.AccessKeyID.GetValue()),
		objectstorage.SecretAccessKeyID(params.MinioCfg.SecretAccessKey.GetValue()),
		objectstorage.UseSSL(params.MinioCfg.UseSSL.GetAsBool()),
		objectstorage.SslCACert(params.MinioCfg.SslCACert.GetValue()),
		objectstorage.BucketName(params.MinioCfg.BucketName.GetValue()),
		objectstorage.UseIAM(params.MinioCfg.UseIAM.GetAsBool()),
		objectstorage.CloudProvider(params.MinioCfg.CloudProvider.GetValue()),
		objectstorage.IAMEndpoint(params.MinioCfg.IAMEndpoint.GetValue()),
		objectstorage.CreateBucket(true))
}

func SaveBinLog(ctx context.Context,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	msgLength int,
	schema *schemapb.CollectionSchema,
	chunkManager storage.ChunkManager,
) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	insertData, err := GenInsertData(msgLength, schema)
	if err != nil {
		return nil, nil, err
	}
	return SaveBinLogWithData(ctx, collectionID, partitionID, segmentID, insertData, schema, chunkManager)
}

// SaveBinLogWithData is the same as SaveBinLog but accepts caller-built
// InsertData. Tests that need to inspect or control the exact data written
// (e.g. nullable vector tests that verify per-row payloads) use this.
func SaveBinLogWithData(ctx context.Context,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	insertData *storage.InsertData,
	schema *schemapb.CollectionSchema,
	chunkManager storage.ChunkManager,
) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	log := log.Ctx(ctx)
	binLogs, statsLogs, err := serializeInsertData(collectionID, partitionID, segmentID, insertData, schema)
	if err != nil {
		log.Warn("serializeInsertData return error", zap.Error(err))
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

		k := metautil.JoinIDPath(collectionID, partitionID, segmentID, fieldID)
		key := path.Join(chunkManager.RootPath(), "insert-log", k)
		kvs[key] = blob.Value
		fieldBinlog = append(fieldBinlog, &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{{
				LogPath:    key,
				EntriesNum: blob.RowNum,
			}},
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

		k := metautil.JoinIDPath(collectionID, partitionID, segmentID, fieldID)
		key := path.Join(chunkManager.RootPath(), "stats-log", k)
		kvs[key] = blob.Value
		statsBinlog = append(statsBinlog, &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{{LogPath: key}},
		})
	}
	log.Debug("[query node unittest] save statsLog file to MinIO/S3")

	err = chunkManager.MultiWrite(ctx, kvs)
	return fieldBinlog, statsBinlog, err
}

func serializeInsertData(collectionID int64,
	partitionID int64,
	segmentID int64,
	insertData *storage.InsertData,
	schema *schemapb.CollectionSchema,
) ([]*storage.Blob, []*storage.Blob, error) {
	collMeta := genCollectionMeta(collectionID, partitionID, schema)
	inCodec := storage.NewInsertCodecWithSchema(collMeta)

	binLogs, err := inCodec.Serialize(partitionID, segmentID, insertData)
	if err != nil {
		return nil, nil, err
	}

	statsLog, err := inCodec.SerializePkStatsByData(insertData)
	if err != nil {
		return nil, nil, err
	}

	return binLogs, []*storage.Blob{statsLog}, nil
}

func genCollectionMeta(collectionID int64, partitionID int64, schema *schemapb.CollectionSchema) *etcdpb.CollectionMeta {
	colInfo := &etcdpb.CollectionMeta{
		ID:           collectionID,
		Schema:       schema,
		PartitionIDs: []int64{partitionID},
	}
	return colInfo
}

// NullablePatternValidData returns a deterministic valid_data bitmap for
// nullable test fields: every 3rd row is null (rows 0, 3, 6, ...). Used by
// both the insert generator and tests that want to know the pattern upfront.
func NullablePatternValidData(msgLength int) []bool {
	out := make([]bool, msgLength)
	for i := range out {
		out[i] = i%3 != 0
	}
	return out
}

func compactFloatVecData(src []float32, valid []bool, dim int) []float32 {
	out := make([]float32, 0, len(src))
	for i, v := range valid {
		if v {
			out = append(out, src[i*dim:(i+1)*dim]...)
		}
	}
	return out
}

func compactByteVecData(src []byte, valid []bool, rowBytes int) []byte {
	out := make([]byte, 0, len(src))
	for i, v := range valid {
		if v {
			out = append(out, src[i*rowBytes:(i+1)*rowBytes]...)
		}
	}
	return out
}

func compactInt8VecData(src []int8, valid []bool, dim int) []int8 {
	out := make([]int8, 0, len(src))
	for i, v := range valid {
		if v {
			out = append(out, src[i*dim:(i+1)*dim]...)
		}
	}
	return out
}

// GenInsertData builds InsertData for testing. Nullable fields are stamped
// with ValidData from NullablePatternValidData so that end-to-end tests can
// predict which rows are null.
func GenInsertData(msgLength int, schema *schemapb.CollectionSchema) (*storage.InsertData, error) {
	insertData := &storage.InsertData{
		Data: make(map[int64]storage.FieldData),
	}

	for _, f := range schema.Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			insertData.Data[f.FieldID] = &storage.BoolFieldData{
				Data: testutils.GenerateBoolArray(msgLength),
			}
		case schemapb.DataType_Int8:
			insertData.Data[f.FieldID] = &storage.Int8FieldData{
				Data: testutils.GenerateInt8Array(msgLength),
			}
		case schemapb.DataType_Int16:
			insertData.Data[f.FieldID] = &storage.Int16FieldData{
				Data: testutils.GenerateInt16Array(msgLength),
			}
		case schemapb.DataType_Int32:
			insertData.Data[f.FieldID] = &storage.Int32FieldData{
				Data: testutils.GenerateInt32Array(msgLength),
			}
		case schemapb.DataType_Int64:
			insertData.Data[f.FieldID] = &storage.Int64FieldData{
				Data: testutils.GenerateInt64Array(msgLength),
			}
		case schemapb.DataType_Float:
			insertData.Data[f.FieldID] = &storage.FloatFieldData{
				Data: testutils.GenerateFloat32Array(msgLength),
			}
		case schemapb.DataType_Double:
			insertData.Data[f.FieldID] = &storage.DoubleFieldData{
				Data: testutils.GenerateFloat64Array(msgLength),
			}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			insertData.Data[f.FieldID] = &storage.StringFieldData{
				Data: testutils.GenerateStringArray(msgLength),
			}
		case schemapb.DataType_Array:
			insertData.Data[f.FieldID] = &storage.ArrayFieldData{
				ElementType: schemapb.DataType_Int32,
				Data:        testutils.GenerateArrayOfIntArray(msgLength),
			}
		case schemapb.DataType_JSON:
			insertData.Data[f.FieldID] = &storage.JSONFieldData{
				Data: testutils.GenerateJSONArray(msgLength),
			}
		case schemapb.DataType_FloatVector:
			dim := SimpleFloatVecField.dim // if no dim specified, use simpleFloatVecField's dim
			insertData.Data[f.FieldID] = &storage.FloatVectorFieldData{
				Data: testutils.GenerateFloatVectors(msgLength, dim),
				Dim:  dim,
			}
		case schemapb.DataType_Float16Vector:
			dim := simpleFloat16VecField.dim
			insertData.Data[f.FieldID] = &storage.Float16VectorFieldData{
				Data: testutils.GenerateFloat16Vectors(msgLength, dim),
				Dim:  dim,
			}
		case schemapb.DataType_BFloat16Vector:
			dim := simpleFloat16VecField.dim
			insertData.Data[f.FieldID] = &storage.BFloat16VectorFieldData{
				Data: testutils.GenerateBFloat16Vectors(msgLength, dim),
				Dim:  dim,
			}
		case schemapb.DataType_BinaryVector:
			dim := simpleBinVecField.dim
			insertData.Data[f.FieldID] = &storage.BinaryVectorFieldData{
				Data: testutils.GenerateBinaryVectors(msgLength, dim),
				Dim:  dim,
			}
		case schemapb.DataType_SparseFloatVector:
			contents, dim := testutils.GenerateSparseFloatVectorsData(msgLength)
			insertData.Data[f.FieldID] = &storage.SparseFloatVectorFieldData{
				SparseFloatArray: schemapb.SparseFloatArray{
					Contents: contents,
					Dim:      dim,
				},
			}
		default:
			err := errors.New("data type not supported")
			return nil, err
		}

		// Populate ValidData for nullable fields using the shared pattern so
		// binlog serialization preserves the null bits end-to-end. For vector
		// fields the binlog serializer requires Data to be pre-compacted to
		// valid_count * dim (null rows dropped), so we shrink Data here too.
		if f.Nullable {
			valid := NullablePatternValidData(msgLength)
			switch fd := insertData.Data[f.FieldID].(type) {
			case *storage.FloatVectorFieldData:
				fd.Data = compactFloatVecData(fd.Data, valid, fd.Dim)
				fd.ValidData = valid
			case *storage.Float16VectorFieldData:
				fd.Data = compactByteVecData(fd.Data, valid, fd.Dim*2)
				fd.ValidData = valid
			case *storage.BFloat16VectorFieldData:
				fd.Data = compactByteVecData(fd.Data, valid, fd.Dim*2)
				fd.ValidData = valid
			case *storage.BinaryVectorFieldData:
				fd.Data = compactByteVecData(fd.Data, valid, fd.Dim/8)
				fd.ValidData = valid
			case *storage.Int8VectorFieldData:
				fd.Data = compactInt8VecData(fd.Data, valid, fd.Dim)
				fd.ValidData = valid
			case *storage.BoolFieldData:
				fd.ValidData = valid
			case *storage.Int8FieldData:
				fd.ValidData = valid
			case *storage.Int16FieldData:
				fd.ValidData = valid
			case *storage.Int32FieldData:
				fd.ValidData = valid
			case *storage.Int64FieldData:
				fd.ValidData = valid
			case *storage.FloatFieldData:
				fd.ValidData = valid
			case *storage.DoubleFieldData:
				fd.ValidData = valid
			case *storage.StringFieldData:
				fd.ValidData = valid
			case *storage.JSONFieldData:
				fd.ValidData = valid
			case *storage.ArrayFieldData:
				fd.ValidData = valid
			}
		}
	}
	// set data for rowID field
	insertData.Data[rowIDFieldID] = &storage.Int64FieldData{
		Data: testutils.GenerateInt64Array(msgLength),
	}
	// set data for ts field
	insertData.Data[timestampFieldID] = &storage.Int64FieldData{
		Data: genTimestampFieldData(msgLength),
	}
	return insertData, nil
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

func SaveDeltaLog(collectionID int64,
	partitionID int64,
	segmentID int64,
	cm storage.ChunkManager,
) ([]*datapb.FieldBinlog, error) {
	log := log.Ctx(context.TODO())
	binlogWriter := storage.NewDeleteBinlogWriter(schemapb.DataType_String, collectionID, partitionID, segmentID)
	eventWriter, _ := binlogWriter.NextDeleteEventWriter()
	dData := &storage.DeleteData{
		Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(2)},
		Tss:      []typeutil.Timestamp{100, 200},
		RowCount: 2,
	}

	sizeTotal := 0
	for i := int64(0); i < dData.RowCount; i++ {
		int64PkValue := dData.Pks[i].(*storage.Int64PrimaryKey).Value
		ts := dData.Tss[i]
		eventWriter.AddOneStringToPayload(fmt.Sprintf("%d,%d", int64PkValue, ts), true)
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
	pkFieldID := int64(106)
	fieldBinlog := make([]*datapb.FieldBinlog, 0)
	log.Debug("[query node unittest] save delta log", zap.Int64("fieldID", pkFieldID))
	key := metautil.JoinIDPath(collectionID, partitionID, segmentID, pkFieldID)
	// keyPath := path.Join(defaultLocalStorage, "delta-log", key)
	keyPath := path.Join(cm.RootPath(), "delta-log", key)
	kvs[keyPath] = blob.Value
	fieldBinlog = append(fieldBinlog, &datapb.FieldBinlog{
		FieldID: pkFieldID,
		Binlogs: []*datapb.Binlog{{
			LogPath:       keyPath,
			TimestampFrom: 100,
			TimestampTo:   200,
		}},
	})
	log.Debug("[query node unittest] save delta log file to MinIO/S3")

	return fieldBinlog, cm.MultiWrite(context.Background(), kvs)
}

func SaveBM25Log(collectionID int64, partitionID int64, segmentID int64, fieldID int64, msgLength int, cm storage.ChunkManager) (*datapb.FieldBinlog, error) {
	stats := storage.NewBM25Stats()

	for i := 0; i < msgLength; i++ {
		stats.Append(map[uint32]float32{1: 1})
	}

	bytes, err := stats.Serialize()
	if err != nil {
		return nil, err
	}

	kvs := make(map[string][]byte, 1)
	key := path.Join(cm.RootPath(), common.SegmentBm25LogPath, metautil.JoinIDPath(collectionID, partitionID, segmentID, fieldID, 1001))
	kvs[key] = bytes
	fieldBinlog := &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: []*datapb.Binlog{{
			LogPath:       key,
			TimestampFrom: 100,
			TimestampTo:   200,
		}},
	}
	return fieldBinlog, cm.MultiWrite(context.Background(), kvs)
}

func GenAndSaveIndexV2(collectionID, partitionID, segmentID, buildID int64,
	fieldSchema *schemapb.FieldSchema,
	indexInfo *indexpb.IndexInfo,
	cm storage.ChunkManager,
	msgLength int,
) (*querypb.FieldIndexInfo, error) {
	typeParams := funcutil.KeyValuePair2Map(indexInfo.GetTypeParams())
	indexParams := funcutil.KeyValuePair2Map(indexInfo.GetIndexParams())

	index, err := indexcgowrapper.NewCgoIndex(fieldSchema.GetDataType(), typeParams, indexParams)
	if err != nil {
		return nil, err
	}
	defer index.Delete()

	var dataset *indexcgowrapper.Dataset
	switch fieldSchema.DataType {
	case schemapb.DataType_BinaryVector:
		dataset = indexcgowrapper.GenBinaryVecDataset(testutils.GenerateBinaryVectors(msgLength, DefaultDim))
	case schemapb.DataType_FloatVector:
		dataset = indexcgowrapper.GenFloatVecDataset(testutils.GenerateFloatVectors(msgLength, DefaultDim))
	case schemapb.DataType_Float16Vector:
		dataset = indexcgowrapper.GenFloat16VecDataset(testutils.GenerateFloat16Vectors(msgLength, DefaultDim))
	case schemapb.DataType_BFloat16Vector:
		dataset = indexcgowrapper.GenBFloat16VecDataset(testutils.GenerateBFloat16Vectors(msgLength, DefaultDim))
	case schemapb.DataType_SparseFloatVector:
		contents, dim := testutils.GenerateSparseFloatVectorsData(msgLength)
		dataset = indexcgowrapper.GenSparseFloatVecDataset(&storage.SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Contents: contents,
				Dim:      dim,
			},
		})
	}

	err = index.Build(dataset)
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
		buildID,
		0,
		collectionID,
		partitionID,
		segmentID,
		fieldSchema.GetFieldID(),
		indexParams,
		indexInfo.GetIndexName(),
		indexInfo.GetIndexID(),
		binarySet,
	)
	if err != nil {
		return nil, err
	}

	indexPaths := make([]string, 0)
	for _, index := range serializedIndexBlobs {
		indexPath := filepath.Join(cm.RootPath(), "index_files",
			strconv.Itoa(int(segmentID)), index.Key)
		indexPaths = append(indexPaths, indexPath)
		err := cm.Write(context.Background(), indexPath, index.Value)
		if err != nil {
			return nil, err
		}
	}
	indexVersion := segcore.GetIndexEngineInfo()

	return &querypb.FieldIndexInfo{
		FieldID:             fieldSchema.GetFieldID(),
		IndexName:           indexInfo.GetIndexName(),
		IndexParams:         indexInfo.GetIndexParams(),
		IndexFilePaths:      indexPaths,
		CurrentIndexVersion: indexVersion.CurrentIndexVersion,
		IndexID:             indexInfo.GetIndexID(),
	}, nil
}

func GenAndSaveIndex(collectionID, partitionID, segmentID, fieldID int64, msgLength int, indexType, metricType string, cm storage.ChunkManager) (*querypb.FieldIndexInfo, error) {
	typeParams, indexParams := genIndexParams(indexType, metricType)
	indexParams[common.LoadPriorityKey] = "HIGH"
	index, err := indexcgowrapper.NewCgoIndex(schemapb.DataType_FloatVector, typeParams, indexParams)
	if err != nil {
		return nil, err
	}
	defer index.Delete()

	err = index.Build(indexcgowrapper.GenFloatVecDataset(testutils.GenerateFloatVectors(msgLength, DefaultDim)))
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
		collectionID,
		partitionID,
		segmentID,
		SimpleFloatVecField.ID,
		indexParams,
		"querynode-test",
		0,
		binarySet,
	)
	if err != nil {
		return nil, err
	}

	indexPaths := make([]string, 0)
	for _, index := range serializedIndexBlobs {
		// indexPath := filepath.Join(defaultLocalStorage, strconv.Itoa(int(segmentID)), index.Key)
		indexPath := filepath.Join(cm.RootPath(), "index_files",
			strconv.Itoa(int(segmentID)), index.Key)
		indexPaths = append(indexPaths, indexPath)
		err := cm.Write(context.Background(), indexPath, index.Value)
		if err != nil {
			return nil, err
		}
	}
	indexEngineInfo := segcore.GetIndexEngineInfo()

	return &querypb.FieldIndexInfo{
		FieldID:             fieldID,
		IndexName:           "querynode-test",
		IndexParams:         funcutil.Map2KeyValuePair(indexParams),
		IndexFilePaths:      indexPaths,
		CurrentIndexVersion: indexEngineInfo.CurrentIndexVersion,
	}, nil
}

func genIndexParams(indexType, metricType string) (map[string]string, map[string]string) {
	typeParams := make(map[string]string)
	typeParams[common.DimKey] = strconv.Itoa(DefaultDim)

	indexParams := make(map[string]string)
	indexParams[common.IndexTypeKey] = indexType
	indexParams[common.MetricTypeKey] = metricType
	indexParams["index_mode"] = "cpu"
	switch indexType {
	case IndexFaissIDMap: // float vector
	case IndexFaissIVFFlat:
		indexParams["nlist"] = strconv.Itoa(nlist)
	case IndexFaissIVFPQ:
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	case IndexFaissIVFSQ8:
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["nbits"] = strconv.Itoa(nbits)
	case IndexHNSW:
		indexParams["M"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		// indexParams["ef"] = strconv.Itoa(ef)
	case IndexFaissBinIVFFlat: // binary vector
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	case IndexFaissBinIDMap:
		// indexParams[common.DimKey] = strconv.Itoa(defaultDim)
	default:
		panic("")
	}

	return typeParams, indexParams
}

func genStorageConfig() *indexpb.StorageConfig {
	return &indexpb.StorageConfig{
		Address:         paramtable.Get().MinioCfg.Address.GetValue(),
		AccessKeyID:     paramtable.Get().MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKey: paramtable.Get().MinioCfg.SecretAccessKey.GetValue(),
		BucketName:      paramtable.Get().MinioCfg.BucketName.GetValue(),
		RootPath:        paramtable.Get().MinioCfg.RootPath.GetValue(),
		IAMEndpoint:     paramtable.Get().MinioCfg.IAMEndpoint.GetValue(),
		UseSSL:          paramtable.Get().MinioCfg.UseSSL.GetAsBool(),
		SslCACert:       paramtable.Get().MinioCfg.SslCACert.GetValue(),
		UseIAM:          paramtable.Get().MinioCfg.UseIAM.GetAsBool(),
		StorageType:     paramtable.Get().CommonCfg.StorageType.GetValue(),
	}
}

func genSearchRequest(nq int64, indexType string, collection *segcore.CCollection) (*internalpb.SearchRequest, error) {
	placeHolder, err := genPlaceHolderGroup(nq)
	if err != nil {
		return nil, err
	}
	planStr, err2 := genDSLByIndexType(collection.Schema(), indexType)
	if err2 != nil {
		return nil, err2
	}
	var planpb planpb.PlanNode
	// proto.UnmarshalText(planStr, &planpb)
	prototext.Unmarshal([]byte(planStr), &planpb)
	serializedPlan, err3 := proto.Marshal(&planpb)
	if err3 != nil {
		return nil, err3
	}
	return &internalpb.SearchRequest{
		Base:               genCommonMsgBase(commonpb.MsgType_Search, 0),
		CollectionID:       collection.ID(),
		PlaceholderGroup:   placeHolder,
		SerializedExprPlan: serializedPlan,
		DslType:            commonpb.DslType_BoolExprV1,
		Nq:                 nq,
	}, nil
}

func genCommonMsgBase(msgType commonpb.MsgType, targetID int64) *commonpb.MsgBase {
	return &commonpb.MsgBase{
		MsgType:  msgType,
		MsgID:    rand.Int63(),
		TargetID: targetID,
	}
}

func genPlaceHolderGroup(nq int64) ([]byte, error) {
	placeholderValue := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: make([][]byte, 0),
	}
	for i := int64(0); i < nq; i++ {
		vec := make([]float32, DefaultDim)
		for j := 0; j < DefaultDim; j++ {
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

func genDSLByIndexType(schema *schemapb.CollectionSchema, indexType string) (string, error) {
	switch indexType {
	case IndexFaissIDMap: // float vector
		return genBruteForceDSL(schema, defaultTopK, defaultRoundDecimal)
	case IndexHNSW:
		return genHNSWDSL(schema, ef, defaultTopK, defaultRoundDecimal)
	}
	return "", errors.New("Invalid indexType")
}

func genBruteForceDSL(schema *schemapb.CollectionSchema, topK int64, roundDecimal int64) (string, error) {
	var vecFieldName string
	var metricType string
	topKStr := strconv.FormatInt(topK, 10)
	nProbStr := strconv.Itoa(defaultNProb)
	roundDecimalStr := strconv.FormatInt(roundDecimal, 10)
	var fieldID int64
	for _, f := range schema.Fields {
		if f.DataType == schemapb.DataType_FloatVector {
			vecFieldName = f.Name
			fieldID = f.FieldID
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
	return `vector_anns: <
              field_id: ` + fmt.Sprintf("%d", fieldID) + `
              query_info: <
                topk: ` + topKStr + `
                round_decimal: ` + roundDecimalStr + `
                metric_type: "` + metricType + `"
                search_params: "{\"nprobe\": ` + nProbStr + `}"
              >
              placeholder_tag: "$0"
            >`, nil
}

func genHNSWDSL(schema *schemapb.CollectionSchema, ef int, topK int64, roundDecimal int64) (string, error) {
	var vecFieldName string
	var metricType string
	efStr := strconv.Itoa(ef)
	topKStr := strconv.FormatInt(topK, 10)
	roundDecimalStr := strconv.FormatInt(roundDecimal, 10)
	var fieldID int64
	for _, f := range schema.Fields {
		if f.DataType == schemapb.DataType_FloatVector {
			vecFieldName = f.Name
			fieldID = f.FieldID
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
	return `vector_anns: <
              field_id: ` + fmt.Sprintf("%d", fieldID) + `
              query_info: <
                topk: ` + topKStr + `
                round_decimal: ` + roundDecimalStr + `
                metric_type: "` + metricType + `"
                search_params: "{\"ef\": ` + efStr + `}"
              >
              placeholder_tag: "$0"
            >`, nil
}

func GenSearchPlanAndRequests(collection *segcore.CCollection, segments []int64, indexType string, nq int64) (*segcore.SearchRequest, error) {
	iReq, _ := genSearchRequest(nq, indexType, collection)
	queryReq := &querypb.SearchRequest{
		Req:         iReq,
		DmlChannels: []string{fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collection.ID())},
		SegmentIDs:  segments,
		Scope:       querypb.DataScope_Historical,
	}
	return segcore.NewSearchRequest(collection, queryReq, queryReq.Req.GetPlaceholderGroup())
}

// GenSearchPlanAndRequestsWithTopK creates a search request with custom topK.
func GenSearchPlanAndRequestsWithTopK(collection *segcore.CCollection, segments []int64, nq, topK int64) (*segcore.SearchRequest, error) {
	return genSearchRequestWithOptions(collection, segments, nq, topK, nil)
}

// GenSearchPlanAndRequestsWithOutputFields creates a search request with custom topK and output fields.
func GenSearchPlanAndRequestsWithOutputFields(collection *segcore.CCollection, segments []int64, nq, topK int64, outputFieldIDs []int64) (*segcore.SearchRequest, error) {
	return genSearchRequestWithOptions(collection, segments, nq, topK, outputFieldIDs)
}

// GenSearchPlanAndRequestsWithNoMatchPKPredicate creates a search request whose
// PK predicate filters out every row, so segcore SearchResult.total_data_cnt_
// differs from the static segment row count.
func GenSearchPlanAndRequestsWithNoMatchPKPredicate(collection *segcore.CCollection, segments []int64, nq, topK int64) (*segcore.SearchRequest, error) {
	planStr, err := genBruteForceDSL(collection.Schema(), topK, defaultRoundDecimal)
	if err != nil {
		return nil, err
	}
	var planNode planpb.PlanNode
	if err := prototext.Unmarshal([]byte(planStr), &planNode); err != nil {
		return nil, err
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(collection.Schema())
	if err != nil {
		return nil, err
	}
	if pkField.GetDataType() != schemapb.DataType_Int64 {
		return nil, errors.New("no-match PK predicate helper only supports int64 primary key")
	}

	planNode.GetVectorAnns().Predicates = &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{
					FieldId:      pkField.GetFieldID(),
					DataType:     pkField.GetDataType(),
					IsPrimaryKey: pkField.GetIsPrimaryKey(),
					IsAutoID:     pkField.GetAutoID(),
				},
				Values: []*planpb.GenericValue{
					{
						Val: &planpb.GenericValue_Int64Val{
							Int64Val: math.MaxInt64,
						},
					},
				},
			},
		},
	}
	return buildSearchRequestFromPlanNode(collection, segments, nq, &planNode)
}

// GenSearchPlanAndRequestsWithGroupBy creates a search request with group-by enabled.
// groupByFieldID is the scalar field to group by; groupSize is the per-group limit.
func GenSearchPlanAndRequestsWithGroupBy(
	collection *segcore.CCollection,
	segments []int64,
	nq, topK int64,
	groupByFieldID int64,
	groupSize int64,
) (*segcore.SearchRequest, error) {
	planStr, err := genBruteForceDSL(collection.Schema(), topK, defaultRoundDecimal)
	if err != nil {
		return nil, err
	}
	var planNode planpb.PlanNode
	if err := prototext.Unmarshal([]byte(planStr), &planNode); err != nil {
		return nil, err
	}
	// genBruteForceDSL always emits a vector_anns plan, so VectorAnns/QueryInfo
	// are non-nil here. If genBruteForceDSL ever changes to emit a different
	// plan shape this will start panicking — keep them in sync.
	planNode.GetVectorAnns().QueryInfo.GroupByFieldId = groupByFieldID
	planNode.GetVectorAnns().QueryInfo.GroupSize = groupSize
	return buildSearchRequestFromPlanNode(collection, segments, nq, &planNode)
}

func genSearchRequestWithOptions(collection *segcore.CCollection, segments []int64, nq, topK int64, outputFieldIDs []int64) (*segcore.SearchRequest, error) {
	planStr, err := genBruteForceDSL(collection.Schema(), topK, defaultRoundDecimal)
	if err != nil {
		return nil, err
	}
	var planNode planpb.PlanNode
	if err := prototext.Unmarshal([]byte(planStr), &planNode); err != nil {
		return nil, err
	}
	if len(outputFieldIDs) > 0 {
		planNode.OutputFieldIds = outputFieldIDs
	}
	return buildSearchRequestFromPlanNode(collection, segments, nq, &planNode)
}

// buildSearchRequestFromPlanNode marshals the plan and wraps it in the
// internalpb / querypb envelope shared by every test variant.
func buildSearchRequestFromPlanNode(
	collection *segcore.CCollection,
	segments []int64,
	nq int64,
	planNode *planpb.PlanNode,
) (*segcore.SearchRequest, error) {
	placeHolder, err := genPlaceHolderGroup(nq)
	if err != nil {
		return nil, err
	}
	serializedPlan, err := proto.Marshal(planNode)
	if err != nil {
		return nil, err
	}
	iReq := &internalpb.SearchRequest{
		Base:               genCommonMsgBase(commonpb.MsgType_Search, 0),
		CollectionID:       collection.ID(),
		PlaceholderGroup:   placeHolder,
		SerializedExprPlan: serializedPlan,
		DslType:            commonpb.DslType_BoolExprV1,
		Nq:                 nq,
		// Use MaxTimestamp so all inserted rows are visible to the search.
		// Mock-inserted rows have timestamps starting from 1; the default 0
		// would make every row invisible.
		MvccTimestamp: typeutil.MaxTimestamp,
	}
	queryReq := &querypb.SearchRequest{
		Req:         iReq,
		DmlChannels: []string{fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collection.ID())},
		SegmentIDs:  segments,
		Scope:       querypb.DataScope_Historical,
	}
	return segcore.NewSearchRequest(collection, queryReq, queryReq.Req.GetPlaceholderGroup())
}

// GenSearchPlanAndRequestsFilterOnly creates a filter-only search request for
// two-stage search testing. The request has FilterOnly=true so segcore only
// returns valid_count per segment instead of actual search results.
func GenSearchPlanAndRequestsFilterOnly(collection *segcore.CCollection, segments []int64, nq, topK int64) (*segcore.SearchRequest, error) {
	_, searchReq, err := GenFilterOnlySearchRequests(collection, segments, nq, topK, collection.ID())
	return searchReq, err
}

// buildQueryRequest assembles a querypb.SearchRequest from a brute-force DSL
// plan. collectionID overrides the proto's CollectionID because
// CCollection.ID() may be 0 when the collection is created through the
// segment manager. mutatePlan is an optional hook for tests that need to tweak
// QueryInfo (e.g. set group_by, global_refine ratios) before serialization.
func buildQueryRequest(
	collection *segcore.CCollection,
	segments []int64,
	nq, topK, collectionID int64,
	filterOnly bool,
	mutatePlan func(*planpb.PlanNode),
) (*querypb.SearchRequest, error) {
	planStr, err := genBruteForceDSL(collection.Schema(), topK, defaultRoundDecimal)
	if err != nil {
		return nil, err
	}
	var planNode planpb.PlanNode
	if err := prototext.Unmarshal([]byte(planStr), &planNode); err != nil {
		return nil, err
	}
	if mutatePlan != nil {
		mutatePlan(&planNode)
	}
	placeHolder, err := genPlaceHolderGroup(nq)
	if err != nil {
		return nil, err
	}
	serializedPlan, err := proto.Marshal(&planNode)
	if err != nil {
		return nil, err
	}
	return &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			Base:               genCommonMsgBase(commonpb.MsgType_Search, 0),
			CollectionID:       collectionID,
			PlaceholderGroup:   placeHolder,
			SerializedExprPlan: serializedPlan,
			DslType:            commonpb.DslType_BoolExprV1,
			Nq:                 nq,
			Topk:               topK,
			MvccTimestamp:      typeutil.MaxTimestamp,
		},
		DmlChannels: []string{fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)},
		SegmentIDs:  segments,
		Scope:       querypb.DataScope_Historical,
		FilterOnly:  filterOnly,
	}, nil
}

// GenQueryRequestWithGlobalRefine builds a querypb.SearchRequest whose plan
// has QueryInfo.SearchTopkRatio / RefineTopkRatio set. Ratios must both be
// >= 1.0 (or both == 0 for disabled); anything between (0, 1) is rejected by
// PlanProto.cpp.
func GenQueryRequestWithGlobalRefine(
	collection *segcore.CCollection,
	segments []int64,
	nq, topK, collectionID int64,
	searchTopkRatio, refineTopkRatio float32,
) (*querypb.SearchRequest, error) {
	return buildQueryRequest(collection, segments, nq, topK, collectionID, false,
		func(plan *planpb.PlanNode) {
			plan.GetVectorAnns().QueryInfo.SearchTopkRatio = searchTopkRatio
			plan.GetVectorAnns().QueryInfo.RefineTopkRatio = refineTopkRatio
		})
}

// GenQueryRequest builds a querypb.SearchRequest for callers that construct
// their own SearchTask and want Execute() to build the segcore.SearchRequest
// itself.
func GenQueryRequest(collection *segcore.CCollection, segments []int64, nq, topK, collectionID int64) (*querypb.SearchRequest, error) {
	return buildQueryRequest(collection, segments, nq, topK, collectionID, false, nil)
}

// GenFilterOnlySearchRequests creates both the querypb.SearchRequest and
// segcore.SearchRequest with FilterOnly=true, for tests that need both
// (e.g., a SearchTask driver test that also keeps a segcore reference).
func GenFilterOnlySearchRequests(collection *segcore.CCollection, segments []int64, nq, topK, collectionID int64) (*querypb.SearchRequest, *segcore.SearchRequest, error) {
	queryReq, err := buildQueryRequest(collection, segments, nq, topK, collectionID, true, nil)
	if err != nil {
		return nil, nil, err
	}
	searchReq, err := segcore.NewSearchRequest(collection, queryReq, queryReq.Req.GetPlaceholderGroup())
	if err != nil {
		return nil, nil, err
	}
	return queryReq, searchReq, nil
}

func GenInsertMsg(collection *segcore.CCollection, partitionID, segment int64, numRows int) (*msgstream.InsertMsg, error) {
	fieldsData := make([]*schemapb.FieldData, 0)

	for _, f := range collection.Schema().Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleBoolField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Int8:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleInt8Field.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Int16:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleInt16Field.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Int32:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleInt32Field.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Int64:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleInt64Field.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Float:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleFloatField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Double:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleDoubleField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_VarChar:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleVarCharField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Array:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleArrayField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_JSON:
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(f.DataType, simpleJSONField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_FloatVector:
			dim := SimpleFloatVecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, testutils.GenerateVectorFieldDataWithID(f.DataType, f.Name, f.FieldID, numRows, dim))
		case schemapb.DataType_BinaryVector:
			dim := simpleBinVecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, testutils.GenerateVectorFieldDataWithID(f.DataType, f.Name, f.FieldID, numRows, dim))
		case schemapb.DataType_Float16Vector:
			dim := simpleFloat16VecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, testutils.GenerateVectorFieldDataWithID(f.DataType, f.Name, f.FieldID, numRows, dim))
		case schemapb.DataType_BFloat16Vector:
			dim := simpleBFloat16VecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, testutils.GenerateVectorFieldDataWithID(f.DataType, f.Name, f.FieldID, numRows, dim))
		case schemapb.DataType_SparseFloatVector:
			fieldsData = append(fieldsData, testutils.GenerateVectorFieldDataWithID(f.DataType, f.Name, f.FieldID, numRows, 0))
		default:
			err := errors.New("data type not supported")
			return nil, err
		}
	}

	return &msgstream.InsertMsg{
		BaseMsg: genMsgStreamBaseMsg(),
		InsertRequest: &msgpb.InsertRequest{
			Base:           genCommonMsgBase(commonpb.MsgType_Insert, 0),
			CollectionName: "test-collection",
			PartitionName:  "test-partition",
			CollectionID:   collection.ID(),
			PartitionID:    partitionID,
			SegmentID:      segment,
			ShardName:      fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collection.ID()),
			Timestamps:     genSimpleTimestampFieldData(numRows),
			RowIDs:         genSimpleRowIDField(numRows),
			FieldsData:     fieldsData,
			NumRows:        uint64(numRows),
			Version:        msgpb.InsertDataVersion_ColumnBased,
		},
	}, nil
}

func genMsgStreamBaseMsg() msgstream.BaseMsg {
	return msgstream.BaseMsg{
		Ctx:            context.Background(),
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
		MsgPosition: &msgpb.MsgPosition{
			ChannelName: "",
			MsgID:       []byte{},
			MsgGroup:    "",
			Timestamp:   10,
		},
	}
}

func genSimpleTimestampFieldData(numRows int) []uint64 {
	times := make([]uint64, numRows)
	for i := 0; i < numRows; i++ {
		times[i] = uint64(i)
	}
	// timestamp 0 is not allowed
	times[0] = 1
	return times
}

func genSimpleRowIDField(numRows int) []int64 {
	ids := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		ids[i] = int64(i)
	}
	return ids
}

func GenSimpleRetrievePlan(collection *segcore.CCollection) (*segcore.RetrievePlan, error) {
	timestamp := storage.Timestamp(1000)
	planBytes, err := genSimpleRetrievePlanExpr(collection.Schema())
	if err != nil {
		return nil, err
	}

	plan, err2 := segcore.NewRetrievePlan(collection, planBytes, timestamp, 100, 0, 0, 0)
	return plan, err2
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

func GenFieldData(fieldName string, fieldID int64, fieldType schemapb.DataType, fieldValue interface{}, dim int64) *schemapb.FieldData {
	if fieldType < 100 {
		return testutils.GenerateScalarFieldDataWithValue(fieldType, fieldName, fieldID, fieldValue)
	}
	return testutils.GenerateVectorFieldDataWithValue(fieldType, fieldName, fieldID, fieldValue, int(dim))
}

func GenSearchResultData(nq int64, topk int64, ids []int64, scores []float32, topks []int64) *schemapb.SearchResultData {
	return &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       topk,
		FieldsData: nil,
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		},
		Topks: topks,
	}
}

// CheckSearchResult validates a SearchResult from the Go reduce pipeline.
// It unmarshals the SlicedBlob and verifies that NumQueries, TopK, IDs,
// Scores, and Topks are self-consistent. This replaces the old
// CheckSearchResult that validated the C++ reduce output.
func CheckSearchResult(t *testing.T, result *internalpb.SearchResults, expectedNQ, expectedTopK int64) {
	t.Helper()
	require.NotNil(t, result, "SearchResult must not be nil")

	assert.Equal(t, expectedNQ, result.NumQueries, "NumQueries mismatch")
	assert.Equal(t, expectedTopK, result.TopK, "TopK mismatch")
	require.NotEmpty(t, result.SlicedBlob, "SlicedBlob must not be empty")

	slice := &schemapb.SearchResultData{}
	require.NoError(t, proto.Unmarshal(result.SlicedBlob, slice), "failed to unmarshal SlicedBlob")

	assert.Equal(t, expectedNQ, slice.NumQueries, "slice NumQueries mismatch")
	assert.Equal(t, expectedTopK, slice.TopK, "slice TopK mismatch")

	require.Len(t, slice.Topks, int(expectedNQ), "Topks length must equal NQ")

	var totalRows int64
	for i, perNQ := range slice.Topks {
		assert.LessOrEqual(t, perNQ, expectedTopK,
			"NQ %d: topk %d exceeds expected topK %d", i, perNQ, expectedTopK)
		totalRows += perNQ
	}

	assert.Len(t, slice.Scores, int(totalRows), "Scores length must equal sum(Topks)")

	require.NotNil(t, slice.Ids, "IDs must not be nil")
	require.NotNil(t, slice.Ids.IdField, "IDs.IdField must not be nil")
	switch ids := slice.Ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		assert.Len(t, ids.IntId.Data, int(totalRows), "int64 IDs length must equal sum(Topks)")
	case *schemapb.IDs_StrId:
		assert.Len(t, ids.StrId.Data, int(totalRows), "string IDs length must equal sum(Topks)")
	default:
		t.Fatalf("unexpected IDs type: %T", slice.Ids.IdField)
	}
}
