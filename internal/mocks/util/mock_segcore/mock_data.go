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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
				}
			}
		case schemapb.DataType_BinaryVector:
			{
				index.IndexParams = []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: metric.JACCARD},
					{Key: common.IndexTypeKey, Value: IndexFaissBinIVFFlat},
					{Key: "nlist", Value: "128"},
				}
			}
		case schemapb.DataType_SparseFloatVector:
			{
				index.IndexParams = []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: metric.IP},
					{Key: common.IndexTypeKey, Value: IndexSparseWand},
					{Key: "M", Value: "16"},
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
		storage.RootPath(rootPath),
		storage.Address(params.MinioCfg.Address.GetValue()),
		storage.AccessKeyID(params.MinioCfg.AccessKeyID.GetValue()),
		storage.SecretAccessKeyID(params.MinioCfg.SecretAccessKey.GetValue()),
		storage.UseSSL(params.MinioCfg.UseSSL.GetAsBool()),
		storage.SslCACert(params.MinioCfg.SslCACert.GetValue()),
		storage.BucketName(params.MinioCfg.BucketName.GetValue()),
		storage.UseIAM(params.MinioCfg.UseIAM.GetAsBool()),
		storage.CloudProvider(params.MinioCfg.CloudProvider.GetValue()),
		storage.IAMEndpoint(params.MinioCfg.IAMEndpoint.GetValue()),
		storage.CreateBucket(true))
}

func SaveBinLog(ctx context.Context,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	msgLength int,
	schema *schemapb.CollectionSchema,
	chunkManager storage.ChunkManager,
) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	log := log.Ctx(ctx)
	binLogs, statsLogs, err := genStorageBlob(collectionID,
		partitionID,
		segmentID,
		msgLength,
		schema)
	if err != nil {
		log.Warn("getStorageBlob return error", zap.Error(err))
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

func genStorageBlob(collectionID int64,
	partitionID int64,
	segmentID int64,
	msgLength int,
	schema *schemapb.CollectionSchema,
) ([]*storage.Blob, []*storage.Blob, error) {
	collMeta := genCollectionMeta(collectionID, partitionID, schema)
	inCodec := storage.NewInsertCodecWithSchema(collMeta)
	insertData, err := genInsertData(msgLength, schema)
	if err != nil {
		return nil, nil, err
	}

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

func genInsertData(msgLength int, schema *schemapb.CollectionSchema) (*storage.InsertData, error) {
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
	if indexType == IndexFaissIDMap { // float vector
	} else if indexType == IndexFaissIVFFlat {
		indexParams["nlist"] = strconv.Itoa(nlist)
	} else if indexType == IndexFaissIVFPQ {
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	} else if indexType == IndexFaissIVFSQ8 {
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["nbits"] = strconv.Itoa(nbits)
	} else if indexType == IndexHNSW {
		indexParams["M"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		// indexParams["ef"] = strconv.Itoa(ef)
	} else if indexType == IndexFaissBinIVFFlat { // binary vector
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	} else if indexType == IndexFaissBinIDMap {
		// indexParams[common.DimKey] = strconv.Itoa(defaultDim)
	} else {
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
	if indexType == IndexFaissIDMap { // float vector
		return genBruteForceDSL(schema, defaultTopK, defaultRoundDecimal)
	} else if indexType == IndexHNSW {
		return genHNSWDSL(schema, ef, defaultTopK, defaultRoundDecimal)
	}
	return "", fmt.Errorf("Invalid indexType")
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

func CheckSearchResult(ctx context.Context, nq int64, plan *segcore.SearchPlan, searchResult *segcore.SearchResult) error {
	searchResults := make([]*segcore.SearchResult, 0)
	searchResults = append(searchResults, searchResult)

	topK := plan.GetTopK()
	sliceNQs := []int64{nq / 5, nq / 5, nq / 5, nq / 5, nq / 5}
	sliceTopKs := []int64{topK, topK / 2, topK, topK, topK / 2}
	sInfo := segcore.ParseSliceInfo(sliceNQs, sliceTopKs, nq)

	res, err := segcore.ReduceSearchResultsAndFillData(ctx, plan, searchResults, 1, sInfo.SliceNQs, sInfo.SliceTopKs)
	if err != nil {
		return err
	}

	for i := 0; i < len(sInfo.SliceNQs); i++ {
		blob, err := segcore.GetSearchResultDataBlob(ctx, res, i)
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
		if result.NumQueries != sInfo.SliceNQs[i] {
			return fmt.Errorf("unexpected nq when checkSearchResult")
		}
		// search empty segment, return empty result.IDs
		if len(result.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data) <= 0 {
			return fmt.Errorf("unexpected Ids when checkSearchResult")
		}
		if len(result.Scores) <= 0 {
			return fmt.Errorf("unexpected Scores when checkSearchResult")
		}
	}

	for _, searchResult := range searchResults {
		searchResult.Release()
	}
	segcore.DeleteSearchResultDataBlobs(res)
	return nil
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

	plan, err2 := segcore.NewRetrievePlan(collection, planBytes, timestamp, 100, 0)
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
