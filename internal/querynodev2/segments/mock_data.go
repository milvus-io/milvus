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

package segments

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
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	IndexFaissIDMap      = "FLAT"
	IndexFaissIVFFlat    = "IVF_FLAT"
	IndexFaissIVFPQ      = "IVF_PQ"
	IndexFaissIVFSQ8     = "IVF_SQ8"
	IndexFaissBinIDMap   = "BIN_FLAT"
	IndexFaissBinIVFFlat = "BIN_IVF_FLAT"
	IndexHNSW            = "HNSW"

	L2       = "L2"
	IP       = "IP"
	hamming  = "HAMMING"
	Jaccard  = "JACCARD"
	tanimoto = "TANIMOTO"

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
	defaultDim        = 128
	defaultMetricType = "L2"

	dimKey = common.DimKey

	defaultLocalStorage = "/tmp/milvus_test/querynode"
)

// ---------- unittest util functions ----------
// gen collection schema for
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

var simpleJSONField = constFieldParam{
	id:        109,
	dataType:  schemapb.DataType_JSON,
	fieldName: "jsonField",
}

var simpleArrayField = constFieldParam{
	id:        110,
	dataType:  schemapb.DataType_Array,
	fieldName: "arrayField",
}

var simpleVarCharField = constFieldParam{
	id:        111,
	dataType:  schemapb.DataType_VarChar,
	fieldName: "varCharField",
}

var rowIDField = constFieldParam{
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
		ElementType:  schemapb.DataType_Int32,
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

	if param.dataType == schemapb.DataType_VarChar {
		field.TypeParams = []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "12"},
		}
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

func GenTestCollectionSchema(collectionName string, pkType schemapb.DataType) *schemapb.CollectionSchema {
	fieldBool := genConstantFieldSchema(simpleBoolField)
	fieldInt8 := genConstantFieldSchema(simpleInt8Field)
	fieldInt16 := genConstantFieldSchema(simpleInt16Field)
	fieldInt32 := genConstantFieldSchema(simpleInt32Field)
	fieldFloat := genConstantFieldSchema(simpleFloatField)
	fieldDouble := genConstantFieldSchema(simpleDoubleField)
	// fieldArray := genConstantFieldSchema(simpleArrayField)
	fieldJSON := genConstantFieldSchema(simpleJSONField)
	floatVecFieldSchema := genVectorFieldSchema(simpleFloatVecField)
	binVecFieldSchema := genVectorFieldSchema(simpleBinVecField)
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
			// fieldArray,
			fieldJSON,
			floatVecFieldSchema,
			binVecFieldSchema,
			pkFieldSchema,
		},
	}

	for i, field := range schema.GetFields() {
		field.FieldID = 100 + int64(i)
	}
	return &schema
}

func GenTestIndexMeta(collectionID int64, schema *schemapb.CollectionSchema) *segcorepb.CollectionIndexMeta {
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	maxIndexRecordPerSegment := int64(0)
	if err != nil || sizePerRecord == 0 {
		log.Warn("failed to transfer segment size to collection, because failed to estimate size per record", zap.Error(err))
	} else {
		threshold := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024
		proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
		maxIndexRecordPerSegment = int64(threshold * proportion / float64(sizePerRecord))
	}

	fieldIndexMetas := make([]*segcorepb.FieldIndexMeta, 0)
	fieldIndexMetas = append(fieldIndexMetas, &segcorepb.FieldIndexMeta{
		CollectionID: collectionID,
		FieldID:      simpleFloatVecField.id,
		IndexName:    "querynode-test",
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   dimKey,
				Value: strconv.Itoa(simpleFloatVecField.dim),
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   metricTypeKey,
				Value: simpleFloatVecField.metricType,
			},
			{
				Key:   common.IndexTypeKey,
				Value: IndexFaissIVFFlat,
			},
			{
				Key:   "nlist",
				Value: "128",
			},
		},
		IsAutoIndex: false,
		UserIndexParams: []*commonpb.KeyValuePair{
			{},
		},
	})

	indexMeta := segcorepb.CollectionIndexMeta{
		MaxIndexRowCount: maxIndexRecordPerSegment,
		IndexMetas:       fieldIndexMetas,
	}

	return &indexMeta
}

// ---------- unittest util functions ----------
// gen field data
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
		ret = append(ret, rand.Int31())
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
		ret = append(ret, strconv.Itoa(rand.Int()))
	}
	return ret
}
func generateArrayArray(numRows int) []*schemapb.ScalarField {
	ret := make([]*schemapb.ScalarField, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: generateInt32Array(10),
				},
			},
		})
	}
	return ret
}
func generateJSONArray(numRows int) [][]byte {
	ret := make([][]byte, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, []byte(fmt.Sprintf(`{"key":%d}`, i+1)))
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

func GenTestScalarFieldData(dType schemapb.DataType, fieldName string, fieldID int64, numRows int) *schemapb.FieldData {
	ret := &schemapb.FieldData{
		Type:      dType,
		FieldName: fieldName,
		Field:     nil,
	}

	switch dType {
	case schemapb.DataType_Bool:
		ret.FieldId = fieldID
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
		ret.FieldId = fieldID
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
		ret.FieldId = fieldID
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
		ret.FieldId = fieldID
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
		ret.FieldId = fieldID
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
		ret.FieldId = fieldID
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
		ret.FieldId = fieldID
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
		ret.FieldId = fieldID
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: generateStringArray(numRows),
					},
				},
			},
		}

	case schemapb.DataType_Array:
		ret.FieldId = fieldID
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data: generateArrayArray(numRows),
					},
				},
			},
		}

	case schemapb.DataType_JSON:
		ret.FieldId = fieldID
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: generateJSONArray(numRows),
					}},
			}}

	default:
		panic("data type not supported")
	}

	return ret
}

func GenTestVectorFiledData(dType schemapb.DataType, fieldName string, fieldID int64, numRows int, dim int) *schemapb.FieldData {
	ret := &schemapb.FieldData{
		Type:      dType,
		FieldName: fieldName,
		Field:     nil,
	}
	switch dType {
	case schemapb.DataType_BinaryVector:
		ret.FieldId = fieldID
		ret.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: generateBinaryVectors(numRows, dim),
				},
			},
		}
	case schemapb.DataType_FloatVector:
		ret.FieldId = fieldID
		ret.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: generateFloatVectors(numRows, dim),
					},
				},
			},
		}
	default:
		panic("data type not supported")
	}
	return ret
}

func SaveBinLog(ctx context.Context,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	msgLength int,
	schema *schemapb.CollectionSchema,
	chunkManager storage.ChunkManager,
) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
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
		//key := path.Join(defaultLocalStorage, "insert-log", k)
		key := path.Join(paramtable.Get().MinioCfg.RootPath.GetValue(), "insert-log", k)
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

		k := JoinIDPath(collectionID, partitionID, segmentID, fieldID)
		//key := path.Join(defaultLocalStorage, "stats-log", k)
		key := path.Join(paramtable.Get().MinioCfg.RootPath.GetValue(), "stats-log", k)
		kvs[key] = blob.Value[:]
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
	schema *schemapb.CollectionSchema) ([]*storage.Blob, []*storage.Blob, error) {
	tmpSchema := &schemapb.CollectionSchema{
		Name:   schema.Name,
		AutoID: schema.AutoID,
		Fields: []*schemapb.FieldSchema{genConstantFieldSchema(rowIDField), genConstantFieldSchema(timestampField)},
	}
	tmpSchema.Fields = append(tmpSchema.Fields, schema.Fields...)
	collMeta := genCollectionMeta(collectionID, partitionID, tmpSchema)
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

	// set data for rowID field
	insertData.Data[rowIDFieldID] = &storage.Int64FieldData{
		Data: generateInt64Array(msgLength),
	}
	// set data for ts field
	insertData.Data[timestampFieldID] = &storage.Int64FieldData{
		Data: genTimestampFieldData(msgLength),
	}

	for _, f := range schema.Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			insertData.Data[f.FieldID] = &storage.BoolFieldData{
				Data: generateBoolArray(msgLength),
			}
		case schemapb.DataType_Int8:
			insertData.Data[f.FieldID] = &storage.Int8FieldData{
				Data: generateInt8Array(msgLength),
			}
		case schemapb.DataType_Int16:
			insertData.Data[f.FieldID] = &storage.Int16FieldData{
				Data: generateInt16Array(msgLength),
			}
		case schemapb.DataType_Int32:
			insertData.Data[f.FieldID] = &storage.Int32FieldData{
				Data: generateInt32Array(msgLength),
			}
		case schemapb.DataType_Int64:
			insertData.Data[f.FieldID] = &storage.Int64FieldData{
				Data: generateInt64Array(msgLength),
			}
		case schemapb.DataType_Float:
			insertData.Data[f.FieldID] = &storage.FloatFieldData{
				Data: generateFloat32Array(msgLength),
			}
		case schemapb.DataType_Double:
			insertData.Data[f.FieldID] = &storage.DoubleFieldData{
				Data: generateFloat64Array(msgLength),
			}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			insertData.Data[f.FieldID] = &storage.StringFieldData{
				Data: generateStringArray(msgLength),
			}
		case schemapb.DataType_Array:
			insertData.Data[f.FieldID] = &storage.ArrayFieldData{
				Data: generateArrayArray(msgLength),
			}
		case schemapb.DataType_JSON:
			insertData.Data[f.FieldID] = &storage.JSONFieldData{
				Data: generateJSONArray(msgLength),
			}
		case schemapb.DataType_FloatVector:
			dim := simpleFloatVecField.dim // if no dim specified, use simpleFloatVecField's dim
			insertData.Data[f.FieldID] = &storage.FloatVectorFieldData{
				Data: generateFloatVectors(msgLength, dim),
				Dim:  dim,
			}
		case schemapb.DataType_BinaryVector:
			dim := simpleBinVecField.dim
			insertData.Data[f.FieldID] = &storage.BinaryVectorFieldData{
				Data: generateBinaryVectors(msgLength, dim),
				Dim:  dim,
			}
		default:
			err := errors.New("data type not supported")
			return nil, err
		}
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

	binlogWriter := storage.NewDeleteBinlogWriter(schemapb.DataType_String, collectionID, partitionID, segmentID)
	eventWriter, _ := binlogWriter.NextDeleteEventWriter()
	dData := &storage.DeleteData{
		Pks:      []storage.PrimaryKey{&storage.Int64PrimaryKey{Value: 1}, &storage.Int64PrimaryKey{Value: 2}},
		Tss:      []typeutil.Timestamp{100, 200},
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
	pkFieldID := int64(106)
	fieldBinlog := make([]*datapb.FieldBinlog, 0)
	log.Debug("[query node unittest] save delta log", zap.Int64("fieldID", pkFieldID))
	key := JoinIDPath(collectionID, partitionID, segmentID, pkFieldID)
	//keyPath := path.Join(defaultLocalStorage, "delta-log", key)
	keyPath := path.Join(paramtable.Get().MinioCfg.RootPath.GetValue(), "delta-log", key)
	kvs[keyPath] = blob.Value[:]
	fieldBinlog = append(fieldBinlog, &datapb.FieldBinlog{
		FieldID: pkFieldID,
		Binlogs: []*datapb.Binlog{{LogPath: keyPath}},
	})
	log.Debug("[query node unittest] save delta log file to MinIO/S3")

	return fieldBinlog, cm.MultiWrite(context.Background(), kvs)
}

func GenAndSaveIndex(collectionID, partitionID, segmentID, fieldID int64, msgLength int, indexType, metricType string, cm storage.ChunkManager) (*querypb.FieldIndexInfo, error) {
	typeParams, indexParams := genIndexParams(indexType, metricType)

	index, err := indexcgowrapper.NewCgoIndex(schemapb.DataType_FloatVector, typeParams, indexParams)
	if err != nil {
		return nil, err
	}
	defer index.Delete()

	err = index.Build(indexcgowrapper.GenFloatVecDataset(generateFloatVectors(msgLength, defaultDim)))
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
		simpleFloatVecField.id,
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
		//indexPath := filepath.Join(defaultLocalStorage, strconv.Itoa(int(segmentID)), index.Key)
		indexPath := filepath.Join(paramtable.Get().MinioCfg.RootPath.GetValue(), "index_files",
			strconv.Itoa(int(segmentID)), index.Key)
		indexPaths = append(indexPaths, indexPath)
		err := cm.Write(context.Background(), indexPath, index.Value)
		if err != nil {
			return nil, err
		}
	}

	return &querypb.FieldIndexInfo{
		FieldID:        fieldID,
		EnableIndex:    true,
		IndexName:      "querynode-test",
		IndexParams:    funcutil.Map2KeyValuePair(indexParams),
		IndexFilePaths: indexPaths,
	}, nil
}

func genIndexParams(indexType, metricType string) (map[string]string, map[string]string) {
	typeParams := make(map[string]string)
	typeParams[common.DimKey] = strconv.Itoa(defaultDim)

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
		//indexParams["ef"] = strconv.Itoa(ef)
	} else if indexType == IndexFaissBinIVFFlat { // binary vector
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	} else if indexType == IndexFaissBinIDMap {
		//indexParams[common.DimKey] = strconv.Itoa(defaultDim)
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
		UseIAM:          paramtable.Get().MinioCfg.UseIAM.GetAsBool(),
		StorageType:     paramtable.Get().CommonCfg.StorageType.GetValue(),
	}
}

func genSearchRequest(nq int64, indexType string, collection *Collection) (*internalpb.SearchRequest, error) {
	placeHolder, err := genPlaceHolderGroup(nq)
	if err != nil {
		return nil, err
	}
	planStr, err2 := genDSLByIndexType(collection.Schema(), indexType)
	if err2 != nil {
		return nil, err2
	}
	var planpb planpb.PlanNode
	proto.UnmarshalText(planStr, &planpb)
	serializedPlan, err3 := proto.Marshal(&planpb)
	if err3 != nil {
		return nil, err3
	}
	return &internalpb.SearchRequest{
		Base:               genCommonMsgBase(commonpb.MsgType_Search, 0),
		CollectionID:       collection.ID(),
		PartitionIDs:       collection.GetPartitions(),
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

func checkSearchResult(nq int64, plan *SearchPlan, searchResult *SearchResult) error {
	searchResults := make([]*SearchResult, 0)
	searchResults = append(searchResults, searchResult)

	topK := plan.getTopK()
	sliceNQs := []int64{nq / 5, nq / 5, nq / 5, nq / 5, nq / 5}
	sliceTopKs := []int64{topK, topK / 2, topK, topK, topK / 2}
	sInfo := ParseSliceInfo(sliceNQs, sliceTopKs, nq)

	res, err := ReduceSearchResultsAndFillData(plan, searchResults, 1, sInfo.SliceNQs, sInfo.SliceTopKs)
	if err != nil {
		return err
	}

	for i := 0; i < len(sInfo.SliceNQs); i++ {
		blob, err := GetSearchResultDataBlob(res, i)
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
		if len(result.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data) != 0 {
			return fmt.Errorf("unexpected Ids when checkSearchResult")
		}
		if len(result.Scores) != 0 {
			return fmt.Errorf("unexpected Scores when checkSearchResult")
		}
	}

	DeleteSearchResults(searchResults)
	DeleteSearchResultDataBlobs(res)
	return nil
}

func genSearchPlanAndRequests(collection *Collection, segments []int64, indexType string, nq int64) (*SearchRequest, error) {

	iReq, _ := genSearchRequest(nq, indexType, collection)
	queryReq := &querypb.SearchRequest{
		Req:             iReq,
		DmlChannels:     []string{"dml"},
		SegmentIDs:      segments,
		FromShardLeader: true,
		Scope:           querypb.DataScope_Historical,
	}
	return NewSearchRequest(collection, queryReq, queryReq.Req.GetPlaceholderGroup())
}

func genInsertMsg(collection *Collection, partitionID, segment int64, numRows int) (*msgstream.InsertMsg, error) {
	fieldsData := make([]*schemapb.FieldData, 0)

	for _, f := range collection.Schema().Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleBoolField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Int8:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleInt8Field.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Int16:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleInt16Field.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Int32:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleInt32Field.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Int64:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleInt64Field.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Float:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleFloatField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Double:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleDoubleField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_VarChar:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleVarCharField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_Array:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleArrayField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_JSON:
			fieldsData = append(fieldsData, GenTestScalarFieldData(f.DataType, simpleJSONField.fieldName, f.GetFieldID(), numRows))
		case schemapb.DataType_FloatVector:
			dim := simpleFloatVecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, GenTestVectorFiledData(f.DataType, f.Name, f.FieldID, numRows, dim))
		case schemapb.DataType_BinaryVector:
			dim := simpleBinVecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, GenTestVectorFiledData(f.DataType, f.Name, f.FieldID, numRows, dim))
		default:
			err := errors.New("data type not supported")
			return nil, err
		}
	}

	return &msgstream.InsertMsg{
		BaseMsg: genMsgStreamBaseMsg(),
		InsertRequest: msgpb.InsertRequest{
			Base:           genCommonMsgBase(commonpb.MsgType_Insert, 0),
			CollectionName: "test-collection",
			PartitionName:  "test-partition",
			CollectionID:   collection.ID(),
			PartitionID:    partitionID,
			SegmentID:      segment,
			ShardName:      "dml",
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

func genSimpleRetrievePlan(collection *Collection) (*RetrievePlan, error) {
	timestamp := storage.Timestamp(1000)
	planBytes, err := genSimpleRetrievePlanExpr(collection.schema)
	if err != nil {
		return nil, err
	}

	plan, err2 := NewRetrievePlan(collection, planBytes, timestamp, 100)
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

func genSearchResultData(nq int64, topk int64, ids []int64, scores []float32, topks []int64) *schemapb.SearchResultData {
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
