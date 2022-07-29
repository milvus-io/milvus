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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mq/msgstream"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"
)

// TODO(dragondriver): add more test cases

const (
	maxTestStringLen     = 100
	testBoolField        = "bool"
	testInt32Field       = "int32"
	testInt64Field       = "int64"
	testUInt32Field      = "uint32"
	testUInt64Field      = "uint64"
	testFloatField       = "float"
	testDoubleField      = "double"
	testVarCharField     = "varChar"
	testFloatVecField    = "fvec"
	testBinaryVecField   = "bvec"
	testVecDim           = 128
	testMaxVarCharLength = 100
)

func constructCollectionSchema(
	int64Field, floatVecField string,
	dim int,
	collectionName string,
) *schemapb.CollectionSchema {

	pk := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	return &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			pk,
			fVec,
		},
	}
}

func constructCollectionSchemaByDataType(collectionName string, fieldName2DataType map[string]schemapb.DataType, primaryFieldName string, autoID bool) *schemapb.CollectionSchema {
	fieldsSchema := make([]*schemapb.FieldSchema, 0)

	for fieldName, dataType := range fieldName2DataType {
		fieldSchema := &schemapb.FieldSchema{
			Name:     fieldName,
			DataType: dataType,
		}
		if dataType == schemapb.DataType_FloatVector || dataType == schemapb.DataType_BinaryVector {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(testVecDim),
				},
			}
		}
		if dataType == schemapb.DataType_VarChar {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   "max_length",
					Value: strconv.Itoa(testMaxVarCharLength),
				},
			}
		}
		if fieldName == primaryFieldName {
			fieldSchema.IsPrimaryKey = true
			fieldSchema.AutoID = autoID
		}

		fieldsSchema = append(fieldsSchema, fieldSchema)
	}

	return &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: fieldsSchema,
	}
}

func constructCollectionSchemaWithAllType(
	boolField, int32Field, int64Field, floatField, doubleField, uint32Field, uint64Field string,
	floatVecField, binaryVecField string,
	dim int,
	collectionName string,
) *schemapb.CollectionSchema {

	b := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         boolField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Bool,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	i32 := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int32Field,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int32,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	i64 := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	u32 := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         uint32Field,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_UInt32,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	u64 := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         uint64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_UInt64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	f := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Float,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	d := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         doubleField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Double,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	bVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         binaryVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}

	if enableMultipleVectorFields {
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				b,
				i32,
				i64,
				u32,
				u64,
				f,
				d,
				fVec,
				bVec,
			},
		}
	}

	return &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			b,
			i32,
			i64,
			u32,
			u64,
			f,
			d,
			fVec,
			// bVec,
		},
	}
}

func constructPlaceholderGroup(
	nq, dim int,
) *commonpb.PlaceholderGroup {
	values := make([][]byte, 0, nq)
	for i := 0; i < nq; i++ {
		bs := make([]byte, 0, dim*4)
		for j := 0; j < dim; j++ {
			var buffer bytes.Buffer
			f := rand.Float32()
			err := binary.Write(&buffer, common.Endian, f)
			if err != nil {
				panic(err)
			}
			bs = append(bs, buffer.Bytes()...)
		}
		values = append(values, bs)
	}

	return &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   commonpb.PlaceholderType_FloatVector,
				Values: values,
			},
		},
	}
}

func constructSearchRequest(
	dbName, collectionName string,
	expr string,
	floatVecField string,
	nq, dim, nprobe, topk, roundDecimal int,
) *milvuspb.SearchRequest {
	params := make(map[string]string)
	params["nprobe"] = strconv.Itoa(nprobe)
	b, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	plg := constructPlaceholderGroup(nq, dim)
	plgBs, err := proto.Marshal(plg)
	if err != nil {
		panic(err)
	}

	return &milvuspb.SearchRequest{
		Base:             nil,
		DbName:           dbName,
		CollectionName:   collectionName,
		PartitionNames:   nil,
		Dsl:              expr,
		PlaceholderGroup: plgBs,
		DslType:          commonpb.DslType_BoolExprV1,
		OutputFields:     nil,
		SearchParams: []*commonpb.KeyValuePair{
			{
				Key:   MetricTypeKey,
				Value: distance.L2,
			},
			{
				Key:   SearchParamsKey,
				Value: string(b),
			},
			{
				Key:   AnnsFieldKey,
				Value: floatVecField,
			},
			{
				Key:   TopKKey,
				Value: strconv.Itoa(topk),
			},
			{
				Key:   RoundDecimalKey,
				Value: strconv.Itoa(roundDecimal),
			},
		},
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
	}
}

func TestTranslateOutputFields(t *testing.T) {
	const (
		idFieldName           = "id"
		tsFieldName           = "timestamp"
		floatVectorFieldName  = "float_vector"
		binaryVectorFieldName = "binary_vector"
	)
	var outputFields []string
	var err error

	schema := &schemapb.CollectionSchema{
		Name:        "TestTranslateOutputFields",
		Description: "TestTranslateOutputFields",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{Name: idFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: tsFieldName, DataType: schemapb.DataType_Int64},
			{Name: floatVectorFieldName, DataType: schemapb.DataType_FloatVector},
			{Name: binaryVectorFieldName, DataType: schemapb.DataType_BinaryVector},
		},
	}

	outputFields, err = translateOutputFields([]string{}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{" * "}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{" % "}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", "%"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", tsFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", idFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	//=========================================================================
	outputFields, err = translateOutputFields([]string{}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", "%"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", idFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
}

func TestCreateCollectionTask(t *testing.T) {
	Params.InitOnce()

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	shardsNum := int32(2)
	prefix := "TestCreateCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "fvec"
	varCharField := "varChar"

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type[int64Field] = schemapb.DataType_Int64
	fieldName2Type[varCharField] = schemapb.DataType_VarChar
	fieldName2Type[floatVecField] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, int64Field, false)
	var marshaledSchema []byte
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	task := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	t.Run("on enqueue", func(t *testing.T) {
		err := task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, commonpb.MsgType_CreateCollection, task.Type())
	})

	t.Run("ctx", func(t *testing.T) {
		traceCtx := task.TraceCtx()
		assert.NotNil(t, traceCtx)
	})

	t.Run("id", func(t *testing.T) {
		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())
	})

	t.Run("name", func(t *testing.T) {
		assert.Equal(t, CreateCollectionTaskName, task.Name())
	})

	t.Run("ts", func(t *testing.T) {
		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())
	})

	t.Run("process task", func(t *testing.T) {
		var err error

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		// recreate -> fail
		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		err = task.PostExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("PreExecute", func(t *testing.T) {
		var err error

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		task.Schema = []byte{0x1, 0x2, 0x3, 0x4}
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.Schema = marshaledSchema

		task.ShardsNum = Params.ProxyCfg.MaxShardNum + 1
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.ShardsNum = shardsNum

		reqBackup := proto.Clone(task.CreateCollectionRequest).(*milvuspb.CreateCollectionRequest)
		schemaBackup := proto.Clone(schema).(*schemapb.CollectionSchema)

		schemaWithTooManyFields := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, Params.ProxyCfg.MaxFieldNum+1),
		}
		marshaledSchemaWithTooManyFields, err := proto.Marshal(schemaWithTooManyFields)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = marshaledSchemaWithTooManyFields
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		task.CreateCollectionRequest = reqBackup

		// validateCollectionName

		schema.Name = " " // empty
		emptyNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = emptyNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema.Name = prefix
		for i := 0; i < int(Params.ProxyCfg.MaxNameLength); i++ {
			schema.Name += strconv.Itoa(i % 10)
		}
		tooLongNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = tooLongNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema.Name = "$" // invalid first char
		invalidFirstCharSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidFirstCharSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validateDuplicatedFieldName
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields = append(schema.Fields, schema.Fields[0])
		duplicatedFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = duplicatedFieldsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validatePrimaryKey
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			schema.Fields[idx].IsPrimaryKey = false
		}
		noPrimaryFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noPrimaryFieldsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validateFieldName
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			schema.Fields[idx].Name = "$"
		}
		invalidFieldNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidFieldNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validateMaxLengthPerRow
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_VarChar {
				schema.Fields[idx].TypeParams = nil
			}
		}
		noTypeParamsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noTypeParamsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// ValidateVectorField
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = nil
			}
		}
		noDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "not int",
					},
				}
			}
		}
		dimNotIntSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = dimNotIntSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension) + 1),
					},
				}
			}
		}
		tooLargeDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = tooLargeDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields[1].DataType = schemapb.DataType_BinaryVector
		schema.Fields[1].TypeParams = []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension) + 1),
			},
		}
		binaryTooLargeDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = binaryTooLargeDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:      0,
			Name:         "second_vector",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(128),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		})
		twoVecFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = twoVecFieldsSchema
		err = task.PreExecute(ctx)
		if enableMultipleVectorFields {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	})
}

func TestDropCollectionTask(t *testing.T) {
	Params.InitOnce()

	prefix := "TestDropCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	ctx := context.Background()

	task := &dropCollectionTask{
		Condition: NewTaskCondition(ctx),
		DropCollectionRequest: &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:    ctx,
		result: nil,
	}

	task.SetID(100)
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, DropCollectionTaskName, task.Name())
	assert.Equal(t, commonpb.MsgType_DropCollection, task.Type())
	task.SetTs(100)
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())

	err := task.PreExecute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)
	task.CollectionName = collectionName

	cache := newMockCache()
	chMgr := newMockChannelsMgr()
	rc := newMockRootCoord()

	globalMetaCache = cache
	task.chMgr = chMgr
	task.rootCoord = rc

	cache.setGetIDFunc(func(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
		return 0, errors.New("mock")
	})
	err = task.Execute(ctx)
	assert.Error(t, err)
	cache.setGetIDFunc(func(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
		return 0, nil
	})

	rc.DropCollectionFunc = func(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
		return nil, errors.New("mock")
	}
	err = task.Execute(ctx)
	assert.Error(t, err)
	rc.DropCollectionFunc = func(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
		return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
	}

	// normal case
	err = task.Execute(ctx)
	assert.NoError(t, err)
	err = task.PostExecute(ctx)
	assert.NoError(t, err)
}

func TestHasCollectionTask(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)
	prefix := "TestHasCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := int32(2)
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	//CreateCollection
	task := &hasCollectionTask{
		Condition: NewTaskCondition(ctx),
		HasCollectionRequest: &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_HasCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, false, task.result.Value)
	// createCollection in RootCood and fill GlobalMetaCache
	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, collectionName)

	// success to drop collection
	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, true, task.result.Value)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	rc.updateState(internalpb.StateCode_Abnormal)
	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.Nil(t, err)
	err = task.Execute(ctx)
	assert.NotNil(t, err)

}

func TestDescribeCollectionTask(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	//CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DescribeCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err := task.Execute(ctx)
	assert.Nil(t, err)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	// describe collection with id
	task.CollectionID = 1
	task.CollectionName = ""
	err = task.PreExecute(ctx)
	assert.NoError(t, err)

	rc.Stop()
	task.CollectionID = 0
	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.Nil(t, err)
	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, task.result.Status.ErrorCode)
}

func TestDescribeCollectionTask_ShardsNum1(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := int32(2)
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, collectionName)

	//CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	err = task.PreExecute(ctx)
	assert.Nil(t, err)

	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	assert.Equal(t, shardsNum, task.result.ShardsNum)

}

func TestDescribeCollectionTask_ShardsNum2(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
	}

	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, collectionName)

	//CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.Nil(t, err)

	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	assert.Equal(t, common.DefaultShardsNum, task.result.ShardsNum)
	rc.Stop()
}

func TestCreatePartitionTask(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestCreatePartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &createPartitionTask{
		Condition: NewTaskCondition(ctx),
		CreatePartitionRequest: &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_CreatePartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)
}

func TestDropPartitionTask(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestDropPartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &dropPartitionTask{
		Condition: NewTaskCondition(ctx),
		DropPartitionRequest: &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DropPartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)
}

func TestHasPartitionTask(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestHasPartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &hasPartitionTask{
		Condition: NewTaskCondition(ctx),
		HasPartitionRequest: &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_HasPartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)
}

func TestShowPartitionsTask(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestShowPartitionsTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &showPartitionsTask{
		Condition: NewTaskCondition(ctx),
		ShowPartitionsRequest: &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{partitionName},
			Type:           milvuspb.ShowType_All,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_ShowPartitions, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	task.PartitionNames = []string{"#0xc0de"}
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionNames = []string{partitionName}
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	err = task.Execute(ctx)
	assert.NotNil(t, err)

}
func TestTask_Int64PrimaryKey(t *testing.T) {
	var err error

	Params.InitOnce()

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()

	ctx := context.Background()

	mgr := newShardClientMgr()
	err = InitMetaCache(ctx, rc, qc, mgr)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestTask_all"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}
	nb := 10

	t.Run("create collection", func(t *testing.T) {
		schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createColT := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:       ctx,
			rootCoord: rc,
			result:    nil,
			schema:    nil,
		}

		assert.NoError(t, createColT.OnEnqueue())
		assert.NoError(t, createColT.PreExecute(ctx))
		assert.NoError(t, createColT.Execute(ctx))
		assert.NoError(t, createColT.PostExecute(ctx))

		_, _ = rc.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, factory)
	defer chMgr.removeAllDMLStream()

	_, err = chMgr.getOrCreateDmlStream(collectionID)
	assert.NoError(t, err)
	pchans, err := chMgr.getChannels(collectionID)
	assert.NoError(t, err)

	interval := time.Millisecond * 10
	tso := newMockTsoAllocator()

	ticker := newChannelsTimeTicker(ctx, interval, []string{}, newGetStatisticsFunc(pchans), tso)
	_ = ticker.start()
	defer ticker.close()

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, Params.ProxyCfg.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	segAllocator.Init()
	_ = segAllocator.Start()
	defer segAllocator.Close()

	t.Run("insert", func(t *testing.T) {
		hash := generateHashKeys(nb)
		task := &insertTask{
			BaseInsertTask: BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: internalpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: Params.ProxyCfg.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        internalpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
			vChannels:     nil,
			pChannels:     nil,
			schema:        nil,
		}

		for fieldName, dataType := range fieldName2Types {
			task.FieldsData = append(task.FieldsData, generateFieldData(dataType, fieldName, nb))
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		task := &deleteTask{
			Condition: NewTaskCondition(ctx),
			BaseDeleteTask: msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: internalpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  Params.ProxyCfg.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			deleteExpr: "int64 in [0, 1]",
			ctx:        ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			chMgr:    chMgr,
			chTicker: ticker,
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NotNil(t, task.TraceCtx())

		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())

		task.Base.MsgType = commonpb.MsgType_Delete
		assert.Equal(t, commonpb.MsgType_Delete, task.Type())

		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())

		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))

		task2 := &deleteTask{
			Condition: NewTaskCondition(ctx),
			BaseDeleteTask: msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: internalpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  Params.ProxyCfg.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			deleteExpr: "int64 not in [0, 1]",
			ctx:        ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			chMgr:    chMgr,
			chTicker: ticker,
		}
		assert.Error(t, task2.PreExecute(ctx))
	})
}

func TestTask_VarCharPrimaryKey(t *testing.T) {
	var err error

	Params.InitOnce()

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := NewQueryCoordMock()
	qc.Start()
	defer qc.Stop()

	ctx := context.Background()

	mgr := newShardClientMgr()
	err = InitMetaCache(ctx, rc, qc, mgr)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestTask_all"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testVarCharField:  schemapb.DataType_VarChar,
		testFloatVecField: schemapb.DataType_FloatVector}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}
	nb := 10

	t.Run("create collection", func(t *testing.T) {
		schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testVarCharField, false)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createColT := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:       ctx,
			rootCoord: rc,
			result:    nil,
			schema:    nil,
		}

		assert.NoError(t, createColT.OnEnqueue())
		assert.NoError(t, createColT.PreExecute(ctx))
		assert.NoError(t, createColT.Execute(ctx))
		assert.NoError(t, createColT.PostExecute(ctx))

		_, _ = rc.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, factory)
	defer chMgr.removeAllDMLStream()

	_, err = chMgr.getOrCreateDmlStream(collectionID)
	assert.NoError(t, err)
	pchans, err := chMgr.getChannels(collectionID)
	assert.NoError(t, err)

	interval := time.Millisecond * 10
	tso := newMockTsoAllocator()

	ticker := newChannelsTimeTicker(ctx, interval, []string{}, newGetStatisticsFunc(pchans), tso)
	_ = ticker.start()
	defer ticker.close()

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, Params.ProxyCfg.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	segAllocator.Init()
	_ = segAllocator.Start()
	defer segAllocator.Close()

	t.Run("insert", func(t *testing.T) {
		hash := generateHashKeys(nb)
		task := &insertTask{
			BaseInsertTask: BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: internalpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: Params.ProxyCfg.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        internalpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
			vChannels:     nil,
			pChannels:     nil,
			schema:        nil,
		}

		fieldID := common.StartOfUserFieldID
		for fieldName, dataType := range fieldName2Types {
			task.FieldsData = append(task.FieldsData, generateFieldData(dataType, fieldName, nb))
			fieldID++
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		task := &deleteTask{
			Condition: NewTaskCondition(ctx),
			BaseDeleteTask: msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: internalpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  Params.ProxyCfg.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			deleteExpr: "varChar in [\"milvus\", \"test\"]",
			ctx:        ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			chMgr:    chMgr,
			chTicker: ticker,
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NotNil(t, task.TraceCtx())

		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())

		task.Base.MsgType = commonpb.MsgType_Delete
		assert.Equal(t, commonpb.MsgType_Delete, task.Type())

		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())

		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))

		task2 := &deleteTask{
			Condition: NewTaskCondition(ctx),
			BaseDeleteTask: msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: internalpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  Params.ProxyCfg.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			deleteExpr: "varChar not in [\"milvus\", \"test\"]",
			ctx:        ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			chMgr:    chMgr,
			chTicker: ticker,
		}
		assert.Error(t, task2.PreExecute(ctx))
	})
}

func TestCreateAlias_all(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestCreateAlias_all"
	collectionName := prefix + funcutil.GenRandomStr()
	task := &CreateAliasTask{
		Condition: NewTaskCondition(ctx),
		CreateAliasRequest: &milvuspb.CreateAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias1",
		},
		ctx: ctx,
		result: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())

	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_CreateAlias
	assert.Equal(t, commonpb.MsgType_CreateAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}

func TestDropAlias_all(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	task := &DropAliasTask{
		Condition: NewTaskCondition(ctx),
		DropAliasRequest: &milvuspb.DropAliasRequest{
			Base:  nil,
			Alias: "alias1",
		},
		ctx: ctx,
		result: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())
	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_DropAlias
	assert.Equal(t, commonpb.MsgType_DropAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

}

func TestAlterAlias_all(t *testing.T) {
	Params.InitOnce()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestAlterAlias_all"
	collectionName := prefix + funcutil.GenRandomStr()
	task := &AlterAliasTask{
		Condition: NewTaskCondition(ctx),
		AlterAliasRequest: &milvuspb.AlterAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias1",
		},
		ctx: ctx,
		result: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())

	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_AlterAlias
	assert.Equal(t, commonpb.MsgType_AlterAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}

func Test_createIndexTask_getIndexedField(t *testing.T) {
	collectionName := "test"
	fieldName := "test"

	cit := &createIndexTask{
		CreateIndexRequest: &milvuspb.CreateIndexRequest{
			CollectionName: collectionName,
			FieldName:      fieldName,
		},
	}

	t.Run("normal", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         fieldName,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_FloatVector,
						TypeParams:   nil,
						IndexParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
						AutoID: false,
					},
				},
			}, nil
		})
		globalMetaCache = cache
		field, err := cit.getIndexedField(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, fieldName, field.GetName())
	})

	t.Run("schema not found", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return nil, errors.New("mock")
		})
		globalMetaCache = cache
		_, err := cit.getIndexedField(context.Background())
		assert.Error(t, err)
	})

	t.Run("invalid schema", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						Name: fieldName,
					},
					{
						Name: fieldName, // duplicate
					},
				},
			}, nil
		})
		globalMetaCache = cache
		_, err := cit.getIndexedField(context.Background())
		assert.Error(t, err)
	})

	t.Run("field not found", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						Name: fieldName + fieldName,
					},
				},
			}, nil
		})
		globalMetaCache = cache
		_, err := cit.getIndexedField(context.Background())
		assert.Error(t, err)
	})
}

func Test_fillDimension(t *testing.T) {
	t.Run("scalar", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
		}
		assert.NoError(t, fillDimension(f, nil))
	})

	t.Run("no dim in schema", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
		}
		assert.Error(t, fillDimension(f, nil))
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		assert.Error(t, fillDimension(f, map[string]string{"dim": "8"}))
	})

	t.Run("normal", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		m := map[string]string{}
		assert.NoError(t, fillDimension(f, m))
		assert.Equal(t, "128", m["dim"])
	})
}

func Test_checkTrain(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		m := map[string]string{
			"index_type":  "IVF_FLAT",
			"nlist":       "1024",
			"metric_type": "L2",
		}
		assert.NoError(t, checkTrain(f, m))
	})

	t.Run("scalar", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
		}
		m := map[string]string{
			"index_type": "scalar",
		}
		assert.NoError(t, checkTrain(f, m))
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		m := map[string]string{
			"index_type":  "IVF_FLAT",
			"nlist":       "1024",
			"metric_type": "L2",
			"dim":         "8",
		}
		assert.Error(t, checkTrain(f, m))
	})

	t.Run("invalid params", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		m := map[string]string{
			"index_type":  "IVF_FLAT",
			"metric_type": "L2",
		}
		assert.Error(t, checkTrain(f, m))
	})
}

func Test_createIndexTask_PreExecute(t *testing.T) {
	collectionName := "test"
	fieldName := "test"

	cit := &createIndexTask{
		CreateIndexRequest: &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_CreateIndex,
			},
			CollectionName: collectionName,
			FieldName:      fieldName,
		},
	}

	t.Run("normal", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetIDFunc(func(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
			return 100, nil
		})
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         fieldName,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_FloatVector,
						TypeParams:   nil,
						IndexParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
						AutoID: false,
					},
				},
			}, nil
		})
		globalMetaCache = cache
		cit.CreateIndexRequest.ExtraParams = []*commonpb.KeyValuePair{
			{
				Key:   "index_type",
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: "1024",
			},
			{
				Key:   "metric_type",
				Value: "L2",
			},
		}
		assert.NoError(t, cit.PreExecute(context.Background()))
	})

	t.Run("collection not found", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetIDFunc(func(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
			return 0, errors.New("mock")
		})
		globalMetaCache = cache
		assert.Error(t, cit.PreExecute(context.Background()))
	})
}
