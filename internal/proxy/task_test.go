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
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/pkg/util/uniquegenerator"
)

// TODO(dragondriver): add more test cases

const (
	maxTestStringLen     = 100
	testBoolField        = "bool"
	testInt32Field       = "int32"
	testInt64Field       = "int64"
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
				Key:   common.DimKey,
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

func constructCollectionSchemaEnableDynamicSchema(
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
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	return &schemapb.CollectionSchema{
		Name:               collectionName,
		Description:        "",
		AutoID:             false,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			pk,
			fVec,
		},
	}
}

func ConstructCollectionSchemaWithPartitionKey(collectionName string, fieldName2DataType map[string]schemapb.DataType, primaryFieldName string, partitionKeyFieldName string, autoID bool) *schemapb.CollectionSchema {
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2DataType, primaryFieldName, autoID)
	for _, field := range schema.Fields {
		if field.Name == partitionKeyFieldName {
			field.IsPartitionKey = true
		}
	}

	return schema
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
					Key:   common.DimKey,
					Value: strconv.Itoa(testVecDim),
				},
			}
		}
		if dataType == schemapb.DataType_VarChar {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
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
	boolField, int32Field, int64Field, floatField, doubleField string,
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
				Key:   common.DimKey,
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
				Key:   common.DimKey,
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
				Key:   common.MetricTypeKey,
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
	var userOutputFields []string
	var err error

	schema := &schemapb.CollectionSchema{
		Name:        "TestTranslateOutputFields",
		Description: "TestTranslateOutputFields",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{Name: idFieldName, FieldID: 0, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: tsFieldName, FieldID: 1, DataType: schemapb.DataType_Int64},
			{Name: floatVectorFieldName, FieldID: 100, DataType: schemapb.DataType_FloatVector},
			{Name: binaryVectorFieldName, FieldID: 101, DataType: schemapb.DataType_BinaryVector},
		},
	}

	outputFields, userOutputFields, err = translateOutputFields([]string{}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{}, outputFields)
	assert.ElementsMatch(t, []string{}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{"*"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{" * "}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{"*", tsFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, userOutputFields)

	//=========================================================================
	outputFields, userOutputFields, err = translateOutputFields([]string{}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{"*"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{"*", tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, userOutputFields)

	outputFields, userOutputFields, err = translateOutputFields([]string{"A"}, schema, true)
	assert.Error(t, err)

	t.Run("enable dynamic schema", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "TestTranslateOutputFields",
			Description:        "TestTranslateOutputFields",
			AutoID:             false,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{Name: idFieldName, FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{Name: tsFieldName, FieldID: 2, DataType: schemapb.DataType_Int64},
				{Name: floatVectorFieldName, FieldID: 100, DataType: schemapb.DataType_FloatVector},
				{Name: binaryVectorFieldName, FieldID: 101, DataType: schemapb.DataType_BinaryVector},
				{Name: common.MetaFieldName, FieldID: 102, DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		outputFields, userOutputFields, err = translateOutputFields([]string{"A", idFieldName}, schema, true)
		assert.Equal(t, nil, err)
		assert.ElementsMatch(t, []string{common.MetaFieldName, idFieldName}, outputFields)
		assert.ElementsMatch(t, []string{"A", idFieldName}, userOutputFields)

		outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta[\"A\"]"}, schema, true)
		assert.Error(t, err)

		outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta[]"}, schema, true)
		assert.Error(t, err)

		outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta[\"\"]"}, schema, true)
		assert.Error(t, err)

		outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta["}, schema, true)
		assert.Error(t, err)

		outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "[]"}, schema, true)
		assert.Error(t, err)

		outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "A > 1"}, schema, true)
		assert.Error(t, err)

		outputFields, userOutputFields, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, ""}, schema, true)
		assert.Error(t, err)
	})
}

func TestCreateCollectionTask(t *testing.T) {

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	shardsNum := common.DefaultShardsNum
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

		task.ShardsNum = Params.ProxyCfg.MaxShardNum.GetAsInt32() + 1
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.ShardsNum = shardsNum

		reqBackup := proto.Clone(task.CreateCollectionRequest).(*milvuspb.CreateCollectionRequest)
		schemaBackup := proto.Clone(schema).(*schemapb.CollectionSchema)

		schemaWithTooManyFields := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, Params.ProxyCfg.MaxFieldNum.GetAsInt32()+1),
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
		for i := 0; i < Params.ProxyCfg.MaxNameLength.GetAsInt(); i++ {
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
						Key:   common.DimKey,
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
						Key:   common.DimKey,
						Value: strconv.Itoa(Params.ProxyCfg.MaxDimension.GetAsInt() + 1),
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
				Key:   common.DimKey,
				Value: strconv.Itoa(Params.ProxyCfg.MaxDimension.GetAsInt() + 1),
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
					Key:   common.DimKey,
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

	t.Run("specify dynamic field", func(t *testing.T) {
		dynamicField := &schemapb.FieldSchema{
			Name:      "json",
			IsDynamic: true,
		}
		var marshaledSchema []byte
		schema2 := &schemapb.CollectionSchema{
			Name:   collectionName,
			Fields: append(schema.Fields, dynamicField),
		}
		marshaledSchema, err := proto.Marshal(schema2)
		assert.NoError(t, err)

		task2 := &createCollectionTask{
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

		err = task2.OnEnqueue()
		assert.NoError(t, err)

		err = task2.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestHasCollectionTask(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
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
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, false, task.result.Value)
	// createCollection in RootCood and fill GlobalMetaCache
	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)

	// success to drop collection
	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, true, task.result.Value)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	rc.updateState(commonpb.StateCode_Abnormal)
	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.NoError(t, err)
	err = task.Execute(ctx)
	assert.Error(t, err)

}

func TestDescribeCollectionTask(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
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
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err := task.Execute(ctx)
	assert.NoError(t, err)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	// describe collection with id
	task.CollectionID = 1
	task.CollectionName = ""
	err = task.PreExecute(ctx)
	assert.NoError(t, err)

	rc.Stop()
	task.CollectionID = 0
	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.NoError(t, err)
	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, task.result.Status.ErrorCode)
}

func TestDescribeCollectionTask_ShardsNum1(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := common.DefaultShardsNum
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
	globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)

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
	assert.NoError(t, err)

	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	assert.Equal(t, shardsNum, task.result.ShardsNum)
	assert.Equal(t, collectionName, task.result.GetCollectionName())
}

func TestDescribeCollectionTask_EnableDynamicSchema(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := common.DefaultShardsNum
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchemaEnableDynamicSchema(int64Field, floatVecField, dim, collectionName)
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
	globalMetaCache.GetCollectionID(ctx, dbName, collectionName)

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
	assert.NoError(t, err)

	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	assert.Equal(t, shardsNum, task.result.ShardsNum)
	assert.Equal(t, collectionName, task.result.GetCollectionName())
	assert.Equal(t, 2, len(task.result.Schema.Fields))
}

func TestDescribeCollectionTask_ShardsNum2(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
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
	globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)

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
	assert.NoError(t, err)

	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	assert.Equal(t, common.DefaultShardsNum, task.result.ShardsNum)
	assert.Equal(t, collectionName, task.result.GetCollectionName())
	rc.Stop()
}

func TestCreatePartitionTask(t *testing.T) {
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
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.Error(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)
}

func TestDropPartitionTask(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestDropPartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()
	qc := getQueryCoord()
	qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		PartitionIDs: []int64{},
	}, nil)
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil)

	mockCache := NewMockCache(t)
	mockCache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(UniqueID(1), nil)
	mockCache.On("GetPartitionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(UniqueID(1), nil)
	mockCache.On("GetCollectionSchema",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(&schemapb.CollectionSchema{}, nil)
	globalMetaCache = mockCache

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
		ctx:        ctx,
		rootCoord:  rc,
		queryCoord: qc,
		result:     nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DropPartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.Error(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	t.Run("get collectionID error", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(1), errors.New("error"))
		mockCache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(&schemapb.CollectionSchema{}, nil)
		globalMetaCache = mockCache
		task.PartitionName = "partition1"
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("partition not exist", func(t *testing.T) {
		task.PartitionName = "partition2"

		mockCache := NewMockCache(t)
		mockCache.On("GetPartitionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), merr.WrapErrPartitionNotFound(partitionName))
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(1), nil)
		mockCache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(&schemapb.CollectionSchema{}, nil)
		globalMetaCache = mockCache
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("get partition error", func(t *testing.T) {
		task.PartitionName = "partition3"

		mockCache := NewMockCache(t)
		mockCache.On("GetPartitionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), errors.New("error"))
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(1), nil)
		mockCache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(&schemapb.CollectionSchema{}, nil)
		globalMetaCache = mockCache
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestHasPartitionTask(t *testing.T) {
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
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.Error(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)
}

func TestShowPartitionsTask(t *testing.T) {
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
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.Error(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	task.PartitionNames = []string{"#0xc0de"}
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.PartitionNames = []string{partitionName}
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	err = task.Execute(ctx)
	assert.Error(t, err)

}

func TestTask_Int64PrimaryKey(t *testing.T) {
	var err error

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
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
				SourceID:  paramtable.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
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

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	_ = segAllocator.Start()
	defer segAllocator.Close()

	t.Run("insert", func(t *testing.T) {
		hash := generateHashKeys(nb)
		task := &insertTask{
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        msgpb.InsertDataVersion_ColumnBased,
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
			task.insertMsg.FieldsData = append(task.insertMsg.FieldsData, generateFieldData(dataType, fieldName, nb))
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		task := &deleteTask{
			Condition: NewTaskCondition(ctx),
			deleteMsg: &msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: msgpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			idAllocator: idAllocator,
			deleteExpr:  "int64 in [0, 1]",
			ctx:         ctx,
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

		task.deleteMsg.Base.MsgType = commonpb.MsgType_Delete
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
			deleteMsg: &msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: msgpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			idAllocator: idAllocator,
			deleteExpr:  "int64 not in [0, 1]",
			ctx:         ctx,
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

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
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
				SourceID:  paramtable.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
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

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
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
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        msgpb.InsertDataVersion_ColumnBased,
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
			task.insertMsg.FieldsData = append(task.insertMsg.FieldsData, generateFieldData(dataType, fieldName, nb))
			fieldID++
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("upsert", func(t *testing.T) {
		hash := generateHashKeys(nb)
		task := &upsertTask{
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &BaseInsertTask{
					BaseMsg: msgstream.BaseMsg{
						HashValues: hash,
					},
					InsertRequest: msgpb.InsertRequest{
						Base: &commonpb.MsgBase{
							MsgType:  commonpb.MsgType_Insert,
							MsgID:    0,
							SourceID: paramtable.GetNodeID(),
						},
						DbName:         dbName,
						CollectionName: collectionName,
						PartitionName:  partitionName,
						NumRows:        uint64(nb),
						Version:        msgpb.InsertDataVersion_ColumnBased,
					},
				},
				DeleteMsg: &msgstream.DeleteMsg{
					BaseMsg: msgstream.BaseMsg{
						HashValues: hash,
					},
					DeleteRequest: msgpb.DeleteRequest{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_Delete,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  paramtable.GetNodeID(),
						},
						DbName:         dbName,
						CollectionName: collectionName,
						PartitionName:  partitionName,
					},
				},
			},

			Condition: NewTaskCondition(ctx),
			req: &milvuspb.UpsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Insert,
					MsgID:    0,
					SourceID: paramtable.GetNodeID(),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionName:  partitionName,
				HashKeys:       hash,
				NumRows:        uint32(nb),
			},
			ctx: ctx,
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
			task.req.FieldsData = append(task.req.FieldsData, generateFieldData(dataType, fieldName, nb))
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
			deleteMsg: &msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: msgpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			idAllocator: idAllocator,
			deleteExpr:  "varChar in [\"milvus\", \"test\"]",
			ctx:         ctx,
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

		task.deleteMsg.Base.MsgType = commonpb.MsgType_Delete
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
			deleteMsg: &msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: msgpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			idAllocator: idAllocator,
			deleteExpr:  "varChar not in [\"milvus\", \"test\"]",
			ctx:         ctx,
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
		req: &milvuspb.CreateIndexRequest{
			CollectionName: collectionName,
			FieldName:      fieldName,
		},
	}

	t.Run("normal", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(&schemapb.CollectionSchema{
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
		}, nil)

		globalMetaCache = cache
		field, err := cit.getIndexedField(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, fieldName, field.GetName())
	})

	t.Run("schema not found", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(nil, errors.New("mock"))
		globalMetaCache = cache
		_, err := cit.getIndexedField(context.Background())
		assert.Error(t, err)
	})

	t.Run("invalid schema", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: fieldName,
				},
				{
					Name: fieldName, // duplicate
				},
			},
		}, nil)
		globalMetaCache = cache
		_, err := cit.getIndexedField(context.Background())
		assert.Error(t, err)
	})

	t.Run("field not found", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: fieldName + fieldName,
				},
			},
		}, nil)
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
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		assert.Error(t, fillDimension(f, map[string]string{common.DimKey: "8"}))
	})

	t.Run("normal", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		m := map[string]string{}
		assert.NoError(t, fillDimension(f, m))
		assert.Equal(t, "128", m[common.DimKey])
	})
}

func Test_checkTrain(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		m := map[string]string{
			common.IndexTypeKey:  "IVF_FLAT",
			"nlist":              "1024",
			common.MetricTypeKey: "L2",
		}
		assert.NoError(t, checkTrain(f, m))
	})

	t.Run("scalar", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
		}
		m := map[string]string{
			common.IndexTypeKey: "scalar",
		}
		assert.NoError(t, checkTrain(f, m))
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		m := map[string]string{
			common.IndexTypeKey:  "IVF_FLAT",
			"nlist":              "1024",
			common.MetricTypeKey: "L2",
			common.DimKey:        "8",
		}
		assert.Error(t, checkTrain(f, m))
	})

	t.Run("invalid params", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		m := map[string]string{
			common.IndexTypeKey:  "IVF_FLAT",
			common.MetricTypeKey: "L2",
		}
		assert.Error(t, checkTrain(f, m))
	})
}

func Test_createIndexTask_PreExecute(t *testing.T) {
	collectionName := "test"
	fieldName := "test"

	cit := &createIndexTask{
		req: &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_CreateIndex,
			},
			CollectionName: collectionName,
			FieldName:      fieldName,
		},
	}

	t.Run("normal", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(100), nil)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(&schemapb.CollectionSchema{
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
		}, nil)
		globalMetaCache = cache
		cit.req.ExtraParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: "1024",
			},
			{
				Key:   common.MetricTypeKey,
				Value: "L2",
			},
		}
		assert.NoError(t, cit.PreExecute(context.Background()))
	})

	t.Run("collection not found", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), errors.New("mock"))
		globalMetaCache = cache
		assert.Error(t, cit.PreExecute(context.Background()))
	})

	t.Run("index name length exceed 255", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(100), nil)
		globalMetaCache = cache

		for i := 0; i < 256; i++ {
			cit.req.IndexName += "a"
		}
		err := cit.PreExecute(context.Background())

		assert.Error(t, err)
	})

	t.Run("index name start with number", func(t *testing.T) {
		cit.req.IndexName = "12a"
		err := cit.PreExecute(context.Background())

		assert.Error(t, err)
	})

	t.Run("index name include special characters", func(t *testing.T) {
		cit.req.IndexName = "ac#1"
		err := cit.PreExecute(context.Background())

		assert.Error(t, err)
	})
}

func Test_dropCollectionTask_PreExecute(t *testing.T) {
	dct := &dropCollectionTask{DropCollectionRequest: &milvuspb.DropCollectionRequest{
		Base:           &commonpb.MsgBase{},
		CollectionName: "0xffff", // invalid
	}}
	ctx := context.Background()
	err := dct.PreExecute(ctx)
	assert.Error(t, err)
	dct.DropCollectionRequest.CollectionName = "valid"
	err = dct.PreExecute(ctx)
	assert.NoError(t, err)
}

func Test_dropCollectionTask_Execute(t *testing.T) {
	mockRC := mocks.NewRootCoord(t)
	mockRC.On("DropCollection",
		mock.Anything, // context.Context
		mock.Anything, // *milvuspb.DropCollectionRequest
	).Return(&commonpb.Status{}, func(ctx context.Context, request *milvuspb.DropCollectionRequest) error {
		switch request.GetCollectionName() {
		case "c1":
			return errors.New("error mock DropCollection")
		case "c2":
			return common.NewStatusError(commonpb.ErrorCode_CollectionNotExists, "collection not exist")
		default:
			return nil
		}
	})

	ctx := context.Background()

	dct := &dropCollectionTask{rootCoord: mockRC, DropCollectionRequest: &milvuspb.DropCollectionRequest{CollectionName: "normal"}}
	err := dct.Execute(ctx)
	assert.NoError(t, err)

	dct.DropCollectionRequest.CollectionName = "c1"
	err = dct.Execute(ctx)
	assert.Error(t, err)

	dct.DropCollectionRequest.CollectionName = "c2"
	err = dct.Execute(ctx)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, dct.result.GetErrorCode())
}

func Test_dropCollectionTask_PostExecute(t *testing.T) {
	dct := &dropCollectionTask{}
	assert.NoError(t, dct.PostExecute(context.Background()))
}

func Test_loadCollectionTask_Execute(t *testing.T) {
	rc := newMockRootCoord()
	dc := NewDataCoordMock()

	qc := getQueryCoord()
	qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		PartitionIDs: []int64{},
	}, nil)
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil)

	dbName := funcutil.GenRandomStr()
	collectionName := funcutil.GenRandomStr()
	collectionID := UniqueID(1)
	//fieldName := funcutil.GenRandomStr()
	indexName := funcutil.GenRandomStr()
	ctx := context.Background()
	indexID := int64(1000)

	shardMgr := newShardClientMgr()
	// failed to get collection id.
	_ = InitMetaCache(ctx, rc, qc, shardMgr)

	rc.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Schema:         newTestSchema(),
			CollectionID:   collectionID,
			CollectionName: request.CollectionName,
		}, nil
	}

	lct := &loadCollectionTask{
		LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_LoadCollection,
				MsgID:     1,
				Timestamp: 1,
				SourceID:  1,
				TargetID:  1,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			ReplicaNumber:  1,
		},
		ctx:          ctx,
		queryCoord:   qc,
		datacoord:    dc,
		result:       nil,
		collectionID: 0,
	}

	t.Run("indexcoord describe index error", func(t *testing.T) {
		err := lct.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("indexcoord describe index not success", func(t *testing.T) {
		dc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "fail reason",
				},
			}, nil
		}

		err := lct.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("no vector index", func(t *testing.T) {
		dc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				IndexInfos: []*indexpb.IndexInfo{
					{
						CollectionID:         collectionID,
						FieldID:              100,
						IndexName:            indexName,
						IndexID:              indexID,
						TypeParams:           nil,
						IndexParams:          nil,
						IndexedRows:          1025,
						TotalRows:            1025,
						State:                commonpb.IndexState_Finished,
						IndexStateFailReason: "",
						IsAutoIndex:          false,
						UserIndexParams:      nil,
					},
				},
			}, nil
		}

		err := lct.Execute(ctx)
		assert.Error(t, err)
	})
}

func Test_loadPartitionTask_Execute(t *testing.T) {
	rc := newMockRootCoord()
	dc := NewDataCoordMock()

	qc := getQueryCoord()
	qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		PartitionIDs: []int64{},
	}, nil)
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil)

	dbName := funcutil.GenRandomStr()
	collectionName := funcutil.GenRandomStr()
	collectionID := UniqueID(1)
	//fieldName := funcutil.GenRandomStr()
	indexName := funcutil.GenRandomStr()
	ctx := context.Background()
	indexID := int64(1000)

	shardMgr := newShardClientMgr()
	// failed to get collection id.
	_ = InitMetaCache(ctx, rc, qc, shardMgr)

	rc.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Schema:         newTestSchema(),
			CollectionID:   collectionID,
			CollectionName: request.CollectionName,
		}, nil
	}

	lpt := &loadPartitionsTask{
		LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_LoadCollection,
				MsgID:     1,
				Timestamp: 1,
				SourceID:  1,
				TargetID:  1,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			ReplicaNumber:  1,
		},
		ctx:          ctx,
		queryCoord:   qc,
		datacoord:    dc,
		result:       nil,
		collectionID: 0,
	}

	t.Run("indexcoord describe index error", func(t *testing.T) {
		err := lpt.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("indexcoord describe index not success", func(t *testing.T) {
		dc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "fail reason",
				},
			}, nil
		}

		err := lpt.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("no vector index", func(t *testing.T) {
		dc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				IndexInfos: []*indexpb.IndexInfo{
					{
						CollectionID:         collectionID,
						FieldID:              100,
						IndexName:            indexName,
						IndexID:              indexID,
						TypeParams:           nil,
						IndexParams:          nil,
						IndexedRows:          1025,
						TotalRows:            1025,
						State:                commonpb.IndexState_Finished,
						IndexStateFailReason: "",
						IsAutoIndex:          false,
						UserIndexParams:      nil,
					},
				},
			}, nil
		}

		err := lpt.Execute(ctx)
		assert.Error(t, err)
	})
}

func TestCreateResourceGroupTask(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
	qc.EXPECT().CreateResourceGroup(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)

	createRGReq := &milvuspb.CreateResourceGroupRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		ResourceGroup: "rg",
	}

	task := &CreateResourceGroupTask{
		CreateResourceGroupRequest: createRGReq,
		ctx:                        ctx,
		queryCoord:                 qc,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_CreateResourceGroup, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)
}

func TestDropResourceGroupTask(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
	qc.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)

	dropRGReq := &milvuspb.DropResourceGroupRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		ResourceGroup: "rg",
	}

	task := &DropResourceGroupTask{
		DropResourceGroupRequest: dropRGReq,
		ctx:                      ctx,
		queryCoord:               qc,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DropResourceGroup, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)
}

func TestTransferNodeTask(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
	qc.EXPECT().TransferNode(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)

	req := &milvuspb.TransferNodeRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		SourceResourceGroup: "rg1",
		TargetResourceGroup: "rg2",
		NumNode:             1,
	}

	task := &TransferNodeTask{
		TransferNodeRequest: req,
		ctx:                 ctx,
		queryCoord:          qc,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_TransferNode, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)
}

func TestTransferReplicaTask(t *testing.T) {
	rc := &MockRootCoordClientInterface{}
	qc := getQueryCoord()
	qc.EXPECT().TransferReplica(mock.Anything, mock.Anything).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)
	// make it avoid remote call on rc
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection1")

	req := &milvuspb.TransferReplicaRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		CollectionName:      "collection1",
		SourceResourceGroup: "rg1",
		TargetResourceGroup: "rg2",
		NumReplica:          1,
	}

	task := &TransferReplicaTask{
		TransferReplicaRequest: req,
		ctx:                    ctx,
		queryCoord:             qc,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_TransferReplica, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)
}

func TestListResourceGroupsTask(t *testing.T) {
	rc := &MockRootCoordClientInterface{}
	qc := getQueryCoord()
	qc.EXPECT().ListResourceGroups(mock.Anything, mock.Anything).Return(&milvuspb.ListResourceGroupsResponse{
		Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		ResourceGroups: []string{meta.DefaultResourceGroupName, "rg"},
	}, nil)
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)

	req := &milvuspb.ListResourceGroupsRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
	}

	task := &ListResourceGroupsTask{
		ListResourceGroupsRequest: req,
		ctx:                       ctx,
		queryCoord:                qc,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_ListResourceGroups, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	groups := task.result.GetResourceGroups()
	assert.Contains(t, groups, meta.DefaultResourceGroupName)
	assert.Contains(t, groups, "rg")
}

func TestDescribeResourceGroupTask(t *testing.T) {
	rc := &MockRootCoordClientInterface{}
	qc := getQueryCoord()
	qc.EXPECT().DescribeResourceGroup(mock.Anything, mock.Anything).Return(&querypb.DescribeResourceGroupResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		ResourceGroup: &querypb.ResourceGroupInfo{
			Name:             "rg",
			Capacity:         2,
			NumAvailableNode: 1,
			NumOutgoingNode:  map[int64]int32{1: 1},
			NumIncomingNode:  map[int64]int32{2: 2},
		},
	}, nil)
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)
	// make it avoid remote call on rc
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection1")
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection2")

	req := &milvuspb.DescribeResourceGroupRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		ResourceGroup: "rg",
	}

	task := &DescribeResourceGroupTask{
		DescribeResourceGroupRequest: req,
		ctx:                          ctx,
		queryCoord:                   qc,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DescribeResourceGroup, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	groupInfo := task.result.GetResourceGroup()
	outgoingNodeNum := groupInfo.GetNumOutgoingNode()
	incomingNodeNum := groupInfo.GetNumIncomingNode()
	assert.NotNil(t, outgoingNodeNum["collection1"])
	assert.NotNil(t, incomingNodeNum["collection2"])
}

func TestDescribeResourceGroupTaskFailed(t *testing.T) {
	rc := &MockRootCoordClientInterface{}
	qc := getQueryCoord()
	qc.EXPECT().DescribeResourceGroup(mock.Anything, mock.Anything).Return(&querypb.DescribeResourceGroupResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
	}, nil)
	qc.Start()
	defer qc.Stop()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, qc, mgr)
	// make it avoid remote call on rc
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection1")
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection2")

	req := &milvuspb.DescribeResourceGroupRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		ResourceGroup: "rgggg",
	}

	task := &DescribeResourceGroupTask{
		DescribeResourceGroupRequest: req,
		ctx:                          ctx,
		queryCoord:                   qc,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DescribeResourceGroup, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, task.result.Status.ErrorCode)
}

func TestCreateCollectionTaskWithPartitionKey(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	shardsNum := common.DefaultShardsNum
	prefix := "TestCreateCollectionTaskWithPartitionKey"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	int64Field := &schemapb.FieldSchema{
		Name:         "int64",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
	}
	varCharField := &schemapb.FieldSchema{
		Name:     "varChar",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "max_length",
				Value: strconv.Itoa(testMaxVarCharLength),
			},
		},
	}
	floatVecField := &schemapb.FieldSchema{
		Name:     "fvec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(testVecDim),
			},
		},
	}
	partitionKeyField := &schemapb.FieldSchema{
		Name:           "partition_key",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	schema := &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: []*schemapb.FieldSchema{int64Field, varCharField, partitionKeyField, floatVecField},
	}

	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	task := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				Timestamp: Timestamp(time.Now().UnixNano()),
			},
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

	t.Run("PreExecute", func(t *testing.T) {
		var err error

		// test default num partitions
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, common.DefaultPartitionsWithPartitionKey, task.GetNumPartitions())

		// test specify num partition without partition key field
		partitionKeyField.IsPartitionKey = false
		task.NumPartitions = common.DefaultPartitionsWithPartitionKey * 2
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		partitionKeyField.IsPartitionKey = true

		// test multi partition key field
		varCharField.IsPartitionKey = true
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		varCharField.IsPartitionKey = false

		// test partitions < 0
		task.NumPartitions = -2
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.NumPartitions = 1000

		// test partition key type not in [int64, varChar]
		partitionKeyField.DataType = schemapb.DataType_FloatVector
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		partitionKeyField.DataType = schemapb.DataType_Int64

		// test partition key field not primary key field
		primaryField, _ := typeutil.GetPrimaryFieldSchema(schema)
		primaryField.IsPartitionKey = true
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		primaryField.IsPartitionKey = false

		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("Execute", func(t *testing.T) {
		err = task.Execute(ctx)
		assert.NoError(t, err)

		// check default partitions
		err = InitMetaCache(ctx, rc, nil, nil)
		assert.NoError(t, err)
		partitionNames, err := getDefaultPartitionNames(ctx, "", task.CollectionName)
		assert.NoError(t, err)
		assert.Equal(t, task.GetNumPartitions(), int64(len(partitionNames)))

		createPartitionTask := &createPartitionTask{
			Condition: NewTaskCondition(ctx),
			CreatePartitionRequest: &milvuspb.CreatePartitionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionName:  "new_partition",
			},
			ctx:       ctx,
			rootCoord: rc,
		}
		err = createPartitionTask.PreExecute(ctx)
		assert.Error(t, err)

		dropPartitionTask := &dropPartitionTask{
			Condition: NewTaskCondition(ctx),
			DropPartitionRequest: &milvuspb.DropPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionName:  "new_partition",
			},
			ctx:       ctx,
			rootCoord: rc,
		}
		err = dropPartitionTask.PreExecute(ctx)
		assert.Error(t, err)

		loadPartitionTask := &loadPartitionsTask{
			Condition: NewTaskCondition(ctx),
			LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionNames: []string{"_default_0"},
			},
			ctx: ctx,
		}
		err = loadPartitionTask.PreExecute(ctx)
		assert.Error(t, err)

		releasePartitionsTask := &releasePartitionsTask{
			Condition: NewTaskCondition(ctx),
			ReleasePartitionsRequest: &milvuspb.ReleasePartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionNames: []string{"_default_0"},
			},
			ctx: ctx,
		}
		err = releasePartitionsTask.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestPartitionKey(t *testing.T) {
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	qc := getQueryCoord()
	qc.Start()
	defer qc.Stop()

	ctx := context.Background()

	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rc, qc, mgr)
	assert.NoError(t, err)

	shardsNum := common.DefaultShardsNum
	prefix := "TestInsertTaskWithPartitionKey"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64_field", false)
	partitionKeyField := &schemapb.FieldSchema{
		Name:           "partition_key_field",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	fieldName2Type["partition_key_field"] = schemapb.DataType_Int64
	schema.Fields = append(schema.Fields, partitionKeyField)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	t.Run("create collection", func(t *testing.T) {
		createCollectionTask := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         "",
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
				NumPartitions:  common.DefaultPartitionsWithPartitionKey,
			},
			ctx:       ctx,
			rootCoord: rc,
			result:    nil,
			schema:    nil,
		}
		err = createCollectionTask.PreExecute(ctx)
		assert.NoError(t, err)
		err = createCollectionTask.Execute(ctx)
		assert.NoError(t, err)
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
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

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	segAllocator.Init()
	_ = segAllocator.Start()
	defer segAllocator.Close()

	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
	assert.NoError(t, err)
	assert.Equal(t, common.DefaultPartitionsWithPartitionKey, int64(len(partitionNames)))

	nb := 10
	fieldID := common.StartOfUserFieldID
	fieldDatas := make([]*schemapb.FieldData, 0)
	for fieldName, dataType := range fieldName2Type {
		fieldData := generateFieldData(dataType, fieldName, nb)
		fieldData.FieldId = int64(fieldID)
		fieldDatas = append(fieldDatas, generateFieldData(dataType, fieldName, nb))
		fieldID++
	}

	t.Run("Insert", func(t *testing.T) {
		it := &insertTask{
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{},
				InsertRequest: msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					FieldsData:     fieldDatas,
					NumRows:        uint64(nb),
					Version:        msgpb.InsertDataVersion_ColumnBased,
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

		// don't support specify partition name if use partition key
		it.insertMsg.PartitionName = partitionNames[0]
		assert.Error(t, it.PreExecute(ctx))

		it.insertMsg.PartitionName = ""
		assert.NoError(t, it.OnEnqueue())
		assert.NoError(t, it.PreExecute(ctx))
		assert.NoError(t, it.Execute(ctx))
		assert.NoError(t, it.PostExecute(ctx))
	})

	t.Run("Upsert", func(t *testing.T) {
		hash := generateHashKeys(nb)
		ut := &upsertTask{
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
			baseMsg: msgstream.BaseMsg{
				HashValues: hash,
			},
			req: &milvuspb.UpsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Upsert),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				CollectionName: collectionName,
				FieldsData:     fieldDatas,
				NumRows:        uint32(nb),
			},

			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
		}

		// don't support specify partition name if use partition key
		ut.req.PartitionName = partitionNames[0]
		assert.Error(t, ut.PreExecute(ctx))

		ut.req.PartitionName = ""
		assert.NoError(t, ut.OnEnqueue())
		assert.NoError(t, ut.PreExecute(ctx))
		assert.NoError(t, ut.Execute(ctx))
		assert.NoError(t, ut.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		dt := &deleteTask{
			Condition: NewTaskCondition(ctx),
			deleteMsg: &BaseDeleteTask{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: msgpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
				},
			},
			deleteExpr: "int64_field in [0, 1]",
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
			idAllocator: idAllocator,
			chMgr:       chMgr,
			chTicker:    ticker,
		}
		// don't support specify partition name if use partition key
		dt.deleteMsg.PartitionName = partitionNames[0]
		assert.Error(t, dt.PreExecute(ctx))

		dt.deleteMsg.PartitionName = ""
		assert.NoError(t, dt.PreExecute(ctx))
		assert.NoError(t, dt.Execute(ctx))
		assert.NoError(t, dt.PostExecute(ctx))
	})

	t.Run("search", func(t *testing.T) {
		searchTask := &searchTask{
			ctx: ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{},
			},
			request: &milvuspb.SearchRequest{
				CollectionName: collectionName,
				Nq:             1,
			},
			qc: qc,
			tr: timerecord.NewTimeRecorder("test-search"),
		}

		// don't support specify partition name if use partition key
		searchTask.request.PartitionNames = partitionNames
		err = searchTask.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("query", func(t *testing.T) {
		queryTask := &queryTask{
			ctx: ctx,
			RetrieveRequest: &internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{},
			},
			request: &milvuspb.QueryRequest{
				CollectionName: collectionName,
			},
			qc: qc,
		}

		// don't support specify partition name if use partition key
		queryTask.request.PartitionNames = partitionNames
		err = queryTask.PreExecute(ctx)
		assert.Error(t, err)
	})
}
