package testcases

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// create collection with nullable fields and insert with column / nullableColumn
func TestNullableDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create fields: pk + floatVec + all nullable scalar fields
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	schema := entity.NewSchema().WithName(common.GenRandomString("nullable_default_value", 5)).WithField(pkField).WithField(vecField)

	// create collection with all supported nullable fields
	expNullableFields := make([]string, 0)
	for _, fieldType := range hp.GetAllNullableFieldType() {
		nullableField := entity.NewField().WithName(common.GenRandomString("null", 5)).WithDataType(fieldType).WithNullable(true)
		if fieldType == entity.FieldTypeVarChar {
			nullableField.WithMaxLength(common.TestMaxLen)
		}
		if fieldType == entity.FieldTypeArray {
			nullableField.WithElementType(entity.FieldTypeInt64).WithMaxCapacity(common.TestCapacity)
		}
		schema.WithField(nullableField)
		expNullableFields = append(expNullableFields, nullableField.Name)
	}

	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	// describe collection and check nullable fields
	descCollection, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	common.CheckFieldsNullable(t, expNullableFields, descCollection.Schema)

	prepare := hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// insert data with default column
	defColumnOpt := hp.TNewColumnOptions()
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), defColumnOpt)

	// query with null expr
	for _, nullField := range expNullableFields {
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s is null", nullField)).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, 0, count)
	}

	// insert data with nullable column
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 1
	}
	columnOpt := hp.TNewColumnOptions()
	for _, name := range expNullableFields {
		columnOpt = columnOpt.WithColumnOption(name, hp.TNewDataOption().TWithValidData(validData))
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), columnOpt)

	hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query with null expr
	for _, nullField := range expNullableFields {
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s is null", nullField)).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, common.DefaultNb/2, count)
	}
}

// create collection with default value and insert with column / nullableColumn
func TestDefaultValueDefault(t *testing.T) {
	t.Skip("set defaultValue and insert with default column gets unexpected error, waiting for fix")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create fields: pk + floatVec + default value scalar fields
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	schema := entity.NewSchema().WithName(common.GenRandomString("default_value", 5)).WithField(pkField).WithField(vecField)

	// create collection with all supported nullable fields
	defaultBoolField := entity.NewField().WithName(common.GenRandomString("bool", 3)).WithDataType(entity.FieldTypeBool).WithDefaultValueBool(true)
	defaultInt8Field := entity.NewField().WithName(common.GenRandomString("int8", 3)).WithDataType(entity.FieldTypeInt8).WithDefaultValueInt(-1)
	defaultInt16Field := entity.NewField().WithName(common.GenRandomString("int16", 3)).WithDataType(entity.FieldTypeInt16).WithDefaultValueInt(4)
	defaultInt32Field := entity.NewField().WithName(common.GenRandomString("int32", 3)).WithDataType(entity.FieldTypeInt32).WithDefaultValueInt(2000)
	defaultInt64Field := entity.NewField().WithName(common.GenRandomString("int64", 3)).WithDataType(entity.FieldTypeInt64).WithDefaultValueLong(10000)
	defaultFloatField := entity.NewField().WithName(common.GenRandomString("float", 3)).WithDataType(entity.FieldTypeFloat).WithDefaultValueFloat(-1.0)
	defaultDoubleField := entity.NewField().WithName(common.GenRandomString("double", 3)).WithDataType(entity.FieldTypeDouble).WithDefaultValueDouble(math.MaxFloat64)
	defaultVarCharField := entity.NewField().WithName(common.GenRandomString("varchar", 3)).WithDataType(entity.FieldTypeVarChar).WithDefaultValueString("default").WithMaxLength(common.TestMaxLen)
	schema.WithField(defaultBoolField).WithField(defaultInt8Field).WithField(defaultInt16Field).WithField(defaultInt32Field).WithField(defaultInt64Field).WithField(defaultFloatField).WithField(defaultDoubleField).WithField(defaultVarCharField)

	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
	common.CheckErr(t, err, true)
	coll, _ := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckFieldsDefaultValue(t, map[string]interface{}{
		defaultBoolField.Name:    true,
		defaultInt8Field.Name:    int8(-1),
		defaultInt16Field.Name:   int16(4),
		defaultInt32Field.Name:   int32(2000),
		defaultInt64Field.Name:   int64(10000),
		defaultFloatField.Name:   float32(-1.0),
		defaultDoubleField.Name:  math.MaxFloat64,
		defaultVarCharField.Name: "default",
	}, coll.Schema)

	prepare := hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// insert data with default column
	defColumnOpt := hp.TNewColumnOptions()
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), defColumnOpt)

	// query with null expr
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s == -1", defaultInt8Field.Name)).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 0, count)

	// insert data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	columnOpt := hp.TNewColumnOptions()
	for _, name := range []string{
		defaultBoolField.Name, defaultInt8Field.Name, defaultInt16Field.Name, defaultInt32Field.Name,
		defaultInt64Field.Name, defaultFloatField.Name, defaultDoubleField.Name, defaultVarCharField.Name,
	} {
		columnOpt = columnOpt.WithColumnOption(name, hp.TNewDataOption().TWithValidData(validData))
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), columnOpt)

	// query with expr check default value
	type exprCount struct {
		expr  string
		count int64
	}
	exprCounts := []exprCount{
		{expr: fmt.Sprintf("%s == %t", defaultBoolField.Name, true), count: common.DefaultNb * 5 / 4},
		{expr: fmt.Sprintf("%s == %d", defaultInt8Field.Name, -1), count: common.DefaultNb/2 + 10}, // int8 [-128, 127]
		{expr: fmt.Sprintf("%s == %d", defaultInt16Field.Name, 4), count: common.DefaultNb/2 + 2},
		{expr: fmt.Sprintf("%s == %d", defaultInt32Field.Name, 2000), count: common.DefaultNb / 2},
		{expr: fmt.Sprintf("%s == %d", defaultInt64Field.Name, 10000), count: common.DefaultNb / 2},
		{expr: fmt.Sprintf("%s == %f", defaultFloatField.Name, -1.0), count: common.DefaultNb / 2},
		{expr: fmt.Sprintf("%s == %f", defaultDoubleField.Name, math.MaxFloat64), count: common.DefaultNb / 2},
		{expr: fmt.Sprintf("%s == '%s'", defaultVarCharField.Name, "default"), count: common.DefaultNb / 2},
	}
	for _, exprCount := range exprCounts {
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprCount.expr).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.Equal(t, exprCount.count, count)
	}
}

func TestNullableInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	pkField := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.GenRandomString("vec", 3)).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	// pk field not support null
	pkFieldNull := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithNullable(true)
	schema := entity.NewSchema().WithName(common.GenRandomString("nullable_invalid_field", 5)).WithField(pkFieldNull).WithField(vecField)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
	common.CheckErr(t, err, false, "primary field not support null")

	// vector type not support null
	notSupportedNullableDataTypes := []entity.FieldType{entity.FieldTypeFloatVector, entity.FieldTypeBinaryVector, entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector, entity.FieldTypeSparseVector, entity.FieldTypeInt8Vector}
	for _, fieldType := range notSupportedNullableDataTypes {
		nullableVectorField := entity.NewField().WithName(common.GenRandomString("null", 3)).WithDataType(fieldType).WithNullable(true)
		if fieldType != entity.FieldTypeSparseVector {
			nullableVectorField.WithDim(128)
		}
		schema := entity.NewSchema().WithName(common.GenRandomString("nullable_invalid_field", 5)).WithField(pkField).WithField(nullableVectorField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
		common.CheckErr(t, err, false, "vector type not support null")
	}

	// partition-key field not support null
	partitionField := entity.NewField().WithName(common.GenRandomString("partition", 3)).WithDataType(entity.FieldTypeInt64).WithIsPartitionKey(true).WithNullable(true)
	schema = entity.NewSchema().WithName(common.GenRandomString("nullable_invalid_field", 5)).WithField(pkField).WithField(vecField).WithField(partitionField)
	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
	common.CheckErr(t, err, false, "partition key field not support nullable")
}

func TestDefaultValueInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	pkField := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.GenRandomString("vec", 3)).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	// pk field not support default value
	pkFieldNull := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithDefaultValueLong(1)
	schema := entity.NewSchema().WithName(common.GenRandomString("def_invalid_field", 5)).WithField(pkFieldNull).WithField(vecField)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
	common.CheckErr(t, err, false, "primary field not support default_value")

	// vector type not support default value
	notSupportedDefaultValueDataTypes := []entity.FieldType{entity.FieldTypeFloatVector, entity.FieldTypeBinaryVector, entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector, entity.FieldTypeSparseVector, entity.FieldTypeInt8Vector}
	for _, fieldType := range notSupportedDefaultValueDataTypes {
		nullableVectorField := entity.NewField().WithName(common.GenRandomString("def", 3)).WithDataType(fieldType).WithDefaultValueFloat(2.0)
		if fieldType != entity.FieldTypeSparseVector {
			nullableVectorField.WithDim(128)
		}
		schema := entity.NewSchema().WithName(common.GenRandomString("def_invalid_field", 5)).WithField(pkField).WithField(nullableVectorField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
		common.CheckErr(t, err, false, "type not support default_value")
	}
	// json and array type not support default value
	notSupportedDefaultValueDataTypes = []entity.FieldType{entity.FieldTypeJSON, entity.FieldTypeArray}
	for _, fieldType := range notSupportedDefaultValueDataTypes {
		nullableVectorField := entity.NewField().WithName(common.GenRandomString("def", 3)).WithDataType(fieldType).WithElementType(entity.FieldTypeFloat).WithMaxCapacity(100).WithDefaultValueFloat(2.0)
		schema := entity.NewSchema().WithName(common.GenRandomString("def_invalid_field", 5)).WithField(pkField).WithField(vecField).WithField(nullableVectorField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
		common.CheckErr(t, err, false, "type not support default_value")
	}
}

func TestDefaultValueInvalidValue(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	pkField := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.GenRandomString("vec", 3)).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	// 定义测试用例
	testCases := []struct {
		name        string
		fieldName   string
		dataType    entity.FieldType
		setupField  func() *entity.Field
		expectedErr string
	}{
		{
			name:      "varchar field default_value_length > max_length",
			fieldName: common.DefaultVarcharFieldName,
			dataType:  entity.FieldTypeVarChar,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultVarcharFieldName).
					WithDataType(entity.FieldTypeVarChar).
					WithDefaultValueString("defaultaaaaaaaaa").
					WithMaxLength(2)
			},
			expectedErr: "the length (16) of string exceeds max length (2)",
		},
		{
			name:      "varchar field with int default_value",
			fieldName: common.DefaultVarcharFieldName,
			dataType:  entity.FieldTypeVarChar,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultVarcharFieldName).
					WithDataType(entity.FieldTypeVarChar).
					WithDefaultValueInt(2).
					WithMaxLength(100)
			},
			expectedErr: fmt.Sprintf("type (VarChar) of field (%s) is not equal to the type(DataType_Int) of default_value", common.DefaultVarcharFieldName),
		},
		{
			name:      "int32 field with int64 default_value",
			fieldName: common.DefaultInt32FieldName,
			dataType:  entity.FieldTypeInt32,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultInt32FieldName).
					WithDataType(entity.FieldTypeInt32).
					WithDefaultValueLong(2)
			},
			expectedErr: fmt.Sprintf("type (Int32) of field (%s) is not equal to the type(DataType_Int64) of default_value", common.DefaultInt32FieldName),
		},
		{
			name:      "int64 field with int default_value",
			fieldName: common.DefaultInt64FieldName,
			dataType:  entity.FieldTypeInt64,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultInt64FieldName).
					WithDataType(entity.FieldTypeInt64).
					WithDefaultValueInt(2)
			},
			expectedErr: fmt.Sprintf("type (Int64) of field (%s) is not equal to the type(DataType_Int) of default_value", common.DefaultInt64FieldName),
		},
		{
			name:      "float field with double default_value",
			fieldName: common.DefaultFloatFieldName,
			dataType:  entity.FieldTypeFloat,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultFloatFieldName).
					WithDataType(entity.FieldTypeFloat).
					WithDefaultValueDouble(2.6)
			},
			expectedErr: fmt.Sprintf("type (Float) of field (%s) is not equal to the type(DataType_Double) of default_value", common.DefaultFloatFieldName),
		},
		{
			name:      "double field with varchar default_value",
			fieldName: common.DefaultDoubleFieldName,
			dataType:  entity.FieldTypeDouble,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultDoubleFieldName).
					WithDataType(entity.FieldTypeDouble).
					WithDefaultValueString("2.6")
			},
			expectedErr: fmt.Sprintf("type (Double) of field (%s) is not equal to the type(DataType_VarChar) of default_value", common.DefaultDoubleFieldName),
		},
	}

	// 执行测试用例
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defField := tc.setupField()
			schema := entity.NewSchema().WithName("def_invalid_field").WithField(pkField).WithField(vecField).WithField(defField)
			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
			common.CheckErr(t, err, false, tc.expectedErr)
		})
	}
}

func TestDefaultValueOutOfRange(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	pkField := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.GenRandomString("vec", 3)).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	testCases := []struct {
		name        string
		fieldName   string
		dataType    entity.FieldType
		setupField  func() *entity.Field
		expectedErr string
	}{
		{
			name:      "int8 field with out_of_range default_value",
			fieldName: common.DefaultInt8FieldName,
			dataType:  entity.FieldTypeInt8,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultInt8FieldName).WithDataType(entity.FieldTypeInt8).WithDefaultValueInt(128)
			},
			expectedErr: "[128 out of range -128 <= value <= 127]",
		},
		{
			name:      "int16 field with out_of_range default_value",
			fieldName: common.DefaultInt16FieldName,
			dataType:  entity.FieldTypeInt16,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultInt16FieldName).WithDataType(entity.FieldTypeInt16).WithDefaultValueInt(-32769)
			},
			expectedErr: "[-32769 out of range -32768 <= value <= 32767]",
		},
	}

	// 执行测试用例
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defField := tc.setupField()
			schema := entity.NewSchema().WithName(common.GenRandomString("def", 5)).WithField(pkField).WithField(vecField).WithField(defField)
			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
			common.CheckErr(t, err, false, tc.expectedErr)
		})
	}
}

// test default value "" and insert ""
func TestDefaultValueVarcharEmpty(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithDefaultValue(""))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	coll, _ := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckFieldsDefaultValue(t, map[string]interface{}{
		common.DefaultVarcharFieldName: "",
	}, coll.Schema)

	// insert data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)))
	hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	expr := fmt.Sprintf("%s == ''", common.DefaultVarcharFieldName)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb/2, count)

	// insert varchar data: ""
	varcharValues := make([]string, common.DefaultNb/2)
	for i := 0; i < common.DefaultNb/2; i++ {
		varcharValues[i] = ""
	}
	columnOpt := hp.TNewColumnOptions().WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb)).
		WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData).TWithTextData(varcharValues))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), columnOpt)
	countRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ = countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb*3/2, count)
}

// test insert with nullableColumn into normal collection
func TestNullableDefaultInsertInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create normal default collection -> insert null values
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption())
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption())

	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeSparseVector, *hp.TNewDataOption().TWithSparseMaxLen(common.DefaultDim))
	varcharColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeVarChar, *hp.TNewDataOption().TWithValidData(validData))
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn).WithColumns(vecColumn).WithColumns(varcharColumn))
	common.CheckErr(t, err, false, "the length of valid_data of field(varchar) is wrong")
}

// test insert with part/all/not null -> query check
func TestNullableQuery(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	coll, _ := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckFieldsNullable(t, []string{common.DefaultVarcharFieldName}, coll.Schema)

	// insert data with part null, all null, all valid
	partNullValidData := make([]bool, common.DefaultNb)
	allNullValidData := make([]bool, common.DefaultNb)
	allValidData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		partNullValidData[i] = i%3 == 0
		allNullValidData[i] = false
		allValidData[i] = true
	}
	// [o, nb] -> 2*nb/3 null
	// [nb, 2*nb] -> all null
	// [2*nb, 3*nb] -> all valid
	// [3*nb, 4*nb] -> all valid
	for i, data := range [][]bool{partNullValidData, allNullValidData, allValidData} {
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)).
			WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(data).TWithStart(common.DefaultNb*i)))
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
		WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*3)))

	hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	exprCounts := []struct {
		expr  string
		count int64
	}{
		{expr: fmt.Sprintf("%s is null and (%d <= %s < %d)", common.DefaultVarcharFieldName, 0, common.DefaultInt64FieldName, common.DefaultNb), count: common.DefaultNb * 2 / 3},
		{expr: fmt.Sprintf("%s is null and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb, common.DefaultInt64FieldName, common.DefaultNb*2), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s is null and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb*2, common.DefaultInt64FieldName, common.DefaultNb*3), count: 0},
		{expr: fmt.Sprintf("%s is not null and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb*3, common.DefaultInt64FieldName, common.DefaultNb*4), count: common.DefaultNb},
	}
	for _, exprCount := range exprCounts {
		log.Info("exprCount", zap.String("expr", exprCount.expr), zap.Int64("count", exprCount.count))
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprCount.expr).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, exprCount.count, count)
	}
}

// test insert with part/all/not default value -> query check
func TestDefaultValueQuery(t *testing.T) {
	t.Skip("set defaultValue and insert with default column gets unexpected error, waiting for fix")
	for _, nullable := range [2]bool{false, true} {
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := hp.CreateDefaultMilvusClient(ctx, t)

		fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithDefaultValue("test").TWithNullable(nullable))
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
		coll, _ := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
		common.CheckFieldsDefaultValue(t, map[string]interface{}{
			common.DefaultVarcharFieldName: "test",
		}, coll.Schema)

		// insert data with part null, all null, all valid
		partNullValidData := make([]bool, common.DefaultNb)
		allNullValidData := make([]bool, common.DefaultNb)
		allValidData := make([]bool, common.DefaultNb)
		for i := 0; i < common.DefaultNb; i++ {
			partNullValidData[i] = i%2 == 0
			allNullValidData[i] = false
			allValidData[i] = true
		}
		// [o, nb] -> nb/2 default value
		// [nb, 2*nb] -> all default value
		// [2*nb, 3*nb] -> all valid
		// [3*nb, 4*nb] -> all valid
		for i, data := range [][]bool{partNullValidData, allNullValidData, allValidData} {
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
				WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)).
				WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(data)))
		}
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*3)))

		hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		exprCounts := []struct {
			expr  string
			count int64
		}{
			{expr: fmt.Sprintf("%s == 'test' and (%d <= %s < %d)", common.DefaultVarcharFieldName, 0, common.DefaultInt64FieldName, common.DefaultNb), count: common.DefaultNb / 2},
			{expr: fmt.Sprintf("%s == 'test' and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb, common.DefaultInt64FieldName, common.DefaultNb*2), count: common.DefaultNb},
			{expr: fmt.Sprintf("%s == 'test' and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb*2, common.DefaultInt64FieldName, common.DefaultNb*3), count: 0},
			{expr: fmt.Sprintf("%s == 'test' and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb*3, common.DefaultInt64FieldName, common.DefaultNb*4), count: 0},
			{expr: fmt.Sprintf("%s == 'test'", common.DefaultVarcharFieldName), count: common.DefaultNb * 3 / 2},
		}
		for _, exprCount := range exprCounts {
			log.Info("exprCount", zap.String("expr", exprCount.expr), zap.Int64("count", exprCount.count))
			countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprCount.expr).WithOutputFields(common.QueryCountFieldName))
			common.CheckErr(t, err, true)
			count, _ := countRes.Fields[0].GetAsInt64(0)
			require.EqualValues(t, exprCount.count, count)
		}
	}
}

// clustering-key nullable
func TestNullableClusteringKey(t *testing.T) {
	// test clustering key nullable
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true).TWithIsClusteringKey(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))

	// insert with valid data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	for i := 0; i < 5; i++ {
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)).
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)))
	}

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	expr := fmt.Sprintf("%s == '1'", common.DefaultVarcharFieldName)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 5, count)
}

// partition-key nullable
func TestDefaultValuePartitionKey(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithDefaultValue("parkey").TWithIsPartitionKey(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong), hp.TWithNullablePartitions(3))

	// insert with valid data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	for i := 0; i < 3; i++ {
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)).
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)))
	}

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	expr := "varchar like 'parkey%'"
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb*3/2, count)
}

func TestNullableGroubBy(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))

	// insert with valid data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		if i > 200 {
			validData[i] = true
		}
	}
	for i := 0; i < 10; i++ {
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)).
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)))
	}

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// nullable field as group by field
	queryVec := hp.GenSearchVectors(2, common.DefaultDim, entity.FieldTypeSparseVector)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithGroupByField(common.DefaultVarcharFieldName))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, 2, common.DefaultLimit)
}

func TestNullableSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))

	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
		WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)))

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with nullable expr and output nullable / dynamic field
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)
	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithFilter(expr).WithOutputFields(common.DefaultVarcharFieldName, common.DefaultDynamicFieldName))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	common.CheckOutputFields(t, []string{common.DefaultVarcharFieldName, common.DefaultDynamicFieldName}, searchRes[0].Fields)

	for _, field := range searchRes[0].Fields {
		if field.Name() == common.DefaultVarcharFieldName {
			for i := 0; i < field.Len(); i++ {
				isNull, err := field.IsNull(i)
				common.CheckErr(t, err, true)
				require.True(t, isNull)
			}
		}
	}
}

func TestDefaultValueSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithDefaultValue("test").TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))

	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
		WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)))

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with nullable expr and output nullable / dynamic field
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)
	expr := fmt.Sprintf("%s == 'test'", common.DefaultVarcharFieldName)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithFilter(expr).WithOutputFields(common.DefaultVarcharFieldName, common.DefaultDynamicFieldName))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	common.CheckOutputFields(t, []string{common.DefaultVarcharFieldName, common.DefaultDynamicFieldName}, searchRes[0].Fields)

	for _, field := range searchRes[0].Fields {
		if field.Name() == common.DefaultVarcharFieldName {
			for i := 0; i < field.Len(); i++ {
				fieldData, _ := field.GetAsString(i)
				require.EqualValues(t, "test", fieldData)
			}
		}
	}
}

// test nullable fields in all scalar index
func TestNullableAutoScalarIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create fields: pk + floatVec + all nullable scalar fields
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	schema := entity.NewSchema().WithName(common.GenRandomString("nullable_default_value", 5)).WithField(pkField).WithField(vecField)

	// create collection with all supported nullable fields
	expNullableFields := make([]string, 0)
	for _, fieldType := range hp.GetAllNullableFieldType() {
		nullableField := entity.NewField().WithName(common.GenRandomString("null", 5)).WithDataType(fieldType).WithNullable(true)
		if fieldType == entity.FieldTypeVarChar {
			nullableField.WithMaxLength(common.TestMaxLen)
		}
		if fieldType == entity.FieldTypeArray {
			nullableField.WithElementType(entity.FieldTypeInt64).WithMaxCapacity(common.TestCapacity)
		}
		schema.WithField(nullableField)
		expNullableFields = append(expNullableFields, nullableField.Name)
	}

	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	// describe collection and check nullable fields
	descCollection, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	common.CheckFieldsNullable(t, expNullableFields, descCollection.Schema)

	// insert data with nullable column
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 1
	}
	columnOpt := hp.TNewColumnOptions()
	for _, name := range expNullableFields {
		columnOpt = columnOpt.WithColumnOption(name, hp.TNewDataOption().TWithValidData(validData))
	}
	for i := 0; i < 3; i++ {
		columnOpt = columnOpt.WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i))
		hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), columnOpt)
	}
	prepare := hp.CollPrepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create auto scalar index for all nullable fields
	indexOpt := hp.TNewIndexParams(schema)
	for _, name := range expNullableFields {
		indexOpt = indexOpt.TWithFieldIndex(map[string]index.Index{name: index.NewAutoIndex(entity.L2)})
	}
	prepare.CreateIndex(ctx, t, mc, indexOpt)
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query with null expr
	for _, nullField := range expNullableFields {
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s is null", nullField)).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, common.DefaultNb*3/2, count)
	}
}

func TestNullableUpsert(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query is null return empty result
	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 0, count)

	// upsert with part null, all null, all valid
	partNullValidData := make([]bool, common.DefaultNb)
	allNullValidData := make([]bool, common.DefaultNb)
	allValidData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		partNullValidData[i] = i%2 == 0
		allNullValidData[i] = false
		allValidData[i] = true
	}
	validCount := []int{common.DefaultNb / 2, 0, common.DefaultNb}
	expNullCount := []int{common.DefaultNb / 2, common.DefaultNb, 0}

	pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeSparseVector, *hp.TNewDataOption())

	for i, validData := range [][]bool{partNullValidData, allNullValidData, allValidData} {
		varcharData := make([]string, validCount[i])
		for j := 0; j < validCount[i]; j++ {
			varcharData[j] = "aaa"
		}
		nullVarcharColumn, err := column.NewNullableColumnVarChar(common.DefaultVarcharFieldName, varcharData, validData)
		common.CheckErr(t, err, true)

		upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn).WithColumns(vecColumn).WithColumns(nullVarcharColumn))
		common.CheckErr(t, err, true)
		require.EqualValues(t, common.DefaultNb, upsertRes.UpsertCount)

		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, expNullCount[i], count)
	}
}

func TestNullableDelete(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
		WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete null
	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)
	deleteRes, err := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(expr))
	common.CheckErr(t, err, true)
	require.EqualValues(t, common.DefaultNb/2, deleteRes.DeleteCount)

	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 0, count)
}

// TODO: test rows with nullable and default value
func TestNullableRows(t *testing.T) {
	t.Skip("Waiting for rows-inserts to support nullable and defaultValue")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption().TWithEnableDynamicField(false),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	rows := hp.GenInt64VarcharSparseRows(common.DefaultNb, false, false, *hp.TNewDataOption().TWithValidData(validData))
	insertRes, err := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, common.DefaultNb, insertRes.InsertCount)

	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb/2, count)
}
