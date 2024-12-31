package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

var prefix = "collection"

// test create default floatVec and binaryVec collection
func TestCreateCollection(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	for _, collectionFieldsType := range []hp.CollectionFieldsType{hp.Int64Vec, hp.VarcharBinary, hp.Int64VarcharSparseVec, hp.AllFields} {
		fields := hp.FieldsFact.GenFieldsForCollection(collectionFieldsType, hp.TNewFieldsOption())
		schema := hp.GenSchema(hp.TNewSchemaOption().TWithFields(fields))
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
		common.CheckErr(t, err, true)

		// has collections and verify
		has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(schema.CollectionName))
		common.CheckErr(t, err, true)
		require.True(t, has)

		// list collections and verify
		collections, err := mc.ListCollections(ctx, client.NewListCollectionOption())
		common.CheckErr(t, err, true)
		require.Contains(t, collections, schema.CollectionName)
	}
}

func TestCreateAutoIdCollectionField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)
	varcharField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithIsAutoID(true).WithMaxLength(common.MaxLength)
	for _, pkField := range []*entity.Field{int64Field, varcharField} {
		// pk field with name
		collName := common.GenRandomString(prefix, 6)
		schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// verify field name
		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		common.CheckErr(t, err, true)
		require.True(t, coll.Schema.AutoID)
		require.True(t, coll.Schema.Fields[0].AutoID)

		// insert
		vecColumn := hp.GenColumnData(common.DefaultNb, vecField.DataType, *hp.TNewDataOption())
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, vecColumn))
		common.CheckErr(t, err, true)
	}
}

// create collection and specify shard num
func TestCreateCollectionShards(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)
	for _, shard := range []int32{-1, 0, 2, 16} {
		// pk field with name
		collName := common.GenRandomString(prefix, 6)
		schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithShardNum(shard))
		common.CheckErr(t, err, true)

		// verify field name
		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		common.CheckErr(t, err, true)
		if shard < 1 {
			shard = 1
		}
		require.Equal(t, shard, coll.ShardNum)
	}
}

// test create auto collection with schema
func TestCreateAutoIdCollectionSchema(t *testing.T) {
	t.Skip("waiting for valid AutoId from schema params")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	for _, pkFieldType := range []entity.FieldType{entity.FieldTypeVarChar, entity.FieldTypeInt64} {
		pkField := entity.NewField().WithName("pk").WithDataType(pkFieldType).WithIsPrimaryKey(true).WithMaxLength(common.MaxLength)

		// pk field with name
		schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithAutoID(true)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// verify field name
		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		common.CheckErr(t, err, true)
		log.Info("schema autoID", zap.Bool("schemaAuto", coll.Schema.AutoID))
		log.Info("field autoID", zap.Bool("fieldAuto", coll.Schema.Fields[0].AutoID))

		// insert
		vecColumn := hp.GenColumnData(common.DefaultNb, vecField.DataType, *hp.TNewDataOption())
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, vecColumn))
		common.CheckErr(t, err, false, "field pk not passed")
	}
}

// test create auto collection with collection option
func TestCreateAutoIdCollection(t *testing.T) {
	t.Skip("waiting for valid AutoId from collection option")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	for _, pkFieldType := range []entity.FieldType{entity.FieldTypeVarChar, entity.FieldTypeInt64} {
		pkField := entity.NewField().WithName("pk").WithDataType(pkFieldType).WithIsPrimaryKey(true).WithMaxLength(common.MaxLength)

		// pk field with name
		schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithAutoID(true))
		common.CheckErr(t, err, true)

		// verify field name
		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		common.CheckErr(t, err, true)
		log.Info("schema autoID", zap.Bool("schemaAuto", coll.Schema.AutoID))
		log.Info("field autoID", zap.Bool("fieldAuto", coll.Schema.Fields[0].AutoID))

		// insert
		vecColumn := hp.GenColumnData(common.DefaultNb, vecField.DataType, *hp.TNewDataOption())
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, vecColumn))
		common.CheckErr(t, err, false, "field pk not passed")
	}
}

func TestCreateJsonCollection(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	pkField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(common.MaxLength)
	jsonField := entity.NewField().WithName(common.DefaultJSONFieldName).WithDataType(entity.FieldTypeJSON)

	// pk field with name
	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithField(jsonField)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// verify field name
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	require.True(t, has)
}

func TestCreateArrayCollections(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	pkField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(common.MaxLength)

	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)

	for _, eleType := range hp.GetAllArrayElementType() {
		arrayField := entity.NewField().WithName(hp.GetFieldNameByElementType(eleType)).WithDataType(entity.FieldTypeArray).WithElementType(eleType).WithMaxCapacity(common.MaxCapacity)
		if eleType == entity.FieldTypeVarChar {
			arrayField.WithMaxLength(common.MaxLength)
		}
		schema.WithField(arrayField)
	}

	// pk field with name
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// verify field name
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	require.True(t, has)
}

// test create collection with partition key not supported field type
func TestCreateCollectionPartitionKey(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	for _, fieldType := range []entity.FieldType{entity.FieldTypeVarChar, entity.FieldTypeInt64} {
		partitionKeyField := entity.NewField().WithName("par_key").WithDataType(fieldType).WithIsPartitionKey(true).WithMaxLength(common.TestMaxLen)
		collName := common.GenRandomString(prefix, 6)
		schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField).WithField(partitionKeyField)

		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		common.CheckErr(t, err, true)

		for _, field := range coll.Schema.Fields {
			if field.Name == "par_key" {
				require.True(t, field.IsPartitionKey)
			}
		}

		// verify partitions
		partitions, err := mc.ListPartitions(ctx, client.NewListPartitionOption(collName))
		require.Len(t, partitions, common.DefaultPartitionNum)
		common.CheckErr(t, err, true)
	}
}

// test create partition key collection WithPartitionNum
func TestCreateCollectionPartitionKeyNumPartition(t *testing.T) {
	t.Skip("Waiting for WithPartitionNum")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	partitionKeyField := entity.NewField().WithName("par_key").WithDataType(entity.FieldTypeInt64).WithIsPartitionKey(true)
	t.Parallel()

	for _, numPartition := range []int64{1, 128, 64, 4096} {
		collName := common.GenRandomString(prefix, 6)
		schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField).WithField(partitionKeyField)

		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// verify partitions num
		partitions, err := mc.ListPartitions(ctx, client.NewListPartitionOption(collName))
		require.Len(t, partitions, int(numPartition))
		common.CheckErr(t, err, true)
	}
}

func TestCreateCollectionDynamicSchema(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	pkField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(common.MaxLength)

	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithDynamicFieldEnabled(true)
	// pk field with name
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// verify field name
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	require.True(t, coll.Schema.EnableDynamicField)

	// insert dynamic
	columnOption := *hp.TNewDataOption()
	varcharColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeVarChar, columnOption)
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, columnOption)
	dynamicData := hp.GenDynamicColumnData(0, common.DefaultNb)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, varcharColumn, vecColumn).WithColumns(dynamicData...))
	common.CheckErr(t, err, true)
}

func TestCreateCollectionDynamic(t *testing.T) {
	t.Skip("waiting for dynamicField alignment")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	pkField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(common.MaxLength)

	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)
	// pk field with name
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithDynamicSchema(true))
	common.CheckErr(t, err, true)

	// verify field name
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	require.True(t, has)

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	log.Info("collection dynamic", zap.Bool("collectionSchema", coll.Schema.EnableDynamicField))
	common.CheckErr(t, err, true)
	// require.True(t, coll.Schema.Fields[0].IsDynamic)

	// insert dynamic
	columnOption := *hp.TNewDataOption()
	varcharColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeVarChar, columnOption)
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, columnOption)
	dynamicData := hp.GenDynamicColumnData(0, common.DefaultNb)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, varcharColumn, vecColumn).WithColumns(dynamicData...))
	common.CheckErr(t, err, false, "field dynamicNumber does not exist")
}

func TestCreateCollectionAllFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	schema := entity.NewSchema().WithName(collName)

	// gen all fields except sparse vector
	fields := hp.FieldsFactory{}.GenFieldsForCollection(hp.AllFields, hp.TNewFieldsOption())
	for _, field := range fields {
		schema.WithField(field)
	}

	// pk field with name
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// verify field name
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	require.True(t, has)
}

func TestCreateCollectionSparseVector(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	sparseVecField := entity.NewField().WithName(common.DefaultSparseVecFieldName).WithDataType(entity.FieldTypeSparseVector)
	pkField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(common.MaxLength)

	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(sparseVecField)
	// pk field with name
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithDynamicSchema(true))
	common.CheckErr(t, err, true)

	// verify field name
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	require.True(t, has)
}

func TestCreateCollectionWithValidFieldName(t *testing.T) {
	t.Parallel()
	// connect
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection with valid field name
	for _, name := range common.GenValidNames() {
		collName := common.GenRandomString(prefix, 6)

		// pk field with name
		pkField := entity.NewField().WithName(name).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
		vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
		schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// verify field name
		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		common.CheckErr(t, err, true)
		require.Equal(t, name, coll.Schema.Fields[0].Name)
	}
}

func genDefaultSchema() *entity.Schema {
	int64Pk := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	varchar := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(common.TestMaxLen)
	floatVec := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	binaryVec := entity.NewField().WithName(common.DefaultBinaryVecFieldName).WithDataType(entity.FieldTypeBinaryVector).WithDim(common.DefaultDim)

	schema := entity.NewSchema().WithField(int64Pk).WithField(varchar).WithField(floatVec).WithField(binaryVec)
	return schema
}

// create collection with valid name
func TestCreateCollectionWithValidName(t *testing.T) {
	t.Parallel()
	// connect
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, name := range common.GenValidNames() {
		schema := genDefaultSchema().WithName(name)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema))
		common.CheckErr(t, err, true)

		collections, err := mc.ListCollections(ctx, client.NewListCollectionOption())
		common.CheckErr(t, err, true)
		require.Contains(t, collections, name)

		err = mc.DropCollection(ctx, client.NewDropCollectionOption(name))
		common.CheckErr(t, err, true)
	}
}

// create collection with invalid field name
func TestCreateCollectionWithInvalidFieldName(t *testing.T) {
	t.Parallel()
	// connect
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection with invalid field name
	for _, invalidName := range common.GenInvalidNames() {
		log.Debug("TestCreateCollectionWithInvalidFieldName", zap.String("fieldName", invalidName))
		pkField := entity.NewField().WithName(invalidName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
		vecField := entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(128)
		schema := entity.NewSchema().WithName("aaa").WithField(pkField).WithField(vecField)
		collOpt := client.NewCreateCollectionOption("aaa", schema)

		err := mc.CreateCollection(ctx, collOpt)
		common.CheckErr(t, err, false, "field name should not be empty",
			"The first character of a field name must be an underscore or letter",
			"Field name cannot only contain numbers, letters, and underscores",
			"The length of a field name must be less than 255 characters",
			"Field name can only contain numbers, letters, and underscores")
	}
}

// create collection with invalid collection name: invalid str, schemaName isn't equal to collectionName, schema name is empty
func TestCreateCollectionWithInvalidCollectionName(t *testing.T) {
	t.Parallel()
	// connect
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection and schema no name
	schema := genDefaultSchema()
	err2 := mc.CreateCollection(ctx, client.NewCreateCollectionOption("", schema))
	common.CheckErr(t, err2, false, "collection name should not be empty")

	// create collection with invalid schema name
	for _, invalidName := range common.GenInvalidNames() {
		log.Debug("TestCreateCollectionWithInvalidCollectionName", zap.String("collectionName", invalidName))

		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(invalidName, schema))
		common.CheckErr(t, err, false, "collection name should not be empty",
			"the first character of a collection name must be an underscore or letter",
			"collection name can only contain numbers, letters and underscores",
			fmt.Sprintf("the length of a collection name must be less than %d characters", common.MaxCollectionNameLen))
	}
}

// create collection missing pk field or vector field
func TestCreateCollectionInvalidFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	type invalidFieldsStruct struct {
		fields []*entity.Field
		errMsg string
	}
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	pkField2 := entity.NewField().WithName("pk").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	varcharField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar)
	stringField := entity.NewField().WithName("str").WithDataType(entity.FieldTypeString)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	noneField := entity.NewField().WithName("none").WithDataType(entity.FieldTypeNone)
	invalidFields := []invalidFieldsStruct{
		{fields: []*entity.Field{pkField}, errMsg: "schema does not contain vector field"},
		{fields: []*entity.Field{vecField}, errMsg: "primary key is not specified"},
		{fields: []*entity.Field{pkField, pkField2, vecField}, errMsg: "there are more than one primary key"},
		{fields: []*entity.Field{pkField, vecField, noneField}, errMsg: "data type None is not valid"},
		{fields: []*entity.Field{pkField, vecField, stringField}, errMsg: "string data type not supported yet, please use VarChar type instead"},
		{fields: []*entity.Field{pkField, vecField, varcharField}, errMsg: "type param(max_length) should be specified for varChar field"},
	}

	collName := common.GenRandomString(prefix, 6)
	for _, invalidField := range invalidFields {
		schema := entity.NewSchema().WithName(collName)
		for _, field := range invalidField.fields {
			schema.WithField(field)
		}
		collOpt := client.NewCreateCollectionOption(collName, schema)
		err := mc.CreateCollection(ctx, collOpt)
		common.CheckErr(t, err, false, invalidField.errMsg)
	}
}

// create autoID or not collection with non-int64 and non-varchar field
func TestCreateCollectionInvalidAutoPkField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection with autoID true or not
	collName := common.GenRandomString(prefix, 6)

	for _, autoId := range [2]bool{true, false} {
		vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
		// pk field type: non-int64 and non-varchar
		for _, fieldType := range hp.GetInvalidPkFieldType() {
			invalidPkField := entity.NewField().WithName("pk").WithDataType(fieldType).WithIsPrimaryKey(true)
			schema := entity.NewSchema().WithName(collName).WithField(vecField).WithField(invalidPkField).WithAutoID(autoId)
			errNonInt64Field := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
			common.CheckErr(t, errNonInt64Field, false, "the data type of primary key should be Int64 or VarChar")
		}
	}
}

// test create collection with duplicate field name
func TestCreateCollectionDuplicateField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// duplicate field
	pkField := entity.NewField().WithName("id").WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true)
	pkField2 := entity.NewField().WithName("id").WithDataType(entity.FieldTypeVarChar)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	// two vector fields have same name
	collName := common.GenRandomString(prefix, 6)
	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithField(vecField)
	errDupField := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, errDupField, false, "duplicated field name")

	// two named "id" fields, one is pk field and other is scalar field
	schema2 := entity.NewSchema().WithName(collName).WithField(pkField).WithField(pkField2).WithField(vecField).WithAutoID(true)
	errDupField2 := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema2))
	common.CheckErr(t, errDupField2, false, "duplicated field name")
}

// test create collection with partition key not supported field type
func TestCreateCollectionInvalidPartitionKeyType(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	collName := common.GenRandomString(prefix, 6)

	for _, fieldType := range hp.GetInvalidPartitionKeyFieldType() {
		log.Debug("TestCreateCollectionInvalidPartitionKeyType", zap.Any("partitionKeyFieldType", fieldType))
		partitionKeyField := entity.NewField().WithName("parKey").WithDataType(fieldType).WithIsPartitionKey(true)
		if fieldType == entity.FieldTypeArray {
			partitionKeyField.WithElementType(entity.FieldTypeInt64)
		}
		schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField).WithField(partitionKeyField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, false, "the data type of partition key should be Int64 or VarChar")
	}
}

// partition key field cannot be primary field, d can only be one partition key field
func TestCreateCollectionPartitionKeyPk(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsPartitionKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	collName := common.GenRandomString(prefix, 6)

	schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "the partition key field must not be primary field")
}

// can only be one partition key field
func TestCreateCollectionPartitionKeyNum(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	collName := common.GenRandomString(prefix, 6)

	pkField1 := entity.NewField().WithName("pk_1").WithDataType(entity.FieldTypeInt64).WithIsPartitionKey(true)
	pkField2 := entity.NewField().WithName("pk_2").WithDataType(entity.FieldTypeVarChar).WithMaxLength(common.TestMaxLen).WithIsPartitionKey(true)

	schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField).WithField(pkField1).WithField(pkField2)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "there are more than one partition key")
}

func TestPartitionKeyInvalidNumPartition(t *testing.T) {
	t.Skip("Waiting for num partition")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// prepare field and schema
	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	pkField1 := entity.NewField().WithName("partitionKeyField").WithDataType(entity.FieldTypeInt64).WithIsPartitionKey(true)

	// schema
	collName := common.GenRandomString(prefix, 6)
	schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField).WithField(pkField1)
	invalidNumPartitionStruct := []struct {
		numPartitions int64
		errMsg        string
	}{
		{common.MaxPartitionNum + 1, "exceeds max configuration (1024)"},
		{-1, "the specified partitions should be greater than 0 if partition key is used"},
	}
	for _, npStruct := range invalidNumPartitionStruct {
		// create collection with num partitions
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, false, npStruct.errMsg)
	}
}

// test create collection with multi auto id
func TestCreateCollectionMultiAutoId(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	schema := entity.NewSchema().WithField(
		entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).WithField(
		entity.NewField().WithName("dupInt").WithDataType(entity.FieldTypeInt64).WithIsAutoID(true)).WithField(
		entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim),
	).WithName(collName)
	errMultiAuto := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, errMultiAuto, false, "only one field can speficy AutoID with true")
}

// test create collection with different autoId between pk field and schema
func TestCreateCollectionInconsistentAutoId(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, autoId := range []bool{true, false} {
		log.Debug("TestCreateCollectionInconsistentAutoId", zap.Bool("autoId", autoId))
		collName := common.GenRandomString(prefix, 6)
		// field and schema have opposite autoID
		schema := entity.NewSchema().WithField(
			entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(autoId)).WithField(
			entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim),
		).WithName(collName).WithAutoID(!autoId)

		// create collection
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// describe collection
		coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
		common.CheckErr(t, err, true)
		require.EqualValues(t, autoId, coll.Schema.AutoID)
		for _, field := range coll.Schema.Fields {
			if field.Name == common.DefaultInt64FieldName {
				require.EqualValues(t, autoId, coll.Schema.Fields[0].AutoID)
			}
		}
	}
}

// create collection with field or schema description
func TestCreateCollectionDescription(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// gen field with description
	pkDesc := "This is pk field"
	schemaDesc := "This is schema"
	collName := common.GenRandomString(prefix, 6)

	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithDescription(pkDesc)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithDescription(schemaDesc)

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.EqualValues(t, schemaDesc, coll.Schema.Description)
	for _, field := range coll.Schema.Fields {
		if field.Name == common.DefaultInt64FieldName {
			require.Equal(t, pkDesc, field.Description)
		} else {
			require.Empty(t, field.Description)
		}
	}
}

// test invalid dim of binary field
func TestCreateBinaryCollectionInvalidDim(t *testing.T) {
	t.Parallel()
	type invalidDimStruct struct {
		dim    int64
		errMsg string
	}

	invalidDims := []invalidDimStruct{
		{dim: 10, errMsg: "should be multiple of 8"},
		{dim: 0, errMsg: "should be in range 2 ~ 32768"},
		{dim: 1, errMsg: "should be in range 2 ~ 32768"},
		{dim: common.MaxDim * 9, errMsg: "binary vector dimension should be in range 2 ~ 262144"},
		{dim: common.MaxDim*8 + 1, errMsg: "binary vector dimension should be multiple of 8"},
	}

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, invalidDim := range invalidDims {
		log.Debug("TestCreateBinaryCollectionInvalidDim", zap.Int64("dim", invalidDim.dim))
		collName := common.GenRandomString(prefix, 6)
		// field and schema have opposite autoID
		schema := entity.NewSchema().WithField(
			entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).WithField(
			entity.NewField().WithName(common.DefaultBinaryVecFieldName).WithDataType(entity.FieldTypeBinaryVector).WithDim(invalidDim.dim),
		).WithName(collName)

		// create collection
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, false, invalidDim.errMsg)
	}
}

// test invalid dim of float vector
func TestCreateFloatCollectionInvalidDim(t *testing.T) {
	t.Parallel()
	type invalidDimStruct struct {
		dim    string
		errMsg string
	}

	invalidDims := []invalidDimStruct{
		{dim: "0", errMsg: "should be in range 2 ~ 32768"},
		{dim: "1", errMsg: "should be in range 2 ~ 32768"},
		{dim: "", errMsg: "invalid syntax"},
		{dim: "中文", errMsg: "invalid syntax"},
		{dim: "%$#", errMsg: "invalid syntax"},
		{dim: fmt.Sprintf("%d", common.MaxDim+1), errMsg: "float vector dimension should be in range 2 ~ 32768"},
	}

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, vecType := range []entity.FieldType{entity.FieldTypeFloatVector, entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector} {
		for _, invalidDim := range invalidDims {
			log.Debug("TestCreateBinaryCollectionInvalidDim", zap.String("dim", invalidDim.dim))
			collName := common.GenRandomString(prefix, 6)

			schema := entity.NewSchema().WithField(
				entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).WithField(
				entity.NewField().WithName("pk").WithDataType(vecType).WithTypeParams(entity.TypeParamDim, invalidDim.dim),
			).WithName(collName)

			// create collection
			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
			common.CheckErr(t, err, false, invalidDim.errMsg)
		}
	}
}

func TestCreateVectorWithoutDim(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	collName := common.GenRandomString(prefix, 6)

	schema := entity.NewSchema().WithField(
		entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).WithField(
		entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector),
	).WithName(collName)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "dimension is not defined in field type params, check type param `dim` for vector field")
}

// specify dim for sparse vector -> error
func TestCreateCollectionSparseVectorWithDim(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	collName := common.GenRandomString(prefix, 6)

	schema := entity.NewSchema().WithField(
		entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).WithField(
		entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector).WithDim(common.DefaultDim),
	).WithName(collName)

	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "dim should not be specified for sparse vector field sparse")
}

func TestCreateArrayFieldInvalidCapacity(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)

	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	arrayField := entity.NewField().WithName(common.DefaultArrayFieldName).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeFloat)
	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithField(arrayField)

	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "type param(max_capacity) should be specified for array field")

	// invalid Capacity
	for _, invalidCapacity := range []int64{-1, 0, common.MaxCapacity + 1} {
		arrayField.WithMaxCapacity(invalidCapacity)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, false, "the maximum capacity specified for a Array should be in (0, 4096]")
	}
}

// test create collection varchar array with invalid max length
func TestCreateVarcharArrayInvalidLength(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)

	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	arrayVarcharField := entity.NewField().WithName(common.DefaultArrayFieldName).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).WithMaxCapacity(100)
	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithField(arrayVarcharField)

	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "type param(max_length) should be specified for varChar field")
	// invalid Capacity
	for _, invalidLength := range []int64{-1, 0, common.MaxLength + 1} {
		arrayVarcharField.WithMaxLength(invalidLength)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, false, "the maximum length specified for a VarChar should be in (0, 1048576]")
	}
}

// test create collection varchar array with invalid max length
func TestCreateVarcharInvalidLength(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)

	varcharField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	schema := entity.NewSchema().WithName(collName).WithField(varcharField).WithField(vecField)
	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, "type param(max_length) should be specified for varChar field")
	// invalid Capacity
	for _, invalidLength := range []int64{-1, 0, common.MaxLength + 1} {
		varcharField.WithMaxLength(invalidLength)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, false, "the maximum length specified for a VarChar should be in (0, 1048576]")
	}
}

func TestCreateArrayNotSupportedFieldType(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	// not supported ElementType: Array, Json, FloatVector, BinaryVector
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	for _, fieldType := range []entity.FieldType{entity.FieldTypeArray, entity.FieldTypeJSON, entity.FieldTypeBinaryVector, entity.FieldTypeFloatVector} {
		field := entity.NewField().WithName("array").WithDataType(entity.FieldTypeArray).WithElementType(fieldType)
		schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithField(field)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, false, fmt.Sprintf("element type %s is not supported", fieldType.Name()))
	}
}

// the num of vector fields > default limit=4
func TestCreateMultiVectorExceed(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	schema := entity.NewSchema().WithName(collName).WithField(pkField)
	for i := 0; i < common.MaxVectorFieldNum+1; i++ {
		vecField := entity.NewField().WithName(fmt.Sprintf("vec_%d", i)).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
		schema.WithField(vecField)
	}
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, false, fmt.Sprintf("maximum vector field's number should be limited to %d", common.MaxVectorFieldNum))
}

// func TestCreateCollection(t *testing.T) {}
func TestCreateCollectionInvalidShards(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)
	for _, shard := range []int32{common.MaxShardNum + 1} {
		// pk field with name
		collName := common.GenRandomString(prefix, 6)
		schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithShardNum(shard))
		common.CheckErr(t, err, false, fmt.Sprintf("maximum shards's number should be limited to %d", common.MaxShardNum))
	}
}

func TestCreateCollectionInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	type mSchemaErr struct {
		schema *entity.Schema
		errMsg string
	}
	vecField := entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(8)
	mSchemaErrs := []mSchemaErr{
		{schema: nil, errMsg: "schema does not contain vector field"},
		{schema: entity.NewSchema().WithField(vecField), errMsg: "primary key is not specified"}, // no pk field
		{schema: entity.NewSchema().WithField(vecField).WithField(entity.NewField()), errMsg: "primary key is not specified"},
		{schema: entity.NewSchema().WithField(vecField).WithField(entity.NewField().WithIsPrimaryKey(true)), errMsg: "the data type of primary key should be Int64 or VarChar"},
		{schema: entity.NewSchema().WithField(vecField).WithField(entity.NewField().WithIsPrimaryKey(true).WithDataType(entity.FieldTypeVarChar)), errMsg: "field name should not be empty"},
	}
	for _, mSchema := range mSchemaErrs {
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, mSchema.schema))
		common.CheckErr(t, err, false, mSchema.errMsg)
	}
}
