package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// create -> add field -> index -> load -> insert -> query/search
func TestAddCollectionField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	// Test cases for different defaultValue and filter
	testCases := []struct {
		name         string
		defaultValue int64
		filter       string
	}{
		{
			name:         "defaultValueNone",
			defaultValue: -1,
			filter:       fmt.Sprintf("%s is null", common.DefaultNewField),
		},
		{
			name:         "defaultValue100",
			defaultValue: 100,
			filter:       fmt.Sprintf("%s == 100", common.DefaultNewField),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// create collection option: WithConsistencyLevel Strong,
			collName := common.GenRandomString("addfield", 6)
			err := mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
			common.CheckErr(t, err, true)

			// verify collection option
			coll, _ := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
			require.Equal(t, entity.FieldTypeInt64, coll.Schema.PKField().DataType)

			// the field to add
			newField := entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(true)
			if tc.defaultValue != -1 {
				newField.WithDefaultValueLong(tc.defaultValue)
			}
			err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collName, newField))

			common.CheckErr(t, err, true)

			// load -> insert
			prepare, _ := hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(coll.Schema), hp.TNewDataOption())
			prepare.FlushData(ctx, t, mc, collName)

			countRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(tc.filter).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))

			common.CheckErr(t, err, true)
			count, _ := countRes.Fields[0].GetAsInt64(0)
			require.EqualValues(t, common.DefaultNb, count)

			vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
			resSearch, err := mc.Search(ctx, client.NewSearchOption(collName, common.DefaultLimit, vectors).WithFilter(tc.filter).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)
			common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
		})
	}
}

func TestAlterCollectionSchemaFunctionField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	previousStorageV3, err := hp.AlterServerConfig("common.storage.useLoonFFI", "true")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = hp.AlterServerConfig("common.storage.useLoonFFI", previousStorageV3)
	})

	previousSchemaBump, err := hp.AlterServerConfig("dataCoord.compaction.bumpSchemaVersion.enabled", "true")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = hp.AlterServerConfig("dataCoord.compaction.bumpSchemaVersion.enabled", previousSchemaBump)
	})

	collectionName := common.GenRandomString("alter_function_field", 6)
	schema := entity.NewSchema().
		WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).
		WithField(entity.NewField().WithName("text").WithDataType(entity.FieldTypeVarChar).WithMaxLength(1024).WithEnableAnalyzer(true).WithAnalyzerParams(map[string]any{"tokenizer": "standard"})).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(8))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))

	outputField := entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector)
	function := entity.NewFunction().
		WithName("bm25").
		WithType(entity.FunctionTypeBM25).
		WithInputFields("text").
		WithOutputFields("sparse")
	require.NoError(t, mc.AddFunctionField(ctx, client.NewAddFunctionFieldOption(collectionName, outputField, function, index.NewSparseInvertedIndex(entity.BM25, 0.2)).WithIndexName("sparse_idx")))

	collection, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collectionName))
	require.NoError(t, err)
	require.Contains(t, mapFieldsByName(collection.Schema.Fields), "sparse")
	require.Contains(t, mapFunctionsByName(collection.Schema.Functions), "bm25")

	require.NoError(t, mc.DropFunctionField(ctx, client.NewDropFunctionFieldOption(collectionName, "bm25")))
	collection, err = mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collectionName))
	require.NoError(t, err)
	require.NotContains(t, mapFieldsByName(collection.Schema.Fields), "sparse")
	require.NotContains(t, mapFunctionsByName(collection.Schema.Functions), "bm25")

	field := entity.NewField().WithName("status").WithDataType(entity.FieldTypeInt32).WithNullable(true)
	require.NoError(t, mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collectionName, field)))
	require.NoError(t, mc.DropCollectionField(ctx, client.NewDropCollectionFieldOption(collectionName, "status")))
	collection, err = mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collectionName))
	require.NoError(t, err)
	require.NotContains(t, mapFieldsByName(collection.Schema.Fields), "status")
}

func mapFieldsByName(fields []*entity.Field) map[string]*entity.Field {
	result := make(map[string]*entity.Field, len(fields))
	for _, field := range fields {
		result[field.Name] = field
	}
	return result
}

func mapFunctionsByName(functions []*entity.Function) map[string]*entity.Function {
	result := make(map[string]*entity.Function, len(functions))
	for _, function := range functions {
		result[function.Name] = function
	}
	return result
}

func TestAddCollectionStructField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{
		Address:  hp.GetAddr(),
		Username: hp.GetUser(),
		Password: hp.GetPassword(),
	})

	collName := common.GenRandomString("addstructfield", 6)
	err := mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
	common.CheckErr(t, err, true)

	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim))
	structField := entity.NewField().
		WithName("clips").
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeStruct).
		WithMaxCapacity(16).
		WithNullable(true).
		WithStructSchema(structSchema)

	err = mc.AddCollectionStructField(ctx, client.NewAddCollectionStructFieldOption(collName, structField))
	common.CheckErr(t, err, true)

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)

	var added *entity.Field
	for _, field := range coll.Schema.Fields {
		if field.Name == "clips" {
			added = field
			break
		}
	}
	require.NotNil(t, added)
	require.Equal(t, entity.FieldTypeArray, added.DataType)
	require.Equal(t, entity.FieldTypeStruct, added.ElementType)
	require.True(t, added.Nullable)
	require.Equal(t, "16", added.TypeParams[entity.TypeParamMaxCapacity])
	require.NotNil(t, added.StructSchema)
	require.Len(t, added.StructSchema.Fields, 2)
	require.Equal(t, "tag", added.StructSchema.Fields[0].Name)
	require.Equal(t, entity.FieldTypeVarChar, added.StructSchema.Fields[0].DataType)
	require.Equal(t, "embedding", added.StructSchema.Fields[1].Name)
	require.Equal(t, entity.FieldTypeFloatVector, added.StructSchema.Fields[1].DataType)
	dim, err := added.StructSchema.Fields[1].GetDim()
	common.CheckErr(t, err, true)
	require.EqualValues(t, common.DefaultDim, dim)
}

// parameterized test for add field invalid cases
func TestAddCollectionFieldInvalid(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	testCases := []struct {
		name            string
		setupCollection func(string) error
		fieldBuilder    func() *entity.Field
		expectedError   string
	}{
		{
			name: "addInvalidField",
			setupCollection: func(collName string) error {
				return mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
			},
			fieldBuilder: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeVarChar).WithNullable(true)
			},
			expectedError: "type param(max_length) should be specified for the field(" + common.DefaultNewField + ") of collection",
		},
		{
			name: "addVectorFieldWithoutNullable",
			setupCollection: func(collName string) error {
				return mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
			},
			fieldBuilder: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
			},
			expectedError: "adding vector field to existing collection requires nullable=true, field name = " + common.DefaultNewField,
		},
		{
			name: "addFieldAsPrimary",
			setupCollection: func(collName string) error {
				return mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
			},
			fieldBuilder: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(true).WithIsPrimaryKey(true)
			},
			expectedError: "not support to add pk field, field name = " + common.DefaultNewField + ": invalid parameter",
		},
		{
			name: "addFieldAsAutoId",
			setupCollection: func(collName string) error {
				return mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
			},
			fieldBuilder: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(true).WithIsAutoID(true)
			},
			expectedError: "only primary field can speficy AutoID with true, field name = " + common.DefaultNewField + ": invalid parameter",
		},
		{
			name: "addFieldWithDisableNullable",
			setupCollection: func(collName string) error {
				return mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
			},
			fieldBuilder: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(false)
			},
			expectedError: "added field must be nullable, please check it, field name = " + common.DefaultNewField + ": invalid parameter",
		},
		{
			name: "addFieldAsPartitionKey",
			setupCollection: func(collName string) error {
				return mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
			},
			fieldBuilder: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(true).WithIsPartitionKey(true)
			},
			expectedError: "not support to add partition key field, field name  = " + common.DefaultNewField + ": invalid parameter",
		},
		{
			name: "addFieldExceedMaxLength",
			setupCollection: func(collName string) error {
				return mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
			},
			fieldBuilder: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeVarChar).WithNullable(true).WithMaxLength(65536)
			},
			expectedError: "the maximum length specified for the field(" + common.DefaultNewField + ") should be in (0, 65535], but got 65536 instead: invalid parameter",
		},
		{
			name: "addFieldAsClusterKey",
			setupCollection: func(collName string) error {
				// create collection with cluster key field
				int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
				vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
				varcharField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithIsClusteringKey(true)

				schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField).WithField(varcharField)
				return mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
			},
			fieldBuilder: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(true).WithIsClusteringKey(true)
			},
			expectedError: "already has another clustering key field, field name: " + common.DefaultNewField + ": invalid parameter",
		},
		{
			name: "addFieldSameOtherName",
			setupCollection: func(collName string) error {
				// create collection with varchar field
				int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
				vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
				varcharField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithIsClusteringKey(true)

				schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField).WithField(varcharField)
				return mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
			},
			fieldBuilder: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithNullable(true).WithMaxLength(64)
			},
			expectedError: "duplicated field name varchar: invalid parameter",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			collName := common.GenRandomString("addfield", 6)

			// setup collection
			err := tc.setupCollection(collName)
			common.CheckErr(t, err, true)

			// add field and expect error
			newField := tc.fieldBuilder()
			err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collName, newField))
			common.CheckErr(t, err, false, tc.expectedError)
		})
	}
}

// test add field when max field number exceeded
func TestCollectionAddFieldExceedMaxFieldNumber(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("addfield", 6)
	err := mc.CreateCollection(ctx, client.SimpleCreateCollectionOptions(collName, common.DefaultDim))
	common.CheckErr(t, err, true)

	// add fields until reaching max field number (64)
	for i := 0; i < 62; i++ {
		newField := entity.NewField().WithName(fmt.Sprintf("%s_%d", common.DefaultNewField, i)).WithDataType(entity.FieldTypeVarChar).WithNullable(true).WithMaxLength(64)
		err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collName, newField))
		common.CheckErr(t, err, true)
	}

	// try to add one more field to exceed max field number
	newField := entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeVarChar).WithNullable(true).WithMaxLength(64)
	err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collName, newField))
	common.CheckErr(t, err, false, "The number of fields has reached the maximum value 64: invalid parameter")
}

// create Inverted index for added field and drop index
func TestIndexAddedField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Define different index type test cases
	testCases := []struct {
		name          string
		indexType     string
		createIndex   func() index.Index
		expectedError string
	}{
		{
			name:        "InvertedIndex",
			indexType:   "INVERTED",
			createIndex: index.NewInvertedIndex,
		},
		{
			name:        "SortedIndex",
			indexType:   "STL_SORT",
			createIndex: index.NewSortedIndex,
		},
		{
			name:        "TrieIndex",
			indexType:   "TRIE",
			createIndex: index.NewTrieIndex,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := hp.NewCreateCollectionParams(hp.Int64MultiVec)
			prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

			// insert
			ip := hp.NewInsertParams(schema)
			prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
			prepare.FlushData(ctx, t, mc, schema.CollectionName)

			// the field to add
			newField := entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeVarChar).WithNullable(true).WithDefaultValueString("100").WithMaxLength(100)
			err := mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(schema.CollectionName, newField))
			common.CheckErr(t, err, true)

			// create index on varchar field
			idx := tc.createIndex()
			idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, newField.Name, idx))

			if tc.expectedError != "" {
				common.CheckErr(t, err, false, tc.expectedError)
				return
			}

			common.CheckErr(t, err, true)
			err = idxTask.Await(ctx)
			common.CheckErr(t, err, true)

			// describe index
			expIndex := index.NewGenericIndex(newField.Name, idx.Params())
			_index, _ := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, newField.Name))
			common.CheckIndex(t, _index, expIndex, common.TNewCheckIndexOpt(common.DefaultNb))

			// drop index
			errDrop := mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, idx.Name()))
			common.CheckErr(t, errDrop, true)
			_idx, errDescribe := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, idx.Name()))
			common.CheckErr(t, errDescribe, false, "index not found")
			common.CheckIndex(t, _idx, nil, common.TNewCheckIndexOpt(0).TWithIndexRows(0, 0, 0).TWithIndexState(common.IndexStateIndexStateNone))
		})
	}
}

func TestInsertWithAddedField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	for _, autoID := range [2]bool{false, true} {
		// create collection
		cp := hp.NewCreateCollectionParams(hp.Int64Vec)
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithAutoID(autoID), hp.TNewSchemaOption())

		// create partition
		parName := common.GenRandomString("par", 4)
		err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
		common.CheckErr(t, err, true)

		// add field
		newField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithNullable(true).WithDefaultValueString("100").WithMaxLength(100)
		err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(schema.CollectionName, newField))
		common.CheckErr(t, err, true)

		// insert without added field
		columnOpt := hp.TNewDataOption().TWithDim(common.DefaultDim)
		pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *columnOpt)
		vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *columnOpt)
		insertOpt := client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(vecColumn)
		if !autoID {
			insertOpt.WithColumns(pkColumn)
		}
		insertRes, err := mc.Insert(ctx, insertOpt.WithPartition(parName))
		common.CheckErr(t, err, true)
		if !autoID {
			common.CheckInsertResult(t, pkColumn, insertRes)
		}
		// insert with added field
		addedFieldColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeVarChar, *columnOpt)
		insertOpt = client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(vecColumn, addedFieldColumn)
		if !autoID {
			insertOpt.WithColumns(pkColumn)
		}
		insertRes, err = mc.Insert(ctx, insertOpt.WithPartition(parName))
		common.CheckErr(t, err, true)
		if !autoID {
			common.CheckInsertResult(t, pkColumn, insertRes)
		}
	}
}

func TestUpsertDynamicAddField(t *testing.T) {
	t.Parallel()

	// enable dynamic field/add field and insert dynamic/added column
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// add field
	defaultNewField := common.DefaultDynamicNumberField
	newField := entity.NewField().WithName(defaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(true).WithDefaultValueLong(888)
	err := mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(schema.CollectionName, newField))
	common.CheckErr(t, err, true)

	upsertNb := 10
	// 1. upsert exist pk without dynamic column and with added column
	opt := *hp.TNewDataOption()
	pkColumn, vecColumn := hp.GenColumnData(upsertNb, entity.FieldTypeInt64, opt), hp.GenColumnData(upsertNb, entity.FieldTypeFloatVector, opt)
	addedColumn := hp.GenColumnData(upsertNb, entity.FieldTypeInt64, *opt.TWithFieldName(defaultNewField))
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn, vecColumn, addedColumn))
	common.CheckErr(t, err, true)

	// query and gets empty
	resSet, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("$meta['%s'] < %d", common.DefaultDynamicNumberField, upsertNb)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 0, resSet.GetColumn(common.DefaultDynamicFieldName).Len())

	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s < %d", defaultNewField, upsertNb)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, upsertNb, resSet.GetColumn(common.DefaultDynamicFieldName).Len())

	// 2. upsert not exist pk with added column
	opt.TWithStart(common.DefaultNb)
	pkColumn2 := hp.GenColumnData(upsertNb, entity.FieldTypeInt64, *opt.TWithFieldName(common.DefaultInt64FieldName))
	vecColumn2 := hp.GenColumnData(upsertNb, entity.FieldTypeFloatVector, *opt.TWithFieldName(common.DefaultFloatVecFieldName))
	addedColumn2 := hp.GenColumnData(upsertNb, entity.FieldTypeInt64, *opt.TWithFieldName(defaultNewField))
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn2, vecColumn2, addedColumn2))
	common.CheckErr(t, err, true)

	// query and gets added field
	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s >= %d", defaultNewField, common.DefaultNb)).
		WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, upsertNb, resSet.GetColumn(defaultNewField).Len())
}

// query with dynamic field same as added field
func TestQueryWithDynamicAddedField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create and insert with dynamic field enabled
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	_, insertRes := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	// index
	it, _ := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, index.NewSCANNIndex(entity.COSINE, 32, false)))
	err := it.Await(ctx)
	common.CheckErr(t, err, true)

	// load
	lt, _ := mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName))
	err = lt.Await(ctx)
	common.CheckErr(t, err, true)

	// add field name as dynamic dynamicNumber field
	defaultValue := 8888
	defaultNewField := common.DefaultDynamicNumberField
	newField := entity.NewField().WithName(defaultNewField).WithDataType(entity.FieldTypeInt32).
		WithDefaultValueInt(int32(defaultValue)).WithNullable(true)
	err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(schema.CollectionName, newField))
	common.CheckErr(t, err, true)

	// insert with added field(dynamicNumber)
	_, insertRes2 := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithStart(common.DefaultNb))
	require.Equal(t, common.DefaultNb, int(insertRes2.InsertCount))

	// query with old data before add field
	expr := fmt.Sprintf("$meta['%s'] in [0, 1, 2, 3, 4]", common.DefaultDynamicNumberField)
	queryRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{insertRes.IDs.Slice(0, 5)})

	// query with old data after add field
	expr = fmt.Sprintf("%s == %d", defaultNewField, defaultValue)
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{insertRes.IDs.Slice(0, common.DefaultNb)})

	// query with new data after add field
	expr = fmt.Sprintf("%s != %d", defaultNewField, defaultValue)
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{insertRes2.IDs.Slice(0, common.DefaultNb)})
}

// search with dynamic field same as added field
func TestSearchWithDynamicAddedField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create and insert with dynamic field enabled
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	// index
	it, _ := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, index.NewSCANNIndex(entity.COSINE, 32, false)))
	err := it.Await(ctx)
	common.CheckErr(t, err, true)

	// load
	lt, _ := mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName))
	err = lt.Await(ctx)
	common.CheckErr(t, err, true)

	// add field name as dynamic dynamicNumber field
	defaultValue := 8888
	defaultNewField := common.DefaultDynamicNumberField
	newField := entity.NewField().WithName(defaultNewField).WithDataType(entity.FieldTypeInt32).
		WithDefaultValueInt(int32(defaultValue)).WithNullable(true)
	err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(schema.CollectionName, newField))
	common.CheckErr(t, err, true)

	// insert with added field(dynamicNumber)
	_, insertRes2 := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithStart(common.DefaultNb))
	require.Equal(t, common.DefaultNb, int(insertRes2.InsertCount))

	// search with old data before add field
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	expr := fmt.Sprintf("$meta['%s'] is not null", common.DefaultDynamicNumberField)
	resSearch, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
		WithFilter(expr))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)

	// search with old data after add field
	expr = fmt.Sprintf("%s == %d", defaultNewField, defaultValue)
	resSearch, err = mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
		WithFilter(expr))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)

	// search with new data after add field
	expr = fmt.Sprintf("%s != %d", defaultNewField, defaultValue)
	resSearch, err = mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
		WithFilter(expr))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
}

// test delete with added field
func TestDeleteWithAddedField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// the field to add
	newField := entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(true).WithDefaultValueLong(100)
	err := mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(schema.CollectionName, newField))
	common.CheckErr(t, err, true)

	// insert
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index and load collection
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete with expr
	delRes, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(common.DefaultNewField+" is null"))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(0), delRes.DeleteCount)

	// delete with int64 pk
	delRes, errDelete = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(common.DefaultNewField+" == 100"))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(common.DefaultNb), delRes.DeleteCount)

	// query, verify delete success
	exprQuery := fmt.Sprintf("%s > 0", common.DefaultInt64FieldName)
	queryRes, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprQuery).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errQuery, true)
	require.Zero(t, queryRes.ResultCount)
}
