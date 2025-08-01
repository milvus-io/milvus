package testcases

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestUpdatePartialFields(t *testing.T) {
	/*
		1. prepare create -> insert -> index -> load -> query
		2. partial update existing entities -> data updated -> query and verify
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	// connect
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	genPkWithSingleScalarField := func(option *hp.GenDataOption) ([]column.Column, []column.Column) {
		log.Info("genPkWithSingleScalarField")
		columns := make([]column.Column, 0, 2)
		columns = append(columns, hp.GenColumnDataWithOption(entity.FieldTypeInt64, *option))
		columns = append(columns, hp.GenColumnDataWithOption(entity.FieldTypeFloat, *option))
		return columns, nil
	}

	genPkWithSinglVectorField := func(option *hp.GenDataOption) ([]column.Column, []column.Column) {
		log.Info("genPkWithSinglVectorField")
		columns := make([]column.Column, 0, 2)
		columns = append(columns, hp.GenColumnDataWithOption(entity.FieldTypeInt64, *option))
		columns = append(columns, hp.GenColumnDataWithOption(entity.FieldTypeFloatVector, *option))
		return columns, nil
	}

	updateNb := 200
	for _, genColumnsFunc := range []func(*hp.GenDataOption) ([]column.Column, []column.Column){genPkWithSingleScalarField, genPkWithSinglVectorField} {
		// perform partial update operation for existing entities [0, 200) -> query and verify
		columns, dynamicColumns := genColumnsFunc(hp.TNewDataOption().TWithNb(updateNb).TWithStart(0))
		updateRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(columns...).WithColumns(dynamicColumns...).WithPartialUpdate(true))
		common.CheckErr(t, err, true)
		require.EqualValues(t, updateNb, updateRes.UpsertCount)

		expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, updateNb)
		resSet, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		common.CheckPartialResult(t, append(columns, hp.MergeColumnsToDynamic(updateNb, dynamicColumns, common.DefaultDynamicFieldName)), resSet.Fields)
	}
}

func TestPartialUpdateDynamicField(t *testing.T) {
	// enable dynamic field and perform partial update operations on dynamic columns
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	time.Sleep(time.Second * 4)
	// verify that dynamic field exists
	testNb := 10
	resSet, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s < %d", common.DefaultDynamicNumberField, testNb)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, testNb, resSet.GetColumn(common.DefaultDynamicFieldName).Len())

	// 1. query and gets empty
	targetPk := int64(20000)
	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s == %d", common.DefaultInt64FieldName, targetPk)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 0, resSet.GetColumn(common.DefaultDynamicFieldName).Len())

	// 2. perform partial update operation for existing pk with dynamic column [a=1]
	dynamicColumnA := column.NewColumnInt32(common.DefaultDynamicNumberField, []int32{1})
	vecColumn := hp.GenColumnData(1, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	pkColumnA := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{targetPk})
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumnA, dynamicColumnA, vecColumn).WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	time.Sleep(time.Second * 4)
	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s == %d", common.DefaultInt64FieldName, targetPk)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, resSet.GetColumn(common.DefaultDynamicFieldName).Len())
	common.EqualColumn(t, hp.MergeColumnsToDynamic(1, []column.Column{dynamicColumnA}, common.DefaultDynamicFieldName), resSet.GetColumn(common.DefaultDynamicFieldName))

	// 3. perform partial update operation for existing pk with dynamic column [b=true]
	dynamicColumnB := column.NewColumnBool(common.DefaultDynamicBoolField, []bool{true})
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumnA, dynamicColumnB).WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	time.Sleep(time.Second * 4)
	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s == %d", common.DefaultInt64FieldName, targetPk)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, resSet.GetColumn(common.DefaultDynamicFieldName).Len())
	common.EqualColumn(t, hp.MergeColumnsToDynamic(1, []column.Column{dynamicColumnA, dynamicColumnB}, common.DefaultDynamicFieldName), resSet.GetColumn(common.DefaultDynamicFieldName))

	// 4. perform partial update operation for existing pk with dynamic column [a=2, b=false]
	dynamicColumnA = column.NewColumnInt32(common.DefaultDynamicNumberField, []int32{2})
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumnA, dynamicColumnA).WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	time.Sleep(time.Second * 4)
	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s == %d", common.DefaultInt64FieldName, targetPk)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, resSet.GetColumn(common.DefaultDynamicFieldName).Len())
	common.EqualColumn(t, hp.MergeColumnsToDynamic(1, []column.Column{dynamicColumnA, dynamicColumnB}, common.DefaultDynamicFieldName), resSet.GetColumn(common.DefaultDynamicFieldName))
}

func TestUpdateNullableFieldBehavior(t *testing.T) {
	/*
		Test nullable field behavior for Update operation:
		1. Insert data with nullable field having a value
		2. Update the same entity without providing the nullable field
		3. Verify that the nullable field retains its original value (not set to null)
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Create collection with nullable field using custom schema
	collName := common.GenRandomString("update_nullable", 6)

	// Create fields including nullable field
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	nullableField := entity.NewField().WithName("nullable_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100).WithNullable(true)

	fields := []*entity.Field{pkField, vecField, nullableField}
	schema := hp.GenSchema(hp.TNewSchemaOption().TWithName(collName).TWithDescription("test nullable field behavior for update").TWithFields(fields))

	// Create collection using schema
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// Cleanup
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*10)
		defer cancel()
		err := mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		common.CheckErr(t, err, true)
	})

	// Insert initial data with nullable field having a value
	pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{1, 2, 3})
	vecColumn := hp.GenColumnData(3, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	nullableColumn := column.NewColumnVarChar("nullable_varchar", []string{"original_1", "original_2", "original_3"})

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(pkColumn, vecColumn, nullableColumn))
	common.CheckErr(t, err, true)

	// Use prepare pattern for remaining operations
	prepare := &hp.CollectionPrepare{}

	// Flush data
	prepare.FlushData(ctx, t, mc, collName)

	// Create index for vector field
	indexParams := hp.TNewIndexParams(schema)
	prepare.CreateIndex(ctx, t, mc, indexParams)

	// Load collection
	loadParams := hp.NewLoadParams(collName)
	prepare.Load(ctx, t, mc, loadParams)

	// Wait for loading to complete
	time.Sleep(time.Second * 5)

	// Update entities without providing nullable field (should retain original values)
	updatePkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{1, 2})
	updateVecColumn := hp.GenColumnData(2, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithStart(100))

	updateRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(updatePkColumn, updateVecColumn).WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 2, updateRes.UpsertCount)

	// Wait for consistency
	time.Sleep(time.Second * 3)

	// Query to verify nullable field retains original values
	resSet, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("%s in [1, 2]", common.DefaultInt64FieldName)).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	// Verify results
	require.Equal(t, 2, resSet.GetColumn("nullable_varchar").Len())
	nullableResults := resSet.GetColumn("nullable_varchar").(*column.ColumnVarChar).Data()
	require.Equal(t, "original_1", nullableResults[0])
	require.Equal(t, "original_2", nullableResults[1])
}
