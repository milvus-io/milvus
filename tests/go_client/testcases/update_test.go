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
	t.Parallel()

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
	t.Parallel()

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

func TestPartialUpdateDynamicSchemaStaticOnly(t *testing.T) {
	t.Parallel()

	/*
		Test partial upsert with dynamic schema enabled but data contains ONLY static fields:
		1. Create collection with enable_dynamic_field=true, schema has id + vector + name(nullable)
		2. Insert initial data with only static fields (no dynamic data)
		3. Partial upsert a mixed batch: existing rows + new rows, all with only static fields
		4. Verify that existing rows are updated and new rows are inserted correctly

		This reproduces a bug where $meta valid_data length mismatches when:
		- enable_dynamic_field=true
		- partial_update=true
		- data contains NO dynamic fields
		- batch mixes existing and new primary keys
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("update_dyn_static", 6)

	// Create schema: id(pk) + vector + name(nullable), enable_dynamic_field=true
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	nameField := entity.NewField().WithName("name").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithNullable(true)

	fields := []*entity.Field{pkField, vecField, nameField}
	schema := hp.GenSchema(hp.TNewSchemaOption().
		TWithName(collName).
		TWithDescription("test partial update with dynamic schema and static-only data").
		TWithFields(fields).
		TWithEnableDynamicField(true))

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*10)
		defer cancel()
		err := mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		common.CheckErr(t, err, true)
	})

	// Insert initial data: 10 rows with ONLY static fields (no dynamic data at all)
	initNb := 10
	initPks := make([]int64, initNb)
	initNames := make([]string, initNb)
	for i := 0; i < initNb; i++ {
		initPks[i] = int64(i)
		initNames[i] = fmt.Sprintf("item_%d", i)
	}
	initPkCol := column.NewColumnInt64(common.DefaultInt64FieldName, initPks)
	initVecCol := hp.GenColumnData(initNb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	initNameCol := column.NewColumnVarChar("name", initNames)

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(initPkCol, initVecCol, initNameCol))
	common.CheckErr(t, err, true)

	// Flush -> Index -> Load
	prepare := &hp.CollectionPrepare{}
	prepare.FlushData(ctx, t, mc, collName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(collName))
	time.Sleep(time.Second * 3)

	// Partial upsert: mixed batch with existing rows (id=0,1) and new rows (id=800,801)
	// All rows contain ONLY static fields, no dynamic data
	upsertPks := []int64{0, 1, 800, 801}
	upsertNames := []string{"static_upd_0", "static_upd_1", "static_new_800", "static_new_801"}
	upsertNb := len(upsertPks)

	upsertPkCol := column.NewColumnInt64(common.DefaultInt64FieldName, upsertPks)
	upsertVecCol := hp.GenColumnData(upsertNb, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithStart(500))
	upsertNameCol := column.NewColumnVarChar("name", upsertNames)

	upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(upsertPkCol, upsertVecCol, upsertNameCol).
		WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	require.EqualValues(t, upsertNb, upsertRes.UpsertCount)

	// Query and verify
	resSet, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("%s in [0, 1, 800, 801]", common.DefaultInt64FieldName)).
		WithOutputFields(common.DefaultInt64FieldName, "name").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	// Build result map by id
	require.Equal(t, 4, resSet.GetColumn(common.DefaultInt64FieldName).Len())
	pkResults := resSet.GetColumn(common.DefaultInt64FieldName).(*column.ColumnInt64).Data()
	nameResults := resSet.GetColumn("name").(*column.ColumnVarChar).Data()
	resultMap := make(map[int64]string)
	for i, pk := range pkResults {
		resultMap[pk] = nameResults[i]
	}

	// Verify existing rows are updated
	require.Equal(t, "static_upd_0", resultMap[0], "id=0 should be updated")
	require.Equal(t, "static_upd_1", resultMap[1], "id=1 should be updated")

	// Verify new rows are inserted
	require.Equal(t, "static_new_800", resultMap[800], "id=800 should be inserted")
	require.Equal(t, "static_new_801", resultMap[801], "id=801 should be inserted")
}

func TestPartialUpdateDynamicSchemaWithDynamicFields(t *testing.T) {
	t.Parallel()

	/*
		Test partial upsert with dynamic schema enabled AND user-provided dynamic fields:
		1. Create collection with enable_dynamic_field=true, schema has id + vector + name(nullable)
		2. Insert initial data with static fields + dynamic field "color"
		3. Partial upsert a mixed batch (existing + new rows) with static + dynamic fields
		4. Verify existing rows updated, new rows inserted, dynamic fields correct

		This reproduces a bug where $meta valid_data length mismatches when:
		- enable_dynamic_field=true
		- partial_update=true
		- data contains user-provided dynamic fields
		- batch mixes existing and new primary keys
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("update_dyn_fields", 6)

	// Create schema: id(pk) + vector + name(nullable), enable_dynamic_field=true
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	nameField := entity.NewField().WithName("name").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithNullable(true)

	fields := []*entity.Field{pkField, vecField, nameField}
	schema := hp.GenSchema(hp.TNewSchemaOption().
		TWithName(collName).
		TWithDescription("test partial update with dynamic schema and dynamic data").
		TWithFields(fields).
		TWithEnableDynamicField(true))

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*10)
		defer cancel()
		err := mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		common.CheckErr(t, err, true)
	})

	// Insert initial data with static fields + dynamic field "color"
	initNb := 10
	initPks := make([]int64, initNb)
	initNames := make([]string, initNb)
	for i := 0; i < initNb; i++ {
		initPks[i] = int64(i)
		initNames[i] = fmt.Sprintf("item_%d", i)
	}
	initPkCol := column.NewColumnInt64(common.DefaultInt64FieldName, initPks)
	initVecCol := hp.GenColumnData(initNb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	initNameCol := column.NewColumnVarChar("name", initNames)
	initColorCol := column.NewColumnVarChar("color", []string{
		"red", "blue", "green", "yellow", "purple",
		"orange", "pink", "white", "black", "gray",
	})

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(initPkCol, initVecCol, initNameCol, initColorCol))
	common.CheckErr(t, err, true)

	// Flush -> Index -> Load
	prepare := &hp.CollectionPrepare{}
	prepare.FlushData(ctx, t, mc, collName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(collName))
	time.Sleep(time.Second * 3)

	// Partial upsert mixed batch with static + dynamic fields
	// existing rows (id=0,1) + new rows (id=800,801), with dynamic field "color"
	upsertPks := []int64{0, 1, 800, 801}
	upsertNames := []string{"upd_0", "upd_1", "new_800", "new_801"}
	upsertColors := []string{"gold", "silver", "bronze", "copper"}
	upsertNb := len(upsertPks)

	upsertPkCol := column.NewColumnInt64(common.DefaultInt64FieldName, upsertPks)
	upsertVecCol := hp.GenColumnData(upsertNb, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithStart(500))
	upsertNameCol := column.NewColumnVarChar("name", upsertNames)
	upsertColorCol := column.NewColumnVarChar("color", upsertColors)

	upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(upsertPkCol, upsertVecCol, upsertNameCol, upsertColorCol).
		WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	require.EqualValues(t, upsertNb, upsertRes.UpsertCount)

	// Query and verify
	resSet, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("%s in [0, 1, 800, 801]", common.DefaultInt64FieldName)).
		WithOutputFields(common.DefaultInt64FieldName, "name", "color").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	require.Equal(t, 4, resSet.GetColumn(common.DefaultInt64FieldName).Len())
	pkResults := resSet.GetColumn(common.DefaultInt64FieldName).(*column.ColumnInt64).Data()
	nameResults := resSet.GetColumn("name").(*column.ColumnVarChar).Data()

	// Build result maps
	nameMap := make(map[int64]string)
	for i, pk := range pkResults {
		nameMap[pk] = nameResults[i]
	}

	// Verify static field "name"
	require.Equal(t, "upd_0", nameMap[0], "id=0 name should be updated")
	require.Equal(t, "upd_1", nameMap[1], "id=1 name should be updated")
	require.Equal(t, "new_800", nameMap[800], "id=800 name should be inserted")
	require.Equal(t, "new_801", nameMap[801], "id=801 name should be inserted")

	// Verify dynamic field "color" updated for all rows
	colorCol := resSet.GetColumn("color")
	require.NotNil(t, colorCol, "color column should exist")
	require.Equal(t, 4, colorCol.Len())
	colorMap := make(map[int64]string)
	for i, pk := range pkResults {
		val, err := colorCol.GetAsString(i)
		require.NoError(t, err)
		colorMap[pk] = val
	}
	require.Equal(t, "gold", colorMap[0], "id=0 color should be updated")
	require.Equal(t, "silver", colorMap[1], "id=1 color should be updated")
	require.Equal(t, "bronze", colorMap[800], "id=800 color should be inserted")
	require.Equal(t, "copper", colorMap[801], "id=801 color should be inserted")
}

func TestUpdateNullableFieldBehavior(t *testing.T) {
	t.Parallel()

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

func TestUpdateDefaultValueFieldBehavior(t *testing.T) {
	t.Parallel()

	/*
		Test default value field behavior for Update operation:
		1. Insert data with a non-nullable default-value field having explicit values
		2. Partial update the same entities without providing the default-value field
		3. Verify that the field retains its original value (not replaced with the default)
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Create collection with non-nullable default-value field
	collName := common.GenRandomString("update_default", 6)

	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	defaultField := entity.NewField().WithName("default_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100).WithDefaultValueString("default_val")

	fields := []*entity.Field{pkField, vecField, defaultField}
	schema := hp.GenSchema(hp.TNewSchemaOption().TWithName(collName).TWithDescription("test default value field behavior for update").TWithFields(fields))

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*10)
		defer cancel()
		err := mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		common.CheckErr(t, err, true)
	})

	// Insert initial data with explicit varchar values
	pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{1, 2, 3})
	vecColumn := hp.GenColumnData(3, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	defaultColumn := column.NewColumnVarChar("default_varchar", []string{"original_1", "original_2", "original_3"})

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(pkColumn, vecColumn, defaultColumn))
	common.CheckErr(t, err, true)

	// Flush -> Index -> Load
	prepare := &hp.CollectionPrepare{}
	prepare.FlushData(ctx, t, mc, collName)
	indexParams := hp.TNewIndexParams(schema)
	prepare.CreateIndex(ctx, t, mc, indexParams)
	loadParams := hp.NewLoadParams(collName)
	prepare.Load(ctx, t, mc, loadParams)

	time.Sleep(time.Second * 5)

	// Partial update PKs 1,2 with only pk + vec (omit default_varchar)
	updatePkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{1, 2})
	updateVecColumn := hp.GenColumnData(2, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithStart(100))

	updateRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(updatePkColumn, updateVecColumn).WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 2, updateRes.UpsertCount)

	time.Sleep(time.Second * 3)

	// Query PKs 1,2 -> verify default_varchar retains original values
	resSet, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("%s in [1, 2]", common.DefaultInt64FieldName)).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	require.Equal(t, 2, resSet.GetColumn("default_varchar").Len())
	defaultResults := resSet.GetColumn("default_varchar").(*column.ColumnVarChar).Data()
	require.Equal(t, "original_1", defaultResults[0])
	require.Equal(t, "original_2", defaultResults[1])

	// Query PK 3 -> verify unchanged
	resSet3, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("%s == 3", common.DefaultInt64FieldName)).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	require.Equal(t, 1, resSet3.GetColumn("default_varchar").Len())
	unchangedResults := resSet3.GetColumn("default_varchar").(*column.ColumnVarChar).Data()
	require.Equal(t, "original_3", unchangedResults[0])
}

func TestPartialUpdateEmptyStringDefaultValue(t *testing.T) {
	t.Parallel()

	/*
		Test partial update on a non-nullable field with empty string as default value:
		1. Create collection with non-nullable varchar field, defaultValue=""
		2. Insert rows with explicit non-empty values
		3. Partial update to a different non-empty value → should succeed
		4. Partial update to empty string "" → should succeed (empty string is a valid value, not null)
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("update_empty_default", 6)

	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	tagField := entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithDefaultValueString("")

	fields := []*entity.Field{pkField, vecField, tagField}
	schema := hp.GenSchema(hp.TNewSchemaOption().
		TWithName(collName).
		TWithDescription("test partial update with empty string default value").
		TWithFields(fields))

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*10)
		defer cancel()
		err := mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		common.CheckErr(t, err, true)
	})

	// Step 2: Insert 3 rows with explicit tag values
	pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{1, 2, 3})
	vecColumn := hp.GenColumnData(3, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	tagColumn := column.NewColumnVarChar("tag", []string{"alpha", "beta", "gamma"})

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(pkColumn, vecColumn, tagColumn))
	common.CheckErr(t, err, true)

	// Flush -> Index -> Load
	prepare := &hp.CollectionPrepare{}
	prepare.FlushData(ctx, t, mc, collName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(collName))
	time.Sleep(time.Second * 3)

	// Step 3: Partial update id=1 tag to a different non-empty value
	updatePk1 := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{1})
	updateVec1 := hp.GenColumnData(1, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithStart(100))
	updateTag1 := column.NewColumnVarChar("tag", []string{"updated_alpha"})

	res1, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(updatePk1, updateVec1, updateTag1).
		WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 1, res1.UpsertCount)

	// Verify id=1 tag updated
	resSet1, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("%s == 1", common.DefaultInt64FieldName)).
		WithOutputFields("tag").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	tagResults1 := resSet1.GetColumn("tag").(*column.ColumnVarChar).Data()
	require.Equal(t, "updated_alpha", tagResults1[0], "id=1 tag should be updated to new value")

	// Step 4: Partial update id=2 tag to empty string ""
	updatePk2 := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{2})
	updateVec2 := hp.GenColumnData(1, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithStart(200))
	updateTag2 := column.NewColumnVarChar("tag", []string{""})

	res2, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(updatePk2, updateVec2, updateTag2).
		WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 1, res2.UpsertCount)

	// Verify id=2 tag updated to empty string
	resSet2, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("%s == 2", common.DefaultInt64FieldName)).
		WithOutputFields("tag").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	tagResults2 := resSet2.GetColumn("tag").(*column.ColumnVarChar).Data()
	require.Equal(t, "", tagResults2[0], "id=2 tag should be updated to empty string")

	// Verify id=3 tag unchanged
	resSet3, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("%s == 3", common.DefaultInt64FieldName)).
		WithOutputFields("tag").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	tagResults3 := resSet3.GetColumn("tag").(*column.ColumnVarChar).Data()
	require.Equal(t, "gamma", tagResults3[0], "id=3 tag should remain unchanged")
}

func TestUpsertDefaultValueWithCompressedValidData(t *testing.T) {
	t.Parallel()

	/*
		Test upsert with compressed ValidData for a field with defaultValue.
		This covers the upsert path (task_upsert.go queryPreExecute) where
		FillWithDefaultValue is dispatched instead of FillWithNullValue.

		1. Create collection with nullable + defaultValue varchar field
		2. Insert 3 rows with explicit values
		3. Upsert 3 rows using NullableColumn (compressed format with ValidData),
		   where row 3 has valid=false -> should be filled with default value
		4. Query and verify row 3 gets the default value
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("upsert_default_compressed", 6)

	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	defaultField := entity.NewField().WithName("default_varchar").WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(100).WithNullable(true).WithDefaultValueString("default_val")

	fields := []*entity.Field{pkField, vecField, defaultField}
	schema := hp.GenSchema(hp.TNewSchemaOption().TWithName(collName).TWithDescription("test upsert default value with compressed valid data").TWithFields(fields))

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*10)
		defer cancel()
		err := mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
		common.CheckErr(t, err, true)
	})

	// Insert 3 rows with explicit values
	pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{1, 2, 3})
	vecColumn := hp.GenColumnData(3, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	varcharColumn := column.NewColumnVarChar("default_varchar", []string{"v1", "v2", "v3"})

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(pkColumn, vecColumn, varcharColumn))
	common.CheckErr(t, err, true)

	prepare := &hp.CollectionPrepare{}
	prepare.FlushData(ctx, t, mc, collName)
	indexParams := hp.TNewIndexParams(schema)
	prepare.CreateIndex(ctx, t, mc, indexParams)
	loadParams := hp.NewLoadParams(collName)
	prepare.Load(ctx, t, mc, loadParams)

	time.Sleep(time.Second * 5)

	// Upsert 3 rows with compressed ValidData:
	// data=["new1","new2"], validData=[true,true,false]
	// Row 3 (valid=false) should be filled with "default_val" by FillWithDefaultValue
	upsertPkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{1, 2, 3})
	upsertVecColumn := hp.GenColumnData(3, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithStart(100))
	nullableVarcharColumn, err := column.NewNullableColumnVarChar("default_varchar", []string{"new1", "new2"}, []bool{true, true, false})
	common.CheckErr(t, err, true)

	upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(upsertPkColumn, upsertVecColumn, nullableVarcharColumn))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 3, upsertRes.UpsertCount)

	time.Sleep(time.Second * 3)

	// Query all 3 rows and verify
	resSet, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("%s in [1, 2, 3]", common.DefaultInt64FieldName)).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	require.Equal(t, 3, resSet.GetColumn("default_varchar").Len())
	results := resSet.GetColumn("default_varchar").(*column.ColumnVarChar).Data()
	require.Equal(t, "new1", results[0])
	require.Equal(t, "new2", results[1])
	require.Equal(t, "default_val", results[2])
}
