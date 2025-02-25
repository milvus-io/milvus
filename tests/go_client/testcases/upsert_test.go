package testcases

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestUpsertAllFields(t *testing.T) {
	/*
		1. prepare create -> insert -> index -> load -> query
		2. upsert exist entities -> data updated -> query and verify
		3. delete some pks -> query and verify
		4. upsert part deleted(not exist) pk and part existed pk -> query and verify
		5. upsert all not exist pk -> query and verify
	*/
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	// connect
	mc := createDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	upsertNb := 200
	for _, genColumnsFunc := range []func(*entity.Schema, *hp.GenDataOption) ([]column.Column, []column.Column){hp.GenColumnsBasedSchema, hp.GenColumnsBasedSchemaWithFp32VecConversion} {
		// upsert exist entities [0, 200) -> query and verify
		columns, dynamicColumns := genColumnsFunc(schema, hp.TNewDataOption().TWithNb(upsertNb))
		upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(columns...).WithColumns(dynamicColumns...))
		common.CheckErr(t, err, true)
		require.EqualValues(t, upsertNb, upsertRes.UpsertCount)

		expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, upsertNb)
		resSet, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		common.CheckQueryResult(t, append(columns, hp.MergeColumnsToDynamic(upsertNb, dynamicColumns, common.DefaultDynamicFieldName)), resSet.Fields)

		// deleted all upsert entities -> query and verify
		delRes, err := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(expr))
		common.CheckErr(t, err, true)
		require.EqualValues(t, upsertNb, delRes.DeleteCount)

		resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		require.Zero(t, resSet.ResultCount)

		// upsert part deleted(not exist) pk and part existed pk [100, 500) -> query and verify the updated entities
		newUpsertNb := 400
		newUpsertStart := 100
		columnsPart, dynamicColumnsPart := genColumnsFunc(schema, hp.TNewDataOption().TWithNb(newUpsertNb).TWithStart(newUpsertStart))
		upsertResPart, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(columnsPart...).WithColumns(dynamicColumnsPart...))
		common.CheckErr(t, err, true)
		require.EqualValues(t, newUpsertNb, upsertResPart.UpsertCount)

		newExpr := fmt.Sprintf("%d <= %s < %d", newUpsertStart, common.DefaultInt64FieldName, newUpsertNb+newUpsertStart)
		resSetPart, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(newExpr).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		common.CheckQueryResult(t, append(columnsPart, hp.MergeColumnsToDynamic(newUpsertNb, dynamicColumnsPart, common.DefaultDynamicFieldName)), resSetPart.Fields)

		// upsert all deleted(not exist) pk [0, 100)
		columnsNot, dynamicColumnsNot := genColumnsFunc(schema, hp.TNewDataOption().TWithNb(newUpsertStart))
		upsertResNot, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(columnsNot...).WithColumns(dynamicColumnsNot...))
		common.CheckErr(t, err, true)
		require.EqualValues(t, newUpsertStart, upsertResNot.UpsertCount)

		newExprNot := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, newUpsertStart)
		resSetNot, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(newExprNot).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		common.CheckQueryResult(t, append(columnsNot, hp.MergeColumnsToDynamic(newUpsertStart, dynamicColumnsNot, common.DefaultDynamicFieldName)), resSetNot.Fields)
	}
}

func TestUpsertSparse(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus-sdk-go/issues/769")
	/*
		1. prepare create -> insert -> index -> load -> query
		2. upsert exist entities -> data updated -> query and verify
		3. delete some pks -> query and verify
		4. upsert part deleted(not exist) pk and part existed pk -> query and verify
		5. upsert all not exist pk -> query and verify
	*/
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	// connect
	mc := createDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithSparseMaxLen(128).TWithNb(0))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	upsertNb := 200

	// upsert exist entities [0, 200) -> query and verify
	columns, dynamicColumns := hp.GenColumnsBasedSchema(schema, hp.TNewDataOption().TWithNb(upsertNb))
	upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(columns...).WithColumns(dynamicColumns...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, upsertNb, upsertRes.UpsertCount)

	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, upsertNb)
	resSet, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, append(columns, hp.MergeColumnsToDynamic(upsertNb, dynamicColumns, common.DefaultDynamicFieldName)), resSet.Fields)

	// deleted all upsert entities -> query and verify
	delRes, err := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(expr))
	common.CheckErr(t, err, true)
	require.EqualValues(t, upsertNb, delRes.DeleteCount)

	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Zero(t, resSet.ResultCount)

	// upsert part deleted(not exist) pk and part existed pk [100, 500) -> query and verify the updated entities
	newUpsertNb := 400
	newUpsertStart := 100
	columnsPart, dynamicColumnsPart := hp.GenColumnsBasedSchema(schema, hp.TNewDataOption().TWithNb(newUpsertNb).TWithStart(newUpsertStart))
	upsertResPart, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(columnsPart...).WithColumns(dynamicColumnsPart...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, newUpsertNb, upsertResPart.UpsertCount)

	newExpr := fmt.Sprintf("%d <= %s < %d", newUpsertStart, common.DefaultInt64FieldName, newUpsertNb+newUpsertStart)
	resSetPart, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(newExpr).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, append(columnsPart, hp.MergeColumnsToDynamic(newUpsertNb, dynamicColumnsPart, common.DefaultDynamicFieldName)), resSetPart.Fields)

	// upsert all deleted(not exist) pk [0, 100)
	columnsNot, dynamicColumnsNot := hp.GenColumnsBasedSchema(schema, hp.TNewDataOption().TWithNb(newUpsertStart))
	upsertResNot, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(columnsNot...).WithColumns(dynamicColumnsNot...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, newUpsertStart, upsertResNot.UpsertCount)

	newExprNot := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, newUpsertStart)
	resSetNot, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(newExprNot).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, append(columnsNot, hp.MergeColumnsToDynamic(newUpsertStart, dynamicColumnsNot, common.DefaultDynamicFieldName)), resSetNot.Fields)
}

func TestUpsertVarcharPk(t *testing.T) {
	/*
		test upsert varchar pks
		upsert after query
		upsert "a" -> " a " -> actually new insert
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	upsertNb := 10
	// upsert exist entities [0, 10) varchar: ["1", ... "9"]
	genDataOpt := *hp.TNewDataOption()
	varcharColumn, binaryColumn := hp.GenColumnData(upsertNb, entity.FieldTypeVarChar, genDataOpt), hp.GenColumnData(upsertNb, entity.FieldTypeBinaryVector, genDataOpt)
	upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(varcharColumn, binaryColumn))
	common.CheckErr(t, err, true)
	common.EqualColumn(t, varcharColumn, upsertRes.IDs)

	// query and verify the updated entities
	expr := fmt.Sprintf("%s in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'] ", common.DefaultVarcharFieldName)
	resSet1, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, []column.Column{varcharColumn, binaryColumn}, resSet1.Fields)

	// upsert varchar (with space): [" 1 ", ... " 9 "]
	varcharValues := make([]string, 0, upsertNb)
	for i := 0; i < upsertNb; i++ {
		varcharValues = append(varcharValues, " "+strconv.Itoa(i)+" ")
	}
	varcharColumn1 := column.NewColumnVarChar(common.DefaultVarcharFieldName, varcharValues)
	binaryColumn1 := hp.GenColumnData(upsertNb, entity.FieldTypeBinaryVector, genDataOpt)
	upsertRes1, err1 := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(varcharColumn1, binaryColumn1))
	common.CheckErr(t, err1, true)
	common.EqualColumn(t, varcharColumn1, upsertRes1.IDs)

	// query old varchar pk (no space): ["1", ... "9"]
	resSet2, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, []column.Column{varcharColumn, binaryColumn}, resSet2.Fields)

	// query and verify the updated entities
	exprNew := fmt.Sprintf("%s like ' %% ' ", common.DefaultVarcharFieldName)
	resSet3, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprNew).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, []column.Column{varcharColumn1, binaryColumn1}, resSet3.Fields)
}

// test upsert with partition
func TestUpsertMultiPartitions(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	parName := common.GenRandomString("p", 4)
	err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert [0, nb) into default, insert [nb, nb*2) into new
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(parName), hp.TNewDataOption().TWithStart(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// upsert new partition
	columns, dynamicColumns := hp.GenColumnsBasedSchema(schema, hp.TNewDataOption().TWithStart(common.DefaultNb))
	upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(columns...).WithColumns(dynamicColumns...).WithPartition(parName))
	common.CheckErr(t, err, true)
	require.EqualValues(t, common.DefaultNb, upsertRes.UpsertCount)

	// query and verify
	expr := fmt.Sprintf("%d <= %s < %d", common.DefaultNb, common.DefaultInt64FieldName, common.DefaultNb+200)
	resSet3, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	expColumns := []column.Column{hp.MergeColumnsToDynamic(200, dynamicColumns, common.DefaultDynamicFieldName)}
	for _, c := range columns {
		expColumns = append(expColumns, c.Slice(0, 200))
	}
	common.CheckQueryResult(t, expColumns, resSet3.Fields)
}

func TestUpsertSamePksManyTimes(t *testing.T) {
	// upsert pks [0, 1000) many times with different vector
	// query -> gets last upsert entities

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	// create and insert
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	var _columns []column.Column
	upsertNb := 10
	for i := 0; i < 10; i++ {
		// upsert exist entities [0, 10)
		_columns, _ = hp.GenColumnsBasedSchema(schema, hp.TNewDataOption().TWithNb(upsertNb))
		_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(_columns...))
		common.CheckErr(t, err, true)
	}

	// flush -> index -> load
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query and verify the updated entities
	resSet, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, upsertNb)).
		WithOutputFields(common.DefaultFloatVecFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	for _, c := range _columns {
		if c.Name() == common.DefaultFloatVecFieldName {
			common.EqualColumn(t, c, resSet.GetColumn(common.DefaultFloatVecFieldName))
		}
	}
}

// test upsert autoId collection
func TestUpsertAutoID(t *testing.T) {
	/*
		prepare autoID collection
		upsert not exist pk -> error
		upsert exist pk -> error ? autoID not supported upsert
	*/
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	nb := 100

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption().TWithAutoID(true), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	_, insertRes := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(nb))

	// upsert autoID collection with existed pks -> actually delete passed pks and auto generate new pks ? So weired
	vecColumn := hp.GenColumnData(nb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(insertRes.IDs, vecColumn))
	common.CheckErr(t, err, true)
	log.Debug("upsertRes", zap.Any("len", upsertRes.IDs.(*column.ColumnInt64).Data()))

	// insertRes pks were deleted
	expr := fmt.Sprintf("%s <= %d", common.DefaultInt64FieldName, insertRes.IDs.(*column.ColumnInt64).Data()[nb-1])
	log.Debug("expr", zap.String("expr", expr))
	resSet, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithOutputFields(common.DefaultFloatVecFieldName).WithFilter(expr))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 0, resSet.ResultCount)

	exprUpsert := fmt.Sprintf("%s <= %d", common.DefaultInt64FieldName, upsertRes.IDs.(*column.ColumnInt64).Data()[nb-1])
	log.Debug("expr", zap.String("expr", expr))
	resSet1, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithOutputFields(common.DefaultFloatVecFieldName).WithFilter(exprUpsert))
	common.CheckErr(t, err, true)
	common.EqualColumn(t, vecColumn, resSet1.GetColumn(common.DefaultFloatVecFieldName))

	// upsert with not existing pks -> actually auto generate id ?? so weired
	pkColumn := hp.GenColumnData(100, entity.FieldTypeInt64, *hp.TNewDataOption())
	upsertRes, err1 := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn, vecColumn))
	common.CheckErr(t, err1, true)
	require.EqualValues(t, nb, upsertRes.UpsertCount)

	// query and verify upsert result
	upsertPks := upsertRes.IDs.(*column.ColumnInt64).Data()
	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithOutputFields(common.DefaultFloatVecFieldName).
		WithFilter(fmt.Sprintf("%d <= %s", upsertPks[0], common.DefaultInt64FieldName)))
	common.CheckErr(t, err, true)
	common.EqualColumn(t, vecColumn, resSet.GetColumn(common.DefaultFloatVecFieldName))

	// upsert without pks -> error
	vecColumn = hp.GenColumnData(nb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(vecColumn))
	common.CheckErr(t, err, false, "has no corresponding fieldData pass in: invalid parameter")
}

// test upsert with invalid collection / partition name
func TestUpsertNotExistCollectionPartition(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// upsert not exist collection
	_, errUpsert := mc.Upsert(ctx, client.NewColumnBasedInsertOption("aaa"))
	common.CheckErr(t, errUpsert, false, "can't find collection")

	// create default collection with autoID true
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	_, errUpsert = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithPartition("aaa"))
	common.CheckErr(t, errUpsert, false, "num_rows should be greater than 0")

	// upsert not exist partition
	opt := *hp.TNewDataOption()
	pkColumn, vecColumn := hp.GenColumnData(10, entity.FieldTypeInt64, opt), hp.GenColumnData(10, entity.FieldTypeFloatVector, opt)
	_, errUpsert = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithPartition("aaa").WithColumns(pkColumn, vecColumn))
	common.CheckErr(t, errUpsert, false, "partition not found[partition=aaa]")
}

// test upsert with invalid column data
func TestUpsertInvalidColumnData(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create and insert
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	upsertNb := 10
	// 1. upsert missing columns
	opt := *hp.TNewDataOption()
	pkColumn, vecColumn := hp.GenColumnData(upsertNb, entity.FieldTypeInt64, opt), hp.GenColumnData(upsertNb, entity.FieldTypeFloatVector, opt)
	_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn))
	common.CheckErr(t, err, false, fmt.Sprintf("fieldSchema(%s) has no corresponding fieldData pass in", common.DefaultFloatVecFieldName))

	// 2. upsert extra a column
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn, vecColumn, vecColumn))
	common.CheckErr(t, err, false, fmt.Sprintf("duplicated column %s found", common.DefaultFloatVecFieldName))

	// 3. upsert vector has different dim
	dimColumn := hp.GenColumnData(upsertNb, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithDim(64))
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn, dimColumn))
	common.CheckErr(t, err, false, fmt.Sprintf("params column %s vector dim 64 not match collection definition, which has dim of %d",
		common.DefaultFloatVecFieldName, common.DefaultDim))

	// 4. different columns has different length
	diffLenColumn := hp.GenColumnData(upsertNb+1, entity.FieldTypeFloatVector, opt)
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn, diffLenColumn))
	common.CheckErr(t, err, false, "column size not match")

	// 5. column type different with schema
	varColumn := hp.GenColumnData(upsertNb, entity.FieldTypeVarChar, opt)
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn, varColumn, vecColumn))
	common.CheckErr(t, err, false, "field varchar does not exist in collection")

	// 6. empty column
	pkColumnEmpty, vecColumnEmpty := hp.GenColumnData(0, entity.FieldTypeInt64, opt), hp.GenColumnData(0, entity.FieldTypeFloatVector, opt)
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumnEmpty, vecColumnEmpty))
	common.CheckErr(t, err, false, "num_rows should be greater than 0")

	// 6. empty column
	pkColumnEmpty, vecColumnEmpty = hp.GenColumnData(0, entity.FieldTypeInt64, opt), hp.GenColumnData(10, entity.FieldTypeFloatVector, opt)
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumnEmpty, vecColumnEmpty))
	common.CheckErr(t, err, false, "invalid parameter[expected=need long int array][actual=got nil]")

	// 6. empty column
	pkColumnEmpty, vecColumnEmpty = hp.GenColumnData(10, entity.FieldTypeInt64, opt), hp.GenColumnData(0, entity.FieldTypeFloatVector, opt)
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumnEmpty, vecColumnEmpty))
	common.CheckErr(t, err, false, "column size not match")
}

func TestUpsertDynamicField(t *testing.T) {
	// enable dynamic field and insert dynamic column
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// verify that dynamic field exists
	upsertNb := 10
	resSet, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s < %d", common.DefaultDynamicNumberField, upsertNb)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, upsertNb, resSet.GetColumn(common.DefaultDynamicFieldName).Len())

	// 1. upsert exist pk without dynamic column
	opt := *hp.TNewDataOption()
	pkColumn, vecColumn := hp.GenColumnData(upsertNb, entity.FieldTypeInt64, opt), hp.GenColumnData(upsertNb, entity.FieldTypeFloatVector, opt)
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn, vecColumn))
	common.CheckErr(t, err, true)

	// query and gets empty
	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s < %d", common.DefaultDynamicNumberField, upsertNb)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 0, resSet.GetColumn(common.DefaultDynamicFieldName).Len())

	// 2. upsert not exist pk with dynamic column ->  field dynamicNumber does not exist in collection
	opt.TWithStart(common.DefaultNb)
	pkColumn2, vecColumn2 := hp.GenColumnData(upsertNb, entity.FieldTypeInt64, opt), hp.GenColumnData(upsertNb, entity.FieldTypeFloatVector, opt)
	dynamicColumns := hp.GenDynamicColumnData(common.DefaultNb, upsertNb)
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn2, vecColumn2).WithColumns(dynamicColumns...))
	common.CheckErr(t, err, true)

	// query and gets dynamic field
	resSet, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s >= %d", common.DefaultDynamicNumberField, common.DefaultNb)).
		WithOutputFields(common.DefaultDynamicFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.EqualColumn(t, hp.MergeColumnsToDynamic(upsertNb, dynamicColumns, common.DefaultDynamicFieldName), resSet.GetColumn(common.DefaultDynamicFieldName))
}

func TestUpsertWithoutLoading(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create and insert
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	// upsert
	upsertNb := 10
	opt := *hp.TNewDataOption()
	pkColumn, jsonColumn, vecColumn := hp.GenColumnData(upsertNb, entity.FieldTypeInt64, opt), hp.GenColumnData(upsertNb, entity.FieldTypeJSON, opt), hp.GenColumnData(upsertNb, entity.FieldTypeFloatVector, opt)
	_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn, jsonColumn, vecColumn))
	common.CheckErr(t, err, true)

	// index -> load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query and verify
	resSet, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, upsertNb)).
		WithOutputFields("*").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, []column.Column{pkColumn, jsonColumn, vecColumn}, resSet.Fields)
}

func TestUpsertPartitionKeyCollection(t *testing.T) {
	t.Skip("waiting gen partition key field")
}
