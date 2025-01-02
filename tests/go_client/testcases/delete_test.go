package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/remeh/sizedwaitgroup"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestDelete(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index and load collection
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete with expr
	expr := fmt.Sprintf("%s < 10", common.DefaultInt64FieldName)
	ids := []int64{10, 11, 12, 13, 14}
	delRes, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(expr))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(10), delRes.DeleteCount)

	// delete with int64 pk
	delRes, errDelete = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithInt64IDs(common.DefaultInt64FieldName, ids))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(5), delRes.DeleteCount)

	// query, verify delete success
	exprQuery := fmt.Sprintf("%s < 15", common.DefaultInt64FieldName)
	queryRes, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprQuery).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errQuery, true)
	require.Zero(t, queryRes.ResultCount)
}

// test delete with string pks
func TestDeleteVarcharPks(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index and load collection
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete varchar with pk
	ids := []string{"0", "1", "2", "3", "4"}
	expr := "varchar like '1%' "
	delRes, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithStringIDs(common.DefaultVarcharFieldName, ids))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(5), delRes.DeleteCount)

	delRes, errDelete = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(expr))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(1110), delRes.DeleteCount)

	// query, verify delete success
	exprQuery := "varchar like '1%' and varchar not in ['0', '1', '2', '3', '4'] "
	queryRes, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprQuery).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errQuery, true)
	require.Zero(t, queryRes.ResultCount)
}

// test delete from empty collection
func TestDeleteEmptyCollection(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// delete expr-in from empty collection
	delExpr := fmt.Sprintf("%s in [0]", common.DefaultInt64FieldName)
	delRes, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(delExpr))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(1), delRes.DeleteCount)

	// delete complex expr from empty collection
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	comExpr := fmt.Sprintf("%s < 10", common.DefaultInt64FieldName)
	delRes, errDelete = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(comExpr))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(0), delRes.DeleteCount)
}

// test delete from an not exist collection or partition
func TestDeleteNotExistName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// delete from not existed collection
	_, errDelete := mc.Delete(ctx, client.NewDeleteOption("aaa").WithExpr(""))
	common.CheckErr(t, errDelete, false, "collection not found")

	// delete from not existed partition
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	_, errDelete = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithPartition("aaa").WithExpr("int64 < 10"))
	common.CheckErr(t, errDelete, false, "partition not found[partition=aaa]")
}

// test delete with complex expr without loading
// delete without loading support: pk ids
func TestDeleteComplexExprWithoutLoad(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	cp := hp.NewCreateCollectionParams(hp.Int64VecAllScalar)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	idsPk := []int64{0, 1, 2, 3, 4}
	_, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithInt64IDs(common.DefaultInt64FieldName, idsPk))
	common.CheckErr(t, errDelete, true)

	_, errDelete = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithStringIDs(common.DefaultVarcharFieldName, []string{"0", "1"}))
	common.CheckErr(t, errDelete, false, "collection not loaded")

	// delete varchar with pk
	expr := fmt.Sprintf("%s < 100", common.DefaultInt64FieldName)
	_, errDelete2 := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(expr))
	common.CheckErr(t, errDelete2, false, "collection not loaded")

	// index and load collection
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	res, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s >= 0 ", common.DefaultInt64FieldName)).
		WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := res.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(common.DefaultNb-5), count)
}

// test delete with nil ids
func TestDeleteEmptyIds(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// delete
	_, err := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithInt64IDs(common.DefaultInt64FieldName, nil))
	common.CheckErr(t, err, false, "failed to create delete plan: cannot parse expression: int64 in []")

	_, err = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithInt64IDs(common.DefaultInt64FieldName, []int64{}))
	common.CheckErr(t, err, false, "failed to create delete plan: cannot parse expression: int64 in []")

	_, err = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithStringIDs(common.DefaultInt64FieldName, []string{""}))
	common.CheckErr(t, err, false, "failed to create delete plan: cannot parse expression: int64 in [\"\"]")

	t.Log("https://github.com/milvus-io/milvus/issues/33761")
	_, err = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(""))
	common.CheckErr(t, err, false, "delete plan can't be empty or always true")

	_, err = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName))
	common.CheckErr(t, err, false, "delete plan can't be empty or always true")
}

// test delete with string pks
func TestDeleteVarcharEmptyIds(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index and load collection
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	exprQuery := "varchar != '' "

	// delete varchar with empty ids
	delRes, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithStringIDs(common.DefaultVarcharFieldName, []string{}))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(0), delRes.DeleteCount)
	queryRes, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprQuery).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errQuery, true)
	require.Equal(t, common.DefaultNb, queryRes.ResultCount)

	// delete with default string ids
	delRes, errDelete = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithStringIDs(common.DefaultVarcharFieldName, []string{""}))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(1), delRes.DeleteCount)
	queryRes, errQuery = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprQuery).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errQuery, true)
	require.Equal(t, common.DefaultNb, queryRes.ResultCount)
}

// test delete with invalid ids
func TestDeleteInvalidIds(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	_, err := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithInt64IDs(common.DefaultVarcharFieldName, []int64{0}))
	common.CheckErr(t, err, false, "failed to create delete plan: cannot parse expression: varchar in [0]")

	_, err = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithInt64IDs(common.DefaultInt64FieldName, []int64{0}))
	common.CheckErr(t, err, false, "failed to create delete plan: cannot parse expression: int64 in [0]")

	_, err = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithStringIDs(common.DefaultInt64FieldName, []string{"0"}))
	common.CheckErr(t, err, false, "failed to create delete plan: cannot parse expression: int64 in [\"0\"]")
}

// test delete with non-pk ids
func TestDeleteWithIds(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection and a partition
	pkName := "pk"
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64)
	varcharField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(common.MaxLength)
	collName := common.GenRandomString(prefix, 6)
	schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithField(int64Field).WithField(varcharField)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// insert
	insertOpt := client.NewColumnBasedInsertOption(collName)
	for _, field := range schema.Fields {
		if field.Name == pkName {
			insertOpt.WithColumns(hp.GenColumnData(common.DefaultNb, field.DataType, *hp.TNewDataOption().TWithFieldName(pkName)))
		} else {
			insertOpt.WithColumns(hp.GenColumnData(common.DefaultNb, field.DataType, *hp.TNewDataOption()))
		}
	}
	_, err = mc.Insert(ctx, insertOpt)
	common.CheckErr(t, err, true)
	// index and load
	hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	hp.CollPrepare.Load(ctx, t, mc, hp.NewLoadParams(collName))

	// delete with non-pk fields ids
	resDe1, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithInt64IDs(common.DefaultInt64FieldName, []int64{0, 1}))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(2), resDe1.DeleteCount)

	resDe2, err2 := mc.Delete(ctx, client.NewDeleteOption(collName).WithStringIDs(common.DefaultVarcharFieldName, []string{"2", "3", "4"}))
	common.CheckErr(t, err2, true)
	require.Equal(t, int64(3), resDe2.DeleteCount)

	// query and verify
	resQuery, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("pk < 5").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Zero(t, resQuery.ResultCount)
}

// test delete with default partition name params
func TestDeleteDefaultPartitionName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection and a partition
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())
	parName := "p1"
	err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert [0, 3000) into default, insert [3000, 6000) into p1
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(parName), hp.TNewDataOption().TWithStart(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete with default params, actually delete from all partitions
	expr := fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)
	resDel, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(expr))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(common.DefaultNb*2), resDel.DeleteCount)

	// query, verify delete all partitions
	queryRes, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errQuery, true)
	require.Zero(t, queryRes.ResultCount)

	queryRes, errQuery = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithPartitions(common.DefaultPartition, parName).
		WithConsistencyLevel(entity.ClStrong).WithFilter(expr))
	common.CheckErr(t, errQuery, true)
	require.Zero(t, queryRes.ResultCount)
}

// test delete with empty partition "": actually delete from all partitions
func TestDeleteEmptyPartitionName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection and a partition
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())
	parName := "p1"
	err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert [0, 3000) into default, insert [3000, 6000) into p1
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(parName), hp.TNewDataOption().TWithStart(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete with default params, actually delete from all partitions
	expr := fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)
	resDel, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(expr).WithPartition(""))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(common.DefaultNb*2), resDel.DeleteCount)

	// query, verify delete all partitions
	queryRes, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errQuery, true)
	require.Zero(t, queryRes.ResultCount)

	queryRes, errQuery = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithPartitions(common.DefaultPartition, parName).
		WithConsistencyLevel(entity.ClStrong).WithFilter(expr))
	common.CheckErr(t, errQuery, true)
	require.Zero(t, queryRes.ResultCount)
}

// test delete with partition name
func TestDeletePartitionName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection and a partition
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())
	parName := "p1"
	err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert [0, 3000) into default, insert [3000, 6000) into parName
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(parName), hp.TNewDataOption().TWithStart(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete with default params, actually delete from all partitions
	exprDefault := fmt.Sprintf("%s < 200", common.DefaultInt64FieldName)
	exprP1 := fmt.Sprintf("%s >= 4500", common.DefaultInt64FieldName)
	exprQuery := fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)

	// delete ids that not existed in partition
	// delete [0, 200) from p1
	del1, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(exprDefault).WithPartition(parName))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(0), del1.DeleteCount)

	// delete [4800, 6000) from _default
	del2, errDelete := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(exprP1).WithPartition(common.DefaultPartition))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(0), del2.DeleteCount)

	// query and verify
	resQuery, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprQuery).WithOutputFields(common.QueryCountFieldName).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := resQuery.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(common.DefaultNb*2), count)

	// delete from partition
	del1, errDelete = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(exprDefault).WithPartition(common.DefaultPartition))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(200), del1.DeleteCount)

	del2, errDelete = mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(exprP1).WithPartition(parName))
	common.CheckErr(t, errDelete, true)
	require.Equal(t, int64(1500), del2.DeleteCount)

	// query, verify delete all partitions
	queryRes, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprQuery).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errQuery, true)
	require.Equal(t, common.DefaultNb*2-200-1500, queryRes.ResultCount)

	queryRes, errQuery = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprQuery).WithConsistencyLevel(entity.ClStrong).
		WithPartitions(common.DefaultPartition, parName))
	common.CheckErr(t, errQuery, true)
	require.Equal(t, common.DefaultNb*2-200-1500, queryRes.ResultCount)
}

// test delete ids field not pk int64
func TestDeleteComplexExpr(t *testing.T) {
	t.Parallel()

	type exprCount struct {
		expr  string
		count int
	}
	capacity := common.TestCapacity
	exprLimits := []exprCount{
		{expr: fmt.Sprintf("%s >= 1000 || %s > 2000", common.DefaultInt64FieldName, common.DefaultInt64FieldName), count: 2000},

		// json and dynamic field filter expr: == < in bool/ list/ int
		{expr: fmt.Sprintf("%s['number'] < 100 and %s['number'] != 0", common.DefaultJSONFieldName, common.DefaultJSONFieldName), count: 50},
		{expr: fmt.Sprintf("%s < 100", common.DefaultDynamicNumberField), count: 100},
		{expr: fmt.Sprintf("%s == false", common.DefaultDynamicBoolField), count: 2000},
		{expr: fmt.Sprintf("%s['string'] in ['1', '2', '5'] ", common.DefaultJSONFieldName), count: 3},
		{expr: fmt.Sprintf("%s['list'][0] < 10 ", common.DefaultJSONFieldName), count: 5},
		{expr: fmt.Sprintf("%s[\"dynamicList\"] != [2, 3]", common.DefaultDynamicFieldName), count: 0},

		// json contains
		{expr: fmt.Sprintf("json_contains (%s['list'], 2)", common.DefaultJSONFieldName), count: 1},
		{expr: fmt.Sprintf("JSON_CONTAINS_ANY (%s['list'], [1, 3])", common.DefaultJSONFieldName), count: 2},
		// string like
		{expr: "dynamicString like '1%' ", count: 1111},

		// key exist
		{expr: fmt.Sprintf("exists %s['list']", common.DefaultJSONFieldName), count: common.DefaultNb / 2},

		// data type not match and no error
		{expr: fmt.Sprintf("%s['number'] == '0' ", common.DefaultJSONFieldName), count: 0},

		// json field
		{expr: fmt.Sprintf("%s > 1499.5", common.DefaultJSONFieldName), count: 1500 / 2},                                 // json >= 1500.0
		{expr: fmt.Sprintf("%s like '21%%'", common.DefaultJSONFieldName), count: 100 / 4},                               // json like '21%'
		{expr: fmt.Sprintf("%s == [1503, 1504]", common.DefaultJSONFieldName), count: 1},                                 // json == [1,2]
		{expr: fmt.Sprintf("%s[0][0] > 1", common.DefaultJSONFieldName), count: 0},                                       // json == [1,2]
		{expr: fmt.Sprintf("%s[0] == false", common.DefaultBoolArrayField), count: common.DefaultNb / 2},                 //  array[0] ==
		{expr: fmt.Sprintf("%s[0] > 0", common.DefaultInt8ArrayField), count: 1524},                                      //  array[0] > int8 range: [-128, 127]
		{expr: fmt.Sprintf("json_contains (%s, 1)", common.DefaultInt32ArrayField), count: 2},                            // json_contains(array, 1)
		{expr: fmt.Sprintf("json_contains_any (%s, [0, 100, 10])", common.DefaultFloatArrayField), count: 101},           // json_contains_any (array, [x])
		{expr: fmt.Sprintf("%s == [0, 1]", common.DefaultDoubleArrayField), count: 0},                                    //  array ==
		{expr: fmt.Sprintf("array_length(%s) == %d", common.DefaultDoubleArrayField, capacity), count: common.DefaultNb}, //  array_length
	}
	sizedwaitgroup := sizedwaitgroup.New(5)
	testFunc := func(exprLimit exprCount) {
		defer sizedwaitgroup.Done()
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
		mc := createDefaultMilvusClient(ctx, t)

		// create collection and a partition
		cp := hp.NewCreateCollectionParams(hp.Int64VecAllScalar)
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

		// insert [0, 3000) into default
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithMaxCapacity(common.TestCapacity))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)

		// index and load
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		log.Debug("TestDeleteComplexExpr", zap.Any("expr", exprLimit.expr))

		resDe, err := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(exprLimit.expr))
		common.CheckErr(t, err, true)
		log.Debug("delete count", zap.Bool("equal", int64(exprLimit.count) == resDe.DeleteCount))
		// require.Equal(t, int64(exprLimit.count), resDe.DeleteCount)

		resQuery, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprLimit.expr).WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		require.Zero(t, resQuery.ResultCount)
	}

	for _, exprLimit := range exprLimits {
		exprLimit := exprLimit
		sizedwaitgroup.Add()
		go testFunc(exprLimit)
	}
	sizedwaitgroup.Wait()
}

func TestDeleteInvalidExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection and a partition
	cp := hp.NewCreateCollectionParams(hp.Int64VecAllScalar)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// insert [0, 3000) into default
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithMaxCapacity(common.TestCapacity))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	for _, _invalidExpr := range common.InvalidExpressions {
		_, err := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(_invalidExpr.Expr))
		common.CheckErr(t, err, _invalidExpr.ErrNil, _invalidExpr.ErrMsg)
	}
}

// test delete with duplicated data ids
func TestDeleteDuplicatedPks(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection and a partition
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithIsDynamic(true), hp.TNewSchemaOption())

	// insert [0, 3000) into default
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithMaxCapacity(common.TestCapacity))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index and load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete
	deleteIDs := []int64{0, 0, 0, 0, 0}
	delRes, err := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithInt64IDs(common.DefaultInt64FieldName, deleteIDs))
	common.CheckErr(t, err, true)
	require.Equal(t, 5, int(delRes.DeleteCount))

	// query, verify delete success
	expr := fmt.Sprintf("%s >= 0 ", common.DefaultInt64FieldName)
	resQuery, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errQuery, true)
	require.Equal(t, common.DefaultNb-1, resQuery.ResultCount)
}
