package testcases

import (
	"encoding/json"
	"fmt"
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

// test query from default partition
func TestQueryDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create and insert
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, insertRes := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	// flush -> index -> load
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query
	expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, 100)
	queryRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{insertRes.IDs.Slice(0, 100)})

	// query with limit
	LimitRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithLimit(10))
	common.CheckErr(t, err, true)
	require.Equal(t, 10, LimitRes.ResultCount)
	require.Equal(t, 10, LimitRes.GetColumn(common.DefaultInt64FieldName).Len())

	// get ids -> same result with query
	ids := hp.GenColumnData(100, entity.FieldTypeInt64, *hp.TNewDataOption().TWithFieldName(common.DefaultInt64FieldName))
	getRes, errGet := mc.Get(ctx, client.NewQueryOption(schema.CollectionName).WithIDs(ids))
	common.CheckErr(t, errGet, true)
	common.CheckQueryResult(t, getRes.Fields, []column.Column{insertRes.IDs.Slice(0, 100)})
}

// test query with varchar field filter
func TestQueryVarcharPkDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create and insert
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, insertRes := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	// flush -> index -> load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query
	expr := fmt.Sprintf("%s in ['0', '1', '2', '3', '4']", common.DefaultVarcharFieldName)
	queryRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{insertRes.IDs.Slice(0, 5)})

	// get ids -> same result with query
	varcharValues := []string{"0", "1", "2", "3", "4"}
	ids := column.NewColumnVarChar(common.DefaultVarcharFieldName, varcharValues)
	getRes, errGet := mc.Get(ctx, client.NewQueryOption(schema.CollectionName).WithIDs(ids).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errGet, true)
	common.CheckQueryResult(t, getRes.Fields, []column.Column{insertRes.IDs.Slice(0, 5)})
}

// test get with invalid ids
func TestGetInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create and insert
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	// flush -> index -> load
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// get ids with varchar ids -> error
	varcharValues := []string{"0", "1", "2", "3", "4"}
	ids := column.NewColumnVarChar(common.DefaultVarcharFieldName, varcharValues)
	_, errGet := mc.Get(ctx, client.NewQueryOption(schema.CollectionName).WithIDs(ids))
	common.CheckErr(t, errGet, false, "field varchar not exist: invalid parameter")

	// get ids with varchar ids -> error
	ids = column.NewColumnVarChar(common.DefaultInt64FieldName, varcharValues)
	_, errGet = mc.Get(ctx, client.NewQueryOption(schema.CollectionName).WithIDs(ids))
	common.CheckErr(t, errGet, false, "cannot parse expression: int64 in")

	// get ids with non-pk column -> error for empty filter
	t.Log("https://github.com/milvus-io/milvus/issues/38859")
	values := []float32{0.0, 1.0}
	ids2 := column.NewColumnFloat(common.DefaultInt64FieldName, values)
	_, errGet = mc.Get(ctx, client.NewQueryOption(schema.CollectionName).WithIDs(ids2))
	common.CheckErr(t, errGet, false, "empty expression should be used with limit")
}

// query from not existed collection name and partition name
func TestQueryNotExistName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// query with not existed collection
	expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, 100)
	_, errCol := mc.Query(ctx, client.NewQueryOption("aaa").WithFilter(expr))
	common.CheckErr(t, errCol, false, "can't find collection")

	// create -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query with not existed partition
	_, errPar := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions("aaa"))
	common.CheckErr(t, errPar, false, "partition name aaa not found")
}

// test query with invalid partition name
func TestQueryInvalidPartitionName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection and partition
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	expr := fmt.Sprintf("%s >= %d", common.DefaultInt64FieldName, 0)
	emptyPartitionName := ""
	// query from "" partitions, expect to query from default partition
	_, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions(emptyPartitionName))
	common.CheckErr(t, err, false, "Partition name should not be empty")
}

// test query with empty partition name
func TestQueryPartition(t *testing.T) {
	parName := "p1"

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection and partition
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert [0, 3000) into default, insert [3000, 6000) into parName
	_, i1Res := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	_, i2Res := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(parName), hp.TNewDataOption().TWithStart(common.DefaultNb))

	// flush -> index -> load
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	expr := fmt.Sprintf("%s >= %d", common.DefaultInt64FieldName, 0)
	expColumn := hp.GenColumnData(common.DefaultNb*2, entity.FieldTypeInt64, *hp.TNewDataOption().TWithStart(0))

	// query with default params, expect to query from all partitions
	queryRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{expColumn})

	// query with empty partition names
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions().WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{expColumn})

	// query with default partition
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions(common.DefaultPartition).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{i1Res.IDs})

	// query with specify partition
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions(parName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{i2Res.IDs})

	// query with all partitions
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions(common.DefaultPartition, parName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{expColumn})
}

// test query with invalid partition name
func TestQueryWithoutExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection and partition
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// query without expr
	_, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName))
	common.CheckErr(t, err, false, "empty expression should be used with limit")

	// query with empty expr
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(""))
	common.CheckErr(t, err, false, "empty expression should be used with limit")
}

// test query empty output fields: []string{} -> default pk
// test query empty output fields: []string{""} -> error
// test query with not existed field ["aa"]: error or as dynamic field
// test query with part not existed field ["aa", "$meat"]: error or as dynamic field
// test query with repeated field: ["*", "$meat"], ["floatVec", floatVec"] unique field
func TestQueryOutputFields(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, enableDynamic := range [2]bool{true, false} {
		// create -> insert -> flush -> index -> load
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(enableDynamic))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, 10)

		// query with empty output fields []string{}-> output "int64"
		queryNilOutputs, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields())
		common.CheckErr(t, err, true)
		common.CheckOutputFields(t, []string{common.DefaultInt64FieldName}, queryNilOutputs.Fields)

		// query with empty output fields []string{""}-> output "int64" and dynamic field
		_, err1 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields(""))
		if enableDynamic {
			common.CheckErr(t, err1, false, "parse output field name failed")
		} else {
			common.CheckErr(t, err1, false, "not exist")
		}

		// query with not existed field -> output empty data field as dynamic or error
		fakeName := "aaa"
		res2, err2 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields(fakeName))
		if enableDynamic {
			common.CheckErr(t, err2, true)
			for _, c := range res2.Fields {
				log.Info("data", zap.String("name", c.Name()), zap.Any("type", c.Type()), zap.Any("data", c.FieldData()))
			}
			common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, fakeName}, res2.Fields)
			dynamicColumn := hp.MergeColumnsToDynamic(10, []column.Column{}, common.DefaultDynamicFieldName)
			// dynamicColumn := hp.MergeColumnsToDynamic(10, hp.GenDynamicColumnData(0, 10), common.DefaultDynamicFieldName)
			expColumns := []column.Column{
				hp.GenColumnData(10, entity.FieldTypeInt64, *hp.TNewDataOption()),
				column.NewColumnDynamic(dynamicColumn, fakeName),
			}
			common.CheckQueryResult(t, expColumns, res2.Fields)
		} else {
			common.CheckErr(t, err2, false, fmt.Sprintf("%s not exist", fakeName))
		}

		// query with part not existed field ["aa", "$meat"]: error or as dynamic field
		res3, err3 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields(fakeName, common.DefaultDynamicFieldName))
		if enableDynamic {
			common.CheckErr(t, err3, true)
			common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, fakeName, common.DefaultDynamicFieldName}, res3.Fields)
		} else {
			common.CheckErr(t, err3, false, "not exist")
		}

		// query with repeated field: ["*", "$meat"], ["floatVec", floatVec"] unique field
		res4, err4 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields("*", common.DefaultDynamicFieldName))
		if enableDynamic {
			common.CheckErr(t, err4, true)
			common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultDynamicFieldName}, res4.Fields)
		} else {
			common.CheckErr(t, err4, false, "$meta not exist")
		}

		res5, err5 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields(common.DefaultFloatVecFieldName, common.DefaultFloatVecFieldName, common.DefaultInt64FieldName))
		common.CheckErr(t, err5, true)
		common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName}, res5.Fields)
	}
}

// test query output all fields and verify data
func TestQueryOutputAllFieldsColumn(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection
	for _, isDynamic := range [2]bool{true, false} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(isDynamic))
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// insert
		columns := make([]column.Column, 0, len(schema.Fields)+1)
		dynamicColumns := hp.GenDynamicColumnData(0, common.DefaultNb)
		genDataOpt := hp.TNewDataOption().TWithMaxCapacity(common.TestCapacity)
		insertOpt := client.NewColumnBasedInsertOption(schema.CollectionName)
		for _, field := range schema.Fields {
			if field.DataType == entity.FieldTypeArray {
				genDataOpt.TWithElementType(field.ElementType)
			}
			columns = append(columns, hp.GenColumnData(common.DefaultNb, field.DataType, *genDataOpt.TWithDim(common.DefaultDim)))
		}
		if isDynamic {
			insertOpt.WithColumns(dynamicColumns...)
		}
		ids, err := mc.Insert(ctx, insertOpt.WithColumns(columns...))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(common.DefaultNb), ids.InsertCount)
		prepare.FlushData(ctx, t, mc, schema.CollectionName)

		// query output all fields -> output all fields, includes vector and $meta field
		pos := 10
		allFieldsName := make([]string, 0, len(schema.Fields))
		for _, field := range schema.Fields {
			allFieldsName = append(allFieldsName, field.Name)
		}
		if isDynamic {
			allFieldsName = append(allFieldsName, common.DefaultDynamicFieldName)
		}
		queryResultAll, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).
			WithFilter(fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, pos)).WithOutputFields("*"))
		common.CheckErr(t, errQuery, true)
		common.CheckOutputFields(t, allFieldsName, queryResultAll.Fields)

		expColumns := make([]column.Column, 0, len(columns)+1)
		for _, _column := range columns {
			expColumns = append(expColumns, _column.Slice(0, pos))
		}
		if isDynamic {
			expColumns = append(expColumns, hp.MergeColumnsToDynamic(pos, dynamicColumns, common.DefaultDynamicFieldName))
		}
		common.CheckQueryResult(t, expColumns, queryResultAll.Fields)
	}
}

// test query output all fields
func TestQueryOutputAllFieldsRows(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus/issues/33459")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(),
		hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// prepare and insert data
	rows := hp.GenAllFieldsRows(common.DefaultNb, false, *hp.TNewDataOption().TWithMaxCapacity(common.TestCapacity))
	ids, err := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(common.DefaultNb), ids.InsertCount)

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query output all fields -> output all fields, includes vector and $meta field
	allFieldsName := []string{common.DefaultDynamicFieldName}
	for _, field := range schema.Fields {
		allFieldsName = append(allFieldsName, field.Name)
	}
	queryResultAll, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).
		WithFilter(fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, 10)).WithOutputFields("*"))
	common.CheckErr(t, errQuery, true)
	common.CheckOutputFields(t, allFieldsName, queryResultAll.Fields)
}

// test query output varchar and binaryVector fields
func TestQueryOutputBinaryAndVarchar(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// insert
	columns := make([]column.Column, 0, len(schema.Fields)+1)
	dynamicColumns := hp.GenDynamicColumnData(0, common.DefaultNb)

	for _, field := range schema.Fields {
		columns = append(columns, hp.GenColumnData(common.DefaultNb, field.DataType, *hp.TNewDataOption().TWithDim(common.DefaultDim)))
	}
	ids, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, columns...).WithColumns(dynamicColumns...))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(common.DefaultNb), ids.InsertCount)
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// query output all fields -> output all fields, includes vector and $meta field
	expr := fmt.Sprintf("%s in ['0', '1', '2', '3', '4', '5'] ", common.DefaultVarcharFieldName)
	allFieldsName := []string{common.DefaultVarcharFieldName, common.DefaultBinaryVecFieldName, common.DefaultDynamicFieldName}
	queryResultAll, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).
		WithFilter(expr).WithOutputFields("*"))
	common.CheckErr(t, errQuery, true)
	common.CheckOutputFields(t, allFieldsName, queryResultAll.Fields)

	expColumns := []column.Column{hp.MergeColumnsToDynamic(6, dynamicColumns, common.DefaultDynamicFieldName)}
	for _, _column := range columns {
		expColumns = append(expColumns, _column.Slice(0, 6))
	}
	common.CheckQueryResult(t, expColumns, queryResultAll.Fields)
}

func TestQueryOutputSparse(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus-sdk-go/issues/769")
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// insert
	columns := make([]column.Column, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		columns = append(columns, hp.GenColumnData(common.DefaultNb, field.DataType, *hp.TNewDataOption().TWithSparseMaxLen(10)))
	}

	ids, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, columns...))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(common.DefaultNb), ids.InsertCount)
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// query output all fields -> output all fields, includes vector and $meta field
	expr := fmt.Sprintf("%s < 100 ", common.DefaultInt64FieldName)
	expFieldsName := []string{common.DefaultInt64FieldName, common.DefaultVarcharFieldName, common.DefaultSparseVecFieldName}
	queryResultAll, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields("*"))
	common.CheckErr(t, errQuery, true)
	common.CheckOutputFields(t, expFieldsName, queryResultAll.Fields)

	expColumns := make([]column.Column, 0, len(columns))
	for _, _column := range columns {
		expColumns = append(expColumns, _column.Slice(0, 100))
	}
	common.CheckQueryResult(t, expColumns, queryResultAll.Fields)
}

// test query different array rows has different element length
func TestQueryArrayDifferentLenBetweenRows(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecAllScalar),
		hp.TNewFieldsOption().TWithMaxCapacity(common.TestCapacity*2), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// insert 2 batch with array capacity 100 and 200
	for i := 0; i < 2; i++ {
		columns := make([]column.Column, 0, len(schema.Fields))
		// each batch has different array capacity
		genDataOpt := hp.TNewDataOption().TWithMaxCapacity(common.TestCapacity * (i + 1)).TWithStart(common.DefaultNb * i)
		for _, field := range schema.Fields {
			if field.DataType == entity.FieldTypeArray {
				genDataOpt.TWithElementType(field.ElementType)
			}
			columns = append(columns, hp.GenColumnData(common.DefaultNb, field.DataType, *genDataOpt))
		}
		ids, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, columns...))
		common.CheckErr(t, err, true)
		require.Equal(t, int64(common.DefaultNb), ids.InsertCount)
	}
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// query array idx exceeds max capacity, array[200]
	expr := fmt.Sprintf("%s[%d] > 0", common.DefaultInt64ArrayField, common.TestCapacity*2)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(0), count)

	countRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields("Count(*)"))
	common.CheckErr(t, err, true)
	count, _ = countRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(0), count)

	// query: some rows has element greater than expr index array[100]
	expr2 := fmt.Sprintf("%s[%d] > 0", common.DefaultInt64ArrayField, common.TestCapacity)
	countRes2, err2 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr2).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err2, true)
	count2, _ := countRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(common.DefaultNb), count2)
}

// test query with expr and verify output dynamic field data
func TestQueryJsonDynamicExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	time.Sleep(400 * time.Millisecond)
	// query with different expr and count
	expr := fmt.Sprintf("%s['number'] < 10 || %s < 10", common.DefaultJSONFieldName, common.DefaultDynamicNumberField)

	queryRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong).
		WithOutputFields(common.DefaultJSONFieldName, common.DefaultDynamicFieldName))

	// verify output fields and count, dynamicNumber value
	common.CheckErr(t, err, true)
	common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultJSONFieldName, common.DefaultDynamicFieldName}, queryRes.Fields)
	require.Equal(t, 10, queryRes.ResultCount)
	for _, _column := range queryRes.Fields {
		if _column.Name() == common.DefaultDynamicNumberField {
			var numberData []int64
			for i := 0; i < _column.Len(); i++ {
				line, _ := _column.GetAsInt64(i)
				numberData = append(numberData, line)
			}
			require.Equal(t, numberData, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
		}
	}
}

// test query with invalid expr
func TestQueryInvalidExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	for _, _invalidExpr := range common.InvalidExpressions {
		_, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(_invalidExpr.Expr))
		common.CheckErr(t, err, _invalidExpr.ErrNil, _invalidExpr.ErrMsg)
	}
}

// Test query json and dynamic collection with string expr
func TestQueryCountJsonDynamicExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	time.Sleep(400 * time.Millisecond)
	// query with different expr and count
	type exprCount struct {
		expr  string
		count int64
	}
	exprCounts := []exprCount{
		{expr: "", count: common.DefaultNb},
		// pk int64 field expr: < in && ||
		{expr: fmt.Sprintf("%s < 1000", common.DefaultInt64FieldName), count: 1000},
		{expr: fmt.Sprintf("%s in [0, 1, 2]", common.DefaultInt64FieldName), count: 3},
		{expr: fmt.Sprintf("%s >= 1000 && %s < 2000", common.DefaultInt64FieldName, common.DefaultInt64FieldName), count: 1000},
		{expr: fmt.Sprintf("%s >= 1000 || %s > 2000", common.DefaultInt64FieldName, common.DefaultInt64FieldName), count: 2000},
		{expr: fmt.Sprintf("%s < 1000", common.DefaultFloatFieldName), count: 1000},

		// json and dynamic field filter expr: == < in bool/ list/ int
		{expr: fmt.Sprintf("%s['number'] == 0", common.DefaultJSONFieldName), count: 0},
		{expr: fmt.Sprintf("%s['number'] < 100 and %s['number'] != 0", common.DefaultJSONFieldName, common.DefaultJSONFieldName), count: 50},
		{expr: fmt.Sprintf("%s < 100", common.DefaultDynamicNumberField), count: 100},
		{expr: "dynamicNumber % 2 == 0", count: 1500},
		{expr: fmt.Sprintf("%s['bool'] == true", common.DefaultJSONFieldName), count: 1500 / 2},
		{expr: fmt.Sprintf("%s == false", common.DefaultDynamicBoolField), count: 2000},
		{expr: fmt.Sprintf("%s in ['1', '2'] ", common.DefaultDynamicStringField), count: 2},
		{expr: fmt.Sprintf("%s['string'] in ['1', '2', '5'] ", common.DefaultJSONFieldName), count: 3},
		{expr: fmt.Sprintf("%s['list'] == [1, 2] ", common.DefaultJSONFieldName), count: 1},
		{expr: fmt.Sprintf("%s['list'] == [0, 1] ", common.DefaultJSONFieldName), count: 0},
		{expr: fmt.Sprintf("%s['list'][0] < 10 ", common.DefaultJSONFieldName), count: 5},
		{expr: fmt.Sprintf("%s[\"dynamicList\"] != [2, 3]", common.DefaultDynamicFieldName), count: 0},

		// json contains
		{expr: fmt.Sprintf("json_contains (%s['list'], 2)", common.DefaultJSONFieldName), count: 1},
		{expr: fmt.Sprintf("json_contains (%s['number'], 0)", common.DefaultJSONFieldName), count: 0},
		{expr: fmt.Sprintf("json_contains_all (%s['list'], [1, 2])", common.DefaultJSONFieldName), count: 1},
		{expr: fmt.Sprintf("JSON_CONTAINS_ANY (%s['list'], [1, 3])", common.DefaultJSONFieldName), count: 2},
		// string like
		{expr: "dynamicString like '1%' ", count: 1111},

		// key exist
		{expr: fmt.Sprintf("exists %s['list']", common.DefaultJSONFieldName), count: common.DefaultNb / 4},
		{expr: "exists a ", count: 0},
		{expr: fmt.Sprintf("exists %s ", common.DefaultDynamicListField), count: common.DefaultNb},
		{expr: fmt.Sprintf("exists %s ", common.DefaultDynamicStringField), count: common.DefaultNb},
		// data type not match and no error
		{expr: fmt.Sprintf("%s['number'] == '0' ", common.DefaultJSONFieldName), count: 0},

		// json field
		{expr: fmt.Sprintf("%s >= 1500", common.DefaultJSONFieldName), count: 1500 / 2},    // json >= 1500
		{expr: fmt.Sprintf("%s > 1499.5", common.DefaultJSONFieldName), count: 1500 / 2},   // json >= 1500.0
		{expr: fmt.Sprintf("%s like '21%%'", common.DefaultJSONFieldName), count: 100 / 4}, // json like '21%'
		{expr: fmt.Sprintf("%s == [1503, 1504]", common.DefaultJSONFieldName), count: 1},   // json == [1,2]
		{expr: fmt.Sprintf("%s[0] > 1", common.DefaultJSONFieldName), count: 1500 / 4},     // json[0] > 1
		{expr: fmt.Sprintf("%s[0][0] > 1", common.DefaultJSONFieldName), count: 0},         // json == [1,2]

		// Key and value types do not match
		{expr: fmt.Sprintf("%s['float'] <= 3000", common.DefaultJSONFieldName), count: common.DefaultNb / 4},
		{expr: fmt.Sprintf("%s['float'] <= 3000.0", common.DefaultJSONFieldName), count: common.DefaultNb / 4},
		{expr: fmt.Sprintf("%s['string'] > 0", common.DefaultJSONFieldName), count: 0},
		{expr: fmt.Sprintf("%s['floatArray'][0] < 1000.0", common.DefaultJSONFieldName), count: 500},
		{expr: fmt.Sprintf("%s['stringArray'][0] == '00100'", common.DefaultJSONFieldName), count: 1},
	}

	for _, _exprCount := range exprCounts {
		log.Debug("TestQueryCountJsonDynamicExpr", zap.String("expr", _exprCount.expr))
		countRes, _ := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(_exprCount.expr).WithOutputFields(common.QueryCountFieldName))
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.Equal(t, _exprCount.count, count)
	}
}

func TestQueryNestedJsonExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	jsonValues := make([][]byte, 0, common.DefaultNb)
	nestedDepth := 100
	for i := 0; i < common.DefaultNb; i++ {
		var m map[string]interface{}
		if i%2 == 0 {
			m = make(map[string]interface{})
		} else {
			m = hp.GenNestedJSON(nestedDepth, i)
		}
		bs, _ := json.Marshal(&m)
		jsonValues = append(jsonValues, bs)
	}
	jsonColumn := column.NewColumnJSONBytes(common.DefaultJSONFieldName, jsonValues)
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn, jsonColumn))
	common.CheckErr(t, err, true)

	type exprCount struct {
		expr  string
		count int64
	}
	exprKey := hp.GenNestedJSONExprKey(nestedDepth, common.DefaultJSONFieldName)
	nestedExpr := exprKey + " < 1000 "
	t.Log("https://github.com/milvus-io/milvus/issues/39822")
	exprCounts := []exprCount{
		//{expr: fmt.Sprintf("json_length(%s) == 0", common.DefaultJSONFieldName), count: common.DefaultNb / 2},
		{expr: nestedExpr, count: 500},
	}
	for _, _exprCount := range exprCounts {
		log.Info("TestQueryCountJsonDynamicExpr", zap.String("expr", _exprCount.expr))
		countRes, _ := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(_exprCount.expr).WithOutputFields(common.QueryCountFieldName))
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.Equal(t, _exprCount.count, count)
	}
}

func TestQueryNumberJsonExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// Generate test data with boundary values
	pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	jsonValues := make([][]byte, 0, common.DefaultNb)

	// Define boundary values for different numeric types
	boundaryValues := map[string]interface{}{
		"int8_min":     int8(-128),
		"int8_max":     int8(127),
		"int16_min":    int16(-32768),
		"int16_max":    int16(32767),
		"int32_min":    int32(-2147483648),
		"int32_max":    int32(2147483647),
		"int64_min":    int64(-9223372036854775808),
		"int64_max":    int64(9223372036854775807),
		"float_min":    float32(-3.402823e+38),
		"float_max":    float32(3.402823e+38),
		"double_min":   float64(-1.7976931348623157e+308),
		"double_max":   float64(1.7976931348623157e+308),
		"array_int8":   []int8{-128, 127},
		"array_int16":  []int16{-32768, 32767},
		"array_int32":  []int32{-2147483648, 2147483647},
		"array_int64":  []int64{-9223372036854775808, 9223372036854775807},
		"array_float":  []float64{-3.402823e+38, 3.402823e+38},
		"array_double": []float64{-1.7976931348623157e+308, 1.7976931348623157e+308},
	}

	// Generate JSON documents
	expCount := 100
	for i := 0; i < common.DefaultNb; i++ {
		m := make(map[string]interface{})
		if i < expCount {
			for k, v := range boundaryValues {
				m[k] = v
			}
		} else {
			// Rest documents contain regular values
			m["val1"] = 0e+1
			m["val2"] = 1e22
			m["val3"] = -0.000000000000000000000000000000000000000000000000000000000000000000000000000001
			m["val4"] = -0
			m["val5"] = -123
			m["val6"] = 123.456e78
			m["val7"] = 123.456789
		}
		bs, _ := json.Marshal(&m)
		jsonValues = append(jsonValues, bs)
	}
	jsonColumn := column.NewColumnJSONBytes(common.DefaultJSONFieldName, jsonValues)
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn, jsonColumn))
	common.CheckErr(t, err, true)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Test queries with boundary values
	t.Log("https://github.com/milvus-io/milvus/issues/40707")
	t.Log("https://github.com/milvus-io/milvus/issues/40729")
	testCases := []struct {
		expr  string
		count int
	}{
		// Test int8 boundary values
		{expr: fmt.Sprintf("%s['int8_min'] == -128", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['int8_max'] == 127", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['array_int8'][0] < 0", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['array_int8'][0] == -128", common.DefaultJSONFieldName), count: expCount},

		// Test int16 boundary values
		{expr: fmt.Sprintf("%s['int16_min'] == -32768", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['int16_max'] == 32767", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['array_int16'][0] == -32768", common.DefaultJSONFieldName), count: expCount},

		// Test int32 boundary values
		{expr: fmt.Sprintf("%s['int32_min'] == -2147483648", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['int32_max'] == 2147483647", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['array_int32'][0] == -2147483648", common.DefaultJSONFieldName), count: expCount},

		// Test int64 boundary values
		//{expr: fmt.Sprintf("%s['int64_min'] == -9223372036854775808", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['int64_max'] == 9223372036854775807", common.DefaultJSONFieldName), count: expCount},
		//{expr: fmt.Sprintf("%s['array_int64'][0] == -9223372036854775808", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['array_int64'][1] == 9223372036854775807", common.DefaultJSONFieldName), count: expCount},

		// Test float boundary values (approximate comparison due to floating point precision)
		{expr: fmt.Sprintf("%s['float_min'] <= -3.402823e+38", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['float_max'] >= 3.402823e+38", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['array_float'][0] == -3.402823e+38", common.DefaultJSONFieldName), count: expCount},

		// Test double boundary values (approximate comparison due to floating point precision)
		{expr: fmt.Sprintf("%s['double_min'] <= -1.7976931348623157e+308", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['double_max'] >= 1.7976931348623157e+308", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['array_double'][0] == -1.7976931348623157e+308", common.DefaultJSONFieldName), count: expCount},
		{expr: fmt.Sprintf("%s['array_double'][1] == 1.7976931348623157e+308", common.DefaultJSONFieldName), count: expCount},

		// Test regular value queries
		{expr: fmt.Sprintf("%s['val1'] == 0e+1", common.DefaultJSONFieldName), count: common.DefaultNb - expCount},
		{expr: fmt.Sprintf("%s['val2'] == 1e22", common.DefaultJSONFieldName), count: common.DefaultNb - expCount},
		{expr: fmt.Sprintf("%s['val3'] < 0", common.DefaultJSONFieldName), count: common.DefaultNb - expCount},
		{expr: fmt.Sprintf("%s['val4'] == 0", common.DefaultJSONFieldName), count: common.DefaultNb - expCount},
		{expr: fmt.Sprintf("%s['val5'] == -123", common.DefaultJSONFieldName), count: common.DefaultNb - expCount},
		{expr: fmt.Sprintf("%s['val6'] == 123.456e78", common.DefaultJSONFieldName), count: common.DefaultNb - expCount},
		{expr: fmt.Sprintf("%s['val7'] == 123.456789", common.DefaultJSONFieldName), count: common.DefaultNb - expCount},
	}

	for _, tc := range testCases {
		t.Run(tc.expr, func(t *testing.T) {
			countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).
				WithFilter(tc.expr).WithOutputFields(common.QueryCountFieldName))
			common.CheckErr(t, err, true)
			count, _ := countRes.Fields[0].GetAsInt64(0)
			require.EqualValues(t, tc.count, count, "Query expression: %s", tc.expr)
		})
	}
}

func TestQueryObjectJsonExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// Generate test data with boundary values
	pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	jsonValues := make([][]byte, 0, common.DefaultNb)

	// Define boundary values for different numeric types
	boundaryValues := map[string]interface{}{
		"varchar": map[string]string{
			"a":        "b",
			"π":        "π",
			"asd ":     " ", //nolint
			"utf8":     "€𝄞",
			"comments": "a/*b*/c/*d//e",
			"unicode":  "\u041f\u043e\u043b\u0442\u043e\u0440\u0430", // "Полтора"
		},
		"escape": map[string]string{
			"allowed_escape":   "\"\\/\b\f\n\n\t",
			"double_escape":    "aa\n",
			"null_escape":      "\u0000",
			"uescaped_newline": "new\u000Aline",
		},
		"int64": map[string]int64{
			"":             1,
			"foo\u0000bar": 42,
			"min":          -9223372036854775808,
			"max":          9223372036854775807,
		},
		"interface": []map[string]interface{}{
			{"string": []map[string]interface{}{{"id": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}}, "id": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},
			{"int8": []int8{1, -128, 127}},
			{"int16": []int16{-1, 1, -32768, 32767}},
			{"int32": []int32{-1, 1, -2147483648, 2147483647}},
			{"int64": []int64{-1, 1, -9223372036854775808, 9223372036854775807}},
			{"float32": []float32{-1.0, 1.0, -3.402823e+38, 3.402823e+38}},
			{"double": []float64{-1.0, 1.0, -1.7976931348623157e+308, 1.7976931348623157e+308}},
			{"bool": []bool{true, false}},
			{"empty_array": []float32{}},
		},
		"language": []map[string]interface{}{
			{"中文": "月亮"}, {"English": "moon"}, {"중국인": "달"}, {"日本語": "シャオミン"},
		},
	}
	// Generate JSON documents
	for i := 0; i < common.DefaultNb; i++ {
		m := make(map[string]interface{})
		for k, v := range boundaryValues {
			m[k] = v
		}
		bs, _ := json.Marshal(&m)
		jsonValues = append(jsonValues, bs)
	}
	jsonColumn := column.NewColumnJSONBytes(common.DefaultJSONFieldName, jsonValues)
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn, jsonColumn))
	common.CheckErr(t, err, true)

	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Test queries with boundary values
	t.Log("https://github.com/milvus-io/milvus/issues/40729")
	testCases := []struct {
		expr  string
		count int
	}{
		// Test varchar
		// Test varchar
		{expr: fmt.Sprintf("%s['varchar']['a'] == 'b'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['varchar']['π'] == 'π'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['varchar']['asd '] == ' '", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['varchar']['utf8'] == '€𝄞'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['varchar']['comments'] == 'a/*b*/c/*d//e'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['varchar']['unicode'] == 'Полтора' ", common.DefaultJSONFieldName), count: common.DefaultNb},

		// Test escape boundary values
		{expr: fmt.Sprintf("%s['escape']['allowed_escape'] == '\\\"\\\\/\\b\\f\\n\\n\\t'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['escape']['double_escape'] == 'aa\\n'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['escape']['null_escape'] == '\u0000'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['escape']['uescaped_newline'] == 'new\\nline'", common.DefaultJSONFieldName), count: common.DefaultNb},

		// Test int64 boundary values
		//{expr: fmt.Sprintf("%s['int64'][''] == 1", common.DefaultJSONFieldName), count: common.DefaultNb},
		//{expr: fmt.Sprintf("%s['int64']['foo\\u0000bar'] == 42", common.DefaultJSONFieldName), count: common.DefaultNb},
		//{expr: fmt.Sprintf("%s['int64']['min'] == -9223372036854775808", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['int64']['max'] == 9223372036854775807", common.DefaultJSONFieldName), count: common.DefaultNb},

		// Test interface boundary values
		{expr: fmt.Sprintf("%s['interface'][0]['id'] == 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][0]['string'][0]['id'] == 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][1]['int8'][0] == 1", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][1]['int8'][1] == -128", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][1]['int8'][2] == 127", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][2]['int16'][0] == -1", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][2]['int16'][1] == 1", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][2]['int16'][2] == -32768", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][2]['int16'][3] == 32767", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][3]['int32'][0] == -1", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][3]['int32'][1] == 1", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][3]['int32'][2] == -2147483648", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][3]['int32'][3] == 2147483647", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][4]['int64'][0] == -1", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][4]['int64'][1] == 1", common.DefaultJSONFieldName), count: common.DefaultNb},
		//{expr: fmt.Sprintf("%s['interface'][4]['int64'][2] == -9223372036854775808", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][4]['int64'][3] == 9223372036854775807", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][5]['float32'][0] == -1.0", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][5]['float32'][1] == 1.0", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][5]['float32'][2] == -3.402823e+38", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][5]['float32'][3] == 3.402823e+38", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][6]['double'][0] == -1.0", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][6]['double'][1] == 1.0", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][6]['double'][2] == -1.7976931348623157e+308", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][6]['double'][3] == 1.7976931348623157e+308", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][7]['bool'][0] == true", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['interface'][7]['bool'][1] == false", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("array_length(%s['interface'][0]['empty_array']) == 0", common.DefaultJSONFieldName), count: common.DefaultNb},

		// language
		{expr: fmt.Sprintf("%s['language'][0]['中文'] == '月亮'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['language'][1]['English'] == 'moon'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['language'][2]['중국인'] == '달'", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s['language'][3]['日本語'] == 'シャオミン'", common.DefaultJSONFieldName), count: common.DefaultNb},
	}

	for _, tc := range testCases {
		t.Run(tc.expr, func(t *testing.T) {
			countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).
				WithFilter(tc.expr).WithOutputFields(common.QueryCountFieldName))
			common.CheckErr(t, err, true)
			count, _ := countRes.Fields[0].GetAsInt64(0)
			require.EqualValues(t, tc.count, count, "Query expression: %s", tc.expr)
		})
	}
}

func TestQueryNullJsonExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	jsonField := entity.NewField().WithName(common.DefaultJSONFieldName).WithDataType(entity.FieldTypeJSON).WithNullable(true)
	schema := entity.NewSchema().WithName(common.GenRandomString("null_json", 10)).WithField(pkField).WithField(vecField).WithField(jsonField)
	mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))

	prepare := hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Generate test data with boundary values
	pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	jsonValues := make([][]byte, 0, common.DefaultNb)
	validData := make([]bool, 0, common.DefaultNb)

	// Generate JSON documents
	for i := 0; i < common.DefaultNb; i++ {
		_mod := i % 3
		var m interface{}
		if _mod == 0 {
			m = map[string][]float32{"value": {0.1, 0.1, 0.1, 0.1, 0.1}}
			bs, _ := json.Marshal(&m)
			jsonValues = append(jsonValues, bs)
			validData = append(validData, true)
		} else if _mod == 1 {
			m = map[string][]string{"value": {"zz0", "zz0", "zz0", "zz0"}}
			bs, _ := json.Marshal(&m)
			jsonValues = append(jsonValues, bs)
			validData = append(validData, true)
		} else {
			validData = append(validData, false)
		}
	}
	nullColumn, err := column.NewNullableColumnJSONBytes(common.DefaultJSONFieldName, jsonValues, validData)
	common.CheckErr(t, err, true)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn, nullColumn))
	common.CheckErr(t, err, true)

	// Test queries with boundary values
	testCases := []struct {
		expr  string
		count int
	}{
		// Test null boundary values
		{expr: fmt.Sprintf("%s is null", common.DefaultJSONFieldName), count: common.DefaultNb / 3},
		{expr: fmt.Sprintf("%s IS NULL", common.DefaultJSONFieldName), count: common.DefaultNb / 3},
		{expr: fmt.Sprintf("%s IS NOT NULL", common.DefaultJSONFieldName), count: common.DefaultNb * 2 / 3},
		{expr: fmt.Sprintf("%s is not null", common.DefaultJSONFieldName), count: common.DefaultNb * 2 / 3},

		{expr: fmt.Sprintf("%s['value'][0] == 'zz0' ", common.DefaultJSONFieldName), count: common.DefaultNb / 3},
		{expr: fmt.Sprintf("%s['value'][0] == 0.1 ", common.DefaultJSONFieldName), count: common.DefaultNb / 3},
	}

	for _, tc := range testCases {
		t.Run(tc.expr, func(t *testing.T) {
			countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).
				WithFilter(tc.expr).WithOutputFields(common.QueryCountFieldName))
			common.CheckErr(t, err, true)
			count, _ := countRes.Fields[0].GetAsInt64(0)
			require.EqualValues(t, tc.count, count, "Query expression: %s", tc.expr)
		})
	}

	for _, invalidExpr := range []string{
		fmt.Sprintf("%s is NOT NULL", common.DefaultJSONFieldName),
		fmt.Sprintf("%s IS not NULL", common.DefaultJSONFieldName),
	} {
		_, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
			WithConsistencyLevel(entity.ClStrong).
			WithFilter(invalidExpr).
			WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, false, "cannot parse expression")
	}
}

// test query with all kinds of array expr
func TestQueryArrayFieldExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// create collection
	capacity := int64(common.TestCapacity)
	type exprCount struct {
		expr  string
		count int64
	}
	exprCounts := []exprCount{
		{expr: fmt.Sprintf("%s[0] == false", common.DefaultBoolArrayField), count: common.DefaultNb / 2},                 //  array[0] ==
		{expr: fmt.Sprintf("%s[0] > 0", common.DefaultInt64ArrayField), count: common.DefaultNb - 1},                     //  array[0] >
		{expr: fmt.Sprintf("%s[0] > 0", common.DefaultInt8ArrayField), count: 1524},                                      //  array[0] > int8 range: [-128, 127]
		{expr: fmt.Sprintf("json_contains (%s, %d)", common.DefaultInt16ArrayField, capacity), count: capacity},          // json_contains(array, 1)
		{expr: fmt.Sprintf("array_contains (%s, %d)", common.DefaultInt16ArrayField, capacity), count: capacity},         // array_contains(array, 1)
		{expr: fmt.Sprintf("array_contains (%s, 1)", common.DefaultInt32ArrayField), count: 2},                           // array_contains(array, 1)
		{expr: fmt.Sprintf("json_contains (%s, 1)", common.DefaultInt32ArrayField), count: 2},                            // json_contains(array, 1)
		{expr: fmt.Sprintf("array_contains (%s, 1000000)", common.DefaultInt32ArrayField), count: 0},                     // array_contains(array, 1)
		{expr: fmt.Sprintf("json_contains_all (%s, [90, 91])", common.DefaultInt64ArrayField), count: 91},                // json_contains_all(array, [x])
		{expr: fmt.Sprintf("array_contains_all (%s, [1, 2])", common.DefaultInt64ArrayField), count: 2},                  // array_contains_all(array, [x])
		{expr: fmt.Sprintf("array_contains_any (%s, [0, 100, 10000])", common.DefaultFloatArrayField), count: 101},       // array_contains_any(array, [x])
		{expr: fmt.Sprintf("json_contains_any (%s, [0, 100, 10])", common.DefaultFloatArrayField), count: 101},           // json_contains_any (array, [x])
		{expr: fmt.Sprintf("%s == [0, 1]", common.DefaultDoubleArrayField), count: 0},                                    //  array ==
		{expr: fmt.Sprintf("array_length(%s) == 10", common.DefaultVarcharArrayField), count: 0},                         //  array_length
		{expr: fmt.Sprintf("array_length(%s) == %d", common.DefaultDoubleArrayField, capacity), count: common.DefaultNb}, //  array_length
	}

	for _, _exprCount := range exprCounts {
		log.Debug("TestQueryCountJsonDynamicExpr", zap.String("expr", _exprCount.expr))
		countRes, _ := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(_exprCount.expr).WithOutputFields(common.QueryCountFieldName))
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.Equal(t, _exprCount.count, count)
	}
}

// test query output invalid count(*) fields
func TestQueryOutputInvalidOutputFieldCount(t *testing.T) {
	type invalidCountStruct struct {
		countField string
		errMsg     string
	}
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(false))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// invalid expr
	invalidOutputFieldCount := []invalidCountStruct{
		{countField: "ccount(*)", errMsg: "field ccount(*) not exist"},
		{countField: "count[*]", errMsg: "field count[*] not exist"},
		{countField: "count", errMsg: "field count not exist"},
		{countField: "count(**)", errMsg: "field count(**) not exist"},
	}
	for _, invalidCount := range invalidOutputFieldCount {
		queryExpr := fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)

		// query with empty output fields []string{}-> output "int64"
		_, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(queryExpr).WithOutputFields(invalidCount.countField))
		common.CheckErr(t, err, false, invalidCount.errMsg)
	}
}

func TestQueryWithTemplateParam(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	time.Sleep(400 * time.Millisecond)
	// query
	int64Values := make([]int64, 0, 1000)
	for i := 10; i < 10+1000; i++ {
		int64Values = append(int64Values, int64(i))
	}
	// default
	queryRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
		WithFilter(fmt.Sprintf("%s in {int64Values}", common.DefaultInt64FieldName)).WithTemplateParam("int64Values", int64Values).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{column.NewColumnInt64(common.DefaultInt64FieldName, int64Values)})

	// cover keys
	res, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("int64 < {k2}").WithTemplateParam("k2", 10).WithTemplateParam("k2", 5).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 5, res.ResultCount)

	// array contains
	anyValues := []int64{0.0, 100.0, 10000.0}
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
		WithFilter(fmt.Sprintf("json_contains_any (%s, {any_values})", common.DefaultFloatArrayField)).WithTemplateParam("any_values", anyValues).
		WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 101, count)

	// dynamic
	countRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
		WithFilter("dynamicNumber % 2 == {v}").WithTemplateParam("v", 0).WithOutputFields(common.QueryCountFieldName).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 1500, count)

	// json['bool']
	countRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
		WithFilter(fmt.Sprintf("%s['bool'] == {v}", common.DefaultJSONFieldName)).
		WithTemplateParam("v", false).
		WithOutputFields(common.QueryCountFieldName).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 1500/2, count)

	// bool
	countRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
		WithFilter(fmt.Sprintf("%s == {v}", common.DefaultBoolFieldName)).
		WithTemplateParam("v", true).
		WithOutputFields(common.QueryCountFieldName).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	count, _ = countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb/2, count)

	// and {expr: fmt.Sprintf("%s >= 1000 && %s < 2000", common.DefaultInt64FieldName, common.DefaultInt64FieldName), count: 1000},
	res, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
		WithFilter(fmt.Sprintf("%s >= {k1} && %s < {k2}", common.DefaultInt64FieldName, common.DefaultInt64FieldName)).
		WithTemplateParam("v", 0).WithTemplateParam("k1", 1000).
		WithTemplateParam("k2", 2000).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 1000, res.ResultCount)
}

func TestQueryWithTemplateParamInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	time.Sleep(400 * time.Millisecond)
	// query with invalid template
	// expr := "varchar like 'a%' "
	_, err2 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("varchar like {key1}").WithTemplateParam("key1", "'a%'"))
	common.CheckErr(t, err2, false, "mismatched input '{' expecting StringLiteral")

	// no template param
	_, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("int64 in {key1}"))
	common.CheckErr(t, err, false, "the value of expression template variable name {key1} is not found")

	// template param with empty expr
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("").WithTemplateParam("a", 12))
	common.CheckErr(t, err, false, "empty expression should be used with limit")

	// *** template param with field name key -> error ***
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("{field} < 10").WithTemplateParam("field", "int64"))
	common.CheckErr(t, err, false, "cannot parse expression")
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("{field} < {v}").WithTemplateParam("field", "int64").WithTemplateParam("v", 10))
	common.CheckErr(t, err, false, "placeholder was not supported between two constants with operator")
	// exists x
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("exists {x}").WithTemplateParam("x", "json"))
	common.CheckErr(t, err, false, "exists operations are only supported on single fields now")

	// compare two fields
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("{f1} > {f2}").WithTemplateParam("f1", "f1").WithTemplateParam("f2", "f2"))
	common.CheckErr(t, err, false, "placeholder was not supported between two constants with operator")

	// expr key != template key
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("int64 in {key1}").WithTemplateParam("key2", []int64{0, 1, 2}))
	common.CheckErr(t, err, false, "the value of expression template variable name {key1} is not found")

	// template missing some keys
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("{k1} < int64 < {k2}").WithTemplateParam("k1", 10))
	common.CheckErr(t, err, false, "the upper value of expression template variable name {k2} is not found")

	// template value type is valid
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("int64 < {k1}").WithTemplateParam("k1", []int64{0, 1, 3}))
	common.CheckErr(t, err, false, "cannot cast value to Int64")

	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("int64 < {k1}").WithTemplateParam("k1", "10"))
	common.CheckErr(t, err, false, "cannot cast value to Int64")

	// invalid expr
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("{name} == 'O'Reilly'").WithTemplateParam("name", common.DefaultVarcharFieldName))
	common.CheckErr(t, err, false, "cannot parse expression")

	// invalid expr
	_, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("{_123} > 10").WithTemplateParam("_123", common.DefaultInt64FieldName))
	common.CheckErr(t, err, false, "cannot parse expression")
}

func TestRunAnalyzer(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// run analyzer with default analyzer
	tokens, err := mc.RunAnalyzer(ctx, client.NewRunAnalyzerOption([]string{"test doc"}))
	require.NoError(t, err)
	for i, text := range []string{"test", "doc"} {
		require.Equal(t, text, tokens[0].Tokens[i].Text)
	}

	// run analyzer with invalid params
	_, err = mc.RunAnalyzer(ctx, client.NewRunAnalyzerOption([]string{"text doc"}).WithAnalyzerParams("invalid params}"))
	common.CheckErr(t, err, false, "JsonError")

	// run analyzer with custom analyzer
	tokens, err = mc.RunAnalyzer(ctx, client.NewRunAnalyzerOption([]string{"test doc"}).
		WithAnalyzerParams(`{"type": "standard", "stop_words": ["test"]}`))

	require.NoError(t, err)
	for i, text := range []string{"doc"} {
		require.Equal(t, text, tokens[0].Tokens[i].Text)
	}
}
