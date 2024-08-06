package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// test query from default partition
func TestQueryDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
}

// test query with varchar field filter
func TestQueryVarcharPkDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create and insert
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, insertRes := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	// flush -> index -> load
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query
	expr := fmt.Sprintf("%s in ['0', '1', '2', '3', '4']", common.DefaultVarcharFieldName)
	queryRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{insertRes.IDs.Slice(0, 5)})
}

// query from not existed collection name and partition name
func TestQueryNotExistName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// query with not existed collection
	expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, 100)
	_, errCol := mc.Query(ctx, client.NewQueryOption("aaa").WithFilter(expr))
	common.CheckErr(t, errCol, false, "can't find collection")

	// create -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query with not existed partition
	_, errPar := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions([]string{"aaa"}))
	common.CheckErr(t, errPar, false, "partition name aaa not found")
}

// test query with invalid partition name
func TestQueryInvalidPartitionName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection and partition
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	expr := fmt.Sprintf("%s >= %d", common.DefaultInt64FieldName, 0)
	emptyPartitionName := ""
	// query from "" partitions, expect to query from default partition
	_, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions([]string{emptyPartitionName}))
	common.CheckErr(t, err, false, "Partition name should not be empty")
}

// test query with empty partition name
func TestQueryPartition(t *testing.T) {
	parName := "p1"

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions([]string{}).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{expColumn})

	// query with default partition
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions([]string{common.DefaultPartition}).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{i1Res.IDs})

	// query with specify partition
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions([]string{parName}).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{i2Res.IDs})

	// query with all partitions
	queryRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithPartitions([]string{common.DefaultPartition, parName}).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckQueryResult(t, queryRes.Fields, []column.Column{expColumn})
}

// test query with invalid partition name
func TestQueryWithoutExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	t.Skip("verify TODO")
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, enableDynamic := range [2]bool{true, false} {
		// create -> insert -> flush -> index -> load
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(enableDynamic))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, 10)

		// query with empty output fields []string{}-> output "int64"
		queryNilOutputs, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields([]string{}))
		common.CheckErr(t, err, true)
		common.CheckOutputFields(t, []string{common.DefaultInt64FieldName}, queryNilOutputs.Fields)

		// query with empty output fields []string{""}-> output "int64" and dynamic field
		_, err1 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields([]string{""}))
		if enableDynamic {
			common.CheckErr(t, err1, false, "parse output field name failed")
		} else {
			common.CheckErr(t, err1, false, "not exist")
		}

		// query with not existed field -> output field as dynamic or error
		fakeName := "aaa"
		res2, err2 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields([]string{fakeName}))
		if enableDynamic {
			common.CheckErr(t, err2, true)
			for _, c := range res2.Fields {
				log.Debug("data", zap.String("name", c.Name()), zap.Any("type", c.Type()), zap.Any("data", c.FieldData()))
			}
			common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, fakeName}, res2.Fields)
			dynamicColumn := hp.MergeColumnsToDynamic(10, hp.GenDynamicColumnData(0, 10), common.DefaultDynamicFieldName)
			expColumns := []column.Column{
				hp.GenColumnData(10, entity.FieldTypeInt64, *hp.TNewDataOption()),
				column.NewColumnDynamic(dynamicColumn, fakeName),
			}
			common.CheckQueryResult(t, expColumns, res2.Fields)
		} else {
			common.CheckErr(t, err2, false, fmt.Sprintf("%s not exist", fakeName))
		}

		// query with part not existed field ["aa", "$meat"]: error or as dynamic field
		res3, err3 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields([]string{fakeName, common.DefaultDynamicFieldName}))
		if enableDynamic {
			common.CheckErr(t, err3, true)
			common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, fakeName, common.DefaultDynamicFieldName}, res3.Fields)
		} else {
			common.CheckErr(t, err3, false, "not exist")
		}

		// query with repeated field: ["*", "$meat"], ["floatVec", floatVec"] unique field
		res4, err4 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields([]string{"*", common.DefaultDynamicFieldName}))
		if enableDynamic {
			common.CheckErr(t, err4, true)
			common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultDynamicFieldName}, res4.Fields)
		} else {
			common.CheckErr(t, err4, false, "$meta not exist")
		}

		res5, err5 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields(
			[]string{common.DefaultFloatVecFieldName, common.DefaultFloatVecFieldName, common.DefaultInt64FieldName}))
		common.CheckErr(t, err5, true)
		common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName}, res5.Fields)
	}
}

// test query output all fields and verify data
func TestQueryOutputAllFieldsColumn(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
			WithFilter(fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, pos)).WithOutputFields([]string{"*"}))
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
	mc := createDefaultMilvusClient(ctx, t)

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
		WithFilter(fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, 10)).WithOutputFields([]string{"*"}))
	common.CheckErr(t, errQuery, true)
	common.CheckOutputFields(t, allFieldsName, queryResultAll.Fields)
}

// test query output varchar and binaryVector fields
func TestQueryOutputBinaryAndVarchar(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
		WithFilter(expr).WithOutputFields([]string{"*"}))
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
	mc := createDefaultMilvusClient(ctx, t)

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
	queryResultAll, errQuery := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields([]string{"*"}))
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
	mc := createDefaultMilvusClient(ctx, t)

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
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields([]string{common.QueryCountFieldName}))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(0), count)

	countRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr).WithOutputFields([]string{"Count(*)"}))
	common.CheckErr(t, err, true)
	count, _ = countRes.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(0), count)

	// query: some rows has element greater than expr index array[100]
	expr2 := fmt.Sprintf("%s[%d] > 0", common.DefaultInt64ArrayField, common.TestCapacity)
	countRes2, err2 := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(expr2).WithOutputFields([]string{common.QueryCountFieldName}))
	common.CheckErr(t, err2, true)
	count2, _ := countRes2.Fields[0].GetAsInt64(0)
	require.Equal(t, int64(common.DefaultNb), count2)
}

// test query with expr and verify output dynamic field data
func TestQueryJsonDynamicExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query with different expr and count
	expr := fmt.Sprintf("%s['number'] < 10 || %s < 10", common.DefaultJSONFieldName, common.DefaultDynamicNumberField)

	queryRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithConsistencyLevel(entity.ClStrong).
		WithOutputFields([]string{common.DefaultJSONFieldName, common.DefaultDynamicFieldName}))

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
	mc := createDefaultMilvusClient(ctx, t)

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
	mc := createDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

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
	}

	for _, _exprCount := range exprCounts {
		log.Debug("TestQueryCountJsonDynamicExpr", zap.String("expr", _exprCount.expr))
		countRes, _ := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(_exprCount.expr).WithOutputFields([]string{common.QueryCountFieldName}))
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.Equal(t, _exprCount.count, count)
	}
}

// test query with all kinds of array expr
func TestQueryArrayFieldExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
		countRes, _ := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(_exprCount.expr).WithOutputFields([]string{common.QueryCountFieldName}))
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
	mc := createDefaultMilvusClient(ctx, t)

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
		_, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithFilter(queryExpr).WithOutputFields([]string{invalidCount.countField}))
		common.CheckErr(t, err, false, invalidCount.errMsg)
	}
}
