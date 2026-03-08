package testcases

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestQueryIteratorDefault tests query iterator with default parameters
func TestQueryIteratorDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*2).TWithStart(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query iterator with default batch
	itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	common.CheckQueryIteratorResult(ctx, t, itr, common.DefaultNb*3, common.WithExpBatchSize(hp.GenBatchSizes(common.DefaultNb*3, common.DefaultBatchSize)))
}

// TestQueryIteratorHitEmpty tests query iterator on empty collection
func TestQueryIteratorHitEmpty(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> index -> load (no data inserted)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query iterator with default batch
	itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	rs, err := itr.Next(ctx)
	require.Empty(t, rs.Fields)
	require.ErrorIs(t, err, io.EOF)
	common.CheckQueryIteratorResult(ctx, t, itr, 0, common.WithExpBatchSize(hp.GenBatchSizes(0, common.DefaultBatchSize)))
}

// TestQueryIteratorBatchSize tests query iterator with different batch sizes
func TestQueryIteratorBatchSize(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	nb := 201
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(nb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	type batchStruct struct {
		batch        int
		expBatchSize []int
	}
	batchStructs := []batchStruct{
		{batch: nb / 2, expBatchSize: hp.GenBatchSizes(nb, nb/2)},
		{batch: nb, expBatchSize: hp.GenBatchSizes(nb, nb)},
		{batch: nb + 1, expBatchSize: hp.GenBatchSizes(nb, nb+1)},
	}

	for _, _batchStruct := range batchStructs {
		// query iterator with different batch sizes
		itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithBatchSize(_batchStruct.batch))
		common.CheckErr(t, err, true)
		common.CheckQueryIteratorResult(ctx, t, itr, nb, common.WithExpBatchSize(_batchStruct.expBatchSize))
	}
}

// TestQueryIteratorOutputAllFields tests query iterator with all fields output
func TestQueryIteratorOutputAllFields(t *testing.T) {
	t.Parallel()
	for _, dynamic := range [2]bool{false, true} {
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := hp.CreateDefaultMilvusClient(ctx, t)

		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(),
			hp.TNewSchemaOption().TWithEnableDynamicField(dynamic), hp.TWithConsistencyLevel(entity.ClStrong))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		var allFieldsName []string
		for _, field := range schema.Fields {
			allFieldsName = append(allFieldsName, field.Name)
		}
		if dynamic {
			allFieldsName = append(allFieldsName, common.DefaultDynamicFieldName)
		}

		// output * fields
		nbFilter := 1001
		batch := 500
		expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, nbFilter)

		itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithBatchSize(batch).WithOutputFields("*").WithFilter(expr))
		common.CheckErr(t, err, true)
		common.CheckQueryIteratorResult(ctx, t, itr, nbFilter, common.WithExpBatchSize(hp.GenBatchSizes(nbFilter, batch)), common.WithExpOutputFields(allFieldsName))
	}
}

// TestQueryIteratorSparseVecFields tests query iterator with sparse vector fields
func TestQueryIteratorSparseVecFields(t *testing.T) {
	t.Parallel()
	for _, withRows := range [2]bool{true, false} {
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := hp.CreateDefaultMilvusClient(ctx, t)

		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(),
			hp.TNewSchemaOption().TWithEnableDynamicField(true), hp.TWithConsistencyLevel(entity.ClStrong))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithIsRows(withRows), hp.TNewDataOption())
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		fieldsName := []string{common.DefaultDynamicFieldName}
		for _, field := range schema.Fields {
			fieldsName = append(fieldsName, field.Name)
		}

		// output * fields
		itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithBatchSize(400).WithOutputFields("*"))
		common.CheckErr(t, err, true)
		common.CheckQueryIteratorResult(ctx, t, itr, common.DefaultNb, common.WithExpBatchSize(hp.GenBatchSizes(common.DefaultNb, 400)), common.WithExpOutputFields(fieldsName))
	}
}

// TestQueryIteratorInvalid tests query iterator with invalid parameters
func TestQueryIteratorInvalid(t *testing.T) {
	nb := 201
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(nb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query iterator with not exist collection name
	_, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(common.GenRandomString("c", 5)))
	common.CheckErr(t, err, false, "collection not found", "can't find collection")

	// query iterator with not exist partition name
	_, errPar := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithPartitions(common.GenRandomString("p", 5)))
	common.CheckErr(t, errPar, false, "partition name", "not found")

	// query iterator with not exist partition name
	_, errPar = mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithPartitions(common.GenRandomString("p", 5), common.DefaultPartition))
	common.CheckErr(t, errPar, false, "partition name", "not found")

	// query iterator with count(*)
	_, errOutput := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, errOutput, false, "count entities with pagination is not allowed", "count(*)")

	// query iterator with invalid batch size
	for _, batch := range []int{-1, 0} {
		_, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithBatchSize(batch))
		common.CheckErr(t, err, false, "batch size", "must be greater than 0", "cannot less than 1")
	}
}

// TestQueryIteratorInvalidExpr tests query iterator with invalid expressions
func TestQueryIteratorInvalidExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	for _, _invalidExprs := range common.InvalidExpressions {
		t.Log(_invalidExprs)
		_, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithFilter(_invalidExprs.Expr))
		common.CheckErr(t, err, _invalidExprs.ErrNil, _invalidExprs.ErrMsg, "")
	}
}

// TestQueryIteratorOutputFieldDynamic tests query iterator with non-existed field when dynamic enabled or not
func TestQueryIteratorOutputFieldDynamic(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	nb := 201

	for _, dynamic := range [2]bool{true, false} {
		// create -> insert -> flush -> index -> load
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(),
			hp.TNewSchemaOption().TWithEnableDynamicField(dynamic), hp.TWithConsistencyLevel(entity.ClStrong))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(nb))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// query iterator with not existed output fields: if dynamic, non-existent field are equivalent to dynamic field
		itr, errOutput := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithOutputFields("aaa"))
		if dynamic {
			common.CheckErr(t, errOutput, true)
			expFields := []string{common.DefaultInt64FieldName, "aaa"}
			common.CheckQueryIteratorResult(ctx, t, itr, nb, common.WithExpBatchSize(hp.GenBatchSizes(nb, common.DefaultBatchSize)), common.WithExpOutputFields(expFields))
		} else {
			common.CheckErr(t, errOutput, false, "field aaa not exist", "field not exist")
		}
	}
}

// TestQueryIteratorExpr tests query iterator with various expressions
func TestQueryIteratorExpr(t *testing.T) {
	type exprCount struct {
		expr  string
		count int
	}
	capacity := common.TestCapacity
	exprLimits := []exprCount{
		{expr: fmt.Sprintf("%s in [0, 1, 2]", common.DefaultInt64FieldName), count: 3},
		{expr: fmt.Sprintf("%s >= 1000 || %s > 2000", common.DefaultInt64FieldName, common.DefaultInt64FieldName), count: 2000},
		{expr: fmt.Sprintf("%s >= 1000 and %s < 2000", common.DefaultInt64FieldName, common.DefaultInt64FieldName), count: 1000},

		// json and dynamic field filter expr: == < in bool/ list/ int
		// {expr: fmt.Sprintf("%s['number'] == 0", common.DefaultJSONFieldName), count: 1500},
		// {expr: fmt.Sprintf("%s['number'] < 100 and %s['number'] != 0", common.DefaultJSONFieldName, common.DefaultJSONFieldName), count: 99},
		{expr: fmt.Sprintf("%s < 100", common.DefaultDynamicNumberField), count: 100},
		{expr: "dynamicNumber % 2 == 0", count: 1500},
		{expr: fmt.Sprintf("%s == false", common.DefaultDynamicBoolField), count: 1500},
		{expr: fmt.Sprintf("%s in ['1', '2'] ", common.DefaultDynamicStringField), count: 2},
		{expr: fmt.Sprintf("%s['string'] in ['1', '2', '5'] ", common.DefaultJSONFieldName), count: 3},
		{expr: fmt.Sprintf("%s['list'] == [1, 2] ", common.DefaultJSONFieldName), count: 1},
		{expr: fmt.Sprintf("%s['list'][0] < 10 ", common.DefaultJSONFieldName), count: 10},
		{expr: fmt.Sprintf("%s[\"dynamicList\"] != [2, 3]", common.DefaultDynamicFieldName), count: 0},

		// json contains
		{expr: fmt.Sprintf("json_contains (%s['list'], 2)", common.DefaultJSONFieldName), count: 1},
		{expr: fmt.Sprintf("json_contains (%s['number'], 0)", common.DefaultJSONFieldName), count: 0},
		{expr: fmt.Sprintf("JSON_CONTAINS_ANY (%s['list'], [1, 3])", common.DefaultJSONFieldName), count: 2},
		// string like
		{expr: "dynamicString like '1%' ", count: 1111},

		// key exist
		{expr: fmt.Sprintf("exists %s['list']", common.DefaultJSONFieldName), count: common.DefaultNb},
		{expr: "exists a ", count: 0},
		{expr: fmt.Sprintf("exists %s ", common.DefaultDynamicStringField), count: common.DefaultNb},

		// data type not match and no error
		{expr: fmt.Sprintf("%s['number'] == '0' ", common.DefaultJSONFieldName), count: 0},

		// json field
		{expr: fmt.Sprintf("%s >= 1500", common.DefaultJSONFieldName), count: 1500},                                      // json >= 1500
		{expr: fmt.Sprintf("%s > 1499.5", common.DefaultJSONFieldName), count: 1500},                                     // json >= 1500.0
		{expr: fmt.Sprintf("%s like '21%%'", common.DefaultJSONFieldName), count: 100},                                   // json like '21%'
		{expr: fmt.Sprintf("%s == [1503, 1504]", common.DefaultJSONFieldName), count: 1},                                 // json == [1,2]
		{expr: fmt.Sprintf("%s[0] > 1", common.DefaultJSONFieldName), count: 1500},                                       // json[0] > 1
		{expr: fmt.Sprintf("%s[0][0] > 1", common.DefaultJSONFieldName), count: 0},                                       // json == [1,2]
		{expr: fmt.Sprintf("%s[0] == false", common.DefaultBoolArrayField), count: common.DefaultNb / 2},                 //  array[0] ==
		{expr: fmt.Sprintf("%s[0] > 0", common.DefaultInt64ArrayField), count: common.DefaultNb - 1},                     //  array[0] >
		{expr: fmt.Sprintf("array_contains (%s, %d)", common.DefaultInt16ArrayField, capacity), count: capacity},         // array_contains(array, 1)
		{expr: fmt.Sprintf("json_contains (%s, 1)", common.DefaultInt32ArrayField), count: 2},                            // json_contains(array, 1)
		{expr: fmt.Sprintf("array_contains (%s, 1000000)", common.DefaultInt32ArrayField), count: 0},                     // array_contains(array, 1)
		{expr: fmt.Sprintf("json_contains_all (%s, [90, 91])", common.DefaultInt64ArrayField), count: 91},                // json_contains_all(array, [x])
		{expr: fmt.Sprintf("json_contains_any (%s, [0, 100, 10])", common.DefaultFloatArrayField), count: 101},           // json_contains_any (array, [x])
		{expr: fmt.Sprintf("%s == [0, 1]", common.DefaultDoubleArrayField), count: 0},                                    //  array ==
		{expr: fmt.Sprintf("array_length(%s) == %d", common.DefaultDoubleArrayField, capacity), count: common.DefaultNb}, //  array_length
	}

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(),
		hp.TNewSchemaOption().TWithEnableDynamicField(true), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	batch := 500
	for _, exprLimit := range exprLimits {
		rs, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprLimit.expr).WithOutputFields("count(*)"))
		common.CheckErr(t, err, true)
		expectCount, err := rs.GetColumn("count(*)").GetAsInt64(0)
		common.CheckErr(t, err, true)

		log.Info("case expr is", zap.String("expr", exprLimit.expr), zap.Int64("expectedCount", expectCount))
		itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithBatchSize(batch).WithFilter(exprLimit.expr))
		common.CheckErr(t, err, true)
		common.CheckQueryIteratorResult(ctx, t, itr, int(expectCount), common.WithExpBatchSize(hp.GenBatchSizes(int(expectCount), batch)))
	}
}

// TestQueryIteratorPartitions tests query iterator with partition filtering
func TestQueryIteratorPartitions(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))

	// create partition
	pName := "p1"
	err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, pName))
	common.CheckErr(t, err, true)

	// insert [0, nb) into partition: _default
	nb := 1500
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(nb))

	// insert [nb, nb*2) into partition: p1
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(pName), hp.TNewDataOption().TWithNb(nb).TWithStart(nb))

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query iterator with partition
	expr := fmt.Sprintf("%s < %d", common.DefaultInt64FieldName, nb)
	mParLimit := map[string]int{
		common.DefaultPartition: nb,
		pName:                   0,
	}
	for par, limit := range mParLimit {
		itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithFilter(expr).WithPartitions(par))
		common.CheckErr(t, err, true)
		common.CheckQueryIteratorResult(ctx, t, itr, limit, common.WithExpBatchSize(hp.GenBatchSizes(limit, common.DefaultBatchSize)))
	}
}

// TestQueryIteratorWithLimit tests query iterator with limit
func TestQueryIteratorWithLimit(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*2))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query iterator with limit
	limit := int64(2000)
	batch := 500
	itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithIteratorLimit(limit).WithBatchSize(batch))
	common.CheckErr(t, err, true)
	common.CheckQueryIteratorResult(ctx, t, itr, int(limit), common.WithExpBatchSize(hp.GenBatchSizes(int(limit), batch)))
}

// TestQueryIteratorGrowing tests query iterator on growing segments
func TestQueryIteratorGrowing(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> index -> load -> insert (growing)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*2))

	// query iterator growing
	limit := int64(1000)
	itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithIteratorLimit(limit).WithBatchSize(100))
	common.CheckErr(t, err, true)
	common.CheckQueryIteratorResult(ctx, t, itr, int(limit), common.WithExpBatchSize(hp.GenBatchSizes(int(limit), 100)))
}

// TestQueryIteratorConsistencyLevel tests query iterator with different consistency levels
func TestQueryIteratorConsistencyLevel(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query iterator with different consistency levels
	for _, cl := range []entity.ConsistencyLevel{entity.ClStrong, entity.ClBounded, entity.ClEventually} {
		itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(schema.CollectionName).WithConsistencyLevel(cl).WithBatchSize(500))
		common.CheckErr(t, err, true)
		actualLimit := 0
		for {
			rs, err := itr.Next(ctx)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Error("QueryIterator next gets error", zap.Error(err))
				break
			}
			actualLimit = actualLimit + rs.ResultCount
		}
		require.LessOrEqual(t, actualLimit, common.DefaultNb)
	}
}
