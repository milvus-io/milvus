package testcases

import (
	"fmt"
	"io"
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

func TestSearchIteratorDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*2))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))

	// search iterator default
	itr, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector))
	common.CheckErr(t, err, true)
	actualLimit := 0
	for {
		rs, err := itr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Error("SearchIterator next gets error", zap.Error(err))
				break
			}
		}
		actualLimit = actualLimit + rs.ResultCount
	}
	require.LessOrEqual(t, actualLimit, common.DefaultNb*2)

	// search iterator with limit
	limit := 2000
	itr, err = mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithIteratorLimit(int64(limit)))
	common.CheckErr(t, err, true)
	common.CheckSearchIteratorResult(ctx, t, itr, limit, common.WithExpBatchSize(hp.GenBatchSizes(limit, common.DefaultBatchSize)))
}

func TestSearchIteratorGrowing(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*2))

	// search iterator growing
	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	// wait limit support
	limit := 1000
	itr, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithIteratorLimit(int64(limit)).WithBatchSize(100))
	common.CheckErr(t, err, true)
	common.CheckSearchIteratorResult(ctx, t, itr, limit, common.WithExpBatchSize(hp.GenBatchSizes(limit, 100)))
}

func TestSearchIteratorHitEmpty(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search
	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	itr, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector))
	common.CheckErr(t, err, true)
	common.CheckSearchIteratorResult(ctx, t, itr, 0, common.WithExpBatchSize(hp.GenBatchSizes(0, common.DefaultBatchSize)))
}

func TestSearchIteratorBatchSize(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search iterator with special limit: 0, -1, -2
	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	itr, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithIteratorLimit(0))
	common.CheckErr(t, err, true)
	common.CheckSearchIteratorResult(ctx, t, itr, 0, common.WithExpBatchSize(hp.GenBatchSizes(0, common.DefaultBatchSize)))

	for _, _limit := range []int64{-1, -2} {
		itr, err = mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithIteratorLimit(_limit))
		common.CheckErr(t, err, true)
		actualLimit := 0
		for {
			rs, err := itr.Next(ctx)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Error("SearchIterator next gets error", zap.Error(err))
				break
			}
			actualLimit = actualLimit + rs.ResultCount
			require.LessOrEqual(t, rs.ResultCount, common.DefaultBatchSize)
		}
		require.LessOrEqual(t, actualLimit, common.DefaultNb)
	}

	// search iterator
	type batchStruct struct {
		batch        int
		expBatchSize []int
	}
	limit := 201
	batchStructs := []batchStruct{
		{batch: limit / 2, expBatchSize: hp.GenBatchSizes(limit, limit/2)},
		{batch: limit, expBatchSize: hp.GenBatchSizes(limit, limit)},
		{batch: limit + 1, expBatchSize: hp.GenBatchSizes(limit, limit+1)},
	}

	for _, _batchStruct := range batchStructs {
		vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
		itr, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithIteratorLimit(int64(limit)).WithBatchSize(_batchStruct.batch))
		common.CheckErr(t, err, true)
		common.CheckSearchIteratorResult(ctx, t, itr, limit, common.WithExpBatchSize(_batchStruct.expBatchSize))
	}
}

func TestSearchIteratorOutputAllFields(t *testing.T) {
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
		vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
		itr, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithANNSField(common.DefaultFloatVecFieldName).
			WithOutputFields("*").WithIteratorLimit(100).WithBatchSize(12))
		common.CheckErr(t, err, true)
		common.CheckSearchIteratorResult(ctx, t, itr, 100, common.WithExpBatchSize(hp.GenBatchSizes(100, 12)), common.WithExpOutputFields(allFieldsName))
	}
}

func TestQueryIteratorOutputSparseFieldsRows(t *testing.T) {
	t.Parallel()
	// connect
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
		vector := common.GenSparseVector(common.DefaultDim)
		itr, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithOutputFields("*").WithIteratorLimit(200).WithBatchSize(120))
		common.CheckErr(t, err, true)
		common.CheckSearchIteratorResult(ctx, t, itr, 200, common.WithExpBatchSize(hp.GenBatchSizes(200, 120)), common.WithExpOutputFields(fieldsName))
	}
}

func TestSearchIteratorInvalid(t *testing.T) {
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

	// search iterator with not exist collection name
	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	_, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(common.GenRandomString("c", 5), vector))
	common.CheckErr(t, err, false, "collection not found")

	// search iterator with not exist partition name
	_, err = mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithPartitions(common.GenRandomString("p", 5)))
	common.CheckErr(t, err, false, "not found")
	_, err = mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithPartitions(common.DefaultPartition, common.GenRandomString("p", 5)))
	common.CheckErr(t, err, false, "not found")

	// search iterator with not exist vector field name
	_, err = mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithANNSField(common.GenRandomString("f", 5)))
	common.CheckErr(t, err, false, "failed to get field schema by name")

	// search iterator with count(*)
	_, err = mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, false, "field count(*) not exist")

	// search iterator with invalid batch size
	for _, batch := range []int{-1, 0, -2} {
		_, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithBatchSize(batch))
		common.CheckErr(t, err, false, "batch size must be greater than 0")
	}

	itr, err2 := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithBatchSize(common.MaxTopK+1))
	common.CheckErr(t, err2, true)
	_, err2 = itr.Next(ctx)
	common.CheckErr(t, err2, false, "batch size is invalid, it should be in range [1, 16384]")

	// search iterator with invalid offset
	for _, offset := range []int{-2, -1, common.MaxTopK + 1} {
		_, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithOffset(offset))
		common.CheckErr(t, err, false, "it should be in range [1, 16384]")
	}
}

func TestSearchIteratorWithInvalidExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	for _, _invalidExprs := range common.InvalidExpressions {
		t.Log(_invalidExprs)
		_, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithFilter(_invalidExprs.Expr))
		common.CheckErr(t, err, _invalidExprs.ErrNil, _invalidExprs.ErrMsg, "")
	}
}

func TestSearchIteratorTemplateKey(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*2))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))

	// search iterator default
	value := 2000
	itr, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithIteratorLimit(100).WithBatchSize(10).
		WithFilter(fmt.Sprintf("%s < {key}", common.DefaultInt64FieldName)).WithTemplateParam("key", value))
	common.CheckErr(t, err, true)
	actualLimit := 0
	for {
		rs, err := itr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Error("SearchIterator next gets error", zap.Error(err))
			break
		}
		actualLimit = actualLimit + rs.ResultCount
		require.Equal(t, 10, rs.ResultCount)

		// check result ids < value
		for _, id := range rs.IDs.(*column.ColumnInt64).Data() {
			require.Less(t, id, int64(value))
		}
	}
	require.LessOrEqual(t, actualLimit, 100)
}

func TestSearchIteratorGroupBy(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	_, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithGroupByField(common.DefaultInt64FieldName).
		WithIteratorLimit(500).WithBatchSize(100))
	common.CheckErr(t, err, false, "Not allowed to do groupBy when doing iteration")
}

func TestSearchIteratorIgnoreGrowing(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// growing pk [DefaultNb, DefaultNb*2]
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb).TWithStart(common.DefaultNb))

	// search iterator growing
	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	itr, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithIgnoreGrowing(true).WithIteratorLimit(100).WithBatchSize(10))
	common.CheckErr(t, err, true)
	actualLimit := 0
	for {
		rs, err := itr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Error("SearchIterator next gets error", zap.Error(err))
			break
		}
		actualLimit = actualLimit + rs.ResultCount
		for _, id := range rs.IDs.(*column.ColumnInt64).Data() {
			require.Less(t, id, int64(common.DefaultNb))
		}
	}
	require.LessOrEqual(t, actualLimit, 100)
}

func TestSearchIteratorNull(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	int32NullField := entity.NewField().WithName(common.DefaultInt32FieldName).WithDataType(entity.FieldTypeInt32).WithNullable(true)
	schema := entity.NewSchema().WithName(common.GenRandomString("null_int32", 10)).WithField(pkField).WithField(vecField).WithField(int32NullField)
	errCreate := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errCreate, true)

	prepare := hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Generate test data with boundary values
	nb := common.DefaultNb * 3
	pkColumn := hp.GenColumnData(nb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(nb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	int32Values := make([]int32, 0, nb)
	validData := make([]bool, 0, nb)

	// Generate JSON documents
	for i := 0; i < nb; i++ {
		_mod := i % 2
		if _mod == 0 {
			validData = append(validData, false)
		} else {
			int32Values = append(int32Values, int32(i))
			validData = append(validData, true)
		}
	}
	nullColumn, err := column.NewNullableColumnInt32(common.DefaultInt32FieldName, int32Values, validData)
	common.CheckErr(t, err, true)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn, nullColumn))
	common.CheckErr(t, err, true)

	// search iterator with null expr
	expr := fmt.Sprintf("%s is null", common.DefaultInt32FieldName)
	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	itr, err2 := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithFilter(expr).WithIteratorLimit(100).WithBatchSize(10).WithOutputFields(common.DefaultInt32FieldName))
	common.CheckErr(t, err2, true)
	actualLimit := 0
	for {
		rs, err := itr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Error("SearchIterator next gets error", zap.Error(err))
			break
		}
		actualLimit = actualLimit + rs.ResultCount
		require.Equal(t, 10, rs.ResultCount)
		for _, field := range rs.Fields {
			if field.Name() == common.DefaultInt32FieldName {
				for i := 0; i < field.Len(); i++ {
					isNull, err := field.IsNull(i)
					common.CheckErr(t, err, true)
					require.True(t, isNull)
				}
			}
		}
	}
	require.LessOrEqual(t, actualLimit, 100)
}

func TestSearchIteratorDefaultValue(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	int32NullField := entity.NewField().WithName(common.DefaultInt32FieldName).WithDataType(entity.FieldTypeInt32).WithDefaultValueInt(100)
	schema := entity.NewSchema().WithName(common.GenRandomString("null_int32", 10)).WithField(pkField).WithField(vecField).WithField(int32NullField)
	errCreate := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, errCreate, true)

	prepare := hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Generate test data with boundary values
	nb := common.DefaultNb * 3
	pkColumn := hp.GenColumnData(nb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(nb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	int32Values := make([]int32, 0, nb)
	validData := make([]bool, 0, nb)

	// Generate JSON documents
	for i := 0; i < nb; i++ {
		_mod := i % 2
		if _mod == 0 {
			validData = append(validData, false)
		} else {
			int32Values = append(int32Values, int32(i))
			validData = append(validData, true)
		}
	}
	nullColumn, err := column.NewNullableColumnInt32(common.DefaultInt32FieldName, int32Values, validData)
	common.CheckErr(t, err, true)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn, nullColumn))
	common.CheckErr(t, err, true)

	// search iterator with null expr
	expr := fmt.Sprintf("%s == 100", common.DefaultInt32FieldName)
	vector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	itr, err2 := mc.SearchIterator(ctx, client.NewSearchIteratorOption(schema.CollectionName, vector).WithFilter(expr).WithIteratorLimit(100).WithBatchSize(10).WithOutputFields(common.DefaultInt32FieldName))
	common.CheckErr(t, err2, true)
	actualLimit := 0
	for {
		rs, err := itr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Error("SearchIterator next gets error", zap.Error(err))
			break
		}
		actualLimit = actualLimit + rs.ResultCount
		require.Equal(t, 10, rs.ResultCount)
		for _, field := range rs.Fields {
			if field.Name() == common.DefaultInt32FieldName {
				for i := 0; i < field.Len(); i++ {
					fieldData, _ := field.Get(i)
					require.EqualValues(t, 100, fieldData)
				}
			}
		}
	}
	require.LessOrEqual(t, actualLimit, 100)
}
