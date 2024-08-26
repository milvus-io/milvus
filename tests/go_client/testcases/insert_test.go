package testcases

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestInsertDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	for _, autoID := range [2]bool{false, true} {
		// create collection
		cp := hp.NewCreateCollectionParams(hp.Int64Vec)
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithAutoID(autoID), hp.TNewSchemaOption())

		// insert
		columnOpt := hp.TNewDataOption().TWithDim(common.DefaultDim)
		pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *columnOpt)
		vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *columnOpt)
		insertOpt := client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(vecColumn)
		if !autoID {
			insertOpt.WithColumns(pkColumn)
		}
		insertRes, err := mc.Insert(ctx, insertOpt)
		common.CheckErr(t, err, true)
		if !autoID {
			common.CheckInsertResult(t, pkColumn, insertRes)
		}
	}
}

func TestInsertDefaultPartition(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	for _, autoID := range [2]bool{false, true} {
		// create collection
		cp := hp.NewCreateCollectionParams(hp.Int64Vec)
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithAutoID(autoID), hp.TNewSchemaOption())

		// create partition
		parName := common.GenRandomString("par", 4)
		err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
		common.CheckErr(t, err, true)

		// insert
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
	}
}

func TestInsertVarcharPkDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	for _, autoID := range [2]bool{false, true} {
		// create collection
		cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithAutoID(autoID).TWithMaxLen(20), hp.TNewSchemaOption())

		// insert
		columnOpt := hp.TNewDataOption().TWithDim(common.DefaultDim)
		pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeVarChar, *columnOpt)
		vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeBinaryVector, *columnOpt)
		insertOpt := client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(vecColumn)
		if !autoID {
			insertOpt.WithColumns(pkColumn)
		}
		insertRes, err := mc.Insert(ctx, insertOpt)
		common.CheckErr(t, err, true)
		if !autoID {
			common.CheckInsertResult(t, pkColumn, insertRes)
		}
	}
}

// test insert data into collection that has all scala fields
func TestInsertAllFieldsData(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	for _, dynamic := range [2]bool{false, true} {
		// create collection
		cp := hp.NewCreateCollectionParams(hp.AllFields)
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(dynamic))

		// insert
		insertOpt := client.NewColumnBasedInsertOption(schema.CollectionName)
		columnOpt := hp.TNewDataOption().TWithDim(common.DefaultDim)
		for _, field := range schema.Fields {
			if field.DataType == entity.FieldTypeArray {
				columnOpt.TWithElementType(field.ElementType)
			}
			_column := hp.GenColumnData(common.DefaultNb, field.DataType, *columnOpt)
			insertOpt.WithColumns(_column)
		}
		if dynamic {
			insertOpt.WithColumns(hp.GenDynamicColumnData(0, common.DefaultNb)...)
		}
		insertRes, errInsert := mc.Insert(ctx, insertOpt)
		common.CheckErr(t, errInsert, true)
		pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *columnOpt)
		common.CheckInsertResult(t, pkColumn, insertRes)

		// flush and check row count
		flushTak, _ := mc.Flush(ctx, client.NewFlushOption(schema.CollectionName))
		err := flushTak.Await(ctx)
		common.CheckErr(t, err, true)
	}
}

// test insert dynamic data with column
func TestInsertDynamicExtraColumn(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// insert without dynamic field
	insertOpt := client.NewColumnBasedInsertOption(schema.CollectionName)
	columnOpt := hp.TNewDataOption().TWithDim(common.DefaultDim)

	for _, field := range schema.Fields {
		_column := hp.GenColumnData(common.DefaultNb, field.DataType, *columnOpt)
		insertOpt.WithColumns(_column)
	}
	insertRes, errInsert := mc.Insert(ctx, insertOpt)
	common.CheckErr(t, errInsert, true)
	require.Equal(t, common.DefaultNb, int(insertRes.InsertCount))

	// insert with dynamic field
	insertOptDynamic := client.NewColumnBasedInsertOption(schema.CollectionName)
	columnOpt.TWithStart(common.DefaultNb)
	for _, fieldType := range hp.GetAllScalarFieldType() {
		if fieldType == entity.FieldTypeArray {
			columnOpt.TWithElementType(entity.FieldTypeInt64).TWithMaxCapacity(2)
		}
		_column := hp.GenColumnData(common.DefaultNb, fieldType, *columnOpt)
		insertOptDynamic.WithColumns(_column)
	}
	insertOptDynamic.WithColumns(hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *columnOpt))
	insertRes2, errInsert2 := mc.Insert(ctx, insertOptDynamic)
	common.CheckErr(t, errInsert2, true)
	require.Equal(t, common.DefaultNb, int(insertRes2.InsertCount))

	// index
	it, _ := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, index.NewSCANNIndex(entity.COSINE, 32, false)))
	err := it.Await(ctx)
	common.CheckErr(t, err, true)

	// load
	lt, _ := mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName))
	err = lt.Await(ctx)
	common.CheckErr(t, err, true)

	// query
	res, _ := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter("int64 == 3000").WithOutputFields([]string{"*"}))
	common.CheckOutputFields(t, []string{common.DefaultFloatVecFieldName, common.DefaultInt64FieldName, common.DefaultDynamicFieldName}, res.Fields)
	for _, c := range res.Fields {
		log.Debug("data", zap.Any("data", c.FieldData()))
	}
}

// test insert array column with empty data
func TestInsertEmptyArray(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VecArray)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	columnOpt := hp.TNewDataOption().TWithDim(common.DefaultDim).TWithMaxCapacity(0)
	insertOpt := client.NewColumnBasedInsertOption(schema.CollectionName)
	for _, field := range schema.Fields {
		if field.DataType == entity.FieldTypeArray {
			columnOpt.TWithElementType(field.ElementType)
		}
		_column := hp.GenColumnData(common.DefaultNb, field.DataType, *columnOpt)
		insertOpt.WithColumns(_column)
	}

	_, err := mc.Insert(ctx, insertOpt)
	common.CheckErr(t, err, true)
}

func TestInsertArrayDataTypeNotMatch(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// share field and data
	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	int64Column := hp.GenColumnData(100, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(100, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithDim(128))
	for _, eleType := range hp.GetAllArrayElementType() {
		collName := common.GenRandomString(prefix, 6)
		arrayField := entity.NewField().WithName("array").WithDataType(entity.FieldTypeArray).WithElementType(eleType).WithMaxCapacity(100).WithMaxLength(100)

		// create collection
		schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField).WithField(arrayField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// prepare data
		columnType := entity.FieldTypeInt64
		if eleType == entity.FieldTypeInt64 {
			columnType = entity.FieldTypeBool
		}
		arrayColumn := hp.GenColumnData(100, entity.FieldTypeArray, *hp.TNewDataOption().TWithElementType(columnType).TWithFieldName("array"))
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, int64Column, vecColumn, arrayColumn))
		common.CheckErr(t, err, false, "insert data does not match")
	}
}

func TestInsertArrayDataCapacityExceed(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// share field and data
	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	int64Column := hp.GenColumnData(100, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(100, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithDim(128))
	for _, eleType := range hp.GetAllArrayElementType() {
		collName := common.GenRandomString(prefix, 6)
		arrayField := entity.NewField().WithName("array").WithDataType(entity.FieldTypeArray).WithElementType(eleType).WithMaxCapacity(common.TestCapacity).WithMaxLength(100)

		// create collection
		schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField).WithField(arrayField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
		common.CheckErr(t, err, true)

		// insert array data capacity > field.MaxCapacity
		arrayColumn := hp.GenColumnData(100, entity.FieldTypeArray, *hp.TNewDataOption().TWithElementType(eleType).TWithFieldName("array").TWithMaxCapacity(common.TestCapacity * 2))
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, int64Column, vecColumn, arrayColumn))
		common.CheckErr(t, err, false, "array length exceeds max capacity")
	}
}

// test insert not exist collection or not exist partition
func TestInsertNotExist(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// insert data into not exist collection
	intColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption("notExist", intColumn))
	common.CheckErr(t, err, false, "can't find collection")

	// insert data into not exist partition
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithDim(common.DefaultDim))
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, intColumn, vecColumn).WithPartition("aaa"))
	common.CheckErr(t, err, false, "partition not found")
}

// test insert data columns len, order mismatch fields
func TestInsertColumnsMismatchFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// column data
	columnOpt := hp.TNewDataOption().TWithDim(common.DefaultDim)
	intColumn := hp.GenColumnData(100, entity.FieldTypeInt64, *columnOpt)
	floatColumn := hp.GenColumnData(100, entity.FieldTypeFloat, *columnOpt)
	vecColumn := hp.GenColumnData(100, entity.FieldTypeFloatVector, *columnOpt)

	// insert
	collName := schema.CollectionName

	// len(column) < len(fields)
	_, errInsert := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, intColumn))
	common.CheckErr(t, errInsert, false, "not passed")

	// len(column) > len(fields)
	_, errInsert2 := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, intColumn, vecColumn, vecColumn))
	common.CheckErr(t, errInsert2, false, "duplicated column")

	//
	_, errInsert3 := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, intColumn, floatColumn, vecColumn))
	common.CheckErr(t, errInsert3, false, "does not exist in collection")

	// order(column) != order(fields)
	_, errInsert4 := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, vecColumn, intColumn))
	common.CheckErr(t, errInsert4, true)
}

// test insert with columns which has different len
func TestInsertColumnsDifferentLen(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// column data
	columnOpt := hp.TNewDataOption().TWithDim(common.DefaultDim)
	intColumn := hp.GenColumnData(100, entity.FieldTypeInt64, *columnOpt)
	vecColumn := hp.GenColumnData(200, entity.FieldTypeFloatVector, *columnOpt)

	// len(column) < len(fields)
	_, errInsert := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, intColumn, vecColumn))
	common.CheckErr(t, errInsert, false, "column size not match")
}

// test insert invalid column: empty column or dim not match
func TestInsertInvalidColumn(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	// create collection
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert with empty column data
	pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, []int64{})
	vecColumn := hp.GenColumnData(100, entity.FieldTypeFloatVector, *hp.TNewDataOption())

	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn))
	common.CheckErr(t, err, false, "need long int array][actual=got nil]")

	// insert with empty vector data
	vecColumn2 := column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, [][]float32{})
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn2))
	common.CheckErr(t, err, false, "num_rows should be greater than 0")

	// insert with vector data dim not match
	vecColumnDim := column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim-8, [][]float32{})
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumnDim))
	common.CheckErr(t, err, false, "vector dim 120 not match collection definition")
}

// test insert invalid column: empty column or dim not match
func TestInsertColumnVarcharExceedLen(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	// create collection
	varcharMaxLen := 10
	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithMaxLen(int64(varcharMaxLen)), hp.TNewSchemaOption())

	// insert with empty column data
	varcharValues := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		_value := common.GenRandomString("", varcharMaxLen+1)
		varcharValues = append(varcharValues, _value)
	}
	pkColumn := column.NewColumnVarChar(common.DefaultVarcharFieldName, varcharValues)
	vecColumn := hp.GenColumnData(100, entity.FieldTypeBinaryVector, *hp.TNewDataOption())

	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn))
	common.CheckErr(t, err, false, "length of varchar field varchar exceeds max length")
}

// test insert sparse vector
func TestInsertSparseData(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert sparse data
	columnOpt := hp.TNewDataOption()
	pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *columnOpt)
	columns := []column.Column{
		pkColumn,
		hp.GenColumnData(common.DefaultNb, entity.FieldTypeVarChar, *columnOpt),
		hp.GenColumnData(common.DefaultNb, entity.FieldTypeSparseVector, *columnOpt.TWithSparseMaxLen(common.DefaultDim)),
	}
	inRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, columns...))
	common.CheckErr(t, err, true)
	common.CheckInsertResult(t, pkColumn, inRes)
}

func TestInsertSparseDataMaxDim(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert sparse data
	columnOpt := hp.TNewDataOption()
	pkColumn := hp.GenColumnData(1, entity.FieldTypeInt64, *columnOpt)
	varcharColumn := hp.GenColumnData(1, entity.FieldTypeVarChar, *columnOpt)

	// sparse vector with max dim
	positions := []uint32{0, math.MaxUint32 - 10, math.MaxUint32 - 1}
	values := []float32{0.453, 5.0776, 100.098}
	sparseVec, err := entity.NewSliceSparseEmbedding(positions, values)
	common.CheckErr(t, err, true)

	sparseColumn := column.NewColumnSparseVectors(common.DefaultSparseVecFieldName, []entity.SparseEmbedding{sparseVec})
	inRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, varcharColumn, sparseColumn))
	common.CheckErr(t, err, true)
	common.CheckInsertResult(t, pkColumn, inRes)
}

// empty spare vector can't be searched, but can be queried
func TestInsertReadSparseEmptyVector(t *testing.T) {
	// invalid sparse vector: positions >= uint32
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// insert data column
	columnOpt := hp.TNewDataOption()
	data := []column.Column{
		hp.GenColumnData(1, entity.FieldTypeInt64, *columnOpt),
		hp.GenColumnData(1, entity.FieldTypeVarChar, *columnOpt),
	}

	//  sparse vector: empty position and values
	sparseVec, err := entity.NewSliceSparseEmbedding([]uint32{}, []float32{})
	common.CheckErr(t, err, true)
	data2 := append(data, column.NewColumnSparseVectors(common.DefaultSparseVecFieldName, []entity.SparseEmbedding{sparseVec}))
	insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, data2...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 1, insertRes.InsertCount)

	// query and check vector is empty
	resQuery, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithLimit(10).WithOutputFields([]string{common.DefaultSparseVecFieldName}).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, resQuery.ResultCount)
	log.Info("sparseVec", zap.Any("data", resQuery.GetColumn(common.DefaultSparseVecFieldName).(*column.ColumnSparseFloatVector).Data()))
	common.EqualColumn(t, resQuery.GetColumn(common.DefaultSparseVecFieldName), column.NewColumnSparseVectors(common.DefaultSparseVecFieldName, []entity.SparseEmbedding{sparseVec}))
}

func TestInsertSparseInvalidVector(t *testing.T) {
	// invalid sparse vector: len(positions) != len(values)
	positions := []uint32{1, 10}
	values := []float32{0.4, 5.0, 0.34}
	_, err := entity.NewSliceSparseEmbedding(positions, values)
	common.CheckErr(t, err, false, "invalid sparse embedding input, positions shall have same number of values")

	// invalid sparse vector: positions >= uint32
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert data column
	columnOpt := hp.TNewDataOption()
	data := []column.Column{
		hp.GenColumnData(1, entity.FieldTypeInt64, *columnOpt),
		hp.GenColumnData(1, entity.FieldTypeVarChar, *columnOpt),
	}
	// invalid sparse vector: position > (maximum of uint32 - 1)
	positions = []uint32{math.MaxUint32}
	values = []float32{0.4}
	sparseVec, err := entity.NewSliceSparseEmbedding(positions, values)
	common.CheckErr(t, err, true)
	data1 := append(data, column.NewColumnSparseVectors(common.DefaultSparseVecFieldName, []entity.SparseEmbedding{sparseVec}))
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, data1...))
	common.CheckErr(t, err, false, "invalid index in sparse float vector: must be less than 2^32-1")
}

func TestInsertSparseVectorSamePosition(t *testing.T) {
	// invalid sparse vector: positions >= uint32
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert data column
	columnOpt := hp.TNewDataOption()
	data := []column.Column{
		hp.GenColumnData(1, entity.FieldTypeInt64, *columnOpt),
		hp.GenColumnData(1, entity.FieldTypeVarChar, *columnOpt),
	}
	// invalid sparse vector: position > (maximum of uint32 - 1)
	sparseVec, err := entity.NewSliceSparseEmbedding([]uint32{2, 10, 2}, []float32{0.4, 0.5, 0.6})
	common.CheckErr(t, err, true)
	data = append(data, column.NewColumnSparseVectors(common.DefaultSparseVecFieldName, []entity.SparseEmbedding{sparseVec}))
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, data...))
	common.CheckErr(t, err, false, "unsorted or same indices in sparse float vector")
}

/******************
 Test insert rows
******************/

// test insert rows enable or disable dynamic field
func TestInsertDefaultRows(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, autoId := range []bool{false, true} {
		cp := hp.NewCreateCollectionParams(hp.Int64Vec)
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithAutoID(autoId), hp.TNewSchemaOption())
		log.Info("fields", zap.Any("FieldNames", schema.Fields))

		// insert rows
		rows := hp.GenInt64VecRows(common.DefaultNb, false, autoId, *hp.TNewDataOption())
		log.Info("rows data", zap.Any("rows[8]", rows[8]))
		ids, err := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
		common.CheckErr(t, err, true)
		if !autoId {
			int64Values := make([]int64, 0, common.DefaultNb)
			for i := 0; i < common.DefaultNb; i++ {
				int64Values = append(int64Values, int64(i+1))
			}
			common.CheckInsertResult(t, column.NewColumnInt64(common.DefaultInt64FieldName, int64Values), ids)
		}
		require.Equal(t, ids.InsertCount, int64(common.DefaultNb))

		// flush and check row count
		flushTask, errFlush := mc.Flush(ctx, client.NewFlushOption(schema.CollectionName))
		common.CheckErr(t, errFlush, true)
		errFlush = flushTask.Await(ctx)
		common.CheckErr(t, errFlush, true)
	}
}

// test insert rows enable or disable dynamic field
func TestInsertAllFieldsRows(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus/issues/33459")
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, enableDynamicField := range [2]bool{true, false} {
		cp := hp.NewCreateCollectionParams(hp.AllFields)
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(enableDynamicField))
		log.Info("fields", zap.Any("FieldNames", schema.Fields))

		// insert rows
		rows := hp.GenAllFieldsRows(common.DefaultNb, false, *hp.TNewDataOption())
		log.Debug("", zap.Any("row[0]", rows[0]))
		log.Debug("", zap.Any("row", rows[1]))
		ids, err := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
		common.CheckErr(t, err, true)

		int64Values := make([]int64, 0, common.DefaultNb)
		for i := 0; i < common.DefaultNb; i++ {
			int64Values = append(int64Values, int64(i))
		}
		common.CheckInsertResult(t, column.NewColumnInt64(common.DefaultInt64FieldName, int64Values), ids)

		// flush and check row count
		flushTask, errFlush := mc.Flush(ctx, client.NewFlushOption(schema.CollectionName))
		common.CheckErr(t, errFlush, true)
		errFlush = flushTask.Await(ctx)
		common.CheckErr(t, errFlush, true)
	}
}

// test insert rows enable or disable dynamic field
func TestInsertVarcharRows(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus/issues/33457")
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, autoId := range []bool{true} {
		cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithAutoID(autoId))
		log.Info("fields", zap.Any("FieldNames", schema.Fields))

		// insert rows
		rows := hp.GenInt64VarcharSparseRows(common.DefaultNb, false, autoId, *hp.TNewDataOption().TWithSparseMaxLen(1000))
		ids, err := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
		common.CheckErr(t, err, true)

		int64Values := make([]int64, 0, common.DefaultNb)
		for i := 0; i < common.DefaultNb; i++ {
			int64Values = append(int64Values, int64(i))
		}
		common.CheckInsertResult(t, column.NewColumnInt64(common.DefaultInt64FieldName, int64Values), ids)

		// flush and check row count
		flushTask, errFlush := mc.Flush(ctx, client.NewFlushOption(schema.CollectionName))
		common.CheckErr(t, errFlush, true)
		errFlush = flushTask.Await(ctx)
		common.CheckErr(t, errFlush, true)
	}
}

func TestInsertSparseRows(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	int64Field := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	sparseField := entity.NewField().WithName(common.DefaultSparseVecFieldName).WithDataType(entity.FieldTypeSparseVector)
	collName := common.GenRandomString("insert", 6)
	schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(sparseField)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// prepare rows
	rows := make([]interface{}, 0, common.DefaultNb)

	// BaseRow generate insert rows
	for i := 0; i < common.DefaultNb; i++ {
		vec := common.GenSparseVector(500)
		// log.Info("", zap.Any("SparseVec", vec))
		baseRow := hp.BaseRow{
			Int64:     int64(i + 1),
			SparseVec: vec,
		}
		rows = append(rows, &baseRow)
	}
	ids, err := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
	common.CheckErr(t, err, true)

	int64Values := make([]int64, 0, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		int64Values = append(int64Values, int64(i+1))
	}
	common.CheckInsertResult(t, column.NewColumnInt64(common.DefaultInt64FieldName, int64Values), ids)

	// flush and check row count
	flushTask, errFlush := mc.Flush(ctx, client.NewFlushOption(schema.CollectionName))
	common.CheckErr(t, errFlush, true)
	errFlush = flushTask.Await(ctx)
	common.CheckErr(t, errFlush, true)
}

// test field name: pk, row json name: int64
func TestInsertRowFieldNameNotMatch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection with pk name: pk
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	int64Field := entity.NewField().WithName("pk").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	collName := common.GenRandomString(prefix, 6)
	schema := entity.NewSchema().WithName(collName).WithField(int64Field).WithField(vecField)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// insert rows, with json key name: int64
	rows := hp.GenInt64VecRows(10, false, false, *hp.TNewDataOption())
	_, errInsert := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
	common.CheckErr(t, errInsert, false, "row 0 does not has field pk")
}

// test field name: pk, row json name: int64
func TestInsertRowMismatchFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithDim(8), hp.TNewSchemaOption())

	// rows fields < schema fields
	rowsLess := make([]interface{}, 0, 10)
	for i := 1; i < 11; i++ {
		row := hp.BaseRow{
			Int64: int64(i),
		}
		rowsLess = append(rowsLess, row)
	}
	_, errInsert := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rowsLess...))
	common.CheckErr(t, errInsert, false, "[expected=need float vector][actual=got nil]")

	/*
		// extra fields
		t.Log("https://github.com/milvus-io/milvus/issues/33487")
		rowsMore := make([]interface{}, 0, 10)
		for i := 1; i< 11; i++ {
			row := hp.BaseRow{
				Int64: int64(i),
				Int32: int32(i),
				FloatVec: common.GenFloatVector(8),
			}
			rowsMore = append(rowsMore, row)
		}
		log.Debug("Row data", zap.Any("row[0]", rowsMore[0]))
		_, errInsert = mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rowsMore...))
		common.CheckErr(t, errInsert, false, "")
	*/

	// rows order != schema order
	rowsOrder := make([]interface{}, 0, 10)
	for i := 1; i < 11; i++ {
		row := hp.BaseRow{
			FloatVec: common.GenFloatVector(8),
			Int64:    int64(i),
		}
		rowsOrder = append(rowsOrder, row)
	}
	log.Debug("Row data", zap.Any("row[0]", rowsOrder[0]))
	_, errInsert = mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rowsOrder...))
	common.CheckErr(t, errInsert, true)
}

func TestInsertAutoIDInvalidRow(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus/issues/33460")
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, autoId := range []bool{false, true} {
		cp := hp.NewCreateCollectionParams(hp.Int64Vec)
		_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithAutoID(autoId), hp.TNewSchemaOption())

		// insert rows: autoId true -> o pk data; autoID false -> has pk data
		rows := hp.GenInt64VecRows(10, false, !autoId, *hp.TNewDataOption())
		log.Info("rows data", zap.Any("rows[8]", rows[0]))
		_, err := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
		common.CheckErr(t, err, false, "missing pk data")
	}
}

func TestFlushRate(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	// create collection
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption().TWithAutoID(true), hp.TNewSchemaOption())

	// insert
	columnOpt := hp.TNewDataOption().TWithDim(common.DefaultDim)
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *columnOpt)
	insertOpt := client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(vecColumn)
	_, err := mc.Insert(ctx, insertOpt)
	common.CheckErr(t, err, true)

	_, err = mc.Flush(ctx, client.NewFlushOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	_, err = mc.Flush(ctx, client.NewFlushOption(schema.CollectionName))
	common.CheckErr(t, err, false, "request is rejected by grpc RateLimiter middleware, please retry later: rate limit exceeded[rate=0.1]")
}
