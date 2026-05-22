package testcases

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	internalTakeDim          = 8
	internalTakeBinaryDim    = 16
	internalTakeInt8VecField = "int8Vec"
	internalTakeTsField      = "ts"
)

func TestSearchDefault(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	resSearch, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
}

func TestSearchDefaultGrowing(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> index -> load -> insert
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())

	// search
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeBinaryVector)
	resSearch, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
}

// test search collection and partition name not exist
func TestSearchInvalidCollectionPartitionName(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// search with not exist collection
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	_, err := mc.Search(ctx, client.NewSearchOption("aaa", common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "can't find collection")

	// search with empty collections name
	_, err = mc.Search(ctx, client.NewSearchOption("", common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "collection name should not be empty")

	// search with not exist partition
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, err1 := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithPartitions("aaa"))
	common.CheckErr(t, err1, false, "partition name aaa not found")

	// search with empty partition name []string{""} -> error
	_, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).
		WithConsistencyLevel(entity.ClStrong).WithANNSField(common.DefaultFloatVecFieldName).WithPartitions(""))
	common.CheckErr(t, errSearch, false, "Partition name should not be empty")
}

// test search empty collection -> return empty
func TestSearchEmptyCollection(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, enableDynamicField := range []bool{true, false} {
		// create -> index -> load
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(),
			hp.TNewSchemaOption().TWithEnableDynamicField(enableDynamicField))
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		type mNameVec struct {
			fieldName string
			queryVec  []entity.Vector
		}
		for _, _mNameVec := range []mNameVec{
			{fieldName: common.DefaultFloatVecFieldName, queryVec: hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)},
			{fieldName: common.DefaultFloat16VecFieldName, queryVec: hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloat16Vector)},
			{fieldName: common.DefaultBFloat16VecFieldName, queryVec: hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeBFloat16Vector)},
			{fieldName: common.DefaultBinaryVecFieldName, queryVec: hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeBinaryVector)},
		} {
			resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, _mNameVec.queryVec).
				WithConsistencyLevel(entity.ClStrong).WithANNSField(_mNameVec.fieldName))
			common.CheckErr(t, errSearch, true)
			common.CheckSearchResult(t, resSearch, common.DefaultNq, 0)
		}
	}
}

func TestSearchEmptySparseCollection(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(),
		hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)
	resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).
		WithConsistencyLevel(entity.ClStrong).WithANNSField(common.DefaultSparseVecFieldName))
	common.CheckErr(t, errSearch, true)
	common.CheckSearchResult(t, resSearch, common.DefaultNq, 0)
}

// test search with partition names []string{}, specify partitions
func TestSearchPartitions(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	parName := common.GenRandomString("p", 4)
	// create collection and partition
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption().TWithAutoID(true),
		hp.TNewSchemaOption().TWithEnableDynamicField(true))
	err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// insert autoID data into parName and _default partitions
	_defVec := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	_defDynamic := hp.GenDynamicColumnData(0, common.DefaultNb)
	insertRes1, err1 := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(_defVec).WithColumns(_defDynamic...))
	common.CheckErr(t, err1, true)

	_parVec := hp.GenColumnData(common.DefaultNb, entity.FieldTypeFloatVector, *hp.TNewDataOption())
	insertRes2, err2 := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(_parVec))
	common.CheckErr(t, err2, true)

	// flush -> FLAT index -> load
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultFloatVecFieldName: index.NewFlatIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with empty partition name []string{""} -> error
	vectors := make([]entity.Vector, 0, 2)
	// query first ID of _default and parName partition
	_defId0, _ := insertRes1.IDs.GetAsInt64(0)
	_parId0, _ := insertRes2.IDs.GetAsInt64(0)
	queryRes, _ := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("int64 in [%d, %d]", _defId0, _parId0)).WithOutputFields("*"))
	require.ElementsMatch(t, []int64{_defId0, _parId0}, queryRes.GetColumn(common.DefaultInt64FieldName).(*column.ColumnInt64).Data())
	for _, vec := range queryRes.GetColumn(common.DefaultFloatVecFieldName).(*column.ColumnFloatVector).Data() {
		vectors = append(vectors, vec)
	}

	for _, partitions := range [][]string{{}, {common.DefaultPartition, parName}} {
		// search with empty partition names slice []string{} -> all partitions
		searchResult, errSearch1 := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, 5, vectors).
			WithConsistencyLevel(entity.ClStrong).WithANNSField(common.DefaultFloatVecFieldName).WithPartitions(partitions...).WithOutputFields("*"))

		// check search result contains search vector, which from all partitions
		common.CheckErr(t, errSearch1, true)
		common.CheckSearchResult(t, searchResult, len(vectors), 5)
		require.Contains(t, searchResult[0].IDs.(*column.ColumnInt64).Data(), _defId0)
		require.Contains(t, searchResult[1].IDs.(*column.ColumnInt64).Data(), _parId0)
		require.EqualValues(t, searchResult[0].GetColumn(common.DefaultFloatVecFieldName).(*column.ColumnFloatVector).Data()[0], vectors[0])
		require.EqualValues(t, searchResult[1].GetColumn(common.DefaultFloatVecFieldName).(*column.ColumnFloatVector).Data()[0], vectors[1])
	}
}

func TestInternalCollectionTakeOutputFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	requireInternalTakeStorageV2(t)
	enableInternalTakeForOutput(t)

	collName := common.GenRandomString("internal_take_all", 6)
	schema, outputFields := buildInternalTakeAllTypesSchema(collName)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	})

	data := buildInternalTakeAllTypesData(t, 12)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, data.columns...))
	common.CheckErr(t, err, true)

	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, flushTask.Await(ctx), true)

	indexes := map[string]index.Index{
		common.DefaultFloatVecFieldName:    index.NewFlatIndex(entity.L2),
		common.DefaultBinaryVecFieldName:   index.NewBinFlatIndex(entity.HAMMING),
		common.DefaultFloat16VecFieldName:  index.NewFlatIndex(entity.L2),
		common.DefaultBFloat16VecFieldName: index.NewFlatIndex(entity.L2),
		internalTakeInt8VecField: index.NewGenericIndex("int8_hnsw", map[string]string{
			index.MetricTypeKey: string(entity.COSINE),
			index.IndexTypeKey:  "HNSW",
			"M":                 "8",
			"efConstruction":    "64",
		}),
		common.DefaultSparseVecFieldName: index.NewSparseInvertedIndex(entity.IP, 0.3),
	}
	for fieldName, idx := range indexes {
		idxTask, idxErr := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, idx))
		common.CheckErr(t, idxErr, true)
		require.NoError(t, idxTask.Await(ctx), "create index on %s", fieldName)
	}

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	expr := fmt.Sprintf("%s in [0, 2, 4]", common.DefaultInt64FieldName)
	queryRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
		WithConsistencyLevel(entity.ClStrong).
		WithFilter(expr).
		WithOutputFields(outputFields...))
	common.CheckErr(t, err, true)
	require.Equal(t, 3, queryRes.ResultCount)
	common.CheckOutputFields(t, outputFields, queryRes.Fields)
	require.ElementsMatch(t, []int64{0, 2, 4}, queryRes.GetColumn(common.DefaultInt64FieldName).(*column.ColumnInt64).Data())
	assertInternalTakeAllTypesResult(t, queryRes, data, outputFields)

	vectors := make([]entity.Vector, 0, queryRes.ResultCount)
	for _, vec := range queryRes.GetColumn(common.DefaultFloatVecFieldName).(*column.ColumnFloatVector).Data() {
		require.Len(t, vec, internalTakeDim)
		vectors = append(vectors, vec)
	}

	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, 3, vectors).
		WithConsistencyLevel(entity.ClStrong).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithOutputFields(outputFields...))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, len(vectors), 3)
	for _, result := range searchRes {
		common.CheckOutputFields(t, outputFields, result.Fields)
		assertInternalTakeAllTypesResult(t, result, data, outputFields)
	}
}

func TestInternalCollectionTakeStructArrayOutputFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	requireInternalTakeStorageV2(t)
	enableInternalTakeForOutput(t)

	collName, _, data := canonicalStructArrayCollection(t, ctx, mc, 32)
	t.Cleanup(func() {
		_ = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	})

	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id in [0, 2, 4]").
		WithOutputFields("id", "clips").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 3, queryRes.ResultCount)
	common.CheckOutputFields(t, []string{"id", "clips"}, queryRes.Fields)
	assertStructArrayOutput(t, queryRes)

	searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 3,
		[]entity.Vector{entity.FloatVector(data.NormalVectors[0])}).
		WithANNSField("normal_vector").
		WithOutputFields("id", "clips").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, 1, 3)
	common.CheckOutputFields(t, []string{"id", "clips"}, searchRes[0].Fields)
	assertStructArrayOutput(t, searchRes[0])
}

type internalTakeAllTypesData struct {
	columns       []column.Column
	bools         []bool
	int8s         []int8
	int16s        []int16
	int32s        []int32
	int64s        []int64
	floats        []float32
	doubles       []float64
	varchars      []string
	jsons         [][]byte
	geometries    []string
	timestamps    []time.Time
	boolArrays    [][]bool
	int8Arrays    [][]int8
	int16Arrays   [][]int16
	int32Arrays   [][]int32
	int64Arrays   [][]int64
	floatArrays   [][]float32
	doubleArrays  [][]float64
	varcharArrays [][]string
	floatVecs     [][]float32
	binaryVecs    [][]byte
	float16Vecs   [][]byte
	bfloat16Vecs  [][]byte
	int8Vecs      [][]int8
	sparseVecs    []entity.SparseEmbedding
}

func requireInternalTakeStorageV2(t *testing.T) {
	t.Helper()

	value, err := hp.GetServerConfig("common.storage.useLoonFFI")
	require.NoError(t, err)
	if !strings.EqualFold(value, "true") {
		t.Skipf("internal take requires StorageV2/V3 reader, common.storage.useLoonFFI=%q", value)
	}
}

func enableInternalTakeForOutput(t *testing.T) {
	t.Helper()

	configKey := "queryNode.internalCollection.useTakeForOutput"
	prevConfig, err := hp.AlterServerConfig(configKey, "true")
	require.NoError(t, err)
	t.Cleanup(func() {
		if prevConfig == "" {
			prevConfig = "false"
		}
		_, _ = hp.AlterServerConfig(configKey, prevConfig)
	})
}

func buildInternalTakeAllTypesSchema(collName string) (*entity.Schema, []string) {
	outputFields := []string{
		common.DefaultInt64FieldName,
		common.DefaultBoolFieldName,
		common.DefaultInt8FieldName,
		common.DefaultInt16FieldName,
		common.DefaultInt32FieldName,
		common.DefaultFloatFieldName,
		common.DefaultDoubleFieldName,
		common.DefaultVarcharFieldName,
		common.DefaultJSONFieldName,
		common.DefaultGeometryFieldName,
		internalTakeTsField,
		common.DefaultBoolArrayField,
		common.DefaultInt8ArrayField,
		common.DefaultInt16ArrayField,
		common.DefaultInt32ArrayField,
		common.DefaultInt64ArrayField,
		common.DefaultFloatArrayField,
		common.DefaultDoubleArrayField,
		common.DefaultVarcharArrayField,
		common.DefaultFloatVecFieldName,
		common.DefaultBinaryVecFieldName,
		common.DefaultFloat16VecFieldName,
		common.DefaultBFloat16VecFieldName,
		internalTakeInt8VecField,
		common.DefaultSparseVecFieldName,
	}
	schema := entity.NewSchema().
		WithName(collName).
		WithField(entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(common.DefaultBoolFieldName).WithDataType(entity.FieldTypeBool)).
		WithField(entity.NewField().WithName(common.DefaultInt8FieldName).WithDataType(entity.FieldTypeInt8)).
		WithField(entity.NewField().WithName(common.DefaultInt16FieldName).WithDataType(entity.FieldTypeInt16)).
		WithField(entity.NewField().WithName(common.DefaultInt32FieldName).WithDataType(entity.FieldTypeInt32)).
		WithField(entity.NewField().WithName(common.DefaultFloatFieldName).WithDataType(entity.FieldTypeFloat)).
		WithField(entity.NewField().WithName(common.DefaultDoubleFieldName).WithDataType(entity.FieldTypeDouble)).
		WithField(entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(common.TestMaxLen)).
		WithField(entity.NewField().WithName(common.DefaultJSONFieldName).WithDataType(entity.FieldTypeJSON)).
		WithField(entity.NewField().WithName(common.DefaultGeometryFieldName).WithDataType(entity.FieldTypeGeometry)).
		WithField(entity.NewField().WithName(internalTakeTsField).WithDataType(entity.FieldTypeTimestamptz)).
		WithField(entity.NewField().WithName(common.DefaultBoolArrayField).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeBool).WithMaxCapacity(common.TestCapacity)).
		WithField(entity.NewField().WithName(common.DefaultInt8ArrayField).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt8).WithMaxCapacity(common.TestCapacity)).
		WithField(entity.NewField().WithName(common.DefaultInt16ArrayField).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt16).WithMaxCapacity(common.TestCapacity)).
		WithField(entity.NewField().WithName(common.DefaultInt32ArrayField).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt32).WithMaxCapacity(common.TestCapacity)).
		WithField(entity.NewField().WithName(common.DefaultInt64ArrayField).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64).WithMaxCapacity(common.TestCapacity)).
		WithField(entity.NewField().WithName(common.DefaultFloatArrayField).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeFloat).WithMaxCapacity(common.TestCapacity)).
		WithField(entity.NewField().WithName(common.DefaultDoubleArrayField).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeDouble).WithMaxCapacity(common.TestCapacity)).
		WithField(entity.NewField().WithName(common.DefaultVarcharArrayField).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).WithMaxLength(common.TestMaxLen).WithMaxCapacity(common.TestCapacity)).
		WithField(entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(internalTakeDim)).
		WithField(entity.NewField().WithName(common.DefaultBinaryVecFieldName).WithDataType(entity.FieldTypeBinaryVector).WithDim(internalTakeBinaryDim)).
		WithField(entity.NewField().WithName(common.DefaultFloat16VecFieldName).WithDataType(entity.FieldTypeFloat16Vector).WithDim(internalTakeDim)).
		WithField(entity.NewField().WithName(common.DefaultBFloat16VecFieldName).WithDataType(entity.FieldTypeBFloat16Vector).WithDim(internalTakeDim)).
		WithField(entity.NewField().WithName(internalTakeInt8VecField).WithDataType(entity.FieldTypeInt8Vector).WithDim(internalTakeDim)).
		WithField(entity.NewField().WithName(common.DefaultSparseVecFieldName).WithDataType(entity.FieldTypeSparseVector))
	return schema, outputFields
}

func buildInternalTakeAllTypesData(t *testing.T, nb int) internalTakeAllTypesData {
	t.Helper()

	data := internalTakeAllTypesData{
		bools:         make([]bool, 0, nb),
		int8s:         make([]int8, 0, nb),
		int16s:        make([]int16, 0, nb),
		int32s:        make([]int32, 0, nb),
		int64s:        make([]int64, 0, nb),
		floats:        make([]float32, 0, nb),
		doubles:       make([]float64, 0, nb),
		varchars:      make([]string, 0, nb),
		jsons:         make([][]byte, 0, nb),
		geometries:    make([]string, 0, nb),
		timestamps:    make([]time.Time, 0, nb),
		boolArrays:    make([][]bool, 0, nb),
		int8Arrays:    make([][]int8, 0, nb),
		int16Arrays:   make([][]int16, 0, nb),
		int32Arrays:   make([][]int32, 0, nb),
		int64Arrays:   make([][]int64, 0, nb),
		floatArrays:   make([][]float32, 0, nb),
		doubleArrays:  make([][]float64, 0, nb),
		varcharArrays: make([][]string, 0, nb),
		floatVecs:     make([][]float32, 0, nb),
		binaryVecs:    make([][]byte, 0, nb),
		float16Vecs:   make([][]byte, 0, nb),
		bfloat16Vecs:  make([][]byte, 0, nb),
		int8Vecs:      make([][]int8, 0, nb),
		sparseVecs:    make([]entity.SparseEmbedding, 0, nb),
	}
	baseTime := time.Date(2026, time.May, 26, 8, 0, 0, 0, time.UTC)
	for i := 0; i < nb; i++ {
		id := int64(i)
		floatVec := internalTakeFloatVector(i)
		fp16 := entity.FloatVector(floatVec).ToFloat16Vector()
		bf16 := entity.FloatVector(floatVec).ToBFloat16Vector()
		sparse, err := entity.NewSliceSparseEmbedding(
			[]uint32{uint32(i + 1), uint32(i + 17)},
			[]float32{float32(i) + 0.25, float32(i) + 0.75})
		require.NoError(t, err)

		data.int64s = append(data.int64s, id)
		data.bools = append(data.bools, i%2 == 0)
		data.int8s = append(data.int8s, int8(i-6))
		data.int16s = append(data.int16s, int16(i*10))
		data.int32s = append(data.int32s, int32(i*100))
		data.floats = append(data.floats, float32(i)+0.25)
		data.doubles = append(data.doubles, float64(i)+0.5)
		data.varchars = append(data.varchars, fmt.Sprintf("varchar_%02d", i))
		data.jsons = append(data.jsons, []byte(fmt.Sprintf(`{"id":%d,"tag":"row_%02d"}`, i, i)))
		data.geometries = append(data.geometries, fmt.Sprintf("POINT (%d %d)", i, i+1))
		data.timestamps = append(data.timestamps, baseTime.Add(time.Duration(i)*time.Minute))
		data.boolArrays = append(data.boolArrays, []bool{i%2 == 0, i%3 == 0})
		data.int8Arrays = append(data.int8Arrays, []int8{int8(i), int8(i + 1)})
		data.int16Arrays = append(data.int16Arrays, []int16{int16(i * 10), int16(i*10 + 1)})
		data.int32Arrays = append(data.int32Arrays, []int32{int32(i * 100), int32(i*100 + 1)})
		data.int64Arrays = append(data.int64Arrays, []int64{id, id + 100})
		data.floatArrays = append(data.floatArrays, []float32{float32(i) + 0.1, float32(i) + 0.2})
		data.doubleArrays = append(data.doubleArrays, []float64{float64(i) + 0.01, float64(i) + 0.02})
		data.varcharArrays = append(data.varcharArrays, []string{fmt.Sprintf("a_%02d", i), fmt.Sprintf("b_%02d", i)})
		data.floatVecs = append(data.floatVecs, floatVec)
		data.binaryVecs = append(data.binaryVecs, []byte{byte(i), byte(i + 16)})
		data.float16Vecs = append(data.float16Vecs, []byte(fp16))
		data.bfloat16Vecs = append(data.bfloat16Vecs, []byte(bf16))
		data.int8Vecs = append(data.int8Vecs, internalTakeInt8Vector(i))
		data.sparseVecs = append(data.sparseVecs, sparse)
	}

	data.columns = []column.Column{
		column.NewColumnInt64(common.DefaultInt64FieldName, data.int64s),
		column.NewColumnBool(common.DefaultBoolFieldName, data.bools),
		column.NewColumnInt8(common.DefaultInt8FieldName, data.int8s),
		column.NewColumnInt16(common.DefaultInt16FieldName, data.int16s),
		column.NewColumnInt32(common.DefaultInt32FieldName, data.int32s),
		column.NewColumnFloat(common.DefaultFloatFieldName, data.floats),
		column.NewColumnDouble(common.DefaultDoubleFieldName, data.doubles),
		column.NewColumnVarChar(common.DefaultVarcharFieldName, data.varchars),
		column.NewColumnJSONBytes(common.DefaultJSONFieldName, data.jsons),
		column.NewColumnGeometryWKT(common.DefaultGeometryFieldName, data.geometries),
		column.NewColumnTimestamptz(internalTakeTsField, data.timestamps),
		column.NewColumnBoolArray(common.DefaultBoolArrayField, data.boolArrays),
		column.NewColumnInt8Array(common.DefaultInt8ArrayField, data.int8Arrays),
		column.NewColumnInt16Array(common.DefaultInt16ArrayField, data.int16Arrays),
		column.NewColumnInt32Array(common.DefaultInt32ArrayField, data.int32Arrays),
		column.NewColumnInt64Array(common.DefaultInt64ArrayField, data.int64Arrays),
		column.NewColumnFloatArray(common.DefaultFloatArrayField, data.floatArrays),
		column.NewColumnDoubleArray(common.DefaultDoubleArrayField, data.doubleArrays),
		column.NewColumnVarCharArray(common.DefaultVarcharArrayField, data.varcharArrays),
		column.NewColumnFloatVector(common.DefaultFloatVecFieldName, internalTakeDim, data.floatVecs),
		column.NewColumnBinaryVector(common.DefaultBinaryVecFieldName, internalTakeBinaryDim, data.binaryVecs),
		column.NewColumnFloat16Vector(common.DefaultFloat16VecFieldName, internalTakeDim, data.float16Vecs),
		column.NewColumnBFloat16Vector(common.DefaultBFloat16VecFieldName, internalTakeDim, data.bfloat16Vecs),
		column.NewColumnInt8Vector(internalTakeInt8VecField, internalTakeDim, data.int8Vecs),
		column.NewColumnSparseVectors(common.DefaultSparseVecFieldName, data.sparseVecs),
	}
	return data
}

func internalTakeFloatVector(row int) []float32 {
	vector := make([]float32, internalTakeDim)
	for dim := range vector {
		vector[dim] = float32(row) + float32(dim)/10
	}
	return vector
}

func internalTakeInt8Vector(row int) []int8 {
	vector := make([]int8, internalTakeDim)
	for dim := range vector {
		vector[dim] = int8(row + dim)
	}
	return vector
}

func assertInternalTakeAllTypesResult(t *testing.T, result client.ResultSet, data internalTakeAllTypesData, outputFields []string) {
	t.Helper()

	for _, fieldName := range outputFields {
		col := result.GetColumn(fieldName)
		require.NotNil(t, col, "missing output field %s", fieldName)
		require.Equal(t, result.ResultCount, col.Len(), "field %s row count", fieldName)
	}
	for i := 0; i < result.ResultCount; i++ {
		id, err := result.GetColumn(common.DefaultInt64FieldName).GetAsInt64(i)
		require.NoError(t, err)
		row := int(id)
		require.GreaterOrEqual(t, row, 0)
		require.Less(t, row, len(data.int64s))

		boolVal, err := result.GetColumn(common.DefaultBoolFieldName).GetAsBool(i)
		require.NoError(t, err)
		require.Equal(t, data.bools[row], boolVal)

		int8Val, err := result.GetColumn(common.DefaultInt8FieldName).GetAsInt64(i)
		require.NoError(t, err)
		require.EqualValues(t, data.int8s[row], int8Val)

		int16Val, err := result.GetColumn(common.DefaultInt16FieldName).GetAsInt64(i)
		require.NoError(t, err)
		require.EqualValues(t, data.int16s[row], int16Val)

		int32Val, err := result.GetColumn(common.DefaultInt32FieldName).GetAsInt64(i)
		require.NoError(t, err)
		require.EqualValues(t, data.int32s[row], int32Val)

		floatVal, err := result.GetColumn(common.DefaultFloatFieldName).GetAsDouble(i)
		require.NoError(t, err)
		require.InDelta(t, data.floats[row], floatVal, 1e-6)

		doubleVal, err := result.GetColumn(common.DefaultDoubleFieldName).GetAsDouble(i)
		require.NoError(t, err)
		require.InDelta(t, data.doubles[row], doubleVal, 1e-9)

		varcharVal, err := result.GetColumn(common.DefaultVarcharFieldName).GetAsString(i)
		require.NoError(t, err)
		require.Equal(t, data.varchars[row], varcharVal)

		jsonRaw, err := result.GetColumn(common.DefaultJSONFieldName).Get(i)
		require.NoError(t, err)
		require.Equal(t, data.jsons[row], jsonRaw.([]byte))

		geometryVal, err := result.GetColumn(common.DefaultGeometryFieldName).GetAsString(i)
		require.NoError(t, err)
		require.NotEmpty(t, geometryVal)

		tsVal, err := result.GetColumn(internalTakeTsField).GetAsString(i)
		require.NoError(t, err)
		require.NotEmpty(t, tsVal)

		assertColumnValue(t, result, common.DefaultBoolArrayField, i, data.boolArrays[row])
		assertColumnValue(t, result, common.DefaultInt8ArrayField, i, data.int8Arrays[row])
		assertColumnValue(t, result, common.DefaultInt16ArrayField, i, data.int16Arrays[row])
		assertColumnValue(t, result, common.DefaultInt32ArrayField, i, data.int32Arrays[row])
		assertColumnValue(t, result, common.DefaultInt64ArrayField, i, data.int64Arrays[row])
		assertColumnValue(t, result, common.DefaultFloatArrayField, i, data.floatArrays[row])
		assertColumnValue(t, result, common.DefaultDoubleArrayField, i, data.doubleArrays[row])
		assertColumnValue(t, result, common.DefaultVarcharArrayField, i, data.varcharArrays[row])

		floatVecRaw, err := result.GetColumn(common.DefaultFloatVecFieldName).Get(i)
		require.NoError(t, err)
		require.Equal(t, data.floatVecs[row], []float32(floatVecRaw.(entity.FloatVector)))

		binaryVecRaw, err := result.GetColumn(common.DefaultBinaryVecFieldName).Get(i)
		require.NoError(t, err)
		require.Equal(t, data.binaryVecs[row], []byte(binaryVecRaw.(entity.BinaryVector)))

		fp16Raw, err := result.GetColumn(common.DefaultFloat16VecFieldName).Get(i)
		require.NoError(t, err)
		require.Equal(t, data.float16Vecs[row], []byte(fp16Raw.(entity.Float16Vector)))

		bf16Raw, err := result.GetColumn(common.DefaultBFloat16VecFieldName).Get(i)
		require.NoError(t, err)
		require.Equal(t, data.bfloat16Vecs[row], []byte(bf16Raw.(entity.BFloat16Vector)))

		int8VecRaw, err := result.GetColumn(internalTakeInt8VecField).Get(i)
		require.NoError(t, err)
		require.Equal(t, data.int8Vecs[row], []int8(int8VecRaw.(entity.Int8Vector)))

		sparseRaw, err := result.GetColumn(common.DefaultSparseVecFieldName).Get(i)
		require.NoError(t, err)
		require.Equal(t, data.sparseVecs[row].Serialize(), sparseRaw.(entity.SparseEmbedding).Serialize())
	}
}

func assertColumnValue(t *testing.T, result client.ResultSet, fieldName string, idx int, expected any) {
	t.Helper()

	raw, err := result.GetColumn(fieldName).Get(idx)
	require.NoError(t, err)
	require.Equal(t, expected, raw)
}

func assertStructArrayOutput(t *testing.T, result client.ResultSet) {
	t.Helper()

	clips := result.GetColumn("clips")
	require.NotNil(t, clips)
	require.Equal(t, result.ResultCount, clips.Len())
	for i := 0; i < result.ResultCount; i++ {
		raw, err := clips.Get(i)
		require.NoError(t, err)
		row, ok := raw.(map[string]any)
		require.Truef(t, ok, "clips row %d has unexpected type %T", i, raw)
		require.Contains(t, row, "clip_str")
		require.Contains(t, row, "clip_embedding1")
		require.Contains(t, row, "clip_embedding2")
	}
}

// test query empty output fields: []string{} -> []string{}
// test query empty output fields: []string{""} -> error
func TestSearchEmptyOutputFields(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, dynamic := range []bool{true, false} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(dynamic))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		resSearch, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields())
		common.CheckErr(t, err, true)
		common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
		common.CheckOutputFields(t, []string{}, resSearch[0].Fields)

		_, err = mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields(""))
		if dynamic {
			common.CheckErr(t, err, false, "parse output field name failed")
		} else {
			common.CheckErr(t, err, false, "field  not exist")
		}
	}
}

// test query with not existed field ["aa"]: error or as dynamic field
// test query with part not existed field ["aa", "$meat"]: error or as dynamic field
// test query with repeated field: ["*", "$meat"], ["floatVec", floatVec"] unique field
func TestSearchNotExistOutputFields(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, enableDynamic := range []bool{false, true} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(enableDynamic))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// search vector output fields not exist, part exist
		type dynamicOutputFields struct {
			outputFields    []string
			expOutputFields []string
		}
		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		dof := []dynamicOutputFields{
			{outputFields: []string{"aaa"}, expOutputFields: []string{"aaa"}},
			{outputFields: []string{"aaa", common.DefaultDynamicFieldName}, expOutputFields: []string{"aaa", common.DefaultDynamicFieldName}},
			{outputFields: []string{"*", common.DefaultDynamicFieldName}, expOutputFields: []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultDynamicFieldName}},
		}

		for _, _dof := range dof {
			resSearch, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields(_dof.outputFields...))
			if enableDynamic {
				common.CheckErr(t, err, true)
				common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
				common.CheckOutputFields(t, _dof.expOutputFields, resSearch[0].Fields)
			} else {
				common.CheckErr(t, err, false, "not exist")
			}
		}
		existedRepeatedFields := []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultInt64FieldName, common.DefaultFloatVecFieldName}
		resSearch2, err2 := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields(existedRepeatedFields...))
		common.CheckErr(t, err2, true)
		common.CheckSearchResult(t, resSearch2, common.DefaultNq, common.DefaultLimit)
		common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName}, resSearch2[0].Fields)
	}
}

// test search output all * fields when enable dynamic and insert dynamic column data
func TestSearchOutputAllFields(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	//
	allFieldsName := []string{common.DefaultDynamicFieldName}
	for _, field := range schema.Fields {
		allFieldsName = append(allFieldsName, field.Name)
	}
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)

	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
		WithANNSField(common.DefaultFloatVecFieldName).WithOutputFields("*"))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	for _, res := range searchRes {
		common.CheckOutputFields(t, allFieldsName, res.Fields)
	}
}

// test search output all * fields when enable dynamic and insert dynamic column data
func TestSearchOutputBinaryPk(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	//
	allFieldsName := []string{common.DefaultDynamicFieldName}
	for _, field := range schema.Fields {
		allFieldsName = append(allFieldsName, field.Name)
	}
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeBinaryVector)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields("*"))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	for _, res := range searchRes {
		common.CheckOutputFields(t, allFieldsName, res.Fields)
	}
}

// test search output all * fields when enable dynamic and insert dynamic column data
func TestSearchOutputSparse(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	//
	allFieldsName := []string{common.DefaultDynamicFieldName}
	for _, field := range schema.Fields {
		allFieldsName = append(allFieldsName, field.Name)
	}
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
		WithANNSField(common.DefaultSparseVecFieldName).WithOutputFields("*"))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	for _, res := range searchRes {
		common.CheckOutputFields(t, allFieldsName, res.Fields)
	}
}

// test search with invalid vector field name: not exist; non-vector field, empty fiend name, json and dynamic field -> error
func TestSearchInvalidVectorField(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	type invalidVectorFieldStruct struct {
		vectorField string
		errNil      bool
		errMsg      string
	}

	invalidVectorFields := []invalidVectorFieldStruct{
		// not exist field
		{vectorField: common.DefaultBinaryVecFieldName, errNil: false, errMsg: fmt.Sprintf("failed to get field schema by name: fieldName(%s) not found", common.DefaultBinaryVecFieldName)},

		// non-vector field
		{vectorField: common.DefaultInt64FieldName, errNil: false, errMsg: fmt.Sprintf("failed to create query plan: field (%s) to search is not of vector data type", common.DefaultInt64FieldName)},

		// json field
		{vectorField: common.DefaultJSONFieldName, errNil: false, errMsg: fmt.Sprintf("failed to get field schema by name: fieldName(%s) not found", common.DefaultJSONFieldName)},

		// dynamic field
		{vectorField: common.DefaultDynamicFieldName, errNil: false, errMsg: fmt.Sprintf("failed to get field schema by name: fieldName(%s) not found", common.DefaultDynamicFieldName)},

		// allows empty vector field name
		{vectorField: "", errNil: true, errMsg: ""},
	}

	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)
	for _, invalidVectorField := range invalidVectorFields {
		_, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithANNSField(invalidVectorField.vectorField))
		common.CheckErr(t, err, invalidVectorField.errNil, invalidVectorField.errMsg)
	}
}

// test search with invalid vectors
func TestSearchInvalidVectors(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64MultiVec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	type invalidVectorsStruct struct {
		fieldName string
		vectors   []entity.Vector
		errMsg    string
	}

	invalidVectors := []invalidVectorsStruct{
		// dim not match
		{fieldName: common.DefaultFloatVecFieldName, vectors: hp.GenSearchVectors(common.DefaultNq, 64, entity.FieldTypeFloatVector), errMsg: "vector dimension mismatch"},
		{fieldName: common.DefaultFloat16VecFieldName, vectors: hp.GenSearchVectors(common.DefaultNq, 64, entity.FieldTypeFloat16Vector), errMsg: "vector dimension mismatch"},

		// vector type not match
		{fieldName: common.DefaultFloatVecFieldName, vectors: hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeBinaryVector), errMsg: "vector type must be the same"},
		{fieldName: common.DefaultBFloat16VecFieldName, vectors: hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloat16Vector), errMsg: "vector type must be the same"},

		// empty vectors
		{fieldName: common.DefaultBinaryVecFieldName, vectors: []entity.Vector{}, errMsg: "nq [0] is invalid"},
		{fieldName: common.DefaultFloatVecFieldName, vectors: []entity.Vector{entity.FloatVector{}}, errMsg: "vector dimension mismatch"},
		{vectors: hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector), errMsg: "multiple anns_fields exist, please specify a anns_field in search_params"},
		{fieldName: "", vectors: hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector), errMsg: "multiple anns_fields exist, please specify a anns_field in search_params"},
	}

	for _, invalidVector := range invalidVectors {
		_, errSearchEmpty := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, invalidVector.vectors).WithANNSField(invalidVector.fieldName))
		common.CheckErr(t, errSearchEmpty, false, invalidVector.errMsg)
	}
}

// test search with invalid vectors
func TestSearchEmptyInvalidVectors(t *testing.T) {
	t.Log("https://github.com/milvus-io/milvus/issues/33639")
	t.Log("https://github.com/milvus-io/milvus/issues/33637")
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	type invalidVectorsStruct struct {
		vectors []entity.Vector
		errNil  bool
		errMsg  string
	}

	invalidVectors := []invalidVectorsStruct{
		// dim not match
		{vectors: hp.GenSearchVectors(common.DefaultNq, 64, entity.FieldTypeFloatVector), errNil: true, errMsg: "vector dimension mismatch"},

		// vector type not match
		{vectors: hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeBinaryVector), errNil: true, errMsg: "vector type must be the same"},

		// empty vectors
		{vectors: []entity.Vector{}, errNil: false, errMsg: "nq [0] is invalid"},
		{vectors: []entity.Vector{entity.FloatVector{}}, errNil: true, errMsg: "vector dimension mismatch"},
	}

	for _, invalidVector := range invalidVectors {
		_, errSearchEmpty := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, invalidVector.vectors).WithANNSField(common.DefaultFloatVecFieldName))
		common.CheckErr(t, errSearchEmpty, invalidVector.errNil, invalidVector.errMsg)
	}
}

// test search metric type isn't the same with index metric type
func TestSearchNotMatchMetricType(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).
		TWithFieldIndex(map[string]index.Index{common.DefaultFloatVecFieldName: index.NewHNSWIndex(entity.COSINE, 8, 200)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	_, errSearchEmpty := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithSearchParam("metric_type", "L2"))
	common.CheckErr(t, errSearchEmpty, false, "metric type not match: invalid parameter")
}

// test search with invalid topK -> error
func TestSearchInvalidTopK(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	for _, invalidTopK := range []int{-1, 0, 16385} {
		_, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, invalidTopK, vectors))
		common.CheckErr(t, errSearch, false, "should be in range [1, 16384]")
	}
}

// test search with invalid topK -> error
func TestSearchInvalidOffset(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	for _, invalidOffset := range []int{-1, common.MaxTopK + 1} {
		_, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithOffset(invalidOffset))
		common.CheckErr(t, errSearch, false, "should be in range [1, 16384]")
	}
}

// test search with invalid search params
func TestSearchInvalidSearchParams(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).
		TWithFieldIndex(map[string]index.Index{common.DefaultFloatVecFieldName: index.NewHNSWIndex(entity.COSINE, 8, 200)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)

	// search with invalid hnsw ef
	invalidEfs := []int{-1, 0, 32769}
	for _, invalidEf := range invalidEfs {
		_, errHnsw := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithSearchParam("ef", strconv.Itoa(invalidEf)))
		common.CheckErr(t, errHnsw, true, "No error for invalid search params")
	}

	// test search params mismatch index type, hnsw index and ivf sq8 search param -> search with default hnsw params, ef=topK
	invalidNprobes := []int{-1, 0, 65537}
	for _, invalidNprobe := range invalidNprobes {
		_, errHnsw := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithSearchParam("nprobe", strconv.Itoa(invalidNprobe)))
		common.CheckErr(t, errHnsw, true, "No error for invalid search params")
	}

	// search with index hnsw search param ef < topK -> error
	res, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithSearchParam("ef", "7"))
	common.CheckErr(t, err, true, "ef(7) should be larger than k(10), but no error")
	common.CheckSearchResult(t, res, 1, common.DefaultLimit)
}

// search with index scann search param ef < topK -> error
func TestSearchInvalidScannReorderK(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{
		common.DefaultFloatVecFieldName: index.NewSCANNIndex(entity.COSINE, 16, true),
	}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with invalid reorder_k < topK
	vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)

	// search with invalid hnsw ef
	_, errScann := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).
		WithSearchParam("nprobe", "8").WithSearchParam("reorder_k", strconv.Itoa(common.DefaultLimit-1)))
	common.CheckErr(t, errScann, true, "No error for invalid search params")

	// valid scann index search reorder_k
	res, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).
		WithSearchParam("nprobe", "8").WithSearchParam("reorder_k", "20"))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, res, 1, common.DefaultLimit)
}

// test search with scann index params: with_raw_data and metrics_type [L2, IP, COSINE]
func TestSearchScannAllMetricsWithRawData(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{}, 3)
	wg := sync.WaitGroup{}
	testFunc := func(withRawData bool, metricType entity.MetricType) {
		defer func() {
			wg.Done()
			<-ch
		}()
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := hp.CreateDefaultMilvusClient(ctx, t)

		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON),
			hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{
			common.DefaultFloatVecFieldName: index.NewSCANNIndex(metricType, 16, withRawData),
		}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// search and output all fields
		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).
			WithConsistencyLevel(entity.ClStrong).WithOutputFields("*"))
		common.CheckErr(t, errSearch, true)
		common.CheckOutputFields(t, []string{
			common.DefaultInt64FieldName, common.DefaultJSONFieldName,
			common.DefaultFloatVecFieldName, common.DefaultDynamicFieldName,
		}, resSearch[0].Fields)
		common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
	}
	for _, withRawData := range []bool{true, false} {
		for _, metricType := range []entity.MetricType{entity.L2, entity.IP, entity.COSINE} {
			ch <- struct{}{}
			wg.Add(1)
			go testFunc(withRawData, metricType)
		}
	}
	wg.Wait()
}

// test search with valid expression
func TestSearchExpr(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	type mExprExpected struct {
		expr  string
		ids   []int64
		value any
	}

	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	for _, _mExpr := range []mExprExpected{
		{expr: fmt.Sprintf("%s < 10", common.DefaultInt64FieldName), ids: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{expr: fmt.Sprintf("%s in [10, 100]", common.DefaultInt64FieldName), ids: []int64{10, 100}},
	} {
		resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithFilter(_mExpr.expr))
		common.CheckErr(t, errSearch, true)
		for _, res := range resSearch {
			require.ElementsMatch(t, _mExpr.ids, res.IDs.(*column.ColumnInt64).Data())
		}
	}
	// search with template param
	for _, _mExpr := range []mExprExpected{
		{expr: fmt.Sprintf("%s < {v}", common.DefaultInt64FieldName), ids: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, value: 10},
		{expr: fmt.Sprintf("%s in {v}", common.DefaultInt64FieldName), ids: []int64{10, 100}, value: []int64{10, 100}},
	} {
		resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithFilter(_mExpr.expr).WithTemplateParam("v", _mExpr.value))
		common.CheckErr(t, errSearch, true)
		for _, res := range resSearch {
			require.ElementsMatch(t, _mExpr.ids, res.IDs.(*column.ColumnInt64).Data())
		}
	}
}

// test search with invalid expression
func TestSearchInvalidExpr(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with invalid expr
	vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	for _, exprStruct := range common.InvalidExpressions {
		mlog.Debug(context.TODO(), "TestSearchInvalidExpr", zap.String("expr", exprStruct.Expr))
		_, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
			WithFilter(exprStruct.Expr).WithANNSField(common.DefaultFloatVecFieldName))
		common.CheckErr(t, errSearch, exprStruct.ErrNil, exprStruct.ErrMsg)
	}
}

func TestSearchJsonFieldExpr(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	exprs := []string{
		"",
		fmt.Sprintf("exists %s['number'] ", common.DefaultJSONFieldName),   // exists
		"json[\"number\"] > 1 and json[\"number\"] < 1000",                 // > and
		fmt.Sprintf("%s[\"number\"] > 10", common.DefaultJSONFieldName),    // number >
		fmt.Sprintf("%s != 10 ", common.DefaultJSONFieldName),              // json != 10
		fmt.Sprintf("%s[\"number\"] < 2000", common.DefaultJSONFieldName),  // number <
		fmt.Sprintf("%s[\"bool\"] != true", common.DefaultJSONFieldName),   // bool !=
		fmt.Sprintf("%s[\"bool\"] == False", common.DefaultJSONFieldName),  // bool ==
		fmt.Sprintf("%s[\"bool\"] in [true]", common.DefaultJSONFieldName), // bool in
		fmt.Sprintf("%s[\"string\"] >= '1' ", common.DefaultJSONFieldName), // string >=
		fmt.Sprintf("%s['list'][0] > 200", common.DefaultJSONFieldName),    // list filter
		fmt.Sprintf("%s['list'] != [2, 3]", common.DefaultJSONFieldName),   // json[list] !=
		fmt.Sprintf("%s > 2000", common.DefaultJSONFieldName),              // json > 2000
		fmt.Sprintf("%s like '2%%' ", common.DefaultJSONFieldName),         // json like '2%'
		fmt.Sprintf("%s[0] > 2000 ", common.DefaultJSONFieldName),          // json[0] > 2000
		fmt.Sprintf("%s > 2000.5 ", common.DefaultJSONFieldName),           // json > 2000.5
	}

	for _, dynamicField := range []bool{false, true} {
		// create collection
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption().
			TWithEnableDynamicField(dynamicField))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// search with jsonField expr key datatype and json data type mismatch
		for _, expr := range exprs {
			t.Run(fmt.Sprintf("expr=%s_dynamic-%t", expr, dynamicField), func(t *testing.T) {
				mlog.Debug(context.TODO(), "TestSearchJsonFieldExpr", zap.String("expr", expr))
				vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
				searchRes, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
					WithFilter(expr).WithANNSField(common.DefaultFloatVecFieldName).WithOutputFields(common.DefaultInt64FieldName, common.DefaultJSONFieldName))
				common.CheckErr(t, errSearch, true)
				common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultJSONFieldName}, searchRes[0].Fields)
				common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
			})
		}
	}
}

func TestSearchDynamicFieldExpr(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	// create collection
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption().
		TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	exprs := []string{
		"",
		"exists dynamicNumber", // exist without dynamic fieldName
		fmt.Sprintf("exists %s[\"dynamicNumber\"]", common.DefaultDynamicFieldName), // exist with fieldName
		fmt.Sprintf("%s[\"dynamicNumber\"] > 10", common.DefaultDynamicFieldName),   // int expr with fieldName
		fmt.Sprintf("%s[\"dynamicBool\"] == true", common.DefaultDynamicFieldName),  // bool with fieldName
		"dynamicBool == False", // bool without fieldName
		fmt.Sprintf("%s['dynamicString'] == '1'", common.DefaultDynamicFieldName), // string with fieldName
		"dynamicString != \"2\" ", // string without fieldName
	}

	// search with jsonField expr key datatype and json data type mismatch
	for _, expr := range exprs {
		mlog.Debug(context.TODO(), "TestSearchDynamicFieldExpr", zap.String("expr", expr))
		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		searchRes, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
			WithFilter(expr).WithANNSField(common.DefaultFloatVecFieldName).WithOutputFields(common.DefaultInt64FieldName, "dynamicNumber", "number"))
		common.CheckErr(t, errSearch, true)
		common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, "dynamicNumber", "number"}, searchRes[0].Fields)
		if expr == "$meta['dynamicString'] == '1'" {
			common.CheckSearchResult(t, searchRes, common.DefaultNq, 1)
		} else {
			common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
		}
	}

	// search with expr filter number and, &&, or, ||
	exprs2 := []string{
		"dynamicNumber > 1 and dynamicNumber <= 999", // int expr without fieldName
		fmt.Sprintf("%s['dynamicNumber'] > 1 && %s['dynamicNumber'] < 1000", common.DefaultDynamicFieldName, common.DefaultDynamicFieldName),
		"dynamicNumber < 888 || dynamicNumber < 1000",
		fmt.Sprintf("%s['dynamicNumber'] < 888 or %s['dynamicNumber'] < 1000", common.DefaultDynamicFieldName, common.DefaultDynamicFieldName),
		fmt.Sprintf("%s[\"dynamicNumber\"] < 1000", common.DefaultDynamicFieldName), // int expr with fieldName
	}

	for _, expr := range exprs2 {
		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		searchRes, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
			WithFilter(expr).WithANNSField(common.DefaultFloatVecFieldName).
			WithOutputFields(common.DefaultInt64FieldName, common.DefaultJSONFieldName, common.DefaultDynamicFieldName, "dynamicNumber", "number"))
		common.CheckErr(t, errSearch, true)
		common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultJSONFieldName, common.DefaultDynamicFieldName, "dynamicNumber", "number"}, searchRes[0].Fields)
		for _, res := range searchRes {
			for _, id := range res.IDs.(*column.ColumnInt64).Data() {
				require.Less(t, id, int64(1000))
			}
		}
	}
}

func TestSearchArrayFieldExpr(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecArray), hp.TNewFieldsOption(), hp.TNewSchemaOption().
		TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	var capacity int64 = common.TestCapacity
	exprs := []string{
		fmt.Sprintf("%s[0] == false", common.DefaultBoolArrayField),                            // array[0] ==
		fmt.Sprintf("%s[0] > 0", common.DefaultInt64ArrayField),                                // array[0] >
		fmt.Sprintf("json_contains (%s, %d)", common.DefaultInt16ArrayField, capacity),         // json_contains
		fmt.Sprintf("array_contains (%s, %d)", common.DefaultInt16ArrayField, capacity),        // array_contains
		fmt.Sprintf("json_contains_all (%s, [90, 91])", common.DefaultInt64ArrayField),         // json_contains_all
		fmt.Sprintf("array_contains_all (%s, [90, 91])", common.DefaultInt64ArrayField),        // array_contains_all
		fmt.Sprintf("array_contains_any (%s, [0, 100, 10000])", common.DefaultFloatArrayField), // array_contains_any
		fmt.Sprintf("json_contains_any (%s, [0, 100, 10])", common.DefaultFloatArrayField),     // json_contains_any
		fmt.Sprintf("array_length(%s) == %d", common.DefaultDoubleArrayField, capacity),        // array_length
	}

	// search with jsonField expr key datatype and json data type mismatch
	allArrayFields := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		if field.DataType == entity.FieldTypeArray {
			allArrayFields = append(allArrayFields, field.Name)
		}
	}
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	for _, expr := range exprs {
		searchRes, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
			WithFilter(expr).WithOutputFields(allArrayFields...))
		common.CheckErr(t, errSearch, true)
		common.CheckOutputFields(t, allArrayFields, searchRes[0].Fields)
		common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	}

	// search hits empty
	searchRes, errSearchEmpty := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
		WithFilter(fmt.Sprintf("array_contains (%s, 1000000)", common.DefaultInt32ArrayField)).WithOutputFields(allArrayFields...))
	common.CheckErr(t, errSearchEmpty, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, 0)
}

// test search with field not existed expr: if dynamic
func TestSearchNotExistedExpr(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, isDynamic := range [2]bool{true, false} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().
			TWithEnableDynamicField(isDynamic))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// search with invalid expr
		vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
		expr := "id in [0]"
		res, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
			WithFilter(expr).WithANNSField(common.DefaultFloatVecFieldName))
		if isDynamic {
			common.CheckErr(t, errSearch, true)
			common.CheckSearchResult(t, res, 1, 0)
		} else {
			common.CheckErr(t, errSearch, false, "not exist")
		}
	}
}

// test search with fp16/ bf16 /binary vector
func TestSearchMultiVectors(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64MultiVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().
		TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*2))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	flatIndex := index.NewFlatIndex(entity.L2)
	binIndex := index.NewGenericIndex(common.DefaultBinaryVecFieldName, map[string]string{"nlist": "64", index.MetricTypeKey: "JACCARD", index.IndexTypeKey: "BIN_IVF_FLAT"})
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{
		common.DefaultFloatVecFieldName:    flatIndex,
		common.DefaultFloat16VecFieldName:  flatIndex,
		common.DefaultBFloat16VecFieldName: flatIndex,
		common.DefaultBinaryVecFieldName:   binIndex,
	}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with all kinds of vectors
	type fieldTestCase struct {
		fieldName     string
		fieldType     entity.FieldType
		metricType    entity.MetricType
		genVectorFunc func(nq int, dim int, dataType entity.FieldType) []entity.Vector
	}
	testCases := []fieldTestCase{
		{fieldName: common.DefaultFloatVecFieldName, fieldType: entity.FieldTypeFloatVector, metricType: entity.L2, genVectorFunc: hp.GenSearchVectors},
		{fieldName: common.DefaultBinaryVecFieldName, fieldType: entity.FieldTypeBinaryVector, metricType: entity.JACCARD, genVectorFunc: hp.GenSearchVectors},
		{fieldName: common.DefaultFloat16VecFieldName, fieldType: entity.FieldTypeFloat16Vector, metricType: entity.L2, genVectorFunc: hp.GenSearchVectors},
		{fieldName: common.DefaultBFloat16VecFieldName, fieldType: entity.FieldTypeBFloat16Vector, metricType: entity.L2, genVectorFunc: hp.GenSearchVectors},
		// field type is float16 / bfloat16, but query with float vector
		{fieldName: common.DefaultFloat16VecFieldName, fieldType: entity.FieldTypeFloat16Vector, metricType: entity.L2, genVectorFunc: hp.GenFp16OrBf16VectorsFromFloatVector},
		{fieldName: common.DefaultBFloat16VecFieldName, fieldType: entity.FieldTypeBFloat16Vector, metricType: entity.L2, genVectorFunc: hp.GenFp16OrBf16VectorsFromFloatVector},
	}

	for _, tc := range testCases {
		queryVec := tc.genVectorFunc(common.DefaultNq, common.DefaultDim, tc.fieldType)
		expr := fmt.Sprintf("%s > 10", common.DefaultInt64FieldName)

		resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit*2, queryVec).WithConsistencyLevel(entity.ClStrong).
			WithFilter(expr).WithANNSField(tc.fieldName).WithOutputFields("*"))
		common.CheckErr(t, errSearch, true)
		common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit*2)
		common.CheckOutputFields(t, []string{
			common.DefaultInt64FieldName, common.DefaultFloatVecFieldName,
			common.DefaultBinaryVecFieldName, common.DefaultFloat16VecFieldName, common.DefaultBFloat16VecFieldName, common.DefaultDynamicFieldName,
		}, resSearch[0].Fields)

		// pagination search
		resPage, errPage := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithConsistencyLevel(entity.ClStrong).
			WithFilter(expr).WithANNSField(tc.fieldName).WithOutputFields("*").WithOffset(10))

		common.CheckErr(t, errPage, true)
		common.CheckSearchResult(t, resPage, common.DefaultNq, common.DefaultLimit)
		for i := 0; i < common.DefaultNq; i++ {
			require.Equal(t, resSearch[i].IDs.(*column.ColumnInt64).Data()[10:], resPage[i].IDs.(*column.ColumnInt64).Data())
		}
		common.CheckOutputFields(t, []string{
			common.DefaultInt64FieldName, common.DefaultFloatVecFieldName,
			common.DefaultBinaryVecFieldName, common.DefaultFloat16VecFieldName, common.DefaultBFloat16VecFieldName, common.DefaultDynamicFieldName,
		}, resPage[0].Fields)

		// TODO range search
		// TODO iterator search
	}
}

func TestSearchSparseVector(t *testing.T) {
	t.Parallel()
	idxInverted := index.NewGenericIndex(common.DefaultSparseVecFieldName, map[string]string{"drop_ratio_build": "0.2", index.MetricTypeKey: "IP", index.IndexTypeKey: "SPARSE_INVERTED_INDEX"})
	idxWand := index.NewGenericIndex(common.DefaultSparseVecFieldName, map[string]string{"drop_ratio_build": "0.3", index.MetricTypeKey: "IP", index.IndexTypeKey: "SPARSE_WAND"})
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, idx := range []index.Index{idxInverted, idxWand} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().
			TWithEnableDynamicField(true))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithSparseMaxLen(128).TWithNb(common.DefaultNb*2))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultSparseVecFieldName: idx}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// search
		queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)
		resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithConsistencyLevel(entity.ClStrong).
			WithOutputFields("*"))

		common.CheckErr(t, errSearch, true)
		require.Len(t, resSearch, common.DefaultNq)
		outputFields := []string{common.DefaultInt64FieldName, common.DefaultVarcharFieldName, common.DefaultSparseVecFieldName, common.DefaultDynamicFieldName}
		for _, res := range resSearch {
			require.LessOrEqual(t, res.ResultCount, common.DefaultLimit)
			if res.ResultCount == common.DefaultLimit {
				common.CheckOutputFields(t, outputFields, resSearch[0].Fields)
			}
		}
	}
}

// test search with invalid sparse vector
func TestSearchInvalidSparseVector(t *testing.T) {
	t.Parallel()

	idxInverted := index.NewGenericIndex(common.DefaultSparseVecFieldName, map[string]string{"drop_ratio_build": "0.2", index.MetricTypeKey: "IP", index.IndexTypeKey: "SPARSE_INVERTED_INDEX"})
	idxWand := index.NewGenericIndex(common.DefaultSparseVecFieldName, map[string]string{"drop_ratio_build": "0.3", index.MetricTypeKey: "IP", index.IndexTypeKey: "SPARSE_WAND"})
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, idx := range []index.Index{idxInverted, idxWand} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().
			TWithEnableDynamicField(true))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithSparseMaxLen(128))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultSparseVecFieldName: idx}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		_, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, []entity.Vector{}).WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, errSearch, false, "nq (number of search vector per search request) should be in range [1, 16384]")

		positions := make([]uint32, 100)
		values := make([]float32, 100)
		for i := 0; i < 100; i++ {
			positions[i] = uint32(1)
			values[i] = rand.Float32()
		}
		vector, _ := entity.NewSliceSparseEmbedding(positions, values)
		_, errSearch2 := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, []entity.Vector{vector}).WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, errSearch2, false, "Invalid sparse row: id should be strict ascending")
	}
}

// test search with empty sparse vector
func TestSearchWithEmptySparseVector(t *testing.T) {
	t.Parallel()
	idxInverted := index.NewSparseInvertedIndex(entity.IP, 0.1)
	idxWand := index.NewSparseWANDIndex(entity.IP, 0.1)
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, idx := range []index.Index{idxInverted, idxWand} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().
			TWithEnableDynamicField(true))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithSparseMaxLen(128))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultSparseVecFieldName: idx}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// An empty sparse vector is considered to be uncorrelated with any other vector.
		vector1, err := entity.NewSliceSparseEmbedding([]uint32{}, []float32{})
		common.CheckErr(t, err, true)
		searchRes, errSearch1 := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, []entity.Vector{vector1}).WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, errSearch1, true)
		common.CheckSearchResult(t, searchRes, 1, 0)
	}
}

// test search from empty sparse vectors collection
func TestSearchFromEmptySparseVector(t *testing.T) {
	t.Parallel()

	idxInverted := index.NewSparseInvertedIndex(entity.IP, 0.1)
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, idx := range []index.Index{idxInverted} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().
			TWithEnableDynamicField(true))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithSparseMaxLen(128).TWithStart(common.DefaultNb))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultSparseVecFieldName: idx}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// insert sparse vector: empty position and values
		columnOpt := hp.TNewDataOption()
		data := []column.Column{
			hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *columnOpt),
			hp.GenColumnData(common.DefaultNb, entity.FieldTypeVarChar, *columnOpt),
		}
		sparseVecs := make([]entity.SparseEmbedding, 0, common.DefaultNb)
		for i := 0; i < common.DefaultNb; i++ {
			vec, _ := entity.NewSliceSparseEmbedding([]uint32{}, []float32{})
			sparseVecs = append(sparseVecs, vec)
		}

		data = append(data, column.NewColumnSparseVectors(common.DefaultSparseVecFieldName, sparseVecs))
		insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, data...))
		common.CheckErr(t, err, true)
		require.EqualValues(t, common.DefaultNb, insertRes.InsertCount)

		// search vector is or not empty sparse vector
		vector1, _ := entity.NewSliceSparseEmbedding([]uint32{}, []float32{})
		vector2, _ := entity.NewSliceSparseEmbedding([]uint32{0, 2, 5, 10, 100}, []float32{rand.Float32(), rand.Float32(), rand.Float32(), rand.Float32(), rand.Float32()})

		// search from sparse collection: part normal sparse vectors, part empty sparse
		// excepted: The empty vector is not related to any other vector, so it will not be returned，and alsopty obtained as the search vector.
		for limit, vector := range map[int]entity.Vector{0: vector1, common.DefaultLimit: vector2} {
			searchRes, errSearch1 := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, []entity.Vector{vector}).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, errSearch1, true)
			common.CheckSearchResult(t, searchRes, 1, limit)
		}
	}
}

func TestSearchSparseVectorPagination(t *testing.T) {
	t.Parallel()
	idxInverted := index.NewGenericIndex(common.DefaultSparseVecFieldName, map[string]string{"drop_ratio_build": "0.2", index.MetricTypeKey: "IP", index.IndexTypeKey: "SPARSE_INVERTED_INDEX"})
	idxWand := index.NewGenericIndex(common.DefaultSparseVecFieldName, map[string]string{"drop_ratio_build": "0.3", index.MetricTypeKey: "IP", index.IndexTypeKey: "SPARSE_WAND"})
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, idx := range []index.Index{idxInverted, idxWand} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().
			TWithEnableDynamicField(true))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithSparseMaxLen(128))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultSparseVecFieldName: idx}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// search
		queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)
		resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithConsistencyLevel(entity.ClStrong).
			WithOutputFields("*"))
		common.CheckErr(t, errSearch, true)
		require.Len(t, resSearch, common.DefaultNq)

		pageSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithConsistencyLevel(entity.ClStrong).
			WithOutputFields("*").WithOffset(5))
		common.CheckErr(t, errSearch, true)
		require.Len(t, pageSearch, common.DefaultNq)
		for i := 0; i < len(resSearch); i++ {
			if resSearch[i].ResultCount == common.DefaultLimit && pageSearch[i].ResultCount == 5 {
				require.Equal(t, resSearch[i].IDs.(*column.ColumnInt64).Data()[5:], pageSearch[i].IDs.(*column.ColumnInt64).Data())
			}
		}
	}
}

// test sparse vector unsupported search: TODO iterator search
func TestSearchSparseVectorNotSupported(t *testing.T) {
	t.Parallel()

	t.Skip("Go-sdk support iterator search in progress")
}

func TestRangeSearchSparseVector(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().
		TWithEnableDynamicField(true))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithSparseMaxLen(128))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// range search
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)

	resRange, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithSearchParam("drop_ratio_search", "0.2"))
	common.CheckErr(t, errSearch, true)
	require.Len(t, resRange, common.DefaultNq)
	for _, res := range resRange {
		mlog.Info(context.TODO(), "default search", zap.Any("score", res.Scores))
	}

	annParams := index.NewSparseAnnParam()
	annParams.WithRadius(10)
	annParams.WithRangeFilter(30)
	annParams.WithDropRatio(0.2)
	resRange, errSearch = mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).
		WithAnnParam(annParams))
	common.CheckErr(t, errSearch, true)
	common.CheckErr(t, errSearch, true)
	require.Len(t, resRange, common.DefaultNq)
	for _, res := range resRange {
		mlog.Info(context.TODO(), "range search", zap.Any("score", res.Scores))
	}
	for _, res := range resRange {
		for _, s := range res.Scores {
			require.GreaterOrEqual(t, s, float32(10))
			require.Less(t, s, float32(30))
		}
	}
}
