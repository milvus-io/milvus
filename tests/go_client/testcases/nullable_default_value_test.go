package testcases

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func int64SliceToString(ids []int64) string {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(strs, ", ")
}

type NullableVectorType struct {
	Name      string
	FieldType entity.FieldType
}

func GetVectorTypes() []NullableVectorType {
	return []NullableVectorType{
		{"FloatVector", entity.FieldTypeFloatVector},
		{"BinaryVector", entity.FieldTypeBinaryVector},
		{"Float16Vector", entity.FieldTypeFloat16Vector},
		{"BFloat16Vector", entity.FieldTypeBFloat16Vector},
		{"Int8Vector", entity.FieldTypeInt8Vector},
		{"SparseVector", entity.FieldTypeSparseVector},
	}
}

func GetNullPercents() []int {
	return []int{0, 30}
}

type NullableVectorTestData struct {
	ValidData       []bool
	ValidCount      int
	PkToVecIdx      map[int64]int
	OriginalVectors interface{}
	VecColumn       column.Column
	SearchVec       entity.Vector
}

func GenerateNullableVectorTestData(t *testing.T, vt NullableVectorType, nb int, nullPercent int, fieldName string) *NullableVectorTestData {
	data := &NullableVectorTestData{
		ValidData:  make([]bool, nb),
		PkToVecIdx: make(map[int64]int),
	}

	for i := range nb {
		data.ValidData[i] = (i % 100) >= nullPercent
		if data.ValidData[i] {
			data.ValidCount++
		}
	}

	vecIdx := 0
	for i := range nb {
		if data.ValidData[i] {
			data.PkToVecIdx[int64(i)] = vecIdx
			vecIdx++
		}
	}

	var err error
	switch vt.FieldType {
	case entity.FieldTypeFloatVector:
		vectors := make([][]float32, data.ValidCount)
		for i := range data.ValidCount {
			vec := make([]float32, common.DefaultDim)
			for j := range common.DefaultDim {
				vec[j] = float32(i*common.DefaultDim+j) / 10000.0
			}
			vectors[i] = vec
		}
		data.OriginalVectors = vectors
		data.VecColumn, err = column.NewNullableColumnFloatVector(fieldName, common.DefaultDim, vectors, data.ValidData)
		if data.ValidCount > 0 {
			data.SearchVec = entity.FloatVector(vectors[0])
		}

	case entity.FieldTypeBinaryVector:
		byteDim := common.DefaultDim / 8
		vectors := make([][]byte, data.ValidCount)
		for i := range data.ValidCount {
			vec := make([]byte, byteDim)
			for j := range byteDim {
				vec[j] = byte((i + j) % 256)
			}
			vectors[i] = vec
		}
		data.OriginalVectors = vectors
		data.VecColumn, err = column.NewNullableColumnBinaryVector(fieldName, common.DefaultDim, vectors, data.ValidData)
		if data.ValidCount > 0 {
			data.SearchVec = entity.BinaryVector(vectors[0])
		}

	case entity.FieldTypeFloat16Vector:
		vectors := make([][]byte, data.ValidCount)
		for i := range data.ValidCount {
			vectors[i] = common.GenFloat16Vector(common.DefaultDim)
		}
		data.OriginalVectors = vectors
		data.VecColumn, err = column.NewNullableColumnFloat16Vector(fieldName, common.DefaultDim, vectors, data.ValidData)
		if data.ValidCount > 0 {
			data.SearchVec = entity.Float16Vector(vectors[0])
		}

	case entity.FieldTypeBFloat16Vector:
		vectors := make([][]byte, data.ValidCount)
		for i := range data.ValidCount {
			vectors[i] = common.GenBFloat16Vector(common.DefaultDim)
		}
		data.OriginalVectors = vectors
		data.VecColumn, err = column.NewNullableColumnBFloat16Vector(fieldName, common.DefaultDim, vectors, data.ValidData)
		if data.ValidCount > 0 {
			data.SearchVec = entity.BFloat16Vector(vectors[0])
		}

	case entity.FieldTypeInt8Vector:
		vectors := make([][]int8, data.ValidCount)
		for i := range data.ValidCount {
			vec := make([]int8, common.DefaultDim)
			for j := range common.DefaultDim {
				vec[j] = int8((i + j) % 127)
			}
			vectors[i] = vec
		}
		data.OriginalVectors = vectors
		data.VecColumn, err = column.NewNullableColumnInt8Vector(fieldName, common.DefaultDim, vectors, data.ValidData)
		if data.ValidCount > 0 {
			data.SearchVec = entity.Int8Vector(vectors[0])
		}

	case entity.FieldTypeSparseVector:
		vectors := make([]entity.SparseEmbedding, data.ValidCount)
		for i := range data.ValidCount {
			positions := []uint32{0, uint32(i + 1), uint32(i + 1000)}
			values := []float32{1.0, float32(i+1) / 1000.0, 0.1}
			vectors[i], err = entity.NewSliceSparseEmbedding(positions, values)
			common.CheckErr(t, err, true)
		}
		data.OriginalVectors = vectors
		data.VecColumn, err = column.NewNullableColumnSparseFloatVector(fieldName, vectors, data.ValidData)
		if data.ValidCount > 0 {
			data.SearchVec = vectors[0]
		}
	}
	common.CheckErr(t, err, true)

	return data
}

type IndexConfig struct {
	Name       string
	IndexType  string
	MetricType entity.MetricType
	Params     map[string]string
}

func GetIndexesForVectorType(fieldType entity.FieldType) []IndexConfig {
	switch fieldType {
	case entity.FieldTypeFloatVector:
		return []IndexConfig{
			{"FLAT", "FLAT", entity.L2, nil},
			{"IVF_FLAT", "IVF_FLAT", entity.L2, map[string]string{"nlist": "128"}},
			{"IVF_SQ8", "IVF_SQ8", entity.L2, map[string]string{"nlist": "128"}},
			{"IVF_PQ", "IVF_PQ", entity.L2, map[string]string{"nlist": "128", "m": "8", "nbits": "8"}},
			{"HNSW", "HNSW", entity.L2, map[string]string{"M": "16", "efConstruction": "200"}},
			{"SCANN", "SCANN", entity.L2, map[string]string{"nlist": "128", "with_raw_data": "true"}},
			{"DISKANN", "DISKANN", entity.L2, nil},
		}
	case entity.FieldTypeBinaryVector:
		return []IndexConfig{
			{"BIN_FLAT", "BIN_FLAT", entity.JACCARD, nil},
			{"BIN_IVF_FLAT", "BIN_IVF_FLAT", entity.JACCARD, map[string]string{"nlist": "128"}},
		}
	case entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector:
		return []IndexConfig{
			{"FLAT", "FLAT", entity.L2, nil},
			{"IVF_FLAT", "IVF_FLAT", entity.L2, map[string]string{"nlist": "128"}},
			{"IVF_SQ8", "IVF_SQ8", entity.L2, map[string]string{"nlist": "128"}},
			{"HNSW", "HNSW", entity.L2, map[string]string{"M": "16", "efConstruction": "200"}},
		}
	case entity.FieldTypeInt8Vector:
		return []IndexConfig{
			{"HNSW", "HNSW", entity.COSINE, map[string]string{"M": "16", "efConstruction": "200"}},
		}
	case entity.FieldTypeSparseVector:
		return []IndexConfig{
			{"SPARSE_INVERTED_INDEX", "SPARSE_INVERTED_INDEX", entity.IP, map[string]string{"drop_ratio_build": "0.1"}},
			{"SPARSE_WAND", "SPARSE_WAND", entity.IP, map[string]string{"drop_ratio_build": "0.1"}},
		}
	default:
		return []IndexConfig{
			{"FLAT", "FLAT", entity.L2, nil},
		}
	}
}

func CreateIndexFromConfig(fieldName string, cfg IndexConfig) index.Index {
	params := map[string]string{
		index.MetricTypeKey: string(cfg.MetricType),
		index.IndexTypeKey:  cfg.IndexType,
	}
	for k, v := range cfg.Params {
		params[k] = v
	}
	return index.NewGenericIndex(fieldName, params)
}

func CreateNullableVectorIndex(vt NullableVectorType) index.Index {
	return CreateNullableVectorIndexWithFieldName(vt, "vector")
}

func CreateNullableVectorIndexWithFieldName(vt NullableVectorType, fieldName string) index.Index {
	indexes := GetIndexesForVectorType(vt.FieldType)
	if len(indexes) > 0 {
		return CreateIndexFromConfig(fieldName, indexes[0])
	}
	return index.NewGenericIndex(fieldName, map[string]string{
		index.MetricTypeKey: string(entity.L2),
		index.IndexTypeKey:  "FLAT",
	})
}

func VerifyNullableVectorData(t *testing.T, vt NullableVectorType, queryResult client.ResultSet, pkToVecIdx map[int64]int, originalVectors interface{}, context string) {
	pkCol := queryResult.GetColumn(common.DefaultInt64FieldName).(*column.ColumnInt64)
	vecCol := queryResult.GetColumn("vector")
	for i := 0; i < queryResult.ResultCount; i++ {
		pk, _ := pkCol.GetAsInt64(i)
		isNull, _ := vecCol.IsNull(i)

		if origIdx, ok := pkToVecIdx[pk]; ok {
			require.False(t, isNull, "%s: vector should not be null for pk %d", context, pk)
			vecData, _ := vecCol.Get(i)

			switch vt.FieldType {
			case entity.FieldTypeFloatVector:
				vectors := originalVectors.([][]float32)
				queriedVec := []float32(vecData.(entity.FloatVector))
				require.EqualValues(t, common.DefaultDim, len(queriedVec), "%s: vector dimension should match for pk %d", context, pk)
				origVec := vectors[origIdx]
				for j := range origVec {
					require.InDelta(t, origVec[j], queriedVec[j], 1e-6, "%s: vector element %d should match for pk %d", context, j, pk)
				}
			case entity.FieldTypeInt8Vector:
				vectors := originalVectors.([][]int8)
				queriedVec := []int8(vecData.(entity.Int8Vector))
				require.EqualValues(t, common.DefaultDim, len(queriedVec), "%s: vector dimension should match for pk %d", context, pk)
				origVec := vectors[origIdx]
				for j := range origVec {
					require.EqualValues(t, origVec[j], queriedVec[j], "%s: vector element %d should match for pk %d", context, j, pk)
				}
			case entity.FieldTypeBinaryVector:
				vectors := originalVectors.([][]byte)
				queriedVec := []byte(vecData.(entity.BinaryVector))
				byteDim := common.DefaultDim / 8
				require.EqualValues(t, byteDim, len(queriedVec), "%s: vector byte dimension should match for pk %d", context, pk)
				origVec := vectors[origIdx]
				for j := range origVec {
					require.EqualValues(t, origVec[j], queriedVec[j], "%s: vector byte %d should match for pk %d", context, j, pk)
				}
			case entity.FieldTypeFloat16Vector:
				queriedVec := []byte(vecData.(entity.Float16Vector))
				byteDim := common.DefaultDim * 2
				require.EqualValues(t, byteDim, len(queriedVec), "%s: vector byte dimension should match for pk %d", context, pk)
			case entity.FieldTypeBFloat16Vector:
				queriedVec := []byte(vecData.(entity.BFloat16Vector))
				byteDim := common.DefaultDim * 2
				require.EqualValues(t, byteDim, len(queriedVec), "%s: vector byte dimension should match for pk %d", context, pk)
			case entity.FieldTypeSparseVector:
				vectors := originalVectors.([]entity.SparseEmbedding)
				queriedVec := vecData.(entity.SparseEmbedding)
				origVec := vectors[origIdx]
				require.EqualValues(t, origVec.Len(), queriedVec.Len(), "%s: sparse vector length should match for pk %d", context, pk)
				for j := 0; j < origVec.Len(); j++ {
					origPos, origVal, _ := origVec.Get(j)
					queriedPos, queriedVal, _ := queriedVec.Get(j)
					require.EqualValues(t, origPos, queriedPos, "%s: sparse vector position %d should match for pk %d", context, j, pk)
					require.InDelta(t, origVal, queriedVal, 1e-6, "%s: sparse vector value %d should match for pk %d", context, j, pk)
				}
			}
		} else {
			require.True(t, isNull, "%s: vector should be null for pk %d", context, pk)
			vecData, _ := vecCol.Get(i)
			require.Nil(t, vecData, "%s: null vector data should be nil for pk %d", context, pk)
		}
	}
}

func VerifyNullableVectorDataWithFieldName(t *testing.T, vt NullableVectorType, queryResult client.ResultSet, pkToVecIdx map[int64]int, originalVectors interface{}, fieldName string, context string) {
	pkCol := queryResult.GetColumn(common.DefaultInt64FieldName).(*column.ColumnInt64)
	vecCol := queryResult.GetColumn(fieldName)
	for i := 0; i < queryResult.ResultCount; i++ {
		pk, _ := pkCol.GetAsInt64(i)
		isNull, _ := vecCol.IsNull(i)

		if origIdx, ok := pkToVecIdx[pk]; ok {
			require.False(t, isNull, "%s: vector should not be null for pk %d", context, pk)
			vecData, _ := vecCol.Get(i)

			switch vt.FieldType {
			case entity.FieldTypeFloatVector:
				vectors := originalVectors.([][]float32)
				queriedVec := []float32(vecData.(entity.FloatVector))
				require.EqualValues(t, common.DefaultDim, len(queriedVec), "%s: vector dimension should match for pk %d", context, pk)
				origVec := vectors[origIdx]
				for j := range origVec {
					require.InDelta(t, origVec[j], queriedVec[j], 1e-6, "%s: vector element %d should match for pk %d", context, j, pk)
				}
			case entity.FieldTypeInt8Vector:
				vectors := originalVectors.([][]int8)
				queriedVec := []int8(vecData.(entity.Int8Vector))
				require.EqualValues(t, common.DefaultDim, len(queriedVec), "%s: vector dimension should match for pk %d", context, pk)
				origVec := vectors[origIdx]
				for j := range origVec {
					require.EqualValues(t, origVec[j], queriedVec[j], "%s: vector element %d should match for pk %d", context, j, pk)
				}
			case entity.FieldTypeBinaryVector:
				vectors := originalVectors.([][]byte)
				queriedVec := []byte(vecData.(entity.BinaryVector))
				byteDim := common.DefaultDim / 8
				require.EqualValues(t, byteDim, len(queriedVec), "%s: vector byte dimension should match for pk %d", context, pk)
				origVec := vectors[origIdx]
				for j := range origVec {
					require.EqualValues(t, origVec[j], queriedVec[j], "%s: vector byte %d should match for pk %d", context, j, pk)
				}
			case entity.FieldTypeFloat16Vector:
				queriedVec := []byte(vecData.(entity.Float16Vector))
				byteDim := common.DefaultDim * 2
				require.EqualValues(t, byteDim, len(queriedVec), "%s: vector byte dimension should match for pk %d", context, pk)
			case entity.FieldTypeBFloat16Vector:
				queriedVec := []byte(vecData.(entity.BFloat16Vector))
				byteDim := common.DefaultDim * 2
				require.EqualValues(t, byteDim, len(queriedVec), "%s: vector byte dimension should match for pk %d", context, pk)
			case entity.FieldTypeSparseVector:
				vectors := originalVectors.([]entity.SparseEmbedding)
				queriedVec := vecData.(entity.SparseEmbedding)
				origVec := vectors[origIdx]
				require.EqualValues(t, origVec.Len(), queriedVec.Len(), "%s: sparse vector length should match for pk %d", context, pk)
				for j := 0; j < origVec.Len(); j++ {
					origPos, origVal, _ := origVec.Get(j)
					queriedPos, queriedVal, _ := queriedVec.Get(j)
					require.EqualValues(t, origPos, queriedPos, "%s: sparse vector position %d should match for pk %d", context, j, pk)
					require.InDelta(t, origVal, queriedVal, 1e-6, "%s: sparse vector value %d should match for pk %d", context, j, pk)
				}
			}
		} else {
			require.True(t, isNull, "%s: vector should be null for pk %d", context, pk)
			vecData, _ := vecCol.Get(i)
			require.Nil(t, vecData, "%s: null vector data should be nil for pk %d", context, pk)
		}
	}
}

// create collection with nullable fields and insert with column / nullableColumn
func TestNullableDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create fields: pk + floatVec + all nullable scalar fields
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	schema := entity.NewSchema().WithName(common.GenRandomString("nullable_default_value", 5)).WithField(pkField).WithField(vecField)

	// create collection with all supported nullable fields
	expNullableFields := make([]string, 0)
	for _, fieldType := range hp.GetAllNullableFieldType() {
		nullableField := entity.NewField().WithName(common.GenRandomString("null", 5)).WithDataType(fieldType).WithNullable(true)
		if fieldType == entity.FieldTypeVarChar {
			nullableField.WithMaxLength(common.TestMaxLen)
		}
		if fieldType == entity.FieldTypeArray {
			nullableField.WithElementType(entity.FieldTypeInt64).WithMaxCapacity(common.TestCapacity)
		}
		schema.WithField(nullableField)
		expNullableFields = append(expNullableFields, nullableField.Name)
	}

	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	// describe collection and check nullable fields
	descCollection, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	common.CheckFieldsNullable(t, expNullableFields, descCollection.Schema)

	prepare := hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// insert data with default column
	defColumnOpt := hp.TNewColumnOptions()
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), defColumnOpt)

	// query with null expr
	for _, nullField := range expNullableFields {
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s is null", nullField)).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, 0, count)
	}

	// insert data with nullable column
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 1
	}
	columnOpt := hp.TNewColumnOptions()
	for _, name := range expNullableFields {
		columnOpt = columnOpt.WithColumnOption(name, hp.TNewDataOption().TWithValidData(validData))
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), columnOpt)

	hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query with null expr
	for _, nullField := range expNullableFields {
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s is null", nullField)).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, common.DefaultNb/2, count)
	}
}

// create collection with default value and insert with column / nullableColumn
func TestDefaultValueDefault(t *testing.T) {
	t.Skip("set defaultValue and insert with default column gets unexpected error, waiting for fix")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create fields: pk + floatVec + default value scalar fields
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	schema := entity.NewSchema().WithName(common.GenRandomString("default_value", 5)).WithField(pkField).WithField(vecField)

	// create collection with all supported nullable fields
	defaultBoolField := entity.NewField().WithName(common.GenRandomString("bool", 3)).WithDataType(entity.FieldTypeBool).WithDefaultValueBool(true)
	defaultInt8Field := entity.NewField().WithName(common.GenRandomString("int8", 3)).WithDataType(entity.FieldTypeInt8).WithDefaultValueInt(-1)
	defaultInt16Field := entity.NewField().WithName(common.GenRandomString("int16", 3)).WithDataType(entity.FieldTypeInt16).WithDefaultValueInt(4)
	defaultInt32Field := entity.NewField().WithName(common.GenRandomString("int32", 3)).WithDataType(entity.FieldTypeInt32).WithDefaultValueInt(2000)
	defaultInt64Field := entity.NewField().WithName(common.GenRandomString("int64", 3)).WithDataType(entity.FieldTypeInt64).WithDefaultValueLong(10000)
	defaultFloatField := entity.NewField().WithName(common.GenRandomString("float", 3)).WithDataType(entity.FieldTypeFloat).WithDefaultValueFloat(-1.0)
	defaultDoubleField := entity.NewField().WithName(common.GenRandomString("double", 3)).WithDataType(entity.FieldTypeDouble).WithDefaultValueDouble(math.MaxFloat64)
	defaultVarCharField := entity.NewField().WithName(common.GenRandomString("varchar", 3)).WithDataType(entity.FieldTypeVarChar).WithDefaultValueString("default").WithMaxLength(common.TestMaxLen)
	schema.WithField(defaultBoolField).WithField(defaultInt8Field).WithField(defaultInt16Field).WithField(defaultInt32Field).WithField(defaultInt64Field).WithField(defaultFloatField).WithField(defaultDoubleField).WithField(defaultVarCharField)

	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
	common.CheckErr(t, err, true)
	coll, _ := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckFieldsDefaultValue(t, map[string]interface{}{
		defaultBoolField.Name:    true,
		defaultInt8Field.Name:    int8(-1),
		defaultInt16Field.Name:   int16(4),
		defaultInt32Field.Name:   int32(2000),
		defaultInt64Field.Name:   int64(10000),
		defaultFloatField.Name:   float32(-1.0),
		defaultDoubleField.Name:  math.MaxFloat64,
		defaultVarCharField.Name: "default",
	}, coll.Schema)

	prepare := hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// insert data with default column
	defColumnOpt := hp.TNewColumnOptions()
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), defColumnOpt)

	// query with null expr
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s == -1", defaultInt8Field.Name)).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 0, count)

	// insert data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	columnOpt := hp.TNewColumnOptions()
	for _, name := range []string{
		defaultBoolField.Name, defaultInt8Field.Name, defaultInt16Field.Name, defaultInt32Field.Name,
		defaultInt64Field.Name, defaultFloatField.Name, defaultDoubleField.Name, defaultVarCharField.Name,
	} {
		columnOpt = columnOpt.WithColumnOption(name, hp.TNewDataOption().TWithValidData(validData))
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), columnOpt)

	// query with expr check default value
	type exprCount struct {
		expr  string
		count int64
	}
	exprCounts := []exprCount{
		{expr: fmt.Sprintf("%s == %t", defaultBoolField.Name, true), count: common.DefaultNb * 5 / 4},
		{expr: fmt.Sprintf("%s == %d", defaultInt8Field.Name, -1), count: common.DefaultNb/2 + 10}, // int8 [-128, 127]
		{expr: fmt.Sprintf("%s == %d", defaultInt16Field.Name, 4), count: common.DefaultNb/2 + 2},
		{expr: fmt.Sprintf("%s == %d", defaultInt32Field.Name, 2000), count: common.DefaultNb / 2},
		{expr: fmt.Sprintf("%s == %d", defaultInt64Field.Name, 10000), count: common.DefaultNb / 2},
		{expr: fmt.Sprintf("%s == %f", defaultFloatField.Name, -1.0), count: common.DefaultNb / 2},
		{expr: fmt.Sprintf("%s == %f", defaultDoubleField.Name, math.MaxFloat64), count: common.DefaultNb / 2},
		{expr: fmt.Sprintf("%s == '%s'", defaultVarCharField.Name, "default"), count: common.DefaultNb / 2},
	}
	for _, exprCount := range exprCounts {
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprCount.expr).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.Equal(t, exprCount.count, count)
	}
}

func TestNullableInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	pkField := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.GenRandomString("vec", 3)).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	// pk field not support null
	pkFieldNull := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithNullable(true)
	schema := entity.NewSchema().WithName(common.GenRandomString("nullable_invalid_field", 5)).WithField(pkFieldNull).WithField(vecField)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
	common.CheckErr(t, err, false, "primary field not support null")

	supportedNullableVectorTypes := []entity.FieldType{entity.FieldTypeFloatVector, entity.FieldTypeBinaryVector, entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector, entity.FieldTypeSparseVector, entity.FieldTypeInt8Vector}
	for _, fieldType := range supportedNullableVectorTypes {
		nullableVectorField := entity.NewField().WithName(common.GenRandomString("null", 3)).WithDataType(fieldType).WithNullable(true)
		if fieldType != entity.FieldTypeSparseVector {
			nullableVectorField.WithDim(128)
		}
		schema := entity.NewSchema().WithName(common.GenRandomString("nullable_vector", 5)).WithField(pkField).WithField(nullableVectorField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
		common.CheckErr(t, err, true)
		mc.DropCollection(ctx, client.NewDropCollectionOption(schema.CollectionName))
	}

	// partition-key field not support null
	partitionField := entity.NewField().WithName(common.GenRandomString("partition", 3)).WithDataType(entity.FieldTypeInt64).WithIsPartitionKey(true).WithNullable(true)
	schema = entity.NewSchema().WithName(common.GenRandomString("nullable_invalid_field", 5)).WithField(pkField).WithField(vecField).WithField(partitionField)
	err = mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
	common.CheckErr(t, err, false, "partition key field not support nullable")
}

func TestDefaultValueInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	pkField := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.GenRandomString("vec", 3)).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	// pk field not support default value
	pkFieldNull := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithDefaultValueLong(1)
	schema := entity.NewSchema().WithName(common.GenRandomString("def_invalid_field", 5)).WithField(pkFieldNull).WithField(vecField)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
	common.CheckErr(t, err, false, "primary field not support default_value")

	// vector type not support default value
	notSupportedDefaultValueDataTypes := []entity.FieldType{entity.FieldTypeFloatVector, entity.FieldTypeBinaryVector, entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector, entity.FieldTypeSparseVector, entity.FieldTypeInt8Vector}
	for _, fieldType := range notSupportedDefaultValueDataTypes {
		nullableVectorField := entity.NewField().WithName(common.GenRandomString("def", 3)).WithDataType(fieldType).WithDefaultValueFloat(2.0)
		if fieldType != entity.FieldTypeSparseVector {
			nullableVectorField.WithDim(128)
		}
		schema := entity.NewSchema().WithName(common.GenRandomString("def_invalid_field", 5)).WithField(pkField).WithField(nullableVectorField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
		common.CheckErr(t, err, false, "type not support default_value")
	}
	// json and array type not support default value
	notSupportedDefaultValueDataTypes = []entity.FieldType{entity.FieldTypeJSON, entity.FieldTypeArray}
	for _, fieldType := range notSupportedDefaultValueDataTypes {
		nullableVectorField := entity.NewField().WithName(common.GenRandomString("def", 3)).WithDataType(fieldType).WithElementType(entity.FieldTypeFloat).WithMaxCapacity(100).WithDefaultValueFloat(2.0)
		schema := entity.NewSchema().WithName(common.GenRandomString("def_invalid_field", 5)).WithField(pkField).WithField(vecField).WithField(nullableVectorField)
		err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
		common.CheckErr(t, err, false, "type not support default_value")
	}
}

func TestDefaultValueInvalidValue(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	pkField := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.GenRandomString("vec", 3)).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	// 定义测试用例
	testCases := []struct {
		name        string
		fieldName   string
		dataType    entity.FieldType
		setupField  func() *entity.Field
		expectedErr string
	}{
		{
			name:      "varchar field default_value_length > max_length",
			fieldName: common.DefaultVarcharFieldName,
			dataType:  entity.FieldTypeVarChar,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultVarcharFieldName).
					WithDataType(entity.FieldTypeVarChar).
					WithDefaultValueString("defaultaaaaaaaaa").
					WithMaxLength(2)
			},
			expectedErr: "the length (16) of string exceeds max length (2)",
		},
		{
			name:      "varchar field with int default_value",
			fieldName: common.DefaultVarcharFieldName,
			dataType:  entity.FieldTypeVarChar,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultVarcharFieldName).
					WithDataType(entity.FieldTypeVarChar).
					WithDefaultValueInt(2).
					WithMaxLength(100)
			},
			expectedErr: fmt.Sprintf("type (VarChar) of field (%s) is not equal to the type(DataType_Int) of default_value", common.DefaultVarcharFieldName),
		},
		{
			name:      "int32 field with int64 default_value",
			fieldName: common.DefaultInt32FieldName,
			dataType:  entity.FieldTypeInt32,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultInt32FieldName).
					WithDataType(entity.FieldTypeInt32).
					WithDefaultValueLong(2)
			},
			expectedErr: fmt.Sprintf("type (Int32) of field (%s) is not equal to the type(DataType_Int64) of default_value", common.DefaultInt32FieldName),
		},
		{
			name:      "int64 field with int default_value",
			fieldName: common.DefaultInt64FieldName,
			dataType:  entity.FieldTypeInt64,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultInt64FieldName).
					WithDataType(entity.FieldTypeInt64).
					WithDefaultValueInt(2)
			},
			expectedErr: fmt.Sprintf("type (Int64) of field (%s) is not equal to the type(DataType_Int) of default_value", common.DefaultInt64FieldName),
		},
		{
			name:      "float field with double default_value",
			fieldName: common.DefaultFloatFieldName,
			dataType:  entity.FieldTypeFloat,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultFloatFieldName).
					WithDataType(entity.FieldTypeFloat).
					WithDefaultValueDouble(2.6)
			},
			expectedErr: fmt.Sprintf("type (Float) of field (%s) is not equal to the type(DataType_Double) of default_value", common.DefaultFloatFieldName),
		},
		{
			name:      "double field with varchar default_value",
			fieldName: common.DefaultDoubleFieldName,
			dataType:  entity.FieldTypeDouble,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultDoubleFieldName).
					WithDataType(entity.FieldTypeDouble).
					WithDefaultValueString("2.6")
			},
			expectedErr: fmt.Sprintf("type (Double) of field (%s) is not equal to the type(DataType_VarChar) of default_value", common.DefaultDoubleFieldName),
		},
	}

	// 执行测试用例
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defField := tc.setupField()
			schema := entity.NewSchema().WithName("def_invalid_field").WithField(pkField).WithField(vecField).WithField(defField)
			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
			common.CheckErr(t, err, false, tc.expectedErr)
		})
	}
}

func TestDefaultValueOutOfRange(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	pkField := entity.NewField().WithName(common.GenRandomString("pk", 3)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.GenRandomString("vec", 3)).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)

	testCases := []struct {
		name        string
		fieldName   string
		dataType    entity.FieldType
		setupField  func() *entity.Field
		expectedErr string
	}{
		{
			name:      "int8 field with out_of_range default_value",
			fieldName: common.DefaultInt8FieldName,
			dataType:  entity.FieldTypeInt8,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultInt8FieldName).WithDataType(entity.FieldTypeInt8).WithDefaultValueInt(128)
			},
			expectedErr: "[128 out of range -128 <= value <= 127]",
		},
		{
			name:      "int16 field with out_of_range default_value",
			fieldName: common.DefaultInt16FieldName,
			dataType:  entity.FieldTypeInt16,
			setupField: func() *entity.Field {
				return entity.NewField().WithName(common.DefaultInt16FieldName).WithDataType(entity.FieldTypeInt16).WithDefaultValueInt(-32769)
			},
			expectedErr: "[-32769 out of range -32768 <= value <= 32767]",
		},
	}

	// 执行测试用例
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defField := tc.setupField()
			schema := entity.NewSchema().WithName(common.GenRandomString("def", 5)).WithField(pkField).WithField(vecField).WithField(defField)
			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema))
			common.CheckErr(t, err, false, tc.expectedErr)
		})
	}
}

// test default value "" and insert ""
func TestDefaultValueVarcharEmpty(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithDefaultValue(""))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	coll, _ := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckFieldsDefaultValue(t, map[string]interface{}{
		common.DefaultVarcharFieldName: "",
	}, coll.Schema)

	// insert data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)))
	hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	expr := fmt.Sprintf("%s == ''", common.DefaultVarcharFieldName)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb/2, count)

	// insert varchar data: ""
	varcharValues := make([]string, common.DefaultNb/2)
	for i := 0; i < common.DefaultNb/2; i++ {
		varcharValues[i] = ""
	}
	columnOpt := hp.TNewColumnOptions().WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb)).
		WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData).TWithTextData(varcharValues))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), columnOpt)
	countRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ = countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb*3/2, count)
}

// test insert with nullableColumn into normal collection
func TestNullableDefaultInsertInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create normal default collection -> insert null values
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption())
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption())

	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeSparseVector, *hp.TNewDataOption().TWithSparseMaxLen(common.DefaultDim))
	varcharColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeVarChar, *hp.TNewDataOption().TWithValidData(validData))
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn).WithColumns(vecColumn).WithColumns(varcharColumn))
	common.CheckErr(t, err, false, "the length of valid_data of field(varchar) is wrong")
}

// test insert with part/all/not null -> query check
func TestNullableQuery(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	coll, _ := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckFieldsNullable(t, []string{common.DefaultVarcharFieldName}, coll.Schema)

	// insert data with part null, all null, all valid
	partNullValidData := make([]bool, common.DefaultNb)
	allNullValidData := make([]bool, common.DefaultNb)
	allValidData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		partNullValidData[i] = i%3 == 0
		allNullValidData[i] = false
		allValidData[i] = true
	}
	// [o, nb] -> 2*nb/3 null
	// [nb, 2*nb] -> all null
	// [2*nb, 3*nb] -> all valid
	// [3*nb, 4*nb] -> all valid
	for i, data := range [][]bool{partNullValidData, allNullValidData, allValidData} {
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)).
			WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(data).TWithStart(common.DefaultNb*i)))
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
		WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*3)))

	hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	exprCounts := []struct {
		expr  string
		count int64
	}{
		{expr: fmt.Sprintf("%s is null and (%d <= %s < %d)", common.DefaultVarcharFieldName, 0, common.DefaultInt64FieldName, common.DefaultNb), count: common.DefaultNb * 2 / 3},
		{expr: fmt.Sprintf("%s is null and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb, common.DefaultInt64FieldName, common.DefaultNb*2), count: common.DefaultNb},
		{expr: fmt.Sprintf("%s is null and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb*2, common.DefaultInt64FieldName, common.DefaultNb*3), count: 0},
		{expr: fmt.Sprintf("%s is not null and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb*3, common.DefaultInt64FieldName, common.DefaultNb*4), count: common.DefaultNb},
	}
	for _, exprCount := range exprCounts {
		log.Info("exprCount", zap.String("expr", exprCount.expr), zap.Int64("count", exprCount.count))
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprCount.expr).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, exprCount.count, count)
	}
}

// test insert with part/all/not default value -> query check
func TestDefaultValueQuery(t *testing.T) {
	t.Skip("set defaultValue and insert with default column gets unexpected error, waiting for fix")
	for _, nullable := range [2]bool{false, true} {
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := hp.CreateDefaultMilvusClient(ctx, t)

		fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithDefaultValue("test").TWithNullable(nullable))
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
		coll, _ := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
		common.CheckFieldsDefaultValue(t, map[string]interface{}{
			common.DefaultVarcharFieldName: "test",
		}, coll.Schema)

		// insert data with part null, all null, all valid
		partNullValidData := make([]bool, common.DefaultNb)
		allNullValidData := make([]bool, common.DefaultNb)
		allValidData := make([]bool, common.DefaultNb)
		for i := 0; i < common.DefaultNb; i++ {
			partNullValidData[i] = i%2 == 0
			allNullValidData[i] = false
			allValidData[i] = true
		}
		// [o, nb] -> nb/2 default value
		// [nb, 2*nb] -> all default value
		// [2*nb, 3*nb] -> all valid
		// [3*nb, 4*nb] -> all valid
		for i, data := range [][]bool{partNullValidData, allNullValidData, allValidData} {
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
				WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)).
				WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(data)))
		}
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*3)))

		hp.CollPrepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		exprCounts := []struct {
			expr  string
			count int64
		}{
			{expr: fmt.Sprintf("%s == 'test' and (%d <= %s < %d)", common.DefaultVarcharFieldName, 0, common.DefaultInt64FieldName, common.DefaultNb), count: common.DefaultNb / 2},
			{expr: fmt.Sprintf("%s == 'test' and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb, common.DefaultInt64FieldName, common.DefaultNb*2), count: common.DefaultNb},
			{expr: fmt.Sprintf("%s == 'test' and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb*2, common.DefaultInt64FieldName, common.DefaultNb*3), count: 0},
			{expr: fmt.Sprintf("%s == 'test' and %d <= %s < %d", common.DefaultVarcharFieldName, common.DefaultNb*3, common.DefaultInt64FieldName, common.DefaultNb*4), count: 0},
			{expr: fmt.Sprintf("%s == 'test'", common.DefaultVarcharFieldName), count: common.DefaultNb * 3 / 2},
		}
		for _, exprCount := range exprCounts {
			log.Info("exprCount", zap.String("expr", exprCount.expr), zap.Int64("count", exprCount.count))
			countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(exprCount.expr).WithOutputFields(common.QueryCountFieldName))
			common.CheckErr(t, err, true)
			count, _ := countRes.Fields[0].GetAsInt64(0)
			require.EqualValues(t, exprCount.count, count)
		}
	}
}

// clustering-key nullable
func TestNullableClusteringKey(t *testing.T) {
	// test clustering key nullable
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true).TWithIsClusteringKey(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))

	// insert with valid data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	for i := 0; i < 5; i++ {
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)).
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)))
	}

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	expr := fmt.Sprintf("%s == '1'", common.DefaultVarcharFieldName)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 5, count)
}

// partition-key nullable
func TestDefaultValuePartitionKey(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithDefaultValue("parkey").TWithIsPartitionKey(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong), hp.TWithNullablePartitions(3))

	// insert with valid data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	for i := 0; i < 3; i++ {
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)).
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)))
	}

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultVarcharFieldName: index.NewAutoIndex(entity.COSINE)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	expr := "varchar like 'parkey%'"
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb*3/2, count)
}

func TestNullableGroubBy(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))

	// insert with valid data
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		if i > 200 {
			validData[i] = true
		}
	}
	for i := 0; i < 10; i++ {
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
			WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)).
			WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i)))
	}

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// nullable field as group by field
	queryVec := hp.GenSearchVectors(2, common.DefaultDim, entity.FieldTypeSparseVector)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithGroupByField(common.DefaultVarcharFieldName))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, 2, common.DefaultLimit)
}

func TestNullableSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))

	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
		WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)))

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with nullable expr and output nullable / dynamic field
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)
	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithFilter(expr).WithOutputFields(common.DefaultVarcharFieldName, common.DefaultDynamicFieldName))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	common.CheckOutputFields(t, []string{common.DefaultVarcharFieldName, common.DefaultDynamicFieldName}, searchRes[0].Fields)

	for _, field := range searchRes[0].Fields {
		if field.Name() == common.DefaultVarcharFieldName {
			for i := 0; i < field.Len(); i++ {
				isNull, err := field.IsNull(i)
				common.CheckErr(t, err, true)
				require.True(t, isNull)
			}
		}
	}
}

func TestDefaultValueSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithDefaultValue("test").TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption().TWithEnableDynamicField(true),
		hp.TWithConsistencyLevel(entity.ClStrong))

	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
		WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)))

	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with nullable expr and output nullable / dynamic field
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)
	expr := fmt.Sprintf("%s == 'test'", common.DefaultVarcharFieldName)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithFilter(expr).WithOutputFields(common.DefaultVarcharFieldName, common.DefaultDynamicFieldName))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	common.CheckOutputFields(t, []string{common.DefaultVarcharFieldName, common.DefaultDynamicFieldName}, searchRes[0].Fields)

	for _, field := range searchRes[0].Fields {
		if field.Name() == common.DefaultVarcharFieldName {
			for i := 0; i < field.Len(); i++ {
				fieldData, _ := field.GetAsString(i)
				require.EqualValues(t, "test", fieldData)
			}
		}
	}
}

// test nullable fields in all scalar index
func TestNullableAutoScalarIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create fields: pk + floatVec + all nullable scalar fields
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	schema := entity.NewSchema().WithName(common.GenRandomString("nullable_default_value", 5)).WithField(pkField).WithField(vecField)

	// create collection with all supported nullable fields
	expNullableFields := make([]string, 0)
	for _, fieldType := range hp.GetAllNullableFieldType() {
		nullableField := entity.NewField().WithName(common.GenRandomString("null", 5)).WithDataType(fieldType).WithNullable(true)
		if fieldType == entity.FieldTypeVarChar {
			nullableField.WithMaxLength(common.TestMaxLen)
		}
		if fieldType == entity.FieldTypeArray {
			nullableField.WithElementType(entity.FieldTypeInt64).WithMaxCapacity(common.TestCapacity)
		}
		schema.WithField(nullableField)
		expNullableFields = append(expNullableFields, nullableField.Name)
	}

	// create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(schema.CollectionName, schema).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	// describe collection and check nullable fields
	descCollection, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)
	common.CheckFieldsNullable(t, expNullableFields, descCollection.Schema)

	// insert data with nullable column
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 1
	}
	columnOpt := hp.TNewColumnOptions()
	for _, name := range expNullableFields {
		columnOpt = columnOpt.WithColumnOption(name, hp.TNewDataOption().TWithValidData(validData))
	}
	for i := 0; i < 3; i++ {
		columnOpt = columnOpt.WithColumnOption(common.DefaultInt64FieldName, hp.TNewDataOption().TWithStart(common.DefaultNb*i))
		hp.CollPrepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), columnOpt)
	}
	prepare := hp.CollPrepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create auto scalar index for all nullable fields
	indexOpt := hp.TNewIndexParams(schema)
	for _, name := range expNullableFields {
		indexOpt = indexOpt.TWithFieldIndex(map[string]index.Index{name: index.NewAutoIndex(entity.L2)})
	}
	prepare.CreateIndex(ctx, t, mc, indexOpt)
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query with null expr
	for _, nullField := range expNullableFields {
		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("%s is null", nullField)).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, common.DefaultNb*3/2, count)
	}
}

func TestNullableUpsert(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// query is null return empty result
	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 0, count)

	// upsert with part null, all null, all valid
	partNullValidData := make([]bool, common.DefaultNb)
	allNullValidData := make([]bool, common.DefaultNb)
	allValidData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		partNullValidData[i] = i%2 == 0
		allNullValidData[i] = false
		allValidData[i] = true
	}
	validCount := []int{common.DefaultNb / 2, 0, common.DefaultNb}
	expNullCount := []int{common.DefaultNb / 2, common.DefaultNb, 0}

	pkColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeInt64, *hp.TNewDataOption())
	vecColumn := hp.GenColumnData(common.DefaultNb, entity.FieldTypeSparseVector, *hp.TNewDataOption())

	for i, validData := range [][]bool{partNullValidData, allNullValidData, allValidData} {
		varcharData := make([]string, validCount[i])
		for j := 0; j < validCount[i]; j++ {
			varcharData[j] = "aaa"
		}
		nullVarcharColumn, err := column.NewNullableColumnVarChar(common.DefaultVarcharFieldName, varcharData, validData)
		common.CheckErr(t, err, true)

		upsertRes, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(pkColumn).WithColumns(vecColumn).WithColumns(nullVarcharColumn))
		common.CheckErr(t, err, true)
		require.EqualValues(t, common.DefaultNb, upsertRes.UpsertCount)

		countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, expNullCount[i], count)
	}
}

func TestNullableDelete(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption(),
		hp.TWithConsistencyLevel(entity.ClStrong))
	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions().
		WithColumnOption(common.DefaultVarcharFieldName, hp.TNewDataOption().TWithValidData(validData)))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// delete null
	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)
	deleteRes, err := mc.Delete(ctx, client.NewDeleteOption(schema.CollectionName).WithExpr(expr))
	common.CheckErr(t, err, true)
	require.EqualValues(t, common.DefaultNb/2, deleteRes.DeleteCount)

	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 0, count)
}

// TestNullableRows tests row-based insert with nullable varchar using pointer fields.
func TestNullableRows(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption().TWithEnableDynamicField(false),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	validData := make([]bool, common.DefaultNb)
	for i := 0; i < common.DefaultNb; i++ {
		validData[i] = i%2 == 0
	}
	rows := hp.GenNullableVarcharSparseRows(common.DefaultNb, false, *hp.TNewDataOption().TWithValidData(validData))
	insertRes, err := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, common.DefaultNb, insertRes.InsertCount)

	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, common.DefaultNb/2, count)
}

// TestNullableRowsAllScalarTypes tests row-based insert with all nullable scalar types using pointer fields.
func TestNullableRowsAllScalarTypes(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Create collection with int64 PK + all nullable scalar fields + floatVec
	collName := common.GenRandomString("nullable_scalar_rows", 5)
	pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	boolField := entity.NewField().WithName(common.DefaultBoolFieldName).WithDataType(entity.FieldTypeBool).WithNullable(true)
	int8Field := entity.NewField().WithName(common.DefaultInt8FieldName).WithDataType(entity.FieldTypeInt8).WithNullable(true)
	int16Field := entity.NewField().WithName(common.DefaultInt16FieldName).WithDataType(entity.FieldTypeInt16).WithNullable(true)
	int32Field := entity.NewField().WithName(common.DefaultInt32FieldName).WithDataType(entity.FieldTypeInt32).WithNullable(true)
	floatField := entity.NewField().WithName(common.DefaultFloatFieldName).WithDataType(entity.FieldTypeFloat).WithNullable(true)
	doubleField := entity.NewField().WithName(common.DefaultDoubleFieldName).WithDataType(entity.FieldTypeDouble).WithNullable(true)
	varcharField := entity.NewField().WithName(common.DefaultVarcharFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(common.TestMaxLen).WithNullable(true)
	vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
	schema := entity.NewSchema().WithName(collName).
		WithField(pkField).WithField(boolField).WithField(int8Field).WithField(int16Field).WithField(int32Field).
		WithField(floatField).WithField(doubleField).WithField(varcharField).WithField(vecField)

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	// Create index and load
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, common.DefaultFloatVecFieldName, index.NewAutoIndex(entity.L2)))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Generate rows with half null
	nb := common.DefaultNb
	validData := make([]bool, nb)
	for i := 0; i < nb; i++ {
		validData[i] = i%2 == 0
	}
	rows := hp.GenNullableScalarRows(nb, *hp.TNewDataOption().TWithValidData(validData))
	insertRes, err := mc.Insert(ctx, client.NewRowBasedInsertOption(collName, rows...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, nb, insertRes.InsertCount)

	// Query null count for each nullable field
	nullableFields := []string{
		common.DefaultBoolFieldName, common.DefaultInt8FieldName, common.DefaultInt16FieldName,
		common.DefaultInt32FieldName, common.DefaultFloatFieldName, common.DefaultDoubleFieldName,
		common.DefaultVarcharFieldName,
	}
	for _, fieldName := range nullableFields {
		expr := fmt.Sprintf("%s is null", fieldName)
		countRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
		common.CheckErr(t, err, true)
		count, _ := countRes.Fields[0].GetAsInt64(0)
		require.EqualValues(t, nb/2, count, "unexpected null count for field %s", fieldName)
	}
}

// TestNullableRowsAllNullAndAllValid tests edge cases: all null and all valid rows.
func TestNullableRowsAllNullAndAllValid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption().TWithEnableDynamicField(false),
		hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	nb := common.DefaultNb
	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)

	// Insert all null rows
	allNullData := make([]bool, nb)
	for i := 0; i < nb; i++ {
		allNullData[i] = false
	}
	allNullRows := hp.GenNullableVarcharSparseRows(nb, false, *hp.TNewDataOption().TWithValidData(allNullData))
	insertRes, err := mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, allNullRows...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, nb, insertRes.InsertCount)

	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, nb, count)

	// Insert all valid rows (with start offset to avoid PK conflict)
	allValidData := make([]bool, nb)
	for i := 0; i < nb; i++ {
		allValidData[i] = true
	}
	allValidRows := hp.GenNullableVarcharSparseRows(nb, false, *hp.TNewDataOption().TWithStart(nb).TWithValidData(allValidData))
	insertRes, err = mc.Insert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, allValidRows...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, nb, insertRes.InsertCount)

	// Total null count should still be nb (only the first batch is null)
	countRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ = countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, nb, count)
}

// TestNullableRowsUpsert tests upserting with nullable pointer rows.
func TestNullableRowsUpsert(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fieldsOpt := hp.TNewFieldOptions().WithFieldOption(common.DefaultVarcharFieldName, hp.TNewFieldsOption().TWithNullable(true))
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), fieldsOpt, hp.TNewSchemaOption().TWithEnableDynamicField(false),
		hp.TWithConsistencyLevel(entity.ClStrong))

	// Insert non-null data using column-based insert
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewColumnOptions())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Verify no nulls initially
	expr := fmt.Sprintf("%s is null", common.DefaultVarcharFieldName)
	countRes, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ := countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, 0, count)

	// Upsert with half null using row-based pointer rows
	nb := common.DefaultNb
	validData := make([]bool, nb)
	for i := 0; i < nb; i++ {
		validData[i] = i%2 == 0
	}
	rows := hp.GenNullableVarcharSparseRows(nb, false, *hp.TNewDataOption().TWithValidData(validData))
	upsertRes, err := mc.Upsert(ctx, client.NewRowBasedInsertOption(schema.CollectionName, rows...))
	common.CheckErr(t, err, true)
	require.EqualValues(t, nb, upsertRes.UpsertCount)

	// Verify half are null after upsert
	countRes, err = mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(expr).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, err, true)
	count, _ = countRes.Fields[0].GetAsInt64(0)
	require.EqualValues(t, nb/2, count)
}

func TestNullableVectorAllTypes(t *testing.T) {
	vectorTypes := GetVectorTypes()
	nullPercents := GetNullPercents()

	for _, vt := range vectorTypes {
		for _, nullPercent := range nullPercents {
			testName := fmt.Sprintf("%s_%d%%null", vt.Name, nullPercent)
			t.Run(testName, func(t *testing.T) {
				ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
				mc := hp.CreateDefaultMilvusClient(ctx, t)

				// create collection
				collName := common.GenRandomString("nullable_vec", 5)
				pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
				vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true)
				if vt.FieldType != entity.FieldTypeSparseVector {
					vecField = vecField.WithDim(common.DefaultDim)
				}
				schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)

				err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)

				nb := 500
				validData := make([]bool, nb)
				validCount := 0
				for i := range nb {
					validData[i] = (i % 100) >= nullPercent
					if validData[i] {
						validCount++
					}
				}

				pkData := make([]int64, nb)
				for i := range nb {
					pkData[i] = int64(i)
				}
				pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)

				pkToVecIdx := make(map[int64]int)
				vecIdx := 0
				for i := range nb {
					if validData[i] {
						pkToVecIdx[int64(i)] = vecIdx
						vecIdx++
					}
				}

				var vecColumn column.Column
				var searchVec entity.Vector
				var originalVectors interface{}

				switch vt.FieldType {
				case entity.FieldTypeFloatVector:
					vectors := make([][]float32, validCount)
					for i := range validCount {
						vec := make([]float32, common.DefaultDim)
						for j := range common.DefaultDim {
							vec[j] = float32(i*common.DefaultDim+j) / 10000.0
						}
						vectors[i] = vec
					}
					originalVectors = vectors
					vecColumn, err = column.NewNullableColumnFloatVector("vector", common.DefaultDim, vectors, validData)
					if validCount > 0 {
						searchVec = entity.FloatVector(vectors[0])
					}

				case entity.FieldTypeBinaryVector:
					vectors := make([][]byte, validCount)
					byteDim := common.DefaultDim / 8
					for i := range validCount {
						vec := make([]byte, byteDim)
						for j := range byteDim {
							vec[j] = byte((i + j) % 256)
						}
						vectors[i] = vec
					}
					originalVectors = vectors
					vecColumn, err = column.NewNullableColumnBinaryVector("vector", common.DefaultDim, vectors, validData)
					if validCount > 0 {
						searchVec = entity.BinaryVector(vectors[0])
					}

				case entity.FieldTypeFloat16Vector:
					vectors := make([][]byte, validCount)
					for i := range validCount {
						vectors[i] = common.GenFloat16Vector(common.DefaultDim)
					}
					originalVectors = vectors
					vecColumn, err = column.NewNullableColumnFloat16Vector("vector", common.DefaultDim, vectors, validData)
					if validCount > 0 {
						searchVec = entity.Float16Vector(vectors[0])
					}

				case entity.FieldTypeBFloat16Vector:
					vectors := make([][]byte, validCount)
					for i := range validCount {
						vectors[i] = common.GenBFloat16Vector(common.DefaultDim)
					}
					originalVectors = vectors
					vecColumn, err = column.NewNullableColumnBFloat16Vector("vector", common.DefaultDim, vectors, validData)
					if validCount > 0 {
						searchVec = entity.BFloat16Vector(vectors[0])
					}

				case entity.FieldTypeInt8Vector:
					vectors := make([][]int8, validCount)
					for i := range validCount {
						vec := make([]int8, common.DefaultDim)
						for j := range common.DefaultDim {
							vec[j] = int8((i + j) % 127)
						}
						vectors[i] = vec
					}
					originalVectors = vectors
					vecColumn, err = column.NewNullableColumnInt8Vector("vector", common.DefaultDim, vectors, validData)
					if validCount > 0 {
						searchVec = entity.Int8Vector(vectors[0])
					}

				case entity.FieldTypeSparseVector:
					vectors := make([]entity.SparseEmbedding, validCount)
					for i := range validCount {
						positions := []uint32{0, uint32(i + 1), uint32(i + 1000)}
						values := []float32{1.0, float32(i+1) / 1000.0, 0.1}
						vectors[i], err = entity.NewSliceSparseEmbedding(positions, values)
						common.CheckErr(t, err, true)
					}
					originalVectors = vectors
					vecColumn, err = column.NewNullableColumnSparseFloatVector("vector", vectors, validData)
					if validCount > 0 {
						searchVec = vectors[0]
					}
				}
				common.CheckErr(t, err, true)
				_ = originalVectors

				insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, vecColumn))
				common.CheckErr(t, err, true)
				require.EqualValues(t, nb, insertRes.InsertCount)

				flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
				common.CheckErr(t, err, true)
				err = flushTask.Await(ctx)
				common.CheckErr(t, err, true)

				if validCount > 0 {
					var vecIndex index.Index
					switch vt.FieldType {
					case entity.FieldTypeBinaryVector:
						vecIndex = index.NewGenericIndex("vector", map[string]string{
							index.MetricTypeKey: string(entity.JACCARD),
							index.IndexTypeKey:  "BIN_FLAT",
						})
					case entity.FieldTypeInt8Vector:
						vecIndex = index.NewGenericIndex("vector", map[string]string{
							index.MetricTypeKey: string(entity.COSINE),
							index.IndexTypeKey:  "HNSW",
							"M":                 "16",
							"efConstruction":    "200",
						})
					case entity.FieldTypeSparseVector:
						vecIndex = index.NewSparseInvertedIndex(entity.IP, 0.1)
					default:
						vecIndex = index.NewFlatIndex(entity.L2)
					}
					indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
					common.CheckErr(t, err, true)
					err = indexTask.Await(ctx)
					common.CheckErr(t, err, true)

					loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
					common.CheckErr(t, err, true)
					err = loadTask.Await(ctx)
					common.CheckErr(t, err, true)

					searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{searchVec}).WithANNSField("vector"))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 1, len(searchRes))
					searchIDs := searchRes[0].IDs.(*column.ColumnInt64).Data()
					require.True(t, len(searchIDs) > 0, "search should return results")

					expectedTopK := 10
					if validCount < expectedTopK {
						expectedTopK = validCount
					}
					require.EqualValues(t, expectedTopK, len(searchIDs), "search should return expected number of results")

					for _, id := range searchIDs {
						require.True(t, id >= 0 && id < int64(nb), "search result ID %d should be in range [0, %d)", id, nb)
					}

					verifyVectorData := func(queryResult client.ResultSet, context string) {
						pkCol := queryResult.GetColumn(common.DefaultInt64FieldName).(*column.ColumnInt64)
						vecCol := queryResult.GetColumn("vector")
						for i := 0; i < queryResult.ResultCount; i++ {
							pk, _ := pkCol.GetAsInt64(i)
							isNull, _ := vecCol.IsNull(i)

							if origIdx, ok := pkToVecIdx[pk]; ok {
								require.False(t, isNull, "%s: vector should not be null for pk %d", context, pk)
								vecData, _ := vecCol.Get(i)

								switch vt.FieldType {
								case entity.FieldTypeFloatVector:
									vectors := originalVectors.([][]float32)
									queriedVec := []float32(vecData.(entity.FloatVector))
									require.EqualValues(t, common.DefaultDim, len(queriedVec), "%s: vector dimension should match for pk %d", context, pk)
									origVec := vectors[origIdx]
									for j := range origVec {
										require.InDelta(t, origVec[j], queriedVec[j], 1e-6, "%s: vector element %d should match for pk %d", context, j, pk)
									}
								case entity.FieldTypeInt8Vector:
									vectors := originalVectors.([][]int8)
									queriedVec := []int8(vecData.(entity.Int8Vector))
									require.EqualValues(t, common.DefaultDim, len(queriedVec), "%s: vector dimension should match for pk %d", context, pk)
									origVec := vectors[origIdx]
									for j := range origVec {
										require.EqualValues(t, origVec[j], queriedVec[j], "%s: vector element %d should match for pk %d", context, j, pk)
									}
								case entity.FieldTypeBinaryVector:
									vectors := originalVectors.([][]byte)
									queriedVec := []byte(vecData.(entity.BinaryVector))
									byteDim := common.DefaultDim / 8
									require.EqualValues(t, byteDim, len(queriedVec), "%s: vector byte dimension should match for pk %d", context, pk)
									origVec := vectors[origIdx]
									for j := range origVec {
										require.EqualValues(t, origVec[j], queriedVec[j], "%s: vector byte %d should match for pk %d", context, j, pk)
									}
								case entity.FieldTypeFloat16Vector:
									queriedVec := []byte(vecData.(entity.Float16Vector))
									byteDim := common.DefaultDim * 2
									require.EqualValues(t, byteDim, len(queriedVec), "%s: vector byte dimension should match for pk %d", context, pk)
								case entity.FieldTypeBFloat16Vector:
									queriedVec := []byte(vecData.(entity.BFloat16Vector))
									byteDim := common.DefaultDim * 2
									require.EqualValues(t, byteDim, len(queriedVec), "%s: vector byte dimension should match for pk %d", context, pk)
								case entity.FieldTypeSparseVector:
									vectors := originalVectors.([]entity.SparseEmbedding)
									queriedVec := vecData.(entity.SparseEmbedding)
									origVec := vectors[origIdx]
									require.EqualValues(t, origVec.Len(), queriedVec.Len(), "%s: sparse vector length should match for pk %d", context, pk)
									for j := 0; j < origVec.Len(); j++ {
										origPos, origVal, _ := origVec.Get(j)
										queriedPos, queriedVal, _ := queriedVec.Get(j)
										require.EqualValues(t, origPos, queriedPos, "%s: sparse vector position %d should match for pk %d", context, j, pk)
										require.InDelta(t, origVal, queriedVal, 1e-6, "%s: sparse vector value %d should match for pk %d", context, j, pk)
									}
								}
							} else {
								require.True(t, isNull, "%s: vector should be null for pk %d", context, pk)
								vecData, _ := vecCol.Get(i)
								require.Nil(t, vecData, "%s: null vector data should be nil for pk %d", context, pk)
							}
						}
					}

					if len(searchIDs) > 0 {
						searchQueryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("int64 in [%s]", int64SliceToString(searchIDs))).WithOutputFields("vector"))
						common.CheckErr(t, err, true)
						require.EqualValues(t, len(searchIDs), searchQueryRes.ResultCount, "query by search IDs should return all IDs")
						verifyVectorData(searchQueryRes, "Search results")
					}

					queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 < 10").WithOutputFields("vector"))
					common.CheckErr(t, err, true)
					expectedQueryCount := 10
					require.EqualValues(t, expectedQueryCount, queryRes.ResultCount, "query should return expected count")
					verifyVectorData(queryRes, "Query int64 < 10")

					searchRes, err = mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{searchVec}).
						WithANNSField("vector").WithFilter("int64 < 100").WithOutputFields("vector"))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 1, len(searchRes))
					filteredIDs := searchRes[0].IDs.(*column.ColumnInt64).Data()
					for _, id := range filteredIDs {
						require.True(t, id < 100, "filtered search should only return IDs < 100, got %d", id)
					}
					hybridSearchQueryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("int64 in [%s]", int64SliceToString(filteredIDs))).WithOutputFields("vector"))
					common.CheckErr(t, err, true)
					verifyVectorData(hybridSearchQueryRes, "Hybrid search results")

					countRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("").WithOutputFields("count(*)"))
					common.CheckErr(t, err, true)
					totalCount, err := countRes.Fields[0].GetAsInt64(0)
					common.CheckErr(t, err, true)
					require.EqualValues(t, nb, totalCount, "total count should equal inserted rows")
				}

				err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
				common.CheckErr(t, err, true)
			})
		}
	}
}

func TestNullableVectorWithScalarFilter(t *testing.T) {
	vectorTypes := GetVectorTypes()
	nullPercents := GetNullPercents()

	for _, vt := range vectorTypes {
		for _, nullPercent := range nullPercents {
			testName := fmt.Sprintf("%s_%d%%null", vt.Name, nullPercent)
			t.Run(testName, func(t *testing.T) {
				ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
				mc := hp.CreateDefaultMilvusClient(ctx, t)

				collName := common.GenRandomString("nullable_vec_filter", 5)
				pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
				vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true)
				if vt.FieldType != entity.FieldTypeSparseVector {
					vecField = vecField.WithDim(common.DefaultDim)
				}
				tagField := entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100).WithNullable(true)
				schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithField(tagField)

				err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)

				nb := 500
				testData := GenerateNullableVectorTestData(t, vt, nb, nullPercent, "vector")

				// tag field: 50% null (even rows are valid)
				tagValidData := make([]bool, nb)
				tagValidCount := 0
				for i := range nb {
					tagValidData[i] = i%2 == 0
					if tagValidData[i] {
						tagValidCount++
					}
				}

				pkData := make([]int64, nb)
				for i := range nb {
					pkData[i] = int64(i)
				}
				pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)

				tagData := make([]string, tagValidCount)
				for i := range tagValidCount {
					tagData[i] = fmt.Sprintf("tag_%d", i)
				}
				tagColumn, err := column.NewNullableColumnVarChar("tag", tagData, tagValidData)
				common.CheckErr(t, err, true)

				insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, testData.VecColumn, tagColumn))
				common.CheckErr(t, err, true)
				require.EqualValues(t, nb, insertRes.InsertCount)

				flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
				common.CheckErr(t, err, true)
				err = flushTask.Await(ctx)
				common.CheckErr(t, err, true)

				if testData.ValidCount > 0 {
					vecIndex := CreateNullableVectorIndex(vt)
					indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
					common.CheckErr(t, err, true)
					err = indexTask.Await(ctx)
					common.CheckErr(t, err, true)

					loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
					common.CheckErr(t, err, true)
					err = loadTask.Await(ctx)
					common.CheckErr(t, err, true)

					// Query with scalar filter: tag is not null and int64 < 50
					// int64 < 50 => 50 rows (pk 0-49)
					// tag is not null => even rows only (pk 0, 2, 4, ..., 48) => 25 rows
					queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("tag is not null and int64 < 50").WithOutputFields("vector", "tag"))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 25, queryRes.ResultCount, "query should return 25 rows with tag not null and int64 < 50")
					VerifyNullableVectorData(t, vt, queryRes, testData.PkToVecIdx, testData.OriginalVectors, "Query with tag filter")
				}

				// clean up
				err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
				common.CheckErr(t, err, true)
			})
		}
	}
}

func TestNullableVectorDelete(t *testing.T) {
	vectorTypes := GetVectorTypes()
	nullPercents := GetNullPercents()

	for _, vt := range vectorTypes {
		for _, nullPercent := range nullPercents {
			testName := fmt.Sprintf("%s_%d%%null", vt.Name, nullPercent)
			t.Run(testName, func(t *testing.T) {
				ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
				mc := hp.CreateDefaultMilvusClient(ctx, t)

				collName := common.GenRandomString("nullable_vec_del", 5)
				pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
				vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true)
				if vt.FieldType != entity.FieldTypeSparseVector {
					vecField = vecField.WithDim(common.DefaultDim)
				}
				schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)

				err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)

				nb := 100
				testData := GenerateNullableVectorTestData(t, vt, nb, nullPercent, "vector")

				pkData := make([]int64, nb)
				for i := range nb {
					pkData[i] = int64(i)
				}
				pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)

				insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, testData.VecColumn))
				common.CheckErr(t, err, true)
				require.EqualValues(t, nb, insertRes.InsertCount)

				flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
				common.CheckErr(t, err, true)
				err = flushTask.Await(ctx)
				common.CheckErr(t, err, true)

				if testData.ValidCount > 0 {
					vecIndex := CreateNullableVectorIndex(vt)
					indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
					common.CheckErr(t, err, true)
					err = indexTask.Await(ctx)
					common.CheckErr(t, err, true)

					loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
					common.CheckErr(t, err, true)
					err = loadTask.Await(ctx)
					common.CheckErr(t, err, true)

					// Delete first 25 rows and last 25 rows
					delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr("int64 < 25"))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 25, delRes.DeleteCount)

					delRes, err = mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr("int64 >= 75"))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 25, delRes.DeleteCount)

					// Verify remaining count
					queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("").WithOutputFields("count(*)"))
					common.CheckErr(t, err, true)
					count, err := queryRes.Fields[0].GetAsInt64(0)
					common.CheckErr(t, err, true)
					require.EqualValues(t, 50, count, "remaining count should be 100 - 25 - 25 = 50")

					// Verify deleted rows don't exist
					queryDeletedRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 < 25").WithOutputFields("count(*)"))
					common.CheckErr(t, err, true)
					deletedCount, err := queryDeletedRes.Fields[0].GetAsInt64(0)
					common.CheckErr(t, err, true)
					require.EqualValues(t, 0, deletedCount, "deleted rows should not exist")

					queryDeletedValidRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 >= 75").WithOutputFields("count(*)"))
					common.CheckErr(t, err, true)
					deletedValidCount, err := queryDeletedValidRes.Fields[0].GetAsInt64(0)
					common.CheckErr(t, err, true)
					require.EqualValues(t, 0, deletedValidCount, "deleted valid vector rows should not exist")

					// Verify remaining rows with vector data
					queryValidRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 >= 25 and int64 < 75").WithOutputFields("vector"))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 50, queryValidRes.ResultCount, "should have 50 remaining rows")
					VerifyNullableVectorData(t, vt, queryValidRes, testData.PkToVecIdx, testData.OriginalVectors, "Remaining vector rows")
				}

				// clean up
				err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
				common.CheckErr(t, err, true)
			})
		}
	}
}

func TestNullableVectorUpsert(t *testing.T) {
	autoIDOptions := []bool{false, true}

	for _, autoID := range autoIDOptions {
		testName := fmt.Sprintf("AutoID=%v", autoID)
		t.Run(testName, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := hp.CreateDefaultMilvusClient(ctx, t)

			// Create collection with pk, scalar, and nullable vector fields
			collName := common.GenRandomString("nullable_vec_upsert", 5)
			pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
			if autoID {
				pkField.AutoID = true
			}
			scalarField := entity.NewField().WithName("scalar").WithDataType(entity.FieldTypeInt32).WithNullable(true)
			vecField := entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim).WithNullable(true)
			schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(scalarField).WithField(vecField)

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)

			// Insert initial data: 100 rows
			// Rows 0 to nullPercent-1: valid vector, scalar = i*10
			// Rows nullPercent to nb-1: null vector, scalar = i*10
			nb := 100
			nullPercent := 50
			validData := make([]bool, nb)
			validCount := 0
			for i := range nb {
				validData[i] = i < nullPercent
				if validData[i] {
					validCount++
				}
			}

			pkData := make([]int64, nb)
			scalarData := make([]int32, nb)
			scalarValidData := make([]bool, nb)
			for i := range nb {
				pkData[i] = int64(i)
				scalarData[i] = int32(i * 10)
				scalarValidData[i] = true
			}
			pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)
			scalarColumn, err := column.NewNullableColumnInt32("scalar", scalarData, scalarValidData)
			common.CheckErr(t, err, true)

			vectors := make([][]float32, validCount)
			for i := range validCount {
				vec := make([]float32, common.DefaultDim)
				for j := range common.DefaultDim {
					vec[j] = float32(i*common.DefaultDim+j) / 10000.0
				}
				vectors[i] = vec
			}
			vecColumn, err := column.NewNullableColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, vectors, validData)
			common.CheckErr(t, err, true)

			var insertRes client.InsertResult
			if autoID {
				insertRes, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(scalarColumn, vecColumn))
			} else {
				insertRes, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, scalarColumn, vecColumn))
			}
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, insertRes.InsertCount)

			var actualPkData []int64
			if autoID {
				insertedIDs := insertRes.IDs.(*column.ColumnInt64)
				actualPkData = insertedIDs.Data()
				require.EqualValues(t, nb, len(actualPkData), "inserted PK count should match")
			} else {
				actualPkData = pkData
			}

			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, common.DefaultFloatVecFieldName, index.NewFlatIndex(entity.L2)))
			common.CheckErr(t, err, true)
			err = indexTask.Await(ctx)
			common.CheckErr(t, err, true)

			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			// Track expected state
			expectedVectorMap := make(map[int64][]float32)
			expectedScalarMap := make(map[int64]int32)
			for i := range nb {
				expectedScalarMap[actualPkData[i]] = scalarData[i]
				if i < nullPercent {
					expectedVectorMap[actualPkData[i]] = vectors[i]
				} else {
					expectedVectorMap[actualPkData[i]] = nil
				}
			}

			// Helper: flush, reload, search and query verify
			flushAndVerify := func(expectedValidCount int, context string) {
				time.Sleep(10 * time.Second)
				flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
				common.CheckErr(t, err, true)
				err = flushTask.Await(ctx)
				common.CheckErr(t, err, true)

				err = mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(collName))
				common.CheckErr(t, err, true)

				loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
				common.CheckErr(t, err, true)
				err = loadTask.Await(ctx)
				common.CheckErr(t, err, true)

				// Search verify
				searchVec := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
				searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 100, []entity.Vector{searchVec}).WithANNSField(common.DefaultFloatVecFieldName))
				common.CheckErr(t, err, true)
				require.EqualValues(t, 1, len(searchRes))
				require.EqualValues(t, expectedValidCount, searchRes[0].ResultCount, "%s: search should return %d valid vectors", context, expectedValidCount)

				// Query verify all rows
				queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("int64 in [%s]", int64SliceToString(actualPkData))).WithOutputFields("scalar", common.DefaultFloatVecFieldName))
				common.CheckErr(t, err, true)
				require.EqualValues(t, nb, queryRes.ResultCount, "%s: query should return %d rows", context, nb)

				pkCol := queryRes.GetColumn(common.DefaultInt64FieldName).(*column.ColumnInt64)
				scalarCol := queryRes.GetColumn("scalar").(*column.ColumnInt32)
				vecCol := queryRes.GetColumn(common.DefaultFloatVecFieldName).(*column.ColumnFloatVector)

				for i := 0; i < queryRes.ResultCount; i++ {
					pk, _ := pkCol.GetAsInt64(i)
					scalarVal, _ := scalarCol.GetAsInt64(i)
					isNull, _ := vecCol.IsNull(i)

					expectedScalar := expectedScalarMap[pk]
					require.EqualValues(t, expectedScalar, scalarVal, "%s: scalar mismatch for pk %d", context, pk)

					expectedVec := expectedVectorMap[pk]
					if expectedVec != nil {
						require.False(t, isNull, "%s: vector should not be null for pk %d", context, pk)
						vecData, _ := vecCol.Get(i)
						queriedVec := []float32(vecData.(entity.FloatVector))
						for j := range expectedVec {
							require.InDelta(t, expectedVec[j], queriedVec[j], 1e-6, "%s: vector element %d mismatch for pk %d", context, j, pk)
						}
					} else {
						require.True(t, isNull, "%s: vector should be null for pk %d", context, pk)
					}
				}
			}

			// Upsert 1: Update all 100 rows to null vectors
			upsert1PkData := make([]int64, nb)
			for i := range nb {
				upsert1PkData[i] = actualPkData[i]
			}
			upsert1PkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, upsert1PkData)

			allNullValidData := make([]bool, nb)
			upsert1VecColumn, err := column.NewNullableColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, [][]float32{}, allNullValidData)
			common.CheckErr(t, err, true)

			upsert1ScalarData := make([]int32, nb)
			upsert1ScalarValidData := make([]bool, nb)
			for i := range nb {
				upsert1ScalarData[i] = int32(i * 100)
				upsert1ScalarValidData[i] = true
			}
			upsert1ScalarColumn, err := column.NewNullableColumnInt32("scalar", upsert1ScalarData, upsert1ScalarValidData)
			common.CheckErr(t, err, true)

			upsertRes1, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName, upsert1PkColumn, upsert1ScalarColumn, upsert1VecColumn))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, upsertRes1.UpsertCount)

			// For AutoID=true, upsert returns new IDs, need to update actualPkData
			if autoID {
				upsertedIDs := upsertRes1.IDs.(*column.ColumnInt64)
				newPkData := upsertedIDs.Data()
				// Clear old expected state
				expectedVectorMap = make(map[int64][]float32)
				expectedScalarMap = make(map[int64]int32)
				// Update actualPkData with new IDs
				actualPkData = newPkData
			}

			// Update expected state: all vectors null
			for i := range nb {
				expectedVectorMap[actualPkData[i]] = nil
				expectedScalarMap[actualPkData[i]] = upsert1ScalarData[i]
			}

			// Verify after upsert 1: search should return 0 (all null)
			flushAndVerify(0, "After Upsert1-AllNull")

			// Upsert 2: Update rows nullPercent to nb-1 to valid vectors
			upsert2Nb := nb - nullPercent
			upsert2PkData := make([]int64, upsert2Nb)
			for i := range upsert2Nb {
				upsert2PkData[i] = actualPkData[i+nullPercent]
			}
			upsert2PkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, upsert2PkData)

			upsert2Vectors := make([][]float32, upsert2Nb)
			upsert2ValidData := make([]bool, upsert2Nb)
			for i := range upsert2Nb {
				vec := make([]float32, common.DefaultDim)
				for j := range common.DefaultDim {
					vec[j] = float32((i+500)*common.DefaultDim+j) / 10000.0
				}
				upsert2Vectors[i] = vec
				upsert2ValidData[i] = true
			}
			upsert2VecColumn, err := column.NewNullableColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, upsert2Vectors, upsert2ValidData)
			common.CheckErr(t, err, true)

			upsert2ScalarData := make([]int32, upsert2Nb)
			upsert2ScalarValidData := make([]bool, upsert2Nb)
			for i := range upsert2Nb {
				upsert2ScalarData[i] = int32((i + nullPercent) * 200)
				upsert2ScalarValidData[i] = true
			}
			upsert2ScalarColumn, err := column.NewNullableColumnInt32("scalar", upsert2ScalarData, upsert2ScalarValidData)
			common.CheckErr(t, err, true)

			upsertRes2, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName, upsert2PkColumn, upsert2ScalarColumn, upsert2VecColumn))
			common.CheckErr(t, err, true)
			require.EqualValues(t, upsert2Nb, upsertRes2.UpsertCount)

			// For AutoID=true, upsert returns new IDs for the upserted rows
			if autoID {
				upsertedIDs := upsertRes2.IDs.(*column.ColumnInt64)
				newPkData := upsertedIDs.Data()
				// Update actualPkData for rows nullPercent to nb-1
				for i := range upsert2Nb {
					// Remove old expected state
					delete(expectedVectorMap, actualPkData[i+nullPercent])
					delete(expectedScalarMap, actualPkData[i+nullPercent])
					// Update to new PK
					actualPkData[i+nullPercent] = newPkData[i]
				}
			}

			// Update expected state: rows nullPercent to nb-1 now have valid vectors
			for i := range upsert2Nb {
				expectedVectorMap[actualPkData[i+nullPercent]] = upsert2Vectors[i]
				expectedScalarMap[actualPkData[i+nullPercent]] = upsert2ScalarData[i]
			}

			// Verify after upsert 2: search should return upsert2Nb (rows nullPercent to nb-1 valid)
			flushAndVerify(upsert2Nb, "After Upsert2-NullToValid")

			// Upsert 3: Partial update rows 0 to nullPercent-1 (only scalar), vector preserved (still null)
			upsert3Nb := nullPercent
			upsert3PkData := make([]int64, upsert3Nb)
			upsert3ScalarData := make([]int32, upsert3Nb)
			upsert3ScalarValidData := make([]bool, upsert3Nb)
			for i := range upsert3Nb {
				upsert3PkData[i] = actualPkData[i]
				upsert3ScalarData[i] = int32(i * 1000)
				upsert3ScalarValidData[i] = true
			}
			upsert3PkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, upsert3PkData)
			upsert3ScalarColumn, err := column.NewNullableColumnInt32("scalar", upsert3ScalarData, upsert3ScalarValidData)
			common.CheckErr(t, err, true)

			upsertRes3, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName, upsert3PkColumn, upsert3ScalarColumn).WithPartialUpdate(true))
			common.CheckErr(t, err, true)
			require.EqualValues(t, upsert3Nb, upsertRes3.UpsertCount)

			// For AutoID=true, upsert returns new IDs for the upserted rows
			if autoID {
				upsertedIDs := upsertRes3.IDs.(*column.ColumnInt64)
				newPkData := upsertedIDs.Data()
				// Update actualPkData for rows 0 to nullPercent-1
				for i := range upsert3Nb {
					// Remove old expected state
					delete(expectedVectorMap, actualPkData[i])
					delete(expectedScalarMap, actualPkData[i])
					// Update to new PK
					actualPkData[i] = newPkData[i]
				}
			}

			// Update expected state: rows 0 to nullPercent-1 scalar updated, vector preserved (null)
			for i := range upsert3Nb {
				expectedScalarMap[actualPkData[i]] = upsert3ScalarData[i]
				// Vector remains null (preserved from before)
				expectedVectorMap[actualPkData[i]] = nil
			}

			// Verify after upsert 3: search should still return upsert2Nb (only rows nullPercent to nb-1 valid)
			flushAndVerify(upsert2Nb, "After Upsert3-PartialUpdate")

			// clean up
			err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
			common.CheckErr(t, err, true)
		})
	}
}

func TestNullableVectorAllNull(t *testing.T) {
	vectorTypes := GetVectorTypes()

	for _, vt := range vectorTypes {
		testName := fmt.Sprintf("%s_100%%null", vt.Name)
		t.Run(testName, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := hp.CreateDefaultMilvusClient(ctx, t)

			collName := common.GenRandomString("nullable_vec_all", 5)
			pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
			vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true)
			if vt.FieldType != entity.FieldTypeSparseVector {
				vecField = vecField.WithDim(common.DefaultDim)
			}
			schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)

			// Generate test data with 100% null (nullPercent = 100)
			nb := 10000
			testData := GenerateNullableVectorTestData(t, vt, nb, 100, "vector")

			pkData := make([]int64, nb)
			for i := range nb {
				pkData[i] = int64(i)
			}
			pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)

			// insert
			insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, testData.VecColumn))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, insertRes.InsertCount)

			// flush
			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			// create index (use IVF for dense vectors, requires training)
			var vecIndex index.Index
			switch vt.FieldType {
			case entity.FieldTypeBinaryVector:
				vecIndex = index.NewGenericIndex("vector", map[string]string{
					index.MetricTypeKey: string(entity.JACCARD),
					index.IndexTypeKey:  "BIN_IVF_FLAT",
					"nlist":             "128",
				})
			case entity.FieldTypeSparseVector:
				vecIndex = index.NewGenericIndex("vector", map[string]string{
					index.MetricTypeKey: string(entity.IP),
					index.IndexTypeKey:  "SPARSE_INVERTED_INDEX",
				})
			case entity.FieldTypeInt8Vector:
				vecIndex = index.NewGenericIndex("vector", map[string]string{
					index.MetricTypeKey: string(entity.COSINE),
					index.IndexTypeKey:  "HNSW",
					"M":                 "16",
					"efConstruction":    "200",
				})
			default:
				vecIndex = index.NewGenericIndex("vector", map[string]string{
					index.MetricTypeKey: string(entity.L2),
					index.IndexTypeKey:  "IVF_FLAT",
					"nlist":             "128",
				})
			}
			indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
			common.CheckErr(t, err, true)
			err = indexTask.Await(ctx)
			common.CheckErr(t, err, true)

			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			// Generate a search vector (won't match anything since all are null)
			var searchVec entity.Vector
			switch vt.FieldType {
			case entity.FieldTypeFloatVector:
				searchVec = entity.FloatVector(common.GenFloatVector(common.DefaultDim))
			case entity.FieldTypeBinaryVector:
				searchVec = entity.BinaryVector(make([]byte, common.DefaultDim/8))
			case entity.FieldTypeFloat16Vector:
				searchVec = entity.Float16Vector(common.GenFloat16Vector(common.DefaultDim))
			case entity.FieldTypeBFloat16Vector:
				searchVec = entity.BFloat16Vector(common.GenBFloat16Vector(common.DefaultDim))
			case entity.FieldTypeInt8Vector:
				vec := make([]int8, common.DefaultDim)
				searchVec = entity.Int8Vector(vec)
			case entity.FieldTypeSparseVector:
				searchVec, _ = entity.NewSliceSparseEmbedding([]uint32{0}, []float32{1.0})
			}

			// search should return empty results since all vectors are null (not searchable)
			searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{searchVec}).WithANNSField("vector"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 1, len(searchRes))
			searchIDs := searchRes[0].IDs.(*column.ColumnInt64).Data()
			require.EqualValues(t, 0, len(searchIDs), "search should return empty results for all-null vectors")

			// query should return all rows
			queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("").WithOutputFields("count(*)"))
			common.CheckErr(t, err, true)
			count, err := queryRes.Fields[0].GetAsInt64(0)
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, count, "query should return all %d rows even with 100%% null vectors", nb)

			// clean up
			err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
			common.CheckErr(t, err, true)
		})
	}
}

func TestNullableVectorMultiFields(t *testing.T) {
	vectorTypes := GetVectorTypes()

	for _, vt := range vectorTypes {
		testName := vt.Name
		t.Run(testName, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := hp.CreateDefaultMilvusClient(ctx, t)

			collName := common.GenRandomString("nullable_vec_multi", 5)
			pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
			vecField1 := entity.NewField().WithName("vec1").WithDataType(vt.FieldType).WithNullable(true)
			vecField2 := entity.NewField().WithName("vec2").WithDataType(vt.FieldType).WithNullable(true)
			if vt.FieldType != entity.FieldTypeSparseVector {
				vecField1 = vecField1.WithDim(common.DefaultDim)
				vecField2 = vecField2.WithDim(common.DefaultDim)
			}
			schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField1).WithField(vecField2)

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)

			// generate data: vec1 has 70% valid (first 30 per 100 are invalid), vec2 has 30% valid (first 70 per 100 are invalid)
			nb := 100
			nullPercent1 := 30 // vec1: 30% null
			nullPercent2 := 70 // vec2: 70% null

			// Generate test data for both vector fields
			testData1 := GenerateNullableVectorTestData(t, vt, nb, nullPercent1, "vec1")
			testData2 := GenerateNullableVectorTestData(t, vt, nb, nullPercent2, "vec2")

			pkData := make([]int64, nb)
			for i := range nb {
				pkData[i] = int64(i)
			}
			pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)

			// insert
			insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, testData1.VecColumn, testData2.VecColumn))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, insertRes.InsertCount)

			// flush
			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			// create indexes for both vector fields
			vecIndex := CreateNullableVectorIndex(vt)
			indexTask1, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vec1", vecIndex))
			common.CheckErr(t, err, true)
			err = indexTask1.Await(ctx)
			common.CheckErr(t, err, true)

			indexTask2, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vec2", vecIndex))
			common.CheckErr(t, err, true)
			err = indexTask2.Await(ctx)
			common.CheckErr(t, err, true)

			// load
			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			// search on vec1
			searchRes1, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{testData1.SearchVec}).WithANNSField("vec1").WithOutputFields("vec1"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 1, len(searchRes1))
			searchIDs1 := searchRes1[0].IDs.(*column.ColumnInt64).Data()
			require.EqualValues(t, 10, len(searchIDs1), "search on vec1 should return 10 results")
			for _, id := range searchIDs1 {
				_, ok := testData1.PkToVecIdx[id]
				require.True(t, ok, "search on vec1 should only return rows where vec1 is valid, got pk %d", id)
			}

			// search on vec2
			searchRes2, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{testData2.SearchVec}).WithANNSField("vec2").WithOutputFields("vec2"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 1, len(searchRes2))
			searchIDs2 := searchRes2[0].IDs.(*column.ColumnInt64).Data()
			require.EqualValues(t, 10, len(searchIDs2), "search on vec2 should return 10 results")
			for _, id := range searchIDs2 {
				_, ok := testData2.PkToVecIdx[id]
				require.True(t, ok, "search on vec2 should only return rows where vec2 is valid, got pk %d", id)
			}

			// query and verify - rows 0-29 both null
			queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 < 30").WithOutputFields("vec1", "vec2"))
			common.CheckErr(t, err, true)
			VerifyNullableVectorDataWithFieldName(t, vt, queryRes, testData1.PkToVecIdx, testData1.OriginalVectors, "vec1", "query0-29 vec1")
			VerifyNullableVectorDataWithFieldName(t, vt, queryRes, testData2.PkToVecIdx, testData2.OriginalVectors, "vec2", "query0-29 vec2")

			// query rows 30-69: vec1 valid, vec2 null
			queryMixedRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 >= 30 AND int64 < 70").WithOutputFields("vec1", "vec2"))
			common.CheckErr(t, err, true)
			VerifyNullableVectorDataWithFieldName(t, vt, queryMixedRes, testData1.PkToVecIdx, testData1.OriginalVectors, "vec1", "query30-69 vec1")
			VerifyNullableVectorDataWithFieldName(t, vt, queryMixedRes, testData2.PkToVecIdx, testData2.OriginalVectors, "vec2", "query30-69 vec2")

			// query rows 70-99: both valid
			queryBothValidRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 >= 70").WithOutputFields("vec1", "vec2"))
			common.CheckErr(t, err, true)
			VerifyNullableVectorDataWithFieldName(t, vt, queryBothValidRes, testData1.PkToVecIdx, testData1.OriginalVectors, "vec1", "query70-99 vec1")
			VerifyNullableVectorDataWithFieldName(t, vt, queryBothValidRes, testData2.PkToVecIdx, testData2.OriginalVectors, "vec2", "query70-99 vec2")

			// clean up
			err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
			common.CheckErr(t, err, true)
		})
	}
}

func TestNullableVectorPaginatedQuery(t *testing.T) {
	vectorTypes := GetVectorTypes()
	nullPercents := GetNullPercents()

	for _, vt := range vectorTypes {
		for _, nullPercent := range nullPercents {
			testName := fmt.Sprintf("%s_%d%%null", vt.Name, nullPercent)
			t.Run(testName, func(t *testing.T) {
				ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
				mc := hp.CreateDefaultMilvusClient(ctx, t)

				collName := common.GenRandomString("nullable_vec_page", 5)
				pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
				vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true)
				if vt.FieldType != entity.FieldTypeSparseVector {
					vecField = vecField.WithDim(common.DefaultDim)
				}
				schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)

				err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)

				nb := 200
				testData := GenerateNullableVectorTestData(t, vt, nb, nullPercent, "vector")

				pkData := make([]int64, nb)
				for i := range nb {
					pkData[i] = int64(i)
				}
				pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)

				// insert
				insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, testData.VecColumn))
				common.CheckErr(t, err, true)
				require.EqualValues(t, nb, insertRes.InsertCount)

				flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
				common.CheckErr(t, err, true)
				err = flushTask.Await(ctx)
				common.CheckErr(t, err, true)

				if testData.ValidCount > 0 {
					// create index and load
					vecIndex := CreateNullableVectorIndex(vt)
					indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
					common.CheckErr(t, err, true)
					err = indexTask.Await(ctx)
					common.CheckErr(t, err, true)

					loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
					common.CheckErr(t, err, true)
					err = loadTask.Await(ctx)
					common.CheckErr(t, err, true)

					// Test pagination: page1 offset=0, limit=50
					page1Res, err := mc.Query(ctx, client.NewQueryOption(collName).
						WithFilter("").WithOutputFields("vector").WithOffset(0).WithLimit(50))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 50, page1Res.ResultCount, "page 1 should return 50 rows")
					VerifyNullableVectorData(t, vt, page1Res, testData.PkToVecIdx, testData.OriginalVectors, "page1")

					// Test pagination: page2 offset=50, limit=50
					page2Res, err := mc.Query(ctx, client.NewQueryOption(collName).
						WithFilter("").WithOutputFields("vector").WithOffset(50).WithLimit(50))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 50, page2Res.ResultCount, "page 2 should return 50 rows")
					VerifyNullableVectorData(t, vt, page2Res, testData.PkToVecIdx, testData.OriginalVectors, "page2")

					// Test pagination: page3 offset=100, limit=50
					page3Res, err := mc.Query(ctx, client.NewQueryOption(collName).
						WithFilter("").WithOutputFields("vector").WithOffset(100).WithLimit(50))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 50, page3Res.ResultCount, "page 3 should return 50 rows")
					VerifyNullableVectorData(t, vt, page3Res, testData.PkToVecIdx, testData.OriginalVectors, "page3")

					// Test mixed query with filter
					mixedPageRes, err := mc.Query(ctx, client.NewQueryOption(collName).
						WithFilter("int64 >= 40 and int64 < 60").
						WithOutputFields("vector"))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 20, mixedPageRes.ResultCount, "mixed query should return 20 rows")
					VerifyNullableVectorData(t, vt, mixedPageRes, testData.PkToVecIdx, testData.OriginalVectors, "mixed query")
				}

				// clean up
				err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
				common.CheckErr(t, err, true)
			})
		}
	}
}

func TestNullableVectorMultiPartitions(t *testing.T) {
	vectorTypes := GetVectorTypes()

	for _, vt := range vectorTypes {
		testName := vt.Name
		t.Run(testName, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := hp.CreateDefaultMilvusClient(ctx, t)

			collName := common.GenRandomString("nullable_vec_part", 5)
			pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
			vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true)
			if vt.FieldType != entity.FieldTypeSparseVector {
				vecField = vecField.WithDim(common.DefaultDim)
			}
			schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)

			// create partitions
			partitions := []string{"partition_a", "partition_b", "partition_c"}
			for _, p := range partitions {
				err = mc.CreatePartition(ctx, client.NewCreatePartitionOption(collName, p))
				common.CheckErr(t, err, true)
			}

			// insert data into each partition with different null ratios
			nbPerPartition := 100
			nullRatios := []int{0, 30, 50} // 0%, 30%, 50% null for each partition

			// Store all test data and mappings for verification
			allPkToVecIdx := make(map[int64]int)
			var allOriginalVectors interface{}
			var firstSearchVec entity.Vector
			globalVecIdx := 0

			for i, partition := range partitions {
				nullRatio := nullRatios[i]
				testData := GenerateNullableVectorTestData(t, vt, nbPerPartition, nullRatio, "vector")

				// pk column with unique ids per partition
				pkData := make([]int64, nbPerPartition)
				for j := range nbPerPartition {
					pkData[j] = int64(i*nbPerPartition + j)
				}
				pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)

				for j := range nbPerPartition {
					if testData.ValidData[j] {
						allPkToVecIdx[pkData[j]] = globalVecIdx
						globalVecIdx++
					}
				}

				// Accumulate original vectors for verification
				switch vt.FieldType {
				case entity.FieldTypeFloatVector:
					if allOriginalVectors == nil {
						allOriginalVectors = make([][]float32, 0)
					}
					allOriginalVectors = append(allOriginalVectors.([][]float32), testData.OriginalVectors.([][]float32)...)
				case entity.FieldTypeBinaryVector:
					if allOriginalVectors == nil {
						allOriginalVectors = make([][]byte, 0)
					}
					allOriginalVectors = append(allOriginalVectors.([][]byte), testData.OriginalVectors.([][]byte)...)
				case entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector:
					if allOriginalVectors == nil {
						allOriginalVectors = make([][]byte, 0)
					}
					allOriginalVectors = append(allOriginalVectors.([][]byte), testData.OriginalVectors.([][]byte)...)
				case entity.FieldTypeInt8Vector:
					if allOriginalVectors == nil {
						allOriginalVectors = make([][]int8, 0)
					}
					allOriginalVectors = append(allOriginalVectors.([][]int8), testData.OriginalVectors.([][]int8)...)
				case entity.FieldTypeSparseVector:
					if allOriginalVectors == nil {
						allOriginalVectors = make([]entity.SparseEmbedding, 0)
					}
					allOriginalVectors = append(allOriginalVectors.([]entity.SparseEmbedding), testData.OriginalVectors.([]entity.SparseEmbedding)...)
				}

				// Save first search vector (from partition_a with 0% null)
				if i == 0 && testData.SearchVec != nil {
					firstSearchVec = testData.SearchVec
				}

				// insert into partition
				insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, testData.VecColumn).WithPartition(partition))
				common.CheckErr(t, err, true)
				require.EqualValues(t, nbPerPartition, insertRes.InsertCount)
			}

			// flush
			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			// create index and load
			vecIndex := CreateNullableVectorIndex(vt)
			indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
			common.CheckErr(t, err, true)
			err = indexTask.Await(ctx)
			common.CheckErr(t, err, true)

			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			// search in specific partition - verify all results from partition_a
			searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{firstSearchVec}).
				WithANNSField("vector").
				WithPartitions("partition_a"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 1, len(searchRes))
			searchIDs := searchRes[0].IDs.(*column.ColumnInt64).Data()
			require.EqualValues(t, 10, len(searchIDs), "search in partition_a should return 10 results")
			// partition_a has 0% null, so all 100 vectors are valid, IDs should be 0-99
			for _, id := range searchIDs {
				require.True(t, id >= 0 && id < int64(nbPerPartition), "partition_a IDs should be in range [0, %d), got %d", nbPerPartition, id)
				// Verify all search results have valid vectors
				_, ok := allPkToVecIdx[id]
				require.True(t, ok, "search result pk %d should have a valid vector", id)
			}

			// search across all partitions - should return results from any partition
			searchRes, err = mc.Search(ctx, client.NewSearchOption(collName, 50, []entity.Vector{firstSearchVec}).
				WithANNSField("vector"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 1, len(searchRes))
			allSearchIDs := searchRes[0].IDs.(*column.ColumnInt64).Data()
			require.EqualValues(t, 50, len(allSearchIDs), "search across all partitions should return 50 results")
			// Verify all search results have valid vectors
			for _, id := range allSearchIDs {
				_, ok := allPkToVecIdx[id]
				require.True(t, ok, "all partitions search result pk %d should have a valid vector", id)
			}

			// query each partition to verify counts
			expectedCounts := []int64{100, 100, 100} // total rows in each partition
			for i, partition := range partitions {
				queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).
					WithFilter("").
					WithOutputFields("count(*)").
					WithPartitions(partition))
				common.CheckErr(t, err, true)
				count, err := queryRes.Fields[0].GetAsInt64(0)
				common.CheckErr(t, err, true)
				require.EqualValues(t, expectedCounts[i], count, "partition %s should have %d rows", partition, expectedCounts[i])
			}

			// query with vector output from specific partition - partition_a (0% null)
			queryVecRes, err := mc.Query(ctx, client.NewQueryOption(collName).
				WithFilter("int64 < 10").
				WithOutputFields("vector").
				WithPartitions("partition_a"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 10, queryVecRes.ResultCount, "query partition_a with int64 < 10 should return 10 rows")
			VerifyNullableVectorData(t, vt, queryVecRes, allPkToVecIdx, allOriginalVectors, "query partition_a int64 < 10")

			// query partition_b which has 30% null (rows 100-129 are null, 130-199 are valid)
			queryPartBRes, err := mc.Query(ctx, client.NewQueryOption(collName).
				WithFilter("int64 >= 100 AND int64 < 150").
				WithOutputFields("vector").
				WithPartitions("partition_b"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 50, queryPartBRes.ResultCount, "query partition_b with 100 <= int64 < 150 should return 50 rows")
			VerifyNullableVectorData(t, vt, queryPartBRes, allPkToVecIdx, allOriginalVectors, "query partition_b int64 100-149")

			// query partition_c which has 50% null (rows 200-249 are null, 250-299 are valid)
			queryPartCRes, err := mc.Query(ctx, client.NewQueryOption(collName).
				WithFilter("int64 >= 200 AND int64 < 260").
				WithOutputFields("vector").
				WithPartitions("partition_c"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 60, queryPartCRes.ResultCount, "query partition_c with 200 <= int64 < 260 should return 60 rows")
			VerifyNullableVectorData(t, vt, queryPartCRes, allPkToVecIdx, allOriginalVectors, "query partition_c int64 200-259")

			// verify total count across all partitions
			totalCountRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("").WithOutputFields("count(*)"))
			common.CheckErr(t, err, true)
			totalCount, err := totalCountRes.Fields[0].GetAsInt64(0)
			common.CheckErr(t, err, true)
			require.EqualValues(t, nbPerPartition*3, totalCount, "total count should be %d", nbPerPartition*3)

			// clean up
			err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
			common.CheckErr(t, err, true)
		})
	}
}

func TestNullableVectorCompaction(t *testing.T) {
	vectorTypes := GetVectorTypes()

	for _, vt := range vectorTypes {
		testName := vt.Name
		t.Run(testName, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
			mc := hp.CreateDefaultMilvusClient(ctx, t)

			collName := common.GenRandomString("nullable_vec_comp", 5)
			pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
			vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true)
			if vt.FieldType != entity.FieldTypeSparseVector {
				vecField = vecField.WithDim(common.DefaultDim)
			}
			schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)

			// insert data in multiple batches to create multiple segments
			nb := 200
			nullPercent := 30

			// Store all vectors and mappings for verification
			allPkToVecIdx := make(map[int64]int)
			var allOriginalVectors interface{}
			var searchVec entity.Vector
			globalVecIdx := 0

			// batch 1: generate test data
			testData1 := GenerateNullableVectorTestData(t, vt, nb, nullPercent, "vector")

			pkData1 := make([]int64, nb)
			for i := range nb {
				pkData1[i] = int64(i)
			}
			pkColumn1 := column.NewColumnInt64(common.DefaultInt64FieldName, pkData1)

			for i := range nb {
				if testData1.ValidData[i] {
					allPkToVecIdx[pkData1[i]] = globalVecIdx
					globalVecIdx++
				}
			}

			// Store original vectors
			allOriginalVectors = testData1.OriginalVectors
			searchVec = testData1.SearchVec

			insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn1, testData1.VecColumn))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, insertRes.InsertCount)

			// flush to create segment
			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			// wait for rate limiter reset before next flush (rate=0.1 means 1 flush per 10s)
			time.Sleep(10 * time.Second)

			testData2 := GenerateNullableVectorTestData(t, vt, nb, nullPercent, "vector")

			pkData2 := make([]int64, nb)
			for i := range nb {
				pkData2[i] = int64(nb + i)
			}
			pkColumn2 := column.NewColumnInt64(common.DefaultInt64FieldName, pkData2)

			for i := range nb {
				if testData2.ValidData[i] {
					allPkToVecIdx[pkData2[i]] = globalVecIdx
					globalVecIdx++
				}
			}

			// Accumulate original vectors for verification
			switch vt.FieldType {
			case entity.FieldTypeFloatVector:
				allOriginalVectors = append(allOriginalVectors.([][]float32), testData2.OriginalVectors.([][]float32)...)
			case entity.FieldTypeBinaryVector:
				allOriginalVectors = append(allOriginalVectors.([][]byte), testData2.OriginalVectors.([][]byte)...)
			case entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector:
				allOriginalVectors = append(allOriginalVectors.([][]byte), testData2.OriginalVectors.([][]byte)...)
			case entity.FieldTypeInt8Vector:
				allOriginalVectors = append(allOriginalVectors.([][]int8), testData2.OriginalVectors.([][]int8)...)
			case entity.FieldTypeSparseVector:
				allOriginalVectors = append(allOriginalVectors.([]entity.SparseEmbedding), testData2.OriginalVectors.([]entity.SparseEmbedding)...)
			}

			insertRes, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn2, testData2.VecColumn))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, insertRes.InsertCount)

			// flush to create another segment
			flushTask, err = mc.Flush(ctx, client.NewFlushOption(collName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			// create index and load
			vecIndex := CreateNullableVectorIndex(vt)
			indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
			common.CheckErr(t, err, true)
			err = indexTask.Await(ctx)
			common.CheckErr(t, err, true)

			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			// delete some data (mix of valid and null vectors) - first 50 rows from batch 1
			delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr("int64 < 50"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 50, delRes.DeleteCount, "should delete 50 rows")

			// trigger manual compaction
			compactID, err := mc.Compact(ctx, client.NewCompactOption(collName))
			common.CheckErr(t, err, true)
			t.Logf("Compaction started with ID: %d", compactID)

			// wait for compaction to complete
			for i := 0; i < 60; i++ {
				state, err := mc.GetCompactionState(ctx, client.NewGetCompactionStateOption(compactID))
				common.CheckErr(t, err, true)
				if state == entity.CompactionStateCompleted {
					t.Log("Compaction completed")
					break
				}
				time.Sleep(time.Second)
			}

			// verify remaining count: 400 total - 50 deleted = 350
			queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("").WithOutputFields("count(*)"))
			common.CheckErr(t, err, true)
			count, err := queryRes.Fields[0].GetAsInt64(0)
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb*2-50, count, "remaining count should be 400 - 50 = 350")

			// verify deleted rows are gone
			queryDeletedRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 < 50").WithOutputFields("count(*)"))
			common.CheckErr(t, err, true)
			deletedCount, err := queryDeletedRes.Fields[0].GetAsInt64(0)
			common.CheckErr(t, err, true)
			require.EqualValues(t, 0, deletedCount, "deleted rows should not exist")

			// search should still work - verify returns results
			searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{searchVec}).WithANNSField("vector"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 1, len(searchRes))
			searchIDs := searchRes[0].IDs.(*column.ColumnInt64).Data()
			require.EqualValues(t, 10, len(searchIDs), "search should return 10 results")
			// All search results should have IDs >= 50 (since we deleted pk < 50) and have valid vectors
			for _, id := range searchIDs {
				require.True(t, id >= 50, "search results should not include deleted IDs, got %d", id)
				_, ok := allPkToVecIdx[id]
				require.True(t, ok, "search result pk %d should have a valid vector", id)
			}

			// query with output vector field - verify remaining valid vectors in batch 1
			queryRes, err = mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 >= 50 and int64 < 100").WithOutputFields("vector"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 50, queryRes.ResultCount, "should have 50 rows in range [50, 100)")
			VerifyNullableVectorData(t, vt, queryRes, allPkToVecIdx, allOriginalVectors, "query batch1 remaining 50-99")

			queryMixedRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 >= 200 and int64 < 250").WithOutputFields("vector"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 50, queryMixedRes.ResultCount, "should have 50 rows in range [200, 250)")
			VerifyNullableVectorData(t, vt, queryMixedRes, allPkToVecIdx, allOriginalVectors, "query batch2 200-249")

			queryBatch2CountRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 >= 200").WithOutputFields("count(*)"))
			common.CheckErr(t, err, true)
			batch2Count, err := queryBatch2CountRes.Fields[0].GetAsInt64(0)
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, batch2Count, "batch 2 should have all %d rows intact", nb)

			// clean up
			err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
			common.CheckErr(t, err, true)
		})
	}
}

func TestNullableVectorAddField(t *testing.T) {
	vectorTypes := GetVectorTypes()

	for _, vt := range vectorTypes {
		t.Run(vt.Name, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := hp.CreateDefaultMilvusClient(ctx, t)

			collName := common.GenRandomString("nullable_vec_add", 5)
			pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
			origVecField := entity.NewField().WithName("orig_vec").WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)
			schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(origVecField)

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)

			nb := 100
			pkData1 := make([]int64, nb)
			for i := range nb {
				pkData1[i] = int64(i)
			}
			pkColumn1 := column.NewColumnInt64(common.DefaultInt64FieldName, pkData1)

			origVecData1 := make([][]float32, nb)
			for i := range nb {
				vec := make([]float32, common.DefaultDim)
				for j := range common.DefaultDim {
					vec[j] = float32(i*common.DefaultDim+j) / 10000.0
				}
				origVecData1[i] = vec
			}
			origVecColumn1 := column.NewColumnFloatVector("orig_vec", common.DefaultDim, origVecData1)

			insertRes1, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn1, origVecColumn1))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, insertRes1.InsertCount)

			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			// wait for rate limiter reset before next flush (rate=0.1 means 1 flush per 10s)
			time.Sleep(10 * time.Second)

			// SparseVector does not need dim, but other vectors do
			newVecField := entity.NewField().WithName("new_vec").WithDataType(vt.FieldType).WithNullable(true)
			if vt.FieldType != entity.FieldTypeSparseVector {
				newVecField = newVecField.WithDim(common.DefaultDim)
			}
			err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collName, newVecField))
			common.CheckErr(t, err, true)

			// verify schema updated
			coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 3, len(coll.Schema.Fields), "should have 3 fields after adding new vector field")

			nullPercent := 30 // 30% null
			testData := GenerateNullableVectorTestData(t, vt, nb, nullPercent, "new_vec")

			pkData2 := make([]int64, nb)
			for i := range nb {
				pkData2[i] = int64(nb + i) // pk starts from nb
			}
			pkColumn2 := column.NewColumnInt64(common.DefaultInt64FieldName, pkData2)

			origVecData2 := make([][]float32, nb)
			for i := range nb {
				vec := make([]float32, common.DefaultDim)
				for j := range common.DefaultDim {
					vec[j] = float32((nb+i)*common.DefaultDim+j) / 10000.0
				}
				origVecData2[i] = vec
			}
			origVecColumn2 := column.NewColumnFloatVector("orig_vec", common.DefaultDim, origVecData2)

			insertRes2, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn2, origVecColumn2, testData.VecColumn))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, insertRes2.InsertCount)

			flushTask2, err := mc.Flush(ctx, client.NewFlushOption(collName))
			common.CheckErr(t, err, true)
			err = flushTask2.Await(ctx)
			common.CheckErr(t, err, true)

			// create indexes
			origVecIndex := index.NewFlatIndex(entity.L2)
			indexTask1, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "orig_vec", origVecIndex))
			common.CheckErr(t, err, true)
			err = indexTask1.Await(ctx)
			common.CheckErr(t, err, true)

			newVecIndex := CreateNullableVectorIndexWithFieldName(vt, "new_vec")
			indexTask2, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "new_vec", newVecIndex))
			common.CheckErr(t, err, true)
			err = indexTask2.Await(ctx)
			common.CheckErr(t, err, true)

			// load collection
			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			// verify total count
			countRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("").WithOutputFields("count(*)"))
			common.CheckErr(t, err, true)
			totalCount, err := countRes.Fields[0].GetAsInt64(0)
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb*2, totalCount, "total count should be %d", nb*2)

			searchVec := entity.FloatVector(origVecData1[0])
			searchRes1, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{searchVec}).WithANNSField("orig_vec").WithOutputFields("new_vec"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, 1, len(searchRes1))
			require.EqualValues(t, 10, len(searchRes1[0].IDs.(*column.ColumnInt64).Data()), "search on orig_vec should return 10 results")

			if testData.SearchVec != nil {
				searchRes2, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{testData.SearchVec}).WithANNSField("new_vec").WithOutputFields("new_vec"))
				common.CheckErr(t, err, true)
				require.EqualValues(t, 1, len(searchRes2))
				searchIDs2 := searchRes2[0].IDs.(*column.ColumnInt64).Data()
				require.EqualValues(t, 10, len(searchIDs2), "search on new_vec should return 10 results")
				for _, id := range searchIDs2 {
					require.True(t, id >= int64(nb), "search on new_vec should only return batch 2 rows, got pk %d", id)
					_, ok := testData.PkToVecIdx[id-int64(nb)]
					require.True(t, ok, "search result pk %d should have valid new_vec", id)
				}
			}

			queryRes1, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 < 100").WithOutputFields("new_vec"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, queryRes1.ResultCount, "should have %d rows in batch 1", nb)
			newVecCol1 := queryRes1.GetColumn("new_vec")
			for i := 0; i < queryRes1.ResultCount; i++ {
				isNull, _ := newVecCol1.IsNull(i)
				require.True(t, isNull, "batch 1 rows should have null new_vec")
			}

			queryRes2, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter("int64 >= 100").WithOutputFields("new_vec"))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, queryRes2.ResultCount, "should have %d rows in batch 2", nb)

			pkToVecIdx2 := make(map[int64]int)
			for pk, idx := range testData.PkToVecIdx {
				// original PkToVecIdx uses pk 0..nb-1, need to map to nb..2*nb-1
				pkToVecIdx2[pk+int64(nb)] = idx
			}
			VerifyNullableVectorDataWithFieldName(t, vt, queryRes2, pkToVecIdx2, testData.OriginalVectors, "new_vec", "query batch 2")

			// clean up
			err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
			common.CheckErr(t, err, true)
		})
	}
}

func TestNullableVectorRangeSearch(t *testing.T) {
	vectorTypes := GetVectorTypes()

	for _, vt := range vectorTypes {
		t.Run(vt.Name, func(t *testing.T) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := hp.CreateDefaultMilvusClient(ctx, t)

			// create collection
			collName := common.GenRandomString("nullable_vec_range", 5)
			pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
			vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true)
			if vt.FieldType != entity.FieldTypeSparseVector {
				vecField = vecField.WithDim(common.DefaultDim)
			}
			schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)

			err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, true)

			// generate data with 30% null
			nb := 500
			nullPercent := 30
			testData := GenerateNullableVectorTestData(t, vt, nb, nullPercent, "vector")

			// pk column
			pkData := make([]int64, nb)
			for i := range nb {
				pkData[i] = int64(i)
			}
			pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)

			// insert
			insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, testData.VecColumn))
			common.CheckErr(t, err, true)
			require.EqualValues(t, nb, insertRes.InsertCount)

			// flush
			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
			common.CheckErr(t, err, true)
			err = flushTask.Await(ctx)
			common.CheckErr(t, err, true)

			// create index with appropriate metric type
			var vecIndex index.Index
			switch vt.FieldType {
			case entity.FieldTypeSparseVector:
				vecIndex = index.NewSparseInvertedIndex(entity.IP, 0.1)
			case entity.FieldTypeBinaryVector:
				// BinaryVector uses Hamming distance
				vecIndex = index.NewBinFlatIndex(entity.HAMMING)
			case entity.FieldTypeInt8Vector:
				// Int8Vector uses COSINE metric
				vecIndex = index.NewHNSWIndex(entity.COSINE, 8, 96)
			default:
				// FloatVector, Float16Vector, BFloat16Vector use L2 metric
				vecIndex = index.NewHNSWIndex(entity.L2, 8, 96)
			}
			indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
			common.CheckErr(t, err, true)
			err = indexTask.Await(ctx)
			common.CheckErr(t, err, true)

			// load
			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
			common.CheckErr(t, err, true)
			err = loadTask.Await(ctx)
			common.CheckErr(t, err, true)

			if testData.SearchVec != nil {
				var searchRes []client.ResultSet
				switch vt.FieldType {
				case entity.FieldTypeSparseVector:
					// For sparse vector, use IP metric with radius and range_filter
					// IP metric: higher is better, range is [radius, range_filter]
					annParams := index.NewSparseAnnParam()
					annParams.WithRadius(0)
					annParams.WithRangeFilter(100)
					annParams.WithDropRatio(0.2)
					searchRes, err = mc.Search(ctx, client.NewSearchOption(collName, 50, []entity.Vector{testData.SearchVec}).
						WithANNSField("vector").WithAnnParam(annParams).WithOutputFields("vector"))
				case entity.FieldTypeBinaryVector:
					// For binary vector, use Hamming distance
					// Hamming distance: smaller is better (number of different bits), range is [range_filter, radius]
					// With dim=128, max Hamming distance is 128
					searchRes, err = mc.Search(ctx, client.NewSearchOption(collName, 50, []entity.Vector{testData.SearchVec}).
						WithANNSField("vector").WithSearchParam("radius", "128").WithSearchParam("range_filter", "0").WithOutputFields("vector"))
				case entity.FieldTypeInt8Vector:
					// For int8 vector, use COSINE metric
					// COSINE distance: range is [0, 2], smaller is better, range is [range_filter, radius]
					searchRes, err = mc.Search(ctx, client.NewSearchOption(collName, 50, []entity.Vector{testData.SearchVec}).
						WithANNSField("vector").WithSearchParam("radius", "2").WithSearchParam("range_filter", "0").WithOutputFields("vector"))
				default:
					// For dense vectors (FloatVector, Float16Vector, BFloat16Vector), use L2 metric
					// L2 distance: smaller is better, so radius is upper bound, range_filter is lower bound
					searchRes, err = mc.Search(ctx, client.NewSearchOption(collName, 50, []entity.Vector{testData.SearchVec}).
						WithANNSField("vector").WithSearchParam("radius", "100").WithSearchParam("range_filter", "0").WithOutputFields("vector"))
				}
				common.CheckErr(t, err, true)
				require.EqualValues(t, 1, len(searchRes))

				// Verify all results have valid vectors (not null)
				searchIDs := searchRes[0].IDs.(*column.ColumnInt64).Data()
				require.Greater(t, len(searchIDs), 0, "range search should return results")
				for _, id := range searchIDs {
					_, ok := testData.PkToVecIdx[id]
					require.True(t, ok, "range search result pk %d should have valid vector", id)
				}

				// Verify scores are within range based on metric type
				scores := searchRes[0].Scores
				for i, score := range scores {
					switch vt.FieldType {
					case entity.FieldTypeSparseVector:
						// IP metric: higher is better, range is [radius, range_filter] = [0, 100]
						require.GreaterOrEqual(t, score, float32(0), "sparse vector score should be >= radius(0), got %f for pk %d", score, searchIDs[i])
						require.LessOrEqual(t, score, float32(100), "sparse vector score should be <= range_filter(100), got %f for pk %d", score, searchIDs[i])
					case entity.FieldTypeBinaryVector:
						// Hamming distance: range is [range_filter, radius] = [0, 128]
						require.GreaterOrEqual(t, score, float32(0), "Hamming score should be >= range_filter(0), got %f for pk %d", score, searchIDs[i])
						require.LessOrEqual(t, score, float32(128), "Hamming score should be <= radius(128), got %f for pk %d", score, searchIDs[i])
					case entity.FieldTypeInt8Vector:
						// COSINE distance: range is [range_filter, radius] = [0, 2]
						require.GreaterOrEqual(t, score, float32(0), "COSINE score should be >= range_filter(0), got %f for pk %d", score, searchIDs[i])
						require.LessOrEqual(t, score, float32(2), "COSINE score should be <= radius(2), got %f for pk %d", score, searchIDs[i])
					default:
						// L2 metric: lower is better, range is [range_filter, radius] = [0, 100]
						require.GreaterOrEqual(t, score, float32(0), "L2 score should be >= range_filter(0), got %f for pk %d", score, searchIDs[i])
						require.LessOrEqual(t, score, float32(100), "L2 score should be <= radius(100), got %f for pk %d", score, searchIDs[i])
					}
				}
			}

			// clean up
			err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
			common.CheckErr(t, err, true)
		})
	}
}

// index building on both SegmentGrowingImpl and ChunkedSegmentSealedImpl
func TestNullableVectorDifferentIndexTypes(t *testing.T) {
	vectorTypes := GetVectorTypes()
	nullPercents := GetNullPercents()

	segmentTypes := []string{"growing", "sealed"}

	for _, vt := range vectorTypes {
		indexConfigs := GetIndexesForVectorType(vt.FieldType)
		for _, nullPercent := range nullPercents {
			for _, segmentType := range segmentTypes {
				// For growing segment, only test once with default index (interim index IVF_FLAT_CC is always used)
				// For sealed segment, iterate through all user-specified index types
				var testIndexConfigs []IndexConfig
				if segmentType == "growing" {
					// Only use first (default) index config for growing segment
					testIndexConfigs = []IndexConfig{indexConfigs[0]}
				} else {
					// Test all index types for sealed segment
					testIndexConfigs = indexConfigs
				}

				for _, idxCfg := range testIndexConfigs {
					testName := fmt.Sprintf("%s_%s_%d%%null_%s", vt.Name, idxCfg.Name, nullPercent, segmentType)
					idxCfgCopy := idxCfg // capture loop variable
					t.Run(testName, func(t *testing.T) {
						ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*10)
						mc := hp.CreateDefaultMilvusClient(ctx, t)

						// Create collection with nullable vector
						collName := common.GenRandomString("nullable_vec_large", 5)
						pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
						vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true)
						if vt.FieldType != entity.FieldTypeSparseVector {
							vecField = vecField.WithDim(common.DefaultDim)
						}
						schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField)

						err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
						common.CheckErr(t, err, true)

						nb := 10000
						validData := make([]bool, nb)
						validCount := 0
						for i := range nb {
							validData[i] = (i % 100) >= nullPercent
							if validData[i] {
								validCount++
							}
						}

						pkToVecIdx := make(map[int64]int)
						vecIdx := 0
						for i := range nb {
							if validData[i] {
								pkToVecIdx[int64(i)] = vecIdx
								vecIdx++
							}
						}

						// Generate pk column
						pkData := make([]int64, nb)
						for i := range nb {
							pkData[i] = int64(i)
						}
						pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)

						// Generate vector column based on type
						var vecColumn column.Column
						var searchVec entity.Vector
						var originalVectors interface{}

						switch vt.FieldType {
						case entity.FieldTypeFloatVector:
							vectors := make([][]float32, validCount)
							for i := range validCount {
								vec := make([]float32, common.DefaultDim)
								for j := range common.DefaultDim {
									vec[j] = float32(i*common.DefaultDim+j) / float32(validCount*common.DefaultDim)
								}
								vectors[i] = vec
							}
							vecColumn, err = column.NewNullableColumnFloatVector("vector", common.DefaultDim, vectors, validData)
							searchVec = entity.FloatVector(vectors[0])
							originalVectors = vectors

						case entity.FieldTypeBinaryVector:
							vectors := make([][]byte, validCount)
							byteDim := common.DefaultDim / 8
							for i := range validCount {
								vec := make([]byte, byteDim)
								for j := range byteDim {
									vec[j] = byte((i + j) % 256)
								}
								vectors[i] = vec
							}
							vecColumn, err = column.NewNullableColumnBinaryVector("vector", common.DefaultDim, vectors, validData)
							searchVec = entity.BinaryVector(vectors[0])
							originalVectors = vectors

						case entity.FieldTypeFloat16Vector:
							vectors := make([][]byte, validCount)
							for i := range validCount {
								vectors[i] = common.GenFloat16Vector(common.DefaultDim)
							}
							vecColumn, err = column.NewNullableColumnFloat16Vector("vector", common.DefaultDim, vectors, validData)
							searchVec = entity.Float16Vector(vectors[0])
							originalVectors = vectors

						case entity.FieldTypeBFloat16Vector:
							vectors := make([][]byte, validCount)
							for i := range validCount {
								vectors[i] = common.GenBFloat16Vector(common.DefaultDim)
							}
							vecColumn, err = column.NewNullableColumnBFloat16Vector("vector", common.DefaultDim, vectors, validData)
							searchVec = entity.BFloat16Vector(vectors[0])
							originalVectors = vectors

						case entity.FieldTypeInt8Vector:
							vectors := make([][]int8, validCount)
							for i := range validCount {
								vec := make([]int8, common.DefaultDim)
								for j := range common.DefaultDim {
									vec[j] = int8((i + j) % 127)
								}
								vectors[i] = vec
							}
							vecColumn, err = column.NewNullableColumnInt8Vector("vector", common.DefaultDim, vectors, validData)
							searchVec = entity.Int8Vector(vectors[0])
							originalVectors = vectors

						case entity.FieldTypeSparseVector:
							vectors := make([]entity.SparseEmbedding, validCount)
							for i := range validCount {
								positions := []uint32{0, uint32(i%1000 + 1), uint32(i%10000 + 1000)}
								values := []float32{1.0, float32(i+1) / 1000.0, 0.1}
								vectors[i], err = entity.NewSliceSparseEmbedding(positions, values)
								common.CheckErr(t, err, true)
							}
							vecColumn, err = column.NewNullableColumnSparseFloatVector("vector", vectors, validData)
							searchVec = vectors[0]
							originalVectors = vectors
						}
						common.CheckErr(t, err, true)

						// Insert data
						insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, vecColumn))
						common.CheckErr(t, err, true)
						require.EqualValues(t, nb, insertRes.InsertCount)

						// For sealed segment, flush before creating index to convert growing to sealed
						if segmentType == "sealed" {
							flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
							common.CheckErr(t, err, true)
							err = flushTask.Await(ctx)
							common.CheckErr(t, err, true)
						}

						// Create index using the config for this test iteration
						vecIndex := CreateIndexFromConfig("vector", idxCfgCopy)
						indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
						common.CheckErr(t, err, true)
						err = indexTask.Await(ctx)
						common.CheckErr(t, err, true)

						// Load collection - specify load fields to potentially skip loading vector raw data
						// When vector has index and is specified in LoadFields, system may use index instead of field data
						loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName).
							WithLoadFields(common.DefaultInt64FieldName, "vector")) // Load pk and vector (via index)
						common.CheckErr(t, err, true)
						err = loadTask.Await(ctx)
						common.CheckErr(t, err, true)

						// Search
						searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{searchVec}).
							WithOutputFields("*").
							WithConsistencyLevel(entity.ClStrong))
						common.CheckErr(t, err, true)
						require.EqualValues(t, 1, len(searchRes))
						require.GreaterOrEqual(t, searchRes[0].ResultCount, 1)

						// Verify search results
						VerifyNullableVectorData(t, vt, searchRes[0], pkToVecIdx, originalVectors, "search")

						// Query to count rows
						queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).
							WithFilter(fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)).
							WithOutputFields("count(*)"))
						common.CheckErr(t, err, true)
						countCol := queryRes.GetColumn("count(*)")
						count, _ := countCol.GetAsInt64(0)
						require.EqualValues(t, nb, count)

						// Query with vector output to verify data
						queryVecRes, err := mc.Query(ctx, client.NewQueryOption(collName).
							WithFilter(fmt.Sprintf("%s < 100", common.DefaultInt64FieldName)).
							WithOutputFields("*"))
						common.CheckErr(t, err, true)

						// Verify query results
						VerifyNullableVectorData(t, vt, queryVecRes, pkToVecIdx, originalVectors, "query")

						// Clean up
						err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
						common.CheckErr(t, err, true)
					})
				}
			}
		}
	}
}

func TestNullableVectorGroupBy(t *testing.T) {
	groupByVectorTypes := []NullableVectorType{
		{"FloatVector", entity.FieldTypeFloatVector},
		{"Float16Vector", entity.FieldTypeFloat16Vector},
		{"BFloat16Vector", entity.FieldTypeBFloat16Vector},
	}
	nullPercents := GetNullPercents()

	for _, vt := range groupByVectorTypes {
		for _, nullPercent := range nullPercents {
			testName := fmt.Sprintf("%s_%d%%null", vt.Name, nullPercent)
			t.Run(testName, func(t *testing.T) {
				ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
				mc := hp.CreateDefaultMilvusClient(ctx, t)

				collName := common.GenRandomString("nullable_vec_groupby", 5)
				pkField := entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
				vecField := entity.NewField().WithName("vector").WithDataType(vt.FieldType).WithNullable(true).WithDim(common.DefaultDim)
				groupField := entity.NewField().WithName("group_id").WithDataType(entity.FieldTypeInt64)
				schema := entity.NewSchema().WithName(collName).WithField(pkField).WithField(vecField).WithField(groupField)

				err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)

				nb := 500
				numGroups := 50
				rowsPerGroup := nb / numGroups

				validData := make([]bool, nb)
				validCount := 0
				for i := range nb {
					validData[i] = (i % 100) >= nullPercent
					if validData[i] {
						validCount++
					}
				}

				pkData := make([]int64, nb)
				groupData := make([]int64, nb)
				for i := range nb {
					pkData[i] = int64(i)
					groupData[i] = int64(i / rowsPerGroup) // 0-9 -> group 0, 10-19 -> group 1, etc.
				}
				pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, pkData)
				groupColumn := column.NewColumnInt64("group_id", groupData)

				var vecColumn column.Column
				var searchVec entity.Vector

				switch vt.FieldType {
				case entity.FieldTypeFloatVector:
					vectors := make([][]float32, validCount)
					for i := range validCount {
						vec := make([]float32, common.DefaultDim)
						for j := range common.DefaultDim {
							vec[j] = float32(i*common.DefaultDim+j) / 10000.0
						}
						vectors[i] = vec
					}
					vecColumn, err = column.NewNullableColumnFloatVector("vector", common.DefaultDim, vectors, validData)
					if validCount > 0 {
						searchVec = entity.FloatVector(vectors[0])
					}

				case entity.FieldTypeFloat16Vector:
					vectors := make([][]byte, validCount)
					for i := range validCount {
						vectors[i] = common.GenFloat16Vector(common.DefaultDim)
					}
					vecColumn, err = column.NewNullableColumnFloat16Vector("vector", common.DefaultDim, vectors, validData)
					if validCount > 0 {
						searchVec = entity.Float16Vector(vectors[0])
					}

				case entity.FieldTypeBFloat16Vector:
					vectors := make([][]byte, validCount)
					for i := range validCount {
						vectors[i] = common.GenBFloat16Vector(common.DefaultDim)
					}
					vecColumn, err = column.NewNullableColumnBFloat16Vector("vector", common.DefaultDim, vectors, validData)
					if validCount > 0 {
						searchVec = entity.BFloat16Vector(vectors[0])
					}
				}
				common.CheckErr(t, err, true)

				// Insert
				insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, pkColumn, vecColumn, groupColumn))
				common.CheckErr(t, err, true)
				require.EqualValues(t, nb, insertRes.InsertCount)

				// Flush
				flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
				common.CheckErr(t, err, true)
				err = flushTask.Await(ctx)
				common.CheckErr(t, err, true)

				if validCount > 0 {
					vecIndex := index.NewFlatIndex(entity.L2)
					indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", vecIndex))
					common.CheckErr(t, err, true)
					err = indexTask.Await(ctx)
					common.CheckErr(t, err, true)

					scalarIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "group_id", index.NewAutoIndex(entity.L2)))
					common.CheckErr(t, err, true)
					err = scalarIndexTask.Await(ctx)
					common.CheckErr(t, err, true)

					// Load
					loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
					common.CheckErr(t, err, true)
					err = loadTask.Await(ctx)
					common.CheckErr(t, err, true)

					searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{searchVec}).
						WithANNSField("vector").
						WithGroupByField("group_id").
						WithOutputFields(common.DefaultInt64FieldName, "group_id"))
					common.CheckErr(t, err, true)
					require.EqualValues(t, 1, len(searchRes))

					// 1. Result count should be <= limit (number of unique groups)
					// 2. Each result should have a unique group_id
					// 3. All returned PKs should have valid vectors (not null)
					resultCount := searchRes[0].ResultCount
					require.LessOrEqual(t, resultCount, 10, "result count should be <= limit")

					// Check unique group_ids
					seenGroups := make(map[int64]bool)
					for i := 0; i < resultCount; i++ {
						groupByValue, err := searchRes[0].GroupByValue.Get(i)
						require.NoError(t, err)
						groupID := groupByValue.(int64)
						require.False(t, seenGroups[groupID], "group_id should be unique in GroupBy results")
						seenGroups[groupID] = true

						// Verify the returned PK has a valid vector
						pkValue, _ := searchRes[0].IDs.GetAsInt64(i)
						require.True(t, validData[pkValue], "returned pk %d should have valid vector", pkValue)
					}
				}

				// Clean up
				err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
				common.CheckErr(t, err, true)
			})
		}
	}
}
