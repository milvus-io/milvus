package testcases

import (
	"fmt"
	"math/rand"
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

func TestSearchDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// search with not exist collection
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	_, err := mc.Search(ctx, client.NewSearchOption("aaa", common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "can't find collection")

	// search with empty collections name
	_, err = mc.Search(ctx, client.NewSearchOption("", common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "collection name should not be empty")

	// search with not exist partition
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, err1 := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithPartitions([]string{"aaa"}))
	common.CheckErr(t, err1, false, "partition name aaa not found")

	// search with empty partition name []string{""} -> error
	_, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).
		WithConsistencyLevel(entity.ClStrong).WithANNSField(common.DefaultFloatVecFieldName).WithPartitions([]string{""}))
	common.CheckErr(t, errSearch, false, "Partition name should not be empty")
}

// test search empty collection -> return empty
func TestSearchEmptyCollection(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus/issues/33952")
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	t.Skip("https://github.com/milvus-io/milvus/issues/33952")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	queryRes, _ := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithFilter(fmt.Sprintf("int64 in [%d, %d]", _defId0, _parId0)).WithOutputFields([]string{"*"}))
	require.ElementsMatch(t, []int64{_defId0, _parId0}, queryRes.GetColumn(common.DefaultInt64FieldName).(*column.ColumnInt64).Data())
	for _, vec := range queryRes.GetColumn(common.DefaultFloatVecFieldName).(*column.ColumnFloatVector).Data() {
		vectors = append(vectors, entity.FloatVector(vec))
	}

	for _, partitions := range [][]string{{}, {common.DefaultPartition, parName}} {
		// search with empty partition names slice []string{} -> all partitions
		searchResult, errSearch1 := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, 5, vectors).
			WithConsistencyLevel(entity.ClStrong).WithANNSField(common.DefaultFloatVecFieldName).WithPartitions(partitions).WithOutputFields([]string{"*"}))

		// check search result contains search vector, which from all partitions
		common.CheckErr(t, errSearch1, true)
		common.CheckSearchResult(t, searchResult, len(vectors), 5)
		require.Contains(t, searchResult[0].IDs.(*column.ColumnInt64).Data(), _defId0)
		require.Contains(t, searchResult[1].IDs.(*column.ColumnInt64).Data(), _parId0)
		require.EqualValues(t, entity.FloatVector(searchResult[0].GetColumn(common.DefaultFloatVecFieldName).(*column.ColumnFloatVector).Data()[0]), vectors[0])
		require.EqualValues(t, entity.FloatVector(searchResult[1].GetColumn(common.DefaultFloatVecFieldName).(*column.ColumnFloatVector).Data()[0]), vectors[1])
	}
}

// test query empty output fields: []string{} -> []string{}
// test query empty output fields: []string{""} -> error
func TestSearchEmptyOutputFields(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	for _, dynamic := range []bool{true, false} {
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(dynamic))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		resSearch, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields([]string{}))
		common.CheckErr(t, err, true)
		common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
		common.CheckOutputFields(t, []string{}, resSearch[0].Fields)

		_, err = mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields([]string{""}))
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
	mc := createDefaultMilvusClient(ctx, t)

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
			resSearch, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields(_dof.outputFields))
			if enableDynamic {
				common.CheckErr(t, err, true)
				common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
				common.CheckOutputFields(t, _dof.expOutputFields, resSearch[0].Fields)
			} else {
				common.CheckErr(t, err, false, "not exist")
			}
		}
		existedRepeatedFields := []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultInt64FieldName, common.DefaultFloatVecFieldName}
		resSearch2, err2 := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields(existedRepeatedFields))
		common.CheckErr(t, err2, true)
		common.CheckSearchResult(t, resSearch2, common.DefaultNq, common.DefaultLimit)
		common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName}, resSearch2[0].Fields)
	}
}

// test search output all * fields when enable dynamic and insert dynamic column data
func TestSearchOutputAllFields(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
		WithANNSField(common.DefaultFloatVecFieldName).WithOutputFields([]string{"*"}))
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
	mc := createDefaultMilvusClient(ctx, t)

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
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields([]string{"*"}))
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
	mc := createDefaultMilvusClient(ctx, t)

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
		WithANNSField(common.DefaultSparseVecFieldName).WithOutputFields([]string{"*"}))
	common.CheckErr(t, err, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	for _, res := range searchRes {
		common.CheckOutputFields(t, allFieldsName, res.Fields)
	}
}

// test search with invalid vector field name: not exist; non-vector field, empty fiend name, json and dynamic field -> error
func TestSearchInvalidVectorField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	mc := createDefaultMilvusClient(ctx, t)

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
	mc := createDefaultMilvusClient(ctx, t)

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
	t.Skip("Waiting for support for specifying search parameters")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).
		TWithFieldIndex(map[string]index.Index{common.DefaultFloatVecFieldName: index.NewHNSWIndex(entity.COSINE, 8, 200)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	_, errSearchEmpty := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors))
	common.CheckErr(t, errSearchEmpty, false, "metric type not match: invalid parameter")
}

// test search with invalid topK -> error
func TestSearchInvalidTopK(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	t.Skip("Waiting for support for specifying search parameters")
}

// search with index hnsw search param ef < topK -> error
func TestSearchEfHnsw(t *testing.T) {
	t.Skip("Waiting for support for specifying search parameters")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).
		TWithFieldIndex(map[string]index.Index{common.DefaultFloatVecFieldName: index.NewHNSWIndex(entity.COSINE, 8, 200)}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	_, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors))
	common.CheckErr(t, err, false, "ef(7) should be larger than k(10)")
}

// test search params mismatch index type, hnsw index and ivf sq8 search param -> search with default hnsw params, ef=topK
func TestSearchSearchParamsMismatchIndex(t *testing.T) {
	t.Skip("Waiting for support for specifying search parameters")
}

// search with index scann search param ef < topK -> error
func TestSearchInvalidScannReorderK(t *testing.T) {
	t.Skip("Waiting for support for specifying search parameters")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{
		common.DefaultFloatVecFieldName: index.NewSCANNIndex(entity.COSINE, 16, true),
	}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with invalid reorder_k < topK

	// valid scann index search reorder_k
}

// test search with scann index params: with_raw_data and metrics_type [L2, IP, COSINE]
func TestSearchScannAllMetricsWithRawData(t *testing.T) {
	t.Skip("Waiting for support scann index params withRawData")
	t.Parallel()
	/*for _, withRawData := range []bool{true, false} {
		for _, metricType := range []entity.MetricType{entity.L2, entity.IP, entity.COSINE} {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := createDefaultMilvusClient(ctx, t)

			prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption())
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema, 500), hp.TNewDataOption())
			prepare.FlushData(ctx, t, mc, schema.CollectionName)
			prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{
				common.DefaultFloatVecFieldName: index.NewSCANNIndex(entity.COSINE, 16),
			}))
			prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

			// search and output all fields
			vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
			resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).WithOutputFields([]string{"*"}))
			common.CheckErr(t, errSearch, true)
			common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultFloatFieldName,
				common.DefaultJSONFieldName, common.DefaultFloatVecFieldName, common.DefaultDynamicFieldName}, resSearch[0].Fields)
			common.CheckSearchResult(t, resSearch, 1, common.DefaultLimit)
		}
	}*/
}

// test search with valid expression
func TestSearchExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	type mExprExpected struct {
		expr string
		ids  []int64
	}

	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	for _, _mExpr := range []mExprExpected{
		{expr: fmt.Sprintf("%s < 10", common.DefaultInt64FieldName), ids: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{expr: fmt.Sprintf("%s in [10, 100]", common.DefaultInt64FieldName), ids: []int64{10, 100}},
	} {
		resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
			WithFilter(_mExpr.expr))
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
	mc := createDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VecJSON), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// search with invalid expr
	vectors := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	for _, exprStruct := range common.InvalidExpressions {
		log.Debug("TestSearchInvalidExpr", zap.String("expr", exprStruct.Expr))
		_, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
			WithFilter(exprStruct.Expr).WithANNSField(common.DefaultFloatVecFieldName))
		common.CheckErr(t, errSearch, exprStruct.ErrNil, exprStruct.ErrMsg)
	}
}

func TestSearchJsonFieldExpr(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

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
			log.Debug("TestSearchJsonFieldExpr", zap.String("expr", expr))
			vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
			searchRes, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
				WithFilter(expr).WithANNSField(common.DefaultFloatVecFieldName).WithOutputFields([]string{common.DefaultInt64FieldName, common.DefaultJSONFieldName}))
			common.CheckErr(t, errSearch, true)
			common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultJSONFieldName}, searchRes[0].Fields)
			common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
		}
	}
}

func TestSearchDynamicFieldExpr(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
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
		log.Debug("TestSearchDynamicFieldExpr", zap.String("expr", expr))
		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		searchRes, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
			WithFilter(expr).WithANNSField(common.DefaultFloatVecFieldName).WithOutputFields([]string{common.DefaultInt64FieldName, "dynamicNumber", "number"}))
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
			WithOutputFields([]string{common.DefaultInt64FieldName, common.DefaultJSONFieldName, common.DefaultDynamicFieldName, "dynamicNumber", "number"}))
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
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
			WithFilter(expr).WithOutputFields(allArrayFields))
		common.CheckErr(t, errSearch, true)
		common.CheckOutputFields(t, allArrayFields, searchRes[0].Fields)
		common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	}

	// search hits empty
	searchRes, errSearchEmpty := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).WithConsistencyLevel(entity.ClStrong).
		WithFilter(fmt.Sprintf("array_contains (%s, 1000000)", common.DefaultInt32ArrayField)).WithOutputFields(allArrayFields))
	common.CheckErr(t, errSearchEmpty, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, 0)
}

// test search with field not existed expr: if dynamic
func TestSearchNotExistedExpr(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

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
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

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
	type mFieldNameType struct {
		fieldName  string
		fieldType  entity.FieldType
		metricType entity.MetricType
	}
	fnts := []mFieldNameType{
		{fieldName: common.DefaultFloatVecFieldName, fieldType: entity.FieldTypeFloatVector, metricType: entity.L2},
		{fieldName: common.DefaultBinaryVecFieldName, fieldType: entity.FieldTypeBinaryVector, metricType: entity.JACCARD},
		{fieldName: common.DefaultFloat16VecFieldName, fieldType: entity.FieldTypeFloat16Vector, metricType: entity.L2},
		{fieldName: common.DefaultBFloat16VecFieldName, fieldType: entity.FieldTypeBFloat16Vector, metricType: entity.L2},
	}

	for _, fnt := range fnts {
		queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, fnt.fieldType)
		expr := fmt.Sprintf("%s > 10", common.DefaultInt64FieldName)

		resSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit*2, queryVec).WithConsistencyLevel(entity.ClStrong).
			WithFilter(expr).WithANNSField(fnt.fieldName).WithOutputFields([]string{"*"}))
		common.CheckErr(t, errSearch, true)
		common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit*2)
		common.CheckOutputFields(t, []string{
			common.DefaultInt64FieldName, common.DefaultFloatVecFieldName,
			common.DefaultBinaryVecFieldName, common.DefaultFloat16VecFieldName, common.DefaultBFloat16VecFieldName, common.DefaultDynamicFieldName,
		}, resSearch[0].Fields)

		// pagination search
		resPage, errPage := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithConsistencyLevel(entity.ClStrong).
			WithFilter(expr).WithANNSField(fnt.fieldName).WithOutputFields([]string{"*"}).WithOffset(10))

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
	mc := createDefaultMilvusClient(ctx, t)

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
			WithOutputFields([]string{"*"}))

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
	mc := createDefaultMilvusClient(ctx, t)

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
	mc := createDefaultMilvusClient(ctx, t)

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
	t.Skip("https://github.com/milvus-io/milvus/issues/33952")
	t.Skip("https://github.com/zilliztech/knowhere/issues/774")
	idxInverted := index.NewSparseInvertedIndex(entity.IP, 0.1)
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

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
		prepare.FlushData(ctx, t, mc, schema.CollectionName)

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
	mc := createDefaultMilvusClient(ctx, t)

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
			WithOutputFields([]string{"*"}))
		common.CheckErr(t, errSearch, true)
		require.Len(t, resSearch, common.DefaultNq)

		pageSearch, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithConsistencyLevel(entity.ClStrong).
			WithOutputFields([]string{"*"}).WithOffset(5))
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
	t.Skip("Go-sdk support iterator search in progress")
}

func TestRangeSearchSparseVector(t *testing.T) {
	t.Skip("Waiting for support range search")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(), hp.TNewSchemaOption().
		TWithEnableDynamicField(true))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithSparseMaxLen(128))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// TODO range search
}
