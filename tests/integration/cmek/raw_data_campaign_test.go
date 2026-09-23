// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmek

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	rawDataRows = 512
	rawDataDim  = 8
)

// rawDataCampaign describes one shared scalar, vector, or StructArray scenario.
type rawDataCampaign struct {
	name       string
	schema     *schemapb.CollectionSchema
	fields     []*schemapb.FieldData
	loadFields []string
	search     bool
	shardsNum  int32
}

// prepareRawDataCollection creates a collection that can accept more batches.
func (s *rawDataSuite) prepareRawDataCollection(ctx context.Context, c rawDataCampaign) *milvuspb.DescribeCollectionResponse {
	collection := "cmek_raw_" + c.name + "_" + funcutil.GenRandomStr()
	shardsNum := c.shardsNum
	if shardsNum == 0 {
		shardsNum = common.DefaultShardsNum
	}
	c.schema.Name = collection
	encoded, err := proto.Marshal(c.schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: s.dbName, CollectionName: collection, Schema: encoded, ShardsNum: shardsNum,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.T().Cleanup(func() { s.cleanupRawCollection(collection) })
	description, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(description, err))
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(description.GetProperties(), common.EncryptionEzIDKey))
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(description.GetSchema().GetProperties(), common.EncryptionEzIDKey))
	if description.GetSchema().GetEnableDynamicField() {
		// The public schema omits $meta. Keep its user-facing struct names,
		// and obtain the dynamic field ID from the coordinator's full schema.
		internal, err := s.Cluster.MixCoordClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base:   &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			DbName: s.dbName, CollectionName: collection,
		})
		s.Require().NoError(merr.CheckRPCCall(internal, err))
		s.Require().Equal(description.GetCollectionID(), internal.GetCollectionID())
		for _, field := range internal.GetSchema().GetFields() {
			if field.GetIsDynamic() {
				description.Schema.Fields = append(description.Schema.Fields, field)
			}
		}
	}
	s.bindRawDataFieldIDs(description.GetSchema(), c.fields)
	// Finish logical-index broadcasts before insert/flush starts the segment lifecycle.
	s.createRawVectorIndexes(ctx, collection, description.GetSchema())
	if s.growingSource {
		status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
			DbName: s.dbName, CollectionName: collection,
		})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForLoadWithDB(ctx, s.dbName, collection)
	}
	return description
}

func (s *rawDataSuite) insertRawDataBatch(ctx context.Context, description *milvuspb.DescribeCollectionResponse, fields []*schemapb.FieldData, rows int) *milvuspb.MutationResult {
	s.bindRawDataFieldIDs(description.GetSchema(), fields)
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName: s.dbName, CollectionName: description.GetCollectionName(), FieldsData: fields,
		HashKeys: integration.GenerateHashKeys(rows), NumRows: uint32(rows),
	})
	s.Require().NoError(merr.CheckRPCCall(insert, err))
	s.Require().Equal(int64(rows), insert.GetInsertCnt())
	return insert
}

// prepareRawDataCampaign returns the complete nonempty output of this flush.
// Compaction is disabled before the cluster starts, so the same segments are
// inspected and then read after release/reload.
func (s *rawDataSuite) prepareRawDataCampaign(ctx context.Context, c rawDataCampaign) (*milvuspb.DescribeCollectionResponse, []*datapb.SegmentInfo) {
	description := s.prepareRawDataCollection(ctx, c)
	collection := description.GetCollectionName()
	s.insertRawDataBatch(ctx, description, c.fields, rawDataRows)
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{DbName: s.dbName, CollectionNames: []string{collection}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	ids := flush.GetCollSegIDs()[collection].GetData()
	s.Require().NotEmpty(ids)
	s.WaitForFlush(ctx, ids, flush.GetCollFlushTs()[collection], s.dbName, collection)
	segments := s.rawFlushedSegments(collection, ids)
	var rows int64
	for _, segment := range segments {
		s.Require().Equal(description.GetCollectionID(), segment.GetCollectionID())
		s.Require().False(segment.GetCompacted())
		s.Require().False(segment.GetIsInvisible())
		rows += segment.GetNumOfRows()
	}
	s.Require().Equal(int64(rawDataRows), rows)
	if s.growingSource {
		s.assertGrowingSourceFlush(segments)
	}
	s.T().Logf("stage=flush campaign=%s collection=%d segments=%v rows=%d", c.name, description.GetCollectionID(), ids, rows)
	return description, segments
}

func (s *rawDataSuite) bindRawDataFieldIDs(schema *schemapb.CollectionSchema, data []*schemapb.FieldData) {
	ids := make(map[string]int64)
	for _, field := range schema.GetFields() {
		ids[field.GetName()] = field.GetFieldID()
	}
	for _, field := range schema.GetStructArrayFields() {
		ids[field.GetName()] = field.GetFieldID()
		for _, child := range field.GetFields() {
			ids[typeutil.ConcatStructFieldName(field.GetName(), child.GetName())] = child.GetFieldID()
		}
	}
	for _, field := range data {
		field.FieldId = ids[field.GetFieldName()]
		s.Require().Positive(field.FieldId, "field %s is missing from accepted schema", field.GetFieldName())
		for _, child := range field.GetStructArrays().GetFields() {
			name := typeutil.ConcatStructFieldName(field.GetFieldName(), child.GetFieldName())
			child.FieldId = ids[name]
			s.Require().Positive(child.FieldId, "field %s is missing from accepted schema", name)
		}
	}
}

func requestedFieldIDs(schema *schemapb.CollectionSchema, names []string) []int64 {
	fields := make(map[string][]int64)
	for _, field := range schema.GetFields() {
		fields[field.GetName()] = []int64{field.GetFieldID()}
	}
	for _, field := range schema.GetStructArrayFields() {
		for _, child := range field.GetFields() {
			fields[field.GetName()] = append(fields[field.GetName()], child.GetFieldID())
		}
	}
	var ids []int64
	for _, name := range names {
		ids = append(ids, fields[name]...)
	}
	return ids
}

func (s *rawDataSuite) assertRawDataOracle(ctx context.Context, collection string, c rawDataCampaign) {
	inserted, loadFields := c.fields, c.loadFields
	count, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName: s.dbName, CollectionName: collection, Expr: "", OutputFields: []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(count, err))
	s.Require().Equal(int64(rawDataRows), count.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])

	query, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName: s.dbName, CollectionName: collection,
		Expr: fmt.Sprintf("%s in [0, %d]", fixturePrimaryKey, rawDataRows-1), OutputFields: loadFields,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(query, err))
	wantedNames := make(map[string]struct{}, len(loadFields))
	for _, name := range loadFields {
		wantedNames[name] = struct{}{}
	}
	selected := make([]*schemapb.FieldData, 0, len(loadFields))
	for _, field := range inserted {
		if _, ok := wantedNames[field.GetFieldName()]; ok {
			selected = append(selected, field)
		}
	}
	expected := typeutil.PrepareResultFieldData(selected, 2)
	for i := range selected {
		typeutil.AppendFieldDataByColumn(expected[i], selected[i], []int64{0, rawDataRows - 1})
	}
	expectedByName := fieldDataByName(expected)
	for _, actual := range query.GetFieldsData() {
		want, ok := expectedByName[actual.GetFieldName()]
		s.Require().True(ok, "query returned unexpected field %s", actual.GetFieldName())
		actualCopy := proto.Clone(actual).(*schemapb.FieldData)
		wantCopy := proto.Clone(want).(*schemapb.FieldData)
		actualCopy.FieldId, wantCopy.FieldId = 0, 0
		switch actual.GetType() {
		case schemapb.DataType_JSON:
			actualRows := actual.GetScalars().GetJsonData().GetData()
			wantRows := want.GetScalars().GetJsonData().GetData()
			s.Require().Len(actualRows, len(wantRows), "field %s row count", actual.GetFieldName())
			for i := range wantRows {
				s.Require().JSONEq(string(wantRows[i]), string(actualRows[i]), "field %s row %d differs after cold load", actual.GetFieldName(), i)
			}
		case schemapb.DataType_SparseFloatVector:
			actualRows := actual.GetVectors().GetSparseFloatVector().GetContents()
			wantRows := want.GetVectors().GetSparseFloatVector().GetContents()
			s.Require().Len(actualRows, len(wantRows), "field %s row count", actual.GetFieldName())
			for i := range wantRows {
				s.Require().Equal(typeutil.SparseFloatBytesToMap(wantRows[i]), typeutil.SparseFloatBytesToMap(actualRows[i]),
					"field %s row %d differs after cold load", actual.GetFieldName(), i)
			}
		case schemapb.DataType_ArrayOfStruct:
			actualFields := actualCopy.GetStructArrays().GetFields()
			wantFields := wantCopy.GetStructArrays().GetFields()
			sort.Slice(actualFields, func(i, j int) bool { return actualFields[i].GetFieldId() < actualFields[j].GetFieldId() })
			sort.Slice(wantFields, func(i, j int) bool { return wantFields[i].GetFieldId() < wantFields[j].GetFieldId() })
			s.Require().True(proto.Equal(wantCopy, actualCopy), "field %s differs after cold load", actual.GetFieldName())
		default:
			s.Require().True(proto.Equal(wantCopy, actualCopy), "field %s differs after cold load", actual.GetFieldName())
		}
		delete(expectedByName, actual.GetFieldName())
	}
	s.Require().Empty(expectedByName)
	if c.search {
		s.assertExactFloatSearch(ctx, collection, "float_vector", firstFloatVector(c.fields, "float_vector", rawDataDim), rawDataRows)
	}
}

func (s *rawDataSuite) assertExactFloatSearch(ctx context.Context, collection, field string, vector []float32, ef int) {
	request := integration.ConstructSearchRequest(s.dbName, collection, "", field, schemapb.DataType_FloatVector,
		[]string{fixturePrimaryKey}, metric.L2, map[string]any{"ef": ef}, 1, len(vector), 1, -1)
	placeholder, err := proto.Marshal(funcutil.Float32VectorsToPlaceholderGroup([][]float32{vector}))
	s.Require().NoError(err)
	request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder}
	result, err := s.Cluster.MilvusClient.Search(ctx, request)
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.Require().Equal([]int64{0}, result.GetResults().GetIds().GetIntId().GetData())
	s.Require().Len(result.GetResults().GetScores(), 1)
	s.Require().InDelta(0, result.GetResults().GetScores()[0], 1e-6)
}

func newRawScalarCampaign() rawDataCampaign {
	fields := []*schemapb.FieldSchema{{Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}
	data := []*schemapb.FieldData{testutils.NewInt64FieldData(fixturePrimaryKey, rawDataRows)}
	loadFields := []string{fixturePrimaryKey}
	scalarTypes := []struct {
		name   string
		typeID schemapb.DataType
	}{
		{"bool_value", schemapb.DataType_Bool},
		{"int8_value", schemapb.DataType_Int8},
		{"int16_value", schemapb.DataType_Int16},
		{"int32_value", schemapb.DataType_Int32},
		{"int64_value", schemapb.DataType_Int64},
		{"float_value", schemapb.DataType_Float},
		{"double_value", schemapb.DataType_Double},
		{"varchar_value", schemapb.DataType_VarChar},
		{"geometry_value", schemapb.DataType_Geometry},
	}
	for _, item := range scalarTypes {
		field := &schemapb.FieldSchema{Name: item.name, DataType: item.typeID}
		if item.typeID == schemapb.DataType_VarChar {
			field.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}}
		}
		fields = append(fields, field)
		fieldData := testutils.GenerateScalarFieldData(item.typeID, item.name, rawDataRows)
		if item.typeID == schemapb.DataType_Geometry {
			values := make([]string, rawDataRows)
			for i := range values {
				values[i] = fmt.Sprintf("POINT (%d %d)", i%180, i%90)
			}
			fieldData = &schemapb.FieldData{
				Type:      schemapb.DataType_Geometry,
				FieldName: item.name,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: values}},
				}},
			}
		}
		if item.typeID == schemapb.DataType_Int8 {
			for i := range fieldData.GetScalars().GetIntData().Data {
				fieldData.GetScalars().GetIntData().Data[i] = int32(i % 100)
			}
		}
		data = append(data, fieldData)
		loadFields = append(loadFields, item.name)
	}
	timestamps := make([]string, rawDataRows)
	baseTimestamp := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	for i := range timestamps {
		timestamps[i] = baseTimestamp.Add(time.Duration(i) * time.Microsecond).Format(time.RFC3339Nano)
	}
	fields = append(fields, &schemapb.FieldSchema{Name: "timestamptz_value", DataType: schemapb.DataType_Timestamptz})
	data = append(data, &schemapb.FieldData{Type: schemapb.DataType_Timestamptz, FieldName: "timestamptz_value", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: timestamps}}}}})
	loadFields = append(loadFields, "timestamptz_value")

	arrayTypes := []schemapb.DataType{
		schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double, schemapb.DataType_VarChar,
	}
	for _, elementType := range arrayTypes {
		name := "array_" + elementType.String()
		typeParams := []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}}
		if elementType == schemapb.DataType_VarChar {
			typeParams = append(typeParams, &commonpb.KeyValuePair{Key: common.MaxLengthKey, Value: "128"})
		}
		fields = append(fields, &schemapb.FieldSchema{Name: name, DataType: schemapb.DataType_Array, ElementType: elementType, TypeParams: typeParams})
		data = append(data, deterministicArrayField(name, elementType, rawDataRows))
		loadFields = append(loadFields, name)
	}
	fields = append(fields, &schemapb.FieldSchema{Name: "json_value", DataType: schemapb.DataType_JSON})
	data = append(data, deterministicJSONField("json_value", rawDataRows, false))
	loadFields = append(loadFields, "json_value", common.MetaFieldName)
	data = append(data, deterministicJSONField(common.MetaFieldName, rawDataRows, true))
	// Partial load requires one vector field. This helper is loaded only to
	// satisfy that collection-level invariant; scalar assertions remain complete.
	fields = append(fields, vectorSchema("scalar_helper", schemapb.DataType_FloatVector, rawDataDim))
	data = append(data, deterministicFloatVectors("scalar_helper", rawDataRows, rawDataDim))
	loadFields = append(loadFields, "scalar_helper")
	return rawDataCampaign{name: "scalar", schema: &schemapb.CollectionSchema{EnableDynamicField: true, Fields: fields}, fields: data, loadFields: loadFields}
}

func newRawVectorCampaign() rawDataCampaign {
	fields := []*schemapb.FieldSchema{{Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}
	data := []*schemapb.FieldData{testutils.NewInt64FieldData(fixturePrimaryKey, rawDataRows)}
	loadFields := []string{fixturePrimaryKey}
	types := []struct {
		name   string
		typeID schemapb.DataType
	}{
		{"binary_vector", schemapb.DataType_BinaryVector},
		{"float_vector", schemapb.DataType_FloatVector},
		{"float16_vector", schemapb.DataType_Float16Vector},
		{"bfloat16_vector", schemapb.DataType_BFloat16Vector},
		{"sparse_vector", schemapb.DataType_SparseFloatVector},
		{"int8_vector", schemapb.DataType_Int8Vector},
	}
	for _, item := range types {
		fields = append(fields, vectorSchema(item.name, item.typeID, rawDataDim))
		data = append(data, deterministicVectorField(item.name, item.typeID, rawDataRows, rawDataDim))
		loadFields = append(loadFields, item.name)
	}
	return rawDataCampaign{name: "vector", schema: &schemapb.CollectionSchema{Fields: fields}, fields: data, loadFields: loadFields, search: true}
}

func newStructArrayCampaign() rawDataCampaign {
	children := []*schemapb.FieldSchema{{
		Name: "ints", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
	}}
	for _, item := range []struct {
		name   string
		typeID schemapb.DataType
	}{
		{"binary_vectors", schemapb.DataType_BinaryVector},
		{"float_vectors", schemapb.DataType_FloatVector},
		{"float16_vectors", schemapb.DataType_Float16Vector},
		{"bfloat16_vectors", schemapb.DataType_BFloat16Vector},
		{"int8_vectors", schemapb.DataType_Int8Vector},
	} {
		children = append(children, &schemapb.FieldSchema{
			Name: item.name, DataType: schemapb.DataType_ArrayOfVector, ElementType: item.typeID,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(rawDataDim)}, {Key: common.MaxCapacityKey, Value: "100"}},
		})
	}
	structField := &schemapb.StructArrayFieldSchema{Name: "structs", Fields: children}
	regularFields := []*schemapb.FieldSchema{{Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}, vectorSchema("struct_helper", schemapb.DataType_FloatVector, rawDataDim)}
	schema := &schemapb.CollectionSchema{
		Fields:            regularFields,
		StructArrayFields: []*schemapb.StructArrayFieldSchema{structField},
	}
	structChildren := []*schemapb.FieldData{deterministicArrayField("ints", schemapb.DataType_Int32, rawDataRows)}
	structChildren[0].FieldId = children[0].GetFieldID()
	for _, child := range children[1:] {
		structChildren = append(structChildren, deterministicVectorArrayField(child.GetName(), child.GetFieldID(), child.GetElementType(), rawDataRows, rawDataDim))
	}
	structData := &schemapb.FieldData{
		Type: schemapb.DataType_ArrayOfStruct, FieldName: structField.GetName(), FieldId: structField.GetFieldID(),
		Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: structChildren}},
	}
	return rawDataCampaign{
		name: "struct_array", schema: schema,
		fields:     []*schemapb.FieldData{testutils.NewInt64FieldData(fixturePrimaryKey, rawDataRows), deterministicFloatVectors("struct_helper", rawDataRows, rawDataDim), structData},
		loadFields: []string{fixturePrimaryKey, structField.GetName()},
	}
}

func vectorSchema(name string, dataType schemapb.DataType, dim int) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{Name: name, DataType: dataType}
	if dataType != schemapb.DataType_SparseFloatVector {
		field.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(dim)}}
	}
	return field
}

func deterministicFloatVectors(name string, rows, dim int) *schemapb.FieldData {
	values := make([]float32, rows*dim)
	for row := 0; row < rows; row++ {
		values[row*dim] = float32(row)
		for column := 1; column < dim; column++ {
			values[row*dim+column] = float32(column) / 100
		}
	}
	return testutils.NewFloatVectorFieldDataWithValue(name, values, dim)
}

func deterministicVectorField(name string, dataType schemapb.DataType, rows, dim int) *schemapb.FieldData {
	floatValues := make([]float32, rows*dim)
	for row := 0; row < rows; row++ {
		for column := 0; column < dim; column++ {
			floatValues[row*dim+column] = float32((row+1)*(column+1)) / 100
		}
	}
	switch dataType {
	case schemapb.DataType_BinaryVector:
		values := make([]byte, rows*dim/8)
		for row := 0; row < rows; row++ {
			values[row*dim/8] = byte(row)
		}
		return testutils.NewBinaryVectorFieldDataWithValue(name, values, dim)
	case schemapb.DataType_FloatVector:
		return deterministicFloatVectors(name, rows, dim)
	case schemapb.DataType_Float16Vector:
		return testutils.NewFloat16VectorFieldDataWithValue(name, typeutil.Float32ArrayToFloat16Bytes(floatValues), dim)
	case schemapb.DataType_BFloat16Vector:
		return testutils.NewBFloat16VectorFieldDataWithValue(name, typeutil.Float32ArrayToBFloat16Bytes(floatValues), dim)
	case schemapb.DataType_SparseFloatVector:
		contents := make([][]byte, rows)
		for row := 0; row < rows; row++ {
			contents[row] = typeutil.CreateSparseFloatRow([]uint32{uint32(row % dim)}, []float32{float32(row + 1)})
		}
		return &schemapb.FieldData{Type: dataType, FieldName: name, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: int64(dim), Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: int64(dim), Contents: contents}},
		}}}
	case schemapb.DataType_Int8Vector:
		values := make([]byte, rows*dim)
		for row := 0; row < rows; row++ {
			for column := 0; column < dim; column++ {
				values[row*dim+column] = byte((row + column) % 100)
			}
		}
		return testutils.NewInt8VectorFieldDataWithValue(name, values, dim)
	default:
		panic("unsupported deterministic vector type: " + dataType.String())
	}
}

func deterministicVectorArrayField(name string, fieldID int64, elementType schemapb.DataType, rows, dim int) *schemapb.FieldData {
	rowValues := make([]*schemapb.VectorField, rows)
	for row := 0; row < rows; row++ {
		floatValues := make([]float32, 2*dim)
		for i := range floatValues {
			floatValues[i] = float32((row+1)*(i+1)) / 100
		}
		value := &schemapb.VectorField{Dim: int64(dim)}
		switch elementType {
		case schemapb.DataType_BinaryVector:
			value.Data = &schemapb.VectorField_BinaryVector{BinaryVector: []byte{byte(row), byte(row + 1)}}
		case schemapb.DataType_FloatVector:
			value.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floatValues}}
		case schemapb.DataType_Float16Vector:
			value.Data = &schemapb.VectorField_Float16Vector{Float16Vector: typeutil.Float32ArrayToFloat16Bytes(floatValues)}
		case schemapb.DataType_BFloat16Vector:
			value.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: typeutil.Float32ArrayToBFloat16Bytes(floatValues)}
		case schemapb.DataType_Int8Vector:
			bytes := make([]byte, 2*dim)
			for i := range bytes {
				bytes[i] = byte((row + i) % 100)
			}
			value.Data = &schemapb.VectorField_Int8Vector{Int8Vector: bytes}
		}
		rowValues[row] = value
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_ArrayOfVector, FieldName: name, FieldId: fieldID,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(dim), Data: &schemapb.VectorField_VectorArray{
			VectorArray: &schemapb.VectorArray{Dim: int64(dim), ElementType: elementType, Data: rowValues},
		}}},
	}
}

func firstFloatVector(fields []*schemapb.FieldData, name string, dim int) []float32 {
	for _, field := range fields {
		if field.GetFieldName() == name {
			return append([]float32(nil), field.GetVectors().GetFloatVector().GetData()[:dim]...)
		}
	}
	return nil
}

func deterministicJSONField(name string, rows int, dynamic bool) *schemapb.FieldData {
	values := make([][]byte, rows)
	for i := range values {
		values[i] = []byte(fmt.Sprintf(`{"row":%d,"kind":"%s"}`, i, name))
	}
	field := testutils.NewJSONFieldDataWithValue(name, values)
	field.IsDynamic = dynamic
	return field
}

func deterministicArrayField(name string, elementType schemapb.DataType, rows int) *schemapb.FieldData {
	values := make([]*schemapb.ScalarField, rows)
	for row := range values {
		switch elementType {
		case schemapb.DataType_Bool:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{row%2 == 0, row%3 == 0}}}}
		case schemapb.DataType_Int8:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{int32(row % 100), int32((row + 1) % 100)}}}}
		case schemapb.DataType_Int16, schemapb.DataType_Int32:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{int32(row), int32(row + 1)}}}}
		case schemapb.DataType_Int64:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(row), int64(row + 1)}}}}
		case schemapb.DataType_Float:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{float32(row), float32(row) + .5}}}}
		case schemapb.DataType_Double:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{float64(row), float64(row) + .5}}}}
		case schemapb.DataType_VarChar:
			values[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{fmt.Sprintf("row-%d", row), "tail"}}}}
		}
	}
	return &schemapb.FieldData{Type: schemapb.DataType_Array, FieldName: name, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: values, ElementType: elementType}}}}}
}

func fieldDataByName(fields []*schemapb.FieldData) map[string]*schemapb.FieldData {
	result := make(map[string]*schemapb.FieldData, len(fields))
	for _, field := range fields {
		result[field.GetFieldName()] = field
	}
	return result
}
