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

package importv2

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// The element-null import suites import a struct with element-level nulls into
// one collection (B) and insert the same logical rows into a twin collection
// (A), then require the same query and search results from both, and the
// query results to match the model the rows were built from. The struct holds
// a legacy Int64 sub-field, an element-nullable VarChar, a nested
// Array<Array<Int64>> with nullable inner arrays and leaves, an
// element-nullable FloatVector and a non-nullable FloatVector.
const (
	enStruct = "clips"
	enDim    = 8
	enTopK   = 10
	// enIDThreshold splits the legacy id values (pk*10 + element index).
	enIDThreshold = 100
)

// enBaseChildren are the sub-fields of the collections at creation, in schema order.
var enBaseChildren = []string{"id", "label", "tags", "nvec", "vec"}

// enElem is one struct element. A nil pointer or slice is a null element value;
// a nil leaf in tags is a null leaf.
type enElem struct {
	id    int64
	label *string
	tags  *[]*int64
	nvec  []float32
	vec   []float32
	c     *int64 // only set in batches written for the added sub-field c
}

type enRow struct {
	pk    int64
	emb   []float32
	null  bool // the whole struct is null
	elems []enElem
}

func enVector(seed int64) []float32 {
	r := rand.New(rand.NewSource(seed))
	v := make([]float32, enDim)
	for d := range v {
		v[d] = r.Float32()
	}
	return v
}

// enBatch builds count rows starting at pk start. Every sixth row has a null
// struct, one an empty struct array and one only null element values; the
// others have 1..4 elements with null labels, null inner arrays, empty inner
// arrays, null leaves and null vectors at different phases. withC sets the
// added sub-field c on every other element.
func enBatch(start int64, count int, withC bool) []enRow {
	rows := make([]enRow, 0, count)
	for pk := start; pk < start+int64(count); pk++ {
		row := enRow{pk: pk, emb: enVector(pk * 1000)}
		switch pk % 6 {
		case 5:
			row.null = true
		case 2:
			row.elems = []enElem{}
		default:
			allNull := pk%6 == 4
			n := int(pk%4) + 1
			for j := 0; j < n; j++ {
				q := int(pk) + j
				e := enElem{id: pk*10 + int64(j), vec: enVector(pk*1000 + int64(j)*10 + 1)}
				if !allNull && q%3 != 1 {
					label := fmt.Sprintf("l%d_%d", pk, j)
					e.label = &label
				}
				if !allNull && q%4 != 3 {
					leaves := make([]*int64, 0, q%3)
					for k := 0; k < q%3; k++ {
						if (q+k)%3 == 2 {
							leaves = append(leaves, nil)
							continue
						}
						v := pk*100 + int64(j*10+k)
						leaves = append(leaves, &v)
					}
					e.tags = &leaves
				}
				if !allNull && q%3 != 2 {
					e.nvec = enVector(pk*1000 + int64(j)*10 + 2)
				}
				if withC && q%2 == 0 {
					c := pk*1000 + int64(j)
					e.c = &c
				}
				row.elems = append(row.elems, e)
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// value returns the element value of one sub-field in the shape enDecodeChild
// produces from query output.
func (e enElem) value(child string) any {
	switch child {
	case "id":
		return e.id
	case "label":
		if e.label == nil {
			return nil
		}
		return *e.label
	case "c":
		if e.c == nil {
			return nil
		}
		return *e.c
	case "tags":
		if e.tags == nil {
			return nil
		}
		leaves := make([]any, 0, len(*e.tags))
		for _, leaf := range *e.tags {
			if leaf == nil {
				leaves = append(leaves, nil)
			} else {
				leaves = append(leaves, *leaf)
			}
		}
		return leaves
	case "nvec":
		if e.nvec == nil {
			return nil
		}
		return e.nvec
	case "vec":
		return e.vec
	}
	panic("unknown sub-field " + child)
}

func enContains(children []string, child string) bool {
	for _, c := range children {
		if c == child {
			return true
		}
	}
	return false
}

func enKV(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

// enSubField returns the schema of one sub-field with a fixed local field ID.
// Struct and sub-fields are nullable so that a struct row can be null.
func enSubField(child string) *schemapb.FieldSchema {
	capacity := enKV(common.MaxCapacityKey, "10")
	switch child {
	case "id":
		return &schemapb.FieldSchema{
			FieldID: 103, Name: child, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
			Nullable: true, TypeParams: []*commonpb.KeyValuePair{capacity},
		}
	case "label":
		return &schemapb.FieldSchema{
			FieldID: 104, Name: child, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar,
			Nullable: true, ElementNullable: true,
			TypeParams: []*commonpb.KeyValuePair{capacity, enKV(common.MaxLengthKey, "32")},
		}
	case "tags":
		return &schemapb.FieldSchema{
			FieldID: 105, Name: child, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
			Nullable: true, ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{capacity},
			TypeSchema: &schemapb.TypeSchema{
				Nullable:   true,
				TypeParams: []*commonpb.KeyValuePair{enKV(common.MaxCapacityKey, "10")},
				Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
					Nullable:   true,
					TypeParams: []*commonpb.KeyValuePair{enKV(common.MaxCapacityKey, "4")},
					Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
						Nullable: true,
						Kind:     &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64},
					}},
				}},
			},
		}
	case "nvec":
		return &schemapb.FieldSchema{
			FieldID: 106, Name: child, DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
			Nullable: true, ElementNullable: true,
			TypeParams: []*commonpb.KeyValuePair{capacity, enKV(common.DimKey, strconv.Itoa(enDim))},
		}
	case "vec":
		return &schemapb.FieldSchema{
			FieldID: 107, Name: child, DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
			Nullable: true, TypeParams: []*commonpb.KeyValuePair{capacity, enKV(common.DimKey, strconv.Itoa(enDim))},
		}
	case "c":
		return &schemapb.FieldSchema{
			FieldID: 108, Name: child, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
			Nullable: true, ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{capacity},
		}
	}
	panic("unknown sub-field " + child)
}

// enSchema builds the collection schema, or the schema a file is generated
// from, with the given sub-fields.
func enSchema(name string, children []string) *schemapb.CollectionSchema {
	subFields := make([]*schemapb.FieldSchema, 0, len(children))
	for _, child := range children {
		subFields = append(subFields, enSubField(child))
	}
	return &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{enKV(common.DimKey, strconv.Itoa(enDim))},
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 102, Name: enStruct, Nullable: true, Fields: subFields}},
	}
}

// enInsertChild builds the compact insert payload of one sub-field: a null
// struct row has no entry, element-nullable scalars and vectors carry only
// their valid values, a nested row carries only its non-null inner arrays and
// each inner array only its valid leaves.
func enInsertChild(child string, rows []enRow) *schemapb.FieldData {
	var scalars []*schemapb.ScalarField
	var vectors []*schemapb.VectorField
	structValid := make([]bool, 0, len(rows))
	for _, row := range rows {
		structValid = append(structValid, !row.null)
		if row.null {
			continue
		}
		switch child {
		case "id":
			values := make([]int64, 0, len(row.elems))
			for _, e := range row.elems {
				values = append(values, e.id)
			}
			scalars = append(scalars, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}})
		case "label":
			var values []string
			var valid []bool
			for _, e := range row.elems {
				valid = append(valid, e.label != nil)
				if e.label != nil {
					values = append(values, *e.label)
				}
			}
			scalars = append(scalars, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}, ValidData: valid,
			})
		case "c":
			var values []int64
			var valid []bool
			for _, e := range row.elems {
				valid = append(valid, e.c != nil)
				if e.c != nil {
					values = append(values, *e.c)
				}
			}
			scalars = append(scalars, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}, ValidData: valid,
			})
		case "tags":
			var inner []*schemapb.ScalarField
			var valid []bool
			for _, e := range row.elems {
				valid = append(valid, e.tags != nil)
				if e.tags == nil {
					continue
				}
				var leaves []int64
				var leafValid []bool
				for _, leaf := range *e.tags {
					leafValid = append(leafValid, leaf != nil)
					if leaf != nil {
						leaves = append(leaves, *leaf)
					}
				}
				inner = append(inner, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: leaves}}, ValidData: leafValid,
				})
			}
			scalars = append(scalars, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_Int64, Data: inner,
				}},
				ValidData: valid,
			})
		case "nvec":
			var data []float32
			var valid []bool
			for _, e := range row.elems {
				valid = append(valid, e.nvec != nil)
				data = append(data, e.nvec...)
			}
			vectors = append(vectors, &schemapb.VectorField{
				Dim: enDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}}, ValidData: valid,
			})
		case "vec":
			var data []float32
			for _, e := range row.elems {
				data = append(data, e.vec...)
			}
			vectors = append(vectors, &schemapb.VectorField{
				Dim: enDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}},
			})
		default:
			panic("unknown sub-field " + child)
		}
	}
	var field *schemapb.FieldData
	if child == "nvec" || child == "vec" {
		field = &schemapb.FieldData{
			FieldName: child, Type: schemapb.DataType_ArrayOfVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: enDim,
				Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
					Dim: enDim, ElementType: schemapb.DataType_FloatVector, Data: vectors,
				}},
			}},
		}
	} else {
		field = &schemapb.FieldData{
			FieldName: child, Type: schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: scalars}},
			}},
		}
	}
	typeutil.SetFieldDataValidData(field, structValid)
	return field
}

// enImportRow builds one non-null sub-field row in the layout import readers
// produce: element-nullable scalars dense with a zero at null slots and
// full-length validity, nested rows with one leaf-typed child per element (a
// null inner array is an empty leaf) and dense leaves, element-nullable vectors
// with only the valid vectors.
func enImportRow(child string, row enRow) any {
	n := len(row.elems)
	switch child {
	case "id":
		values := make([]int64, 0, n)
		for _, e := range row.elems {
			values = append(values, e.id)
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}
	case "label":
		values := make([]string, 0, n)
		valid := make([]bool, 0, n)
		for _, e := range row.elems {
			valid = append(valid, e.label != nil)
			if e.label != nil {
				values = append(values, *e.label)
			} else {
				values = append(values, "")
			}
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}, ValidData: valid}
	case "c":
		values := make([]int64, 0, n)
		valid := make([]bool, 0, n)
		for _, e := range row.elems {
			valid = append(valid, e.c != nil)
			if e.c != nil {
				values = append(values, *e.c)
			} else {
				values = append(values, 0)
			}
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}, ValidData: valid}
	case "tags":
		children := make([]*schemapb.ScalarField, 0, n)
		valid := make([]bool, 0, n)
		for _, e := range row.elems {
			valid = append(valid, e.tags != nil)
			if e.tags == nil {
				children = append(children, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{}}}})
				continue
			}
			leaves := make([]int64, 0, len(*e.tags))
			leafValid := make([]bool, 0, len(*e.tags))
			for _, leaf := range *e.tags {
				leafValid = append(leafValid, leaf != nil)
				if leaf != nil {
					leaves = append(leaves, *leaf)
				} else {
					leaves = append(leaves, 0)
				}
			}
			children = append(children, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: leaves}}, ValidData: leafValid,
			})
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_Int64, Data: children,
			}},
			ValidData: valid,
		}
	case "nvec":
		data := make([]float32, 0, n*enDim)
		valid := make([]bool, 0, n)
		for _, e := range row.elems {
			valid = append(valid, e.nvec != nil)
			data = append(data, e.nvec...)
		}
		return &schemapb.VectorField{
			Dim: enDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}}, ValidData: valid,
		}
	case "vec":
		data := make([]float32, 0, n*enDim)
		for _, e := range row.elems {
			data = append(data, e.vec...)
		}
		return &schemapb.VectorField{Dim: enDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}}}
	}
	panic("unknown sub-field " + child)
}

// enInsertData builds the InsertData the file writers of util_test.go turn
// into a JSON, CSV or Parquet file. fileSchema decides which sub-fields the
// file carries.
func (s *importTestBase) enInsertData(fileSchema *schemapb.CollectionSchema, rows []enRow) *storage.InsertData {
	insertData, err := storage.NewInsertData(fileSchema)
	s.Require().NoError(err)
	for _, row := range rows {
		s.Require().NoError(insertData.Data[100].AppendRow(row.pk))
		s.Require().NoError(insertData.Data[101].AppendRow(row.emb))
		for _, subField := range fileSchema.GetStructArrayFields()[0].GetFields() {
			var value any
			if !row.null {
				value = enImportRow(subField.GetName(), row)
			}
			s.Require().NoError(insertData.Data[subField.GetFieldID()].AppendRow(value))
		}
	}
	return insertData
}

func (s *importTestBase) enCreate(ctx context.Context, schema *schemapb.CollectionSchema) {
	data, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: schema.GetName(), Schema: data, ShardsNum: 1,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
}

func enVectorFields() []string {
	return []string{
		"embedding",
		typeutil.ConcatStructFieldName(enStruct, "vec"),
		typeutil.ConcatStructFieldName(enStruct, "nvec"),
	}
}

// enCreateIndexes indexes the three vector fields. The element-nullable vector
// sub-field only accepts element-level metrics and must be indexed before the
// collection can be loaded.
func (s *importTestBase) enCreateIndexes(ctx context.Context, name string) {
	metrics := []string{metric.L2, metric.MaxSim, metric.L2}
	for i, field := range enVectorFields() {
		status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: name, FieldName: field, IndexName: fmt.Sprintf("idx_%d", i),
			ExtraParams: integration.ConstructIndexParam(enDim, integration.IndexHNSW, metrics[i]),
		})
		s.Require().NoError(merr.CheckRPCCall(status, err), field)
	}
}

func (s *importTestBase) enLoad(ctx context.Context, name string) {
	for _, field := range enVectorFields() {
		s.WaitForIndexBuilt(ctx, name, field)
	}
	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
}

func (s *importTestBase) enRelease(ctx context.Context, name string) {
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
}

func (s *importTestBase) enInsert(ctx context.Context, name string, rows []enRow, children []string) {
	pks := make([]int64, 0, len(rows))
	embeddings := make([]float32, 0, len(rows)*enDim)
	for _, row := range rows {
		pks = append(pks, row.pk)
		embeddings = append(embeddings, row.emb...)
	}
	structChildren := make([]*schemapb.FieldData, 0, len(children))
	for _, child := range children {
		structChildren = append(structChildren, enInsertChild(child, rows))
	}
	result, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, NumRows: uint32(len(rows)), HashKeys: integration.GenerateHashKeys(len(rows)),
		FieldsData: []*schemapb.FieldData{
			{
				FieldName: "pk", Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
				}},
			},
			{
				FieldName: "embedding", Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: enDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: embeddings}},
				}},
			},
			{
				FieldName: enStruct, Type: schemapb.DataType_ArrayOfStruct,
				Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: structChildren}},
			},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.Require().Equal(int64(len(rows)), result.GetInsertCnt())
}

func (s *importTestBase) enImport(ctx context.Context, name string, fileType importutilv2.FileType, fileSchema *schemapb.CollectionSchema, rows []enRow) {
	insertData := s.enInsertData(fileSchema, rows)
	var filePath string
	options := []*commonpb.KeyValuePair{}
	switch fileType {
	case importutilv2.FileTypeJSON:
		filePath = writeJSONFile(s.T(), s.Cluster, fileSchema, insertData)
	case importutilv2.FileTypeCsv:
		var sep rune
		filePath, sep = writeCSVFile(s.T(), s.Cluster, fileSchema, insertData)
		options = append(options, enKV("sep", string(sep)))
	case importutilv2.FileTypeParquet:
		var err error
		filePath, err = writeParquetFile(s.Cluster, fileSchema, insertData)
		s.Require().NoError(err)
	default:
		s.FailNow("unsupported file type " + fileType.String())
	}
	resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: name,
		Files:          []*internalpb.ImportFile{{Paths: []string{filePath}}},
		Options:        options,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	s.Require().NoError(WaitForImportDone(ctx, s.Cluster, resp.GetJobID()))
}

func (s *importTestBase) enFlush(ctx context.Context, name string) {
	resp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	ids := resp.GetFlushCollSegIDs()[name].GetData()
	s.Require().NotEmpty(ids)
	s.WaitForFlush(ctx, ids, resp.GetCollFlushTs()[name], "", name)
}

func (s *importTestBase) enCompact(ctx context.Context, name string) {
	describe, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(describe.GetStatus(), err))
	resp, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID: describe.GetCollectionID(), MajorCompaction: true,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	s.Require().Eventually(func() bool {
		state, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{CompactionID: resp.GetCompactionID()})
		return err == nil && merr.Ok(state.GetStatus()) && state.GetState() == commonpb.CompactionState_Completed
	}, 5*time.Minute, 2*time.Second)
}

func (s *importTestBase) enSchemaVersion(ctx context.Context, name string) int32 {
	resp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	return resp.GetSchema().GetVersion()
}

func (s *importTestBase) enAlter(ctx context.Context, name string, action *milvuspb.AlterCollectionSchemaRequest_Action) {
	before := s.enSchemaVersion(ctx, name)
	resp, err := s.Cluster.MilvusClient.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		CollectionName: name, Action: action,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetAlterStatus(), err))
	s.Require().Eventually(func() bool {
		resp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
		return err == nil && merr.Ok(resp.GetStatus()) && resp.GetSchema().GetVersion() > before
	}, 30*time.Second, 100*time.Millisecond)
}

func (s *importTestBase) enAddSubField(ctx context.Context, name, child string) {
	field := enSubField(child)
	field.FieldID = 0
	s.enAlter(ctx, name, &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
		AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
			FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{StructPath: enStruct, FieldSchema: field}},
		},
	}})
}

func (s *importTestBase) enDropSubField(ctx context.Context, name, child string) {
	s.enAlter(ctx, name, &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
		DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
			Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{
				FieldName: typeutil.ConcatStructFieldName(enStruct, child),
			},
		},
	}})
}

// enDecodeScalarRow reads one Array row of the query output contract: element
// validity in valid_data, dense payload, and for Array<Array<T>> one leaf row
// per element.
func enDecodeScalarRow(row *schemapb.ScalarField) []any {
	valid := row.GetValidData()
	isNull := func(k int) bool { return len(valid) > 0 && (k >= len(valid) || !valid[k]) }
	switch data := row.GetData().(type) {
	case *schemapb.ScalarField_LongData:
		out := make([]any, 0, len(data.LongData.GetData()))
		for k, v := range data.LongData.GetData() {
			if isNull(k) {
				out = append(out, nil)
			} else {
				out = append(out, v)
			}
		}
		return out
	case *schemapb.ScalarField_StringData:
		out := make([]any, 0, len(data.StringData.GetData()))
		for k, v := range data.StringData.GetData() {
			if isNull(k) {
				out = append(out, nil)
			} else {
				out = append(out, v)
			}
		}
		return out
	case *schemapb.ScalarField_ArrayData:
		out := make([]any, 0, len(data.ArrayData.GetData()))
		for k, child := range data.ArrayData.GetData() {
			if isNull(k) {
				out = append(out, nil)
			} else {
				out = append(out, enDecodeScalarRow(child))
			}
		}
		return out
	case nil:
		// An empty row may carry no payload at all.
		return make([]any, len(valid))
	default:
		panic(fmt.Sprintf("unexpected scalar payload %T", data))
	}
}

// enDecodeVectorRow reads one ArrayOfVector row: a compact payload of the valid
// vectors with logical-length valid_data for an element-nullable sub-field, the
// full payload otherwise.
func enDecodeVectorRow(row *schemapb.VectorField, dim int) []any {
	data := row.GetFloatVector().GetData()
	valid := row.GetValidData()
	if len(valid) == 0 {
		out := make([]any, 0, len(data)/dim)
		for p := 0; p+dim <= len(data); p += dim {
			out = append(out, data[p:p+dim])
		}
		return out
	}
	out := make([]any, 0, len(valid))
	p := 0
	for _, v := range valid {
		if !v || p+dim > len(data) {
			out = append(out, nil)
			continue
		}
		out = append(out, data[p:p+dim])
		p += dim
	}
	return out
}

// enDecodeRows turns query output into pk -> {"embedding": vector, sub-field:
// row value}; a null struct row gives nil for every sub-field.
func (s *importTestBase) enDecodeRows(fields []*schemapb.FieldData) map[int64]map[string]any {
	var pks []int64
	var embeddings []float32
	var structField *schemapb.FieldData
	for _, field := range fields {
		switch field.GetFieldName() {
		case "pk":
			pks = field.GetScalars().GetLongData().GetData()
		case "embedding":
			embeddings = field.GetVectors().GetFloatVector().GetData()
		case enStruct:
			structField = field
		}
	}
	s.Require().NotNil(structField, "struct field missing from query output")
	s.Require().Len(embeddings, len(pks)*enDim)
	out := make(map[int64]map[string]any, len(pks))
	for i, pk := range pks {
		values := map[string]any{"embedding": embeddings[i*enDim : (i+1)*enDim]}
		for _, child := range structField.GetStructArrays().GetFields() {
			name, err := typeutil.ExtractStructFieldName(child.GetFieldName())
			s.Require().NoError(err)
			if valid := child.GetValidData(); len(valid) > 0 && !valid[i] {
				values[name] = nil
				continue
			}
			if scalars := child.GetScalars(); scalars != nil {
				rows := scalars.GetArrayData().GetData()
				s.Require().Greater(len(rows), i, "sub-field %s rows", name)
				values[name] = enDecodeScalarRow(rows[i])
				continue
			}
			vectors := child.GetVectors()
			s.Require().NotNil(vectors, "sub-field %s has no payload", name)
			rows := vectors.GetVectorArray().GetData()
			s.Require().Greater(len(rows), i, "sub-field %s rows", name)
			values[name] = enDecodeVectorRow(rows[i], enDim)
		}
		out[pk] = values
	}
	return out
}

func enExpectedRows(rows []enRow, children []string) map[int64]map[string]any {
	out := make(map[int64]map[string]any, len(rows))
	for _, row := range rows {
		values := map[string]any{"embedding": row.emb}
		for _, child := range children {
			if row.null {
				values[child] = nil
				continue
			}
			elems := make([]any, 0, len(row.elems))
			for _, e := range row.elems {
				elems = append(elems, e.value(child))
			}
			values[child] = elems
		}
		out[row.pk] = values
	}
	return out
}

func (s *importTestBase) enQueryAll(ctx context.Context, name string) map[int64]map[string]any {
	resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: name, Expr: "pk >= 0", OutputFields: []string{"pk", "embedding", enStruct},
	})
	s.Require().NoError(merr.CheckRPCCall(resp, err))
	return s.enDecodeRows(resp.GetFieldsData())
}

// enQueryHits returns pk -> element_indices (nil for a row-level query).
func (s *importTestBase) enQueryHits(ctx context.Context, name, expr string) map[int64][]int64 {
	resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: name, Expr: expr, OutputFields: []string{"pk"},
	})
	s.Require().NoError(merr.CheckRPCCall(resp, err), expr)
	var pks []int64
	for _, field := range resp.GetFieldsData() {
		if field.GetFieldName() == "pk" {
			pks = field.GetScalars().GetLongData().GetData()
		}
	}
	indices := resp.GetElementIndices()
	out := make(map[int64][]int64, len(pks))
	for i, pk := range pks {
		var idx []int64
		if i < len(indices) && len(indices[i].GetIndices().GetData()) > 0 {
			idx = indices[i].GetIndices().GetData()
		}
		out[pk] = idx
	}
	return out
}

// enFilter is a query filter with its expected result on the model: row-level
// (row != nil) or element-level (elem != nil).
type enFilter struct {
	expr  string
	needs []string // sub-fields the expression reads
	row   func(enRow) bool
	elem  func(enElem) bool
}

func (f enFilter) expected(rows []enRow) map[int64][]int64 {
	out := make(map[int64][]int64)
	for _, r := range rows {
		if r.null {
			continue
		}
		if f.row != nil {
			if f.row(r) {
				out[r.pk] = nil
			}
			continue
		}
		var idx []int64
		for j, e := range r.elems {
			if f.elem(e) {
				idx = append(idx, int64(j))
			}
		}
		if len(idx) > 0 {
			out[r.pk] = idx
		}
	}
	return out
}

func (s *importTestBase) enFilters(model []enRow) []enFilter {
	var label string
	var tag int64
	foundLabel, foundTag := false, false
	for _, r := range model {
		for j, e := range r.elems {
			if !foundLabel && e.label != nil {
				label, foundLabel = *e.label, true
			}
			if !foundTag && j == 0 && e.tags != nil {
				for _, leaf := range *e.tags {
					if leaf != nil {
						tag, foundTag = *leaf, true
						break
					}
				}
			}
		}
	}
	s.Require().True(foundLabel && foundTag, "model has no non-null label or leading tag")
	anyElem := func(pred func(enElem) bool) func(enRow) bool {
		return func(r enRow) bool {
			for _, e := range r.elems {
				if pred(e) {
					return true
				}
			}
			return false
		}
	}
	return []enFilter{
		{
			expr: fmt.Sprintf("element_filter(%s, $[label] IS NULL)", enStruct), needs: []string{"label"},
			elem: func(e enElem) bool { return e.label == nil },
		},
		{
			expr:  fmt.Sprintf("element_filter(%s, $[id] > %d && $[label] IS NOT NULL)", enStruct, enIDThreshold),
			needs: []string{"id", "label"},
			elem:  func(e enElem) bool { return e.id > enIDThreshold && e.label != nil },
		},
		{
			expr: fmt.Sprintf("element_filter(%s, $[c] IS NOT NULL)", enStruct), needs: []string{"c"},
			elem: func(e enElem) bool { return e.c != nil },
		},
		{
			expr: fmt.Sprintf("MATCH_ANY(%s, $[label] IS NULL)", enStruct), needs: []string{"label"},
			row: anyElem(func(e enElem) bool { return e.label == nil }),
		},
		{
			expr: fmt.Sprintf("array_contains(%s[label], %q)", enStruct, label), needs: []string{"label"},
			row: anyElem(func(e enElem) bool { return e.label != nil && *e.label == label }),
		},
		{
			expr: fmt.Sprintf("array_contains(%s[0][tags], %d)", enStruct, tag), needs: []string{"tags"},
			row: func(r enRow) bool {
				if len(r.elems) == 0 || r.elems[0].tags == nil {
					return false
				}
				for _, leaf := range *r.elems[0].tags {
					if leaf != nil && *leaf == tag {
						return true
					}
				}
				return false
			},
		},
		{
			expr: fmt.Sprintf("%s[0][id] > %d", enStruct, enIDThreshold), needs: []string{"id"},
			row: func(r enRow) bool { return len(r.elems) > 0 && r.elems[0].id > enIDThreshold },
		},
	}
}

type enHit struct {
	pk, offset int64
	score      float32
}

func (s *importTestBase) enSearch(ctx context.Context, request *milvuspb.SearchRequest) []enHit {
	resp, err := s.Cluster.MilvusClient.Search(ctx, request)
	s.Require().NoError(merr.CheckRPCCall(resp, err))
	data := resp.GetResults()
	ids := data.GetIds().GetIntId().GetData()
	offsets := data.GetElementIndices().GetData()
	hits := make([]enHit, len(ids))
	for i, pk := range ids {
		hits[i] = enHit{pk: pk, offset: -1, score: data.GetScores()[i]}
		if i < len(offsets) {
			hits[i].offset = offsets[i]
		}
	}
	return hits
}

// enCompareSearch runs the same request (same query vectors) on both
// collections and requires the same hits.
func (s *importTestBase) enCompareSearch(ctx context.Context, a, b string, request *milvuspb.SearchRequest, msg string) {
	request.CollectionName = a
	other := proto.Clone(request).(*milvuspb.SearchRequest)
	other.CollectionName = b
	want := s.enSearch(ctx, request)
	got := s.enSearch(ctx, other)
	s.Require().NotEmpty(want, msg)
	s.Require().Len(got, len(want), msg)
	for i := range want {
		s.Require().Equal(want[i].pk, got[i].pk, "%s: hit %d pk", msg, i)
		s.Require().Equal(want[i].offset, got[i].offset, "%s: hit %d element offset", msg, i)
		s.Require().InDelta(want[i].score, got[i].score, 1e-4, "%s: hit %d score", msg, i)
	}
}

// enCompare requires the imported collection b to answer like the inserted
// collection a, and both to match the model, for the sub-fields in children.
func (s *importTestBase) enCompare(ctx context.Context, a, b string, model []enRow, children []string, phase string) {
	gotA := s.enQueryAll(ctx, a)
	gotB := s.enQueryAll(ctx, b)
	s.Require().Equal(gotA, gotB, "%s: query output of the imported collection differs from the inserted one", phase)
	s.Require().Equal(enExpectedRows(model, children), gotB, "%s: query output differs from the model", phase)

	for _, f := range s.enFilters(model) {
		usable := true
		for _, need := range f.needs {
			usable = usable && enContains(children, need)
		}
		if !usable {
			continue
		}
		msg := fmt.Sprintf("%s: %s", phase, f.expr)
		want := f.expected(model)
		s.Require().NotEmpty(want, msg)
		hitsA := s.enQueryHits(ctx, a, f.expr)
		hitsB := s.enQueryHits(ctx, b, f.expr)
		s.Require().Equal(hitsA, hitsB, msg)
		s.Require().Equal(want, hitsB, msg)
	}

	nvec := typeutil.ConcatStructFieldName(enStruct, "nvec")
	vec := typeutil.ConcatStructFieldName(enStruct, "vec")
	s.enCompareSearch(ctx, a, b, integration.ConstructElementLevelSearchRequest("", a, "", nvec,
		schemapb.DataType_FloatVector, nil, metric.L2, integration.GetSearchParams(integration.IndexHNSW, metric.L2),
		1, enDim, enTopK, -1), phase+": element-level search on "+nvec)
	s.enCompareSearch(ctx, a, b, integration.ConstructEmbeddingListSearchRequest("", a, "", vec,
		schemapb.DataType_FloatVector, nil, metric.MaxSim, integration.GetSearchParams(integration.IndexHNSW, metric.MaxSim),
		1, enDim, enTopK, -1), phase+": MAX_SIM search on "+vec)
	filter := fmt.Sprintf("MATCH_ANY(%s, $[label] IS NULL)", enStruct)
	s.enCompareSearch(ctx, a, b, integration.ConstructSearchRequest("", a, filter, "embedding",
		schemapb.DataType_FloatVector, nil, metric.L2, integration.GetSearchParams(integration.IndexHNSW, metric.L2),
		1, enDim, enTopK, -1), phase+": filtered search on embedding")
}

func enWithout(children []string, child string) []string {
	out := make([]string, 0, len(children))
	for _, c := range children {
		if c != child {
			out = append(out, c)
		}
	}
	return out
}

// runElementNullImport imports files of one format into collection b and
// inserts the same rows into collection a, then compares a and b after the
// import, after a compaction, after adding an element-nullable sub-field
// (files without it get N null elements, files with it carry values), after
// dropping it (files still carrying it are accepted and the key is ignored) and
// after dropping the first sub-field.
func (s *importTestBase) runElementNullImport(fileType importutilv2.FileType) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 30*time.Minute)
	defer cancel()
	suffix := funcutil.RandomString(8)
	a, b := "en_insert_"+suffix, "en_import_"+suffix
	children := append([]string{}, enBaseChildren...)
	for _, name := range []string{a, b} {
		s.enCreate(ctx, enSchema(name, children))
		s.enCreateIndexes(ctx, name)
	}

	var model []enRow
	write := func(rows []enRow, insertChildren, fileChildren []string) {
		s.enInsert(ctx, a, rows, insertChildren)
		s.enFlush(ctx, a)
		s.enImport(ctx, b, fileType, enSchema("", fileChildren), rows)
		model = append(model, rows...)
	}
	check := func(phase string) {
		s.enLoad(ctx, a)
		s.enLoad(ctx, b)
		s.enCompare(ctx, a, b, model, children, phase)
		s.enRelease(ctx, a)
		s.enRelease(ctx, b)
	}

	write(enBatch(1, 24, false), children, children)
	check("import")

	// Two segments per collection give the compaction real input.
	write(enBatch(101, 12, false), children, children)
	s.enCompact(ctx, a)
	s.enCompact(ctx, b)
	check("compaction")

	for _, name := range []string{a, b} {
		s.enAddSubField(ctx, name, "c")
	}
	// Rows written before the add and rows omitting c read back c as one null
	// per element; null struct rows stay null.
	write(enBatch(201, 12, false), children, children)
	children = append(append([]string{}, children...), "c")
	write(enBatch(301, 12, true), children, children)
	check("added sub-field")

	for _, name := range []string{a, b} {
		s.enDropSubField(ctx, name, "c")
	}
	withDropped := children
	children = enWithout(children, "c")
	// Insert rejects the dropped sub-field; import ignores its key or child.
	write(enBatch(401, 12, true), children, withDropped)
	check("dropped sub-field in files")

	for _, name := range []string{a, b} {
		s.enDropSubField(ctx, name, "id")
	}
	children = enWithout(children, "id")
	write(enBatch(501, 12, false), children, children)
	check("dropped first sub-field")
}

// runElementNullImportRejections checks the import paths that still cannot
// fill element-nullable or nested Array sub-fields: binlog (backup) imports,
// and NumPy files for any collection with a struct field.
func (s *importTestBase) runElementNullImportRejections() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 5*time.Minute)
	defer cancel()
	name := "en_reject_" + funcutil.RandomString(8)
	s.enCreate(ctx, enSchema(name, enBaseChildren))

	resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: name, PartitionName: "_default",
		Files:   []*internalpb.ImportFile{{Paths: []string{"backup/insert_log/", "backup/delta_log/"}}},
		Options: []*commonpb.KeyValuePair{enKV(importutilv2.BackupFlag, "true")},
	})
	s.Require().NoError(err)
	err = merr.Error(resp.GetStatus())
	s.Require().ErrorIs(err, merr.ErrParameterInvalid)
	s.Require().ErrorContains(err,
		"binlog import (backup restore) into a collection with element-nullable or nested Array field "+enStruct+".label is not supported")

	// NumPy has no form for a struct field, so even a struct with only legacy
	// sub-fields is rejected.
	legacy := "en_reject_legacy_" + funcutil.RandomString(8)
	s.enCreate(ctx, enSchema(legacy, []string{"id", "vec"}))
	for _, collection := range []string{name, legacy} {
		resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
			CollectionName: collection,
			Files:          []*internalpb.ImportFile{{Paths: []string{"numpy/pk.npy", "numpy/embedding.npy"}}},
		})
		s.Require().NoError(err)
		err = merr.Error(resp.GetStatus())
		s.Require().ErrorIs(err, merr.ErrImportFailed, collection)
		s.Require().ErrorContains(err, "numpy import does not support struct field '"+enStruct+"'; use JSON, CSV or Parquet files", collection)
	}
}

type ElementNullImportSuite struct {
	importTestBase
}

func (s *ElementNullImportSuite) SetupSuite() {
	s.WithMilvusConfig("mq.type", "woodpecker")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportCheckIntervalHigh.Key, "0.1")
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.ImportScheduleInterval.Key, "0.1")
	s.importTestBase.SetupSuite()
}

func (s *ElementNullImportSuite) TestJSON() {
	s.runElementNullImport(importutilv2.FileTypeJSON)
}

func (s *ElementNullImportSuite) TestCSV() {
	s.runElementNullImport(importutilv2.FileTypeCsv)
}

func (s *ElementNullImportSuite) TestParquet() {
	s.runElementNullImport(importutilv2.FileTypeParquet)
}

func (s *ElementNullImportSuite) TestRejections() {
	s.runElementNullImportRejections()
}

func TestElementNullImport(t *testing.T) {
	suite.Run(t, new(ElementNullImportSuite))
}

// ElementNullImportV3Suite runs the same scenarios through the Import V3
// reshard chain, which shares the readers and the DataNode normalization.
type ElementNullImportV3Suite struct {
	importTestBase
}

func (s *ElementNullImportV3Suite) SetupSuite() {
	s.WithMilvusConfig("mq.type", "woodpecker")
	configureImportV3(&s.MiniClusterSuite)
	s.MiniClusterSuite.SetupSuite()
}

func (s *ElementNullImportV3Suite) TestJSONV3() {
	s.runElementNullImport(importutilv2.FileTypeJSON)
}

func (s *ElementNullImportV3Suite) TestCSVV3() {
	s.runElementNullImport(importutilv2.FileTypeCsv)
}

func (s *ElementNullImportV3Suite) TestParquetV3() {
	s.runElementNullImport(importutilv2.FileTypeParquet)
}

func TestElementNullImportV3(t *testing.T) {
	suite.Run(t, new(ElementNullImportV3Suite))
}
