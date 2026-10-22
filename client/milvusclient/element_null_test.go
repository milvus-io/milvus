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

package milvusclient

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

type ElementNullSuite struct {
	MockSuiteBase

	schema *entity.Schema
}

func (s *ElementNullSuite) SetupSuite() {
	s.MockSuiteBase.SetupSuite()
	s.schema = entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2)).
		WithField(entity.NewField().WithName("clips").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(4).
			WithStructSchema(entity.NewStructSchema().
				WithField(entity.NewField().WithName("score").WithDataType(entity.FieldTypeInt64)).
				WithField(entity.NewField().WithName("lang").WithDataType(entity.FieldTypeVarChar).WithMaxLength(8).WithNullable(true)).
				WithField(entity.NewField().WithName("tags").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).
					WithMaxCapacity(3).WithMaxLength(8).WithNullable(true).WithElementNullable(true))))
}

func (s *ElementNullSuite) idsOf(ids ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
}

func (s *ElementNullSuite) TestSearchElementOffsets() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("offsets per query", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries:     2,
				TopK:           2,
				Ids:            s.idsOf(1, 2, 3),
				Scores:         []float32{0.1, 0.2, 0.3},
				Topks:          []int64{2, 1},
				ElementIndices: &schemapb.LongArray{Data: []int64{0, 3, 1}},
			},
		}, nil).Once()

		results, err := s.client.Search(ctx, NewSearchOption(collName, 2, []entity.Vector{
			entity.FloatVector([]float32{1, 2}), entity.FloatVector([]float32{3, 4}),
		}).WithANNSField("clips[vec]"))
		s.Require().NoError(err)
		s.Require().Len(results, 2)
		s.NoError(results[0].Err)
		s.Equal([]int64{0, 3}, results[0].ElementOffsets)
		s.Equal([]int64{1}, results[1].ElementOffsets)
		s.Equal([]int64{3}, results[0].Slice(1, 2).ElementOffsets)
	})

	s.Run("no offsets for plain search", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       1,
				Ids:        s.idsOf(1),
				Scores:     []float32{0.1},
				Topks:      []int64{1},
			},
		}, nil).Once()

		results, err := s.client.Search(ctx, NewSearchOption(collName, 1, []entity.Vector{entity.FloatVector([]float32{1, 2})}))
		s.Require().NoError(err)
		s.Nil(results[0].ElementOffsets)
	})

	s.Run("offset count mismatch", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(&milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries:     1,
				TopK:           2,
				Ids:            s.idsOf(1, 2),
				Scores:         []float32{0.1, 0.2},
				Topks:          []int64{2},
				ElementIndices: &schemapb.LongArray{Data: []int64{0}},
			},
		}, nil).Once()

		results, err := s.client.Search(ctx, NewSearchOption(collName, 2, []entity.Vector{entity.FloatVector([]float32{1, 2})}))
		s.Require().NoError(err)
		s.Error(results[0].Err)
	})
}

func (s *ElementNullSuite) TestQueryElementOffsets() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("offsets per row", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
			Status:       merr.Success(),
			FieldsData:   []*schemapb.FieldData{s.getInt64FieldData("id", []int64{1, 2})},
			OutputFields: []string{"id"},
			ElementIndices: []*milvuspb.ElementIndices{
				{Indices: &schemapb.LongArray{Data: []int64{0, 2}}},
				{Indices: &schemapb.LongArray{Data: []int64{1}}},
			},
		}, nil).Once()

		rs, err := s.client.Query(ctx, NewQueryOption(collName).WithFilter(`element_filter(clips, $[score] > 1)`).WithOutputFields("id"))
		s.Require().NoError(err)
		s.Equal([][]int64{{0, 2}, {1}}, rs.QueryElementOffsets)
		s.Equal([][]int64{{1}}, rs.Slice(1, 2).QueryElementOffsets)
	})

	s.Run("row count mismatch", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
			Status:         merr.Success(),
			FieldsData:     []*schemapb.FieldData{s.getInt64FieldData("id", []int64{1, 2})},
			OutputFields:   []string{"id"},
			ElementIndices: []*milvuspb.ElementIndices{{Indices: &schemapb.LongArray{Data: []int64{0}}}},
		}, nil).Once()

		_, err := s.client.Query(ctx, NewQueryOption(collName).WithFilter(`element_filter(clips, $[score] > 1)`).WithOutputFields("id"))
		s.Error(err)
	})
}

func (s *ElementNullSuite) TestQueryIteratorElementOffsets() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("offsets follow cached batches", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
			Status:       merr.Success(),
			FieldsData:   []*schemapb.FieldData{s.getInt64FieldData("id", []int64{1, 2, 3})},
			OutputFields: []string{"id"},
			ElementIndices: []*milvuspb.ElementIndices{
				{Indices: &schemapb.LongArray{Data: []int64{0}}},
				{Indices: &schemapb.LongArray{Data: []int64{1, 2}}},
				{Indices: &schemapb.LongArray{Data: []int64{3}}},
			},
		}, nil).Once()

		iter, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collName).
			WithFilter(`element_filter(clips, $[score] > 1)`).
			WithOutputFields("id").
			WithBatchSize(2))
		s.Require().NoError(err)

		rs, err := iter.Next(ctx)
		s.Require().NoError(err)
		s.Equal(2, rs.ResultCount)
		s.Equal([][]int64{{0}, {1, 2}}, rs.QueryElementOffsets)

		rs, err = iter.Next(ctx)
		s.Require().NoError(err)
		s.Equal(1, rs.ResultCount)
		s.Equal([][]int64{{3}}, rs.QueryElementOffsets)
	})

	s.Run("offsets follow overall limit", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
			Status:       merr.Success(),
			FieldsData:   []*schemapb.FieldData{s.getInt64FieldData("id", []int64{1, 2})},
			OutputFields: []string{"id"},
			ElementIndices: []*milvuspb.ElementIndices{
				{Indices: &schemapb.LongArray{Data: []int64{0, 1}}},
				{Indices: &schemapb.LongArray{Data: []int64{2}}},
			},
		}, nil).Once()

		iter, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collName).
			WithFilter(`element_filter(clips, $[score] > 1)`).
			WithOutputFields("id").
			WithBatchSize(2).
			WithIteratorLimit(1))
		s.Require().NoError(err)

		rs, err := iter.Next(ctx)
		s.Require().NoError(err)
		s.Equal(1, rs.ResultCount)
		s.Equal([][]int64{{0, 1}}, rs.QueryElementOffsets)
	})

	s.Run("row count mismatch", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
			Status:         merr.Success(),
			FieldsData:     []*schemapb.FieldData{s.getInt64FieldData("id", []int64{1, 2})},
			OutputFields:   []string{"id"},
			ElementIndices: []*milvuspb.ElementIndices{{Indices: &schemapb.LongArray{Data: []int64{0}}}},
		}, nil).Once()

		_, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collName).
			WithFilter(`element_filter(clips, $[score] > 1)`).
			WithOutputFields("id").
			WithBatchSize(2))
		s.Error(err)
	})
}

func (s *ElementNullSuite) TestResultSetSliceOffsetsRange() {
	rs := ResultSet{
		ResultCount:         2,
		Fields:              DataSet{column.NewColumnInt64("id", []int64{1, 2})},
		QueryElementOffsets: [][]int64{{0, 2}, {1}},
	}
	sliced := rs.Slice(-1, 1)
	s.Equal(1, sliced.ResultCount)
	s.Equal(1, sliced.Fields[0].Len())
	s.Equal([][]int64{{0, 2}}, sliced.QueryElementOffsets)

	sliced = rs.Slice(1, 10)
	s.Equal(1, sliced.ResultCount)
	s.Equal([][]int64{{1}}, sliced.QueryElementOffsets)

	sliced = rs.Slice(3, 5)
	s.Equal(0, sliced.ResultCount)
	s.Nil(sliced.QueryElementOffsets)

	search := ResultSet{
		ResultCount:    3,
		IDs:            column.NewColumnInt64("id", []int64{1, 2, 3}),
		Fields:         DataSet{column.NewColumnInt64("id", []int64{1, 2, 3})},
		Scores:         []float32{0.3, 0.2, 0.1},
		ElementOffsets: []int64{4, 5, 6},
	}
	sliced = search.Slice(-1, 2)
	s.Equal(2, sliced.ResultCount)
	s.Equal(2, sliced.IDs.Len())
	s.Equal([]float32{0.3, 0.2}, sliced.Scores)
	s.Equal([]int64{4, 5}, sliced.ElementOffsets)

	sliced = search.Slice(2, -1)
	s.Equal(1, sliced.ResultCount)
	s.Equal([]float32{0.1}, sliced.Scores)
	s.Equal([]int64{6}, sliced.ElementOffsets)
}

func (s *ElementNullSuite) TestQueryElementNullOutput() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collName := fmt.Sprintf("coll_%s", s.randString(6))
	s.setupCache(collName, s.schema)
	// the struct output carries only score and lang, and lang has no element null in this page
	scoreData := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "score",
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64, Data: []*schemapb.ScalarField{
				{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}},
			}},
		}}},
	}
	langData := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "lang",
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_VarChar, Data: []*schemapb.ScalarField{
				{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"en"}}}, ValidData: []bool{true}},
			}},
		}}},
	}
	s.mock.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status: merr.Success(),
		FieldsData: []*schemapb.FieldData{
			s.getInt64FieldData("id", []int64{1}),
			{
				Type:      schemapb.DataType_ArrayOfStruct,
				FieldName: "clips",
				Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{
					Fields: []*schemapb.FieldData{scoreData, langData},
				}},
			},
		},
		OutputFields: []string{"id", "clips"},
	}, nil).Once()

	rs, err := s.client.Query(ctx, NewQueryOption(collName).WithOutputFields("id", "clips"))
	s.Require().NoError(err)
	clips := rs.GetColumn("clips").(column.StructArrayColumn)
	v, err := clips.Get(0)
	s.Require().NoError(err)
	row := v.(map[string]any)
	s.Equal([]int64{1}, row["score"])
	lang := "en"
	s.Equal([]*string{&lang}, row["lang"])
	for _, field := range clips.Fields() {
		elementCol := field.(column.ElementNullableColumn)
		s.Equal(field.Name() == "lang", elementCol.ElementNullable(), field.Name())
	}
}

func (s *ElementNullSuite) TestInsertElementNull() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkClips := func(fieldsData []*schemapb.FieldData) {
		var clips *schemapb.FieldData
		for _, fd := range fieldsData {
			if fd.GetFieldName() == "clips" {
				clips = fd
			}
		}
		s.Require().NotNil(clips)
		subs := map[string]*schemapb.FieldData{}
		for _, sub := range clips.GetStructArrays().GetFields() {
			subs[sub.GetFieldName()] = sub
		}
		lang := subs["lang"].GetScalars().GetArrayData().GetData()
		s.Require().Len(lang, 2)
		s.Equal([]string{"en"}, lang[0].GetStringData().GetData())
		s.Equal([]bool{true, false}, lang[0].GetValidData())
		s.Equal([]bool{false}, lang[1].GetValidData())

		tags := subs["tags"].GetScalars().GetArrayData()
		s.Equal(schemapb.DataType_Array, tags.GetElementType())
		s.Equal([]bool{true, false}, tags.GetData()[0].GetValidData())
		inner := tags.GetData()[0].GetArrayData().GetData()
		s.Require().Len(inner, 1)
		s.Equal([]string{"a"}, inner[0].GetStringData().GetData())
		s.Equal([]bool{true, false}, inner[0].GetValidData())
	}

	s.Run("row based", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Insert(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ir *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
			checkClips(ir.GetFieldsData())
			return &milvuspb.MutationResult{Status: merr.Success(), InsertCnt: 2, IDs: s.idsOf(1, 2)}, nil
		}).Once()

		en, a := "en", "a"
		rows := []any{
			map[string]any{"id": int64(1), "vector": []float32{1, 2}, "clips": map[string]any{
				"score": []int64{1, 2},
				"lang":  []*string{&en, nil},
				"tags":  [][]*string{{&a, nil}, nil},
			}},
			// lang and tags omitted: one null element each
			map[string]any{"id": int64(2), "vector": []float32{3, 4}, "clips": map[string]any{
				"score": []int64{3},
			}},
		}
		_, err := s.client.Insert(ctx, NewRowBasedInsertOption(collName, rows...))
		s.NoError(err)
	})

	s.Run("column based", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collName, s.schema)
		s.mock.EXPECT().Insert(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ir *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
			checkClips(ir.GetFieldsData())
			return &milvuspb.MutationResult{Status: merr.Success(), InsertCnt: 2, IDs: s.idsOf(1, 2)}, nil
		}).Once()

		lang, err := column.NewElementNullableColumnVarCharArray("lang", [][]string{{"en"}, {}}, [][]bool{{true, false}, {false}})
		s.Require().NoError(err)
		tags, err := column.NewElementNullableColumnVarCharArrayArray("tags",
			[][][]string{{{"a"}}, {}},
			[][]bool{{true, false}, {false}},
			[][][]bool{{{true, false}}, {}})
		s.Require().NoError(err)
		clips := column.NewColumnStructArray("clips", []column.Column{
			column.NewColumnInt64Array("score", [][]int64{{1, 2}, {3}}),
			lang,
			tags,
		})
		_, err = s.client.Insert(ctx, NewColumnBasedInsertOption(collName).
			WithInt64Column("id", []int64{1, 2}).
			WithFloatVectorColumn("vector", 2, [][]float32{{1, 2}, {3, 4}}).
			WithColumns(clips))
		s.NoError(err)
	})
}

func (s *ElementNullSuite) TestAddStructSubField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("sub-field path", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.AlterCollectionSchemaRequest) (*milvuspb.AlterCollectionSchemaResponse, error) {
			infos := req.GetAction().GetAddRequest().GetFieldInfos()
			s.Require().Len(infos, 1)
			s.Equal("clips", infos[0].GetStructPath())
			fieldSchema := infos[0].GetFieldSchema()
			s.Equal("clips[tags]", fieldSchema.GetName())
			s.Equal(schemapb.DataType_Array, fieldSchema.GetDataType())
			s.Equal(schemapb.DataType_Array, fieldSchema.GetElementType())
			s.True(fieldSchema.GetElementNullable())
			s.NotContains(entity.KvPairsMap(fieldSchema.GetTypeParams()), entity.TypeParamMaxCapacity)
			inner := fieldSchema.GetTypeSchema().GetArrayElement()
			s.Equal("3", entity.KvPairsMap(inner.GetTypeParams())[entity.TypeParamMaxCapacity])
			s.Equal(schemapb.DataType_Int64, inner.GetArrayElement().GetLeafType())
			return &milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil
		}).Once()

		field := entity.NewField().WithName("clips[tags]").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeInt64).WithMaxCapacity(3).WithNullable(true)
		s.NoError(s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field)))
	})

	s.Run("unimplemented does not fall back", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).
			Return(nil, grpcstatus.Error(codes.Unimplemented, "method not implemented")).Once()

		field := entity.NewField().WithName("clips[lang]").WithDataType(entity.FieldTypeVarChar).WithMaxLength(8).WithNullable(true)
		err := s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field))
		s.Error(err)
		s.Contains(err.Error(), "struct sub-field")
	})

	s.Run("invalid options", func() {
		for _, field := range []*entity.Field{
			entity.NewField().WithName("clips[lang]").WithDataType(entity.FieldTypeVarChar).WithMaxLength(8),
			entity.NewField().WithName("clips[]").WithDataType(entity.FieldTypeVarChar).WithNullable(true),
			entity.NewField().WithName("[lang]").WithDataType(entity.FieldTypeVarChar).WithNullable(true),
			entity.NewField().WithName("a[b][c]").WithDataType(entity.FieldTypeVarChar).WithNullable(true),
			entity.NewField().WithName("clips[lang").WithDataType(entity.FieldTypeVarChar).WithNullable(true),
		} {
			s.Error(NewAddCollectionFieldOption("coll", field).Validate(), field.Name)
		}
	})
}

func (s *ElementNullSuite) TestDropStructSubField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collName := fmt.Sprintf("coll_%s", s.randString(6))
	s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.AlterCollectionSchemaRequest) (*milvuspb.AlterCollectionSchemaResponse, error) {
		s.Equal("clips[lang]", req.GetAction().GetDropRequest().GetFieldName())
		return &milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil
	}).Once()
	s.NoError(s.client.DropCollectionField(ctx, NewDropCollectionFieldOption(collName, "clips[lang]")))
}

func TestElementNull(t *testing.T) {
	suite.Run(t, new(ElementNullSuite))
}
