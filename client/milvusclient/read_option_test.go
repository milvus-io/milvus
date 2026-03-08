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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type SearchOptionSuite struct {
	suite.Suite
}

type nonSupportData struct{}

func (d nonSupportData) Serialize() []byte {
	return []byte{}
}

func (d nonSupportData) Dim() int {
	return 0
}

func (d nonSupportData) FieldType() entity.FieldType {
	return entity.FieldType(0)
}

func (s *SearchOptionSuite) TestBasic() {
	collName := "search_opt_basic"

	topK := rand.Intn(100) + 1
	opt := NewSearchOption(collName, topK, []entity.Vector{entity.FloatVector([]float32{0.1, 0.2})})

	rerankerFunction := entity.NewFunction().WithName("time_decay").WithInputFields("timestamp").WithType(entity.FunctionTypeRerank).
		WithParam("reranker", "decay").
		WithParam("function", "gauss").
		WithParam("origin", 1754995249).
		WithParam("scale", 7*24*60*60).
		WithParam("offset", 24*60*60).
		WithParam("decay", 0.5)

	opt = opt.WithANNSField("test_field").WithOutputFields("ID", "Value").WithConsistencyLevel(entity.ClStrong).WithFilter("ID > 1000").WithGroupByField("group_field").WithGroupSize(10).WithStrictGroupSize(true).WithFunctionReranker(rerankerFunction)
	req, err := opt.Request()
	s.Require().NoError(err)

	s.Equal(collName, req.GetCollectionName())
	s.Equal("ID > 1000", req.GetDsl())
	s.ElementsMatch([]string{"ID", "Value"}, req.GetOutputFields())
	searchParams := entity.KvPairsMap(req.GetSearchParams())
	annField, ok := searchParams[spAnnsField]
	s.Require().True(ok)
	s.Equal("test_field", annField)
	groupField, ok := searchParams[spGroupBy]
	s.Require().True(ok)
	s.Equal("group_field", groupField)
	groupSize, ok := searchParams[spGroupSize]
	s.Require().True(ok)
	s.Equal("10", groupSize)
	spStrictGroupSize, ok := searchParams[spStrictGroupSize]
	s.Require().True(ok)
	s.Equal("true", spStrictGroupSize)

	functionScore := req.GetFunctionScore()
	s.Len(functionScore.GetFunctions(), 1)

	opt = NewSearchOption(collName, topK, []entity.Vector{nonSupportData{}})
	_, err = opt.Request()
	s.Error(err)
}

func (s *SearchOptionSuite) TestPlaceHolder() {
	type testCase struct {
		tag         string
		input       []entity.Vector
		expectError bool
		expectType  commonpb.PlaceholderType
	}

	sparse, err := entity.NewSliceSparseEmbedding([]uint32{0, 10, 12}, []float32{0.1, 0.2, 0.3})
	s.Require().NoError(err)

	cases := []*testCase{
		{
			tag:        "empty_input",
			input:      nil,
			expectType: commonpb.PlaceholderType_None,
		},
		{
			tag:        "float_vector",
			input:      []entity.Vector{entity.FloatVector([]float32{0.1, 0.2, 0.3})},
			expectType: commonpb.PlaceholderType_FloatVector,
		},
		{
			tag:        "sparse_vector",
			input:      []entity.Vector{sparse},
			expectType: commonpb.PlaceholderType_SparseFloatVector,
		},
		{
			tag:        "fp16_vector",
			input:      []entity.Vector{entity.Float16Vector([]byte{})},
			expectType: commonpb.PlaceholderType_Float16Vector,
		},
		{
			tag:        "bf16_vector",
			input:      []entity.Vector{entity.BFloat16Vector([]byte{})},
			expectType: commonpb.PlaceholderType_BFloat16Vector,
		},
		{
			tag:        "binary_vector",
			input:      []entity.Vector{entity.BinaryVector([]byte{})},
			expectType: commonpb.PlaceholderType_BinaryVector,
		},
		{
			tag:        "int8_vector",
			input:      []entity.Vector{entity.Int8Vector([]int8{})},
			expectType: commonpb.PlaceholderType_Int8Vector,
		},
		{
			tag:        "text",
			input:      []entity.Vector{entity.Text("abc")},
			expectType: commonpb.PlaceholderType_VarChar,
		},
		{
			tag:         "non_supported",
			input:       []entity.Vector{nonSupportData{}},
			expectError: true,
		},
	}
	for _, tc := range cases {
		s.Run(tc.tag, func() {
			phv, err := vector2Placeholder(tc.input)
			if tc.expectError {
				s.Error(err)
			} else {
				s.NoError(err)
				s.Equal(tc.expectType, phv.GetType())
			}
		})
	}
}

func TestSearchOption(t *testing.T) {
	suite.Run(t, new(SearchOptionSuite))
}

func TestAny2TmplValue(t *testing.T) {
	t.Run("primitives", func(t *testing.T) {
		t.Run("int", func(t *testing.T) {
			v := rand.Int()
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			assert.EqualValues(t, v, val.GetInt64Val())
		})

		t.Run("int32", func(t *testing.T) {
			v := rand.Int31()
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			assert.EqualValues(t, v, val.GetInt64Val())
		})

		t.Run("int64", func(t *testing.T) {
			v := rand.Int63()
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			assert.EqualValues(t, v, val.GetInt64Val())
		})

		t.Run("float32", func(t *testing.T) {
			v := rand.Float32()
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			assert.EqualValues(t, v, val.GetFloatVal())
		})

		t.Run("float64", func(t *testing.T) {
			v := rand.Float64()
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			assert.EqualValues(t, v, val.GetFloatVal())
		})

		t.Run("bool", func(t *testing.T) {
			val, err := any2TmplValue(true)
			assert.NoError(t, err)
			assert.True(t, val.GetBoolVal())
		})

		t.Run("string", func(t *testing.T) {
			v := fmt.Sprintf("%v", rand.Int())
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			assert.EqualValues(t, v, val.GetStringVal())
		})
	})

	t.Run("slice", func(t *testing.T) {
		t.Run("int", func(t *testing.T) {
			l := rand.Intn(10) + 1
			v := make([]int, 0, l)
			for i := 0; i < l; i++ {
				v = append(v, rand.Int())
			}
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			data := val.GetArrayVal().GetLongData().GetData()
			assert.Equal(t, l, len(data))
			for i, val := range data {
				assert.EqualValues(t, v[i], val)
			}
		})

		t.Run("int32", func(t *testing.T) {
			l := rand.Intn(10) + 1
			v := make([]int32, 0, l)
			for i := 0; i < l; i++ {
				v = append(v, rand.Int31())
			}
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			data := val.GetArrayVal().GetLongData().GetData()
			assert.Equal(t, l, len(data))
			for i, val := range data {
				assert.EqualValues(t, v[i], val)
			}
		})

		t.Run("int64", func(t *testing.T) {
			l := rand.Intn(10) + 1
			v := make([]int64, 0, l)
			for i := 0; i < l; i++ {
				v = append(v, rand.Int63())
			}
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			data := val.GetArrayVal().GetLongData().GetData()
			assert.Equal(t, l, len(data))
			for i, val := range data {
				assert.EqualValues(t, v[i], val)
			}
		})

		t.Run("float32", func(t *testing.T) {
			l := rand.Intn(10) + 1
			v := make([]float32, 0, l)
			for i := 0; i < l; i++ {
				v = append(v, rand.Float32())
			}
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			data := val.GetArrayVal().GetDoubleData().GetData()
			assert.Equal(t, l, len(data))
			for i, val := range data {
				assert.EqualValues(t, v[i], val)
			}
		})

		t.Run("float64", func(t *testing.T) {
			l := rand.Intn(10) + 1
			v := make([]float64, 0, l)
			for i := 0; i < l; i++ {
				v = append(v, rand.Float64())
			}
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			data := val.GetArrayVal().GetDoubleData().GetData()
			assert.Equal(t, l, len(data))
			for i, val := range data {
				assert.EqualValues(t, v[i], val)
			}
		})

		t.Run("bool", func(t *testing.T) {
			l := rand.Intn(10) + 1
			v := make([]bool, 0, l)
			for i := 0; i < l; i++ {
				v = append(v, rand.Int()%2 == 0)
			}
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			data := val.GetArrayVal().GetBoolData().GetData()
			assert.Equal(t, l, len(data))
			for i, val := range data {
				assert.EqualValues(t, v[i], val)
			}
		})

		t.Run("string", func(t *testing.T) {
			l := rand.Intn(10) + 1
			v := make([]string, 0, l)
			for i := 0; i < l; i++ {
				v = append(v, fmt.Sprintf("%v", rand.Int()))
			}
			val, err := any2TmplValue(v)
			assert.NoError(t, err)
			data := val.GetArrayVal().GetStringData().GetData()
			assert.Equal(t, l, len(data))
			for i, val := range data {
				assert.EqualValues(t, v[i], val)
			}
		})
	})

	t.Run("unsupported", func(*testing.T) {
		_, err := any2TmplValue(struct{}{})
		assert.Error(t, err)

		_, err = any2TmplValue([]struct{}{})
		assert.Error(t, err)
	})
}

func (s *SearchOptionSuite) TestSearchByIDs() {
	collName := "search_by_ids_test"
	limit := 10

	s.Run("int64_ids", func() {
		ids := column.NewColumnInt64("id", []int64{1, 2, 3, 4, 5})
		opt := NewSearchByIDsOption(collName, limit, ids)
		opt = opt.WithANNSField("vector_field").WithConsistencyLevel(entity.ClStrong)

		req, err := opt.Request()
		s.Require().NoError(err)

		s.Equal(collName, req.GetCollectionName())
		s.Equal(int64(5), req.GetNq()) // 5 IDs
		s.NotNil(req.GetIds())
		s.Equal([]int64{1, 2, 3, 4, 5}, req.GetIds().GetIntId().GetData())
	})

	s.Run("varchar_ids", func() {
		ids := column.NewColumnVarChar("id", []string{"a", "b", "c"})
		opt := NewSearchByIDsOption(collName, limit, ids)

		req, err := opt.Request()
		s.Require().NoError(err)

		s.Equal(int64(3), req.GetNq()) // 3 IDs
		s.NotNil(req.GetIds())
		s.Equal([]string{"a", "b", "c"}, req.GetIds().GetStrId().GetData())
	})

	s.Run("empty_ids_error", func() {
		ids := column.NewColumnInt64("id", []int64{})
		opt := NewSearchByIDsOption(collName, limit, ids)

		_, err := opt.Request()
		s.Error(err)
		s.Contains(err.Error(), "cannot be empty")
	})

	s.Run("with_filter", func() {
		ids := column.NewColumnInt64("id", []int64{1, 2, 3})
		opt := NewSearchByIDsOption(collName, limit, ids).
			WithFilter("status == 'active'").
			WithANNSField("vector")

		req, err := opt.Request()
		s.Require().NoError(err)

		s.Equal("status == 'active'", req.GetDsl())
		s.Equal(int64(3), req.GetNq())
	})

	s.Run("with_output_fields", func() {
		ids := column.NewColumnInt64("id", []int64{1, 2})
		opt := NewSearchByIDsOption(collName, limit, ids).
			WithOutputFields("id", "name", "vector")

		req, err := opt.Request()
		s.Require().NoError(err)

		s.ElementsMatch([]string{"id", "name", "vector"}, req.GetOutputFields())
	})

	s.Run("single_id", func() {
		ids := column.NewColumnInt64("id", []int64{42})
		opt := NewSearchByIDsOption(collName, limit, ids)

		req, err := opt.Request()
		s.Require().NoError(err)

		s.Equal(int64(1), req.GetNq())
		s.Equal([]int64{42}, req.GetIds().GetIntId().GetData())
	})

	s.Run("unsupported_id_type_error", func() {
		// Float column is not a valid primary key type
		ids := column.NewColumnFloat("id", []float32{1.0, 2.0})
		opt := NewSearchByIDsOption(collName, limit, ids)

		_, err := opt.Request()
		s.Error(err)
		s.Contains(err.Error(), "failed to convert IDs column")
		s.Contains(err.Error(), "unsupported primary key type")
	})
}

func TestColumn2IDs(t *testing.T) {
	t.Run("int64_column", func(t *testing.T) {
		col := column.NewColumnInt64("pk", []int64{100, 200, 300})
		ids, err := column2IDs(col)
		assert.NoError(t, err)
		assert.NotNil(t, ids.GetIntId())
		assert.Equal(t, []int64{100, 200, 300}, ids.GetIntId().GetData())
	})

	t.Run("varchar_column", func(t *testing.T) {
		col := column.NewColumnVarChar("pk", []string{"id1", "id2", "id3"})
		ids, err := column2IDs(col)
		assert.NoError(t, err)
		assert.NotNil(t, ids.GetStrId())
		assert.Equal(t, []string{"id1", "id2", "id3"}, ids.GetStrId().GetData())
	})

	t.Run("string_column", func(t *testing.T) {
		col := column.NewColumnString("pk", []string{"str1", "str2"})
		ids, err := column2IDs(col)
		assert.NoError(t, err)
		assert.NotNil(t, ids.GetStrId())
		assert.Equal(t, []string{"str1", "str2"}, ids.GetStrId().GetData())
	})

	t.Run("nil_column_error", func(t *testing.T) {
		_, err := column2IDs(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil")
	})

	t.Run("unsupported_type_error", func(t *testing.T) {
		col := column.NewColumnFloat("pk", []float32{1.0, 2.0})
		_, err := column2IDs(col)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported primary key type")
	})
}

func TestAnnRequestWithIDs(t *testing.T) {
	t.Run("basic_with_ids", func(t *testing.T) {
		ids := column.NewColumnInt64("pk", []int64{1, 2, 3})
		req := NewAnnRequest("vector_field", 10)
		req.WithIDs(ids)

		searchReq, err := req.searchRequest()
		assert.NoError(t, err)
		assert.Equal(t, int64(3), searchReq.GetNq())
		assert.NotNil(t, searchReq.GetIds())
	})

	t.Run("ids_override_vectors", func(t *testing.T) {
		// When IDs are set, vectors should be ignored
		vectors := []entity.Vector{entity.FloatVector([]float32{0.1, 0.2, 0.3})}
		ids := column.NewColumnInt64("pk", []int64{1, 2})

		req := NewAnnRequest("vector_field", 10, vectors...)
		req.WithIDs(ids)

		searchReq, err := req.searchRequest()
		assert.NoError(t, err)
		// Nq should be based on IDs count, not vectors count
		assert.Equal(t, int64(2), searchReq.GetNq())
		assert.NotNil(t, searchReq.GetIds())
		assert.Nil(t, searchReq.GetPlaceholderGroup())
	})

	t.Run("with_search_params", func(t *testing.T) {
		ids := column.NewColumnInt64("pk", []int64{1, 2, 3})
		req := NewAnnRequest("vector_field", 10).
			WithIDs(ids).
			WithSearchParam("nprobe", "16").
			WithFilter("category == 'A'")

		searchReq, err := req.searchRequest()
		assert.NoError(t, err)
		assert.Equal(t, "category == 'A'", searchReq.GetDsl())

		params := entity.KvPairsMap(searchReq.GetSearchParams())
		assert.Equal(t, "16", params["nprobe"])
	})
}
