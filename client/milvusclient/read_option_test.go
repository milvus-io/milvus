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

	opt = opt.WithANNSField("test_field").WithOutputFields("ID", "Value").WithConsistencyLevel(entity.ClStrong).WithFilter("ID > 1000").WithGroupByField("group_field").WithGroupSize(10).WithStrictGroupSize(true)
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
