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
	"math/rand"
	"testing"

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

	opt = opt.WithANNSField("test_field").WithOutputFields("ID", "Value").WithConsistencyLevel(entity.ClStrong).WithFilter("ID > 1000")

	req, err := opt.Request()
	s.Require().NoError(err)

	s.Equal(collName, req.GetCollectionName())
	s.Equal("ID > 1000", req.GetDsl())
	s.ElementsMatch([]string{"ID", "Value"}, req.GetOutputFields())
	searchParams := entity.KvPairsMap(req.GetSearchParams())
	annField, ok := searchParams[spAnnsField]
	s.Require().True(ok)
	s.Equal("test_field", annField)

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
