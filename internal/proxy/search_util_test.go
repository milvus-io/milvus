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

package proxy

import (
	"crypto/rand"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/stretchr/testify/suite"
)

type ProcessPlaceholderGroupSuite struct {
	suite.Suite

	schema *schemapb.CollectionSchema
}

func (s *ProcessPlaceholderGroupSuite) SetupSuite() {
	s.schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				Name:         "id",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				Name:     "float32",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
			{
				Name:     "float16",
				DataType: schemapb.DataType_Float16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
			{
				Name:     "bfloat16",
				DataType: schemapb.DataType_BFloat16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
			{
				Name:     "binary",
				DataType: schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
			{
				Name:     "sparse",
				DataType: schemapb.DataType_SparseFloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
}

func (s *ProcessPlaceholderGroupSuite) getVector(dataType schemapb.DataType, dim int) []byte {
	phg := &commonpb.PlaceholderGroup{}
	phv := &commonpb.PlaceholderValue{
		Tag: "$0",
	}
	switch dataType {
	case schemapb.DataType_FloatVector:
		data := make([]byte, 4*dim) // float32 occupies 4 bytes per value
		rand.Read(data)
		phv.Type = commonpb.PlaceholderType_FloatVector
		phv.Values = [][]byte{data}
	case schemapb.DataType_Float16Vector:
		data := make([]byte, 2*dim) // float16 occupies 2 bytes per value
		rand.Read(data)
		phv.Type = commonpb.PlaceholderType_Float16Vector
		phv.Values = [][]byte{data}
	case schemapb.DataType_BFloat16Vector:
		data := make([]byte, 2*dim) // bfloat16 occupies 2 bytes per value
		rand.Read(data)
		phv.Type = commonpb.PlaceholderType_BFloat16Vector
		phv.Values = [][]byte{data}
	case schemapb.DataType_BinaryVector:
		data := make([]byte, dim/8)
		rand.Read(data)
		phv.Type = commonpb.PlaceholderType_BinaryVector
		phv.Values = [][]byte{data}
	case schemapb.DataType_SparseFloatVector:
		data := make([]byte, dim*8)
		rand.Read(data)
		phv.Type = commonpb.PlaceholderType_SparseFloatVector
		phv.Values = [][]byte{data}
	}
	phg.Placeholders = append(phg.Placeholders, phv)

	bs, err := proto.Marshal(phg)
	s.Require().NoError(err)
	return bs
}

func (s *ProcessPlaceholderGroupSuite) TestNormalCase() {
	type testCase struct {
		dataType  schemapb.DataType
		fieldName string
	}

	cases := []testCase{
		{dataType: schemapb.DataType_FloatVector, fieldName: "float32"},
		{dataType: schemapb.DataType_Float16Vector, fieldName: "float16"},
		{dataType: schemapb.DataType_BFloat16Vector, fieldName: "bfloat16"},
		{dataType: schemapb.DataType_BinaryVector, fieldName: "binary"},
		{dataType: schemapb.DataType_SparseFloatVector, fieldName: "sparse"},
	}

	for _, tc := range cases {
		s.Run(tc.dataType.String(), func() {
			phgBytes := s.getVector(tc.dataType, 128)
			task := &searchTask{
				request: &milvuspb.SearchRequest{
					PlaceholderGroup: phgBytes,
				},
				schema:        newSchemaInfo(s.schema),
				annsFieldName: tc.fieldName,
			}
			bs, err := processPlaceholderGroup(task)
			s.NoError(err)
			s.Equal(phgBytes, bs)
		})
	}
}

func (s *ProcessPlaceholderGroupSuite) TestBadCase() {
	s.Run("field_not_found", func() {
		phgBytes := s.getVector(schemapb.DataType_BinaryVector, 128)
		task := &searchTask{
			request: &milvuspb.SearchRequest{
				PlaceholderGroup: phgBytes,
			},
			schema:        newSchemaInfo(s.schema),
			annsFieldName: "bad_field",
		}
		_, err := processPlaceholderGroup(task)
		s.Error(err)
	})

	s.Run("field_type_placeholdertype_not_match", func() {
		phgBytes := s.getVector(schemapb.DataType_BinaryVector, 128)
		task := &searchTask{
			request: &milvuspb.SearchRequest{
				PlaceholderGroup: phgBytes,
			},
			schema:        newSchemaInfo(s.schema),
			annsFieldName: "float32",
		}
		_, err := processPlaceholderGroup(task)
		s.Error(err)
	})

	s.Run("corrupted_placeholder_bytes", func() {
		phgBytes := []byte("\x123\x456")
		task := &searchTask{
			request: &milvuspb.SearchRequest{
				PlaceholderGroup: phgBytes,
			},
			schema:        newSchemaInfo(s.schema),
			annsFieldName: "float32",
		}
		_, err := processPlaceholderGroup(task)
		s.Error(err)
	})
}

func TestProcessPlaceholderGroup(t *testing.T) {
	suite.Run(t, new(ProcessPlaceholderGroupSuite))
}
