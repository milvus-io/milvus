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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type ResultSetSuite struct {
	suite.Suite
}

func (s *ResultSetSuite) TestResultsetUnmarshal() {
	type MyData struct {
		A     int64     `milvus:"name:id"`
		V     []float32 `milvus:"name:vector"`
		Fp16V []byte    `milvus:"name:fp16_vector"`
		Bf16V []byte    `milvus:"name:bf16_vector"`
	}
	type OtherData struct {
		A     string    `milvus:"name:id"`
		V     []float32 `milvus:"name:vector"`
		Fp16V []byte    `milvus:"name:fp16_vector"`
		Bf16V []byte    `milvus:"name:bf16_vector"`
	}

	var (
		idData     = []int64{1, 2, 3}
		vectorData = [][]float32{
			{0.1, 0.2},
			{0.1, 0.2},
			{0.1, 0.2},
		}
	)

	rs := DataSet([]column.Column{
		column.NewColumnInt64("id", idData),
		column.NewColumnFloatVector("vector", 2, vectorData),
		column.NewColumnFloat16VectorFromFp32Vector("fp16_vector", 2, vectorData),
		column.NewColumnBFloat16VectorFromFp32Vector("bf16_vector", 2, vectorData),
	})
	err := rs.Unmarshal([]MyData{})
	s.Error(err)

	receiver := []MyData{}
	err = rs.Unmarshal(&receiver)
	s.Error(err)

	var ptrReceiver []*MyData
	err = rs.Unmarshal(&ptrReceiver)
	s.NoError(err)

	for idx, row := range ptrReceiver {
		s.Equal(row.A, idData[idx])
		s.Equal(row.V, vectorData[idx])
		s.Equal(entity.Float16Vector(row.Fp16V), entity.FloatVector(vectorData[idx]).ToFloat16Vector())
		s.Equal(entity.BFloat16Vector(row.Bf16V), entity.FloatVector(vectorData[idx]).ToBFloat16Vector())
	}

	var otherReceiver []*OtherData
	err = rs.Unmarshal(&otherReceiver)
	s.Error(err)
}

func (s *ResultSetSuite) TestSearchResultUnmarshal() {
	type MyData struct {
		A     int64     `milvus:"name:id"`
		V     []float32 `milvus:"name:vector"`
		Fp16V []byte    `milvus:"name:fp16_vector"`
		Bf16V []byte    `milvus:"name:bf16_vector"`
	}
	type OtherData struct {
		A     string    `milvus:"name:id"`
		V     []float32 `milvus:"name:vector"`
		Fp16V []byte    `milvus:"name:fp16_vector"`
		Bf16V []byte    `milvus:"name:bf16_vector"`
	}

	var (
		idData     = []int64{1, 2, 3}
		vectorData = [][]float32{
			{0.1, 0.2},
			{0.1, 0.2},
			{0.1, 0.2},
		}
	)

	sr := ResultSet{
		sch: entity.NewSchema().
			WithField(entity.NewField().WithName("id").WithIsPrimaryKey(true).WithDataType(entity.FieldTypeInt64)).
			WithField(entity.NewField().WithName("vector").WithDim(2).WithDataType(entity.FieldTypeFloatVector)).
			WithField(entity.NewField().WithName("fp16_vector").WithDim(2).WithDataType(entity.FieldTypeFloat16Vector)).
			WithField(entity.NewField().WithName("bf16_vector").WithDim(2).WithDataType(entity.FieldTypeBFloat16Vector)),
		IDs: column.NewColumnInt64("id", idData),
		Fields: DataSet([]column.Column{
			column.NewColumnFloatVector("vector", 2, vectorData),
			column.NewColumnFloat16VectorFromFp32Vector("fp16_vector", 2, vectorData),
			column.NewColumnBFloat16VectorFromFp32Vector("bf16_vector", 2, vectorData),
		}),
	}
	err := sr.Unmarshal([]MyData{})
	s.Error(err)

	receiver := []MyData{}
	err = sr.Unmarshal(&receiver)
	s.Error(err)

	var ptrReceiver []*MyData
	err = sr.Unmarshal(&ptrReceiver)
	s.NoError(err)

	for idx, row := range ptrReceiver {
		s.Equal(row.A, idData[idx])
		s.Equal(row.V, vectorData[idx])
		s.Equal(entity.Float16Vector(row.Fp16V), entity.FloatVector(vectorData[idx]).ToFloat16Vector())
		s.Equal(entity.BFloat16Vector(row.Bf16V), entity.FloatVector(vectorData[idx]).ToBFloat16Vector())
	}

	var otherReceiver []*OtherData
	err = sr.Unmarshal(&otherReceiver)
	s.Error(err)
}

func TestResults(t *testing.T) {
	suite.Run(t, new(ResultSetSuite))
}
