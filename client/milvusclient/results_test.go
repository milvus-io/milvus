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

func (s *ResultSetSuite) TestDataSetUnmarshalArray() {
	type MyData struct {
		A     int64     `milvus:"name:id"`
		V     []float32 `milvus:"name:vector"`
		Fp16V []byte    `milvus:"name:fp16_vector"`
		Bf16V []byte    `milvus:"name:bf16_vector"`
	}

	var (
		idData     = []int64{1, 2, 3}
		vectorData = [][]float32{
			{0.1, 0.2},
			{0.3, 0.4},
			{0.5, 0.6},
		}
	)

	rs := DataSet([]column.Column{
		column.NewColumnInt64("id", idData),
		column.NewColumnFloatVector("vector", 2, vectorData),
		column.NewColumnFloat16VectorFromFp32Vector("fp16_vector", 2, vectorData),
		column.NewColumnBFloat16VectorFromFp32Vector("bf16_vector", 2, vectorData),
	})

	// success, the length of array match the length of dataset
	var arrayReceiver [3]*MyData
	err := rs.Unmarshal(&arrayReceiver)
	s.NoError(err)

	// check the data in array receiver
	for idx, row := range arrayReceiver {
		s.NotNil(row)
		s.Equal(row.A, idData[idx])
		s.Equal(row.V, vectorData[idx])
		s.Equal(entity.Float16Vector(row.Fp16V), entity.FloatVector(vectorData[idx]).ToFloat16Vector())
		s.Equal(entity.BFloat16Vector(row.Bf16V), entity.FloatVector(vectorData[idx]).ToBFloat16Vector())
	}

	// check the error case: array length mismatch (smaller)
	var smallArrayReceiver [2]*MyData
	err = rs.Unmarshal(&smallArrayReceiver)
	s.Error(err)
	s.Contains(err.Error(), "receiver array length 2 does not match dataset length 3")

	// check the error case: array length mismatch (larger)
	var largeArrayReceiver [5]*MyData
	err = rs.Unmarshal(&largeArrayReceiver)
	s.Error(err)
	s.Contains(err.Error(), "receiver array length 5 does not match dataset length 3")

	// check the error case: array element is not pointer type
	var nonPtrArrayReceiver [3]MyData
	err = rs.Unmarshal(&nonPtrArrayReceiver)
	s.Error(err)
	s.Contains(err.Error(), "receiver must be array of pointers")

	// check the error case: array element is not pointer type
	var directArrayReceiver [3]*MyData
	err = rs.Unmarshal(directArrayReceiver)
	s.Error(err)
	s.Contains(err.Error(), "using unaddressable value")
}

func (s *ResultSetSuite) TestSearchResultUnmarshalArray() {
	type MyData struct {
		A     int64     `milvus:"name:id"`
		V     []float32 `milvus:"name:vector"`
		Fp16V []byte    `milvus:"name:fp16_vector"`
		Bf16V []byte    `milvus:"name:bf16_vector"`
	}

	var (
		idData     = []int64{1, 2, 3}
		vectorData = [][]float32{
			{0.1, 0.2},
			{0.3, 0.4},
			{0.5, 0.6},
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

	// success, the length of array match the length of dataset
	var arrayReceiver [3]*MyData
	err := sr.Unmarshal(&arrayReceiver)
	s.NoError(err)

	// check the data in array receiver
	for idx, row := range arrayReceiver {
		s.NotNil(row)
		s.Equal(idData[idx], row.A)
		s.Equal(vectorData[idx], row.V)
		s.Equal(entity.Float16Vector(row.Fp16V), entity.FloatVector(vectorData[idx]).ToFloat16Vector())
		s.Equal(entity.BFloat16Vector(row.Bf16V), entity.FloatVector(vectorData[idx]).ToBFloat16Vector())
	}

	// check the error case: array length mismatch
	var mismatchArrayReceiver [2]*MyData
	err = sr.Unmarshal(&mismatchArrayReceiver)
	s.Error(err)
	s.Contains(err.Error(), "receiver array length 2 does not match dataset length 3")
}

func (s *ResultSetSuite) TestDataSetUnmarshalArrayEdgeCases() {
	type SimpleData struct {
		ID int64 `milvus:"name:id"`
	}

	// check the edge case: empty dataset
	emptyRS := DataSet([]column.Column{})
	var emptyArrayReceiver [0]*SimpleData
	err := emptyRS.Unmarshal(&emptyArrayReceiver)
	s.NoError(err)

	// check the edge case: single element
	singleRS := DataSet([]column.Column{
		column.NewColumnInt64("id", []int64{42}),
	})
	var singleArrayReceiver [1]*SimpleData
	err = singleRS.Unmarshal(&singleArrayReceiver)
	s.NoError(err)
	s.NotNil(singleArrayReceiver[0])
	s.Equal(int64(42), singleArrayReceiver[0].ID)
}

func TestResults(t *testing.T) {
	suite.Run(t, new(ResultSetSuite))
}
