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
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
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

func (s *ResultSetSuite) TestResultsetUnmarshalTimestamptz() {
	type TsData struct {
		Ts time.Time `milvus:"name:ts"`
	}
	type NullableTsData struct {
		Ts *time.Time `milvus:"name:ts"`
	}
	type StringTsData struct {
		Ts string `milvus:"name:ts"`
	}

	// the server returns timestamptz as ISO strings in the collection timezone
	isoData := []string{
		"2024-01-01T00:00:00+08:00",
		"2024-06-15T12:30:45.123456Z",
	}
	ds := DataSet([]column.Column{
		column.NewColumnTimestamptzIsoString("ts", isoData),
	})

	// time.Time target
	var timeReceiver []*TsData
	err := ds.Unmarshal(&timeReceiver)
	s.NoError(err)
	s.Require().Len(timeReceiver, 2)
	for i, row := range timeReceiver {
		parsed, err := time.Parse(time.RFC3339Nano, isoData[i])
		s.NoError(err)
		s.True(parsed.Equal(row.Ts), "row %d ts = %v, want %v", i, row.Ts, parsed)
	}

	// string target unchanged
	var strReceiver []*StringTsData
	err = ds.Unmarshal(&strReceiver)
	s.NoError(err)
	s.Require().Len(strReceiver, 2)
	for i, row := range strReceiver {
		s.Equal(isoData[i], row.Ts)
	}

	// nullable *time.Time target, including a null row (compact mode)
	nullableCol, err := column.NewNullableColumnTimestamptzIsoString("ts", []string{"2024-01-01T00:00:00Z"}, []bool{true, false})
	s.NoError(err)
	dsN := DataSet([]column.Column{nullableCol})
	var nullReceiver []*NullableTsData
	err = dsN.Unmarshal(&nullReceiver)
	s.NoError(err)
	s.Require().Len(nullReceiver, 2)
	s.Require().NotNil(nullReceiver[0].Ts)
	s.True(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Equal(*nullReceiver[0].Ts))
	s.Nil(nullReceiver[1].Ts)

	// unparseable string into time.Time target returns error
	badCol := column.NewColumnTimestamptzIsoString("ts", []string{"not-a-timestamp"})
	err = DataSet([]column.Column{badCol}).Unmarshal(&[]*TsData{})
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

func (s *ResultSetSuite) TestResultsetUnmarshalNullablePointer() {
	type NullableData struct {
		ID   int64   `milvus:"name:id"`
		Name *string `milvus:"name:name"`
		Age  *int32  `milvus:"name:age"`
	}

	// Create a nullable string column with mixed null/non-null values
	nameCol := column.NewColumnVarChar("name", nil)
	nameCol.SetNullable(true)
	_ = nameCol.AppendValue("alice")
	_ = nameCol.AppendNull()
	_ = nameCol.AppendValue("charlie")

	// Create a nullable int32 column
	ageCol := column.NewColumnInt32("age", nil)
	ageCol.SetNullable(true)
	_ = ageCol.AppendValue(int32(30))
	_ = ageCol.AppendValue(int32(25))
	_ = ageCol.AppendNull()

	rs := DataSet([]column.Column{
		column.NewColumnInt64("id", []int64{1, 2, 3}),
		nameCol,
		ageCol,
	})

	var receiver []*NullableData
	err := rs.Unmarshal(&receiver)
	s.NoError(err)
	s.Require().Len(receiver, 3)

	// Row 0: Name="alice", Age=30
	s.Require().NotNil(receiver[0].Name)
	s.Equal("alice", *receiver[0].Name)
	s.Require().NotNil(receiver[0].Age)
	s.Equal(int32(30), *receiver[0].Age)

	// Row 1: Name=nil, Age=25
	s.Nil(receiver[1].Name)
	s.Require().NotNil(receiver[1].Age)
	s.Equal(int32(25), *receiver[1].Age)

	// Row 2: Name="charlie", Age=nil
	s.Require().NotNil(receiver[2].Name)
	s.Equal("charlie", *receiver[2].Name)
	s.Nil(receiver[2].Age)
}

func (s *ResultSetSuite) TestSearchResultUnmarshalPointerPK() {
	type PtrPKData struct {
		A *int64    `milvus:"name:id"`
		V []float32 `milvus:"name:vector"`
	}

	idData := []int64{1, 2, 3}
	vectorData := [][]float32{
		{0.1, 0.2},
		{0.1, 0.2},
		{0.1, 0.2},
	}

	sr := ResultSet{
		sch: entity.NewSchema().
			WithField(entity.NewField().WithName("id").WithIsPrimaryKey(true).WithDataType(entity.FieldTypeInt64)).
			WithField(entity.NewField().WithName("vector").WithDim(2).WithDataType(entity.FieldTypeFloatVector)),
		IDs: column.NewColumnInt64("id", idData),
		Fields: DataSet([]column.Column{
			column.NewColumnFloatVector("vector", 2, vectorData),
		}),
	}

	var receiver []*PtrPKData
	err := sr.Unmarshal(&receiver)
	s.NoError(err)
	s.Require().Len(receiver, 3)

	for idx, row := range receiver {
		s.Require().NotNil(row.A)
		s.Equal(idData[idx], *row.A)
		s.Equal(vectorData[idx], row.V)
	}
}

func TestResults(t *testing.T) {
	suite.Run(t, new(ResultSetSuite))
}
