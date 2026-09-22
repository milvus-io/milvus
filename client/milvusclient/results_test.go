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
	"github.com/milvus-io/milvus/client/v3/row"
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

func (s *ResultSetSuite) TestResultsetUnmarshalTimestamptz() {
	type Event struct {
		ID    int64      `milvus:"name:id"`
		TS    time.Time  `milvus:"name:ts"`
		OptTS *time.Time `milvus:"name:opt_ts"`
		Raw   string     `milvus:"name:raw_ts"`
	}
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("ts").WithDataType(entity.FieldTypeTimestamptz)).
		WithField(entity.NewField().WithName("opt_ts").WithDataType(entity.FieldTypeTimestamptz).WithNullable(true)).
		WithField(entity.NewField().WithName("raw_ts").WithDataType(entity.FieldTypeTimestamptz))

	ts := time.Date(2024, 1, 2, 11, 4, 5, 123456000, time.FixedZone("UTC+8", 8*3600))
	later := ts.Add(time.Hour)
	iso := ts.Format(time.RFC3339Nano)

	// round trip: rows written from time.Time read back into the same struct
	columns, err := row.AnyToColumns([]any{
		&Event{ID: 1, TS: ts, OptTS: &later, Raw: iso},
		&Event{ID: 2, TS: ts, Raw: iso},
	}, false, schema)
	s.Require().NoError(err)

	var receiver []*Event
	s.Require().NoError(DataSet(columns).Unmarshal(&receiver))
	s.Require().Len(receiver, 2)

	s.True(ts.Equal(receiver[0].TS))
	s.Equal(iso, receiver[0].TS.Format(time.RFC3339Nano), "offset is preserved")
	s.Require().NotNil(receiver[0].OptTS)
	s.True(later.Equal(*receiver[0].OptTS))
	s.Equal(iso, receiver[0].Raw, "string receivers are unchanged")

	s.True(ts.Equal(receiver[1].TS))
	s.Nil(receiver[1].OptTS, "null stays nil")
	s.Equal(iso, receiver[1].Raw)

	// a malformed value surfaces as an error instead of a panic
	var bad []*Event
	err = DataSet([]column.Column{
		column.NewColumnInt64("id", []int64{1}),
		column.NewColumnTimestamptzIsoString("ts", []string{"not-a-timestamp"}),
	}).Unmarshal(&bad)
	s.Error(err)
	s.ErrorContains(err, "not-a-timestamp")
}

func TestResults(t *testing.T) {
	suite.Run(t, new(ResultSetSuite))
}
