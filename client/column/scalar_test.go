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

package column

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v3/entity"
)

type ScalarSuite struct {
	suite.Suite
}

func (s *ScalarSuite) TestBasic() {
	s.Run("column_bool", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []bool{true, false}
		column := NewColumnBool(name, data)
		s.Equal(entity.FieldTypeBool, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetBoolData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnBool)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeBool, column.Type())
		}
	})

	s.Run("column_int8", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int8{1, 2, 3}
		column := NewColumnInt8(name, data)
		s.Equal(entity.FieldTypeInt8, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		fdData := fd.GetScalars().GetIntData().GetData()
		for i, row := range data {
			s.EqualValues(row, fdData[i])
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt8)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt8, column.Type())
		}
	})

	s.Run("column_int16", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int16{1, 2, 3}
		column := NewColumnInt16(name, data)
		s.Equal(entity.FieldTypeInt16, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		fdData := fd.GetScalars().GetIntData().GetData()
		for i, row := range data {
			s.EqualValues(row, fdData[i])
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt16)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt16, column.Type())
		}
	})

	s.Run("column_int32", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int32{1, 2, 3}
		column := NewColumnInt32(name, data)
		s.Equal(entity.FieldTypeInt32, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetIntData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt32)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt32, column.Type())
		}
	})

	s.Run("column_int64", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int64{1, 2, 3}
		column := NewColumnInt64(name, data)
		s.Equal(entity.FieldTypeInt64, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetLongData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt64)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt64, column.Type())
		}
	})

	s.Run("column_float", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []float32{1.1, 2.2, 3.3}
		column := NewColumnFloat(name, data)
		s.Equal(entity.FieldTypeFloat, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetFloatData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnFloat)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeFloat, column.Type())
		}
	})

	s.Run("column_double", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []float64{1.1, 2.2, 3.3}
		column := NewColumnDouble(name, data)
		s.Equal(entity.FieldTypeDouble, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetDoubleData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnDouble)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeDouble, column.Type())
		}
	})

	s.Run("column_varchar", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []string{"a", "b", "c"}
		column := NewColumnVarChar(name, data)
		s.Equal(entity.FieldTypeVarChar, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetStringData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnVarChar)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeVarChar, column.Type())
		}
	})

	s.Run("column_text", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []string{"short text", "长文本", "large text payload"}
		column := NewColumnText(name, data)
		s.Equal(entity.FieldTypeText, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.EqualValues(entity.FieldTypeText, fd.GetType())
		s.Equal(data, fd.GetScalars().GetStringData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnText)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeText, parsed.Type())
		}
	})

	s.Run("column_timestamptz", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		now := time.Now().UTC()
		data := []time.Time{now, now.Add(time.Hour), now.Add(2 * time.Hour)}
		column := NewColumnTimestamptz(name, data)
		s.Equal(entity.FieldTypeTimestamptz, column.Type())
		s.Equal(name, column.Name())
		// verify data is converted to normalized RFC3339Nano format
		expectedStrings := []string{
			formatTimestamptz(data[0]),
			formatTimestamptz(data[1]),
			formatTimestamptz(data[2]),
		}
		s.Equal(expectedStrings, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(expectedStrings, fd.GetScalars().GetStringData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnTimestampTzIsoString)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(expectedStrings, parsed.Data())
			s.Equal(entity.FieldTypeTimestamptz, parsed.Type())
		}
	})

	s.Run("column_timestamptz_iso_string", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []string{
			"2024-01-01T00:00:00Z",
			"2024-06-15T12:30:45.123456789Z",
			"2024-12-31T23:59:59.999999999+08:00",
		}
		column := NewColumnTimestamptzIsoString(name, data)
		s.Equal(entity.FieldTypeTimestamptz, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetStringData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnTimestampTzIsoString)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeTimestamptz, parsed.Type())
		}
	})
}

func (s *ScalarSuite) TestTimestamptzAppendValue() {
	name := fmt.Sprintf("field_%d", rand.Intn(1000))
	column := NewColumnTimestamptz(name, nil)

	now := time.Now().UTC().Truncate(time.Nanosecond)
	s.NoError(column.AppendValue(now))
	s.NoError(column.AppendValue(now.Add(time.Hour)))
	s.NoError(column.AppendValue(now.Format(time.RFC3339Nano)))

	expected := []string{
		formatTimestamptz(now),
		formatTimestamptz(now.Add(time.Hour)),
		// raw ISO string inputs are stored as-is, without normalization
		now.Format(time.RFC3339Nano),
	}
	s.Equal(expected, column.Data())
	s.Equal(expected, column.FieldData().GetScalars().GetStringData().GetData())

	s.Error(column.AppendValue(now.UnixMilli()))
}

func (s *ScalarSuite) TestTimestamptzNormalization() {
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	s.Require().NoError(err)
	// Pre-1901 Shanghai LMT offset carries seconds (+08:05:43) that RFC3339
	// (minute precision) cannot represent; formatting the raw location emits
	// +08:05 and shifts the instant by 43s on re-parse.
	lmt := time.Date(1900, 1, 1, 0, 0, 0, 0, shanghai)
	orig := lmt.UTC()

	column := NewColumnTimestamptz("ts", []time.Time{lmt})
	s.Require().NoError(column.AppendValue(lmt))

	s.Require().Len(column.Data(), 2)
	for i, got := range column.Data() {
		parsed, err := time.Parse(time.RFC3339Nano, got)
		s.Require().NoError(err, "value %d %q must be RFC3339Nano parseable", i, got)
		s.Equal(orig, parsed.UTC(), "value %d %q must preserve the instant of %v", i, got, lmt)
		s.True(strings.HasSuffix(got, "Z"), "value %d %q must be normalized to UTC", i, got)
	}
}

func (s *ScalarSuite) TestSlice() {
	n := 100
	s.Run("column_bool", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]bool, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Int()%2 == 0)
		}
		column := NewColumnBool(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnBool)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_int8", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]int8, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, int8(rand.Intn(math.MaxInt8)))
		}
		column := NewColumnInt8(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnInt8)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_int16", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]int16, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, int16(rand.Intn(math.MaxInt16)))
		}
		column := NewColumnInt16(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnInt16)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_int32", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]int32, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Int31())
		}
		column := NewColumnInt32(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnInt32)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_int64", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]int64, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Int63())
		}
		column := NewColumnInt64(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnInt64)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_float", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]float32, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Float32())
		}
		column := NewColumnFloat(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnFloat)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_double", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]float64, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Float64())
		}
		column := NewColumnDouble(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnDouble)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_varchar", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]string, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, fmt.Sprintf("%d", rand.Int()))
		}
		column := NewColumnVarChar(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnVarChar)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_timestamptz", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		now := time.Now().UTC()
		timeData := make([]time.Time, 0, n)
		for i := 0; i < n; i++ {
			timeData = append(timeData, now.Add(time.Duration(i)*time.Hour))
		}
		column := NewColumnTimestamptz(name, timeData)
		data := column.Data()

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnTimestamptz)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_timestamptz_iso_string", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]string, 0, n)
		for i := 0; i < n; i++ {
			data = append(data, fmt.Sprintf("2024-01-%02dT00:00:00Z", (i%28)+1))
		}
		column := NewColumnTimestamptzIsoString(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnTimestampTzIsoString)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})
}

func TestScalarColumn(t *testing.T) {
	suite.Run(t, new(ScalarSuite))
}
