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

package httpserver

import (
	gojson "encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestFieldSchemaIntegerDefaults(t *testing.T) {
	type testCase struct {
		dataType schemapb.DataType
		literal  string
		want     int64
		invalid  bool
	}
	tests := []testCase{
		{schemapb.DataType_Int8, "-128", -128, false},
		{schemapb.DataType_Int8, "127", 127, false},
		{schemapb.DataType_Int8, "128", 0, true},
		{schemapb.DataType_Int8, "-129", 0, true},
		{schemapb.DataType_Int16, "-32768", -32768, false},
		{schemapb.DataType_Int16, "32767", 32767, false},
		{schemapb.DataType_Int16, "32768", 0, true},
		{schemapb.DataType_Int16, "-32769", 0, true},
		{schemapb.DataType_Int32, "-2147483648", -2147483648, false},
		{schemapb.DataType_Int32, "2147483647", 2147483647, false},
		{schemapb.DataType_Int32, "2147483648", 0, true},
		{schemapb.DataType_Int32, "-2147483649", 0, true},
		{schemapb.DataType_Int64, "9007199254740993", 9007199254740993, false},
		{schemapb.DataType_Int64, "9223372036854775807", math.MaxInt64, false},
		{schemapb.DataType_Int64, "-9223372036854775808", math.MinInt64, false},
		{schemapb.DataType_Int64, "9223372036854775808", 0, true},
		{schemapb.DataType_Int64, "-9223372036854775809", 0, true},
		{schemapb.DataType_Int64, "9007199254740993.0", 9007199254740993, false},
		{schemapb.DataType_Int64, "9.007199254740993e15", 9007199254740993, false},
		{schemapb.DataType_Int64, "1.0", 1, false},
		{schemapb.DataType_Int64, "1e2", 100, false},
		{schemapb.DataType_Int64, "9223372036854775807.1", 0, true},
		{schemapb.DataType_Int64, "9.2233720368547758071e18", 0, true},
		{schemapb.DataType_Int64, "1e9999999999999999999999999", 0, true},
		{schemapb.DataType_Int64, "1e-9999999999999999999999999", 0, true},
		{schemapb.DataType_Int64, "0e9999999999999999999999999", 0, false},
	}
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64} {
		for _, literal := range []string{"1.9", "-1.9", "1e-1", "0.00000000000000000001"} {
			tests = append(tests, testCase{dataType, literal, 0, true})
		}
	}
	for _, test := range tests {
		t.Run(test.dataType.String()+"/"+test.literal, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"fieldName":"number","dataType":%q,"defaultValue": %s }`, test.dataType.String(), test.literal))
			// Gin uses encoding/json, while other code uses the internal JSON
			// implementation. Both must invoke the field's local unmarshaler.
			for _, unmarshal := range []func([]byte, interface{}) error{gojson.Unmarshal, json.Unmarshal} {
				var field FieldSchema
				require.NoError(t, unmarshal(body, &field))
				value, err := convertDefaultValue(field.DefaultValue, test.dataType)
				if test.invalid {
					require.Error(t, err)
					assert.Equal(t, merr.Code(merr.ErrParameterInvalid), merr.Code(err))
					assert.Nil(t, value)
					continue
				}
				require.NoError(t, err)
				require.NotNil(t, value)
				if test.dataType == schemapb.DataType_Int64 {
					assert.Equal(t, test.want, value.GetLongData())
				} else {
					assert.Equal(t, test.want, int64(value.GetIntData()))
				}
			}
		})
	}
}

func TestFieldSchemaDefaultValueCompatibility(t *testing.T) {
	for _, test := range []struct {
		body string
		want interface{}
	}{
		{`{"dataType":"Int64"}`, nil},
		{`{"dataType":"Int64","defaultValue":null}`, nil},
		{`{"dataType":"Bool","defaultValue":true}`, true},
		{`{"dataType":"VarChar","defaultValue":"example"}`, "example"},
		{`{"dataType":"Float","defaultValue":1.9}`, float64(1.9)},
		{`{"dataType":"Double","defaultValue":1.9}`, float64(1.9)},
	} {
		t.Run(test.body, func(t *testing.T) {
			var field FieldSchema
			require.NoError(t, gojson.Unmarshal([]byte(test.body), &field))
			assert.Equal(t, test.want, field.DefaultValue)
			dataType := schemapb.DataType(schemapb.DataType_value[field.DataType])
			value, err := convertDefaultValue(field.DefaultValue, dataType)
			require.NoError(t, err)
			if test.want == nil {
				assert.Nil(t, value)
			} else {
				assert.NotNil(t, value)
			}
		})
	}
	var field FieldSchema
	require.NoError(t, gojson.Unmarshal([]byte(`{"defaultValue":   9007199254740993  , "dataType":"Int64"}`), &field))
	assert.Equal(t, json.Number("9007199254740993"), field.DefaultValue)
	require.NoError(t, gojson.Unmarshal([]byte(`{"dataType":"Int64","defaultValue":42,"elementTypeParams":{"dim":128},"typeParams":{"n":8},"fields":[{"dataType":"Int64","defaultValue":9007199254740993}]}`), &field))
	assert.Equal(t, float64(128), field.ElementTypeParams["dim"])
	assert.Equal(t, float64(8), field.TypeParams["n"])
	require.Len(t, field.Fields, 1)
	assert.Equal(t, json.Number("9007199254740993"), field.Fields[0].DefaultValue)

	for _, dataType := range []schemapb.DataType{schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64} {
		_, err := convertDefaultValue(float64(42), dataType)
		require.NoError(t, err)
		for _, value := range []interface{}{float64(1.9), math.NaN(), math.Inf(1), math.Inf(-1), "42", true} {
			_, err := convertDefaultValue(value, dataType)
			require.Error(t, err)
		}
	}
	value, err := convertDefaultValue(float64(math.MinInt64), schemapb.DataType_Int64)
	require.NoError(t, err)
	assert.Equal(t, int64(math.MinInt64), value.GetLongData())
	_, err = convertDefaultValue(float64(math.MaxInt64), schemapb.DataType_Int64)
	require.Error(t, err)
}
