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

package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/datetime"
)

func TestExpr_DateTimeLiterals(t *testing.T) {
	helper := newTestSchemaHelper(t)

	valid := []string{
		`DateField > "2024-06-22"`,
		`DateField >= '1970-01-01'`,
		`DateField == "1969-12-31"`,
		`DateField in ["2024-06-22", "2024-06-23"]`,
		`DateField is null`,
		`DateField is not null`,
		`TimeField > "13:45:30"`,
		`TimeField <= "24:00:00"`,
		`TimeField == "00:00:00.123456"`,
		`TimeField in ["00:00:00", "12:00:00"]`,
		`TimeField is null`,
		`DateField > DateField`,
		`TimeField != TimeField`,
		`"1970-01-01" <= DateField <= "1970-01-02"`,
		`"00:00:00" < TimeField < "24:00:00"`,
	}
	for _, exprStr := range valid {
		parsed, err := ParseExpr(helper, exprStr, nil)
		assert.NoError(t, err, exprStr)
		assert.NotNil(t, parsed, exprStr)
	}

	invalid := []string{
		`DateField > "2024-06-22T00:00:00Z"`,
		`DateField > "13:45:30"`,
		`TimeField > "2024-06-22"`,
		`TimeField > "12:00:00Z"`,
		`DateField > TimeField`,
		`TimeField > DateField`,
		`DateField > TimestamptzField`,
		`DateField > VarCharField`,
		`TimeField > VarCharField`,
		`DateField + 1 == 2`,
		`TimeField % 2 == 0`,
	}
	for _, expr := range invalid {
		assertInvalidExpr(t, helper, expr)
	}
}

func TestExpr_DateLiteralPacksDays(t *testing.T) {
	helper := newTestSchemaHelper(t)
	expr, err := ParseExpr(helper, `DateField == "1970-01-02"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr.GetUnaryRangeExpr())
	assert.Equal(t, int64(1), expr.GetUnaryRangeExpr().GetValue().GetInt64Val())
	assert.Equal(t, schemapb.DataType_Date, expr.GetUnaryRangeExpr().GetColumnInfo().GetDataType())
}

func TestExpr_DateRangePacksDays(t *testing.T) {
	helper := newTestSchemaHelper(t)
	expr, err := ParseExpr(helper, `"1970-01-01" <= DateField <= "1970-01-02"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr.GetBinaryRangeExpr())
	assert.Equal(t, int64(0), expr.GetBinaryRangeExpr().GetLowerValue().GetInt64Val())
	assert.Equal(t, int64(1), expr.GetBinaryRangeExpr().GetUpperValue().GetInt64Val())
	assert.Equal(t, schemapb.DataType_Date, expr.GetBinaryRangeExpr().GetColumnInfo().GetDataType())
}

func TestExpr_TimeLiteralPacksMicros(t *testing.T) {
	helper := newTestSchemaHelper(t)
	expr, err := ParseExpr(helper, `TimeField == "00:00:01"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr.GetUnaryRangeExpr())
	assert.Equal(t, datetime.MicrosPerSecond, expr.GetUnaryRangeExpr().GetValue().GetInt64Val())
	assert.Equal(t, schemapb.DataType_Time, expr.GetUnaryRangeExpr().GetColumnInfo().GetDataType())
}

func TestGetTargetTypeDateTime(t *testing.T) {
	got, err := getTargetType(schemapb.DataType_Date, schemapb.DataType_Date)
	require.NoError(t, err)
	assert.Equal(t, schemapb.DataType_Date, got)

	got, err = getTargetType(schemapb.DataType_Time, schemapb.DataType_Time)
	require.NoError(t, err)
	assert.Equal(t, schemapb.DataType_Time, got)

	_, err = getTargetType(schemapb.DataType_Date, schemapb.DataType_Time)
	assert.Error(t, err)
}
