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

package datetime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDate(t *testing.T) {
	days, err := ParseDate("1970-01-01")
	require.NoError(t, err)
	assert.Equal(t, int32(0), days)

	days, err = ParseDate("1970-01-02")
	require.NoError(t, err)
	assert.Equal(t, int32(1), days)

	days, err = ParseDate("1969-12-31")
	require.NoError(t, err)
	assert.Equal(t, int32(-1), days)

	days, err = ParseDate("2024-02-29")
	require.NoError(t, err)
	assert.Equal(t, "2024-02-29", FormatDate(days))
}

func TestParseDateRejects(t *testing.T) {
	invalid := []string{
		"",
		"2024-6-22",
		"2024/06/22",
		"2024-06-22T00:00:00",
		"2024-06-22T00:00:00Z",
		"2024-06-22 00:00:00",
		"not-a-date",
		"2024-02-30",
		"2023-02-29",
		"2024-13-01",
	}
	for _, s := range invalid {
		_, err := ParseDate(s)
		assert.Error(t, err, s)
	}
}

func TestParseTime(t *testing.T) {
	micros, err := ParseTime("00:00:00")
	require.NoError(t, err)
	assert.Equal(t, int64(0), micros)

	micros, err = ParseTime("00:00:01")
	require.NoError(t, err)
	assert.Equal(t, MicrosPerSecond, micros)

	micros, err = ParseTime("01:00:00")
	require.NoError(t, err)
	assert.Equal(t, int64(3600)*MicrosPerSecond, micros)

	micros, err = ParseTime("13:45:30.123456")
	require.NoError(t, err)
	assert.Equal(t, "13:45:30.123456", FormatTime(micros))

	micros, err = ParseTime("24:00:00")
	require.NoError(t, err)
	assert.Equal(t, MaxTimeMicros, micros)
	assert.Equal(t, "24:00:00", FormatTime(micros))

	micros, err = ParseTime("24:00:00.000000")
	require.NoError(t, err)
	assert.Equal(t, MaxTimeMicros, micros)
}

func TestParseTimeRejects(t *testing.T) {
	invalid := []string{
		"",
		"1:00:00",
		"25:00:00",
		"12:00:00Z",
		"12:00:00+08:00",
		"12:00:00-05:00",
		"2024-06-22T12:00:00",
		"12:00:00.1234567890",
		"24:00:00.1",
	}
	for _, s := range invalid {
		_, err := ParseTime(s)
		assert.Error(t, err, s)
	}
}

func TestFormatTimeTrimsFraction(t *testing.T) {
	micros, err := ParseTime("12:00:00.100000")
	require.NoError(t, err)
	assert.Equal(t, "12:00:00.1", FormatTime(micros))
}

func TestValidateTimeMicros(t *testing.T) {
	assert.NoError(t, ValidateTimeMicros(0))
	assert.NoError(t, ValidateTimeMicros(MaxTimeMicros))
	assert.Error(t, ValidateTimeMicros(-1))
	assert.Error(t, ValidateTimeMicros(MaxTimeMicros+1))
}
