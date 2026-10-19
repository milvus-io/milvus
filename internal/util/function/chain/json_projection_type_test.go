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

package chain

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	json "github.com/milvus-io/milvus/internal/json"
)

func TestJSONValueConverters(t *testing.T) {
	boolValue, ok := convertJSONBool(true)
	assert.True(t, ok)
	assert.True(t, boolValue)
	_, ok = convertJSONBool("true")
	assert.False(t, ok)

	intValue, ok := convertJSONInt64(json.Number("9223372036854775807"))
	assert.True(t, ok)
	assert.Equal(t, int64(9223372036854775807), intValue)
	_, ok = convertJSONInt64(json.Number("18446744073709551615"))
	assert.False(t, ok)
	for _, token := range []string{"1.0", "1e2", "1E0"} {
		_, ok = convertJSONInt64(json.Number(token))
		assert.False(t, ok, token)
	}

	doubleValue, ok := convertJSONDouble(json.Number("10"))
	assert.True(t, ok)
	assert.Equal(t, 10.0, doubleValue)
	_, ok = convertJSONDouble(json.Number("1e400"))
	assert.False(t, ok)
	for _, token := range []string{"0", "0.0", "1e-400", "-0.0", "-1e-400"} {
		doubleValue, ok = convertJSONDouble(json.Number(token))
		assert.True(t, ok, token)
		assert.Zero(t, doubleValue, token)
		assert.Equal(t, token[0] == '-', math.Signbit(doubleValue), token)
	}

	stringValue, ok := convertJSONString("value")
	assert.True(t, ok)
	assert.Equal(t, "value", stringValue)
	_, ok = convertJSONString(map[string]any{"nested": true})
	assert.False(t, ok)
}
