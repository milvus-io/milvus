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

package util

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringAndByte(t *testing.T) {
	str := "hello"
	assert.Equal(t, []byte(str), ToBytes(str))
	assert.Equal(t, str, ToString(ToBytes(str)))
}

func TestToPhysicalChannel(t *testing.T) {
	assert.Equal(t, "abc", ToPhysicalChannel("abc_"))
	assert.Equal(t, "abc", ToPhysicalChannel("abc_123"))
	assert.Equal(t, "abc", ToPhysicalChannel("abc_defgsg"))
	assert.Equal(t, "abc__", ToPhysicalChannel("abc___defgsg"))
	assert.Equal(t, "abcdef", ToPhysicalChannel("abcdef"))
}

func TestBase64Encode(t *testing.T) {
	str := "foo"
	encodeStr := Base64Encode(str)
	assert.NotEmpty(t, encodeStr)
	decodeByte, err := base64.StdEncoding.DecodeString(encodeStr)
	assert.NoError(t, err)
	var s string
	err = json.Unmarshal(decodeByte, &s)
	assert.NoError(t, err)
	assert.Equal(t, str, s)
}

func TestChan(t *testing.T) {
	s := []string{"a", "b", "c"}
	sByte, err := json.Marshal(s)
	assert.NoError(t, err)
	var a []string
	err = json.Unmarshal(sByte, &a)
	assert.NoError(t, err)
}
