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

package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddOne(t *testing.T) {
	input := ""
	output := AddOne(input)
	assert.Equal(t, output, "")

	input = "a"
	output = AddOne(input)
	assert.Equal(t, output, "b")

	input = "aaa="
	output = AddOne(input)
	assert.Equal(t, output, "aaa>")

	// test the increate case
	binary := []byte{1, 20, 255}
	input = string(binary)
	output = AddOne(input)
	assert.Equal(t, len(output), 4)
	resultb := []byte(output)
	assert.Equal(t, resultb, []byte{1, 20, 255, 0})
}

func TestAfter(t *testing.T) {
	res := After("abc", "x")
	assert.Equal(t, res, "")

	res = After("abc", "bc")
	assert.Equal(t, res, "")

	res = After("abc", "ab")
	assert.Equal(t, res, "c")
}
