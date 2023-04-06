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

package uniquegenerator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNaiveUniqueIntGenerator_GetInts(t *testing.T) {
	generator := NewNaiveUniqueIntGenerator()

	count := 10
	begin, end := generator.GetInts(10)
	assert.Equal(t, count, end-begin)
}

func TestNaiveUniqueIntGenerator_GetInt(t *testing.T) {
	exists := make(map[int]bool)
	num := 10

	generator := NewNaiveUniqueIntGenerator()

	for i := 0; i < num; i++ {
		g := generator.GetInt()
		_, ok := exists[g]
		assert.False(t, ok)
		exists[g] = true
	}
}
