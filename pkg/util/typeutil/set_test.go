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

func TestUniqueSet(t *testing.T) {
	set := make(UniqueSet)
	set.Insert(5, 7, 9)
	assert.True(t, set.Contain(5))
	assert.True(t, set.Contain(7))
	assert.True(t, set.Contain(9))
	assert.True(t, set.Contain(5, 7, 9))

	set.Remove(7)
	assert.True(t, set.Contain(5))
	assert.False(t, set.Contain(7))
	assert.True(t, set.Contain(9))
	assert.False(t, set.Contain(5, 7, 9))
}

func TestUniqueSetClear(t *testing.T) {
	set := make(UniqueSet)
	set.Insert(5, 7, 9)
	assert.True(t, set.Contain(5))
	assert.True(t, set.Contain(7))
	assert.True(t, set.Contain(9))
	assert.Equal(t, 3, set.Len())

	set.Clear()
	assert.False(t, set.Contain(5))
	assert.False(t, set.Contain(7))
	assert.False(t, set.Contain(9))
	assert.Equal(t, 0, set.Len())
}

func TestConcurrentSet(t *testing.T) {
	set := NewConcurrentSet[int]()
	set.Insert(1)
	set.Insert(2)
	set.Insert(3)

	count := 0
	set.Range(func(element int) bool {
		count++
		return true
	})

	assert.Len(t, set.Collect(), count)
}
