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

package memkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryKV_LoadPartial(t *testing.T) {
	memKV := NewMemoryKV()

	key := "TestMemoryKV_LoadPartial_key"
	value := "TestMemoryKV_LoadPartial_value"

	err := memKV.Save(key, value)
	assert.NoError(t, err)

	var start, end int64
	var partial []byte

	// case 0 <= start && start = end && end <= int64(len(value))

	start, end = 1, 2
	partial, err = memKV.LoadPartial(key, start, end)
	assert.NoError(t, err)
	assert.ElementsMatch(t, partial, []byte(value[start:end]))

	start, end = int64(len(value)-2), int64(len(value)-1)
	partial, err = memKV.LoadPartial(key, start, end)
	assert.NoError(t, err)
	assert.ElementsMatch(t, partial, []byte(value[start:end]))

	// error case
	start, end = 5, 3
	_, err = memKV.LoadPartial(key, start, end)
	assert.Error(t, err)

	start, end = 1, 1
	_, err = memKV.LoadPartial(key, start, end)
	assert.Error(t, err)

	err = memKV.Remove(key)
	assert.NoError(t, err)
	start, end = 1, 2
	_, err = memKV.LoadPartial(key, start, end)
	assert.Error(t, err)
}

func TestMemoryKV_GetSize(t *testing.T) {
	memKV := NewMemoryKV()

	key := "TestMemoryKV_GetSize_key"
	value := "TestMemoryKV_GetSize_value"

	err := memKV.Save(key, value)
	assert.NoError(t, err)

	size, err := memKV.GetSize(key)
	assert.NoError(t, err)
	assert.Equal(t, size, int64(len(value)))

	key2 := "TestMemoryKV_GetSize_key2"

	size, err = memKV.GetSize(key2)
	assert.Error(t, err)
	assert.Equal(t, int64(0), size)
}
