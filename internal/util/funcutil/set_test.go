// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SetContain(t *testing.T) {
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"

	// len(m1) < len(m2)
	m1 := make(map[interface{}]struct{})
	m2 := make(map[interface{}]struct{})
	m1[key1] = struct{}{}
	m2[key1] = struct{}{}
	m2[key2] = struct{}{}
	assert.False(t, SetContain(m1, m2))

	// len(m1) >= len(m2), but m2 contains other key not in m1
	m1[key3] = struct{}{}
	assert.False(t, SetContain(m1, m2))

	// m1 contains m2
	m1[key2] = struct{}{}
	assert.True(t, SetContain(m1, m2))
}
