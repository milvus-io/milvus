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

package typeutil

import (
	"log"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestUint64(t *testing.T) {
	var i int64 = -1
	var u uint64 = uint64(i)
	t.Log(i)
	t.Log(u)
}

func TestHash32_Uint64(t *testing.T) {
	var u uint64 = 0x12
	h, err := Hash32Uint64(u)
	assert.Nil(t, err)
	t.Log(h)

	h1, err := Hash32Int64(int64(u))
	assert.Nil(t, err)
	t.Log(h1)
	assert.Equal(t, h, h1)

	b := make([]byte, unsafe.Sizeof(u))
	b[0] = 0x12
	h2, err := Hash32Bytes(b)
	assert.Nil(t, err)

	t.Log(h2)
	assert.Equal(t, h, h2)
}

func TestHash32_String(t *testing.T) {
	var u string = "ok"
	h, err := Hash32String(u)
	assert.Nil(t, err)

	t.Log(h)
	log.Println(h)

	b := []byte(u)
	h2, err := Hash32Bytes(b)
	assert.Nil(t, err)
	log.Println(h2)

	assert.Equal(t, uint32(h), h2)
}
