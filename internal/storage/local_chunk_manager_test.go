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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalChunkManager_GetPath(t *testing.T) {
	lcm := NewLocalChunkManager(localPath)
	path, err := lcm.GetPath("invalid")
	assert.Empty(t, path)
	assert.Error(t, err)
}

func TestLocalChunkManager_Write(t *testing.T) {
	lcm := NewLocalChunkManager(localPath)

	bin := []byte{1, 2, 3}
	err := lcm.Write("1", bin)
	assert.Nil(t, err)
}

func TestLocalChunkManager_Read(t *testing.T) {
	lcm := NewLocalChunkManager(localPath)
	bin, err := lcm.Read("invalid")
	assert.Nil(t, bin)
	assert.Error(t, err)

	content := make([]byte, 8)
	n, err := lcm.ReadAt("invalid", content, 0)
	assert.Zero(t, n)
	assert.Error(t, err)

	bin = []byte{1, 2, 3}
	lcm.Write("1", bin)
	res, err := lcm.Read("1")
	assert.Nil(t, err)
	assert.Equal(t, len(res), len(bin))
}
