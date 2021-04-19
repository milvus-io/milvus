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

package proxyservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobalNodeInfoTable_Register(t *testing.T) {
	table := newGlobalNodeInfoTable()

	idInfoMaps := map[UniqueID]*nodeInfo{
		0: {"localhost", 1080},
		1: {"localhost", 1081},
	}

	var err error

	err = table.Register(0, idInfoMaps[0])
	assert.Equal(t, nil, err)

	err = table.Register(1, idInfoMaps[1])
	assert.Equal(t, nil, err)

	/************** duplicated register ***************/

	err = table.Register(0, idInfoMaps[0])
	assert.Equal(t, nil, err)

	err = table.Register(1, idInfoMaps[1])
	assert.Equal(t, nil, err)
}

func TestGlobalNodeInfoTable_Pick(t *testing.T) {
	table := newGlobalNodeInfoTable()

	var err error

	_, err = table.Pick()
	assert.NotEqual(t, nil, err)

	idInfoMaps := map[UniqueID]*nodeInfo{
		0: {"localhost", 1080},
		1: {"localhost", 1081},
	}

	err = table.Register(0, idInfoMaps[0])
	assert.Equal(t, nil, err)

	err = table.Register(1, idInfoMaps[1])
	assert.Equal(t, nil, err)

	num := 10
	for i := 0; i < num; i++ {
		_, err = table.Pick()
		assert.Equal(t, nil, err)
	}
}

func TestGlobalNodeInfoTable_ObtainAllClients(t *testing.T) {
	table := newGlobalNodeInfoTable()

	var err error

	clients, err := table.ObtainAllClients()
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(clients))
}

func TestGlobalNodeInfoTable_ReleaseAllClients(t *testing.T) {
	table := newGlobalNodeInfoTable()

	err := table.ReleaseAllClients()
	assert.Equal(t, nil, err)
}
