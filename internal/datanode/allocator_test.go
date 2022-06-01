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

package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllocator_Basic(t *testing.T) {
	ms := &RootCoordFactory{}
	allocator := newAllocator(ms)

	t.Run("Test allocID", func(t *testing.T) {
		ms.setID(666)
		_, err := allocator.allocID()
		assert.NoError(t, err)

		ms.setID(-1)
		_, err = allocator.allocID()
		assert.Error(t, err)
	})

	t.Run("Test alloc ID batch", func(t *testing.T) {
		// If id == 0, AllocID will return not successful status
		// If id == -1, AllocID will return err with nil status
		ms.setID(666)
		_, count, err := allocator.allocIDBatch(10)
		assert.NoError(t, err)
		assert.EqualValues(t, 10, count)

		ms.setID(0)
		_, _, err = allocator.allocIDBatch(10)
		assert.Error(t, err)

		ms.setID(-1)
		_, _, err = allocator.allocIDBatch(10)
		assert.Error(t, err)
	})

	t.Run("Test genKey", func(t *testing.T) {
		ms.setID(666)

		type Test struct {
			inIDs  []UniqueID
			outKey string

			description string
		}

		tests := []Test{
			{[]UniqueID{}, "666", "genKey with empty input ids"},
			{[]UniqueID{1}, "1/666", "genKey with 1 input id"},
			{[]UniqueID{1, 2, 3}, "1/2/3/666", "genKey with input 3 ids"},
			{[]UniqueID{2, 2, 2}, "2/2/2/666", "genKey with input 3 ids"},
		}

		for i, test := range tests {
			key, err := allocator.genKey(test.inIDs...)
			assert.NoError(t, err)
			assert.Equalf(t, test.outKey, key, "#%d", i)
		}

		// Status.ErrorCode != Success
		ms.setID(0)
		tests = []Test{
			{[]UniqueID{}, "", "error rpc status"},
			{[]UniqueID{1}, "", "error rpc status"},
		}

		for _, test := range tests {
			k, err := allocator.genKey(test.inIDs...)
			assert.Error(t, err)
			assert.Equal(t, test.outKey, k)
		}

		// Grpc error
		ms.setID(-1)
		tests = []Test{
			{[]UniqueID{}, "", "error rpc"},
			{[]UniqueID{1}, "", "error rpc"},
		}
		for _, test := range tests {
			k, err := allocator.genKey(test.inIDs...)
			assert.Error(t, err)
			assert.Equal(t, test.outKey, k)
		}
	})
}
