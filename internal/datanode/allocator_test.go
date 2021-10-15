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
	})

	t.Run("Test alloc ID batch", func(t *testing.T) {
		// If id == 0, AllocID will return not successful status
		// If id == -1, AllocID will return err
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

		type in struct {
			isalloc bool
			ids     []UniqueID
		}

		type out struct {
			key string
			err error
		}

		type Test struct {
			in
			out
		}

		tests := []Test{
			{in{true, []UniqueID{}}, out{"666", nil}},
			{in{true, []UniqueID{1}}, out{"1/666", nil}},
			{in{true, make([]UniqueID, 0)}, out{"666", nil}},
			{in{false, []UniqueID{}}, out{"", nil}},
			{in{false, []UniqueID{1, 2, 3}}, out{"1/2/3", nil}},
			{in{false, []UniqueID{1}}, out{"1", nil}},
			{in{false, []UniqueID{2, 2, 2}}, out{"2/2/2", nil}},
		}

		for i, test := range tests {
			key, err := allocator.genKey(test.in.isalloc, test.in.ids...)

			assert.Equalf(t, test.out.key, key, "#%d", i)
			assert.Equalf(t, test.out.err, err, "#%d", i)
		}

		// Status.ErrorCode != Success
		ms.setID(0)
		tests = []Test{
			{in{true, []UniqueID{}}, out{}},
			{in{true, []UniqueID{1}}, out{}},
			{in{true, make([]UniqueID, 0)}, out{}},
		}

		for i, test := range tests {
			_, err := allocator.genKey(test.in.isalloc, test.in.ids...)
			assert.Errorf(t, err, "number: %d", i)
		}

		// Grpc error
		ms.setID(-1)
		tests = []Test{
			{in{true, make([]UniqueID, 0)}, out{}},
			{in{true, []UniqueID{1}}, out{}},
			{in{true, make([]UniqueID, 0)}, out{}},
		}

		for i, test := range tests {
			_, err := allocator.genKey(test.in.isalloc, test.in.ids...)
			assert.Errorf(t, err, "number: %d", i)
		}

		// RootCoord's unavailability doesn't affects genKey when alloc == false
		tests = []Test{
			{in{false, []UniqueID{1, 2, 3}}, out{"1/2/3", nil}},
			{in{false, []UniqueID{1}}, out{"1", nil}},
			{in{false, []UniqueID{2, 2, 2}}, out{"2/2/2", nil}},
		}

		for i, test := range tests {
			key, err := allocator.genKey(test.in.isalloc, test.in.ids...)
			assert.Equalf(t, test.out.key, key, "#%d", i)
			assert.Equalf(t, test.out.err, err, "#%d", i)
		}
	})
}
