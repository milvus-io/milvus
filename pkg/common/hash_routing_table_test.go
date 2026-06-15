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

package common

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ RoutingTable[string, RoutingMember] = (*HashRoutingTable[string, RoutingMember])(nil)

type testHasher[K comparable] struct {
	values map[K]uint64
	err    error
}

func (h testHasher[K]) Hash(key K) (uint64, error) {
	if h.err != nil {
		return 0, h.err
	}
	return h.values[key], nil
}

func TestHashRoutingTableLocateKey(t *testing.T) {
	table := NewHashRoutingTable[string](
		[]RoutingMember{"node-1", "node-2", "node-3"},
		testHasher[string]{
			values: map[string]uint64{
				"k0": 0,
				"k1": 1,
				"k4": 4,
			},
		},
	)

	got, err := table.LocateKey("k0")
	require.NoError(t, err)
	assert.Equal(t, RoutingMember("node-1"), got)

	got, err = table.LocateKey("k1")
	require.NoError(t, err)
	assert.Equal(t, RoutingMember("node-2"), got)

	got, err = table.LocateKey("k4")
	require.NoError(t, err)
	assert.Equal(t, RoutingMember("node-2"), got)
}

func TestHashRoutingTableLocateKeyPropagatesHasherError(t *testing.T) {
	expectedErr := errors.New("hash failed")
	table := NewHashRoutingTable[string]([]RoutingMember{"node-1"}, testHasher[string]{err: expectedErr})

	got, err := table.LocateKey("key")

	assert.Empty(t, got)
	assert.ErrorIs(t, err, expectedErr)
}

func TestHashRoutingTableLocateKeyRejectsInvalidTable(t *testing.T) {
	t.Run("empty values", func(t *testing.T) {
		table := NewHashRoutingTable[string, RoutingMember](nil, testHasher[string]{values: map[string]uint64{"key": 1}})

		assert.NotPanics(t, func() {
			_, err := table.LocateKey("key")
			assert.ErrorIs(t, err, ErrRoutingTableNoValues)
		})
	})

	t.Run("nil hasher", func(t *testing.T) {
		table := NewHashRoutingTable[string, RoutingMember]([]RoutingMember{"node-1"}, nil)

		assert.NotPanics(t, func() {
			_, err := table.LocateKey("key")
			assert.ErrorIs(t, err, ErrRoutingTableNoHasher)
		})
	})
}

func TestNewHashRoutingTableClonesValues(t *testing.T) {
	values := []RoutingMember{"node-1"}
	table := NewHashRoutingTable[string](values, testHasher[string]{values: map[string]uint64{"key": 0}})
	values[0] = "node-2"

	got, err := table.LocateKey("key")

	require.NoError(t, err)
	assert.Equal(t, RoutingMember("node-1"), got)
}

func TestHashValueOpCommit(t *testing.T) {
	table := NewHashRoutingTable[string](
		[]RoutingMember{"node-1", "node-2"},
		testHasher[string]{values: map[string]uint64{"key": 1}},
	)

	got, err := table.LocateKey("key")
	require.NoError(t, err)
	assert.Equal(t, RoutingMember("node-2"), got)

	err = table.NewValueOp().Add(RoutingMember("node-3")).Remove(RoutingMember("node-2")).Commit()
	require.NoError(t, err)

	got, err = table.LocateKey("key")
	require.NoError(t, err)
	assert.Equal(t, RoutingMember("node-3"), got)
}
