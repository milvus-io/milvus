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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	pkgtypeutil "github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestLocatePartitionNamesByRoutingTableMatchesLegacyModulo(t *testing.T) {
	t.Run("int64 keys", func(t *testing.T) {
		partitions := []string{"p_0", "p_1", "p_2", "p_3"}
		keys := []int64{0, 1, 10, 100, 1000, -1}
		expected := make(map[string]struct{})
		for _, key := range keys {
			hash, err := pkgtypeutil.Hash32Int64(key)
			assert.NoError(t, err)
			expected[partitions[hash%uint32(len(partitions))]] = struct{}{}
		}

		got, err := locatePartitionNamesByRoutingTable(keys, partitions, int64PartitionKeyHasher{})

		assert.NoError(t, err)
		assert.ElementsMatch(t, mapKeys(expected), got)
	})

	t.Run("varchar keys", func(t *testing.T) {
		partitions := []string{"p_0", "p_1", "p_2"}
		keys := []string{"ab", "bc", "bc", "abd", "milvus"}
		expected := make(map[string]struct{})
		for _, key := range keys {
			hash := pkgtypeutil.HashString2Uint32(key)
			expected[partitions[hash%uint32(len(partitions))]] = struct{}{}
		}

		got, err := locatePartitionNamesByRoutingTable(keys, partitions, stringPartitionKeyHasher{})

		assert.NoError(t, err)
		assert.ElementsMatch(t, mapKeys(expected), got)
	})
}

func TestHashKey2PartitionsMatchesLegacyModulo(t *testing.T) {
	partitions := []string{"p_0", "p_1", "p_2"}

	t.Run("int64 keys", func(t *testing.T) {
		keys := []*planpb.GenericValue{
			{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
			{Val: &planpb.GenericValue_Int64Val{Int64Val: 100}},
			{Val: &planpb.GenericValue_Int64Val{Int64Val: 1000}},
		}
		expected := make(map[string]struct{})
		for _, key := range keys {
			hash, err := pkgtypeutil.Hash32Int64(key.GetInt64Val())
			assert.NoError(t, err)
			expected[partitions[hash%uint32(len(partitions))]] = struct{}{}
		}

		got, err := HashKey2Partitions(&schemapb.FieldSchema{DataType: schemapb.DataType_Int64}, keys, partitions)

		assert.NoError(t, err)
		assert.ElementsMatch(t, mapKeys(expected), got)
	})

	t.Run("varchar keys", func(t *testing.T) {
		keys := []*planpb.GenericValue{
			{Val: &planpb.GenericValue_StringVal{StringVal: "ab"}},
			{Val: &planpb.GenericValue_StringVal{StringVal: "bc"}},
			{Val: &planpb.GenericValue_StringVal{StringVal: "milvus"}},
		}
		expected := make(map[string]struct{})
		for _, key := range keys {
			hash := pkgtypeutil.HashString2Uint32(key.GetStringVal())
			expected[partitions[hash%uint32(len(partitions))]] = struct{}{}
		}

		got, err := HashKey2Partitions(&schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}, keys, partitions)

		assert.NoError(t, err)
		assert.ElementsMatch(t, mapKeys(expected), got)
	})
}

func mapKeys[K comparable](values map[K]struct{}) []K {
	keys := make([]K, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
