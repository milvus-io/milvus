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

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestGenerateBalancedInt64PKs(t *testing.T) {
	t.Run("basic_functionality", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		pks := GenerateBalancedInt64PKs(numRows, numChannels)

		assert.Equal(t, numRows, len(pks), "should generate correct number of PKs")
	})

	t.Run("zero_channels_defaults_to_one", func(t *testing.T) {
		numRows := 10
		pks := GenerateBalancedInt64PKs(numRows, 0)

		assert.Equal(t, numRows, len(pks), "should generate correct number of PKs")
	})

	t.Run("negative_channels_defaults_to_one", func(t *testing.T) {
		numRows := 10
		pks := GenerateBalancedInt64PKs(numRows, -5)

		assert.Equal(t, numRows, len(pks), "should generate correct number of PKs")
	})

	t.Run("balanced_distribution_by_hash", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		pks := GenerateBalancedInt64PKs(numRows, numChannels)

		// Verify distribution by hashing PKs
		channelCounts := make(map[int]int)
		for _, pk := range pks {
			hash := hashInt64ForChannel(pk)
			ch := int(hash % uint32(numChannels))
			channelCounts[ch]++
		}

		// Each channel should have 25 PKs (100/4 = 25)
		expectedCount := numRows / numChannels
		for ch := 0; ch < numChannels; ch++ {
			assert.Equal(t, expectedCount, channelCounts[ch],
				"channel %d should have %d PKs", ch, expectedCount)
		}
	})

	t.Run("remainder_distribution", func(t *testing.T) {
		numRows := 10
		numChannels := 3
		pks := GenerateBalancedInt64PKs(numRows, numChannels)

		channelCounts := make(map[int]int)
		for _, pk := range pks {
			hash := hashInt64ForChannel(pk)
			ch := int(hash % uint32(numChannels))
			channelCounts[ch]++
		}

		// 10 / 3 = 3 base, remainder = 1
		// Channel 0: 4 PKs (3 + 1 from remainder)
		// Channel 1: 3 PKs
		// Channel 2: 3 PKs
		assert.Equal(t, 4, channelCounts[0], "channel 0 should have 4 PKs")
		assert.Equal(t, 3, channelCounts[1], "channel 1 should have 3 PKs")
		assert.Equal(t, 3, channelCounts[2], "channel 2 should have 3 PKs")
	})

	t.Run("unique_pks", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		pks := GenerateBalancedInt64PKs(numRows, numChannels)

		// Verify all PKs are unique
		seen := make(map[int64]bool)
		for _, pk := range pks {
			assert.False(t, seen[pk], "PK %d should be unique", pk)
			seen[pk] = true
		}
	})

	t.Run("positive_pks", func(t *testing.T) {
		numRows := 50
		numChannels := 5
		pks := GenerateBalancedInt64PKs(numRows, numChannels)

		for _, pk := range pks {
			assert.Greater(t, pk, int64(0), "PKs should be positive")
		}
	})
}

func TestHashInt64ForChannel(t *testing.T) {
	t.Run("consistency", func(t *testing.T) {
		// Same input should always produce same output
		pk := int64(12345)
		hash1 := hashInt64ForChannel(pk)
		hash2 := hashInt64ForChannel(pk)

		assert.Equal(t, hash1, hash2, "same input should produce same hash")
	})

	t.Run("different_inputs_different_hashes", func(t *testing.T) {
		// Different inputs should generally produce different hashes
		// (with very high probability)
		hashes := make(map[uint32]int64)
		collisions := 0

		for pk := int64(1); pk <= 1000; pk++ {
			hash := hashInt64ForChannel(pk)
			if existingPK, exists := hashes[hash]; exists {
				collisions++
				t.Logf("collision: PK %d and %d both hash to %d", pk, existingPK, hash)
			}
			hashes[hash] = pk
		}

		// Allow a small number of collisions (hash collisions are possible)
		assert.Less(t, collisions, 10,
			"too many hash collisions for first 1000 PKs")
	})

	t.Run("non_negative_result", func(t *testing.T) {
		// The hash should always be non-negative (due to & 0x7fffffff)
		testCases := []int64{0, 1, -1, 100, -100, 1 << 62, -(1 << 62)}

		for _, pk := range testCases {
			hash := hashInt64ForChannel(pk)
			assert.GreaterOrEqual(t, hash, uint32(0),
				"hash for PK %d should be non-negative", pk)
		}
	})

	t.Run("distribution_across_channels", func(t *testing.T) {
		// Test that hashes distribute well across channels
		numChannels := 8
		channelCounts := make(map[int]int)

		for pk := int64(1); pk <= 8000; pk++ {
			hash := hashInt64ForChannel(pk)
			ch := int(hash % uint32(numChannels))
			channelCounts[ch]++
		}

		// Each channel should have roughly 1000 items (8000/8)
		// Allow 20% variance
		expectedCount := 1000
		tolerance := 200

		for ch := 0; ch < numChannels; ch++ {
			count := channelCounts[ch]
			assert.Greater(t, count, expectedCount-tolerance,
				"channel %d has too few items: %d", ch, count)
			assert.Less(t, count, expectedCount+tolerance,
				"channel %d has too many items: %d", ch, count)
		}
	})
}

func TestGenerateChannelBalancedPrimaryKeys(t *testing.T) {
	t.Run("int64_type", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		fieldName := "test_pk"

		fieldData := GenerateChannelBalancedPrimaryKeys(fieldName, schemapb.DataType_Int64, numRows, numChannels)

		assert.Equal(t, schemapb.DataType_Int64, fieldData.GetType())
		assert.Equal(t, fieldName, fieldData.GetFieldName())

		pks := fieldData.GetScalars().GetLongData().GetData()
		assert.Equal(t, numRows, len(pks))

		// Verify balanced distribution
		channelCounts := make(map[int]int)
		for _, pk := range pks {
			hash := hashInt64ForChannel(pk)
			ch := int(hash % uint32(numChannels))
			channelCounts[ch]++
		}

		expectedCount := numRows / numChannels
		for ch := 0; ch < numChannels; ch++ {
			assert.Equal(t, expectedCount, channelCounts[ch],
				"channel %d should have %d PKs", ch, expectedCount)
		}
	})

	t.Run("varchar_type", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		fieldName := "test_varchar_pk"

		fieldData := GenerateChannelBalancedPrimaryKeys(fieldName, schemapb.DataType_VarChar, numRows, numChannels)

		assert.Equal(t, schemapb.DataType_VarChar, fieldData.GetType())
		assert.Equal(t, fieldName, fieldData.GetFieldName())

		pks := fieldData.GetScalars().GetStringData().GetData()
		assert.Equal(t, numRows, len(pks))

		// Verify balanced distribution
		channelCounts := make(map[int]int)
		for _, pk := range pks {
			hash := hashVarCharForChannel(pk)
			ch := int(hash % uint32(numChannels))
			channelCounts[ch]++
		}

		expectedCount := numRows / numChannels
		for ch := 0; ch < numChannels; ch++ {
			assert.Equal(t, expectedCount, channelCounts[ch],
				"channel %d should have %d PKs", ch, expectedCount)
		}
	})

	t.Run("string_type_as_varchar", func(t *testing.T) {
		numRows := 50
		numChannels := 2
		fieldName := "string_pk"

		fieldData := GenerateChannelBalancedPrimaryKeys(fieldName, schemapb.DataType_String, numRows, numChannels)

		// String type should be treated as VarChar
		assert.Equal(t, schemapb.DataType_VarChar, fieldData.GetType())
		assert.Equal(t, fieldName, fieldData.GetFieldName())

		pks := fieldData.GetScalars().GetStringData().GetData()
		assert.Equal(t, numRows, len(pks))
	})

	t.Run("unsupported_type_panics", func(t *testing.T) {
		assert.Panics(t, func() {
			GenerateChannelBalancedPrimaryKeys("test", schemapb.DataType_Float, 10, 2)
		}, "unsupported type should panic")
	})
}

func TestGenerateBalancedVarCharPKs(t *testing.T) {
	t.Run("basic_functionality", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		pks := GenerateBalancedVarCharPKs(numRows, numChannels)

		assert.Equal(t, numRows, len(pks), "should generate correct number of PKs")
	})

	t.Run("zero_channels_defaults_to_one", func(t *testing.T) {
		numRows := 10
		pks := GenerateBalancedVarCharPKs(numRows, 0)

		assert.Equal(t, numRows, len(pks), "should generate correct number of PKs")
	})

	t.Run("negative_channels_defaults_to_one", func(t *testing.T) {
		numRows := 10
		pks := GenerateBalancedVarCharPKs(numRows, -5)

		assert.Equal(t, numRows, len(pks), "should generate correct number of PKs")
	})

	t.Run("balanced_distribution_by_hash", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		pks := GenerateBalancedVarCharPKs(numRows, numChannels)

		// Verify distribution by hashing PKs
		channelCounts := make(map[int]int)
		for _, pk := range pks {
			hash := hashVarCharForChannel(pk)
			ch := int(hash % uint32(numChannels))
			channelCounts[ch]++
		}

		// Each channel should have 25 PKs (100/4 = 25)
		expectedCount := numRows / numChannels
		for ch := 0; ch < numChannels; ch++ {
			assert.Equal(t, expectedCount, channelCounts[ch],
				"channel %d should have %d PKs", ch, expectedCount)
		}
	})

	t.Run("remainder_distribution", func(t *testing.T) {
		numRows := 10
		numChannels := 3
		pks := GenerateBalancedVarCharPKs(numRows, numChannels)

		channelCounts := make(map[int]int)
		for _, pk := range pks {
			hash := hashVarCharForChannel(pk)
			ch := int(hash % uint32(numChannels))
			channelCounts[ch]++
		}

		// 10 / 3 = 3 base, remainder = 1
		// Channel 0: 4 PKs (3 + 1 from remainder)
		// Channel 1: 3 PKs
		// Channel 2: 3 PKs
		assert.Equal(t, 4, channelCounts[0], "channel 0 should have 4 PKs")
		assert.Equal(t, 3, channelCounts[1], "channel 1 should have 3 PKs")
		assert.Equal(t, 3, channelCounts[2], "channel 2 should have 3 PKs")
	})

	t.Run("unique_pks", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		pks := GenerateBalancedVarCharPKs(numRows, numChannels)

		// Verify all PKs are unique
		seen := make(map[string]bool)
		for _, pk := range pks {
			assert.False(t, seen[pk], "PK %s should be unique", pk)
			seen[pk] = true
		}
	})

	t.Run("non_empty_pks", func(t *testing.T) {
		numRows := 50
		numChannels := 5
		pks := GenerateBalancedVarCharPKs(numRows, numChannels)

		for _, pk := range pks {
			assert.NotEmpty(t, pk, "PKs should not be empty")
		}
	})
}

func TestHashVarCharForChannel(t *testing.T) {
	t.Run("consistency", func(t *testing.T) {
		// Same input should always produce same output
		pk := "test_pk_12345"
		hash1 := hashVarCharForChannel(pk)
		hash2 := hashVarCharForChannel(pk)

		assert.Equal(t, hash1, hash2, "same input should produce same hash")
	})

	t.Run("different_inputs_different_hashes", func(t *testing.T) {
		// Different inputs should generally produce different hashes
		hashes := make(map[uint32]string)
		collisions := 0

		for i := 1; i <= 1000; i++ {
			// Use unique pk format: pk_<number>
			pk := fmt.Sprintf("pk_%d", i)
			hash := hashVarCharForChannel(pk)
			if existingPK, exists := hashes[hash]; exists {
				collisions++
				t.Logf("collision: PK %s and %s both hash to %d", pk, existingPK, hash)
			}
			hashes[hash] = pk
		}

		// Allow some collisions (hash collisions are expected)
		assert.Less(t, collisions, 50,
			"too many hash collisions for first 1000 PKs")
	})

	t.Run("substring_limit", func(t *testing.T) {
		// Strings longer than 100 chars should only hash first 100 chars
		base := "a"
		longStr := ""
		for i := 0; i < 150; i++ {
			longStr += base
		}
		shortStr := longStr[:100]

		// Hash of long string should equal hash of first 100 chars
		hashLong := hashVarCharForChannel(longStr)
		hashShort := hashVarCharForChannel(shortStr)

		assert.Equal(t, hashShort, hashLong,
			"hash of long string should equal hash of first 100 chars")
	})

	t.Run("distribution_across_channels", func(t *testing.T) {
		// Test that hashes distribute well across channels
		numChannels := 8
		channelCounts := make(map[int]int)

		for i := 1; i <= 8000; i++ {
			// Use unique pk format for distribution test
			pk := fmt.Sprintf("distribution_test_pk_%d", i)
			hash := hashVarCharForChannel(pk)
			ch := int(hash % uint32(numChannels))
			channelCounts[ch]++
		}

		// Each channel should have roughly 1000 items (8000/8)
		// Allow 20% variance
		expectedCount := 1000
		tolerance := 200

		for ch := 0; ch < numChannels; ch++ {
			count := channelCounts[ch]
			assert.Greater(t, count, expectedCount-tolerance,
				"channel %d has too few items: %d", ch, count)
			assert.Less(t, count, expectedCount+tolerance,
				"channel %d has too many items: %d", ch, count)
		}
	})
}

// TestHashPK2ChannelsIntegration verifies that GenerateChannelBalancedPrimaryKeys
// produces PKs that are evenly distributed when using the actual HashPK2Channels function.
// This is an end-to-end test to ensure our hash implementation matches Milvus's internal implementation.
func TestHashPK2ChannelsIntegration(t *testing.T) {
	t.Run("int64_pk_balanced_with_HashPK2Channels", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		fieldName := "test_pk"

		// Generate balanced PKs
		fieldData := GenerateChannelBalancedPrimaryKeys(fieldName, schemapb.DataType_Int64, numRows, numChannels)
		pks := fieldData.GetScalars().GetLongData().GetData()

		// Create schemapb.IDs for HashPK2Channels
		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: pks,
				},
			},
		}

		// Create shard names
		shardNames := make([]string, numChannels)
		for i := 0; i < numChannels; i++ {
			shardNames[i] = fmt.Sprintf("shard_%d", i)
		}

		// Use actual HashPK2Channels to get channel assignments
		channelIndices := typeutil.HashPK2Channels(ids, shardNames)

		// Count distribution
		channelCounts := make(map[uint32]int)
		for _, ch := range channelIndices {
			channelCounts[ch]++
		}

		// Verify balanced distribution: each channel should have exactly numRows/numChannels
		expectedCount := numRows / numChannels
		for ch := 0; ch < numChannels; ch++ {
			assert.Equal(t, expectedCount, channelCounts[uint32(ch)],
				"channel %d should have exactly %d PKs via HashPK2Channels", ch, expectedCount)
		}
	})

	t.Run("int64_pk_with_remainder", func(t *testing.T) {
		numRows := 10
		numChannels := 3
		fieldName := "test_pk"

		fieldData := GenerateChannelBalancedPrimaryKeys(fieldName, schemapb.DataType_Int64, numRows, numChannels)
		pks := fieldData.GetScalars().GetLongData().GetData()

		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: pks,
				},
			},
		}

		shardNames := make([]string, numChannels)
		for i := 0; i < numChannels; i++ {
			shardNames[i] = fmt.Sprintf("shard_%d", i)
		}

		channelIndices := typeutil.HashPK2Channels(ids, shardNames)

		channelCounts := make(map[uint32]int)
		for _, ch := range channelIndices {
			channelCounts[ch]++
		}

		// 10 / 3 = 3 base, remainder = 1
		// Channel 0: 4, Channel 1: 3, Channel 2: 3
		assert.Equal(t, 4, channelCounts[0], "channel 0 should have 4 PKs")
		assert.Equal(t, 3, channelCounts[1], "channel 1 should have 3 PKs")
		assert.Equal(t, 3, channelCounts[2], "channel 2 should have 3 PKs")
	})

	t.Run("varchar_pk_balanced_with_HashPK2Channels", func(t *testing.T) {
		numRows := 100
		numChannels := 4
		fieldName := "test_varchar_pk"

		fieldData := GenerateChannelBalancedPrimaryKeys(fieldName, schemapb.DataType_VarChar, numRows, numChannels)
		pks := fieldData.GetScalars().GetStringData().GetData()

		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: pks,
				},
			},
		}

		shardNames := make([]string, numChannels)
		for i := 0; i < numChannels; i++ {
			shardNames[i] = fmt.Sprintf("shard_%d", i)
		}

		channelIndices := typeutil.HashPK2Channels(ids, shardNames)

		channelCounts := make(map[uint32]int)
		for _, ch := range channelIndices {
			channelCounts[ch]++
		}

		expectedCount := numRows / numChannels
		for ch := 0; ch < numChannels; ch++ {
			assert.Equal(t, expectedCount, channelCounts[uint32(ch)],
				"channel %d should have exactly %d PKs via HashPK2Channels", ch, expectedCount)
		}
	})

	t.Run("varchar_pk_with_remainder", func(t *testing.T) {
		numRows := 10
		numChannels := 3
		fieldName := "test_varchar_pk"

		fieldData := GenerateChannelBalancedPrimaryKeys(fieldName, schemapb.DataType_VarChar, numRows, numChannels)
		pks := fieldData.GetScalars().GetStringData().GetData()

		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: pks,
				},
			},
		}

		shardNames := make([]string, numChannels)
		for i := 0; i < numChannels; i++ {
			shardNames[i] = fmt.Sprintf("shard_%d", i)
		}

		channelIndices := typeutil.HashPK2Channels(ids, shardNames)

		channelCounts := make(map[uint32]int)
		for _, ch := range channelIndices {
			channelCounts[ch]++
		}

		// 10 / 3 = 3 base, remainder = 1
		assert.Equal(t, 4, channelCounts[0], "channel 0 should have 4 PKs")
		assert.Equal(t, 3, channelCounts[1], "channel 1 should have 3 PKs")
		assert.Equal(t, 3, channelCounts[2], "channel 2 should have 3 PKs")
	})

	t.Run("large_scale_int64_distribution", func(t *testing.T) {
		numRows := 1000
		numChannels := 8
		fieldName := "test_pk"

		fieldData := GenerateChannelBalancedPrimaryKeys(fieldName, schemapb.DataType_Int64, numRows, numChannels)
		pks := fieldData.GetScalars().GetLongData().GetData()

		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: pks,
				},
			},
		}

		shardNames := make([]string, numChannels)
		for i := 0; i < numChannels; i++ {
			shardNames[i] = fmt.Sprintf("shard_%d", i)
		}

		channelIndices := typeutil.HashPK2Channels(ids, shardNames)

		channelCounts := make(map[uint32]int)
		for _, ch := range channelIndices {
			channelCounts[ch]++
		}

		// Each channel should have exactly 125 PKs (1000/8)
		expectedCount := numRows / numChannels
		for ch := 0; ch < numChannels; ch++ {
			assert.Equal(t, expectedCount, channelCounts[uint32(ch)],
				"channel %d should have exactly %d PKs via HashPK2Channels", ch, expectedCount)
		}
	})

	t.Run("large_scale_varchar_distribution", func(t *testing.T) {
		numRows := 1000
		numChannels := 8
		fieldName := "test_varchar_pk"

		fieldData := GenerateChannelBalancedPrimaryKeys(fieldName, schemapb.DataType_VarChar, numRows, numChannels)
		pks := fieldData.GetScalars().GetStringData().GetData()

		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: pks,
				},
			},
		}

		shardNames := make([]string, numChannels)
		for i := 0; i < numChannels; i++ {
			shardNames[i] = fmt.Sprintf("shard_%d", i)
		}

		channelIndices := typeutil.HashPK2Channels(ids, shardNames)

		channelCounts := make(map[uint32]int)
		for _, ch := range channelIndices {
			channelCounts[ch]++
		}

		// Each channel should have exactly 125 PKs (1000/8)
		expectedCount := numRows / numChannels
		for ch := 0; ch < numChannels; ch++ {
			assert.Equal(t, expectedCount, channelCounts[uint32(ch)],
				"channel %d should have exactly %d PKs via HashPK2Channels", ch, expectedCount)
		}
	})
}
