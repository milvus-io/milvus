// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package bloomfilter

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// buildBinaryFromBlockedBF converts a blockedBloomFilter to binary block data
// and returns (data, numBlocks, k) for constructing a MmapBloomFilter.
func buildBinaryFromBlockedBF(t *testing.T, bf *blockedBloomFilter) ([]byte, uint32, int) {
	data, err := DumpBlockData(bf)
	require.NoError(t, err)

	numBlocks := uint32(len(data) / bfBlockBytes)
	k := int(bf.K())

	return data, numBlocks, k
}

func TestMmapBloomFilter_Consistency(t *testing.T) {
	// Build a blocked bloom filter with known data
	bf := newBlockedBloomFilter(10000, 0.001)

	// Add a set of int64 keys
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i))
		bf.Add(key)
		keys[i] = key
	}

	// Build MmapBloomFilter from the same data
	data, numBlocks, k := buildBinaryFromBlockedBF(t, bf)
	mmapBF := NewMmapBloomFilter(data, 0, numBlocks, k)

	// Verify all added keys test positive in both
	for i, key := range keys {
		origResult := bf.Test(key)
		mmapResult := mmapBF.Test(key)
		assert.Equal(t, origResult, mmapResult, "key %d: original=%v, mmap=%v", i, origResult, mmapResult)
		assert.True(t, mmapResult, "key %d should be found", i)
	}

	// Verify non-existent keys have same behavior
	falsePositives := 0
	for i := 1000; i < 2000; i++ {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i))
		origResult := bf.Test(key)
		mmapResult := mmapBF.Test(key)
		assert.Equal(t, origResult, mmapResult, "non-existent key %d: original=%v, mmap=%v", i, origResult, mmapResult)
		if mmapResult {
			falsePositives++
		}
	}
	// FPR should be reasonable (< 1% with our settings)
	assert.Less(t, falsePositives, 20, "too many false positives: %d/1000", falsePositives)
}

func TestMmapBloomFilter_TestString(t *testing.T) {
	bf := newBlockedBloomFilter(10000, 0.001)

	strings := []string{"hello", "world", "foo", "bar", "milvus"}
	for _, s := range strings {
		bf.AddString(s)
	}

	data, numBlocks, k := buildBinaryFromBlockedBF(t, bf)
	mmapBF := NewMmapBloomFilter(data, 0, numBlocks, k)

	for _, s := range strings {
		assert.Equal(t, bf.TestString(s), mmapBF.TestString(s), "string %q", s)
		assert.True(t, mmapBF.TestString(s), "string %q should be found", s)
	}
}

func TestMmapBloomFilter_TestLocations(t *testing.T) {
	bf := newBlockedBloomFilter(10000, 0.001)

	key := []byte("test_key")
	bf.Add(key)

	data, numBlocks, k := buildBinaryFromBlockedBF(t, bf)
	mmapBF := NewMmapBloomFilter(data, 0, numBlocks, k)

	h := xxh3.Hash(key)
	locs := []uint64{h}

	assert.Equal(t, bf.TestLocations(locs), mmapBF.TestLocations(locs))
	assert.True(t, mmapBF.TestLocations(locs))

	// Empty locs should return true (false positive by convention)
	assert.True(t, mmapBF.TestLocations(nil))
	assert.True(t, mmapBF.TestLocations([]uint64{h, 0})) // len != 1
}

func TestMmapBloomFilter_BatchTestLocations(t *testing.T) {
	bf := newBlockedBloomFilter(10000, 0.001)

	keys := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i))
		bf.Add(key)
		keys[i] = key
	}

	data, numBlocks, k := buildBinaryFromBlockedBF(t, bf)
	mmapBF := NewMmapBloomFilter(data, 0, numBlocks, k)

	locs := make([][]uint64, len(keys))
	for i, key := range keys {
		locs[i] = []uint64{xxh3.Hash(key)}
	}

	hits := make([]bool, len(keys))
	origResult := bf.BatchTestLocations(locs, hits)

	hits2 := make([]bool, len(keys))
	mmapResult := mmapBF.BatchTestLocations(locs, hits2)

	for i := range origResult {
		assert.Equal(t, origResult[i], mmapResult[i], "batch result %d mismatch", i)
	}
}

func TestMmapBloomFilter_Cap(t *testing.T) {
	bf := newBlockedBloomFilter(10000, 0.001)
	data, numBlocks, k := buildBinaryFromBlockedBF(t, bf)
	mmapBF := NewMmapBloomFilter(data, 0, numBlocks, k)

	assert.Equal(t, bf.Cap(), mmapBF.Cap())
	assert.Equal(t, BlockedBF, mmapBF.Type())
	assert.Equal(t, bf.K(), mmapBF.K())
}

func TestMmapBloomFilter_Offset(t *testing.T) {
	// Test that MmapBloomFilter works with a non-zero offset
	bf := newBlockedBloomFilter(10000, 0.001)

	key := []byte("offset_test")
	bf.Add(key)

	rawData, numBlocks, k := buildBinaryFromBlockedBF(t, bf)

	// Prepend some header data
	header := make([]byte, 128)
	copy(header, "some_header_data")
	dataWithHeader := append(header, rawData...)

	mmapBF := NewMmapBloomFilter(dataWithHeader, 128, numBlocks, k)
	assert.True(t, mmapBF.Test(key))

	// Should not find non-existent key
	notFound := mmapBF.Test([]byte("not_in_filter"))
	_ = notFound // may or may not be false positive
}

func TestMmapBloomFilter_ReadOnly(t *testing.T) {
	bf := newBlockedBloomFilter(100, 0.01)
	data, numBlocks, k := buildBinaryFromBlockedBF(t, bf)
	mmapBF := NewMmapBloomFilter(data, 0, numBlocks, k)

	assert.Panics(t, func() { mmapBF.Add([]byte("test")) })
	assert.Panics(t, func() { mmapBF.AddString("test") })
	assert.Panics(t, func() { mmapBF.MarshalJSON() })
	assert.Panics(t, func() { mmapBF.UnmarshalJSON([]byte("{}")) })
}

func TestMmapBloomFilter_LargeScale(t *testing.T) {
	// Test with larger data to ensure correctness at scale
	bf := newBlockedBloomFilter(200000, 0.001)

	numKeys := 100000
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i))
		bf.Add(key)
		keys[i] = key
	}

	data, numBlocks, k := buildBinaryFromBlockedBF(t, bf)
	mmapBF := NewMmapBloomFilter(data, 0, numBlocks, k)

	// Sample check - verify 1000 random keys
	for i := 0; i < numKeys; i += 100 {
		assert.True(t, mmapBF.Test(keys[i]), "key %d should be found", i)
	}

	// Check some non-existent keys
	mismatches := 0
	for i := numKeys; i < numKeys+10000; i++ {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i))
		if bf.Test(key) != mmapBF.Test(key) {
			mismatches++
		}
	}
	assert.Equal(t, 0, mismatches, "all non-existent key results should match")

	fmt.Printf("MmapBF: numBlocks=%d, k=%d, cap=%d bits\n", numBlocks, k, mmapBF.Cap())
}
