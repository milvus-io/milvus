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
	"fmt"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

func TestPerformance(t *testing.T) {
	capacity := 1000000
	fpr := 0.001

	keys := make([][]byte, 0)
	for i := 0; i < capacity; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%d", i)))
	}

	bf1 := newBlockedBloomFilter(uint(capacity), fpr)
	start1 := time.Now()
	for _, key := range keys {
		bf1.Add(key)
	}
	log.Info("Block BF construct time", zap.Duration("time", time.Since(start1)))
	data, err := bf1.MarshalJSON()
	assert.NoError(t, err)
	log.Info("Block BF size", zap.Int("size", len(data)))

	start2 := time.Now()
	for _, key := range keys {
		bf1.Test(key)
	}
	log.Info("Block BF Test cost", zap.Duration("time", time.Since(start2)))

	bf2 := newBasicBloomFilter(uint(capacity), fpr)
	start3 := time.Now()
	for _, key := range keys {
		bf2.Add(key)
	}
	log.Info("Basic BF construct time", zap.Duration("time", time.Since(start3)))
	data, err = bf2.MarshalJSON()
	assert.NoError(t, err)
	log.Info("Basic BF size", zap.Int("size", len(data)))

	start4 := time.Now()
	for _, key := range keys {
		bf2.Test(key)
	}
	log.Info("Basic BF Test cost", zap.Duration("time", time.Since(start4)))
}

func TestPerformance_MultiBF(t *testing.T) {
	capacity := 100000
	fpr := 0.001

	testKeySize := 100000
	testKeys := make([][]byte, 0)
	for i := 0; i < testKeySize; i++ {
		testKeys = append(testKeys, []byte(fmt.Sprintf("key%d", time.Now().UnixNano()+int64(i))))
	}

	bfNum := 100
	bfs1 := make([]*blockedBloomFilter, 0)
	start1 := time.Now()
	for i := 0; i < bfNum; i++ {
		bf1 := newBlockedBloomFilter(uint(capacity), fpr)
		for j := 0; j < capacity; j++ {
			key := fmt.Sprintf("key%d", time.Now().UnixNano()+int64(i))
			bf1.Add([]byte(key))
		}
		bfs1 = append(bfs1, bf1)
	}

	log.Info("Block BF construct cost", zap.Duration("time", time.Since(start1)))

	start3 := time.Now()
	for _, key := range testKeys {
		locations := Locations(key, bfs1[0].K(), BlockedBF)
		for i := 0; i < bfNum; i++ {
			bfs1[i].TestLocations(locations)
		}
	}
	log.Info("Block BF TestLocation cost", zap.Duration("time", time.Since(start3)))

	bfs2 := make([]*basicBloomFilter, 0)
	start1 = time.Now()
	for i := 0; i < bfNum; i++ {
		bf2 := newBasicBloomFilter(uint(capacity), fpr)
		for _, key := range testKeys {
			bf2.Add(key)
		}
		bfs2 = append(bfs2, bf2)
	}

	log.Info("Basic BF construct cost", zap.Duration("time", time.Since(start1)))

	start3 = time.Now()
	for _, key := range testKeys {
		locations := Locations(key, bfs1[0].K(), BasicBF)
		for i := 0; i < bfNum; i++ {
			bfs2[i].TestLocations(locations)
		}
	}
	log.Info("Basic BF TestLocation cost", zap.Duration("time", time.Since(start3)))
}

func TestPerformance_BatchTestLocations(t *testing.T) {
	capacity := 100000
	fpr := 0.001

	testKeySize := 100000
	testKeys := make([][]byte, 0)
	for i := 0; i < testKeySize; i++ {
		testKeys = append(testKeys, []byte(fmt.Sprintf("key%d", time.Now().UnixNano()+int64(i))))
	}

	batchSize := 1000

	bfNum := 100
	bfs1 := make([]*blockedBloomFilter, 0)
	start1 := time.Now()
	for i := 0; i < bfNum; i++ {
		bf1 := newBlockedBloomFilter(uint(capacity), fpr)
		for j := 0; j < capacity; j++ {
			key := fmt.Sprintf("key%d", time.Now().UnixNano()+int64(i))
			bf1.Add([]byte(key))
		}
		bfs1 = append(bfs1, bf1)
	}

	log.Info("Block BF construct cost", zap.Duration("time", time.Since(start1)))

	start3 := time.Now()
	for _, key := range testKeys {
		locations := Locations(key, bfs1[0].K(), BlockedBF)
		for i := 0; i < bfNum; i++ {
			bfs1[i].TestLocations(locations)
		}
	}
	log.Info("Block BF TestLocation cost", zap.Duration("time", time.Since(start3)))

	start3 = time.Now()
	for i := 0; i < testKeySize; i += batchSize {
		endIdx := i + batchSize
		if endIdx > testKeySize {
			endIdx = testKeySize
		}
		locations := lo.Map(testKeys[i:endIdx], func(key []byte, _ int) []uint64 {
			return Locations(key, bfs1[0].K(), BlockedBF)
		})
		hits := make([]bool, batchSize)
		for j := 0; j < bfNum; j++ {
			bfs1[j].BatchTestLocations(locations, hits)
		}
	}
	log.Info("Block BF BatchTestLocation cost", zap.Duration("time", time.Since(start3)))

	bfs2 := make([]*basicBloomFilter, 0)
	start1 = time.Now()
	for i := 0; i < bfNum; i++ {
		bf2 := newBasicBloomFilter(uint(capacity), fpr)
		for j := 0; j < capacity; j++ {
			key := fmt.Sprintf("key%d", time.Now().UnixNano()+int64(i))
			bf2.Add([]byte(key))
		}
		bfs2 = append(bfs2, bf2)
	}

	log.Info("Basic BF construct cost", zap.Duration("time", time.Since(start1)))

	start3 = time.Now()
	for _, key := range testKeys {
		locations := Locations(key, bfs2[0].K(), BasicBF)
		for i := 0; i < bfNum; i++ {
			bfs2[i].TestLocations(locations)
		}
	}
	log.Info("Basic BF TestLocation cost", zap.Duration("time", time.Since(start3)))

	start3 = time.Now()
	for i := 0; i < testKeySize; i += batchSize {
		endIdx := i + batchSize
		if endIdx > testKeySize {
			endIdx = testKeySize
		}
		locations := lo.Map(testKeys[i:endIdx], func(key []byte, _ int) []uint64 {
			return Locations(key, bfs2[0].K(), BasicBF)
		})
		hits := make([]bool, batchSize)
		for j := 0; j < bfNum; j++ {
			bfs2[j].BatchTestLocations(locations, hits)
		}
	}
	log.Info("Block BF BatchTestLocation cost", zap.Duration("time", time.Since(start3)))
}

func TestPerformance_Capacity(t *testing.T) {
	fpr := 0.001

	for _, capacity := range []int64{100, 1000, 10000, 100000, 1000000} {
		keys := make([][]byte, 0)
		for i := 0; i < int(capacity); i++ {
			keys = append(keys, []byte(fmt.Sprintf("key%d", time.Now().UnixNano()+int64(i))))
		}

		start1 := time.Now()
		bf1 := newBlockedBloomFilter(uint(capacity), fpr)
		for _, key := range keys {
			bf1.Add(key)
		}

		log.Info("Block BF construct cost", zap.Duration("time", time.Since(start1)))

		testKeys := make([][]byte, 0)
		for i := 0; i < 10000; i++ {
			testKeys = append(testKeys, []byte(fmt.Sprintf("key%d", time.Now().UnixNano()+int64(i))))
		}

		start3 := time.Now()
		for _, key := range testKeys {
			locations := Locations(key, bf1.K(), bf1.Type())
			bf1.TestLocations(locations)
		}
		_, k := bloom.EstimateParameters(uint(capacity), fpr)
		log.Info("Block BF TestLocation cost", zap.Duration("time", time.Since(start3)), zap.Int("k", int(k)), zap.Int64("capacity", capacity))
	}
}

func TestMarshal(t *testing.T) {
	capacity := 200000
	fpr := 0.001

	keys := make([][]byte, 0)
	for i := 0; i < capacity; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%d", i)))
	}

	// test basic bf
	basicBF := newBasicBloomFilter(uint(capacity), fpr)
	for _, key := range keys {
		basicBF.Add(key)
	}
	data, err := basicBF.MarshalJSON()
	assert.NoError(t, err)
	basicBF2, err := UnmarshalJSON(data, BasicBF)
	assert.NoError(t, err)
	assert.Equal(t, basicBF.Type(), basicBF2.Type())

	for _, key := range keys {
		assert.True(t, basicBF2.Test(key))
	}

	// test block bf
	blockBF := newBlockedBloomFilter(uint(capacity), fpr)
	for _, key := range keys {
		blockBF.Add(key)
	}
	data, err = blockBF.MarshalJSON()
	assert.NoError(t, err)
	blockBF2, err := UnmarshalJSON(data, BlockedBF)
	assert.NoError(t, err)
	assert.Equal(t, blockBF.Type(), blockBF.Type())
	for _, key := range keys {
		assert.True(t, blockBF2.Test(key))
	}

	// test compatible with bits-and-blooms/bloom
	bf := bloom.NewWithEstimates(uint(capacity), fpr)
	for _, key := range keys {
		bf.Add(key)
	}
	data, err = bf.MarshalJSON()
	assert.NoError(t, err)
	bf2, err := UnmarshalJSON(data, BasicBF)
	assert.NoError(t, err)
	for _, key := range keys {
		assert.True(t, bf2.Test(key))
	}

	// test empty bloom filter
	emptyBF := AlwaysTrueBloomFilter
	for _, key := range keys {
		bf.Add(key)
	}
	data, err = emptyBF.MarshalJSON()
	assert.NoError(t, err)
	emptyBF2, err := UnmarshalJSON(data, AlwaysTrueBF)
	assert.NoError(t, err)
	for _, key := range keys {
		assert.True(t, emptyBF2.Test(key))
	}
}
