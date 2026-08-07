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

package proxy

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

const metaCacheBenchmarkSizesEnv = "MILVUS_METACACHE_BENCH_SIZES"

type metaCacheBenchShape struct {
	entries            int
	databases          int
	aliasesPerEntry    int
	withPartitionCache bool
}

func metaCacheBenchmarkSizes(tb testing.TB) []int {
	tb.Helper()
	raw := strings.TrimSpace(os.Getenv(metaCacheBenchmarkSizesEnv))
	if raw == "" {
		return []int{100_000}
	}

	parts := strings.Split(raw, ",")
	sizes := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		require.NoError(tb, err, "parse %s=%q", metaCacheBenchmarkSizesEnv, raw)
		require.Positive(tb, value, "benchmark sizes must be positive")
		sizes = append(sizes, value)
	}
	return sizes
}

func newMetaCacheBench(shape metaCacheBenchShape) *MetaCache {
	if shape.databases <= 0 {
		shape.databases = 1
	}
	cache := &MetaCache{
		collections:    make(map[UniqueID]*collectionInfo, shape.entries),
		nameIdx:        make(map[string]map[string]UniqueID, shape.databases),
		aliasInfo:      make(map[string]map[string]string, shape.databases),
		partitionCache: make(map[string]*partitionInfos),
	}
	for i := 0; i < shape.entries; i++ {
		seedMetaCacheBenchEntry(cache, shape, i)
	}
	return cache
}

func seedMetaCacheBenchEntry(cache *MetaCache, shape metaCacheBenchShape, index int) {
	id := UniqueID(index + 1)
	db := fmt.Sprintf("db_%04d", index%shape.databases)
	name := fmt.Sprintf("collection_%09d", index)
	aliases := make([]string, 0, shape.aliasesPerEntry)

	if _, ok := cache.nameIdx[db]; !ok {
		cache.nameIdx[db] = make(map[string]UniqueID)
	}
	if shape.aliasesPerEntry > 0 {
		if _, ok := cache.aliasInfo[db]; !ok {
			cache.aliasInfo[db] = make(map[string]string)
		}
		for aliasIndex := 0; aliasIndex < shape.aliasesPerEntry; aliasIndex++ {
			alias := fmt.Sprintf("alias_%09d_%02d", index, aliasIndex)
			aliases = append(aliases, alias)
			cache.aliasInfo[db][alias] = name
		}
	}

	cache.collections[id] = &collectionInfo{
		collID:  id,
		dbName:  db,
		schema:  &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Name: name}},
		aliases: aliases,
	}
	cache.nameIdx[db][name] = id

	if shape.withPartitionCache {
		partition := &partitionInfo{name: "_default", partitionID: UniqueID(index + 10_000_000), isDefault: true}
		cache.partitionCache[buildPartitionCacheKey(id)] = &partitionInfos{
			partitionInfos: []*partitionInfo{partition},
			name2Info:      map[string]*partitionInfo{partition.name: partition},
			name2ID:        map[string]int64{partition.name: partition.partitionID},
		}
	}
}

func validateMetaCacheBench(tb testing.TB, cache *MetaCache) {
	tb.Helper()
	cache.mu.RLock()
	violations := metaCacheConsistencyViolations(cache)
	cache.mu.RUnlock()
	require.Empty(tb, violations)
}

func BenchmarkMetaCacheHit(b *testing.B) {
	for _, entries := range metaCacheBenchmarkSizes(b) {
		shape := metaCacheBenchShape{entries: entries, databases: 100, aliasesPerEntry: 1}
		cache := newMetaCacheBench(shape)
		validateMetaCacheBench(b, cache)

		b.Run(fmt.Sprintf("by-id/%d", entries), func(b *testing.B) {
			var cursor atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					index := int(cursor.Add(1)-1) % entries
					entry, ok := cache.getCollection("", "", UniqueID(index+1))
					if !ok || entry.collID != UniqueID(index+1) {
						b.Fatalf("by-id miss for index %d", index)
					}
				}
			})
		})

		b.Run(fmt.Sprintf("by-name/%d", entries), func(b *testing.B) {
			var cursor atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					index := int(cursor.Add(1)-1) % entries
					db := fmt.Sprintf("db_%04d", index%shape.databases)
					name := fmt.Sprintf("collection_%09d", index)
					entry, ok := cache.getCollection(db, name, 0)
					if !ok || entry.collID != UniqueID(index+1) {
						b.Fatalf("by-name miss for index %d", index)
					}
				}
			})
		})

		b.Run(fmt.Sprintf("by-alias/%d", entries), func(b *testing.B) {
			var cursor atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					index := int(cursor.Add(1)-1) % entries
					db := fmt.Sprintf("db_%04d", index%shape.databases)
					alias := fmt.Sprintf("alias_%09d_00", index)
					entry, ok := cache.getCollection(db, alias, 0)
					if !ok || entry.collID != UniqueID(index+1) {
						b.Fatalf("by-alias miss for index %d", index)
					}
				}
			})
		})
	}
}

func BenchmarkMetaCacheInvalidateByID(b *testing.B) {
	ctx := context.Background()
	for _, entries := range metaCacheBenchmarkSizes(b) {
		shape := metaCacheBenchShape{entries: entries, databases: 100, aliasesPerEntry: 1, withPartitionCache: true}
		cache := newMetaCacheBench(shape)
		index := entries / 2
		id := UniqueID(index + 1)

		b.Run(fmt.Sprintf("%d", entries), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				cache.RemoveCollectionsByID(ctx, id)
				b.StopTimer()
				seedMetaCacheBenchEntry(cache, shape, index)
			}
		})
		validateMetaCacheBench(b, cache)
	}
}

func BenchmarkMetaCacheInvalidateKnownAliasTargets(b *testing.B) {
	ctx := context.Background()
	for _, entries := range metaCacheBenchmarkSizes(b) {
		shape := metaCacheBenchShape{entries: entries, databases: 100, aliasesPerEntry: 1, withPartitionCache: true}
		cache := newMetaCacheBench(shape)
		index := entries / 2
		id := UniqueID(index + 1)
		db := fmt.Sprintf("db_%04d", index%shape.databases)
		alias := fmt.Sprintf("alias_%09d_00", index)

		b.Run(fmt.Sprintf("%d", entries), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				cache.InvalidateCollectionMeta(ctx, db, alias, id, true)
				b.StopTimer()
				seedMetaCacheBenchEntry(cache, shape, index)
			}
		})
		validateMetaCacheBench(b, cache)
	}
}

func BenchmarkMetaCachePartitionInvalidation(b *testing.B) {
	ctx := context.Background()
	for _, entries := range metaCacheBenchmarkSizes(b) {
		shape := metaCacheBenchShape{entries: entries, databases: 100, aliasesPerEntry: 1, withPartitionCache: true}
		cache := newMetaCacheBench(shape)
		index := entries / 2
		id := UniqueID(index + 1)
		db := fmt.Sprintf("db_%04d", index%shape.databases)
		name := fmt.Sprintf("collection_%09d", index)

		b.Run(fmt.Sprintf("%d", entries), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				cache.RemovePartition(ctx, db, id, name, "_default")
				b.StopTimer()
				seedMetaCacheBenchEntry(cache, shape, index)
			}
		})
		validateMetaCacheBench(b, cache)
	}
}

func BenchmarkMetaCacheDropDatabase(b *testing.B) {
	ctx := context.Background()
	for _, entries := range metaCacheBenchmarkSizes(b) {
		shape := metaCacheBenchShape{entries: entries, databases: 10, aliasesPerEntry: 1, withPartitionCache: true}
		entriesPerDatabase := entries / shape.databases
		b.Run(fmt.Sprintf("total_%d/db_entries_%d", entries, entriesPerDatabase), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				cache := newMetaCacheBench(shape)
				b.StartTimer()
				cache.RemoveDatabase(ctx, "db_0000")
			}
		})
	}
}

func BenchmarkMetaCacheOldRootcoordAliasHolderFallback(b *testing.B) {
	ctx := context.Background()
	for _, entries := range metaCacheBenchmarkSizes(b) {
		shape := metaCacheBenchShape{entries: entries, databases: 10, aliasesPerEntry: 1}
		b.Run(fmt.Sprintf("%d", entries), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				cache := newMetaCacheBench(shape)
				alias := "alias_000000000_00"
				b.StartTimer()
				cache.RemoveAliasHolders(ctx, "db_0000", alias)
			}
		})
	}
}

func BenchmarkMetaCacheGlobalFillGate(b *testing.B) {
	operations := []struct {
		name      string
		operation string
	}{
		{name: "collection_invalidate", operation: metaCacheOpCollectionInvalidate},
		{name: "database_drop", operation: metaCacheOpDatabaseDrop},
	}
	for _, delay := range []time.Duration{10 * time.Millisecond, 100 * time.Millisecond, time.Second, 5 * time.Second} {
		for _, inflight := range []int{1, 10, 100} {
			for _, operation := range operations {
				name := fmt.Sprintf("delay_%s/inflight_%d/%s", delay, inflight, operation.name)
				b.Run(name, func(b *testing.B) {
					cache := &MetaCache{}
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						readerUnlocks := make([]func(), 0, inflight)
						for j := 0; j < inflight; j++ {
							readerUnlocks = append(readerUnlocks, cache.lockFillRead(metaCacheOpCollectionFill))
						}
						released := make(chan struct{})
						go func() {
							time.Sleep(delay)
							for _, unlock := range readerUnlocks {
								unlock()
							}
							close(released)
						}()

						started := time.Now()
						b.StartTimer()
						writerUnlock := cache.lockFillWrite(operation.operation)
						b.StopTimer()
						wait := time.Since(started)
						writerUnlock()
						<-released
						if wait < delay/2 {
							b.Fatalf("writer wait %s was shorter than expected delay %s", wait, delay)
						}
					}
				})
			}
		}
	}
}

type metaCacheMemorySample struct {
	heapBytes   uint64
	heapObjects uint64
}

func measureMetaCacheMemory(t *testing.T, shape metaCacheBenchShape) metaCacheMemorySample {
	t.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	cache := newMetaCacheBench(shape)
	validateMetaCacheBench(t, cache)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	sample := metaCacheMemorySample{}
	if after.HeapAlloc > before.HeapAlloc {
		sample.heapBytes = after.HeapAlloc - before.HeapAlloc
	}
	if after.HeapObjects > before.HeapObjects {
		sample.heapObjects = after.HeapObjects - before.HeapObjects
	}
	runtime.KeepAlive(cache)
	return sample
}

func positiveMemoryDelta(after, before uint64) uint64 {
	if after <= before {
		return 0
	}
	return after - before
}

func TestMetaCacheMemoryFootprint(t *testing.T) {
	if strings.TrimSpace(os.Getenv(metaCacheBenchmarkSizesEnv)) == "" {
		t.Skipf("set %s=100000,1000000 to run the memory footprint test", metaCacheBenchmarkSizesEnv)
	}

	for _, entries := range metaCacheBenchmarkSizes(t) {
		primary := measureMetaCacheMemory(t, metaCacheBenchShape{entries: entries, databases: 100})
		withAliases := measureMetaCacheMemory(t, metaCacheBenchShape{entries: entries, databases: 100, aliasesPerEntry: 1})
		withPartitions := measureMetaCacheMemory(t, metaCacheBenchShape{
			entries: entries, databases: 100, aliasesPerEntry: 1, withPartitionCache: true,
		})
		aliasBytes := positiveMemoryDelta(withAliases.heapBytes, primary.heapBytes)
		partitionBytes := positiveMemoryDelta(withPartitions.heapBytes, withAliases.heapBytes)
		t.Logf("entries=%d primary_heap_bytes=%d bytes_per_primary_entry=%.2f alias_heap_bytes=%d bytes_per_alias=%.2f partition_heap_bytes=%d bytes_per_partition_list=%.2f combined_heap_objects=%d",
			entries,
			primary.heapBytes,
			float64(primary.heapBytes)/float64(entries),
			aliasBytes,
			float64(aliasBytes)/float64(entries),
			partitionBytes,
			float64(partitionBytes)/float64(entries),
			withPartitions.heapObjects,
		)
	}
}
