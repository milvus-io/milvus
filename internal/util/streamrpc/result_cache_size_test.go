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

package streamrpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func intResult(start, n int64) *internalpb.RetrieveResults {
	data := make([]int64, n)
	for i := range data {
		data[i] = start + int64(i)
	}
	return &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data}},
		},
		AllRetrieveCount:   n,
		ScannedTotalBytes:  n * 8,
		ScannedRemoteBytes: n * 4,
	}
}

// RetrieveResultCache.size drives the flush decisions in ResultCacheServer.Send,
// including the maxMsgSize guard, so it must never under-report. merge used to
// recompute proto.Size over the whole accumulated message, which is O(N^2) over
// a stream; it now accumulates instead. This pins the safety property.
func TestRetrieveResultCacheSizeNeverUnderestimates(t *testing.T) {
	const results, perResult = 200, 500

	cache := &RetrieveResultCache{cap: 1 << 30} // never flushes
	for i := 0; i < results; i++ {
		cache.Put(intResult(int64(i*perResult), perResult))
		assert.GreaterOrEqual(t, cache.size, proto.Size(cache.result),
			"reported size must not be below the real encoded size after %d merges", i+1)
	}

	// Content is still correct.
	got := cache.result.GetIds().GetIntId().GetData()
	assert.Len(t, got, results*perResult)
	for i := range got {
		assert.Equal(t, int64(i), got[i])
	}
	assert.Equal(t, int64(results*perResult), cache.result.GetAllRetrieveCount())
	assert.Equal(t, int64(results*perResult*8), cache.result.GetScannedTotalBytes())
	assert.Equal(t, int64(results*perResult*4), cache.result.GetScannedRemoteBytes())

	// The over-estimate stays proportional to the per-result envelope rather
	// than growing with the payload.
	real := proto.Size(cache.result)
	assert.Less(t, cache.size-real, real/10,
		"over-estimate should stay well under 10%% of the real size")
}

func TestRetrieveResultCacheSizeStringIDs(t *testing.T) {
	cache := &RetrieveResultCache{cap: 1 << 30}
	for i := 0; i < 50; i++ {
		cache.Put(&internalpb.RetrieveResults{
			Ids: &schemapb.IDs{IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{Data: []string{"a", "bb", "ccc"}},
			}},
		})
		assert.GreaterOrEqual(t, cache.size, proto.Size(cache.result))
	}
	assert.Len(t, cache.result.GetIds().GetStrId().GetData(), 150)
}

// The old implementation was quadratic in the number of merged results.
func BenchmarkRetrieveResultCacheMerge(b *testing.B) {
	for _, n := range []int{50, 100, 200} {
		b.Run(strconvItoa(n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cache := &RetrieveResultCache{cap: 1 << 30}
				for j := 0; j < n; j++ {
					cache.Put(intResult(int64(j*5000), 5000))
				}
			}
		})
	}
}

func strconvItoa(n int) string {
	switch n {
	case 50:
		return "50results"
	case 100:
		return "100results"
	default:
		return "200results"
	}
}
