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
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Auto-id primary keys and TSO timestamps are both large enough that their
// varints occupy the full 8-9 bytes. Using small values instead would make the
// Ids column compress far below the FieldsData columns and overstate the effect
// these tests are about.
const (
	firstAutoID    = 458000000000000000
	firstTimestamp = 460000000000000000
)

// deleteStyleResult mirrors what querynode actually streams for
// delete-by-expression: the ID list plus a FieldsData carrying the PK column and
// common.TimeStampField, because the delete plan asks for both
// (proxy/task_delete.go) and the stream path never enables ignoreNonPk.
//
// merge keeps only the Ids, so a fixture without FieldsData cannot observe how
// much of an incoming result is charged but discarded.
func deleteStyleResult(start, n int64) *internalpb.RetrieveResults {
	pks := make([]int64, n)
	tss := make([]int64, n)
	for i := range pks {
		pks[i] = firstAutoID + start + int64(i)
		tss[i] = firstTimestamp + start + int64(i)
	}
	longCol := func(name string, fieldID int64, data []int64) *schemapb.FieldData {
		return &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: name,
			FieldId:   fieldID,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
			}},
		}
	}
	return &internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
		},
		FieldsData:         []*schemapb.FieldData{longCol("pk", 100, pks), longCol("Timestamp", 1, tss)},
		AllRetrieveCount:   n,
		ScannedTotalBytes:  n * 8,
		ScannedRemoteBytes: n * 4,
		CostAggregation:    &internalpb.CostAggregation{ResponseTime: 1, ServiceTime: 1, TotalNQ: 1},
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
		cache.Put(deleteStyleResult(int64(i*perResult), perResult))
		assert.GreaterOrEqual(t, cache.size, proto.Size(cache.result),
			"reported size must not be below the real encoded size after %d merges", i+1)
	}

	// Content is still correct.
	got := cache.result.GetIds().GetIntId().GetData()
	assert.Len(t, got, results*perResult)
	for i := range got {
		assert.Equal(t, int64(firstAutoID+i), got[i])
	}
	assert.Equal(t, int64(results*perResult), cache.result.GetAllRetrieveCount())
	assert.Equal(t, int64(results*perResult*8), cache.result.GetScannedTotalBytes())
	assert.Equal(t, int64(results*perResult*4), cache.result.GetScannedRemoteBytes())

	// Charging the whole incoming envelope rather than the part merge retains
	// costs a steady ~3x here, which would flush at a third of cap. The slack
	// must stay a per-merge constant (the Ids tag and length prefix) plus the
	// one-off headroom, not a fraction of the payload.
	real := proto.Size(cache.result)
	assert.Less(t, cache.size-real, real/100,
		"over-estimate should stay under 1%% of the real size, got %d of %d",
		cache.size-real, real)
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

type recordingStreamServer struct {
	ctx  context.Context
	sent []*internalpb.RetrieveResults
}

func (s *recordingStreamServer) Send(result *internalpb.RetrieveResults) error {
	s.sent = append(s.sent, result)
	return nil
}

func (s *recordingStreamServer) Context() context.Context { return s.ctx }

// The size estimate decides when a batch is full, so an estimate that runs high
// does not just "flush slightly early" — it shrinks every message the stream
// emits. Proxy turns each received message into one deleteTask
// (proxy/task_delete.go), so the batch size here is the delete write batch size.
func TestResultCacheServerFillsBatchesToCapacity(t *testing.T) {
	const (
		batchCap   = 4 << 20 // queryNode.queryStreamBatchSize default
		maxMsgSize = 128 << 20
		perResult  = 1000
		results    = 600
	)

	srv := &recordingStreamServer{ctx: context.Background()}
	cacheSrv := NewResultCacheServer(srv, batchCap, maxMsgSize)
	for i := 0; i < results; i++ {
		assert.NoError(t, cacheSrv.Send(deleteStyleResult(int64(i*perResult), perResult)))
	}
	assert.NoError(t, cacheSrv.Flush())
	assert.NotEmpty(t, srv.sent)

	// Safety first: this is what the estimate exists to guarantee.
	for i, msg := range srv.sent {
		assert.LessOrEqual(t, proto.Size(msg), maxMsgSize, "message %d exceeds maxMsgSize", i)
	}

	// Utilization: every message except the trailing flush should be close to
	// cap. Charging the full envelope instead of the retained IDs puts these at
	// roughly cap/3.
	total := 0
	for _, msg := range srv.sent[:len(srv.sent)-1] {
		size := proto.Size(msg)
		total += size
		assert.Greater(t, size, batchCap*7/10,
			"batches should fill cap, got %d of %d", size, batchCap)
	}
	assert.Positive(t, total)
}

// The old implementation was quadratic in the number of merged results.
func BenchmarkRetrieveResultCacheMerge(b *testing.B) {
	for _, n := range []int{50, 100, 200} {
		b.Run(strconv.Itoa(n)+"results", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cache := &RetrieveResultCache{cap: math.MaxInt}
				for j := 0; j < n; j++ {
					cache.Put(deleteStyleResult(int64(j*5000), 5000))
				}
			}
		})
	}
}
