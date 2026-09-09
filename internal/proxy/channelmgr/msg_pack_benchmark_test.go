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

package channelmgr

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func BenchmarkGenInsertMsgsByPartition(b *testing.B) {
	const (
		rows         = 8192
		shards       = 8
		scalarFields = 8
	)
	ctx := context.Background()
	for _, dim := range []int{8, 768} {
		src := newNullableVectorInsertMsgForPackTest(rows, dim, scalarFields)
		for _, sparse := range []bool{false, true} {
			layout := "contiguous"
			if sparse {
				layout = "noncontiguous"
			}
			groups := make([][]int, shards)
			for row := 0; row < rows; row++ {
				shard := row / (rows / shards)
				if sparse {
					shard = row % shards
				}
				groups[shard] = append(groups[shard], row)
			}
			for _, batching := range []struct {
				name      string
				threshold int
			}{
				{"one_batch", 64 << 20},
				{"split_batches", 64 * (scalarFields*8 + dim*4)},
				{"single_row_batches", 1},
			} {
				b.Run(fmt.Sprintf("dim%d/%s/%s", dim, layout, batching.name), func(b *testing.B) {
					key := paramtable.Get().PulsarCfg.MaxMessageSize.Key
					require.NoError(b, paramtable.Get().Save(key, strconv.Itoa(batching.threshold)))
					b.Cleanup(func() { paramtable.Get().Reset(key) })
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for _, offsets := range groups {
							// Woodpecker has no per-row limit, allowing the smallest
							// threshold to exercise single-row batches at both dims.
							msgs, err := GenInsertMsgsByPartition(ctx, 0, 1, "test_partition",
								offsets, "test_channel", src, message.WALNameWoodpecker)
							if err != nil {
								b.Fatal(err)
							}
							if len(msgs) == 0 {
								b.Fatal("no insert messages generated")
							}
						}
					}
				})
			}
		}
	}
}
