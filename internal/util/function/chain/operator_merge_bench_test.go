/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// MergeOp is the operator every rerank chain starts with, and it was the only
// significant one without a benchmark.
func BenchmarkMergeOp(b *testing.B) {
	benchCases := []struct {
		name   string
		nq     int
		topK   int
		inputs int
	}{
		{"RRF_nq1_topk100_2inputs", 1, 100, 2},
		{"RRF_nq10_topk100_2inputs", 10, 100, 2},
		{"RRF_nq10_topk1000_2inputs", 10, 1000, 2},
		{"RRF_nq10_topk100_4inputs", 10, 100, 4},
	}

	pool := memory.NewGoAllocator()
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			op := NewMergeOp(MergeStrategyRRF, WithRRFK(60))
			ctx := types.NewFuncContext(pool)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dfs := make([]*DataFrame, bc.inputs)
				for j := range dfs {
					resultData := generateSearchResultData(bc.nq, bc.topK, 2)
					df, err := FromSearchResultData(resultData, pool, fieldNamesForNumFields(2))
					if err != nil {
						b.Fatal(err)
					}
					dfs[j] = df
				}
				b.StartTimer()

				out, err := op.ExecuteMulti(ctx, dfs)
				if err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				out.Release()
				for _, df := range dfs {
					df.Release()
				}
				b.StartTimer()
			}
		})
	}
}
