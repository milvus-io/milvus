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

package agg

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// Measure complete grouped Merge calls with overlapping or disjoint partials.
// Decode input once, since the changed storage belongs to the merge buffer.
func BenchmarkGroupedMerge(b *testing.B) {
	const sources, groups = 4, 4096
	for _, disjoint := range []bool{false, true} {
		b.Run(fmt.Sprintf("disjoint=%t", disjoint), func(b *testing.B) {
			r := NewGroupAggReducer([]int64{100}, []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}, -1, buildTestSchema())
			partials := make([]*PartialAggregationResult, sources)
			for source := range partials {
				keys, values := make([]int64, groups), make([]int64, groups)
				for i := range keys {
					keys[i], values[i] = int64(i), 1
					if disjoint {
						keys[i] += int64(source * groups)
					}
				}
				var err error
				partials[source], err = r.DecodePartial([]*schemapb.FieldData{
					genEmptyLongFieldData(schemapb.DataType_Int64, keys),
					genEmptyLongFieldData(schemapb.DataType_Int64, values),
				}, groups)
				if err != nil {
					b.Fatal(err)
				}
			}
			wantGroups, wantValue := groups, int64(sources)
			if disjoint {
				wantGroups, wantValue = sources*groups, 1
			}
			check := func(result *PartialAggregationResult, err error) {
				if err != nil {
					b.Fatal(err)
				}
				values := result.GetFieldDatas()[1].GetScalars().GetLongData().GetData()
				if len(values) != wantGroups || result.GetAllRetrieveCount() != sources*groups {
					b.Fatalf("unexpected groups/count: %d/%d", len(values), result.GetAllRetrieveCount())
				}
				for _, value := range values {
					if value != wantValue {
						b.Fatalf("unexpected aggregate: %d", value)
					}
				}
			}
			check(r.Merge(b.Context(), partials))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := r.Merge(b.Context(), partials)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.GetFieldDatas()[1].GetScalars().GetLongData().GetData()) != wantGroups {
					b.Fatal("unexpected group count")
				}
			}
		})
	}
}
