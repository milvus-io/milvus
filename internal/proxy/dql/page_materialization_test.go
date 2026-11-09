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

package dql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Keep the original plain Proxy operator graph as the integration oracle. It
// includes schema complementation, internal-timestamp removal and final output
// projection, so testing the new operator alone cannot satisfy this comparison.
func legacyPlainPagePipeline(schema *schemapb.CollectionSchema, mode reduce.IReduceType, limit, offset int64) *QueryPipeline {
	b := queryutil.NewPipelineBuilder("legacy-proxy-page-oracle")
	b.Add(queryutil.OpReduceByPK, in(), ch(chanReduced), queryutil.NewSortAndCheckPKOperator(mode, schema))
	b.Add(queryutil.OpSlice, ch(chanReduced), ch(chanSliced), queryutil.NewSliceOperator(limit, offset))
	b.Add("complement_fields", ch(chanSliced), out(), newComplementFieldOperator(schema))
	return &QueryPipeline{pipeline: b.Build(), schema: schema, outputFieldIDs: []int64{100, 101}}
}

func proxyPageInputs(single, elements bool) []*internalpb.RetrieveResults {
	keys := [][]int64{{1, 3, 5}, {2, 4, 6}}
	if single {
		keys = keys[:1]
	}
	inputs := make([]*internalpb.RetrieveResults, len(keys))
	for i, pks := range keys {
		values := make([]int64, len(pks))
		timestamps := make([]int64, len(pks))
		for j, pk := range pks {
			values[j], timestamps[j] = pk*10, 100+pk
		}
		inputs[i] = &internalpb.RetrieveResults{
			Ids: makeTestIntIDs(pks), HasMoreResult: i == 0,
			AllRetrieveCount: 901, ScannedTotalBytes: 902, MvccTimestamp: 903,
			FieldsData: []*schemapb.FieldData{
				makeTestInt64Field(100, "source_pk", pks),
				makeTestInt64Field(101, "source_val", values),
				makeTestInt64Field(common.TimeStampField, "timestamp", timestamps),
			},
		}
		if elements {
			inputs[i].ElementLevel = true
			for j := range pks {
				inputs[i].ElementIndices = append(inputs[i].ElementIndices, &internalpb.ElementIndices{Indices: []int32{0, 2, 5}[:j+1]})
			}
		}
	}
	return inputs
}

func TestProxyPageMaterializationLegacyPipelineParity(t *testing.T) {
	for _, single := range []bool{false, true} {
		for _, elements := range []bool{false, true} {
			for _, mode := range []reduce.IReduceType{reduce.IReduceNoOrder, reduce.IReduceInOrder, reduce.IReduceInOrderForBest} {
				for _, page := range [][2]int64{{2, 0}, {2, 1}, {1, 4}, {3, 99}, {-1, 1}} {
					t.Run(fmt.Sprintf("single=%v/elements=%v/mode=%d/page=%v", single, elements, mode, page), func(t *testing.T) {
						schema := testSchema()
						want, err := legacyPlainPagePipeline(schema, mode, page[0], page[1]).Execute(context.Background(), proxyPageInputs(single, elements))
						require.NoError(t, err)
						candidate, err := NewQueryPipeline(schema, page[0], page[1], mode, nil, nil, nil, nil, []int64{100, 101})
						require.NoError(t, err)
						got, err := candidate.Execute(context.Background(), proxyPageInputs(single, elements))
						require.NoError(t, err)
						require.True(t, proto.Equal(want, got), "legacy=%v\npage=%v", want, got)
						for _, field := range got.GetFieldsData() {
							require.NotEqual(t, int64(common.TimeStampField), field.GetFieldId())
						}
						if !elements && !single && mode == reduce.IReduceNoOrder && page == [2]int64{2, 1} {
							require.Equal(t, []int64{2, 3}, got.FieldsData[0].GetScalars().GetLongData().GetData())
							require.Equal(t, []int64{20, 30}, got.FieldsData[1].GetScalars().GetLongData().GetData())
							require.Equal(t, "pk", got.FieldsData[0].GetFieldName())
							require.Equal(t, "val", got.FieldsData[1].GetFieldName())
						}
					})
				}
			}
		}
	}
}

func TestProxyPageMaterializationDuplicateOutsideOutput(t *testing.T) {
	makeInputs := func() []*internalpb.RetrieveResults {
		inputs := proxyPageInputs(false, false)
		inputs[1].Ids.GetIntId().Data[2] = 5 // duplicate beyond the one-row output page
		return inputs
	}
	schema := testSchema()
	_, oldErr := legacyPlainPagePipeline(schema, reduce.IReduceNoOrder, 1, 0).Execute(context.Background(), makeInputs())
	require.Error(t, oldErr)
	newPipeline, err := NewQueryPipeline(schema, 1, 0, reduce.IReduceNoOrder, nil, nil, nil, nil, []int64{100, 101})
	require.NoError(t, err)
	_, newErr := newPipeline.Execute(context.Background(), makeInputs())
	require.Error(t, newErr)
	require.Equal(t, merr.Code(oldErr), merr.Code(newErr))
}
