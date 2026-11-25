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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryStreamResultPreallocation(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		limit    int64
		capacity int
	}{
		{limit: 16, capacity: 16},
		{limit: 1024, capacity: 1024},
		{limit: 16384, capacity: 1024},
		{limit: typeutil.Unlimited, capacity: 1},
	} {
		t.Run(fmt.Sprintf("limit=%d", tc.limit), func(t *testing.T) {
			const dim = 1536
			chunk := newFinalQueryChunk([]int64{1}, 1)
			chunk.FieldsData = []*schemapb.FieldData{
				testutils.GenerateVectorFieldData(schemapb.DataType_FloatVector, "vector", 1, dim),
			}
			task := &QueryTask{RetrieveRequest: &internalpb.RetrieveRequest{Limit: tc.limit}}
			output, err := task.consumeQueryResultStream(&fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: chunk}}})
			require.NoError(t, err)
			vector := output.GetFieldsData()[0].GetVectors().GetFloatVector().GetData()
			require.Equal(t, chunk.GetFieldsData()[0].GetVectors().GetFloatVector().GetData(), vector)
			require.Equal(t, tc.capacity*dim, cap(vector))
		})
	}
}

func TestSearchStreamResultPreallocation(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		nq, topk int64
		capacity int
	}{
		{nq: 1, topk: 16, capacity: 16},
		{nq: 2, topk: 16, capacity: 32},
		{nq: 1, topk: 16384, capacity: 1024},
		{nq: 8, topk: 512, capacity: 1024},
	} {
		t.Run(fmt.Sprintf("nq=%d/topk=%d", tc.nq, tc.topk), func(t *testing.T) {
			chunk := newFinalSearchChunk([]int64{7}, []float32{1}, "v1")
			chunk.ResultData.NumQueries = tc.nq
			chunk.ResultData.Topks = make([]int64, tc.nq)
			chunk.ResultData.Topks[tc.nq-1] = 1
			chunk.ResultData.FieldsData = newFinalQueryChunk([]int64{7}, 1).FieldsData
			request := &internalpb.SearchRequest{Nq: tc.nq, Topk: tc.topk}
			stream := &searchTaskReduceStream{chunks: []*internalpb.SearchResults{chunk}}
			result, _, err := (&SearchTask{}).consumeSearchResultStream(stream, request,
				newSearchScoreInfo(metric.IP, false), func(*internalpb.SearchResults) {})
			require.NoError(t, err)
			require.Equal(t, []float32{1}, result.Results.GetScores())
			require.Equal(t, tc.capacity, cap(result.Results.GetScores()))
			require.Equal(t, tc.capacity, cap(result.Results.GetFieldsData()[0].GetScalars().GetLongData().GetData()))

			// Group keys and element indices use the same bounded allocation hint.
			chunk.ResultData.GroupByFieldValues = newFinalQueryChunk([]int64{3}, 1).FieldsData
			chunk.ResultData.ElementIndices = &schemapb.LongArray{Data: []int64{0}}
			output := &schemapb.SearchResultData{NumQueries: tc.nq, Topks: make([]int64, tc.nq), Ids: &schemapb.IDs{}}
			_, err = appendFinalSearchChunk(output, chunk.ResultData, make([]int64, tc.nq), 0, tc.topk,
				newSearchScoreInfo(metric.IP, false), false)
			require.NoError(t, err)
			require.Equal(t, []int64{3}, output.GetGroupByFieldValues()[0].GetScalars().GetLongData().GetData())
			require.Equal(t, tc.capacity, cap(output.GetGroupByFieldValues()[0].GetScalars().GetLongData().GetData()))
			require.Equal(t, []int64{0}, output.GetElementIndices().GetData())
			require.Equal(t, tc.capacity, cap(output.GetElementIndices().GetData()))
		})
	}
}

func TestStreamResultsGrowBeyondPreallocation(t *testing.T) {
	paramtable.Init()
	ids := make([]int64, 1200)
	scores := make([]float32, len(ids))
	for i := range ids {
		ids[i] = int64(i)
		scores[i] = float32(len(ids) - i)
	}
	for _, limit := range []int64{16384, typeutil.Unlimited} {
		t.Run(fmt.Sprintf("query/limit=%d", limit), func(t *testing.T) {
			task := &QueryTask{RetrieveRequest: &internalpb.RetrieveRequest{Limit: limit}}
			output, err := task.consumeQueryResultStream(&fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{
				{chunk: newFinalQueryChunk(ids[:600], 1)},
				{chunk: newFinalQueryChunk(ids[600:], 1)},
			}})
			require.NoError(t, err)
			require.Equal(t, ids, output.GetIds().GetIntId().GetData())
			require.Equal(t, ids, output.GetFieldsData()[0].GetScalars().GetLongData().GetData())
		})
	}
	t.Run("search", func(t *testing.T) {
		var chunks []*internalpb.SearchResults
		for _, part := range [][2]int{{0, 600}, {600, len(ids)}} {
			chunk := newFinalSearchChunk(ids[part[0]:part[1]], scores[part[0]:part[1]], "v1")
			chunk.ResultData.FieldsData = newFinalQueryChunk(ids[part[0]:part[1]], 1).FieldsData
			chunks = append(chunks, chunk)
		}
		output, _, err := (&SearchTask{}).consumeSearchResultStream(&searchTaskReduceStream{chunks: chunks},
			&internalpb.SearchRequest{Nq: 1, Topk: 16384}, newSearchScoreInfo(metric.IP, false), func(*internalpb.SearchResults) {})
		require.NoError(t, err)
		require.Equal(t, ids, output.Results.GetIds().GetIntId().GetData())
		require.Equal(t, scores, output.Results.GetScores())
		require.Equal(t, []int64{1200}, output.Results.GetTopks())
		require.Equal(t, ids, output.Results.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	})
}
