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

package searchutil

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestShardReduceStreamDeduplicatesAcrossChunks(t *testing.T) {
	for _, metric := range []string{"IP", "L2"} {
		for _, chunkBytes := range []int{1, 1 << 20} {
			t.Run(fmt.Sprintf("%s/bytes_%d", metric, chunkBytes), func(t *testing.T) {
				left := &fakeReduceStream{recv: []fakeStreamRecv{
					{chunk: newSearchChunk(2, 3, []testHit{{id: 1, score: 0.9}})},
					{chunk: newSearchChunk(2, 3, []testHit{{id: 2, score: 0.7}})},
					{chunk: newSearchChunk(2, 3, []testHit{{id: 3, score: 0.5}})},
					{chunk: newSearchChunk(2, 3, nil, []testHit{{id: 1, score: 0.8}, {id: 4, score: 0.6}})},
				}}
				right := &fakeReduceStream{recv: []fakeStreamRecv{
					{chunk: newSearchChunk(2, 3, []testHit{{id: 1, score: 0.9}})},
					{chunk: newSearchChunk(2, 3, []testHit{{id: 2, score: 0.6}})},
					{chunk: newSearchChunk(2, 3, nil, []testHit{{id: 1, score: 0.9}})},
					{chunk: newSearchChunk(2, 3, nil, []testHit{{id: 2, score: 0.7}})},
					{chunk: newSearchChunk(2, 3, nil, []testHit{{id: 4, score: 0.5}})},
				}}
				expectedScores := [][]float32{{0.9, 0.7, 0.5}, {0.9, 0.7, 0.6}}
				for _, child := range []*fakeReduceStream{left, right} {
					for _, received := range child.recv {
						received.chunk.MetricType = metric
						if metric == "L2" {
							for i := range received.chunk.ResultData.Scores {
								received.chunk.ResultData.Scores[i] -= 1
							}
						}
					}
				}
				if metric == "L2" {
					for _, scores := range expectedScores {
						for i := range scores {
							scores[i] -= 1
						}
					}
				}
				stream, err := NewShardReduceStream(&internalpb.SearchRequest{Nq: 2, Topk: 3, MetricType: metric}, []ReduceStream{left, right}, chunkBytes)
				require.NoError(t, err)
				defer stream.Close()
				ids := make([][]int64, 2)
				scores := make([][]float32, 2)
				for {
					chunk, err := stream.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					data := chunk.GetResultData()
					if chunkBytes == 1 {
						require.Len(t, data.GetScores(), 1)
					}
					offset := int64(0)
					for query, count := range data.GetTopks() {
						ids[query] = append(ids[query], data.GetIds().GetIntId().GetData()[offset:offset+count]...)
						scores[query] = append(scores[query], data.GetScores()[offset:offset+count]...)
						offset += count
					}
				}
				require.Equal(t, [][]int64{{1, 2, 3}, {1, 2, 4}}, ids)
				require.Equal(t, expectedScores, scores)
			})
		}
	}
}

func TestShardReduceStreamPreservesElementHitIdentity(t *testing.T) {
	first := newSearchChunk(1, 3, []testHit{{id: 1, score: 0.9}, {id: 1, score: 0.8}})
	first.ResultData.ElementIndices = &schemapb.LongArray{Data: []int64{1, 2}}
	second := newSearchChunk(1, 3, []testHit{{id: 1, score: 0.85}, {id: 2, score: 0.7}})
	second.ResultData.ElementIndices = &schemapb.LongArray{Data: []int64{1, 1}}
	stream, err := NewShardReduceStream(&internalpb.SearchRequest{Nq: 1, Topk: 3, MetricType: "IP"}, []ReduceStream{
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: first}}},
		&fakeReduceStream{recv: []fakeStreamRecv{{chunk: second}}},
	}, 1<<20)
	require.NoError(t, err)
	defer stream.Close()
	chunk := recvChunk(t, stream)
	require.Equal(t, []int64{1, 1, 2}, chunk.GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, []int64{1, 2, 1}, chunk.GetResultData().GetElementIndices().GetData())
	require.Equal(t, []float32{0.9, 0.8, 0.7}, chunk.GetResultData().GetScores())
}

func TestOrderedReduceStreamPreservesCrossVChannelDuplicates(t *testing.T) {
	request := &internalpb.SearchRequest{Nq: 1, Topk: 3, MetricType: "IP"}
	children := make([]ReduceStream, 0, 2)
	for _, hits := range [][]testHit{
		{{id: 1, score: 0.9}, {id: 2, score: 0.7}},
		{{id: 1, score: 0.8}, {id: 3, score: 0.6}},
	} {
		child, err := NewShardReduceStream(request, []ReduceStream{
			&fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 3, hits)}}},
		}, 1)
		require.NoError(t, err)
		children = append(children, child)
	}
	stream, err := NewReduceStream(request, children, 1<<20)
	require.NoError(t, err)
	defer stream.Close()
	assertSearchChunk(t, recvChunk(t, stream), []int64{1, 1, 2}, []float32{0.9, 0.8, 0.7}, []int64{3})
}
