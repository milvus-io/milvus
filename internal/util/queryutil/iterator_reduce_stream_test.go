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

package queryutil

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestOrderedReduceStreamElementIteratorBoundary(t *testing.T) {
	for _, mode := range []reduce.IReduceType{reduce.IReduceInOrder, reduce.IReduceInOrderForBest} {
		request := &internalpb.RetrieveRequest{IsIterator: true, ReduceType: int32(mode), Limit: 20}
		first := makeElementLevelResult([]int64{1}, []int64{10}, [][]int32{{0, 2}})
		first.Status, first.HasMoreResult = merr.Success(), true
		last := makeElementLevelResult([]int64{3}, []int64{10}, [][]int32{{1}})
		last.Status = merr.Success()
		sibling := makeElementLevelResult([]int64{2, 100}, []int64{10, 10}, [][]int32{{0}, {0}})
		sibling.Status = merr.Success()
		shard, err := NewShardReduceStream(request, nil, []ReduceStream{
			&queryTestStream{recv: []queryTestRecv{{chunk: first}, {chunk: last}}},
			&queryTestStream{recv: []queryTestRecv{{chunk: sibling}}},
		}, 1)
		require.NoError(t, err)
		other := makeElementLevelResult([]int64{4}, []int64{10}, [][]int32{{0}})
		other.Status = merr.Success()
		stream, err := NewReduceStream(request, nil, []ReduceStream{
			shard, &queryTestStream{recv: []queryTestRecv{{chunk: other}}},
		}, 1)
		require.NoError(t, err)
		var ids []int64
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			require.True(t, chunk.GetElementLevel())
			require.Equal(t, len(ids) == 0, chunk.GetHasMoreResult())
			ids = append(ids, queryTestIDs(chunk)...)
		}
		require.Equal(t, []int64{1, 2, 3}, ids)
		require.NoError(t, stream.Close())
	}
}

func TestOrderedReduceStreamIteratorDrainParity(t *testing.T) {
	for _, mode := range []reduce.IReduceType{reduce.IReduceNoOrder, reduce.IReduceInOrder, reduce.IReduceInOrderForBest} {
		for _, shardLocal := range []bool{false, true} {
			for _, incompleteChild := range []int{-1, 0, 1} {
				for _, chunkBytes := range []int{1, 1 << 20} {
					t.Run(fmt.Sprintf("mode_%d/shard_%t/incomplete_%d/bytes_%d", mode, shardLocal, incompleteChild, chunkBytes), func(t *testing.T) {
						inputs := []*internalpb.RetrieveResults{
							newQueryTestChunk([]int64{1, 3}, []int64{10, 30}),
							newQueryTestChunk([]int64{2, 100}, []int64{20, 1000}),
						}
						if incompleteChild >= 0 {
							inputs[incompleteChild].HasMoreResult = true
						}
						batch, err := NewSortAndCheckPKOperator(mode, nil).Run(context.Background(), nil, inputs)
						require.NoError(t, err)
						children := make([]ReduceStream, len(inputs))
						for i, input := range inputs {
							chunks, err := SplitRetrieveResult(input, 1)
							require.NoError(t, err)
							require.Len(t, chunks, 2)
							child := &queryTestStream{}
							for j, chunk := range chunks {
								require.Equal(t, j == 0 && input.GetHasMoreResult(), chunk.GetHasMoreResult())
								child.recv = append(child.recv, queryTestRecv{chunk: chunk})
							}
							children[i] = child
						}
						request := &internalpb.RetrieveRequest{IsIterator: mode != reduce.IReduceNoOrder, ReduceType: int32(mode), Limit: 10}
						var stream ReduceStream
						if shardLocal {
							stream, err = NewShardReduceStream(request, nil, children, chunkBytes)
						} else {
							stream, err = NewReduceStream(request, nil, children, chunkBytes)
						}
						require.NoError(t, err)
						defer stream.Close()
						var ids []int64
						first := true
						for {
							chunk, err := stream.Recv()
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							require.Equal(t, first && incompleteChild >= 0, chunk.GetHasMoreResult())
							first = false
							ids = append(ids, queryTestIDs(chunk)...)
						}
						require.Equal(t, queryTestIDs(batch[0].(*internalpb.RetrieveResults)), ids)
						for _, child := range children {
							require.Equal(t, 1, child.(*queryTestStream).closeCount())
						}
					})
				}
			}
		}
	}
}

func TestOrderedReduceStreamPropagatesIteratorBoundaryBeforeEOF(t *testing.T) {
	for _, mode := range []reduce.IReduceType{reduce.IReduceInOrder, reduce.IReduceInOrderForBest} {
		for _, chunkBytes := range []int{1, queryChunkBytes(t, newQueryTestChunk([]int64{1, 2}, []int64{10, 20}), 2)} {
			t.Run(fmt.Sprintf("mode_%d/bytes_%d", mode, chunkBytes), func(t *testing.T) {
				first := newQueryTestChunk([]int64{1}, []int64{10})
				first.HasMoreResult = true
				request := &internalpb.RetrieveRequest{IsIterator: true, ReduceType: int32(mode), Limit: 10}
				shard, err := NewShardReduceStream(request, nil, []ReduceStream{
					&queryTestStream{recv: []queryTestRecv{
						{chunk: first},
						{chunk: newQueryTestChunk([]int64{3}, []int64{30})},
					}},
					&queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{2, 100}, []int64{20, 1000})}}},
				}, 1)
				require.NoError(t, err)
				otherShard, err := NewShardReduceStream(request, nil, []ReduceStream{
					&queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{4, 200}, []int64{40, 2000})}}},
				}, 1)
				require.NoError(t, err)
				stream, err := NewReduceStream(request, nil, []ReduceStream{shard, otherShard}, chunkBytes)
				require.NoError(t, err)
				defer stream.Close()
				chunk, err := stream.Recv()
				require.NoError(t, err)
				require.True(t, chunk.GetHasMoreResult())
				require.False(t, shard.(*OrderedReduceStream).childStreams.IsDrained(0))
				ids := append([]int64(nil), queryTestIDs(chunk)...)
				for {
					chunk, err := stream.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					require.False(t, chunk.GetHasMoreResult())
					ids = append(ids, queryTestIDs(chunk)...)
				}
				require.Equal(t, []int64{1, 2, 3}, ids)
			})
		}
	}
}

func TestOrderedReduceStreamStopsOnMetadataOnlyIncompleteChild(t *testing.T) {
	stream, err := NewReduceStream(&internalpb.RetrieveRequest{
		IsIterator: true, ReduceType: int32(reduce.IReduceInOrder), Limit: 10,
	}, nil, []ReduceStream{
		&queryTestStream{recv: []queryTestRecv{{chunk: &internalpb.RetrieveResults{HasMoreResult: true}}}},
		&queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{100}, []int64{1000})}}},
	}, 1)
	require.NoError(t, err)
	defer stream.Close()
	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Empty(t, queryTestIDs(chunk))
	require.True(t, chunk.GetHasMoreResult())
	chunk, err = stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
}
