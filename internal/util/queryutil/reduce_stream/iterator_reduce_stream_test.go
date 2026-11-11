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

package reducestream

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	for _, tc := range []struct {
		mode  reduce.IReduceType
		limit int64
	}{
		{reduce.IReduceNoOrder, 10}, {reduce.IReduceInOrder, 10}, {reduce.IReduceInOrderForBest, 10},
		{reduce.IReduceInOrder, typeutil.Unlimited}, {reduce.IReduceInOrderForBest, typeutil.Unlimited},
	} {
		mode := tc.mode
		for _, shardLocal := range []bool{false, true} {
			for _, incompleteChild := range []int{-1, 0, 1} {
				for _, chunkBytes := range []int{1, 1 << 20} {
					t.Run(fmt.Sprintf("mode_%d/limit_%d/shard_%t/incomplete_%d/bytes_%d", mode, tc.limit, shardLocal, incompleteChild, chunkBytes), func(t *testing.T) {
						inputs := []*internalpb.RetrieveResults{
							newQueryTestChunk([]int64{1, 3}, []int64{10, 30}),
							newQueryTestChunk([]int64{2, 100}, []int64{20, 1000}),
						}
						if incompleteChild >= 0 {
							inputs[incompleteChild].HasMoreResult = true
						}
						batch, err := queryutil.NewSortAndCheckPKOperator(mode, nil).Run(context.Background(), nil, inputs)
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
						request := &internalpb.RetrieveRequest{IsIterator: mode != reduce.IReduceNoOrder, ReduceType: int32(mode), Limit: tc.limit}
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

func TestShardReduceStreamTimestampTieAtIteratorBoundary(t *testing.T) {
	for _, mode := range []reduce.IReduceType{reduce.IReduceNoOrder, reduce.IReduceInOrder, reduce.IReduceInOrderForBest} {
		for _, timestamps := range [][2]int64{{100, 200}, {200, 100}, {100, 100}, {0, 200}, {0, 0}} {
			for _, reverse := range []bool{false, true} {
				for _, incomplete := range []int{-1, 0, 1} {
					for _, chunkBytes := range []int{1, 1 << 20} {
						t.Run(fmt.Sprintf("mode_%d/ts_%d_%d/reverse_%t/incomplete_%d/bytes_%d", mode, timestamps[0], timestamps[1], reverse, incomplete, chunkBytes), func(t *testing.T) {
							first := newQueryTestChunk([]int64{1}, []int64{11})
							second := newQueryTestChunk([]int64{1, 2}, []int64{22, 33})
							if timestamps != [2]int64{0, 0} {
								first.FieldsData = append(first.FieldsData, makeTimestampField([]int64{timestamps[0]}))
								second.FieldsData = append(second.FieldsData, makeTimestampField([]int64{timestamps[1], 50}))
							}
							inputs := []*internalpb.RetrieveResults{first, second}
							if reverse {
								inputs[0], inputs[1] = inputs[1], inputs[0]
							}
							if incomplete >= 0 {
								inputs[incomplete].HasMoreResult = true
							}
							batch, err := queryutil.NewReduceByPKWithTimestampOperator(mode, 0, 10, nil).Run(t.Context(), nil, inputs)
							require.NoError(t, err)
							children := make([]ReduceStream, len(inputs))
							for i, input := range inputs {
								chunks, err := SplitRetrieveResult(input, chunkBytes)
								require.NoError(t, err)
								child := &queryTestStream{}
								for _, chunk := range chunks {
									child.recv = append(child.recv, queryTestRecv{chunk: chunk})
								}
								children[i] = child
							}
							stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{IsIterator: mode != reduce.IReduceNoOrder, ReduceType: int32(mode), Limit: 10}, nil, children, chunkBytes)
							require.NoError(t, err)
							defer stream.Close()
							var ids, gotTimestamps, values []int64
							for {
								chunk, err := stream.Recv()
								if err == io.EOF {
									break
								}
								require.NoError(t, err)
								ids = append(ids, queryTestIDs(chunk)...)
								gotTimestamps = append(gotTimestamps, queryutil.ExtractTimestamps(chunk)...)
								if len(chunk.GetFieldsData()) > 0 {
									values = append(values, chunk.GetFieldsData()[0].GetScalars().GetLongData().GetData()...)
								}
							}
							want := batch[0].(*internalpb.RetrieveResults)
							require.Equal(t, queryTestIDs(want), ids)
							require.Equal(t, queryutil.ExtractTimestamps(want), gotTimestamps)
							require.Equal(t, want.GetFieldsData()[0].GetScalars().GetLongData().GetData(), values)
						})
					}
				}
			}
		}
	}
}

func TestShardReduceStreamTimestampTieStringPK(t *testing.T) {
	older := newQueryTestChunk([]int64{1}, []int64{11})
	newer := newQueryTestChunk([]int64{1}, []int64{22})
	for i, chunk := range []*internalpb.RetrieveResults{older, newer} {
		chunk.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"same"}}}}
		chunk.FieldsData = append(chunk.FieldsData, makeTimestampField([]int64{int64((i + 1) * 100)}))
	}
	older.HasMoreResult = true
	inputs := []*internalpb.RetrieveResults{older, newer}
	batch, err := queryutil.NewReduceByPKWithTimestampOperator(reduce.IReduceInOrder, 0, 10, nil).Run(t.Context(), nil, inputs)
	require.NoError(t, err)
	children := []ReduceStream{&queryTestStream{recv: []queryTestRecv{{chunk: older}}}, &queryTestStream{recv: []queryTestRecv{{chunk: newer}}}}
	stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{IsIterator: true, ReduceType: int32(reduce.IReduceInOrder), Limit: 10}, nil, children, 1<<20)
	require.NoError(t, err)
	defer stream.Close()
	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []string{"same"}, chunk.GetIds().GetStrId().GetData())
	require.Equal(t, queryutil.ExtractTimestamps(batch[0].(*internalpb.RetrieveResults)), queryutil.ExtractTimestamps(chunk))
	require.Equal(t, []int64{200}, queryutil.ExtractTimestamps(chunk))
}

func TestShardReduceStreamTimestampTieThreeChildren(t *testing.T) {
	for _, mode := range []reduce.IReduceType{reduce.IReduceNoOrder, reduce.IReduceInOrder, reduce.IReduceInOrderForBest} {
		t.Run(fmt.Sprintf("mode_%d", mode), func(t *testing.T) {
			var inputs []*internalpb.RetrieveResults
			var children []ReduceStream
			for i, ts := range []int64{100, 200, 300} {
				chunk := newQueryTestChunk([]int64{1}, []int64{int64(i)})
				chunk.FieldsData = append(chunk.FieldsData, makeTimestampField([]int64{ts}))
				chunk.HasMoreResult = i == 0
				inputs = append(inputs, chunk)
				children = append(children, &queryTestStream{recv: []queryTestRecv{{chunk: chunk}}})
			}
			batch, err := queryutil.NewReduceByPKWithTimestampOperator(mode, 0, 10, nil).Run(t.Context(), nil, inputs)
			require.NoError(t, err)
			stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{IsIterator: mode != reduce.IReduceNoOrder, ReduceType: int32(mode), Limit: 10}, nil, children, 1<<20)
			require.NoError(t, err)
			defer stream.Close()
			chunk, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, queryutil.ExtractTimestamps(batch[0].(*internalpb.RetrieveResults)), queryutil.ExtractTimestamps(chunk))
			require.Equal(t, []int64{300}, queryutil.ExtractTimestamps(chunk))
		})
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
