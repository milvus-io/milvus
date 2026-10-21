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
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestShardReduceStreamElementCountReplacement(t *testing.T) {
	for _, grows := range []bool{false, true} {
		for _, chunkBytes := range []int{1, 1 << 20} {
			t.Run(fmt.Sprintf("grows_%t/bytes_%d", grows, chunkBytes), func(t *testing.T) {
				oldIndices, newIndices := []int32{0, 1, 2}, []int32{4}
				if grows {
					oldIndices, newIndices = newIndices, oldIndices
				}
				child := &queryTestStream{}
				for _, input := range []*internalpb.RetrieveResults{
					makeElementLevelResult([]int64{1}, []int64{10}, [][]int32{oldIndices}),
					makeElementLevelResult([]int64{1}, []int64{20}, [][]int32{newIndices}),
					makeElementLevelResult([]int64{2}, []int64{10}, [][]int32{{0}}),
				} {
					input.Status = merr.Success()
					child.recv = append(child.recv, queryTestRecv{chunk: input})
				}
				stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{Limit: 2}, nil, []ReduceStream{child}, chunkBytes)
				require.NoError(t, err)
				var ids []int64
				var indices []*internalpb.ElementIndices
				for {
					chunk, err := stream.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					require.True(t, chunk.GetElementLevel())
					ids = append(ids, queryTestIDs(chunk)...)
					indices = append(indices, chunk.GetElementIndices()...)
				}
				require.NoError(t, stream.Close())
				require.Equal(t, newIndices, indices[0].GetIndices())
				if grows {
					require.Equal(t, []int64{1}, ids)
				} else {
					require.Equal(t, []int64{1, 2}, ids)
				}
			})
		}
	}
}

func TestShardReduceStreamResolvesTimestampsAcrossChunks(t *testing.T) {
	for _, limit := range []int64{1, 2, 3} {
		for _, chunkBytes := range []int{1, 1 << 20} {
			t.Run(fmt.Sprintf("limit_%d/bytes_%d", limit, chunkBytes), func(t *testing.T) {
				leftChunks := []*internalpb.RetrieveResults{
					newQueryTestChunk([]int64{1}, []int64{11}),
					newQueryTestChunk([]int64{1}, []int64{1 << 56}),
					newQueryTestChunk([]int64{1, 2}, []int64{12, 22}),
					newQueryTestChunk([]int64{2, 3}, []int64{222, 33}),
				}
				for i, timestamps := range [][]int64{{10}, {30}, {5, 10}, {40, 10}} {
					leftChunks[i].FieldsData = append(leftChunks[i].FieldsData, makeTimestampField(timestamps))
				}
				left := &queryTestStream{}
				for _, chunk := range leftChunks {
					left.recv = append(left.recv, queryTestRecv{chunk: chunk})
				}
				rightChunk := newQueryTestChunk([]int64{1, 2, 3}, []int64{99, 2222, 333})
				rightChunk.FieldsData = append(rightChunk.FieldsData, makeTimestampField([]int64{20, 20, 20}))
				right := &queryTestStream{recv: []queryTestRecv{{chunk: rightChunk}}}
				stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{Limit: limit}, nil, []ReduceStream{left, right}, chunkBytes)
				require.NoError(t, err)
				defer stream.Close()
				var ids, values, timestamps []int64
				for {
					chunk, err := stream.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					if chunkBytes == 1 {
						require.Len(t, queryTestIDs(chunk), 1)
					}
					ids = append(ids, queryTestIDs(chunk)...)
					values = append(values, chunk.GetFieldsData()[0].GetScalars().GetLongData().GetData()...)
					timestamps = append(timestamps, extractTimestamps(chunk)...)
				}
				require.Equal(t, []int64{1, 2, 3}[:limit], ids)
				require.Equal(t, []int64{1 << 56, 222, 333}[:limit], values)
				require.Equal(t, []int64{30, 40, 20}[:limit], timestamps)
			})
		}
	}
}

func TestShardReduceStreamRetainsFirstRowForEqualOrMissingTimestamp(t *testing.T) {
	for _, withTimestamp := range []bool{false, true} {
		t.Run(fmt.Sprintf("timestamp_%t", withTimestamp), func(t *testing.T) {
			first := newQueryTestChunk([]int64{1}, []int64{11})
			second := newQueryTestChunk([]int64{1}, []int64{22})
			if withTimestamp {
				first.FieldsData = append(first.FieldsData, makeTimestampField([]int64{10}))
				second.FieldsData = append(second.FieldsData, makeTimestampField([]int64{10}))
			}
			stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{Limit: 1}, nil, []ReduceStream{
				&queryTestStream{recv: []queryTestRecv{{chunk: first}, {chunk: second}}},
			}, 1)
			require.NoError(t, err)
			defer stream.Close()
			chunk, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, []int64{1}, queryTestIDs(chunk))
			require.Equal(t, []int64{11}, chunk.GetFieldsData()[0].GetScalars().GetLongData().GetData())
		})
	}
}

func TestShardReduceStreamAccountsForReplacementBytes(t *testing.T) {
	for _, grows := range []bool{false, true} {
		t.Run(fmt.Sprintf("grows_%t", grows), func(t *testing.T) {
			oldValue, newValue := int64(1<<56), int64(22)
			if grows {
				oldValue, newValue = newValue, oldValue
			}
			first := newQueryTestChunk([]int64{1, 2}, []int64{11, oldValue})
			first.FieldsData = append(first.FieldsData, makeTimestampField([]int64{10, 10}))
			second := newQueryTestChunk([]int64{2}, []int64{newValue})
			second.FieldsData = append(second.FieldsData, makeTimestampField([]int64{20}))
			last := newQueryTestChunk([]int64{3}, []int64{33})
			last.FieldsData = append(last.FieldsData, makeTimestampField([]int64{10}))
			threshold := queryChunkBytes(t, first, 1) + 1
			if !grows {
				threshold = queryChunkBytes(t, first, 1)*2 + 1
			}
			stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{Limit: 3}, nil, []ReduceStream{
				&queryTestStream{recv: []queryTestRecv{{chunk: first}, {chunk: second}, {chunk: last}}},
			}, threshold)
			require.NoError(t, err)
			defer stream.Close()
			chunk, err := stream.Recv()
			require.NoError(t, err)
			if !grows {
				require.Equal(t, []int64{1, 2, 3}, queryTestIDs(chunk))
				require.Equal(t, []int64{11, newValue, 33}, chunk.GetFieldsData()[0].GetScalars().GetLongData().GetData())
				return
			}
			require.Equal(t, []int64{1}, queryTestIDs(chunk))
			chunk, err = stream.Recv()
			require.NoError(t, err)
			require.Equal(t, []int64{2}, queryTestIDs(chunk))
			require.Equal(t, []int64{newValue}, chunk.GetFieldsData()[0].GetScalars().GetLongData().GetData())
			chunk, err = stream.Recv()
			require.NoError(t, err)
			require.Equal(t, []int64{3}, queryTestIDs(chunk))
		})
	}
}

func TestOrderedReduceStreamRejectsCrossVChannelDuplicatesAfterShardReduction(t *testing.T) {
	request := &internalpb.RetrieveRequest{Limit: 2}
	children := make([]ReduceStream, 0, 2)
	for _, value := range []int64{11, 22} {
		child, err := NewShardReduceStream(request, nil, []ReduceStream{
			&queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{1}, []int64{value})}}},
		}, 1)
		require.NoError(t, err)
		children = append(children, child)
	}
	stream, err := NewReduceStream(request, nil, children, 1<<20)
	require.NoError(t, err)
	defer stream.Close()
	_, err = stream.Recv()
	require.ErrorContains(t, err, "duplicate PK 1")
}

func TestShardReduceStreamPreservesOrderByDuplicateCheck(t *testing.T) {
	stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{
		Limit:         2,
		OrderByFields: []*planpb.OrderByField{{FieldId: 101, Ascending: true}},
	}, nil, []ReduceStream{
		&queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{1}, []int64{11})}}},
		&queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{1}, []int64{22})}}},
	}, 1<<20)
	require.NoError(t, err)
	defer stream.Close()
	_, err = stream.Recv()
	require.ErrorContains(t, err, "duplicate PK 1")
}
