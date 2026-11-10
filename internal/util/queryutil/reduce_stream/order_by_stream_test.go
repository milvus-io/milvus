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
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestShardOrderByStreamMatchesBatch(t *testing.T) {
	for _, chunkBytes := range []int{1, 100, 1 << 20} {
		for _, ascending := range []bool{false, true} {
			for _, nullsFirst := range []bool{false, true} {
				for _, stringPK := range []bool{false, true} {
					t.Run(fmt.Sprintf("bytes_%d/asc_%t/nulls_%t/stringPK_%t", chunkBytes, ascending, nullsFirst, stringPK), func(t *testing.T) {
						inputs := []*internalpb.RetrieveResults{
							newQueryTestChunk([]int64{1, 2, 4}, []int64{10, 20, 30}),
							newQueryTestChunk([]int64{3, 1, 5}, []int64{25, 30, 30}),
						}
						inputs[0].FieldsData = append(inputs[0].FieldsData, makeTimestampField([]int64{100, 100, 100}))
						inputs[1].FieldsData = append(inputs[1].FieldsData, makeTimestampField([]int64{100, 200, 100}))
						inputs[0].FieldsData[0].ValidData = []bool{true, true, false}
						inputs[0].FieldsData[0].GetScalars().GetLongData().Data = []int64{10, 20}
						inputs[1].FieldsData[0].ValidData = []bool{true, true, true}
						for i, input := range inputs {
							input.FieldsData = append(input.FieldsData, &schemapb.FieldData{
								Type: schemapb.DataType_FloatVector, FieldId: 102,
								Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
									Dim: 2, ValidData: []bool{true, false, true},
									Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
										Data: []float32{float32(i + 1), 1, float32(i + 2), 2},
									}},
								}},
							})
						}
						if stringPK {
							for _, input := range inputs {
								var ids []string
								for _, id := range queryTestIDs(input) {
									ids = append(ids, fmt.Sprint(id))
								}
								input.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids}}}
							}
						}
						fields := []*planpb.OrderByField{
							{FieldId: 101, Ascending: ascending, NullsFirst: nullsFirst},
							{FieldId: 1, Ascending: false},
						}
						batchFields := []*orderby.OrderByField{
							{FieldID: 101, DataType: schemapb.DataType_Int64, Ascending: ascending, NullsFirst: nullsFirst},
							{FieldID: 1, DataType: schemapb.DataType_Int64, Ascending: false},
						}
						batch, err := queryutil.NewDeduplicatePKOperator(0, nil).Run(t.Context(), nil, inputs)
						require.NoError(t, err)
						batch, err = queryutil.NewOrderByLimitOperator(batchFields, 3).Run(t.Context(), nil, batch[0])
						require.NoError(t, err)
						var children []ReduceStream
						for _, input := range inputs {
							chunks, err := SplitRetrieveResult(input, chunkBytes)
							require.NoError(t, err)
							child := &queryTestStream{}
							for _, chunk := range chunks {
								child.recv = append(child.recv, queryTestRecv{chunk: chunk})
							}
							children = append(children, child)
						}
						stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{Limit: 3, OrderByFields: fields}, nil, children, chunkBytes)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, stream.Close()) })
						var got []queryReduceUnit
						for {
							chunk, err := stream.Recv()
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							for i := range children {
								require.True(t, stream.(*OrderedReduceStream).childStreams.IsDrained(i))
							}
							for row := 0; row < queryutil.GetRowCount(chunk); row++ {
								got = append(got, queryReduceUnit{result: chunk, rowIndex: int64(row)})
							}
						}
						merged, err := buildRetrieveChunk(got)
						require.NoError(t, err)
						want := batch[0].(*internalpb.RetrieveResults)
						require.True(t, proto.Equal(want.GetIds(), merged.GetIds()), "want %v, got %v", want, merged)
						for i, field := range want.GetFieldsData() {
							require.True(t, proto.Equal(field, merged.GetFieldsData()[i]), "field %d: want %v, got %v", i, field, merged.GetFieldsData()[i])
						}
					})
				}
			}
		}
	}
}

func TestShardOrderByStreamLateVersionChangesTopK(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		first := newQueryTestChunk([]int64{1, 2}, []int64{10, 20})
		first.FieldsData = append(first.FieldsData, makeTimestampField([]int64{100, 100}))
		last := newQueryTestChunk([]int64{3, 1}, []int64{25, 30})
		last.FieldsData = append(last.FieldsData, makeTimestampField([]int64{100, 200}))
		if reverse {
			first, last = last, first
		}
		first.AllRetrieveCount, last.AllRetrieveCount = 2, 3
		last.MvccTimestamp = 200
		last.HasMoreResult = true
		child := &queryTestStream{recv: []queryTestRecv{{chunk: first}, {chunk: last}}}
		stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{
			Limit: 2, OrderByFields: []*planpb.OrderByField{{FieldId: 101, Ascending: true}},
		}, nil, []ReduceStream{child}, 1)
		require.NoError(t, err)
		chunk, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, []int64{2}, queryTestIDs(chunk))
		require.EqualValues(t, 5, chunk.GetAllRetrieveCount())
		require.EqualValues(t, 200, chunk.GetMvccTimestamp())
		require.True(t, chunk.GetHasMoreResult())
		chunk, err = stream.Recv()
		require.NoError(t, err)
		require.Equal(t, []int64{3}, queryTestIDs(chunk))
		require.Zero(t, chunk.GetAllRetrieveCount())
		_, err = stream.Recv()
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, 1, child.closeCount())
	}
}

func TestOrderByStreamRejectsNonAdjacentCrossVChannelDuplicates(t *testing.T) {
	req := &internalpb.RetrieveRequest{Limit: 3, OrderByFields: []*planpb.OrderByField{{FieldId: 101, Ascending: true}}}
	var shards []ReduceStream
	for _, chunk := range []*internalpb.RetrieveResults{
		newQueryTestChunk([]int64{1, 2}, []int64{10, 20}),
		newQueryTestChunk([]int64{3, 1}, []int64{25, 30}),
	} {
		shard, err := NewShardReduceStream(req, nil, []ReduceStream{&queryTestStream{recv: []queryTestRecv{{chunk: chunk}}}}, 1)
		require.NoError(t, err)
		shards = append(shards, shard)
	}
	stream, err := NewReduceStream(req, nil, shards, 1)
	require.NoError(t, err)
	for _, id := range []int64{1, 2, 3} {
		chunk, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, []int64{id}, queryTestIDs(chunk))
	}
	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, stream.Close())
}

func TestShardOrderByStreamFailureBeforeOutput(t *testing.T) {
	child := &queryTestStream{recv: []queryTestRecv{
		{chunk: newQueryTestChunk([]int64{1}, []int64{10})},
		{err: io.ErrUnexpectedEOF},
	}}
	stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{
		Limit: 1, OrderByFields: []*planpb.OrderByField{{FieldId: 101, Ascending: true}},
	}, nil, []ReduceStream{child}, 1)
	require.NoError(t, err)
	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, 1, child.closeCount())
	require.NoError(t, stream.Close())
}

func TestShardOrderByStreamEmptyMetadata(t *testing.T) {
	stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{
		Limit: 1, OrderByFields: []*planpb.OrderByField{{FieldId: 101, Ascending: true}},
	}, nil, []ReduceStream{&queryTestStream{recv: []queryTestRecv{{chunk: &internalpb.RetrieveResults{
		Status: merr.Success(), AllRetrieveCount: 7, MvccTimestamp: 10,
	}}}}}, 1)
	require.NoError(t, err)
	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Zero(t, queryutil.GetRowCount(chunk))
	require.EqualValues(t, 7, chunk.GetAllRetrieveCount())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestShardOrderByStreamEnforcesOutputQuotaBeforeTopK(t *testing.T) {
	params := paramtable.Get()
	key, old := params.QuotaConfig.MaxOutputSize.Key, params.QuotaConfig.MaxOutputSize.GetValue()
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { require.NoError(t, params.Save(key, old)) })
	stream, err := NewShardReduceStream(&internalpb.RetrieveRequest{
		Limit: 1, OrderByFields: []*planpb.OrderByField{{FieldId: 101, Ascending: true}},
	}, nil, []ReduceStream{&queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{1, 2}, []int64{10, 20})}}}}, 1)
	require.NoError(t, err)
	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorContains(t, err, "maxOutputSize")
	require.NoError(t, stream.Close())
}
