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
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSplitRetrieveResult(t *testing.T) {
	result := newQueryTestChunk([]int64{1, 2, 3}, []int64{10, 20, 30})
	result.ReqID = 100
	result.AllRetrieveCount = 3

	chunks, err := SplitRetrieveResult(result, 2)

	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Equal(t, []int64{1, 2}, chunks[0].GetIds().GetIntId().GetData())
	require.Equal(t, []int64{10, 20}, chunks[0].GetFieldsData()[0].GetScalars().GetLongData().GetData())
	require.Equal(t, int64(100), chunks[0].GetReqID())
	require.Equal(t, int64(3), chunks[0].GetAllRetrieveCount())
	require.Equal(t, []int64{3}, chunks[1].GetIds().GetIntId().GetData())
	require.Equal(t, []int64{30}, chunks[1].GetFieldsData()[0].GetScalars().GetLongData().GetData())
	require.Zero(t, chunks[1].GetReqID())
	require.Zero(t, chunks[1].GetAllRetrieveCount())
}

func TestOrderedReduceStreamMergesChildChunks(t *testing.T) {
	childA := &queryTestStream{recv: []queryTestRecv{
		{chunk: newQueryTestChunk([]int64{1, 3}, []int64{10, 30})},
		{chunk: newQueryTestChunk([]int64{5}, []int64{50})},
	}}
	childB := &queryTestStream{recv: []queryTestRecv{
		{chunk: newQueryTestChunk([]int64{2, 4, 6}, []int64{20, 40, 60})},
	}}
	stream, err := NewReduceStream(
		&internalpb.RetrieveRequest{IsIterator: true, Limit: 5},
		[]ReduceStream{childA, childB},
		2,
	)
	require.NoError(t, err)

	first, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, queryTestIDs(first))

	second, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, queryTestIDs(second))

	third, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{5}, queryTestIDs(third))

	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, childA.closeCount())
	require.Equal(t, 1, childB.closeCount())
}

func TestOrderedReduceStreamAcceptsBoundedOrdinaryQuery(t *testing.T) {
	child := &queryTestStream{recv: []queryTestRecv{
		{chunk: newQueryTestChunk([]int64{1, 2}, []int64{10, 20})},
	}}
	stream, err := NewReduceStream(
		&internalpb.RetrieveRequest{Limit: 2},
		[]ReduceStream{child},
		2,
	)
	require.NoError(t, err)

	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, queryTestIDs(chunk))
}

func TestOrderedReduceStreamRejectsDuplicatePK(t *testing.T) {
	childA := &queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{1}, []int64{10})}}}
	childB := &queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{1}, []int64{20})}}}
	stream, err := NewReduceStream(
		&internalpb.RetrieveRequest{IsIterator: true, Limit: 2},
		[]ReduceStream{childA, childB},
		2,
	)
	require.NoError(t, err)

	chunk, err := stream.Recv()

	require.Nil(t, chunk)
	require.Error(t, err)
	require.Equal(t, 1, childA.closeCount())
	require.Equal(t, 1, childB.closeCount())
}

func newQueryTestChunk(ids, values []int64) *internalpb.RetrieveResults {
	return &internalpb.RetrieveResults{
		Status: merr.Success(),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
					},
				},
			},
		},
	}
}

func queryTestIDs(result *internalpb.RetrieveResults) []int64 {
	return result.GetIds().GetIntId().GetData()
}

type queryTestRecv struct {
	chunk *internalpb.RetrieveResults
	err   error
}

type queryTestStream struct {
	mu         sync.Mutex
	recv       []queryTestRecv
	closeCalls int
}

func (s *queryTestStream) Recv() (*internalpb.RetrieveResults, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.recv) == 0 {
		return nil, io.EOF
	}
	next := s.recv[0]
	s.recv = s.recv[1:]
	return next.chunk, next.err
}

func (s *queryTestStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	return nil
}

func (*queryTestStream) Interrupt() (*internalpb.RetrieveResults, error) {
	return nil, errors.New("not implemented")
}

func (s *queryTestStream) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}
