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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSplitSearchResultPreservesGroupColumns(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		result := newSearchChunk(2, 2, []testHit{{1, .9}, {2, .8}}, []testHit{{3, .7}, {4, .6}})
		builder, err := typeutil.NewFieldDataBuilder(schemapb.DataType_Int64, true, 4)
		require.NoError(t, err)
		for _, value := range []any{int64(10), nil, int64(20), int64(10)} {
			builder.Add(value)
		}
		field := builder.Build()
		field.FieldId = 200
		if legacy {
			field.FieldId = 0
			result.ResultData.GroupByFieldValue = field
		} else {
			result.ResultData.GroupByFieldValues = []*schemapb.FieldData{field}
		}
		chunks, err := SplitSearchResult(result, 1)
		require.NoError(t, err)
		require.Len(t, chunks, 4)
		var values []any
		for _, chunk := range chunks {
			group := chunk.GetResultData().GetGroupByFieldValue()
			if !legacy {
				group = chunk.GetResultData().GetGroupByFieldValues()[0]
			}
			values = append(values, typeutil.GetDataIterator(group)(0))
			buffer := &orderedChildBuffer{}
			_, err := buffer.accept(chunk, 2, 2)
			require.NoError(t, err)
			unit := buffer.front()
			withGroup, err := unit.reducibleByteSize()
			require.NoError(t, err)
			unit.data.GroupByFieldValue = nil
			unit.data.GroupByFieldValues = nil
			withoutGroup, err := unit.reducibleByteSize()
			require.NoError(t, err)
			require.Greater(t, withGroup, withoutGroup)
		}
		require.Equal(t, []any{int64(10), nil, int64(20), int64(10)}, values)
	}
}

func TestSearchGroupByStreamRejectsMissingGroupColumn(t *testing.T) {
	child := &fakeReduceStream{recv: []fakeStreamRecv{{chunk: newSearchChunk(1, 1, []testHit{{1, .9}})}}}
	stream, err := NewReduceStream(&internalpb.SearchRequest{Nq: 1, Topk: 1, GroupByFieldIds: []int64{200}}, []ReduceStream{child}, 1)
	require.NoError(t, err)
	_, err = stream.Recv()
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.ErrorContains(t, err, "missing group-by field 200")
	require.NoError(t, stream.Close())
	_, closeCalls := child.calls()
	require.Equal(t, 1, closeCalls)
}

func TestSearchGroupByStreamEmptyChildren(t *testing.T) {
	stream, err := NewReduceStream(&internalpb.SearchRequest{Nq: 1, Topk: 2, GroupSize: 3, GroupByFieldIds: []int64{200}}, nil, 1)
	require.NoError(t, err)
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, stream.Close())
}
