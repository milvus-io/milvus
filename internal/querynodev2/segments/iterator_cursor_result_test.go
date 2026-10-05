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

package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/iteratorutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newCursorAckTestResult(pk int64, acknowledged bool, body bool) *internalpb.SearchResults {
	result := &internalpb.SearchResults{Status: merr.Success(), MetricType: "IP", NumQueries: 1, TopK: 1}
	if acknowledged {
		iteratorutil.MarkPKCursor(result.Status)
	}
	if body {
		result.ResultData = &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}}},
			Scores:     []float32{0.9},
			Topks:      []int64{1},
		}
	}
	return result
}

func TestReduceSearchResultsPKCursorShortcutRetainsEveryParticipant(t *testing.T) {
	for _, tc := range []struct {
		name         string
		emptyWorker  *internalpb.SearchResults
		acknowledged bool
	}{
		{name: "nil worker", emptyWorker: nil},
		{name: "bodyless legacy worker", emptyWorker: &internalpb.SearchResults{}},
		{name: "empty legacy success", emptyWorker: newCursorAckTestResult(0, false, false)},
		{name: "empty acknowledged success", emptyWorker: newCursorAckTestResult(0, true, false), acknowledged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			visible := newCursorAckTestResult(1, true, true)
			before := proto.Clone(visible).(*internalpb.SearchResults)
			out, err := ReduceSearchResults(context.Background(), []*internalpb.SearchResults{visible, tc.emptyWorker},
				reduce.NewReduceSearchResultInfo(1, 1).WithMetricType("IP").WithPkType(schemapb.DataType_Int64))
			require.NoError(t, err)
			require.Equal(t, tc.acknowledged, out.GetStatus().GetExtraInfo()[iteratorutil.CursorVersionKey] == "2")
			require.Equal(t, []int64{1}, out.GetResultData().GetIds().GetIntId().GetData())
			// Stripping a mixed-worker ACK must never mutate the single-body
			// shortcut's input; other consumers can still inspect that worker.
			require.True(t, proto.Equal(before, visible))
			if !tc.acknowledged {
				require.NotSame(t, visible, out)
			}
		})
	}
}

func TestReduceSearchResultsPKCursorNormalReduceRetainsFilteredWorkers(t *testing.T) {
	for _, tc := range []struct {
		name         string
		emptyWorker  *internalpb.SearchResults
		acknowledged bool
	}{
		{name: "nil worker", emptyWorker: nil},
		{name: "empty legacy worker", emptyWorker: newCursorAckTestResult(0, false, false)},
		{name: "empty acknowledged worker", emptyWorker: newCursorAckTestResult(0, true, false), acknowledged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, second := newCursorAckTestResult(1, true, true), newCursorAckTestResult(2, true, true)
			firstBefore, secondBefore := proto.Clone(first), proto.Clone(second)
			out, err := ReduceSearchResults(context.Background(), []*internalpb.SearchResults{first, second, tc.emptyWorker},
				reduce.NewReduceSearchResultInfo(1, 1).WithMetricType("IP").WithPkType(schemapb.DataType_Int64))
			require.NoError(t, err)
			require.Equal(t, tc.acknowledged, out.GetStatus().GetExtraInfo()[iteratorutil.CursorVersionKey] == "2")
			decoded, err := DecodeSearchResults(context.Background(), []*internalpb.SearchResults{out})
			require.NoError(t, err)
			require.Len(t, decoded, 1)
			require.Equal(t, int64(1), decoded[0].GetNumQueries())
			require.Len(t, decoded[0].GetScores(), 1)
			require.True(t, proto.Equal(firstBefore, first))
			require.True(t, proto.Equal(secondBefore, second))
		})
	}
}
