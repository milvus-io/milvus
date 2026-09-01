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

package proxy

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func fencedErr() error {
	return status.NewShardFenced("v0", 100, 0)
}

func appendResponses(errs ...error) streaming.AppendResponses {
	resp := streaming.AppendResponses{Responses: make([]streaming.AppendResponse, 0, len(errs))}
	for _, err := range errs {
		resp.Responses = append(resp.Responses, streaming.AppendResponse{Error: err})
	}
	return resp
}

func TestAllRowOffsets(t *testing.T) {
	assert.Equal(t, []int{0, 1, 2}, allRowOffsets(3))
	assert.Empty(t, allRowOffsets(0))
}

// A nil set is the first attempt: it retains everything and costs no work.
func TestRowSetNilRetainsEverything(t *testing.T) {
	var s rowSet
	in := map[string][]int{"a": {0, 1}, "b": {2}}
	assert.Equal(t, in, s.retain(in))
}

func TestRowSetRetainsOnlyPendingRowsAndDropsEmptyChannels(t *testing.T) {
	s := newRowSet([]int{1, 4})
	got := s.retain(map[string][]int{
		"a": {0, 1, 2},
		"b": {3}, // nothing pending -> channel disappears
		"c": {4, 5},
	})
	assert.Equal(t, map[string][]int{"a": {1}, "c": {4}}, got)
}

// The point of the whole file: rows that landed must never be sent again, and
// the rows of a refused message must all come back.
func TestRefusedRowsCollectsOnlyTheFencedMessages(t *testing.T) {
	resp := appendResponses(nil, fencedErr(), nil, fencedErr())
	offsets := [][]int{{0, 1}, {2, 3}, {4}, {5, 6, 7}}

	refused, fenceErr, fatalErr := refusedRows(resp, offsets)
	require.NoError(t, fatalErr)
	require.Error(t, fenceErr)
	assert.Equal(t, []int{2, 3, 5, 6, 7}, refused)
}

func TestRefusedRowsReportsNothingWhenEveryMessageLanded(t *testing.T) {
	refused, fenceErr, fatalErr := refusedRows(appendResponses(nil, nil), [][]int{{0}, {1}})
	require.NoError(t, fatalErr)
	assert.NoError(t, fenceErr)
	assert.Empty(t, refused)
}

// A non-fence error is not ours to retry: it is returned so the caller fails the
// request rather than replaying rows that may already be durable.
func TestRefusedRowsReturnsANonFenceErrorAsFatal(t *testing.T) {
	boom := errors.New("boom")
	refused, fenceErr, fatalErr := refusedRows(appendResponses(nil, boom, fencedErr()), [][]int{{0}, {1}, {2}})
	assert.ErrorIs(t, fatalErr, boom)
	assert.NoError(t, fenceErr)
	assert.Nil(t, refused)
}

// The delete half passes no offsets: it is re-packed whole, so it only needs to
// know THAT it was fenced.
func TestRefusedRowsToleratesMissingOffsets(t *testing.T) {
	refused, fenceErr, fatalErr := refusedRows(appendResponses(fencedErr()), nil)
	require.NoError(t, fatalErr)
	assert.Error(t, fenceErr)
	assert.Empty(t, refused)
}

func TestShardFencedErrorIsRecognised(t *testing.T) {
	// Guards the tests above against being vacuous: if the constructed error did
	// not read as a fence, every case would take the fatal branch instead.
	assert.True(t, status.AsStreamingError(fencedErr()).IsShardFenced())
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_SHARD_FENCED,
		status.AsStreamingError(fencedErr()).Code)
}
