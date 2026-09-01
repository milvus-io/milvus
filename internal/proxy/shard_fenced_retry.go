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
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

// A write that spans several shards is not all-or-nothing.
//
// AppendMessages hands one message per vchannel to its own producer and commits
// them independently, and the shard interceptor refuses a fenced vchannel at
// append time. So when a shard split fences ONE shard of a hash-routed
// collection -- which is what an automatic doubling does -- the request's other
// shards commit while that one is refused. Re-sending the whole request then
// writes the committed rows a second time.
//
// The retry unit is therefore the ROW, not the request and not the shard: the
// fenced shard never accepts writes again, so the refused rows have to be
// re-routed against the post-split topology, where they land on the shards that
// replaced it. Per-message granularity is exactly what makes this work — one
// message belongs to one vchannel and is appended or refused whole, never in
// part — so the rows of a refused message are precisely the rows still to write.
//
// A rehash fences every shard at once, so every message is refused and the row
// set does not shrink; this is a strict refinement of the previous behavior,
// not a change to it.

// rowSet is the set of row offsets a write attempt still has to place.
// The zero value means "every row", which is the first attempt and the only
// state the non-split path ever sees.
type rowSet map[int]struct{}

func newRowSet(offsets []int) rowSet {
	set := make(rowSet, len(offsets))
	for _, offset := range offsets {
		set[offset] = struct{}{}
	}
	return set
}

// retain narrows a routing result to the rows still pending, dropping any
// channel left with nothing to write. A nil rowSet retains everything, so the
// first attempt costs no work at all.
func (s rowSet) retain(channel2RowOffsets map[string][]int) map[string][]int {
	if s == nil {
		return channel2RowOffsets
	}
	retained := make(map[string][]int, len(channel2RowOffsets))
	for channel, offsets := range channel2RowOffsets {
		kept := make([]int, 0, len(offsets))
		for _, offset := range offsets {
			if _, ok := s[offset]; ok {
				kept = append(kept, offset)
			}
		}
		if len(kept) > 0 {
			retained[channel] = kept
		}
	}
	return retained
}

// refusedRows collects the rows of every message the WAL refused with
// ShardFenced, and reports the first error that was NOT a fence.
//
// A message with no error placed its rows durably: they leave the pending set
// and are never sent again. Any other error is not ours to retry — it is
// returned so the caller fails the request rather than replaying it.
func refusedRows(resp streaming.AppendResponses, messageOffsets [][]int) (refused []int, fenceErr error, fatalErr error) {
	for i, r := range resp.Responses {
		if r.Error == nil {
			continue
		}
		if !status.AsStreamingError(r.Error).IsShardFenced() {
			return nil, nil, r.Error
		}
		fenceErr = r.Error
		if i < len(messageOffsets) {
			refused = append(refused, messageOffsets[i]...)
		}
	}
	return refused, fenceErr, nil
}

// allRowOffsets is the pending set a write starts from: every row of the
// request.
func allRowOffsets(numRows int) []int {
	offsets := make([]int, numRows)
	for i := range offsets {
		offsets[i] = i
	}
	return offsets
}
