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

package idempotencyview

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RecordsFromSections joins the two halves a summary chunk stores apart.
//
// The summary keeps them apart because that is what they are to it: the insert
// section is self-sufficient and read on its own by anything that wants the
// primary keys, and the idempotency section is the overlay that says which
// client key produced a write. Only this view needs both, so the join lives
// here rather than in the store.
//
// keys may be nil, which means no write of that vchannel carried a client key.
// When it is not nil it has exactly the same length as inserts, because a write
// without a key still takes its slot; the store rejects any other shape, and
// this rejects it again rather than trusting the caller -- a misaligned join
// would answer a duplicate with another write's rows.
func RecordsFromSections(
	keys []*streamingpb.VChannelSummaryIdempotencyRecord,
	inserts []*streamingpb.VChannelSummaryInsertRecord,
) ([]*Record, error) {
	if len(keys) != 0 && len(keys) != len(inserts) {
		return nil, merr.WrapErrServiceInternalMsg(
			"idempotency summary sections are misaligned: keys and inserts must pair by position")
	}
	records := make([]*Record, 0, len(inserts))
	for i, insert := range inserts {
		record := &Record{
			SourceMessageID:        insert.GetSourceMessageId(),
			SourceTimeTick:         insert.GetSourceTimetick(),
			LastConfirmedMessageID: insert.GetLastConfirmedMessageId(),
		}
		var rowOffsets []uint32
		if len(keys) != 0 {
			record.IdempotencyKey = keys[i].GetKey()
			rowOffsets = keys[i].GetRowOffsets()
		}
		// A record with neither keys nor offsets carries no result to replay:
		// it is a write the view remembers the position of but has nothing to
		// answer a duplicate with, so the result stays nil rather than becoming
		// an empty message that reads as an answer.
		if insert.GetIds() != nil || len(rowOffsets) > 0 {
			record.InsertResult = &messagespb.IdempotentInsertResult{
				RowOffsets: rowOffsets,
				Ids:        insert.GetIds(),
			}
		}
		records = append(records, record)
	}
	return records, nil
}
