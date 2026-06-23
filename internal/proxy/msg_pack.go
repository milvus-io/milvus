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
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func genInsertMsgsByPartition(ctx context.Context,
	segmentID UniqueID,
	partitionID UniqueID,
	partitionName string,
	rowOffsets []int,
	channelName string,
	insertMsg *msgstream.InsertMsg,
) ([]msgstream.TsMsg, error) {
	msgs, _, err := genInsertMsgsByPartitionWithOCC(ctx, segmentID, partitionID, partitionName, rowOffsets, channelName, insertMsg, nil)
	return msgs, err
}

// OCCRowMeta carries per-row partial-update OCC metadata used by streaming
// upsert. The three slices/IDs are aligned with the rows of the merged
// InsertMsg produced by upsertTask.queryPreExecute.
type OCCRowMeta struct {
	PKs            *schemapb.IDs
	ExpectedTs     []uint64
	ExpectedExists []bool
}

// buildUpsertOCCInput returns OCC metadata for streaming upsert. It returns
// nil when the request is not a partial update or carries no expected-row
// information, so the streaming path falls back to a plain insert.
func buildUpsertOCCInput(partialUpdate bool, pks *schemapb.IDs, expectedTs []uint64, expectedExists []bool) *OCCRowMeta {
	if !partialUpdate || len(expectedTs) == 0 {
		return nil
	}
	return &OCCRowMeta{
		PKs:            pks,
		ExpectedTs:     expectedTs,
		ExpectedExists: expectedExists,
	}
}

// attachOCCToInsertHeader stamps the CAS mode and per-row expected version
// arrays from occ onto header in place. occ is assumed non-nil.
func attachOCCToInsertHeader(header *message.InsertMessageHeader, occ *OCCRowMeta) {
	header.OccMode = messagespb.OCCMode_OCC_MODE_CAS
	header.ExpectedPks = occ.PKs
	header.ExpectedRowTimestamps = occ.ExpectedTs
	header.ExpectedRowExists = occ.ExpectedExists
}

// genInsertMsgsByPartitionWithOCC repacks insert rows into one or more
// InsertMsgs respecting the WAL message size threshold. When occInput is
// non-nil it ALSO returns one OCCRowMeta per output InsertMsg, aligned 1:1
// with the returned msgs, so the caller can attach the matching CAS header.
func genInsertMsgsByPartitionWithOCC(ctx context.Context,
	segmentID UniqueID,
	partitionID UniqueID,
	partitionName string,
	rowOffsets []int,
	channelName string,
	insertMsg *msgstream.InsertMsg,
	occInput *OCCRowMeta,
) ([]msgstream.TsMsg, []*OCCRowMeta, error) {
	threshold := Params.PulsarCfg.MaxMessageSize.GetAsInt()

	// create empty insert message
	createInsertMsg := func(segmentID UniqueID, channelName string) *msgstream.InsertMsg {
		insertReq := &msgpb.InsertRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Insert),
				commonpbutil.WithTimeStamp(insertMsg.BeginTimestamp), // entity's timestamp was set to equal it.BeginTimestamp in preExecute()
				commonpbutil.WithSourceID(insertMsg.Base.SourceID),
			),
			CollectionID:   insertMsg.CollectionID,
			PartitionID:    partitionID,
			DbName:         insertMsg.DbName,
			CollectionName: insertMsg.CollectionName,
			PartitionName:  partitionName,
			SegmentID:      segmentID,
			ShardName:      channelName,
			Version:        msgpb.InsertDataVersion_ColumnBased,
			FieldsData:     make([]*schemapb.FieldData, len(insertMsg.GetFieldsData())),
		}
		msg := &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				Ctx: ctx,
			},
			InsertRequest: insertReq,
		}

		return msg
	}

	fieldsData := insertMsg.GetFieldsData()
	idxComputer := typeutil.NewFieldDataIdxComputer(fieldsData)

	repackedMsgs := make([]msgstream.TsMsg, 0)
	occOuts := make([]*OCCRowMeta, 0)
	requestSize := 0
	msg := createInsertMsg(segmentID, channelName)
	var curOCC *OCCRowMeta
	if occInput != nil {
		curOCC = &OCCRowMeta{PKs: &schemapb.IDs{}}
	}
	for _, offset := range rowOffsets {
		fieldIdxs := idxComputer.Compute(int64(offset))
		curRowMessageSize, err := typeutil.EstimateEntitySize(fieldsData, offset, fieldIdxs...)
		if err != nil {
			return nil, nil, err
		}

		// If the insert message size exceeds the threshold, flush the current
		// message first. A single row can be larger than the threshold, so do
		// not emit an empty message before adding that row.
		if msg.NumRows > 0 && requestSize+curRowMessageSize >= threshold {
			repackedMsgs = append(repackedMsgs, msg)
			if curOCC != nil {
				occOuts = append(occOuts, curOCC)
				curOCC = &OCCRowMeta{PKs: &schemapb.IDs{}}
			}
			msg = createInsertMsg(segmentID, channelName)
			requestSize = 0
		}

		typeutil.AppendFieldData(msg.FieldsData, fieldsData, int64(offset), fieldIdxs...)
		msg.HashValues = append(msg.HashValues, insertMsg.HashValues[offset])
		msg.Timestamps = append(msg.Timestamps, insertMsg.Timestamps[offset])
		msg.RowIDs = append(msg.RowIDs, insertMsg.RowIDs[offset])
		msg.NumRows++
		requestSize += curRowMessageSize
		if curOCC != nil {
			typeutil.AppendIDs(curOCC.PKs, occInput.PKs, offset)
			curOCC.ExpectedTs = append(curOCC.ExpectedTs, occInput.ExpectedTs[offset])
			curOCC.ExpectedExists = append(curOCC.ExpectedExists, occInput.ExpectedExists[offset])
		}
	}
	if msg.NumRows > 0 {
		repackedMsgs = append(repackedMsgs, msg)
		if curOCC != nil {
			occOuts = append(occOuts, curOCC)
		}
	}

	if occInput == nil {
		return repackedMsgs, nil, nil
	}
	return repackedMsgs, occOuts, nil
}
