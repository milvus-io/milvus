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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func assignSegmentID(ctx context.Context, insertMsg *msgstream.InsertMsg, result *milvuspb.MutationResult, channelNames []string, idAllocator *allocator.IDAllocator, segIDAssigner *segIDAssigner) (*msgstream.MsgPack, error) {
	threshold := Params.PulsarCfg.MaxMessageSize.GetAsInt()
	log.Debug("assign segmentid", zap.Int("threshold", threshold))

	msgPack := &msgstream.MsgPack{
		BeginTs: insertMsg.BeginTs(),
		EndTs:   insertMsg.EndTs(),
	}

	// generate hash value for every primary key
	if len(insertMsg.HashValues) != 0 {
		log.Warn("the hashvalues passed through client is not supported now, and will be overwritten")
	}
	insertMsg.HashValues = typeutil.HashPK2Channels(result.IDs, channelNames)
	// groupedHashKeys represents the dmChannel index
	channel2RowOffsets := make(map[string][]int)  //   channelName to count
	channelMaxTSMap := make(map[string]Timestamp) //  channelName to max Timestamp

	// assert len(it.hashValues) < maxInt
	for offset, channelID := range insertMsg.HashValues {
		channelName := channelNames[channelID]
		if _, ok := channel2RowOffsets[channelName]; !ok {
			channel2RowOffsets[channelName] = []int{}
		}
		channel2RowOffsets[channelName] = append(channel2RowOffsets[channelName], offset)

		if _, ok := channelMaxTSMap[channelName]; !ok {
			channelMaxTSMap[channelName] = typeutil.ZeroTimestamp
		}
		ts := insertMsg.Timestamps[offset]
		if channelMaxTSMap[channelName] < ts {
			channelMaxTSMap[channelName] = ts
		}
	}

	// pre-alloc msg id by batch
	var idBegin, idEnd int64
	var err error

	// fetch next id, if not id available, fetch next batch
	// lazy fetch, get first batch after first getMsgID called
	getMsgID := func() (int64, error) {
		if idBegin == idEnd {
			err = retry.Do(ctx, func() error {
				idBegin, idEnd, err = idAllocator.Alloc(16)
				return err
			})
			if err != nil {
				log.Error("failed to allocate msg id", zap.Int64("base.MsgID", insertMsg.Base.MsgID), zap.Error(err))
				return 0, err
			}
		}
		result := idBegin
		idBegin++
		return result, nil
	}

	// create empty insert message
	createInsertMsg := func(segmentID UniqueID, channelName string, msgID int64) *msgstream.InsertMsg {
		insertReq := msgpb.InsertRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Insert),
				commonpbutil.WithMsgID(msgID),
				commonpbutil.WithTimeStamp(insertMsg.BeginTimestamp), // entity's timestamp was set to equal it.BeginTimestamp in preExecute()
				commonpbutil.WithSourceID(insertMsg.Base.SourceID),
			),
			CollectionID:   insertMsg.CollectionID,
			PartitionID:    insertMsg.PartitionID,
			CollectionName: insertMsg.CollectionName,
			PartitionName:  insertMsg.PartitionName,
			SegmentID:      segmentID,
			ShardName:      channelName,
			Version:        msgpb.InsertDataVersion_ColumnBased,
		}
		insertReq.FieldsData = make([]*schemapb.FieldData, len(insertMsg.GetFieldsData()))

		msg := &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				Ctx: ctx,
			},
			InsertRequest: insertReq,
		}

		return msg
	}

	// repack the row data corresponding to the offset to insertMsg
	getInsertMsgsBySegmentID := func(segmentID UniqueID, rowOffsets []int, channelName string, maxMessageSize int) ([]msgstream.TsMsg, error) {
		repackedMsgs := make([]msgstream.TsMsg, 0)
		requestSize := 0
		msgID, err := getMsgID()
		if err != nil {
			return nil, err
		}
		msg := createInsertMsg(segmentID, channelName, msgID)
		for _, offset := range rowOffsets {
			curRowMessageSize, err := typeutil.EstimateEntitySize(insertMsg.GetFieldsData(), offset)
			if err != nil {
				return nil, err
			}

			// if insertMsg's size is greater than the threshold, split into multiple insertMsgs
			if requestSize+curRowMessageSize >= maxMessageSize {
				repackedMsgs = append(repackedMsgs, msg)
				msgID, err = getMsgID()
				if err != nil {
					return nil, err
				}
				msg = createInsertMsg(segmentID, channelName, msgID)
				requestSize = 0
			}

			typeutil.AppendFieldData(msg.FieldsData, insertMsg.GetFieldsData(), int64(offset))
			msg.HashValues = append(msg.HashValues, insertMsg.HashValues[offset])
			msg.Timestamps = append(msg.Timestamps, insertMsg.Timestamps[offset])
			msg.RowIDs = append(msg.RowIDs, insertMsg.RowIDs[offset])
			msg.NumRows++
			requestSize += curRowMessageSize
		}
		repackedMsgs = append(repackedMsgs, msg)

		return repackedMsgs, nil
	}

	// get allocated segmentID info for every dmChannel and repack insertMsgs for every segmentID
	for channelName, rowOffsets := range channel2RowOffsets {
		assignedSegmentInfos, err := segIDAssigner.GetSegmentID(insertMsg.CollectionID, insertMsg.PartitionID, channelName, uint32(len(rowOffsets)), channelMaxTSMap[channelName])
		if err != nil {
			log.Error("allocate segmentID for insert data failed", zap.Int64("collectionID", insertMsg.CollectionID), zap.String("channel name", channelName),
				zap.Int("allocate count", len(rowOffsets)),
				zap.Error(err))
			return nil, err
		}

		startPos := 0
		for segmentID, count := range assignedSegmentInfos {
			subRowOffsets := rowOffsets[startPos : startPos+int(count)]
			insertMsgs, err := getInsertMsgsBySegmentID(segmentID, subRowOffsets, channelName, threshold)
			if err != nil {
				log.Error("repack insert data to insert msgs failed", zap.Int64("collectionID", insertMsg.CollectionID),
					zap.Error(err))
				return nil, err
			}
			msgPack.Msgs = append(msgPack.Msgs, insertMsgs...)
			startPos += int(count)
		}
	}

	return msgPack, nil
}
