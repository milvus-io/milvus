package proxy

import (
	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func insertRepackFunc(tsMsgs []msgstream.TsMsg,
	hashKeys [][]int32,
	segIDAssigner *allocator.SegIDAssigner,
	together bool) (map[int32]*msgstream.MsgPack, error) {

	result := make(map[int32]*msgstream.MsgPack)

	for i, request := range tsMsgs {
		if request.Type() != internalpb.MsgType_kInsert {
			return nil, errors.New(string("msg's must be Insert"))
		}
		insertRequest, ok := request.(*msgstream.InsertMsg)
		if !ok {
			return nil, errors.New(string("msg's must be Insert"))
		}
		keys := hashKeys[i]

		timestampLen := len(insertRequest.Timestamps)
		rowIDLen := len(insertRequest.RowIDs)
		rowDataLen := len(insertRequest.RowData)
		keysLen := len(keys)

		if keysLen != timestampLen || keysLen != rowIDLen || keysLen != rowDataLen {
			return nil, errors.New(string("the length of hashValue, timestamps, rowIDs, RowData are not equal"))
		}

		reqID := insertRequest.ReqID
		collectionName := insertRequest.CollectionName
		partitionTag := insertRequest.PartitionTag
		channelID := insertRequest.ChannelID
		proxyID := insertRequest.ProxyID
		for index, key := range keys {
			ts := insertRequest.Timestamps[index]
			rowID := insertRequest.RowIDs[index]
			row := insertRequest.RowData[index]
			_, ok := result[key]
			if !ok {
				msgPack := msgstream.MsgPack{}
				result[key] = &msgPack
			}
			sliceRequest := internalpb.InsertRequest{
				MsgType:        internalpb.MsgType_kInsert,
				ReqID:          reqID,
				CollectionName: collectionName,
				PartitionTag:   partitionTag,
				SegmentID:      0, // will be assigned later if together
				ChannelID:      channelID,
				ProxyID:        proxyID,
				Timestamps:     []uint64{ts},
				RowIDs:         []int64{rowID},
				RowData:        []*commonpb.Blob{row},
			}
			insertMsg := &msgstream.InsertMsg{
				InsertRequest: sliceRequest,
			}
			if together { // all rows with same hash value are accumulated to only one message
				if len(result[key].Msgs) <= 0 {
					result[key].Msgs = append(result[key].Msgs, insertMsg)
				} else {
					accMsgs, _ := result[key].Msgs[0].(*msgstream.InsertMsg)
					accMsgs.Timestamps = append(accMsgs.Timestamps, ts)
					accMsgs.RowIDs = append(accMsgs.RowIDs, rowID)
					accMsgs.RowData = append(accMsgs.RowData, row)
				}
			} else { // every row is a message
				segID, _ := segIDAssigner.GetSegmentID(collectionName, partitionTag, int32(channelID), 1)
				insertMsg.SegmentID = segID
				result[key].Msgs = append(result[key].Msgs, insertMsg)
			}
		}
	}

	if together {
		for key := range result {
			insertMsg, _ := result[key].Msgs[0].(*msgstream.InsertMsg)
			rowNums := len(insertMsg.RowIDs)
			collectionName := insertMsg.CollectionName
			partitionTag := insertMsg.PartitionTag
			channelID := insertMsg.ChannelID
			segID, _ := segIDAssigner.GetSegmentID(collectionName, partitionTag, int32(channelID), uint32(rowNums))
			insertMsg.SegmentID = segID
			result[key].Msgs[0] = insertMsg
		}
	}

	return result, nil
}
