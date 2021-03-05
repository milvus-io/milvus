package util

import (
	"errors"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type MsgStream = msgstream.MsgStream
type TsMsg = msgstream.TsMsg
type MsgPack = msgstream.MsgPack
type BaseMsg = msgstream.BaseMsg

func InsertRepackFunc(tsMsgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range tsMsgs {
		if request.Type() != commonpb.MsgType_kInsert {
			return nil, errors.New("msg's must be Insert")
		}
		insertRequest := request.(*msgstream.InsertMsg)
		keys := hashKeys[i]

		timestampLen := len(insertRequest.Timestamps)
		rowIDLen := len(insertRequest.RowIDs)
		rowDataLen := len(insertRequest.RowData)
		keysLen := len(keys)

		if keysLen != timestampLen || keysLen != rowIDLen || keysLen != rowDataLen {
			return nil, errors.New("the length of hashValue, timestamps, rowIDs, RowData are not equal")
		}
		for index, key := range keys {
			_, ok := result[key]
			if !ok {
				msgPack := MsgPack{}
				result[key] = &msgPack
			}

			sliceRequest := internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kInsert,
					MsgID:     insertRequest.Base.MsgID,
					Timestamp: insertRequest.Timestamps[index],
					SourceID:  insertRequest.Base.SourceID,
				},
				DbID:           insertRequest.DbID,
				CollectionID:   insertRequest.CollectionID,
				PartitionID:    insertRequest.PartitionID,
				CollectionName: insertRequest.CollectionName,
				PartitionName:  insertRequest.PartitionName,
				SegmentID:      insertRequest.SegmentID,
				ChannelID:      insertRequest.ChannelID,
				Timestamps:     []uint64{insertRequest.Timestamps[index]},
				RowIDs:         []int64{insertRequest.RowIDs[index]},
				RowData:        []*commonpb.Blob{insertRequest.RowData[index]},
			}

			insertMsg := &msgstream.InsertMsg{
				InsertRequest: sliceRequest,
			}
			result[key].Msgs = append(result[key].Msgs, insertMsg)
		}
	}
	return result, nil
}

func DeleteRepackFunc(tsMsgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range tsMsgs {
		if request.Type() != commonpb.MsgType_kDelete {
			return nil, errors.New("msg's must be Delete")
		}
		deleteRequest := request.(*msgstream.DeleteMsg)
		keys := hashKeys[i]

		timestampLen := len(deleteRequest.Timestamps)
		primaryKeysLen := len(deleteRequest.PrimaryKeys)
		keysLen := len(keys)

		if keysLen != timestampLen || keysLen != primaryKeysLen {
			return nil, errors.New("the length of hashValue, timestamps, primaryKeys are not equal")
		}

		for index, key := range keys {
			_, ok := result[key]
			if !ok {
				msgPack := MsgPack{}
				result[key] = &msgPack
			}

			sliceRequest := internalpb2.DeleteRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kDelete,
					MsgID:     deleteRequest.Base.MsgID,
					Timestamp: deleteRequest.Timestamps[index],
					SourceID:  deleteRequest.Base.SourceID,
				},
				CollectionName: deleteRequest.CollectionName,
				ChannelID:      deleteRequest.ChannelID,
				Timestamps:     []uint64{deleteRequest.Timestamps[index]},
				PrimaryKeys:    []int64{deleteRequest.PrimaryKeys[index]},
			}

			deleteMsg := &msgstream.DeleteMsg{
				DeleteRequest: sliceRequest,
			}
			result[key].Msgs = append(result[key].Msgs, deleteMsg)
		}
	}
	return result, nil
}

func DefaultRepackFunc(tsMsgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range tsMsgs {
		keys := hashKeys[i]
		if len(keys) != 1 {
			return nil, errors.New("len(msg.hashValue) must equal 1")
		}
		key := keys[0]
		_, ok := result[key]
		if !ok {
			msgPack := MsgPack{}
			result[key] = &msgPack
		}
		result[key].Msgs = append(result[key].Msgs, request)
	}
	return result, nil
}
