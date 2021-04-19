package proxynode

import (
	"errors"
	"log"
	"sort"
	"unsafe"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

func insertRepackFunc(tsMsgs []msgstream.TsMsg,
	hashKeys [][]int32,
	segIDAssigner *SegIDAssigner,
	together bool) (map[int32]*msgstream.MsgPack, error) {

	result := make(map[int32]*msgstream.MsgPack)

	channelCountMap := make(map[UniqueID]map[int32]uint32)    //  reqID --> channelID to count
	channelMaxTSMap := make(map[UniqueID]map[int32]Timestamp) //  reqID --> channelID to max Timestamp
	reqSchemaMap := make(map[UniqueID][]UniqueID)             //  reqID --> channelID [2]UniqueID {CollectionID, PartitionID}
	channelNamesMap := make(map[UniqueID][]string)            // collectionID --> channelNames

	for i, request := range tsMsgs {
		if request.Type() != commonpb.MsgType_Insert {
			return nil, errors.New("msg's must be Insert")
		}
		insertRequest, ok := request.(*msgstream.InsertMsg)
		if !ok {
			return nil, errors.New("msg's must be Insert")
		}

		keys := hashKeys[i]
		timestampLen := len(insertRequest.Timestamps)
		rowIDLen := len(insertRequest.RowIDs)
		rowDataLen := len(insertRequest.RowData)
		keysLen := len(keys)

		if keysLen != timestampLen || keysLen != rowIDLen || keysLen != rowDataLen {
			return nil, errors.New("the length of hashValue, timestamps, rowIDs, RowData are not equal")
		}

		reqID := insertRequest.Base.MsgID
		if _, ok := channelCountMap[reqID]; !ok {
			channelCountMap[reqID] = make(map[int32]uint32)
		}

		if _, ok := channelMaxTSMap[reqID]; !ok {
			channelMaxTSMap[reqID] = make(map[int32]Timestamp)
		}

		if _, ok := reqSchemaMap[reqID]; !ok {
			reqSchemaMap[reqID] = []UniqueID{insertRequest.CollectionID, insertRequest.PartitionID}
		}

		for idx, channelID := range keys {
			channelCountMap[reqID][channelID]++
			if _, ok := channelMaxTSMap[reqID][channelID]; !ok {
				channelMaxTSMap[reqID][channelID] = typeutil.ZeroTimestamp
			}
			ts := insertRequest.Timestamps[idx]
			if channelMaxTSMap[reqID][channelID] < ts {
				channelMaxTSMap[reqID][channelID] = ts
			}
		}

		collID := insertRequest.CollectionID
		if _, ok := channelNamesMap[collID]; !ok {
			channelNames, err := globalInsertChannelsMap.getInsertChannels(collID)
			if err != nil {
				return nil, err
			}
			channelNamesMap[collID] = channelNames
		}
	}

	var getChannelName = func(collID UniqueID, channelID int32) string {
		if _, ok := channelNamesMap[collID]; !ok {
			return ""
		}
		names := channelNamesMap[collID]
		return names[channelID]
	}

	reqSegCountMap := make(map[UniqueID]map[int32]map[UniqueID]uint32)

	for reqID, countInfo := range channelCountMap {
		if _, ok := reqSegCountMap[reqID]; !ok {
			reqSegCountMap[reqID] = make(map[int32]map[UniqueID]uint32)
		}
		schema := reqSchemaMap[reqID]
		collID, partitionID := schema[0], schema[1]
		for channelID, count := range countInfo {
			ts, ok := channelMaxTSMap[reqID][channelID]
			if !ok {
				ts = typeutil.ZeroTimestamp
				log.Println("Warning: did not get max Timstamp!")
			}
			channelName := getChannelName(collID, channelID)
			if channelName == "" {
				return nil, errors.New("ProxyNode, repack_func, can not found channelName")
			}
			mapInfo, err := segIDAssigner.GetSegmentID(collID, partitionID, channelName, count, ts)
			if err != nil {
				return nil, err
			}
			reqSegCountMap[reqID][channelID] = make(map[UniqueID]uint32)
			reqSegCountMap[reqID][channelID] = mapInfo
			log.Println("ProxyNode: repackFunc, reqSegCountMap, reqID:", reqID, " mapInfo: ", mapInfo)
		}
	}

	reqSegAccumulateCountMap := make(map[UniqueID]map[int32][]uint32)
	reqSegIDMap := make(map[UniqueID]map[int32][]UniqueID)
	reqSegAllocateCounter := make(map[UniqueID]map[int32]uint32)

	for reqID, channelInfo := range reqSegCountMap {
		if _, ok := reqSegAccumulateCountMap[reqID]; !ok {
			reqSegAccumulateCountMap[reqID] = make(map[int32][]uint32)
		}
		if _, ok := reqSegIDMap[reqID]; !ok {
			reqSegIDMap[reqID] = make(map[int32][]UniqueID)
		}
		if _, ok := reqSegAllocateCounter[reqID]; !ok {
			reqSegAllocateCounter[reqID] = make(map[int32]uint32)
		}
		for channelID, segInfo := range channelInfo {
			reqSegAllocateCounter[reqID][channelID] = 0
			keys := make([]UniqueID, len(segInfo))
			i := 0
			for key := range segInfo {
				keys[i] = key
				i++
			}
			sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
			accumulate := uint32(0)
			for _, key := range keys {
				accumulate += segInfo[key]
				if _, ok := reqSegAccumulateCountMap[reqID][channelID]; !ok {
					reqSegAccumulateCountMap[reqID][channelID] = make([]uint32, 0)
				}
				reqSegAccumulateCountMap[reqID][channelID] = append(
					reqSegAccumulateCountMap[reqID][channelID],
					accumulate,
				)
				if _, ok := reqSegIDMap[reqID][channelID]; !ok {
					reqSegIDMap[reqID][channelID] = make([]UniqueID, 0)
				}
				reqSegIDMap[reqID][channelID] = append(
					reqSegIDMap[reqID][channelID],
					key,
				)
			}
		}
	}

	var getSegmentID = func(reqID UniqueID, channelID int32) UniqueID {
		reqSegAllocateCounter[reqID][channelID]++
		cur := reqSegAllocateCounter[reqID][channelID]
		accumulateSlice := reqSegAccumulateCountMap[reqID][channelID]
		segIDSlice := reqSegIDMap[reqID][channelID]
		for index, count := range accumulateSlice {
			if cur <= count {
				return segIDSlice[index]
			}
		}
		log.Panic("Can't Found SegmentID")
		return 0
	}

	factor := 10
	threshold := Params.PulsarMaxMessageSize / factor
	log.Println("threshold of message size: ", threshold)
	// not accurate
	getSizeOfInsertMsg := func(msg *msgstream.InsertMsg) int {
		// if real struct, call unsafe.Sizeof directly,
		// if reference, dereference and then call unsafe.Sizeof,
		// if slice, todo: a common function to calculate size of slice,
		// if map, a little complicated
		size := 0
		size += int(unsafe.Sizeof(msg.BeginTimestamp))
		size += int(unsafe.Sizeof(msg.EndTimestamp))
		size += int(unsafe.Sizeof(msg.HashValues))
		size += len(msg.HashValues) * 4
		size += int(unsafe.Sizeof(*msg.MsgPosition))
		size += int(unsafe.Sizeof(*msg.Base))
		size += int(unsafe.Sizeof(msg.DbName))
		size += int(unsafe.Sizeof(msg.CollectionName))
		size += int(unsafe.Sizeof(msg.PartitionName))
		size += int(unsafe.Sizeof(msg.DbID))
		size += int(unsafe.Sizeof(msg.CollectionID))
		size += int(unsafe.Sizeof(msg.PartitionID))
		size += int(unsafe.Sizeof(msg.SegmentID))
		size += int(unsafe.Sizeof(msg.ChannelID))
		size += int(unsafe.Sizeof(msg.Timestamps))
		size += int(unsafe.Sizeof(msg.RowIDs))
		size += len(msg.RowIDs) * 8
		for _, blob := range msg.RowData {
			size += int(unsafe.Sizeof(blob.Value))
			size += len(blob.Value)
		}
		// log.Println("size of insert message: ", size)
		return size
	}
	// not accurate
	// getSizeOfMsgPack := func(mp *msgstream.MsgPack) int {
	// 	size := 0
	// 	for _, msg := range mp.Msgs {
	// 		insertMsg, ok := msg.(*msgstream.InsertMsg)
	// 		if !ok {
	// 			log.Panic("only insert message is supported!")
	// 		}
	// 		size += getSizeOfInsertMsg(insertMsg)
	// 	}
	// 	return size
	// }

	for i, request := range tsMsgs {
		insertRequest := request.(*msgstream.InsertMsg)
		keys := hashKeys[i]
		reqID := insertRequest.Base.MsgID
		collectionName := insertRequest.CollectionName
		collectionID := insertRequest.CollectionID
		partitionID := insertRequest.PartitionID
		partitionName := insertRequest.PartitionName
		proxyID := insertRequest.Base.SourceID
		channelNames := channelNamesMap[collectionID]
		for index, key := range keys {
			ts := insertRequest.Timestamps[index]
			rowID := insertRequest.RowIDs[index]
			row := insertRequest.RowData[index]
			_, ok := result[key]
			if !ok {
				msgPack := msgstream.MsgPack{}
				result[key] = &msgPack
			}
			segmentID := getSegmentID(reqID, key)
			channelID := channelNames[int(key)%len(channelNames)]
			sliceRequest := internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Insert,
					MsgID:     reqID,
					Timestamp: ts,
					SourceID:  proxyID,
				},
				CollectionID:   collectionID,
				PartitionID:    partitionID,
				CollectionName: collectionName,
				PartitionName:  partitionName,
				SegmentID:      segmentID,
				// todo rename to ChannelName
				// ChannelID:  strconv.FormatInt(int64(key), 10),
				ChannelID:  channelID,
				Timestamps: []uint64{ts},
				RowIDs:     []int64{rowID},
				RowData:    []*commonpb.Blob{row},
			}
			insertMsg := &msgstream.InsertMsg{
				InsertRequest: sliceRequest,
			}
			if together { // all rows with same hash value are accumulated to only one message
				msgNums := len(result[key].Msgs)
				if len(result[key].Msgs) <= 0 {
					result[key].Msgs = append(result[key].Msgs, insertMsg)
				} else if getSizeOfInsertMsg(result[key].Msgs[msgNums-1].(*msgstream.InsertMsg)) >= threshold {
					result[key].Msgs = append(result[key].Msgs, insertMsg)
				} else {
					accMsgs, _ := result[key].Msgs[msgNums-1].(*msgstream.InsertMsg)
					accMsgs.Timestamps = append(accMsgs.Timestamps, ts)
					accMsgs.RowIDs = append(accMsgs.RowIDs, rowID)
					accMsgs.RowData = append(accMsgs.RowData, row)
				}
			} else { // every row is a message
				result[key].Msgs = append(result[key].Msgs, insertMsg)
			}
		}
	}

	return result, nil
}
