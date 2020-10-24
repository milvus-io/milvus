package reader

import (
	"fmt"
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"log"
	"sync"
)

func (node *QueryNode) MessagesPreprocess(insertDeleteMessages []*msgPb.InsertOrDeleteMsg, timeRange TimeRange) msgPb.Status {
	var tMax = timeRange.timestampMax

	// 1. Extract messages before readTimeSync from QueryNodeDataBuffer.
	//    Set valid bitmap to false.
	//    If segmentId dose not exist in segments map, creating an new segment.
	for i, msg := range node.buffer.InsertDeleteBuffer {
		if msg.Timestamp < tMax {
			if !node.FoundSegmentBySegmentID(msg.SegmentId) {
				collection, _ := node.GetCollectionByCollectionName(msg.CollectionName)
				if collection != nil {
					partition := collection.GetPartitionByName(msg.PartitionTag)
					if partition != nil {
						newSegment := partition.NewSegment(msg.SegmentId)
						node.SegmentsMap[msg.SegmentId] = newSegment
					} else {
						log.Fatal("Cannot find partition:", msg.PartitionTag)
					}
				} else {
					log.Fatal("Cannot find collection:", msg.CollectionName)
				}
			}
			if msg.Op == msgPb.OpType_INSERT {
				if msg.RowsData == nil {
					continue
				}
				node.insertData.insertIDs[msg.SegmentId] = append(node.insertData.insertIDs[msg.SegmentId], msg.Uid)
				node.insertData.insertTimestamps[msg.SegmentId] = append(node.insertData.insertTimestamps[msg.SegmentId], msg.Timestamp)
				node.insertData.insertRecords[msg.SegmentId] = append(node.insertData.insertRecords[msg.SegmentId], msg.RowsData.Blob)
			} else if msg.Op == msgPb.OpType_DELETE {
				var r = DeleteRecord{
					entityID:  msg.Uid,
					timestamp: msg.Timestamp,
				}
				node.deletePreprocessData.deleteRecords = append(node.deletePreprocessData.deleteRecords, &r)
				node.deletePreprocessData.count++
			}
			node.buffer.validInsertDeleteBuffer[i] = false
		}
	}

	// 2. Remove invalid messages from buffer.
	tmpInsertOrDeleteBuffer := make([]*msgPb.InsertOrDeleteMsg, 0)
	for i, isValid := range node.buffer.validInsertDeleteBuffer {
		if isValid {
			tmpInsertOrDeleteBuffer = append(tmpInsertOrDeleteBuffer, node.buffer.InsertDeleteBuffer[i])
		}
	}
	node.buffer.InsertDeleteBuffer = tmpInsertOrDeleteBuffer

	// 3. Resize the valid bitmap and set all bits to true.
	node.buffer.validInsertDeleteBuffer = node.buffer.validInsertDeleteBuffer[:len(node.buffer.InsertDeleteBuffer)]
	for i := range node.buffer.validInsertDeleteBuffer {
		node.buffer.validInsertDeleteBuffer[i] = true
	}

	// 4. Extract messages before readTimeSync from current messageClient.
	//    Move massages after readTimeSync to QueryNodeDataBuffer.
	//    Set valid bitmap to true.
	//    If segmentId dose not exist in segments map, creating an new segment.
	for _, msg := range insertDeleteMessages {
		if msg.Timestamp < tMax {
			if !node.FoundSegmentBySegmentID(msg.SegmentId) {
				collection, _ := node.GetCollectionByCollectionName(msg.CollectionName)
				if collection != nil {
					partition := collection.GetPartitionByName(msg.PartitionTag)
					if partition != nil {
						newSegment := partition.NewSegment(msg.SegmentId)
						node.SegmentsMap[msg.SegmentId] = newSegment
					} else {
						log.Fatal("Cannot find partition:", msg.PartitionTag)
					}
				} else {
					log.Fatal("Cannot find collection:", msg.CollectionName)
				}
			}
			if msg.Op == msgPb.OpType_INSERT {
				if msg.RowsData == nil {
					continue
				}
				node.insertData.insertIDs[msg.SegmentId] = append(node.insertData.insertIDs[msg.SegmentId], msg.Uid)
				node.insertData.insertTimestamps[msg.SegmentId] = append(node.insertData.insertTimestamps[msg.SegmentId], msg.Timestamp)
				node.insertData.insertRecords[msg.SegmentId] = append(node.insertData.insertRecords[msg.SegmentId], msg.RowsData.Blob)
			} else if msg.Op == msgPb.OpType_DELETE {
				var r = DeleteRecord{
					entityID:  msg.Uid,
					timestamp: msg.Timestamp,
				}
				node.deletePreprocessData.deleteRecords = append(node.deletePreprocessData.deleteRecords, &r)
				node.deletePreprocessData.count++
			}
		} else {
			node.buffer.InsertDeleteBuffer = append(node.buffer.InsertDeleteBuffer, msg)
			node.buffer.validInsertDeleteBuffer = append(node.buffer.validInsertDeleteBuffer, true)
		}
	}

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) WriterDelete() msgPb.Status {
	// TODO: set timeout
	for {
		if node.deletePreprocessData.count == 0 {
			return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
		}
		node.messageClient.PrepareKey2SegmentMsg()
		var ids, timestamps, segmentIDs = node.GetKey2Segments()
		for i := 0; i < len(*ids); i++ {
			id := (*ids)[i]
			timestamp := (*timestamps)[i]
			segmentID := (*segmentIDs)[i]
			for _, r := range node.deletePreprocessData.deleteRecords {
				if r.timestamp == timestamp && r.entityID == id {
					r.segmentID = segmentID
					node.deletePreprocessData.count--
				}
			}
		}
	}
}

func (node *QueryNode) PreInsertAndDelete() msgPb.Status {
	// 1. Do PreInsert
	for segmentID := range node.insertData.insertRecords {
		var targetSegment, err = node.GetSegmentBySegmentID(segmentID)
		if err != nil {
			fmt.Println(err.Error())
			return msgPb.Status{ErrorCode: 1}
		}

		var numOfRecords = len(node.insertData.insertRecords[segmentID])
		var offset = targetSegment.SegmentPreInsert(numOfRecords)
		node.insertData.insertOffset[segmentID] = offset
	}

	// 2. Sort delete preprocess data by segment id
	for _, r := range node.deletePreprocessData.deleteRecords {
		node.deleteData.deleteIDs[r.segmentID] = append(node.deleteData.deleteIDs[r.segmentID], r.entityID)
		node.deleteData.deleteTimestamps[r.segmentID] = append(node.deleteData.deleteTimestamps[r.segmentID], r.timestamp)
	}

	// 3. Do PreDelete
	for segmentID := range node.deleteData.deleteIDs {
		if segmentID < 0 {
			continue
		}
		var targetSegment, err = node.GetSegmentBySegmentID(segmentID)
		if err != nil {
			fmt.Println(err.Error())
			return msgPb.Status{ErrorCode: 1}
		}

		var numOfRecords = len(node.deleteData.deleteIDs[segmentID])
		var offset = targetSegment.SegmentPreDelete(numOfRecords)
		node.deleteData.deleteOffset[segmentID] = offset
	}

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) DoInsertAndDelete() msgPb.Status {
	var wg sync.WaitGroup
	// Do insert
	for segmentID := range node.insertData.insertRecords {
		wg.Add(1)
		go node.DoInsert(segmentID, &wg)
	}

	// Do delete
	for segmentID, deleteIDs := range node.deleteData.deleteIDs {
		if segmentID < 0 {
			continue
		}
		wg.Add(1)
		var deleteTimestamps = node.deleteData.deleteTimestamps[segmentID]
		go node.DoDelete(segmentID, &deleteIDs, &deleteTimestamps, &wg)
		fmt.Println("Do delete done")
	}

	wg.Wait()
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) DoInsert(segmentID int64, wg *sync.WaitGroup) msgPb.Status {
	var targetSegment, err = node.GetSegmentBySegmentID(segmentID)
	if err != nil {
		fmt.Println(err.Error())
		return msgPb.Status{ErrorCode: 1}
	}

	ids := node.insertData.insertIDs[segmentID]
	timestamps := node.insertData.insertTimestamps[segmentID]
	records := node.insertData.insertRecords[segmentID]
	offsets := node.insertData.insertOffset[segmentID]

	err = targetSegment.SegmentInsert(offsets, &ids, &timestamps, &records)
	fmt.Println("Do insert done, len = ", len(node.insertData.insertIDs[segmentID]))

	node.QueryLog(len(ids))

	if err != nil {
		fmt.Println(err.Error())
		return msgPb.Status{ErrorCode: 1}
	}

	wg.Done()
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) DoDelete(segmentID int64, deleteIDs *[]int64, deleteTimestamps *[]uint64, wg *sync.WaitGroup) msgPb.Status {
	var segment, err = node.GetSegmentBySegmentID(segmentID)
	if err != nil {
		fmt.Println(err.Error())
		return msgPb.Status{ErrorCode: 1}
	}

	offset := node.deleteData.deleteOffset[segmentID]

	node.msgCounter.DeleteCounter += int64(len(*deleteIDs))
	err = segment.SegmentDelete(offset, deleteIDs, deleteTimestamps)
	if err != nil {
		fmt.Println(err.Error())
		return msgPb.Status{ErrorCode: 1}
	}

	wg.Done()
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}
