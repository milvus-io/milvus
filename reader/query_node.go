package reader

/*

#cgo CFLAGS: -I../core/include

#cgo LDFLAGS: -L../core/lib -lmilvus_dog_segment -Wl,-rpath=../core/lib

#include "collection_c.h"
#include "partition_c.h"
#include "segment_c.h"

*/
import "C"

import (
	"errors"
	"fmt"
	msgPb "github.com/czs007/suvlim/pkg/message"
	"github.com/czs007/suvlim/reader/message_client"
	"sort"
	"strconv"
	"sync"
	"time"
)

type DeleteRecord struct {
	entityID  		int64
	timestamp 		uint64
	segmentID 		int64
}

type DeleteRecords struct {
	deleteRecords  		*[]DeleteRecord
	count            	chan int
}

type QueryNodeDataBuffer struct {
	InsertDeleteBuffer      []*msgPb.InsertOrDeleteMsg
	SearchBuffer            []*msgPb.SearchMsg
	validInsertDeleteBuffer []bool
	validSearchBuffer       []bool
}

type QueryNode struct {
	QueryNodeId       uint64
	Collections       []*Collection
	SegmentsMap       map[int64]*Segment
	messageClient     message_client.MessageClient
	queryNodeTimeSync *QueryNodeTime
	deleteRecordsMap  map[TimeRange]DeleteRecords
	buffer            QueryNodeDataBuffer
}

func NewQueryNode(queryNodeId uint64, timeSync uint64) *QueryNode {
	mc := message_client.MessageClient{}

	queryNodeTimeSync := &QueryNodeTime{
		ReadTimeSyncMin: timeSync,
		ReadTimeSyncMax: timeSync,
		WriteTimeSync:   timeSync,
		SearchTimeSync:  timeSync,
		TSOTimeSync:     timeSync,
	}

	segmentsMap := make(map[int64]*Segment)

	return &QueryNode{
		QueryNodeId:       queryNodeId,
		Collections:       nil,
		SegmentsMap:       segmentsMap,
		messageClient:     mc,
		queryNodeTimeSync: queryNodeTimeSync,
		deleteRecordsMap:  make(map[TimeRange]DeleteRecords),
	}
}

func (node *QueryNode) NewCollection(collectionName string, schemaConfig string) *Collection {
	cName := C.CString(collectionName)
	cSchema := C.CString(schemaConfig)
	collection := C.NewCollection(cName, cSchema)

	var newCollection = &Collection{CollectionPtr: collection, CollectionName: collectionName}
	node.Collections = append(node.Collections, newCollection)

	return newCollection
}

func (node *QueryNode) DeleteCollection(collection *Collection) {
	cPtr := collection.CollectionPtr
	C.DeleteCollection(cPtr)

	// TODO: remove from node.Collections
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func (node *QueryNode) doQueryNode(wg *sync.WaitGroup) {
	wg.Add(3)
	// Do insert and delete messages sort, do insert
	go node.InsertAndDelete(node.messageClient.InsertOrDeleteMsg, wg)
	// Do delete messages sort
	go node.searchDeleteInMap()
	// Do delete
	go node.Delete()
	// Do search
	go node.Search(node.messageClient.SearchMsg, wg)
	wg.Wait()
}

func (node *QueryNode) PrepareBatchMsg() {
	node.messageClient.PrepareBatchMsg()
}

func (node *QueryNode) StartMessageClient() {
	// TODO: add consumerMsgSchema
	node.messageClient.InitClient("pulsar://localhost:6650")

	go node.messageClient.ReceiveMessage()
}

// Function `GetSegmentByEntityId` should return entityIDs, timestamps and segmentIDs
func (node *QueryNode) GetKey2Segments() ([]int64, []uint64, []int64) {
	// TODO: get id2segment info from pulsar
	return nil, nil, nil
}

func (node *QueryNode) GetTargetSegment(collectionName *string, partitionTag *string) (*Segment, error) {
	var targetPartition *Partition

	for _, collection := range node.Collections {
		if *collectionName == collection.CollectionName {
			for _, partition := range collection.Partitions {
				if *partitionTag == partition.PartitionName {
					targetPartition = partition
					break
				}
			}
		}
	}

	if targetPartition == nil {
		return nil, errors.New("cannot found target partition")
	}

	for _, segment := range targetPartition.OpenedSegments {
		// TODO: add other conditions
		return segment, nil
	}

	return nil, errors.New("cannot found target segment")
}

func (node *QueryNode) GetCollectionByCollectionName(collectionName string) (*Collection, error) {
	for _, collection := range node.Collections {
		if collection.CollectionName == collectionName {
			return collection, nil
		}
	}

	return nil, errors.New("Cannot found collection: " + collectionName)
}

func (node *QueryNode) GetSegmentBySegmentID(segmentID int64) (*Segment, error) {
	targetSegment := node.SegmentsMap[segmentID]

	if targetSegment == nil {
		return nil, errors.New("cannot found segment with id = " + strconv.FormatInt(segmentID, 10))
	}

	return targetSegment, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func (node *QueryNode) InitQueryNodeCollection() {
	// TODO: remove hard code, add collection creation request
	// TODO: error handle
	var newCollection = node.NewCollection("collection1", "fakeSchema")
	var newPartition = newCollection.NewPartition("partition1")
	// TODO: add segment id
	var _ = newPartition.NewSegment(0)
}

func (node *QueryNode) SegmentsManagement() {
	node.queryNodeTimeSync.UpdateTSOTimeSync()
	var timeNow = node.queryNodeTimeSync.TSOTimeSync
	for _, collection := range node.Collections {
		for _, partition := range collection.Partitions {
			for _, oldSegment := range partition.OpenedSegments {
				// TODO: check segment status
				if timeNow >= oldSegment.SegmentCloseTime {
					// start new segment and add it into partition.OpenedSegments
					// TODO: get segmentID from master
					var segmentID int64 = 0
					var newSegment = partition.NewSegment(segmentID)
					newSegment.SegmentCloseTime = timeNow + SegmentLifetime
					partition.OpenedSegments = append(partition.OpenedSegments, newSegment)
					node.SegmentsMap[segmentID] = newSegment

					// close old segment and move it into partition.ClosedSegments
					// TODO: check status
					var _ = oldSegment.Close()
					partition.ClosedSegments = append(partition.ClosedSegments, oldSegment)
				}
			}
		}
	}
}

func (node *QueryNode) SegmentService() {
	for {
		time.Sleep(200 * time.Millisecond)
		node.SegmentsManagement()
		fmt.Println("do segments management in 200ms")
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// TODO: receive delete messages individually
func (node *QueryNode) InsertAndDelete(insertDeleteMessages []*msgPb.InsertOrDeleteMsg, wg *sync.WaitGroup) msgPb.Status {
	node.queryNodeTimeSync.UpdateReadTimeSync()

	var tMin = node.queryNodeTimeSync.ReadTimeSyncMin
	var tMax = node.queryNodeTimeSync.ReadTimeSyncMax
	var readTimeSyncRange = TimeRange{timestampMin: tMin, timestampMax: tMax}

	var clientId = insertDeleteMessages[0].ClientId

	var insertIDs = make(map[int64][]int64)
	var insertTimestamps = make(map[int64][]uint64)
	var insertRecords = make(map[int64][][]byte)

	// 1. Extract messages before readTimeSync from QueryNodeDataBuffer.
	//    Set valid bitmap to false.
	for i, msg := range node.buffer.InsertDeleteBuffer {
		if msg.Timestamp <= tMax {
			if msg.Op == msgPb.OpType_INSERT {
				insertIDs[msg.SegmentId] = append(insertIDs[msg.SegmentId], msg.Uid)
				insertTimestamps[msg.SegmentId] = append(insertTimestamps[msg.SegmentId], msg.Timestamp)
				insertRecords[msg.SegmentId] = append(insertRecords[msg.SegmentId], msg.RowsData.Blob)
			} else if msg.Op == msgPb.OpType_DELETE {
				var r = DeleteRecord {
					entityID: msg.Uid,
					timestamp: msg.Timestamp,
				}
				*node.deleteRecordsMap[readTimeSyncRange].deleteRecords = append(*node.deleteRecordsMap[readTimeSyncRange].deleteRecords, r)
				node.deleteRecordsMap[readTimeSyncRange].count <- <- node.deleteRecordsMap[readTimeSyncRange].count + 1
			}
			node.buffer.validInsertDeleteBuffer[i] = false
		}
	}

	// 2. Remove invalid messages from buffer.
	for i, isValid := range node.buffer.validInsertDeleteBuffer {
		if !isValid {
			copy(node.buffer.InsertDeleteBuffer[i:], node.buffer.InsertDeleteBuffer[i+1:])                          // Shift a[i+1:] left one index.
			node.buffer.InsertDeleteBuffer[len(node.buffer.InsertDeleteBuffer)-1] = nil                             // Erase last element (write zero value).
			node.buffer.InsertDeleteBuffer = node.buffer.InsertDeleteBuffer[:len(node.buffer.InsertDeleteBuffer)-1] // Truncate slice.
		}
	}

	// 3. Extract messages before readTimeSync from current messageClient.
	//    Move massages after readTimeSync to QueryNodeDataBuffer.
	//    Set valid bitmap to true.
	for _, msg := range insertDeleteMessages {
		if msg.Timestamp <= tMax {
			if msg.Op == msgPb.OpType_INSERT {
				insertIDs[msg.SegmentId] = append(insertIDs[msg.SegmentId], msg.Uid)
				insertTimestamps[msg.SegmentId] = append(insertTimestamps[msg.SegmentId], msg.Timestamp)
				insertRecords[msg.SegmentId] = append(insertRecords[msg.SegmentId], msg.RowsData.Blob)
			} else if msg.Op == msgPb.OpType_DELETE {
				var r = DeleteRecord {
					entityID: msg.Uid,
					timestamp: msg.Timestamp,
				}
				*node.deleteRecordsMap[readTimeSyncRange].deleteRecords = append(*node.deleteRecordsMap[readTimeSyncRange].deleteRecords, r)
				node.deleteRecordsMap[readTimeSyncRange].count <- <- node.deleteRecordsMap[readTimeSyncRange].count + 1
			}
		} else {
			node.buffer.InsertDeleteBuffer = append(node.buffer.InsertDeleteBuffer, msg)
			node.buffer.validInsertDeleteBuffer = append(node.buffer.validInsertDeleteBuffer, true)
		}
	}

	// 4. Do insert
	// TODO: multi-thread insert
	for segmentID, records := range insertRecords {
		var targetSegment, err = node.GetSegmentBySegmentID(segmentID)
		if err != nil {
			fmt.Println(err.Error())
			return msgPb.Status{ErrorCode: 1}
		}
		ids := insertIDs[segmentID]
		timestamps := insertTimestamps[segmentID]
		err = targetSegment.SegmentInsert(&ids, &timestamps, &records, tMin, tMax)
		if err != nil {
			fmt.Println(err.Error())
			return msgPb.Status{ErrorCode: 1}
		}
	}

	wg.Done()
	return publishResult(nil, clientId)
}

func (node *QueryNode) searchDeleteInMap() {
	var ids, timestamps, segmentIDs = node.GetKey2Segments()

	for i := 0; i <= len(ids); i++ {
		id := ids[i]
		timestamp := timestamps[i]
		segmentID := segmentIDs[i]
		for timeRange, records := range node.deleteRecordsMap {
			if timestamp < timeRange.timestampMax && timestamp > timeRange.timestampMin {
				for _, r := range *records.deleteRecords {
					if r.timestamp == timestamp && r.entityID == id {
						r.segmentID = segmentID
						records.count <- <- records.count - 1
					}
				}
			}
		}
	}
}

func (node *QueryNode) Delete() msgPb.Status {
	type DeleteData struct {
		ids 			*[]int64
		timestamp 		*[]uint64
	}
	for timeRange, records := range node.deleteRecordsMap {
		// TODO: multi-thread delete
		if <- records.count == 0 {
			// 1. Sort delete records by segment id
			segment2records := make(map[int64]DeleteData)
			for _, r := range *records.deleteRecords {
				*segment2records[r.segmentID].ids = append(*segment2records[r.segmentID].ids, r.entityID)
				*segment2records[r.segmentID].timestamp = append(*segment2records[r.segmentID].timestamp, r.timestamp)
			}
			// 2. Do batched delete
			for segmentID, deleteData := range segment2records {
				var segment, err = node.GetSegmentBySegmentID(segmentID)
				if err != nil {
					fmt.Println(err.Error())
					return msgPb.Status{ErrorCode: 1}
				}
				err = segment.SegmentDelete(deleteData.ids, deleteData.timestamp, timeRange.timestampMin, timeRange.timestampMax)
				if err != nil {
					fmt.Println(err.Error())
					return msgPb.Status{ErrorCode: 1}
				}
			}
		}
	}

	return msgPb.Status{ErrorCode: 0}
}

func (node *QueryNode) Search(searchMessages []*msgPb.SearchMsg, wg *sync.WaitGroup) msgPb.Status {
	var clientId = searchMessages[0].ClientId

	type SearchResultTmp struct {
		ResultId 			int64
		ResultDistance 		float32
	}

	// Traverse all messages in the current messageClient.
	// TODO: Do not receive batched search requests
	for _, msg := range searchMessages {
		var collectionName = searchMessages[0].CollectionName
		var targetCollection, err = node.GetCollectionByCollectionName(collectionName)
		if err != nil {
			fmt.Println(err.Error())
			return msgPb.Status{ErrorCode: 1}
		}

		var resultsTmp []SearchResultTmp
		// TODO: get top-k's k from queryString
		const TopK = 1

		// 1. Do search in all segments
		var timestamp = msg.Timestamp
		var vector = msg.Records
		for _, partition := range targetCollection.Partitions {
			for _, openSegment := range partition.OpenedSegments {
				var res, err = openSegment.SegmentSearch("", timestamp, vector)
				if err != nil {
					fmt.Println(err.Error())
					return msgPb.Status{ErrorCode: 1}
				}
				for i := 0; i <= len(res.ResultIds); i++ {
					resultsTmp = append(resultsTmp, SearchResultTmp{ResultId: res.ResultIds[i], ResultDistance: res.ResultDistances[i]})
				}
			}
			for _, closedSegment := range partition.ClosedSegments {
				var res, err = closedSegment.SegmentSearch("", timestamp, vector)
				if err != nil {
					fmt.Println(err.Error())
					return msgPb.Status{ErrorCode: 1}
				}
				for i := 0; i <= len(res.ResultIds); i++ {
					resultsTmp = append(resultsTmp, SearchResultTmp{ResultId: res.ResultIds[i], ResultDistance: res.ResultDistances[i]})
				}
			}
		}

		// 2. Reduce results
		sort.Slice(resultsTmp, func(i, j int) bool {
			return resultsTmp[i].ResultDistance < resultsTmp[j].ResultDistance
		})
		resultsTmp = resultsTmp[:TopK]
		var results SearchResult
		for _, res := range resultsTmp {
			results.ResultIds = append(results.ResultIds, res.ResultId)
			results.ResultDistances = append(results.ResultDistances, res.ResultDistance)
		}

		// 3. publish result to pulsar
		publishSearchResult(&results, clientId)
	}

	wg.Done()
	return msgPb.Status{ErrorCode: 0}
}
