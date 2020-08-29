package reader

import "C"
import (
	"errors"
	"fmt"
	"suvlim/pulsar"
	"suvlim/pulsar/schema"
	"sync"
	"time"
)

type QueryNodeDataBuffer struct {
	InsertBuffer  []*schema.InsertMsg
	DeleteBuffer  []*schema.DeleteMsg
	SearchBuffer  []*schema.SearchMsg
	validInsertBuffer  []bool
	validDeleteBuffer  []bool
	validSearchBuffer  []bool
}

type QueryNodeTimeSync struct {
	deleteTimeSync uint64
	insertTimeSync uint64
	searchTimeSync uint64
}

type QueryNode struct {
	Collections               []*Collection
	messageClient 			  pulsar.MessageClient
	queryNodeTimeSync         *QueryNodeTimeSync
	buffer					  QueryNodeDataBuffer
}

func NewQueryNode(timeSync uint64) *QueryNode {
	mc := pulsar.MessageClient{}

	queryNodeTimeSync := &QueryNodeTimeSync {
		deleteTimeSync: timeSync,
		insertTimeSync: timeSync,
		searchTimeSync: timeSync,
	}

	return &QueryNode{
		Collections:           nil,
		messageClient: 		   mc,
		queryNodeTimeSync:     queryNodeTimeSync,
	}
}

func (node *QueryNode)doQueryNode(wg *sync.WaitGroup) {
	wg.Add(3)
	go node.Insert(node.messageClient.InsertMsg, wg)
	go node.Delete(node.messageClient.DeleteMsg, wg)
	go node.Search(node.messageClient.SearchMsg, wg)
	wg.Wait()
}

func (node *QueryNode) PrepareBatchMsg() {
	node.messageClient.PrepareBatchMsg(pulsar.JobType(0))
}

func (node *QueryNode) StartMessageClient() {
	topics := []string{"insert", "delete"}
	node.messageClient.InitClient("pulsar://localhost:6650", topics)

	go node.messageClient.ReceiveMessage()
}

func (node *QueryNode) AddNewCollection(collectionName string, schema CollectionSchema) error {
	var collection, err = NewCollection(collectionName, schema)
	node.Collections = append(node.Collections, collection)
	return err
}

func (node *QueryNode) GetSegmentByEntityId(entityId int64) *Segment {
	// TODO: get id2segment info from pulsar
	return nil
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

	for _, segment := range targetPartition.Segments {
		var segmentStatus = segment.GetStatus()
		if segmentStatus == 0 {
			return segment, nil
		}
	}

	return nil, errors.New("cannot found target segment")
}

func (node *QueryNode) GetTimeSync() uint64 {
	// TODO: Add time sync
	return 0
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func (node *QueryNode) InitQueryNodeCollection() {
	// TODO: remove hard code, add collection creation request
	var collection, _ = NewCollection("collection1", "fakeSchema")
	node.Collections = append(node.Collections, collection)
	var partition, _ = collection.NewPartition("partition1")
	collection.Partitions = append(collection.Partitions, partition)
	var segment, _ = partition.NewSegment()
	partition.Segments = append(partition.Segments, segment)
}

func (node *QueryNode) SegmentsManagement() {
	var timeSync = node.GetTimeSync()
	for _, collection := range node.Collections {
		for _, partition := range collection.Partitions {
			for _, segment := range partition.Segments {
				if timeSync >= segment.SegmentCloseTime {
					segment.Close()
					// TODO: add atomic segment id
					var newSegment, _ = partition.NewSegment()
					newSegment.SegmentCloseTime = timeSync + SegmentLifetime
					partition.Segments = append(partition.Segments, newSegment)
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
func (node *QueryNode) Insert(insertMessages []*schema.InsertMsg, wg *sync.WaitGroup) schema.Status {
	var timeSync = node.GetTimeSync()
	var collectionName = insertMessages[0].CollectionName
	var partitionTag = insertMessages[0].PartitionTag
	var clientId = insertMessages[0].ClientId

	// TODO: prevent Memory copy
	var entityIds []int64
	var timestamps []uint64
	var vectorRecords [][]*schema.FieldValue

	for i, msg := range node.buffer.InsertBuffer {
		if msg.Timestamp <= timeSync {
			entityIds = append(entityIds, msg.EntityId)
			timestamps = append(timestamps, msg.Timestamp)
			vectorRecords = append(vectorRecords, msg.Fields)
			node.buffer.validInsertBuffer[i] = false
		}
	}

	for i, isValid := range node.buffer.validInsertBuffer {
		if !isValid {
			copy(node.buffer.InsertBuffer[i:], node.buffer.InsertBuffer[i+1:]) // Shift a[i+1:] left one index.
			node.buffer.InsertBuffer[len(node.buffer.InsertBuffer)-1] = nil    // Erase last element (write zero value).
			node.buffer.InsertBuffer = node.buffer.InsertBuffer[:len(node.buffer.InsertBuffer)-1]     // Truncate slice.
		}
	}

	for _, msg := range insertMessages {
		if msg.Timestamp <= timeSync {
			entityIds = append(entityIds, msg.EntityId)
			timestamps = append(timestamps, msg.Timestamp)
			vectorRecords = append(vectorRecords, msg.Fields)
		} else {
			node.buffer.InsertBuffer = append(node.buffer.InsertBuffer, msg)
			node.buffer.validInsertBuffer = append(node.buffer.validInsertBuffer, true)
		}
	}

	var targetSegment, err = node.GetTargetSegment(&collectionName, &partitionTag)
	if err != nil {
		// TODO: throw runtime error
		fmt.Println(err.Error())
		return schema.Status{}
	}

	var result = SegmentInsert(targetSegment, collectionName, partitionTag, &entityIds, &timestamps, vectorRecords)

	wg.Done()
	return publishResult(&result, clientId)
}

func (node *QueryNode) Delete(deleteMessages []*schema.DeleteMsg, wg *sync.WaitGroup) schema.Status {
	var timeSync = node.GetTimeSync()
	var collectionName = deleteMessages[0].CollectionName
	var clientId = deleteMessages[0].ClientId

	// TODO: prevent Memory copy
	var entityIds []int64
	var timestamps []uint64

	for i, msg := range node.buffer.DeleteBuffer {
		if msg.Timestamp <= timeSync {
			entityIds = append(entityIds, msg.EntityId)
			timestamps = append(timestamps, msg.Timestamp)
			node.buffer.validDeleteBuffer[i] = false
		}
	}

	for i, isValid := range node.buffer.validDeleteBuffer {
		if !isValid {
			copy(node.buffer.DeleteBuffer[i:], node.buffer.DeleteBuffer[i+1:]) // Shift a[i+1:] left one index.
			node.buffer.DeleteBuffer[len(node.buffer.DeleteBuffer)-1] = nil    // Erase last element (write zero value).
			node.buffer.DeleteBuffer = node.buffer.DeleteBuffer[:len(node.buffer.DeleteBuffer)-1]     // Truncate slice.
		}
	}

	for _, msg := range deleteMessages {
		if msg.Timestamp <= timeSync {
			entityIds = append(entityIds, msg.EntityId)
			timestamps = append(timestamps, msg.Timestamp)
		} else {
			node.buffer.DeleteBuffer = append(node.buffer.DeleteBuffer, msg)
			node.buffer.validDeleteBuffer = append(node.buffer.validDeleteBuffer, true)
		}
	}

	if entityIds == nil {
		// TODO: throw runtime error
		fmt.Println("no entities found")
		return schema.Status{}
	}
	// TODO: does all entities from a common batch are in the same segment?
	var targetSegment = node.GetSegmentByEntityId(entityIds[0])

	var result = SegmentDelete(targetSegment, collectionName, &entityIds, &timestamps)

	wg.Done()
	return publishResult(&result, clientId)
}

func (node *QueryNode) Search(searchMessages []*schema.SearchMsg, wg *sync.WaitGroup) schema.Status {
	var timeSync = node.GetTimeSync()
	var collectionName = searchMessages[0].CollectionName
	var partitionTag = searchMessages[0].PartitionTag
	var clientId = searchMessages[0].ClientId
	var queryString = searchMessages[0].VectorParam.Json

	// TODO: prevent Memory copy
	var records []schema.VectorRecord
	var timestamps []uint64

	for i, msg := range node.buffer.SearchBuffer {
		if msg.Timestamp <= timeSync {
			records = append(records, *msg.VectorParam.RowRecord)
			timestamps = append(timestamps, msg.Timestamp)
			node.buffer.validSearchBuffer[i] = false
		}
	}

	for i, isValid := range node.buffer.validSearchBuffer {
		if !isValid {
			copy(node.buffer.SearchBuffer[i:], node.buffer.SearchBuffer[i+1:]) // Shift a[i+1:] left one index.
			node.buffer.SearchBuffer[len(node.buffer.SearchBuffer)-1] = nil    // Erase last element (write zero value).
			node.buffer.SearchBuffer = node.buffer.SearchBuffer[:len(node.buffer.SearchBuffer)-1]     // Truncate slice.
		}
	}

	for _, msg := range searchMessages {
		if msg.Timestamp <= timeSync {
			records = append(records, *msg.VectorParam.RowRecord)
			timestamps = append(timestamps, msg.Timestamp)
		} else {
			node.buffer.SearchBuffer = append(node.buffer.SearchBuffer, msg)
			node.buffer.validSearchBuffer = append(node.buffer.validSearchBuffer, true)
		}
	}

	var targetSegment, err = node.GetTargetSegment(&collectionName, &partitionTag)
	if err != nil {
		// TODO: throw runtime error
		fmt.Println(err.Error())
		return schema.Status{}
	}

	var result = SegmentSearch(targetSegment, collectionName, queryString, &timestamps, &records)

	wg.Done()
	return publishResult(&result, clientId)
}
