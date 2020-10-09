package reader

import (
	"errors"
	"strconv"
)

// Function `GetSegmentByEntityId` should return entityIDs, timestamps and segmentIDs
func (node *QueryNode) GetKey2Segments() (*[]int64, *[]uint64, *[]int64) {
	var entityIDs = make([]int64, 0)
	var timestamps = make([]uint64, 0)
	var segmentIDs = make([]int64, 0)

	var key2SegMsg = node.messageClient.Key2SegMsg
	for _, msg := range key2SegMsg {
		if msg.SegmentId == nil {
			segmentIDs = append(segmentIDs, -1)
			entityIDs = append(entityIDs, msg.Uid)
			timestamps = append(timestamps, msg.Timestamp)
		} else {
			for _, segmentID := range msg.SegmentId {
				segmentIDs = append(segmentIDs, segmentID)
				entityIDs = append(entityIDs, msg.Uid)
				timestamps = append(timestamps, msg.Timestamp)
			}
		}
	}

	return &entityIDs, &timestamps, &segmentIDs
}

func (node *QueryNode) GetCollectionByID(collectionID uint64) *Collection {
	for _, collection := range node.Collections {
		if collection.CollectionID == collectionID {
			return collection
		}
	}

	return nil
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

func (c *Collection) GetPartitionByName(partitionName string) (partition *Partition) {
	for _, partition := range c.Partitions {
		if partition.PartitionName == partitionName {
			return partition
		}
	}
	return nil
	// TODO: remove from c.Partitions
}
