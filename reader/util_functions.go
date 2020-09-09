package reader

import (
	"errors"
	"strconv"
)

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
