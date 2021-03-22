package queryservice

import (
	"errors"

	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type Replica interface {
	getCollections(dbID UniqueID) ([]*collection, error)
	getPartitions(dbID UniqueID, collectionID UniqueID) ([]*partition, error)
	getSegments(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) ([]*segment, error)
	getCollectionByID(dbID UniqueID, collectionID UniqueID) (*collection, error)
	getPartitionByID(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) (*partition, error)
	addCollection(dbID UniqueID, collectionID UniqueID, schema *schemapb.CollectionSchema) error
	addPartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) error
	updatePartitionState(dbID UniqueID, collectionID UniqueID, partitionID UniqueID, state querypb.PartitionState) error
	getPartitionStates(dbID UniqueID, collectionID UniqueID, partitionIDs []UniqueID) ([]*querypb.PartitionStates, error)
	releaseCollection(dbID UniqueID, collectionID UniqueID) error
	releasePartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) error
	addDmChannel(dbID UniqueID, collectionID UniqueID, channel string, watchedStartPos *internalPb.MsgPosition) error
	addExcludeSegmentIDs(dbID UniqueID, collectionID UniqueID, excludeSegments []UniqueID) error
	//getAssignedNodeIDByChannelName(dbID UniqueID, collectionID UniqueID, channel string) (int64, error)
}

type segment struct {
	id UniqueID
}

type partition struct {
	id       UniqueID
	segments map[UniqueID]*segment
	state    querypb.PartitionState
}

type collection struct {
	id                UniqueID
	partitions        map[UniqueID]*partition
	dmChannels        []string
	dmChannels2Pos    map[string]*internalPb.MsgPosition
	excludeSegmentIds []UniqueID
	schema            *schemapb.CollectionSchema
}

type metaReplica struct {
	dbID           []UniqueID
	db2collections map[UniqueID][]*collection
}

func newMetaReplica() Replica {
	db2collections := make(map[UniqueID][]*collection)
	db2collections[0] = make([]*collection, 0)
	dbIDs := make([]UniqueID, 0)
	dbIDs = append(dbIDs, UniqueID(0))
	return &metaReplica{
		dbID:           dbIDs,
		db2collections: db2collections,
	}
}

func (mp *metaReplica) addCollection(dbID UniqueID, collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	//TODO:: assert dbID = 0 exist
	if _, ok := mp.db2collections[dbID]; ok {
		partitions := make(map[UniqueID]*partition)
		channels := make([]string, 0)
		startPos := make(map[string]*internalPb.MsgPosition)
		excludeSegmentIDs := make([]UniqueID, 0)
		newCollection := &collection{
			id:                collectionID,
			partitions:        partitions,
			schema:            schema,
			dmChannels:        channels,
			dmChannels2Pos:    startPos,
			excludeSegmentIds: excludeSegmentIDs,
		}
		mp.db2collections[dbID] = append(mp.db2collections[dbID], newCollection)
		return nil
	}

	return errors.New("addCollection: can't find dbID when add collection")
}

func (mp *metaReplica) addPartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) error {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collection.id == collectionID {
				partitions := collection.partitions
				segments := make(map[UniqueID]*segment)
				partition := &partition{
					id:       partitionID,
					state:    querypb.PartitionState_NotPresent,
					segments: segments,
				}
				partitions[partitionID] = partition
				return nil
			}
		}
	}
	return errors.New("addPartition: can't find collection when add partition")
}

func (mp *metaReplica) getCollections(dbID UniqueID) ([]*collection, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		return collections, nil
	}

	return nil, errors.New("getCollections: can't find collectionID")
}

func (mp *metaReplica) getPartitions(dbID UniqueID, collectionID UniqueID) ([]*partition, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				partitions := make([]*partition, 0)
				for _, partition := range collection.partitions {
					partitions = append(partitions, partition)
				}
				return partitions, nil
			}
		}
	}

	return nil, errors.New("getPartitions: can't find partitionIDs")
}

func (mp *metaReplica) getSegments(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) ([]*segment, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				if partition, ok := collection.partitions[partitionID]; ok {
					segments := make([]*segment, 0)
					for _, segment := range partition.segments {
						segments = append(segments, segment)
					}
					return segments, nil
				}
			}
		}
	}
	return nil, errors.New("getSegments: can't find segmentID")
}

func (mp *metaReplica) getCollectionByID(dbID UniqueID, collectionID UniqueID) (*collection, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				return collection, nil
			}
		}
	}

	return nil, errors.New("getCollectionByID: can't find collectionID")
}

func (mp *metaReplica) getPartitionByID(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) (*partition, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				partitions := collection.partitions
				if partition, ok := partitions[partitionID]; ok {
					return partition, nil
				}
			}
		}
	}

	return nil, errors.New("getPartitionByID: can't find partitionID")
}

func (mp *metaReplica) updatePartitionState(dbID UniqueID,
	collectionID UniqueID,
	partitionID UniqueID,
	state querypb.PartitionState) error {
	for _, collection := range mp.db2collections[dbID] {
		if collection.id == collectionID {
			if partition, ok := collection.partitions[partitionID]; ok {
				partition.state = state
				return nil
			}
		}
	}
	return errors.New("updatePartitionState: update partition state fail")
}

func (mp *metaReplica) getPartitionStates(dbID UniqueID,
	collectionID UniqueID,
	partitionIDs []UniqueID) ([]*querypb.PartitionStates, error) {
	partitionStates := make([]*querypb.PartitionStates, 0)
	for _, collection := range mp.db2collections[dbID] {
		if collection.id == collectionID {
			for _, partitionID := range partitionIDs {
				if partition, ok := collection.partitions[partitionID]; ok {
					partitionStates = append(partitionStates, &querypb.PartitionStates{
						PartitionID: partitionID,
						State:       partition.state,
					})
				} else {
					partitionStates = append(partitionStates, &querypb.PartitionStates{
						PartitionID: partitionID,
						State:       querypb.PartitionState_NotPresent,
					})
				}
			}
		}
	}
	return partitionStates, nil
}

func (mp *metaReplica) releaseCollection(dbID UniqueID, collectionID UniqueID) error {
	if collections, ok := mp.db2collections[dbID]; ok {
		for i, coll := range collections {
			if collectionID == coll.id {
				newSize := len(collections) - 1
				newColls := make([]*collection, 0, newSize)
				collections[i] = collections[newSize]
				newColls = append(newColls, collections[:newSize]...)
				mp.db2collections[dbID] = newColls
				return nil
			}
		}
	}

	return nil
}

func (mp *metaReplica) releasePartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) error {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				if _, ok := collection.partitions[partitionID]; ok {
					delete(collection.partitions, partitionID)
					return nil
				}
			}
		}
	}

	return nil
}

func (mp *metaReplica) addDmChannel(dbID UniqueID, collectionID UniqueID, channel string, watchedStartPos *internalPb.MsgPosition) error {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				collection.dmChannels = append(collection.dmChannels, channel)
				collection.dmChannels2Pos[channel] = watchedStartPos
				return nil
			}
		}
	}
	return errors.New("addDmChannels: can't find dbID or collectionID")
}

func (mp *metaReplica) addExcludeSegmentIDs(dbID UniqueID, collectionID UniqueID, excludeSegments []UniqueID) error {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				collection.excludeSegmentIds = append(collection.excludeSegmentIds, excludeSegments...)
				return nil
			}
		}
	}
	return errors.New("addExcludeSegmentIDs: can't find dbID or collectionID")
}
