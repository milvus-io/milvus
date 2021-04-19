package queryservice

import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type metaReplica interface {
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
	addDmChannels(dbID UniqueID, collectionID UniqueID, channels2NodeID map[string]UniqueID) error
	getAssignedNodeIDByChannelName(dbID UniqueID, collectionID UniqueID, channel string) (UniqueID, error)
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
	id              UniqueID
	partitions      map[UniqueID]*partition
	dmChannels2Node map[string]UniqueID
	schema          *schemapb.CollectionSchema
}

type metaReplicaImpl struct {
	dbID           []UniqueID
	db2collections map[UniqueID][]*collection
}

func newMetaReplica() metaReplica {
	db2collections := make(map[UniqueID][]*collection)
	db2collections[0] = make([]*collection, 0)
	dbIDs := make([]UniqueID, 0)
	dbIDs = append(dbIDs, UniqueID(0))
	return &metaReplicaImpl{
		dbID:           dbIDs,
		db2collections: db2collections,
	}
}

func (mp *metaReplicaImpl) addCollection(dbID UniqueID, collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	//TODO:: assert dbID = 0 exist
	if _, ok := mp.db2collections[dbID]; ok {
		partitions := make(map[UniqueID]*partition)
		channels := make(map[string]UniqueID)
		newCollection := &collection{
			id:              collectionID,
			partitions:      partitions,
			schema:          schema,
			dmChannels2Node: channels,
		}
		mp.db2collections[dbID] = append(mp.db2collections[dbID], newCollection)
		return nil
	}

	return errors.New("addCollection: can't find dbID when add collection")
}

func (mp *metaReplicaImpl) addPartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) error {
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

func (mp *metaReplicaImpl) getCollections(dbID UniqueID) ([]*collection, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		return collections, nil
	}

	return nil, errors.New("getCollections: can't find collectionID")
}

func (mp *metaReplicaImpl) getPartitions(dbID UniqueID, collectionID UniqueID) ([]*partition, error) {
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

func (mp *metaReplicaImpl) getSegments(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) ([]*segment, error) {
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

func (mp *metaReplicaImpl) getCollectionByID(dbID UniqueID, collectionID UniqueID) (*collection, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				return collection, nil
			}
		}
	}

	return nil, errors.New("getCollectionByID: can't find collectionID")
}

func (mp *metaReplicaImpl) getPartitionByID(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) (*partition, error) {
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

func (mp *metaReplicaImpl) updatePartitionState(dbID UniqueID,
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

func (mp *metaReplicaImpl) getPartitionStates(dbID UniqueID,
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

func (mp *metaReplicaImpl) releaseCollection(dbID UniqueID, collectionID UniqueID) error {
	if collections, ok := mp.db2collections[dbID]; ok {
		for i, collection := range collections {
			if collectionID == collection.id {
				collections = append(collections[:i], collections[i+1:]...)
				return nil
			}
		}
	}
	return errors.New("releaseCollection: can't find dbID or collectionID")
}

func (mp *metaReplicaImpl) releasePartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) error {
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
	return errors.New("releasePartition: can't find dbID or collectionID or partitionID")
}

func (mp *metaReplicaImpl) addDmChannels(dbID UniqueID, collectionID UniqueID, channels2NodeID map[string]UniqueID) error {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				for channel, id := range channels2NodeID {
					collection.dmChannels2Node[channel] = id
				}
				return nil
			}
		}
	}
	return errors.New("addDmChannels: can't find dbID or collectionID")
}

func (mp *metaReplicaImpl) getAssignedNodeIDByChannelName(dbID UniqueID, collectionID UniqueID, channel string) (UniqueID, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				if id, ok := collection.dmChannels2Node[channel]; ok {
					return id, nil
				}
			}
		}
	}

	return 0, errors.New("getAssignedNodeIDByChannelName: can't find dbID or collectionID")
}
