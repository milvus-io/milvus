package queryservice

import (
	"log"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type metaReplica interface {
	getCollectionIDs(dbID UniqueID) ([]UniqueID, error)
	getPartitionIDs(dbID UniqueID, collectionID UniqueID) ([]UniqueID, error)
	getCollection(dbID UniqueID, collectionID UniqueID) *collection
	getSegmentIDs(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) ([]UniqueID, error)
	loadCollection(dbID UniqueID, collectionID UniqueID)
	loadPartitions(dbID UniqueID, collectionID UniqueID, partitionIDs []UniqueID)
	updatePartitionState(dbID UniqueID, collectionID UniqueID, partitionID UniqueID, state querypb.PartitionState)
	getPartitionStates(dbID UniqueID, collectionID UniqueID, partitionIDs []UniqueID) ([]*querypb.PartitionStates, error)
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
	id           UniqueID
	partitions   map[UniqueID]*partition
	node2channel map[int][]string
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

func (mp *metaReplicaImpl) addCollection(dbID UniqueID, collectionID UniqueID) {
	partitions := make(map[UniqueID]*partition)
	node2channel := make(map[int][]string)
	newCollection := &collection{
		id:           collectionID,
		partitions:   partitions,
		node2channel: node2channel,
	}
	mp.db2collections[dbID] = append(mp.db2collections[dbID], newCollection)
}

func (mp *metaReplicaImpl) addPartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) {
	collections := mp.db2collections[dbID]
	for _, collection := range collections {
		if collection.id == collectionID {
			partitions := collection.partitions
			segments := make(map[UniqueID]*segment)
			partitions[partitionID] = &partition{
				id:       partitionID,
				state:    querypb.PartitionState_NotPresent,
				segments: segments,
			}
			return
		}
	}
	log.Fatal("can't find collection when add partition")
}

func (mp *metaReplicaImpl) getCollection(dbID UniqueID, collectionID UniqueID) *collection {
	for _, id := range mp.dbID {
		if id == dbID {
			collections := mp.db2collections[id]
			for _, collection := range collections {
				if collection.id == collectionID {
					return collection
				}
			}
			return nil
		}
	}
	return nil
}

func (mp *metaReplicaImpl) getCollectionIDs(dbID UniqueID) ([]UniqueID, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		collectionIDs := make([]UniqueID, 0)
		for _, collection := range collections {
			collectionIDs = append(collectionIDs, collection.id)
		}
		return collectionIDs, nil
	}

	return nil, errors.New("can't find collection in queryService")
}

func (mp *metaReplicaImpl) getPartitionIDs(dbID UniqueID, collectionID UniqueID) ([]UniqueID, error) {
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				partitions := collection.partitions
				partitionIDs := make([]UniqueID, 0)
				for _, partition := range partitions {
					partitionIDs = append(partitionIDs, partition.id)
				}
				return partitionIDs, nil
			}
		}
	}

	return nil, errors.New("can't find partitions in queryService")
}

func (mp *metaReplicaImpl) getSegmentIDs(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) ([]UniqueID, error) {
	segmentIDs := make([]UniqueID, 0)
	if collections, ok := mp.db2collections[dbID]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				if partition, ok := collection.partitions[partitionID]; ok {
					for _, segment := range partition.segments {
						segmentIDs = append(segmentIDs, segment.id)
					}
				}
			}
		}
	}
	return segmentIDs, nil
}

func (mp *metaReplicaImpl) loadCollection(dbID UniqueID, collectionID UniqueID) {
	collectionIDs, err := mp.getCollectionIDs(dbID)
	if err != nil {
		mp.addCollection(dbID, collectionID)
		return
	}
	for _, id := range collectionIDs {
		if collectionID == id {
			return
		}
	}
	mp.addCollection(dbID, collectionID)
}

func (mp *metaReplicaImpl) loadPartitions(dbID UniqueID, collectionID UniqueID, partitionIDs []UniqueID) {
	var collection *collection = nil
	for _, col := range mp.db2collections[dbID] {
		if col.id == collectionID {
			collection = col
		}
	}
	if collection == nil {
		mp.addCollection(dbID, collectionID)
		for _, col := range mp.db2collections[dbID] {
			if col.id == collectionID {
				collection = col
			}
		}
	}
	for _, partitionID := range partitionIDs {
		match := false
		for _, partition := range collection.partitions {
			if partition.id == partitionID {
				match = true
				continue
			}
		}
		if !match {
			mp.addPartition(dbID, collectionID, partitionID)
		}
	}
}

func (mp *metaReplicaImpl) updatePartitionState(dbID UniqueID,
	collectionID UniqueID,
	partitionID UniqueID,
	state querypb.PartitionState) {
	for _, collection := range mp.db2collections[dbID] {
		if collection.id == collectionID {
			if partition, ok := collection.partitions[partitionID]; ok {
				partition.state = state
				return
			}
		}
	}
	log.Fatal("update partition state fail")
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
