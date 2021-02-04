package queryservice

import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type metaReplica interface {
	getCollections(dbID UniqueID) ([]*collection, error)
	getPartitions(dbID UniqueID, collectionID UniqueID) ([]*partition, error)
	getSegments(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) ([]*segment, error)
	loadCollection(dbID UniqueID, collectionID UniqueID) (*collection, error)
	loadPartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) (*partition, error)
	updatePartitionState(dbID UniqueID, collectionID UniqueID, partitionID UniqueID, state querypb.PartitionState) error
	getPartitionStates(dbID UniqueID, collectionID UniqueID, partitionIDs []UniqueID) ([]*querypb.PartitionStates, error)
	releaseCollection(dbID UniqueID, collectionID UniqueID) error
	releasePartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) error
	addDmChannels(collectionID UniqueID, channels []string) error
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
	id             UniqueID
	partitions     map[UniqueID]*partition
	dmChannelNames []string
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

func (mp *metaReplicaImpl) addCollection(dbID UniqueID, collectionID UniqueID) (*collection, error) {
	//TODO:: assert dbID = 0 exist
	if _, ok := mp.db2collections[dbID]; ok {
		partitions := make(map[UniqueID]*partition)
		newCollection := &collection{
			id:         collectionID,
			partitions: partitions,
		}
		mp.db2collections[dbID] = append(mp.db2collections[dbID], newCollection)
		return newCollection, nil
	}

	return nil, errors.New("addCollection: can't find dbID when add collection")
}

func (mp *metaReplicaImpl) addPartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) (*partition, error) {
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
				return partition, nil
			}
		}
	}
	return nil, errors.New("addPartition: can't find collection when add partition")
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

func (mp *metaReplicaImpl) loadCollection(dbID UniqueID, collectionID UniqueID) (*collection, error) {
	var res *collection = nil
	if collections, err := mp.getCollections(dbID); err == nil {
		for _, collection := range collections {
			if collectionID == collection.id {
				res = collection
			}
		}
	}
	if res == nil {
		collection, err := mp.addCollection(dbID, collectionID)
		if err != nil {
			return nil, err
		}
		res = collection
	}
	return res, nil
}

func (mp *metaReplicaImpl) loadPartition(dbID UniqueID, collectionID UniqueID, partitionID UniqueID) (*partition, error) {
	var collection *collection = nil
	var partition *partition = nil
	var err error
	for _, col := range mp.db2collections[dbID] {
		if col.id == collectionID {
			collection = col
		}
	}
	if collection == nil {
		collection, err = mp.addCollection(dbID, collectionID)
		if err != nil {
			return partition, err
		}
	}
	if _, ok := collection.partitions[partitionID]; !ok {
		partition, err = mp.addPartition(dbID, collectionID, partitionID)
		if err != nil {
			return partition, err
		}
		return partition, nil
	}

	return nil, nil
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

func (mp *metaReplicaImpl) addDmChannels(collectionID UniqueID, channels []string) error {
	//TODO :: use dbID
	if collections, ok := mp.db2collections[0]; ok {
		for _, collection := range collections {
			if collectionID == collection.id {
				dmChannels := collection.dmChannelNames
				for _, channel := range channels {
					match := false
					for _, existedChannel := range dmChannels {
						if channel == existedChannel {
							match = true
							break
						}
					}
					if !match {
						dmChannels = append(dmChannels, channel)
					}
				}
				return nil
			}
		}
	}
	return errors.New("addDmChannels: can't find dbID or collectionID")
}
