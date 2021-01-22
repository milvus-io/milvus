package queryservice

import "github.com/zilliztech/milvus-distributed/internal/errors"

type metaReplica interface {
	getCollectionIDs(id UniqueID) ([]UniqueID, error)
	getPartitionIDs(dbID UniqueID, collectionID UniqueID) ([]UniqueID, error)
}

type metaReplicaImpl struct {
	dbID           []UniqueID
	db2collections map[UniqueID][]*collection
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
