package collection

import (
	"maps"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ CollectionMetaCache = (*collectionCache)(nil)

type collectionCache struct {
	mut         lock.RWMutex
	collections map[typeutil.UniqueID]*CollectionInfo // collection id to collection info
}

func NewCollectionCache() *collectionCache {
	return &collectionCache{
		collections: make(map[int64]*CollectionInfo),
	}
}

// AddCollection adds a collection into meta
// Note that collection info is just for caching and will not be set into etcd from datacoord
func (m *collectionCache) AddCollection(collection *CollectionInfo) {
	log.Info("meta update: add collection", zap.Int64("collectionID", collection.ID))
	m.mut.Lock()
	defer m.mut.Unlock()
	m.collections[collection.ID] = collection
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(len(m.collections)))
	log.Info("meta update: add collection - complete", zap.Int64("collectionID", collection.ID))
}

// DropCollection drop a collection from meta
func (m *collectionCache) DropCollection(collectionID int64) {
	log.Info("meta update: drop collection", zap.Int64("collectionID", collectionID))
	m.mut.Lock()
	defer m.mut.Unlock()
	delete(m.collections, collectionID)
	metrics.CleanupDataCoordWithCollectionID(collectionID)
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(len(m.collections)))
	log.Info("meta update: drop collection - complete", zap.Int64("collectionID", collectionID))
}

// GetCollection returns collection info with provided collection id from local cache
func (m *collectionCache) GetCollection(collectionID typeutil.UniqueID) *CollectionInfo {
	m.mut.RLock()
	defer m.mut.RUnlock()
	collection, ok := m.collections[collectionID]
	if !ok {
		return nil
	}
	return collection
}

// GetCollections returns collections from local cache
func (m *collectionCache) GetCollections() []*CollectionInfo {
	m.mut.RLock()
	defer m.mut.RUnlock()
	collections := make([]*CollectionInfo, 0)
	for _, coll := range m.collections {
		collections = append(collections, coll)
	}
	return collections
}

func (m *collectionCache) GetClonedCollectionInfo(collectionID typeutil.UniqueID) *CollectionInfo {
	m.mut.RLock()
	defer m.mut.RUnlock()

	coll, ok := m.collections[collectionID]
	if !ok {
		return nil
	}

	clonedProperties := make(map[string]string)
	maps.Copy(clonedProperties, coll.Properties)
	cloneColl := &CollectionInfo{
		ID:             coll.ID,
		Schema:         proto.Clone(coll.Schema).(*schemapb.CollectionSchema),
		Partitions:     coll.Partitions,
		StartPositions: common.CloneKeyDataPairs(coll.StartPositions),
		Properties:     clonedProperties,
		DatabaseName:   coll.DatabaseName,
		DatabaseID:     coll.DatabaseID,
		VChannelNames:  coll.VChannelNames,
	}

	return cloneColl
}
