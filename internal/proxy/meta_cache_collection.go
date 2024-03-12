package proxy

import (
	"sync"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// newMetaCacheCollectionCache creates a new metaCacheCollectionCache.
func newMetaCacheCollectionCache() *metaCacheCollectionCache {
	return &metaCacheCollectionCache{
		mu:                  sync.RWMutex{},
		collectionInfos:     make(map[typeutil.UniqueID]*collectionInfo),
		collectionNameIndex: make(map[string]map[string]typeutil.UniqueID),
	}
}

// TODO: we should implement meta cache in independent package, to avoid access of private members of row type like `collectionInfo`...
// metaCacheCollectionCache is a cache for collection information.
type metaCacheCollectionCache struct {
	mu sync.RWMutex

	collectionInfos     map[typeutil.UniqueID]*collectionInfo   // primary index, map id to collection info.
	collectionNameIndex map[string]map[string]typeutil.UniqueID // 2nd, map database name -> collection name -> collection id, keep consistency with collectionInfos.
}

// getCollectionByID returns the collection information related to provided collection id.
func (m *metaCacheCollectionCache) GetCollectionByID(collectionID UniqueID) (*collectionInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, ok := m.collectionInfos[collectionID]
	return info, ok
}

// GetCollectionByName returns the collection information related to provided collection name.
func (m *metaCacheCollectionCache) GetCollectionByName(databaseName string, collectionName string) (*collectionInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	db, ok := m.collectionNameIndex[databaseName]
	if !ok {
		return nil, false
	}
	collectionID, ok := db[collectionName]
	if !ok {
		return nil, false
	}
	info, ok := m.collectionInfos[collectionID]
	return info, ok
}

// HasDatabase checks if the database exists in the collection cache.
func (m *metaCacheCollectionCache) HasDatabase(database string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.collectionNameIndex[database]
	return ok
}

// RemoveCollectionByName removes the collection cache by collection name.
func (m *metaCacheCollectionCache) RemoveCollectionByName(databaseName string, collectionName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.collectionNameIndex[databaseName]; ok {
		if collectionID, ok := m.collectionNameIndex[databaseName][collectionName]; ok {
			m.removeCollectionByID(collectionID)
		}
	}
}

// RemoveCollectionByID removes the collection cache by collection id.
func (m *metaCacheCollectionCache) RemoveCollectionByID(collectionID UniqueID) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removeCollectionByID(collectionID)
}

// removeCollectionByID removes the collection cache by collection id.
func (m *metaCacheCollectionCache) removeCollectionByID(collectionID UniqueID) []string {
	if old, ok := m.collectionInfos[collectionID]; ok {
		relatedNames := make([]string, 0, 1+len(old.GetAliases()))
		relatedNames = append(relatedNames, old.GetCollectionName())
		relatedNames = append(relatedNames, old.GetAliases()...)

		if _, ok := m.collectionNameIndex[old.GetDatabaseName()]; ok {
			delete(m.collectionNameIndex[old.GetDatabaseName()], old.GetCollectionName())
			for _, alias := range old.GetAliases() {
				delete(m.collectionNameIndex[old.GetDatabaseName()], alias)
			}
		}
		delete(m.collectionInfos, collectionID)
		return relatedNames
	}
	return []string{}
}

// RemoveDatabase removes all collection cache and database by database name.
func (m *metaCacheCollectionCache) RemoveDatabase(database string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if collections, ok := m.collectionNameIndex[database]; ok {
		for _, collectionID := range collections {
			delete(m.collectionInfos, collectionID)
		}
		delete(m.collectionNameIndex, database)
	}
}

// RemovePartitionByName removes the partition cache by partition name.
// TODO: It's the only one way that modify the partial cache of collection.
// It's may be a lost update when RemovePartitionByName is called concurrently with `UpdateCollectionCache`,
// without protected by meta_cache's singleflight.
func (m *metaCacheCollectionCache) RemovePartitionByName(databaseName string, collectionName string, partitionName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// remove partition from collection cache.
	var collection *collectionInfo
	if db, ok := m.collectionNameIndex[databaseName]; ok {
		if collectionID, ok := db[collectionName]; ok {
			collection = m.collectionInfos[collectionID]
		}
	}
	if collection == nil {
		return
	}

	partitionInfos := lo.Filter(collection.partInfo.partitionInfos, func(info *partitionInfo, idx int) bool {
		return info.name != partitionName
	})
	// We perform a copy operation for collectionInfo.
	// otherwise, partInfo will be modified on original collectionInfo, a data race happens.
	m.collectionInfos[collection.collID] = &collectionInfo{
		collID:              collection.GetCollectionID(),
		schema:              collection.GetCollectionSchema(),
		partInfo:            parsePartitionsInfo(partitionInfos, collection.GetCollectionSchema().hasPartitionKeyField),
		createdTimestamp:    collection.createdTimestamp,
		createdUtcTimestamp: collection.createdUtcTimestamp,
		consistencyLevel:    collection.consistencyLevel,
		collectionName:      collection.GetCollectionName(),
		databaseName:        collection.GetDatabaseName(),
		aliases:             collection.GetAliases(),
	}
}

// UpdateCollectionCache updates the collection cache with the latest collection information.
func (m *metaCacheCollectionCache) UpdateCollectionCache(collection *collectionInfo) {
	if collection == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// clear old cache if exists.
	m.removeCollectionByID(collection.GetCollectionID())

	// update secondary index.
	if _, ok := m.collectionNameIndex[collection.GetDatabaseName()]; !ok {
		m.collectionNameIndex[collection.GetDatabaseName()] = make(map[string]typeutil.UniqueID)
	}
	// update origin collection name to secondary index.
	m.collectionNameIndex[collection.GetDatabaseName()][collection.GetCollectionName()] = collection.GetCollectionID()
	// update alias to secondary index.
	for _, alias := range collection.GetAliases() {
		m.collectionNameIndex[collection.GetDatabaseName()][alias] = collection.GetCollectionID()
	}
	m.collectionInfos[collection.GetCollectionID()] = collection
}
