package meta

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
)

type Collection struct {
	*querypb.CollectionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
}

func (collection *Collection) Clone() *Collection {
	new := *collection
	new.CollectionLoadInfo = proto.Clone(collection.CollectionLoadInfo).(*querypb.CollectionLoadInfo)
	return &new
}

type Partition struct {
	*querypb.PartitionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
}

func (partition *Partition) Clone() *Partition {
	new := *partition
	new.PartitionLoadInfo = proto.Clone(partition.PartitionLoadInfo).(*querypb.PartitionLoadInfo)
	return &new
}

type CollectionManager struct {
	rwmutex sync.RWMutex

	collections map[UniqueID]*Collection
	partitions  map[UniqueID]*Partition
	store       Store
}

func NewCollectionManager(store Store) *CollectionManager {
	return &CollectionManager{
		collections: make(map[int64]*Collection),
		partitions:  make(map[int64]*Partition),
		store:       store,
	}
}

// Recover recovers collections from kv store,
// panics if failed
func (m *CollectionManager) Recover() error {
	collections, err := m.store.GetCollections()
	if err != nil {
		return err
	}
	partitions, err := m.store.GetPartitions()
	if err != nil {
		return err
	}

	for _, collection := range collections {
		// Collections not loaded done should be deprecated
		if collection.GetStatus() != querypb.LoadStatus_Loaded {
			m.store.ReleaseCollection(collection.GetCollectionID())
			continue
		}

		m.collections[collection.CollectionID] = &Collection{
			CollectionLoadInfo: collection,
		}
	}

	for collection, partitions := range partitions {
		for _, partition := range partitions {
			// Partitions not loaded done should be deprecated
			if partition.GetStatus() != querypb.LoadStatus_Loaded {
				partitionIDs := lo.Map(partitions, func(partition *querypb.PartitionLoadInfo, _ int) int64 {
					return partition.GetPartitionID()
				})
				m.store.ReleasePartition(collection, partitionIDs...)
				break
			}

			m.partitions[partition.PartitionID] = &Partition{
				PartitionLoadInfo: partition,
			}
		}
	}

	return nil
}

func (m *CollectionManager) GetCollection(id UniqueID) *Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.collections[id]
}

func (m *CollectionManager) GetPartition(id UniqueID) *Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.partitions[id]
}

func (m *CollectionManager) GetLoadType(id UniqueID) querypb.LoadType {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	_, ok := m.collections[id]
	if ok {
		return querypb.LoadType_LoadCollection
	}
	if len(m.getPartitionsByCollection(id)) > 0 {
		return querypb.LoadType_LoadPartition
	}
	return querypb.LoadType_UnKnownType
}

func (m *CollectionManager) GetReplicaNumber(id UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[id]
	if ok {
		return collection.GetReplicaNumber()
	}
	partitions := m.getPartitionsByCollection(id)
	if len(partitions) > 0 {
		return partitions[0].GetReplicaNumber()
	}
	return -1
}

func (m *CollectionManager) GetLoadPercentage(id UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[id]
	if ok {
		return collection.LoadPercentage
	}
	partitions := m.getPartitionsByCollection(id)
	if len(partitions) > 0 {
		return lo.SumBy(partitions, func(partition *Partition) int32 {
			return partition.LoadPercentage
		}) / int32(len(partitions))
	}
	return -1
}

func (m *CollectionManager) GetStatus(id UniqueID) querypb.LoadStatus {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[id]
	if ok {
		return collection.GetStatus()
	}
	partitions := m.getPartitionsByCollection(id)
	if len(partitions) == 0 {
		return querypb.LoadStatus_Invalid
	}
	for _, partition := range partitions {
		if partition.GetStatus() == querypb.LoadStatus_Loading {
			return querypb.LoadStatus_Loading
		}
	}
	return querypb.LoadStatus_Loaded
}

func (m *CollectionManager) Exist(id UniqueID) bool {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	_, ok := m.collections[id]
	if ok {
		return true
	}
	partitions := m.getPartitionsByCollection(id)
	return len(partitions) > 0
}

// GetAll returns the collection ID of all loaded collections and partitions
func (m *CollectionManager) GetAll() []int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ids := typeutil.NewUniqueSet()
	for _, collection := range m.collections {
		ids.Insert(collection.GetCollectionID())
	}
	for _, partition := range m.partitions {
		ids.Insert(partition.GetCollectionID())
	}
	return ids.Collect()
}

func (m *CollectionManager) GetAllCollections() []*Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return lo.Values(m.collections)
}

func (m *CollectionManager) GetAllPartitions() []*Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return lo.Values(m.partitions)
}

func (m *CollectionManager) GetPartitionsByCollection(collectionID UniqueID) []*Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getPartitionsByCollection(collectionID)
}

func (m *CollectionManager) getPartitionsByCollection(collectionID UniqueID) []*Partition {
	partitions := make([]*Partition, 0)
	for _, partition := range m.partitions {
		if partition.CollectionID == collectionID {
			partitions = append(partitions, partition)
		}
	}
	return partitions
}

func (m *CollectionManager) PutCollection(collection *Collection) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putCollection(collection, true)
}

func (m *CollectionManager) PutCollectionWithoutSave(collection *Collection) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.putCollection(collection, false)
}

func (m *CollectionManager) UpdateCollection(collection *Collection) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.collections[collection.GetCollectionID()]
	if !ok {
		return ErrCollectionNotFound
	}

	return m.putCollection(collection, true)
}

func (m *CollectionManager) UpdateCollectionInMemory(collection *Collection) bool {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.collections[collection.GetCollectionID()]
	if !ok {
		return false
	}

	m.putCollection(collection, false)
	return true
}

func (m *CollectionManager) putCollection(collection *Collection, withSave bool) error {
	if withSave {
		err := m.store.SaveCollection(collection.CollectionLoadInfo)
		if err != nil {
			return err
		}
	}
	m.collections[collection.CollectionID] = collection

	return nil
}

func (m *CollectionManager) PutPartition(partitions ...*Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putPartition(partitions, true)
}

func (m *CollectionManager) PutPartitionWithoutSave(partitions ...*Partition) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	m.putPartition(partitions, false)
}

func (m *CollectionManager) UpdatePartition(partition *Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.partitions[partition.GetPartitionID()]
	if !ok {
		return ErrPartitionNotFound
	}

	return m.putPartition([]*Partition{partition}, true)
}

func (m *CollectionManager) UpdatePartitionInMemory(partition *Partition) bool {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.partitions[partition.GetPartitionID()]
	if !ok {
		return false
	}

	m.putPartition([]*Partition{partition}, false)
	return true
}

func (m *CollectionManager) putPartition(partitions []*Partition, withSave bool) error {
	if withSave {
		loadInfos := lo.Map(partitions, func(partition *Partition, _ int) *querypb.PartitionLoadInfo {
			return partition.PartitionLoadInfo
		})
		err := m.store.SavePartition(loadInfos...)
		if err != nil {
			return err
		}
	}
	for _, partition := range partitions {
		m.partitions[partition.GetPartitionID()] = partition
	}
	return nil
}

func (m *CollectionManager) RemoveCollection(id UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.collections[id]
	if ok {
		err := m.store.ReleaseCollection(id)
		if err != nil {
			return err
		}
		delete(m.collections, id)
		return nil
	}

	partitions := lo.Map(m.getPartitionsByCollection(id),
		func(partition *Partition, _ int) int64 {
			return partition.GetPartitionID()
		})
	return m.removePartition(partitions...)
}

func (m *CollectionManager) RemovePartition(ids ...UniqueID) error {
	if len(ids) == 0 {
		return nil
	}

	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.removePartition(ids...)
}

func (m *CollectionManager) RemoveCollectionInMemory(id UniqueID) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	delete(m.collections, id)

	partitions := m.getPartitionsByCollection(id)
	for _, partition := range partitions {
		delete(m.partitions, partition.GetPartitionID())
	}
}

func (m *CollectionManager) removePartition(ids ...UniqueID) error {
	partition := m.partitions[ids[0]]
	err := m.store.ReleasePartition(partition.CollectionID, ids...)
	if err != nil {
		return err
	}
	for _, id := range ids {
		delete(m.partitions, id)
	}

	return nil
}
