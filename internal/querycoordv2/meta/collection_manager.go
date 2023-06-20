// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Collection struct {
	*querypb.CollectionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
	UpdatedAt      time.Time

	mut             sync.RWMutex
	refreshNotifier chan struct{}
}

func (collection *Collection) SetRefreshNotifier(notifier chan struct{}) {
	collection.mut.Lock()
	defer collection.mut.Unlock()

	collection.refreshNotifier = notifier
}

func (collection *Collection) IsRefreshed() bool {
	collection.mut.RLock()
	notifier := collection.refreshNotifier
	collection.mut.RUnlock()

	if notifier == nil {
		return true
	}

	select {
	case <-notifier:
		return true

	default:
	}
	return false
}

func (collection *Collection) Clone() *Collection {
	return &Collection{
		CollectionLoadInfo: proto.Clone(collection.CollectionLoadInfo).(*querypb.CollectionLoadInfo),
		LoadPercentage:     collection.LoadPercentage,
		CreatedAt:          collection.CreatedAt,
		UpdatedAt:          collection.UpdatedAt,
		refreshNotifier:    collection.refreshNotifier,
	}
}

type Partition struct {
	*querypb.PartitionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
	UpdatedAt      time.Time
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
func (m *CollectionManager) Recover(broker Broker) error {
	collections, err := m.store.GetCollections()
	if err != nil {
		return err
	}
	partitions, err := m.store.GetPartitions()
	if err != nil {
		return err
	}

	for _, collection := range collections {
		// Dropped collection should be deprecated
		_, err = broker.GetCollectionSchema(context.Background(), collection.GetCollectionID())
		if errors.Is(err, merr.ErrCollectionNotFound) {
			log.Info("skip dropped collection during recovery", zap.Int64("collection", collection.GetCollectionID()))
			m.store.ReleaseCollection(collection.GetCollectionID())
			continue
		}
		if err != nil {
			return err
		}
		// Collections not loaded done should be deprecated
		if collection.GetStatus() != querypb.LoadStatus_Loaded || collection.GetReplicaNumber() <= 0 {
			log.Info("skip recovery and release collection",
				zap.Int64("collectionID", collection.GetCollectionID()),
				zap.String("status", collection.GetStatus().String()),
				zap.Int32("replicaNumber", collection.GetReplicaNumber()),
			)
			m.store.ReleaseCollection(collection.GetCollectionID())
			continue
		}
		m.collections[collection.CollectionID] = &Collection{
			CollectionLoadInfo: collection,
		}
	}

	for collection, partitions := range partitions {
		existPartitions, err := broker.GetPartitions(context.Background(), collection)
		if errors.Is(err, merr.ErrCollectionNotFound) {
			log.Info("skip dropped collection during recovery", zap.Int64("collection", collection))
			m.store.ReleaseCollection(collection)
			continue
		}
		if err != nil {
			return err
		}
		omitPartitions := make([]int64, 0)
		partitions = lo.Filter(partitions, func(partition *querypb.PartitionLoadInfo, _ int) bool {
			if !lo.Contains(existPartitions, partition.GetPartitionID()) {
				omitPartitions = append(omitPartitions, partition.GetPartitionID())
				return false
			}
			return true
		})
		if len(omitPartitions) > 0 {
			log.Info("skip dropped partitions during recovery",
				zap.Int64("collection", collection), zap.Int64s("partitions", omitPartitions))
			m.store.ReleasePartition(collection, omitPartitions...)
		}

		sawLoaded := false
		for _, partition := range partitions {
			// Partitions not loaded done should be deprecated
			if partition.GetStatus() != querypb.LoadStatus_Loaded {
				log.Info("skip recovery and release partition",
					zap.Int64("collectionID", collection),
					zap.Int64("partitionID", partition.GetPartitionID()),
					zap.String("status", partition.GetStatus().String()),
				)
				m.store.ReleasePartition(collection, partition.GetPartitionID())
				continue
			}

			sawLoaded = true
			m.partitions[partition.PartitionID] = &Partition{
				PartitionLoadInfo: partition,
			}
		}

		if !sawLoaded {
			m.store.ReleaseCollection(collection)
		}
	}

	err = m.upgradeRecover(broker)
	if err != nil {
		log.Error("upgrade recover failed", zap.Error(err))
		return err
	}
	return nil
}

// upgradeRecover recovers from old version <= 2.2.x for compatibility.
func (m *CollectionManager) upgradeRecover(broker Broker) error {
	for _, collection := range m.GetAllCollections() {
		// It's a workaround to check if it is old CollectionLoadInfo because there's no
		// loadType in old version, maybe we should use version instead.
		if collection.GetLoadType() == querypb.LoadType_UnKnownType {
			partitionIDs, err := broker.GetPartitions(context.Background(), collection.GetCollectionID())
			if err != nil {
				return err
			}
			partitions := lo.Map(partitionIDs, func(partitionID int64, _ int) *Partition {
				return &Partition{
					PartitionLoadInfo: &querypb.PartitionLoadInfo{
						CollectionID:  collection.GetCollectionID(),
						PartitionID:   partitionID,
						ReplicaNumber: collection.GetReplicaNumber(),
						Status:        querypb.LoadStatus_Loaded,
						FieldIndexID:  collection.GetFieldIndexID(),
					},
					LoadPercentage: 100,
				}
			})
			err = m.putPartition(partitions, true)
			if err != nil {
				return err
			}
		}
	}
	for _, partition := range m.GetAllPartitions() {
		// In old version, collection would NOT be stored if the partition existed.
		if _, ok := m.collections[partition.GetCollectionID()]; !ok {
			col := &Collection{
				CollectionLoadInfo: &querypb.CollectionLoadInfo{
					CollectionID:  partition.GetCollectionID(),
					ReplicaNumber: partition.GetReplicaNumber(),
					Status:        partition.GetStatus(),
					FieldIndexID:  partition.GetFieldIndexID(),
					LoadType:      querypb.LoadType_LoadPartition,
				},
				LoadPercentage: 100,
			}
			err := m.PutCollection(col)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *CollectionManager) GetCollection(collectionID UniqueID) *Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.collections[collectionID]
}

func (m *CollectionManager) GetPartition(partitionID UniqueID) *Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.partitions[partitionID]
}

func (m *CollectionManager) GetLoadType(collectionID UniqueID) querypb.LoadType {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetLoadType()
	}
	return querypb.LoadType_UnKnownType
}

func (m *CollectionManager) GetReplicaNumber(collectionID UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetReplicaNumber()
	}
	return -1
}

// CalculateLoadPercentage checks if collection is currently fully loaded.
func (m *CollectionManager) CalculateLoadPercentage(collectionID UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.calculateLoadPercentage(collectionID)
}

func (m *CollectionManager) calculateLoadPercentage(collectionID UniqueID) int32 {
	collection, ok := m.collections[collectionID]
	if ok {
		partitions := m.getPartitionsByCollection(collectionID)
		if len(partitions) > 0 {
			return lo.SumBy(partitions, func(partition *Partition) int32 {
				return partition.LoadPercentage
			}) / int32(len(partitions))
		}
		if collection.GetLoadType() == querypb.LoadType_LoadCollection {
			// no partition exists
			return 100
		}
	}
	return -1
}

func (m *CollectionManager) GetPartitionLoadPercentage(partitionID UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	partition, ok := m.partitions[partitionID]
	if ok {
		return partition.LoadPercentage
	}
	return -1
}

func (m *CollectionManager) CalculateLoadStatus(collectionID UniqueID) querypb.LoadStatus {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if !ok {
		return querypb.LoadStatus_Invalid
	}
	partitions := m.getPartitionsByCollection(collectionID)
	for _, partition := range partitions {
		if partition.GetStatus() == querypb.LoadStatus_Loading {
			return querypb.LoadStatus_Loading
		}
	}
	if len(partitions) > 0 {
		return querypb.LoadStatus_Loaded
	}
	if collection.GetLoadType() == querypb.LoadType_LoadCollection {
		return querypb.LoadStatus_Loaded
	}
	return querypb.LoadStatus_Invalid
}

func (m *CollectionManager) GetFieldIndex(collectionID UniqueID) map[int64]int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetFieldIndexID()
	}
	return nil
}

func (m *CollectionManager) Exist(collectionID UniqueID) bool {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	_, ok := m.collections[collectionID]
	return ok
}

// GetAll returns the collection ID of all loaded collections
func (m *CollectionManager) GetAll() []int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ids := typeutil.NewUniqueSet()
	for _, collection := range m.collections {
		ids.Insert(collection.GetCollectionID())
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

func (m *CollectionManager) PutCollection(collection *Collection, partitions ...*Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putCollection(true, collection, partitions...)
}

func (m *CollectionManager) PutCollectionWithoutSave(collection *Collection) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putCollection(false, collection)
}

func (m *CollectionManager) putCollection(withSave bool, collection *Collection, partitions ...*Partition) error {
	if withSave {
		partitionInfos := lo.Map(partitions, func(partition *Partition, _ int) *querypb.PartitionLoadInfo {
			return partition.PartitionLoadInfo
		})
		err := m.store.SaveCollection(collection.CollectionLoadInfo, partitionInfos...)
		if err != nil {
			return err
		}
	}
	for _, partition := range partitions {
		partition.UpdatedAt = time.Now()
		m.partitions[partition.GetPartitionID()] = partition
	}
	collection.UpdatedAt = time.Now()
	m.collections[collection.CollectionID] = collection

	return nil
}

func (m *CollectionManager) PutPartition(partitions ...*Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putPartition(partitions, true)
}

func (m *CollectionManager) PutPartitionWithoutSave(partitions ...*Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putPartition(partitions, false)
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
		partition.UpdatedAt = time.Now()
		m.partitions[partition.GetPartitionID()] = partition
	}
	return nil
}

func (m *CollectionManager) UpdateLoadPercent(partitionID int64, loadPercent int32) (int32, error) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	oldPartition, ok := m.partitions[partitionID]
	if !ok {
		return 0, merr.WrapErrPartitionNotFound(partitionID)
	}

	if loadPercent <= oldPartition.LoadPercentage {
		return m.calculateLoadPercentage(oldPartition.GetCollectionID()), nil
	}

	// update partition load percentage
	newPartition := oldPartition.Clone()
	newPartition.LoadPercentage = loadPercent
	savePartition := false
	if loadPercent == 100 {
		savePartition = true
		newPartition.Status = querypb.LoadStatus_Loaded
		elapsed := time.Since(newPartition.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
	}
	err := m.putPartition([]*Partition{newPartition}, savePartition)
	if err != nil {
		return 0, err
	}

	// update collection load percentage
	oldCollection, ok := m.collections[newPartition.CollectionID]
	if !ok {
		return 0, merr.WrapErrCollectionNotFound(newPartition.CollectionID)
	}
	collectionPercent := m.calculateLoadPercentage(oldCollection.CollectionID)
	if collectionPercent <= oldCollection.LoadPercentage {
		return m.calculateLoadPercentage(oldPartition.GetCollectionID()), nil
	}
	newCollection := oldCollection.Clone()
	newCollection.LoadPercentage = collectionPercent
	saveCollection := false
	if collectionPercent == 100 {
		saveCollection = true
		newCollection.Status = querypb.LoadStatus_Loaded
		elapsed := time.Since(newCollection.CreatedAt)

		// TODO: what if part of the collection has been unloaded? Now we decrease the metric only after
		// 	`ReleaseCollection` is triggered. Maybe it's hard to make this metric really accurate.
		metrics.QueryCoordNumCollections.WithLabelValues().Inc()

		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
	}
	return collectionPercent, m.putCollection(saveCollection, newCollection)
}

// RemoveCollection removes collection and its partitions.
func (m *CollectionManager) RemoveCollection(collectionID UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.collections[collectionID]
	if ok {
		err := m.store.ReleaseCollection(collectionID)
		if err != nil {
			return err
		}
		delete(m.collections, collectionID)
		for partID, partition := range m.partitions {
			if partition.CollectionID == collectionID {
				delete(m.partitions, partID)
			}
		}
	}
	return nil
}

func (m *CollectionManager) RemovePartition(ids ...UniqueID) error {
	if len(ids) == 0 {
		return nil
	}

	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.removePartition(ids...)
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
