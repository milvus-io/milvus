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
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/eventlog"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Collection struct {
	*querypb.CollectionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
	UpdatedAt      time.Time

	mut             sync.RWMutex
	refreshNotifier chan struct{}
	LoadSpan        trace.Span
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
		LoadSpan:           collection.LoadSpan,
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

	collections map[typeutil.UniqueID]*Collection
	partitions  map[typeutil.UniqueID]*Partition

	collectionPartitions map[typeutil.UniqueID]typeutil.Set[typeutil.UniqueID]
	catalog              metastore.QueryCoordCatalog
}

func NewCollectionManager(catalog metastore.QueryCoordCatalog) *CollectionManager {
	return &CollectionManager{
		collections:          make(map[int64]*Collection),
		partitions:           make(map[int64]*Partition),
		collectionPartitions: make(map[int64]typeutil.Set[typeutil.UniqueID]),
		catalog:              catalog,
	}
}

// Recover recovers collections from kv store,
// panics if failed
func (m *CollectionManager) Recover(broker Broker) error {
	collections, err := m.catalog.GetCollections()
	if err != nil {
		return err
	}
	partitions, err := m.catalog.GetPartitions()
	if err != nil {
		return err
	}

	ctx := log.WithTraceID(context.Background(), strconv.FormatInt(time.Now().UnixNano(), 10))
	ctxLog := log.Ctx(ctx)
	ctxLog.Info("recover collections and partitions from kv store")

	for _, collection := range collections {
		if collection.GetReplicaNumber() <= 0 {
			ctxLog.Info("skip recovery and release collection due to invalid replica number",
				zap.Int64("collectionID", collection.GetCollectionID()),
				zap.Int32("replicaNumber", collection.GetReplicaNumber()))
			m.catalog.ReleaseCollection(collection.GetCollectionID())
			continue
		}

		if collection.GetStatus() != querypb.LoadStatus_Loaded {
			if collection.RecoverTimes >= paramtable.Get().QueryCoordCfg.CollectionRecoverTimesLimit.GetAsInt32() {
				m.catalog.ReleaseCollection(collection.CollectionID)
				ctxLog.Info("recover loading collection times reach limit, release collection",
					zap.Int64("collectionID", collection.CollectionID),
					zap.Int32("recoverTimes", collection.RecoverTimes))
				break
			}
			// update recoverTimes meta in etcd
			collection.RecoverTimes += 1
			m.putCollection(true, &Collection{CollectionLoadInfo: collection})
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
				if partition.RecoverTimes >= paramtable.Get().QueryCoordCfg.CollectionRecoverTimesLimit.GetAsInt32() {
					m.catalog.ReleaseCollection(collection)
					ctxLog.Info("recover loading partition times reach limit, release collection",
						zap.Int64("collectionID", collection),
						zap.Int32("recoverTimes", partition.RecoverTimes))
					break
				}

				partition.RecoverTimes += 1
				m.putPartition([]*Partition{{PartitionLoadInfo: partition}}, true)
				continue
			}

			m.putPartition([]*Partition{
				{
					PartitionLoadInfo: partition,
				},
			}, false)
		}
	}

	err = m.upgradeRecover(broker)
	if err != nil {
		log.Warn("upgrade recover failed", zap.Error(err))
		return err
	}
	return nil
}

// upgradeRecover recovers from old version <= 2.2.x for compatibility.
func (m *CollectionManager) upgradeRecover(broker Broker) error {
	// for loaded collection from 2.2, it only save a old version CollectionLoadInfo without LoadType.
	// we should update the CollectionLoadInfo and save all PartitionLoadInfo to meta store
	for _, collection := range m.GetAllCollections() {
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

			newInfo := collection.Clone()
			newInfo.LoadType = querypb.LoadType_LoadCollection
			err = m.putCollection(true, newInfo)
			if err != nil {
				return err
			}
		}
	}

	// for loaded partition from 2.2, it only save load PartitionLoadInfo.
	// we should save it's CollectionLoadInfo to meta store
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

func (m *CollectionManager) GetCollection(collectionID typeutil.UniqueID) *Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.collections[collectionID]
}

func (m *CollectionManager) GetPartition(partitionID typeutil.UniqueID) *Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.partitions[partitionID]
}

func (m *CollectionManager) GetLoadType(collectionID typeutil.UniqueID) querypb.LoadType {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetLoadType()
	}
	return querypb.LoadType_UnKnownType
}

func (m *CollectionManager) GetReplicaNumber(collectionID typeutil.UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetReplicaNumber()
	}
	return -1
}

// CalculateLoadPercentage checks if collection is currently fully loaded.
func (m *CollectionManager) CalculateLoadPercentage(collectionID typeutil.UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.calculateLoadPercentage(collectionID)
}

func (m *CollectionManager) calculateLoadPercentage(collectionID typeutil.UniqueID) int32 {
	_, ok := m.collections[collectionID]
	if ok {
		partitions := m.getPartitionsByCollection(collectionID)
		if len(partitions) > 0 {
			return lo.SumBy(partitions, func(partition *Partition) int32 {
				return partition.LoadPercentage
			}) / int32(len(partitions))
		}
	}
	return -1
}

func (m *CollectionManager) GetPartitionLoadPercentage(partitionID typeutil.UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	partition, ok := m.partitions[partitionID]
	if ok {
		return partition.LoadPercentage
	}
	return -1
}

func (m *CollectionManager) CalculateLoadStatus(collectionID typeutil.UniqueID) querypb.LoadStatus {
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

func (m *CollectionManager) GetFieldIndex(collectionID typeutil.UniqueID) map[int64]int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetFieldIndexID()
	}
	return nil
}

func (m *CollectionManager) Exist(collectionID typeutil.UniqueID) bool {
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

func (m *CollectionManager) GetPartitionsByCollection(collectionID typeutil.UniqueID) []*Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getPartitionsByCollection(collectionID)
}

func (m *CollectionManager) getPartitionsByCollection(collectionID typeutil.UniqueID) []*Partition {
	return lo.Map(m.collectionPartitions[collectionID].Collect(), func(partitionID int64, _ int) *Partition { return m.partitions[partitionID] })
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
		err := m.catalog.SaveCollection(collection.CollectionLoadInfo, partitionInfos...)
		if err != nil {
			return err
		}
	}
	for _, partition := range partitions {
		partition.UpdatedAt = time.Now()
		m.partitions[partition.GetPartitionID()] = partition

		partitions := m.collectionPartitions[collection.CollectionID]
		if partitions == nil {
			partitions = make(typeutil.Set[int64])
			m.collectionPartitions[collection.CollectionID] = partitions
		}
		partitions.Insert(partition.GetPartitionID())
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
		err := m.catalog.SavePartition(loadInfos...)
		if err != nil {
			return err
		}
	}
	for _, partition := range partitions {
		partition.UpdatedAt = time.Now()
		m.partitions[partition.GetPartitionID()] = partition
		collID := partition.GetCollectionID()

		partitions := m.collectionPartitions[collID]
		if partitions == nil {
			partitions = make(typeutil.Set[int64])
			m.collectionPartitions[collID] = partitions
		}
		partitions.Insert(partition.GetPartitionID())
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

	// update partition load percentage
	newPartition := oldPartition.Clone()
	newPartition.LoadPercentage = loadPercent
	savePartition := false
	if loadPercent == 100 {
		savePartition = true
		newPartition.Status = querypb.LoadStatus_Loaded
		// if partition becomes loaded, clear it's recoverTimes in load info
		newPartition.RecoverTimes = 0
		elapsed := time.Since(newPartition.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
		eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("Partition %d loaded", partitionID)))
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
	newCollection := oldCollection.Clone()
	newCollection.LoadPercentage = collectionPercent
	saveCollection := false
	if collectionPercent == 100 {
		saveCollection = true
		if newCollection.LoadSpan != nil {
			newCollection.LoadSpan.End()
			newCollection.LoadSpan = nil
		}
		newCollection.Status = querypb.LoadStatus_Loaded

		// if collection becomes loaded, clear it's recoverTimes in load info
		newCollection.RecoverTimes = 0

		// TODO: what if part of the collection has been unloaded? Now we decrease the metric only after
		// 	`ReleaseCollection` is triggered. Maybe it's hard to make this metric really accurate.
		metrics.QueryCoordNumCollections.WithLabelValues().Inc()
		elapsed := time.Since(newCollection.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
		eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("Collection %d loaded", newCollection.CollectionID)))
	}
	return collectionPercent, m.putCollection(saveCollection, newCollection)
}

// RemoveCollection removes collection and its partitions.
func (m *CollectionManager) RemoveCollection(collectionID typeutil.UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.collections[collectionID]
	if ok {
		err := m.catalog.ReleaseCollection(collectionID)
		if err != nil {
			return err
		}
		delete(m.collections, collectionID)
		for _, partition := range m.collectionPartitions[collectionID].Collect() {
			delete(m.partitions, partition)
		}
		delete(m.collectionPartitions, collectionID)
	}
	metrics.CleanQueryCoordMetricsWithCollectionID(collectionID)
	return nil
}

func (m *CollectionManager) RemovePartition(collectionID typeutil.UniqueID, partitionIDs ...typeutil.UniqueID) error {
	if len(partitionIDs) == 0 {
		return nil
	}

	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.removePartition(collectionID, partitionIDs...)
}

func (m *CollectionManager) removePartition(collectionID typeutil.UniqueID, partitionIDs ...typeutil.UniqueID) error {
	err := m.catalog.ReleasePartition(collectionID, partitionIDs...)
	if err != nil {
		return err
	}
	partitions := m.collectionPartitions[collectionID]
	for _, id := range partitionIDs {
		delete(m.partitions, id)
		delete(partitions, id)
	}

	return nil
}
