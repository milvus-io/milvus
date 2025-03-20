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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/eventlog"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type Collection struct {
	*querypb.CollectionLoadInfo
	LoadPercentage int32
	CreatedAt      time.Time
	UpdatedAt      time.Time

	mut             sync.RWMutex
	refreshNotifier chan struct{}
	LoadSpan        trace.Span
	Schema          *schemapb.CollectionSchema
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
		Schema:             collection.Schema,
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
func (m *CollectionManager) Recover(ctx context.Context, broker Broker) error {
	start := time.Now()
	collections, err := m.catalog.GetCollections(ctx)
	if err != nil {
		return err
	}
	log.Ctx(ctx).Info("recover collections from kv store", zap.Duration("dur", time.Since(start)))

	start = time.Now()
	partitions, err := m.catalog.GetPartitions(ctx, lo.Map(collections, func(collection *querypb.CollectionLoadInfo, _ int) int64 {
		return collection.GetCollectionID()
	}))
	if err != nil {
		return err
	}

	ctx = log.WithTraceID(ctx, strconv.FormatInt(time.Now().UnixNano(), 10))
	ctxLog := log.Ctx(ctx)
	ctxLog.Info("recover partitions from kv store", zap.Duration("dur", time.Since(start)))

	for _, collectionLoadInfo := range collections {
		if collectionLoadInfo.GetReplicaNumber() <= 0 {
			ctxLog.Info("skip recovery and release collection due to invalid replica number",
				zap.Int64("collectionID", collectionLoadInfo.GetCollectionID()),
				zap.Int32("replicaNumber", collectionLoadInfo.GetReplicaNumber()))
			m.catalog.ReleaseCollection(ctx, collectionLoadInfo.GetCollectionID())
			continue
		}

		if collectionLoadInfo.GetStatus() != querypb.LoadStatus_Loaded {
			if collectionLoadInfo.RecoverTimes >= paramtable.Get().QueryCoordCfg.CollectionRecoverTimesLimit.GetAsInt32() {
				m.catalog.ReleaseCollection(ctx, collectionLoadInfo.CollectionID)
				ctxLog.Info("recover loading collection times reach limit, release collection",
					zap.Int64("collectionID", collectionLoadInfo.CollectionID),
					zap.Int32("recoverTimes", collectionLoadInfo.RecoverTimes))
				break
			}
			// update recoverTimes meta in etcd
			collectionLoadInfo.RecoverTimes += 1
			m.putCollection(ctx, true, &Collection{CollectionLoadInfo: collectionLoadInfo})
			continue
		}
		resp, err := broker.DescribeCollection(context.Background(), collectionLoadInfo.CollectionID)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		collection := &Collection{
			CollectionLoadInfo: collectionLoadInfo,
			Schema:             resp.Schema,
		}
		err = m.upgradeLoadFields(ctx, collection)
		if err != nil {
			if errors.Is(err, merr.ErrCollectionNotFound) {
				log.Warn("collection not found, skip upgrade logic and wait for release")
			} else {
				log.Warn("upgrade load field failed", zap.Error(err))
				return err
			}
		}

		// update collection's CreateAt and UpdateAt to now after qc restart
		m.putCollection(ctx, false, &Collection{
			CollectionLoadInfo: collection.CollectionLoadInfo,
			CreatedAt:          time.Now(),
			Schema:             collection.Schema,
		})
	}

	for collection, partitions := range partitions {
		for _, partition := range partitions {
			// Partitions not loaded done should be deprecated
			if partition.GetStatus() != querypb.LoadStatus_Loaded {
				if partition.RecoverTimes >= paramtable.Get().QueryCoordCfg.CollectionRecoverTimesLimit.GetAsInt32() {
					m.catalog.ReleaseCollection(ctx, collection)
					ctxLog.Info("recover loading partition times reach limit, release collection",
						zap.Int64("collectionID", collection),
						zap.Int32("recoverTimes", partition.RecoverTimes))
					break
				}

				partition.RecoverTimes += 1
				m.putPartition(ctx, []*Partition{
					{
						PartitionLoadInfo: partition,
						CreatedAt:         time.Now(),
					},
				}, true)
				continue
			}

			m.putPartition(ctx, []*Partition{
				{
					PartitionLoadInfo: partition,
					CreatedAt:         time.Now(),
				},
			}, false)
		}
	}

	return nil
}

func (m *CollectionManager) upgradeLoadFields(ctx context.Context, collection *Collection) error {
	// only fill load fields when value is nil
	if collection.LoadFields != nil {
		return nil
	}

	// fill all field id as legacy default behavior
	collection.LoadFields = lo.FilterMap(collection.Schema.GetFields(), func(fieldSchema *schemapb.FieldSchema, _ int) (int64, bool) {
		// load fields list excludes system fields
		return fieldSchema.GetFieldID(), !common.IsSystemField(fieldSchema.GetFieldID())
	})
	collection.LoadPercentage = 100
	// put updated meta back to store
	err := m.putCollection(ctx, true, collection)
	if err != nil {
		return err
	}

	return nil
}

func (m *CollectionManager) GetCollection(ctx context.Context, collectionID typeutil.UniqueID) *Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.collections[collectionID]
}

func (m *CollectionManager) GetCollectionSchema(ctx context.Context, collectionID typeutil.UniqueID) *schemapb.CollectionSchema {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()
	collection, ok := m.collections[collectionID]
	if !ok {
		return nil
	}
	return collection.Schema
}

func (m *CollectionManager) GetPartition(ctx context.Context, partitionID typeutil.UniqueID) *Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.partitions[partitionID]
}

func (m *CollectionManager) GetLoadType(ctx context.Context, collectionID typeutil.UniqueID) querypb.LoadType {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetLoadType()
	}
	return querypb.LoadType_UnKnownType
}

func (m *CollectionManager) GetReplicaNumber(ctx context.Context, collectionID typeutil.UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetReplicaNumber()
	}
	return -1
}

// CalculateLoadPercentage checks if collection is currently fully loaded.
func (m *CollectionManager) CalculateLoadPercentage(ctx context.Context, collectionID typeutil.UniqueID) int32 {
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

func (m *CollectionManager) GetPartitionLoadPercentage(ctx context.Context, partitionID typeutil.UniqueID) int32 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	partition, ok := m.partitions[partitionID]
	if ok {
		return partition.LoadPercentage
	}
	return -1
}

func (m *CollectionManager) CalculateLoadStatus(ctx context.Context, collectionID typeutil.UniqueID) querypb.LoadStatus {
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

func (m *CollectionManager) GetFieldIndex(ctx context.Context, collectionID typeutil.UniqueID) map[int64]int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetFieldIndexID()
	}
	return nil
}

func (m *CollectionManager) GetLoadFields(ctx context.Context, collectionID typeutil.UniqueID) []int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		return collection.GetLoadFields()
	}
	return nil
}

func (m *CollectionManager) Exist(ctx context.Context, collectionID typeutil.UniqueID) bool {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	_, ok := m.collections[collectionID]
	return ok
}

// GetAll returns the collection ID of all loaded collections
func (m *CollectionManager) GetAll(ctx context.Context) []int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ids := typeutil.NewUniqueSet()
	for _, collection := range m.collections {
		ids.Insert(collection.GetCollectionID())
	}
	return ids.Collect()
}

func (m *CollectionManager) GetAllCollections(ctx context.Context) []*Collection {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return lo.Values(m.collections)
}

func (m *CollectionManager) GetAllPartitions(ctx context.Context) []*Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return lo.Values(m.partitions)
}

func (m *CollectionManager) GetPartitionsByCollection(ctx context.Context, collectionID typeutil.UniqueID) []*Partition {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.getPartitionsByCollection(collectionID)
}

func (m *CollectionManager) getPartitionsByCollection(collectionID typeutil.UniqueID) []*Partition {
	return lo.Map(m.collectionPartitions[collectionID].Collect(), func(partitionID int64, _ int) *Partition { return m.partitions[partitionID] })
}

func (m *CollectionManager) PutCollection(ctx context.Context, collection *Collection, partitions ...*Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putCollection(ctx, true, collection, partitions...)
}

func (m *CollectionManager) PutCollectionWithoutSave(ctx context.Context, collection *Collection) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putCollection(ctx, false, collection)
}

func (m *CollectionManager) putCollection(ctx context.Context, withSave bool, collection *Collection, partitions ...*Partition) error {
	if withSave {
		partitionInfos := lo.Map(partitions, func(partition *Partition, _ int) *querypb.PartitionLoadInfo {
			return partition.PartitionLoadInfo
		})
		err := m.catalog.SaveCollection(ctx, collection.CollectionLoadInfo, partitionInfos...)
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

func (m *CollectionManager) PutPartition(ctx context.Context, partitions ...*Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putPartition(ctx, partitions, true)
}

func (m *CollectionManager) PutPartitionWithoutSave(ctx context.Context, partitions ...*Partition) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.putPartition(ctx, partitions, false)
}

func (m *CollectionManager) putPartition(ctx context.Context, partitions []*Partition, withSave bool) error {
	if withSave {
		loadInfos := lo.Map(partitions, func(partition *Partition, _ int) *querypb.PartitionLoadInfo {
			return partition.PartitionLoadInfo
		})
		err := m.catalog.SavePartition(ctx, loadInfos...)
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

func (m *CollectionManager) updateLoadMetrics() {
	metrics.QueryCoordNumCollections.WithLabelValues().Set(float64(len(lo.Filter(lo.Values(m.collections), func(coll *Collection, _ int) bool { return coll.LoadPercentage == 100 }))))
	metrics.QueryCoordNumPartitions.WithLabelValues().Set(float64(len(lo.Filter(lo.Values(m.partitions), func(part *Partition, _ int) bool { return part.LoadPercentage == 100 }))))
}

func (m *CollectionManager) UpdatePartitionLoadPercent(ctx context.Context, partitionID int64, loadPercent int32) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	oldPartition, ok := m.partitions[partitionID]
	if !ok {
		return merr.WrapErrPartitionNotFound(partitionID)
	}

	// update partition load percentage
	newPartition := oldPartition.Clone()
	newPartition.LoadPercentage = loadPercent
	savePartition := false
	if loadPercent == 100 {
		savePartition = newPartition.Status != querypb.LoadStatus_Loaded || newPartition.RecoverTimes != 0
		newPartition.Status = querypb.LoadStatus_Loaded
		// if partition becomes loaded, clear it's recoverTimes in load info
		newPartition.RecoverTimes = 0
		elapsed := time.Since(newPartition.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
		eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("Partition %d loaded", partitionID)))
	}
	return m.putPartition(ctx, []*Partition{newPartition}, savePartition)
}

func (m *CollectionManager) UpdateCollectionLoadPercent(ctx context.Context, collectionID int64) (int32, error) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	// update collection load percentage
	oldCollection, ok := m.collections[collectionID]
	if !ok {
		return 0, merr.WrapErrCollectionNotFound(collectionID)
	}
	collectionPercent := m.calculateLoadPercentage(oldCollection.CollectionID)
	newCollection := oldCollection.Clone()
	newCollection.LoadPercentage = collectionPercent
	saveCollection := false
	if collectionPercent == 100 {
		saveCollection = newCollection.Status != querypb.LoadStatus_Loaded || newCollection.RecoverTimes != 0
		if newCollection.LoadSpan != nil {
			newCollection.LoadSpan.End()
			newCollection.LoadSpan = nil
		}
		newCollection.Status = querypb.LoadStatus_Loaded

		// if collection becomes loaded, clear it's recoverTimes in load info
		newCollection.RecoverTimes = 0

		defer m.updateLoadMetrics()
		elapsed := time.Since(newCollection.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
		eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("Collection %d loaded", newCollection.CollectionID)))
	}
	return collectionPercent, m.putCollection(ctx, saveCollection, newCollection)
}

// RemoveCollection removes collection and its partitions.
func (m *CollectionManager) RemoveCollection(ctx context.Context, collectionID typeutil.UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	_, ok := m.collections[collectionID]
	if ok {
		err := m.catalog.ReleaseCollection(ctx, collectionID)
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
	m.updateLoadMetrics()
	return nil
}

func (m *CollectionManager) RemovePartition(ctx context.Context, collectionID typeutil.UniqueID, partitionIDs ...typeutil.UniqueID) error {
	if len(partitionIDs) == 0 {
		return nil
	}

	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	err := m.removePartition(ctx, collectionID, partitionIDs...)
	return err
}

func (m *CollectionManager) removePartition(ctx context.Context, collectionID typeutil.UniqueID, partitionIDs ...typeutil.UniqueID) error {
	err := m.catalog.ReleasePartition(ctx, collectionID, partitionIDs...)
	if err != nil {
		return err
	}
	partitions := m.collectionPartitions[collectionID]
	for _, id := range partitionIDs {
		delete(m.partitions, id)
		delete(partitions, id)
	}
	m.updateLoadMetrics()

	return nil
}

func (m *CollectionManager) UpdateReplicaNumber(ctx context.Context, collectionID typeutil.UniqueID, replicaNumber int32) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	collection, ok := m.collections[collectionID]
	if !ok {
		return merr.WrapErrCollectionNotFound(collectionID)
	}
	newCollection := collection.Clone()
	newCollection.ReplicaNumber = replicaNumber

	partitions := m.getPartitionsByCollection(collectionID)
	newPartitions := make([]*Partition, 0, len(partitions))
	for _, partition := range partitions {
		newPartition := partition.Clone()
		newPartition.ReplicaNumber = replicaNumber
		newPartitions = append(newPartitions, newPartition)
	}

	return m.putCollection(ctx, true, newCollection, newPartitions...)
}
