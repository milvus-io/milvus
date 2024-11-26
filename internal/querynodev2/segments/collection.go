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

package segments

/*
#cgo pkg-config: milvus_core

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
*/
import "C"

import (
	"sync"
	"unsafe"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type CollectionManager interface {
	List() []int64
	ListWithName() map[int64]string
	Get(collectionID int64) *Collection
	PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo)
	Ref(collectionID int64, count uint32) bool
	// unref the collection,
	// returns true if the collection ref count goes 0, or the collection not exists,
	// return false otherwise
	Unref(collectionID int64, count uint32) bool

	// addPartitionID would add a loaded partition id to partition id list of collection
	AddPartition(collectionID int64, partitions ...int64)
	// removePartitionID removes the partition id from partition id list of collection
	RemovePartition(collectionID int64, partition ...int64)
	// getPartitionIDs return loaded partitionIDs of collection
	GetPartitions(collectionID int64) []int64
}

type collectionManager struct {
	mut              sync.RWMutex
	collections      map[int64]*Collection
	loadedPartitions map[int64][]int64
}

func NewCollectionManager() *collectionManager {
	return &collectionManager{
		collections:      make(map[int64]*Collection),
		loadedPartitions: make(map[int64][]int64),
	}
}

// getPartitionIDs return partitionIDs of collection
func (m *collectionManager) GetPartitions(collectionID int64) []int64 {
	m.mut.RLock()
	defer m.mut.RUnlock()
	return m.loadedPartitions[collectionID]
}

// addPartitionID would add a partition id to partition id list of collection
func (m *collectionManager) AddPartition(collectionID int64, partitions ...int64) {
	m.mut.Lock()
	defer m.mut.Unlock()
	_, ok := m.loadedPartitions[collectionID]
	if !ok {
		m.loadedPartitions[collectionID] = make([]int64, 0)
	}

	m.loadedPartitions[collectionID] = append(m.loadedPartitions[collectionID], partitions...)
}

// removePartitionID removes the partition id from partition id list of collection
func (m *collectionManager) RemovePartition(collectionID int64, partition ...int64) {
	m.mut.Lock()
	defer m.mut.Unlock()

	loadedPartitions, ok := m.loadedPartitions[collectionID]
	if !ok {
		log.Warn("collection not found", zap.Int64("collectionID", collectionID))
		return
	}

	deleteSet := typeutil.NewUniqueSet(partition...)
	ret := make([]int64, 0)
	for _, partitionID := range loadedPartitions {
		if !deleteSet.Contain(partitionID) {
			ret = append(ret, partitionID)
		}
	}
	m.loadedPartitions[collectionID] = ret
	log.Info("remove partitions", zap.Int64("collection", collectionID), zap.Int64s("partitions", partition))
}

func (m *collectionManager) List() []int64 {
	m.mut.RLock()
	defer m.mut.RUnlock()

	return lo.Keys(m.collections)
}

// return all collections by map id --> name
func (m *collectionManager) ListWithName() map[int64]string {
	m.mut.RLock()
	defer m.mut.RUnlock()

	return lo.MapValues(m.collections, func(coll *Collection, _ int64) string {
		return coll.Schema().GetName()
	})
}

func (m *collectionManager) Get(collectionID int64) *Collection {
	m.mut.RLock()
	defer m.mut.RUnlock()

	return m.collections[collectionID]
}

func (m *collectionManager) PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) {
	m.mut.Lock()
	defer m.mut.Unlock()

	if collection, ok := m.collections[collectionID]; ok {
		// the schema may be changed even the collection is loaded
		collection.schema.Store(schema)
		collection.Ref(1)
		return
	}

	log.Info("put new collection", zap.Int64("collectionID", collectionID), zap.Int64s("partitions", loadMeta.GetPartitionIDs()), zap.Any("schema", schema))
	collection := NewCollection(collectionID, schema, meta, loadMeta)
	collection.Ref(1)
	m.collections[collectionID] = collection

	if m.loadedPartitions[collectionID] == nil {
		m.loadedPartitions[collectionID] = make([]int64, 0)
	}
	m.loadedPartitions[collectionID] = append(m.loadedPartitions[collectionID], loadMeta.GetPartitionIDs()...)
}

func (m *collectionManager) Ref(collectionID int64, count uint32) bool {
	m.mut.Lock()
	defer m.mut.Unlock()

	if collection, ok := m.collections[collectionID]; ok {
		collection.Ref(count)
		return true
	}

	return false
}

func (m *collectionManager) Unref(collectionID int64, count uint32) bool {
	m.mut.Lock()
	defer m.mut.Unlock()

	if collection, ok := m.collections[collectionID]; ok {
		if collection.Unref(count) == 0 {
			log.Info("release collection due to ref count to 0",
				zap.Int64("nodeID", paramtable.GetNodeID()), zap.Int64("collectionID", collectionID))
			delete(m.collections, collectionID)
			DeleteCollection(collection)

			metrics.CleanupQueryNodeCollectionMetrics(paramtable.GetNodeID(), collectionID)
			return true
		}
		return false
	}

	return true
}

// Collection is a wrapper of the underlying C-structure C.CCollection
// In a query node, `Collection` is a replica info of a collection in these query node.
type Collection struct {
	mu            sync.RWMutex // protects colllectionPtr
	collectionPtr C.CCollection
	id            int64
	loadType      querypb.LoadType
	dbName        string
	resourceGroup string
	// resource group of node may be changed if node transfer,
	// but Collection in Manager will be released before assign new replica of new resource group on these node.
	// so we don't need to update resource group in Collection.
	// if resource group is not updated, the reference count of collection manager works failed.
	metricType atomic.String // deprecated
	schema     atomic.Pointer[schemapb.CollectionSchema]
	isGpuIndex bool
	loadFields typeutil.Set[int64]

	refCount *atomic.Uint32
}

// GetDBName returns the database name of collection.
func (c *Collection) GetDBName() string {
	return c.dbName
}

// GetResourceGroup returns the resource group of collection.
func (c *Collection) GetResourceGroup() string {
	return c.resourceGroup
}

// ID returns collection id
func (c *Collection) ID() int64 {
	return c.id
}

// Schema returns the schema of collection
func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema.Load()
}

// IsGpuIndex returns a boolean value indicating whether the collection is using a GPU index.
func (c *Collection) IsGpuIndex() bool {
	return c.isGpuIndex
}

// getLoadType get the loadType of collection, which is loadTypeCollection or loadTypePartition
func (c *Collection) GetLoadType() querypb.LoadType {
	return c.loadType
}

func (c *Collection) Ref(count uint32) uint32 {
	refCount := c.refCount.Add(count)
	log.Info("collection ref increment",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.Int64("collectionID", c.ID()),
		zap.Uint32("refCount", refCount),
	)
	return refCount
}

func (c *Collection) Unref(count uint32) uint32 {
	refCount := c.refCount.Sub(count)
	log.Info("collection ref decrement",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.Int64("collectionID", c.ID()),
		zap.Uint32("refCount", refCount),
	)
	return refCount
}

// newCollection returns a new Collection
func NewCollection(collectionID int64, schema *schemapb.CollectionSchema, indexMeta *segcorepb.CollectionIndexMeta, loadMetaInfo *querypb.LoadMetaInfo) *Collection {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);
	*/

	var loadFieldIDs typeutil.Set[int64]
	loadSchema := typeutil.Clone(schema)

	// if load fields is specified, do filtering logic
	// otherwise use all fields for backward compatibility
	if len(loadMetaInfo.GetLoadFields()) > 0 {
		loadFieldIDs = typeutil.NewSet(loadMetaInfo.GetLoadFields()...)
	} else {
		loadFieldIDs = typeutil.NewSet(lo.Map(loadSchema.GetFields(), func(field *schemapb.FieldSchema, _ int) int64 { return field.GetFieldID() })...)
	}

	schemaBlob, err := proto.Marshal(loadSchema)
	if err != nil {
		log.Warn("marshal schema failed", zap.Error(err))
		return nil
	}

	collection := C.NewCollection(unsafe.Pointer(&schemaBlob[0]), (C.int64_t)(len(schemaBlob)))

	isGpuIndex := false
	if indexMeta != nil && len(indexMeta.GetIndexMetas()) > 0 && indexMeta.GetMaxIndexRowCount() > 0 {
		indexMetaBlob, err := proto.Marshal(indexMeta)
		if err != nil {
			log.Warn("marshal index meta failed", zap.Error(err))
			return nil
		}
		C.SetIndexMeta(collection, unsafe.Pointer(&indexMetaBlob[0]), (C.int64_t)(len(indexMetaBlob)))

		for _, indexMeta := range indexMeta.GetIndexMetas() {
			isGpuIndex = lo.ContainsBy(indexMeta.GetIndexParams(), func(param *commonpb.KeyValuePair) bool {
				return param.Key == common.IndexTypeKey && vecindexmgr.GetVecIndexMgrInstance().IsGPUVecIndex(param.Value)
			})
			if isGpuIndex {
				break
			}
		}
	}

	coll := &Collection{
		collectionPtr: collection,
		id:            collectionID,
		loadType:      loadMetaInfo.GetLoadType(),
		dbName:        loadMetaInfo.GetDbName(),
		resourceGroup: loadMetaInfo.GetResourceGroup(),
		refCount:      atomic.NewUint32(0),
		isGpuIndex:    isGpuIndex,
		loadFields:    loadFieldIDs,
	}
	coll.schema.Store(schema)

	return coll
}

func NewCollectionWithoutSchema(collectionID int64, loadType querypb.LoadType) *Collection {
	return &Collection{
		id:       collectionID,
		loadType: loadType,
		refCount: atomic.NewUint32(0),
	}
}

// new collection without segcore prepare
// ONLY FOR TEST
func NewCollectionWithoutSegcoreForTest(collectionID int64, schema *schemapb.CollectionSchema) *Collection {
	coll := &Collection{
		id:       collectionID,
		refCount: atomic.NewUint32(0),
	}
	coll.schema.Store(schema)
	return coll
}

// deleteCollection delete collection and free the collection memory
func DeleteCollection(collection *Collection) {
	/*
		void
		deleteCollection(CCollection collection);
	*/
	collection.mu.Lock()
	defer collection.mu.Unlock()

	cPtr := collection.collectionPtr
	if cPtr != nil {
		C.DeleteCollection(cPtr)
	}

	collection.collectionPtr = nil
}
