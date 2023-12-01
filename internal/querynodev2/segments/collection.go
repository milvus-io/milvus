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
#cgo pkg-config: milvus_segcore

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
*/
import "C"

import (
	"sync"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// CollectionManager manages all loaded collections,
// WARN: DO NOT add method to get the collection
type CollectionManager interface {
	Get(collectionID int64) *Collection
	Put(collection *Collection)
	Remove(collectionID int64)
}

type collectionManager struct {
	mut         sync.RWMutex
	collections map[int64]*Collection
}

func NewCollectionManager() *collectionManager {
	return &collectionManager{
		collections: make(map[int64]*Collection),
	}
}

func (m *collectionManager) Get(collectionID int64) *Collection {
	m.mut.RLock()
	defer m.mut.RUnlock()

	return m.collections[collectionID]
}

func (m *collectionManager) Put(collection *Collection) {
	m.mut.Lock()
	defer m.mut.Unlock()

	if old, ok := m.collections[collection.ID()]; ok {
		old.Release()
	}

	log.Info("yah01: put collection",
		zap.Int64("collectionID", collection.ID()),
		zap.String("metricType", collection.GetMetricType()),
		zap.Stack("stack"),
	)
	m.collections[collection.ID()] = collection
}

func (m *collectionManager) Remove(collectionID int64) {
	m.mut.Lock()
	defer m.mut.Unlock()

	if collection, ok := m.collections[collectionID]; ok {
		deleteCollection(collection._ptr)
		delete(m.collections, collectionID)
	}
}

// Collection is a wrapper of the underlying C-structure C.CCollection
type Collection struct {
	mu   sync.RWMutex  // guard all
	_ptr C.CCollection // DO NOT access this directly

	id         int64
	partitions *typeutil.ConcurrentSet[int64]
	loadType   querypb.LoadType
	metricType atomic.String
	schema     atomic.Pointer[schemapb.CollectionSchema]
	indexMeta  *segcorepb.CollectionIndexMeta
}

// ID returns collection id
func (c *Collection) ID() int64 {
	return c.id
}

// Schema returns the schema of collection
func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema.Load()
}

// getPartitionIDs return partitionIDs of collection
func (c *Collection) GetPartitions() []int64 {
	return c.partitions.Collect()
}

func (c *Collection) ExistPartition(partitionIDs ...int64) bool {
	return c.partitions.Contain(partitionIDs...)
}

// addPartitionID would add a partition id to partition id list of collection
func (c *Collection) AddPartition(partitions ...int64) {
	for i := range partitions {
		c.partitions.Insert(partitions[i])
	}
	log.Info("add partitions", zap.Int64("collection", c.ID()), zap.Int64s("partitions", partitions))
}

// removePartitionID removes the partition id from partition id list of collection
func (c *Collection) RemovePartition(partitionID int64) {
	c.partitions.Remove(partitionID)
	log.Info("remove partition", zap.Int64("collection", c.ID()), zap.Int64("partition", partitionID))
}

// getLoadType get the loadType of collection, which is loadTypeCollection or loadTypePartition
func (c *Collection) GetLoadType() querypb.LoadType {
	return c.loadType
}

func (c *Collection) SetMetricType(metricType string) {
	c.metricType.Store(metricType)
}

func (c *Collection) GetMetricType() string {
	return c.metricType.Load()
}

// Ptr returns the collection pointer, and a bool flag to indicate whether the pointer is the cached one.
// WARN: keep in mind to release the pointer if the flag is false
func (c *Collection) Ptr() (C.CCollection, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ptr := c._ptr

	// the collection was released after the caller retrieved it
	if ptr == nil {
		return newCCollection(c.schema.Load(), c.indexMeta), false
	}

	return ptr, true
}

func (c *Collection) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	deleteCollection(c._ptr)
	c._ptr = nil
}

func newCCollection(schema *schemapb.CollectionSchema, indexMeta *segcorepb.CollectionIndexMeta) C.CCollection {
	schemaBlob, err := proto.Marshal(schema)
	if err != nil {
		log.Warn("marshal schema failed", zap.Error(err))
		return nil
	}

	collection := C.NewCollection(unsafe.Pointer(&schemaBlob[0]), (C.int64_t)(len(schemaBlob)))
	if indexMeta != nil && len(indexMeta.GetIndexMetas()) > 0 && indexMeta.GetMaxIndexRowCount() > 0 {
		indexMetaBlob, err := proto.Marshal(indexMeta)
		if err != nil {
			log.Warn("marshal index meta failed", zap.Error(err))
			return nil
		}
		C.SetIndexMeta(collection, unsafe.Pointer(&indexMetaBlob[0]), (C.int64_t)(len(indexMetaBlob)))
	}

	return collection
}

// NewCollection returns a new Collection
func NewCollection(collectionID int64, schema *schemapb.CollectionSchema, indexMeta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) *Collection {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);
	*/
	coll := &Collection{
		_ptr:       newCCollection(schema, indexMeta),
		id:         collectionID,
		indexMeta:  indexMeta,
		partitions: typeutil.NewConcurrentSet[int64](),
		loadType:   loadMeta.GetLoadType(),
		metricType: atomic.String{},
		schema:     atomic.Pointer[schemapb.CollectionSchema]{},
	}
	coll.AddPartition(loadMeta.GetPartitionIDs()...)
	coll.schema.Store(schema)
	coll.metricType.Store(loadMeta.GetMetricType())

	return coll
}

func NewCollectionWithoutSchema(collectionID int64, loadType querypb.LoadType) *Collection {
	return &Collection{
		id:         collectionID,
		partitions: typeutil.NewConcurrentSet[int64](),
		loadType:   loadType,
	}
}

// deleteCollection delete collection and free the collection memory
func deleteCollection(collectionPtr C.CCollection) {
	/*
		void
		deleteCollection(CCollection collection);
	*/
	if collectionPtr != nil {
		C.DeleteCollection(collectionPtr)
	}
}
