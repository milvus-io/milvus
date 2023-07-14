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

type CollectionManager interface {
	Get(collectionID int64) *Collection
	PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo)
	Ref(collectionID int64, count uint32) bool
	// unref the collection,
	// returns true if the collection ref count goes 0, or the collection not exists,
	// return false otherwise
	Unref(collectionID int64, count uint32) bool
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

func (m *collectionManager) PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) {
	m.mut.Lock()
	defer m.mut.Unlock()

	if collection, ok := m.collections[collectionID]; ok {
		collection.Ref(1)
		return
	}

	collection := NewCollection(collectionID, schema, meta, loadMeta.GetLoadType())
	collection.metricType.Store(loadMeta.GetMetricType())
	collection.AddPartition(loadMeta.GetPartitionIDs()...)
	collection.Ref(1)
	m.collections[collectionID] = collection
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
			log.Info("release collection due to ref count to 0", zap.Int64("collectionID", collectionID))
			delete(m.collections, collectionID)
			DeleteCollection(collection)
			return true
		}
		return false
	}

	return true
}

// Collection is a wrapper of the underlying C-structure C.CCollection
type Collection struct {
	mu            sync.RWMutex // protects colllectionPtr
	collectionPtr C.CCollection
	id            int64
	partitions    *typeutil.ConcurrentSet[int64]
	loadType      querypb.LoadType
	metricType    atomic.String
	schema        *schemapb.CollectionSchema

	refCount *atomic.Uint32
}

// ID returns collection id
func (c *Collection) ID() int64 {
	return c.id
}

// Schema returns the schema of collection
func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema
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

func (c *Collection) Ref(count uint32) uint32 {
	refCount := c.refCount.Add(count)
	log.Debug("collection ref increment",
		zap.Int64("collectionID", c.ID()),
		zap.Uint32("refCount", refCount),
	)
	return refCount
}

func (c *Collection) Unref(count uint32) uint32 {
	refCount := c.refCount.Sub(count)
	log.Debug("collection ref decrement",
		zap.Int64("collectionID", c.ID()),
		zap.Uint32("refCount", refCount),
	)
	return refCount
}

// newCollection returns a new Collection
func NewCollection(collectionID int64, schema *schemapb.CollectionSchema, indexMeta *segcorepb.CollectionIndexMeta, loadType querypb.LoadType) *Collection {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);
	*/
	schemaBlob := proto.MarshalTextString(schema)
	cSchemaBlob := C.CString(schemaBlob)
	defer C.free(unsafe.Pointer(cSchemaBlob))

	collection := C.NewCollection(cSchemaBlob)

	if indexMeta != nil && len(indexMeta.GetIndexMetas()) > 0 && indexMeta.GetMaxIndexRowCount() > 0 {
		indexMetaBlob := proto.MarshalTextString(indexMeta)
		cIndexMetaBlob := C.CString(indexMetaBlob)
		C.SetIndexMeta(collection, cIndexMetaBlob)
	}

	return &Collection{
		collectionPtr: collection,
		id:            collectionID,
		schema:        schema,
		partitions:    typeutil.NewConcurrentSet[int64](),
		loadType:      loadType,
		refCount:      atomic.NewUint32(0),
	}
}

func NewCollectionWithoutSchema(collectionID int64, loadType querypb.LoadType) *Collection {
	return &Collection{
		id:         collectionID,
		partitions: typeutil.NewConcurrentSet[int64](),
		loadType:   loadType,
		refCount:   atomic.NewUint32(0),
	}
}

// deleteCollection delete collection and free the collection memory
func DeleteCollection(collection *Collection) {
	/*
		void
		deleteCollection(CCollection collection);
	*/
	cPtr := collection.collectionPtr
	C.DeleteCollection(cPtr)

	collection.collectionPtr = nil
}
