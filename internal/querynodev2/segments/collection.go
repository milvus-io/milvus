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

import (
	"encoding/base64"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CollectionManager interface {
	List() []int64
	ListWithName() map[int64]string
	Get(collectionID int64) *Collection
	PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) error
	Ref(collectionID int64, count uint32) bool
	// unref the collection,
	// returns true if the collection ref count goes 0, or the collection not exists,
	// return false otherwise
	Unref(collectionID int64, count uint32) bool
	// UpdateSchema updates the underlying collection schema of the provided collection.
	// schemaBarrierTs is the DDL/update barrier timestamp, not the logical schema
	// version. The manager derives the logical schema version from schema.Version
	// when a schema payload is present.
	UpdateSchema(collectionID int64, schema *schemapb.CollectionSchema, schemaBarrierTs uint64) error
}

type collectionManager struct {
	mut         sync.RWMutex
	collections map[int64]*Collection
}

type collectionSchemaUpdatePlan struct {
	// logicalSchemaVersion is schema.Version from the accepted schema payload.
	// It is the Go-side structural schema freshness key.
	logicalSchemaVersion uint64
	// schemaBarrierTs fences stale load results and orders same-version schema
	// payload refreshes such as collection property snapshots.
	schemaBarrierTs uint64
	// segcoreSchemaVersion is only passed to C++ segcore UpdateSchema. Segcore
	// still has a single increasing version gate, so QueryNode keeps this
	// independent counter after the Go-side freshness check accepts an update.
	segcoreSchemaVersion uint64
}

func NewCollectionManager() *collectionManager {
	return &collectionManager{
		collections: make(map[int64]*Collection),
	}
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

func (m *collectionManager) PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	logicalSchemaVersion := getLoadMetaSchemaVersion(schema, loadMeta)
	schemaBarrierTs := loadMeta.GetSchemaBarrierTs()
	if collection, ok := m.collections[collectionID]; ok {
		// Existing collections may be reached by a later load result or by a
		// same-version properties refresh. Keep the Go-side logical schema version
		// separate from the barrier timestamp so stale schema payloads cannot roll
		// back fields, while newer properties-only payloads can still refresh.
		if plan, shouldUpdate := prepareCollectionSchemaUpdate(collection, logicalSchemaVersion, schemaBarrierTs); shouldUpdate {
			if err := collection.ccollection.UpdateSchema(schema, plan.segcoreSchemaVersion); err != nil {
				return err
			}
			collection.setSchema(schema, plan.logicalSchemaVersion, plan.schemaBarrierTs, plan.segcoreSchemaVersion)
			log.Info("update collection schema",
				zap.Int64("collectionID", collectionID),
				zap.Uint64("schemaVersion", plan.logicalSchemaVersion),
				zap.Uint64("schemaBarrierTs", plan.schemaBarrierTs),
				zap.Uint64("segcoreSchemaVersion", plan.segcoreSchemaVersion),
				zap.Any("schema", schema),
			)
		}
		// Always update index meta to ensure newly indexed fields are visible
		// for search plan creation (CollectionIndexMeta::HasField check).
		if meta != nil {
			if err := collection.ccollection.UpdateIndexMeta(meta); err != nil {
				return err
			}
		}
		collection.Ref(1)
		return nil
	}

	log.Info("put new collection", zap.Int64("collectionID", collectionID), zap.Any("schema", schema))
	collection, err := NewCollection(collectionID, schema, meta, loadMeta)
	log.Info("new collection created", zap.Int64("collectionID", collectionID), zap.Any("schema", schema), zap.Error(err))
	if err != nil {
		return err
	}

	collection.Ref(1)
	m.collections[collectionID] = collection
	m.updateMetric()
	return nil
}

func (m *collectionManager) UpdateSchema(collectionID int64, schema *schemapb.CollectionSchema, schemaBarrierTs uint64) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	collection, ok := m.collections[collectionID]
	if !ok {
		return merr.WrapErrCollectionNotFound(collectionID, "collection not found in querynode collection manager")
	}

	logicalSchemaVersion := getUpdateSchemaVersion(schema, schemaBarrierTs)
	// A schema update carries two ordering domains:
	// - schema.Version is the logical collection schema version and prevents
	//   older schema payloads from overwriting newer fields/functions.
	// - schemaBarrierTs is the DDL barrier timestamp and advances for
	//   properties-only schema snapshots such as ttl_field changes.
	plan, shouldUpdate := prepareCollectionSchemaUpdate(collection, logicalSchemaVersion, schemaBarrierTs)
	if !shouldUpdate {
		return nil
	}

	if err := collection.ccollection.UpdateSchema(schema, plan.segcoreSchemaVersion); err != nil {
		return err
	}
	collection.setSchema(schema, plan.logicalSchemaVersion, plan.schemaBarrierTs, plan.segcoreSchemaVersion)
	return nil
}

// ShouldUpdateCollectionSchema reports whether an UpdateSchema payload would
// change the collection snapshot. Callers that have side effects outside the
// collection manager use this to skip stale/no-op schema messages before those
// side effects run.
func ShouldUpdateCollectionSchema(collection *Collection, schema *schemapb.CollectionSchema, schemaBarrierTs uint64) bool {
	if collection == nil {
		return false
	}
	logicalSchemaVersion := getUpdateSchemaVersion(schema, schemaBarrierTs)
	_, shouldUpdate := prepareCollectionSchemaUpdate(collection, logicalSchemaVersion, schemaBarrierTs)
	return shouldUpdate
}

func prepareCollectionSchemaUpdate(collection *Collection, logicalSchemaVersion uint64, schemaBarrierTs uint64) (collectionSchemaUpdatePlan, bool) {
	_, currentVersion, currentBarrierTs, currentSegcoreSchemaVersion := collection.schemaSnapshotWithSegcoreSchemaVersion()
	// Never allow logical schema version rollback, even if the incoming message
	// has a larger timestamp. This preserves the fix for out-of-order schema
	// messages across replay/channel delivery.
	if logicalSchemaVersion < currentVersion {
		return collectionSchemaUpdatePlan{}, false
	}
	// For the same logical schema version, only a newer barrier can update the
	// payload. This is required for collection properties embedded in schema
	// snapshots because those updates do not necessarily bump schema.Version.
	if logicalSchemaVersion == currentVersion && schemaBarrierTs <= currentBarrierTs {
		return collectionSchemaUpdatePlan{}, false
	}

	appliedBarrierTs := schemaBarrierTs
	if appliedBarrierTs < currentBarrierTs {
		appliedBarrierTs = currentBarrierTs
	}
	return collectionSchemaUpdatePlan{
		logicalSchemaVersion: logicalSchemaVersion,
		schemaBarrierTs:      appliedBarrierTs,
		segcoreSchemaVersion: currentSegcoreSchemaVersion + 1,
	}, true
}

func getUpdateSchemaVersion(schema *schemapb.CollectionSchema, schemaBarrierTs uint64) uint64 {
	// QueryNode orders schema freshness by the logical collection schema version
	// when the schema payload is present. Version 0 is a valid initial schema
	// version, so presence of schema, not non-zero value, selects this path.
	if schema != nil {
		return uint64(schema.GetVersion())
	}
	// Compatibility fallback for old or malformed call paths without schema:
	// the only available ordering value is the barrier timestamp that used to be
	// consumed as this method's version argument.
	return schemaBarrierTs
}

// getLoadMetaSchemaVersion seeds a loaded collection's schema freshness version.
// Schema payload is the source of truth whenever it is present, including the
// valid initial collection schema version 0. The timestamp barrier in load meta
// is not a schema version; it is only used as a compatibility fallback for old
// call paths that can reach here without a schema payload.
func getLoadMetaSchemaVersion(schema *schemapb.CollectionSchema, loadMeta *querypb.LoadMetaInfo) uint64 {
	if schema != nil {
		return uint64(schema.GetVersion())
	}
	if loadMeta == nil {
		return 0
	}
	return loadMeta.GetSchemaBarrierTs()
}

func initialSegcoreSchemaVersion(logicalSchemaVersion uint64, schemaBarrierTs uint64) uint64 {
	// Seed from both domains for rolling/legacy compatibility. C++ creates the
	// initial CCollection schema from schema.Version, while older QueryNode code
	// used the barrier timestamp as the value passed to segcore. Starting at the
	// max keeps the first generated segcoreSchemaVersion above both possible
	// create-time domains; later updates advance this collection-local token
	// independently from both logical schema version and barrier timestamp.
	if schemaBarrierTs > logicalSchemaVersion {
		return schemaBarrierTs
	}
	return logicalSchemaVersion
}

func (m *collectionManager) updateMetric() {
	metrics.QueryNodeNumCollections.WithLabelValues(paramtable.GetStringNodeID()).Set(float64(len(m.collections)))
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
			// Run metrics cleanup in background; DeletePartialMatch is CPU-heavy and should not block Unref.
			nodeID := paramtable.GetNodeID()
			go metrics.CleanupQueryNodeCollectionMetrics(nodeID, collectionID)
			m.updateMetric()
			return true
		}
		return false
	}

	return true
}

type collectionSchemaSnapshot struct {
	schema               *schemapb.CollectionSchema
	logicalSchemaVersion uint64
	schemaBarrierTs      uint64
	// segcoreSchemaVersion is an internal monotonic version passed to C++
	// segcore. It is not the logical collection schema version; Go-side schema
	// freshness is tracked by logicalSchemaVersion and schemaBarrierTs.
	segcoreSchemaVersion uint64
}

// Collection is a wrapper of the underlying C-structure C.CCollection
// In a query node, `Collection` is a replica info of a collection in these query node.
type Collection struct {
	mu            sync.RWMutex // protects colllectionPtr
	ccollection   *segcore.CCollection
	id            int64
	partitions    *typeutil.ConcurrentSet[int64]
	loadType      querypb.LoadType
	dbName        string
	dbProperties  []*commonpb.KeyValuePair
	resourceGroup string
	// resource group of node may be changed if node transfer,
	// but Collection in Manager will be released before assign new replica of new resource group on these node.
	// so we don't need to update resource group in Collection.
	// if resource group is not updated, the reference count of collection manager works failed.
	metricType atomic.String // deprecated
	schema     atomic.Pointer[collectionSchemaSnapshot]
	isGpuIndex bool
	loadFields typeutil.Set[int64]

	refCount *atomic.Uint32
}

// GetDBName returns the database name of collection.
func (c *Collection) GetDBName() string {
	return c.dbName
}

func (c *Collection) GetDBProperties() []*commonpb.KeyValuePair {
	return c.dbProperties
}

// GetResourceGroup returns the resource group of collection.
func (c *Collection) GetResourceGroup() string {
	return c.resourceGroup
}

// ID returns collection id
func (c *Collection) ID() int64 {
	return c.id
}

// GetCCollection returns the CCollection of collection
func (c *Collection) GetCCollection() *segcore.CCollection {
	return c.ccollection
}

func (c *Collection) setSchema(schema *schemapb.CollectionSchema, logicalSchemaVersion uint64, schemaBarrierTs uint64, segcoreSchemaVersion uint64) {
	c.schema.Store(&collectionSchemaSnapshot{
		schema:               schema,
		logicalSchemaVersion: logicalSchemaVersion,
		schemaBarrierTs:      schemaBarrierTs,
		segcoreSchemaVersion: segcoreSchemaVersion,
	})
}

func (c *Collection) SchemaSnapshot() (*schemapb.CollectionSchema, uint64, uint64) {
	schema, logicalSchemaVersion, schemaBarrierTs, _ := c.schemaSnapshotWithSegcoreSchemaVersion()
	return schema, logicalSchemaVersion, schemaBarrierTs
}

func (c *Collection) schemaSnapshotWithSegcoreSchemaVersion() (*schemapb.CollectionSchema, uint64, uint64, uint64) {
	snapshot := c.schema.Load()
	if snapshot == nil {
		return nil, 0, 0, 0
	}
	return snapshot.schema, snapshot.logicalSchemaVersion, snapshot.schemaBarrierTs, snapshot.segcoreSchemaVersion
}

func (c *Collection) SchemaAndVersion() (*schemapb.CollectionSchema, uint64) {
	schema, version, _ := c.SchemaSnapshot()
	return schema, version
}

// SchemaAndSegcoreVersion returns the schema with the monotonic version used
// by C++ segcore's schema apply gate. This is intentionally separate from
// SchemaAndVersion: Go-side freshness uses the logical schema version, while
// segcore segment reopen must stay in the same version domain as CCollection.
func (c *Collection) SchemaAndSegcoreVersion() (*schemapb.CollectionSchema, uint64) {
	schema, _, _, segcoreSchemaVersion := c.schemaSnapshotWithSegcoreSchemaVersion()
	return schema, segcoreSchemaVersion
}

// Schema returns the schema of collection
func (c *Collection) Schema() *schemapb.CollectionSchema {
	schema, _ := c.SchemaAndVersion()
	return schema
}

func (c *Collection) SchemaVersion() uint64 {
	_, version := c.SchemaAndVersion()
	return version
}

// IsGpuIndex returns a boolean value indicating whether the collection is using a GPU index.
func (c *Collection) IsGpuIndex() bool {
	return c.isGpuIndex
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

func (c *Collection) Ref(count uint32) uint32 {
	refCount := c.refCount.Add(count)
	putOrUpdateStorageContext(c.Schema().GetProperties(), c.ID())
	return refCount
}

func (c *Collection) Unref(count uint32) uint32 {
	refCount := c.refCount.Sub(count)
	return refCount
}

// newCollection returns a new Collection
func NewCollection(collectionID int64, schema *schemapb.CollectionSchema, indexMeta *segcorepb.CollectionIndexMeta, loadMetaInfo *querypb.LoadMetaInfo) (*Collection, error) {
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
		for _, structArrayField := range loadSchema.GetStructArrayFields() {
			for _, subField := range structArrayField.GetFields() {
				loadFieldIDs.Insert(subField.GetFieldID())
			}
		}
	}

	isGpuIndex := false
	req := &segcore.CreateCCollectionRequest{
		Schema:        loadSchema,
		LoadFieldList: loadFieldIDs.Collect(),
	}
	if indexMeta != nil && len(indexMeta.GetIndexMetas()) > 0 && indexMeta.GetMaxIndexRowCount() > 0 {
		req.IndexMeta = indexMeta
		for _, indexMeta := range indexMeta.GetIndexMetas() {
			isGpuIndex = gpuIndexRequiresGpu(indexMeta.GetIndexParams())
			if isGpuIndex {
				break
			}
		}
	}

	ccollection, err := segcore.CreateCCollection(req)
	if err != nil {
		log.Warn("create collection failed", zap.Error(err))
		return nil, err
	}
	coll := &Collection{
		ccollection:   ccollection,
		id:            collectionID,
		partitions:    typeutil.NewConcurrentSet[int64](),
		loadType:      loadMetaInfo.GetLoadType(),
		dbName:        loadMetaInfo.GetDbName(),
		dbProperties:  loadMetaInfo.GetDbProperties(),
		resourceGroup: loadMetaInfo.GetResourceGroup(),
		refCount:      atomic.NewUint32(0),
		isGpuIndex:    isGpuIndex,
		loadFields:    loadFieldIDs,
	}
	for _, partitionID := range loadMetaInfo.GetPartitionIDs() {
		coll.partitions.Insert(partitionID)
	}
	logicalSchemaVersion := getLoadMetaSchemaVersion(schema, loadMetaInfo)
	schemaBarrierTs := loadMetaInfo.GetSchemaBarrierTs()
	coll.setSchema(schema, logicalSchemaVersion, schemaBarrierTs, initialSegcoreSchemaVersion(logicalSchemaVersion, schemaBarrierTs))

	return coll, nil
}

// Only for test
func NewTestCollection(collectionID int64, loadType querypb.LoadType, schema *schemapb.CollectionSchema) *Collection {
	col := &Collection{
		id:         collectionID,
		partitions: typeutil.NewConcurrentSet[int64](),
		loadType:   loadType,
		refCount:   atomic.NewUint32(0),
	}
	col.setSchema(schema, 0, 0, initialSegcoreSchemaVersion(0, 0))
	return col
}

// new collection without segcore prepare
// ONLY FOR TEST
func NewCollectionWithoutSegcoreForTest(collectionID int64, schema *schemapb.CollectionSchema) *Collection {
	coll := &Collection{
		id:         collectionID,
		partitions: typeutil.NewConcurrentSet[int64](),
		refCount:   atomic.NewUint32(0),
	}
	logicalSchemaVersion := uint64(schema.GetVersion())
	coll.setSchema(schema, logicalSchemaVersion, 0, initialSegcoreSchemaVersion(logicalSchemaVersion, 0))
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

	if hookutil.IsClusterEncryptionEnabled() {
		ez := hookutil.GetEzByCollProperties(collection.Schema().GetProperties(), collection.ID())
		if ez != nil {
			if err := segcore.UnRefPluginContext(ez); err != nil {
				log.Error("failed to unref plugin context", zap.Int64("collectionID", collection.ID()), zap.Error(err))
			}
		}
	}

	if collection.ccollection == nil {
		return
	}
	collection.ccollection.Release()
	collection.ccollection = nil
}

func putOrUpdateStorageContext(properties []*commonpb.KeyValuePair, collectionID int64) {
	if hookutil.IsClusterEncryptionEnabled() {
		ez := hookutil.GetEzByCollProperties(properties, collectionID)
		if ez != nil {
			key := hookutil.GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
			err := segcore.PutOrRefPluginContext(ez, base64.StdEncoding.EncodeToString(key))
			if err != nil {
				log.Error("failed to put or update plugin context", zap.Int64("collectionID", collectionID), zap.Error(err))
			}
		}
	}
}
