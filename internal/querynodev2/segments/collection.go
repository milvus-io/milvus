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
	"context"
	"encoding/base64"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	// UpdateSchema updates the underlying collection schema of the provided
	// collection. Ordering is by schema.Version, the single monotonic schema version:
	// every real DDL (structural or property/ttl) bumps it, so a same-version payload
	// is a no-op.
	UpdateSchema(collectionID int64, schema *schemapb.CollectionSchema) error
	// UpdateIndexMeta refreshes the collection's CollectionIndexMeta WITHOUT advancing
	// the served schema. Used after segment reopen: reopen decodes newly-indexed
	// fields into the segments but must not advance the served schema (driven only by
	// the stream UpdateSchema), and the SyncType_UpdateVersion path for already-loaded
	// segments does not carry index info — so without this the CollectionIndexMeta
	// lacks the new field and search-plan creation fails its HasField check.
	UpdateIndexMeta(collectionID int64, meta *segcorepb.CollectionIndexMeta) error
}

type collectionManager struct {
	mut         sync.RWMutex
	collections map[int64]*Collection
}

type collectionSchemaUpdatePlan struct {
	// schemaVersion is schema.Version from the accepted schema payload. It is the
	// single monotonic schema version: the Go-side freshness key AND the value
	// passed straight to C++ segcore UpdateSchema (segcore gates on the same
	// schema.Version now that every DDL bumps it).
	schemaVersion uint64
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

// acquireCollectionLease keeps a collection alive after the manager lock is
// released. It intentionally bypasses Collection.Ref because a temporary
// lease must not refresh storage context or become an externally visible ref.
func (m *collectionManager) acquireCollectionLease(collectionID int64) (*Collection, bool) {
	m.mut.RLock()
	defer m.mut.RUnlock()

	collection, ok := m.collections[collectionID]
	if ok {
		collection.refCount.Inc()
	}
	return collection, ok
}

func (m *collectionManager) PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) error {
	schemaVersion := uint64(schema.GetVersion())

	if collection, ok := m.acquireCollectionLease(collectionID); ok {
		defer m.Unref(collectionID, 1)
		return m.putOrRefExisting(collectionID, collection, schema, meta, schemaVersion)
	}

	m.mut.Lock()
	if collection, ok := m.collections[collectionID]; ok {
		collection.refCount.Inc()
		m.mut.Unlock()
		defer m.Unref(collectionID, 1)
		return m.putOrRefExisting(collectionID, collection, schema, meta, schemaVersion)
	}
	defer m.mut.Unlock()

	mlog.Info(context.TODO(), "put new collection", mlog.Int64("collectionID", collectionID), mlog.Any("schema", schema))
	collection, err := NewCollection(collectionID, schema, meta, loadMeta)
	mlog.Info(context.TODO(), "new collection created", mlog.Int64("collectionID", collectionID), mlog.Any("schema", schema), mlog.Err(err))
	if err != nil {
		return err
	}

	collection.Ref(1)
	m.collections[collectionID] = collection
	m.updateMetric()
	return nil
}

func (m *collectionManager) putOrRefExisting(collectionID int64, collection *Collection, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, schemaVersion uint64) error {
	// Existing collections may be reached by a later load result. Order by
	// schema.Version so a stale schema payload cannot roll back fields; a
	// same-version payload is a no-op (every real change bumps schema.Version).
	plan, shouldUpdate, err := collection.applyLoadUpdate(schema, meta, schemaVersion)
	if err != nil {
		return err
	}
	if shouldUpdate {
		mlog.Info(context.TODO(), "update collection schema",
			mlog.Int64("collectionID", collectionID),
			mlog.Uint64("schemaVersion", plan.schemaVersion),
			mlog.Any("schema", schema),
		)
	}
	return nil
}

func (m *collectionManager) UpdateSchema(collectionID int64, schema *schemapb.CollectionSchema) error {
	collection, ok := m.acquireCollectionLease(collectionID)
	if !ok {
		return merr.WrapErrCollectionNotFound(collectionID, "collection not found in querynode collection manager")
	}
	defer m.Unref(collectionID, 1)

	// Ordering is by schema.Version, the single monotonic schema version: it
	// prevents older payloads from overwriting newer fields/functions and, because
	// every real DDL (structural or property/ttl) bumps it, a same-version payload
	// is a no-op.
	_, _, err := collection.applySchemaUpdate(schema, uint64(schema.GetVersion()))
	return err
}

func (m *collectionManager) UpdateIndexMeta(collectionID int64, meta *segcorepb.CollectionIndexMeta) error {
	collection, ok := m.acquireCollectionLease(collectionID)
	if !ok {
		return merr.WrapErrCollectionNotFound(collectionID, "collection not found in querynode collection manager")
	}
	defer m.Unref(collectionID, 1)
	// updateIndexMeta is a no-op when meta equals the current index meta, so a reopen
	// that did not change indexes is cheap; it never advances the served schema.
	return collection.updateIndexMeta(meta)
}

func prepareCollectionSchemaUpdate(collection *Collection, schemaVersion uint64) (collectionSchemaUpdatePlan, bool) {
	_, currentVersion := collection.SchemaAndVersion()
	// Order strictly by schema.Version. A lower version is a stale payload and must
	// be skipped (anti-rollback, out-of-order replay/channel delivery, #50364). An
	// equal version is a no-op: every real change bumps schema.Version now, so the
	// same version cannot carry new content. A higher version applies.
	if schemaVersion <= currentVersion {
		return collectionSchemaUpdatePlan{}, false
	}
	return collectionSchemaUpdatePlan{
		schemaVersion: schemaVersion,
	}, true
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
			mlog.Info(context.TODO(), "release collection due to ref count to 0",
				mlog.Int64("nodeID", paramtable.GetNodeID()), mlog.Int64("collectionID", collectionID))
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
	schema *schemapb.CollectionSchema
	// schemaVersion is schema.Version: the single monotonic schema version. It is
	// both the Go-side freshness key and the value passed to C++ segcore, which
	// gates on the same schema.Version.
	schemaVersion uint64
}

// Collection is a wrapper of the underlying C-structure C.CCollection
// In a query node, `Collection` is a replica info of a collection in these query node.
type Collection struct {
	mu                 sync.RWMutex // protects colllectionPtr
	schemaTransitionMu sync.RWMutex // serializes schema transitions with insert payload conversion and growing writes
	ccollection        *segcore.CCollection
	id                 int64
	partitions         *typeutil.ConcurrentSet[int64]
	loadType           querypb.LoadType
	dbName             string
	dbProperties       []*commonpb.KeyValuePair
	resourceGroup      string
	// resource group of node may be changed if node transfer,
	// but Collection in Manager will be released before assign new replica of new resource group on these node.
	// so we don't need to update resource group in Collection.
	// if resource group is not updated, the reference count of collection manager works failed.
	metricType atomic.String // deprecated
	// schema is the SERVED snapshot: the single ccollection schema used for
	// search-plan creation and the delegator's derived state (idfOracle, function
	// runner). It advances only via the stream UpdateSchema (or a full load). A
	// reopen does NOT advance it — the reopened segment carries its own
	// schema.Version explicitly from the load request.
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

func (c *Collection) NewSearchRequest(req *querypb.SearchRequest, placeholderGroup []byte) (*segcore.SearchRequest, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.ccollection == nil {
		return nil, merr.WrapErrServiceInternal("create search request on released collection")
	}
	return segcore.NewSearchRequest(c.ccollection, req, placeholderGroup)
}

func (c *Collection) NewRetrievePlan(req *querypb.QueryRequest) (*segcore.RetrievePlan, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.ccollection == nil {
		return nil, merr.WrapErrServiceInternal("create retrieve plan on released collection")
	}
	return segcore.NewRetrievePlan(
		c.ccollection,
		req.Req.GetSerializedExprPlan(),
		req.Req.GetMvccTimestamp(),
		req.Req.Base.GetMsgID(),
		req.Req.GetConsistencyLevel(),
		req.Req.GetCollectionTtlTimestamps(),
		req.Req.GetEntityTtlPhysicalTime(),
	)
}

func (c *Collection) CreateCSegment(req *segcore.CreateCSegmentRequest) (segcore.CSegment, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.ccollection == nil {
		return nil, merr.WrapErrServiceInternal("create segment on released collection")
	}
	req.Collection = c.ccollection
	return segcore.CreateCSegment(req)
}

func (c *Collection) updateIndexMeta(meta *segcorepb.CollectionIndexMeta) error {
	if meta == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ccollection == nil {
		return merr.WrapErrServiceInternal("update index meta on released collection")
	}
	if proto.Equal(c.ccollection.IndexMeta(), meta) {
		return nil
	}
	return c.ccollection.UpdateIndexMeta(meta)
}

func (c *Collection) updateSchema(schema *schemapb.CollectionSchema, version uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ccollection == nil {
		return merr.WrapErrServiceInternal("update schema on released collection")
	}
	return c.ccollection.UpdateSchema(schema, version)
}

func (c *Collection) applySchemaUpdate(schema *schemapb.CollectionSchema, schemaVersion uint64) (collectionSchemaUpdatePlan, bool, error) {
	c.lockSchemaTransitionForUpdate()
	defer c.unlockSchemaTransitionForUpdate()

	return c.applySchemaUpdateLocked(schema, schemaVersion)
}

func (c *Collection) applyLoadUpdate(schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, schemaVersion uint64) (collectionSchemaUpdatePlan, bool, error) {
	c.lockSchemaTransitionForUpdate()
	defer c.unlockSchemaTransitionForUpdate()

	plan, shouldUpdate, err := c.applySchemaUpdateLocked(schema, schemaVersion)
	if err != nil {
		return collectionSchemaUpdatePlan{}, false, err
	}
	// Always update index meta to ensure newly indexed fields are visible
	// for search plan creation (CollectionIndexMeta::HasField check).
	if err := c.updateIndexMeta(meta); err != nil {
		return collectionSchemaUpdatePlan{}, false, err
	}
	// The temporary manager lease keeps the collection alive while this update
	// waits. Publish the caller-visible ref only after the schema and index meta
	// that determine its storage context are applied.
	c.Ref(1)
	return plan, shouldUpdate, nil
}

func (c *Collection) applySchemaUpdateLocked(schema *schemapb.CollectionSchema, schemaVersion uint64) (collectionSchemaUpdatePlan, bool, error) {
	plan, shouldUpdate := prepareCollectionSchemaUpdate(c, schemaVersion)
	if !shouldUpdate {
		return collectionSchemaUpdatePlan{}, false, nil
	}
	// schema.Version is passed straight to C++ segcore, which gates on the same
	// monotonic schema.Version.
	if err := c.updateSchema(schema, plan.schemaVersion); err != nil {
		return collectionSchemaUpdatePlan{}, false, err
	}
	c.setSchema(schema, plan.schemaVersion)
	return plan, true, nil
}

func (c *Collection) lockSchemaTransitionForUpdate() {
	c.schemaTransitionMu.Lock()
}

func (c *Collection) unlockSchemaTransitionForUpdate() {
	c.schemaTransitionMu.Unlock()
}

// WithInsertSchemaTransition keeps payload conversion and growing writes in
// one schema epoch. A schema update cannot change the native collection until
// fn returns.
func (c *Collection) WithInsertSchemaTransition(fn func(schema *schemapb.CollectionSchema)) {
	c.schemaTransitionMu.RLock()
	defer c.schemaTransitionMu.RUnlock()

	fn(c.Schema())
}

func (c *Collection) setSchema(schema *schemapb.CollectionSchema, schemaVersion uint64) {
	c.schema.Store(&collectionSchemaSnapshot{
		schema:        schema,
		schemaVersion: schemaVersion,
	})
}

func (c *Collection) SchemaAndVersion() (*schemapb.CollectionSchema, uint64) {
	snapshot := c.schema.Load()
	if snapshot == nil {
		return nil, 0
	}
	return snapshot.schema, snapshot.schemaVersion
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
	mlog.Info(context.TODO(), "add partitions", mlog.Int64("collection", c.ID()), mlog.Int64s("partitions", partitions))
}

// removePartitionID removes the partition id from partition id list of collection
func (c *Collection) RemovePartition(partitionID int64) {
	c.partitions.Remove(partitionID)
	mlog.Info(context.TODO(), "remove partition", mlog.Int64("collection", c.ID()), mlog.Int64("partition", partitionID))
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
		mlog.Warn(context.TODO(), "create collection failed", mlog.Err(err))
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
	coll.setSchema(schema, uint64(schema.GetVersion()))

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
	col.setSchema(schema, uint64(schema.GetVersion()))
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
	coll.setSchema(schema, uint64(schema.GetVersion()))
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
				mlog.Error(context.TODO(), "failed to unref plugin context", mlog.Int64("collectionID", collection.ID()), mlog.Err(err))
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
				mlog.Error(context.TODO(), "failed to put or update plugin context", mlog.Int64("collectionID", collectionID), mlog.Err(err))
			}
		}
	}
}
