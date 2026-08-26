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
	"github.com/milvus-io/milvus/pkg/v3/common"
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
	// schemaBarrierTs fences stale load results and orders same-version runtime
	// property and load-policy snapshots.
	schemaBarrierTs uint64
	// segcoreSchemaVersion is a collection-local runtime revision. It lets C++
	// replace a schema snapshot when runtime properties change without changing
	// the persisted logical schema version used by DataCoord materialization.
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
	loadSchema := schema
	logicalSchema := loadMeta.GetLogicalSchema()
	if logicalSchema == nil {
		// Rolling-upgrade and direct-test compatibility: older senders only carry
		// the effective load schema in the request-level schema field.
		logicalSchema = schema
	}
	logicalSchemaVersion := getLoadMetaSchemaVersion(logicalSchema, loadMeta)
	schemaBarrierTs := loadMeta.GetSchemaBarrierTs()
	loadFields := append([]int64(nil), loadMeta.GetLoadFields()...)

	if collection, ok := m.acquireCollectionLease(collectionID); ok {
		defer m.Unref(collectionID, 1)
		return m.putOrRefExisting(collectionID, collection, logicalSchema, loadSchema, meta, loadFields, logicalSchemaVersion, schemaBarrierTs)
	}

	m.mut.Lock()
	if collection, ok := m.collections[collectionID]; ok {
		collection.refCount.Inc()
		m.mut.Unlock()
		defer m.Unref(collectionID, 1)
		return m.putOrRefExisting(collectionID, collection, logicalSchema, loadSchema, meta, loadFields, logicalSchemaVersion, schemaBarrierTs)
	}
	defer m.mut.Unlock()

	mlog.Info(context.TODO(), "put new collection", mlog.Int64("collectionID", collectionID), mlog.FieldSchema(schema))
	collection, err := NewCollection(collectionID, loadSchema, meta, loadMeta)
	mlog.Info(context.TODO(), "new collection created", mlog.Int64("collectionID", collectionID), mlog.FieldSchema(schema), mlog.Err(err))
	if err != nil {
		return err
	}

	collection.Ref(1)
	m.collections[collectionID] = collection
	m.updateMetric()
	return nil
}

func (m *collectionManager) putOrRefExisting(collectionID int64, collection *Collection, logicalSchema *schemapb.CollectionSchema, loadSchema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadFields []int64, logicalSchemaVersion uint64, schemaBarrierTs uint64) error {
	plan, shouldUpdate, err := collection.applyLoadUpdate(logicalSchema, loadSchema, meta, loadFields, logicalSchemaVersion, schemaBarrierTs)
	if err != nil {
		return err
	}
	if shouldUpdate {
		mlog.Info(context.TODO(), "update collection schema",
			mlog.Int64("collectionID", collectionID),
			mlog.Uint64("schemaVersion", plan.logicalSchemaVersion),
			mlog.Uint64("schemaBarrierTs", plan.schemaBarrierTs),
			mlog.Uint64("segcoreSchemaVersion", plan.segcoreSchemaVersion),
			mlog.FieldSchema(logicalSchema),
		)
	}
	return nil
}

func (m *collectionManager) UpdateSchema(collectionID int64, schema *schemapb.CollectionSchema, schemaBarrierTs uint64) error {
	collection, ok := m.acquireCollectionLease(collectionID)
	if !ok {
		return merr.WrapErrCollectionNotFound(collectionID, "collection not found in querynode collection manager")
	}
	defer m.Unref(collectionID, 1)

	logicalSchemaVersion := getUpdateSchemaVersion(schema, schemaBarrierTs)
	// schema.Version orders immutable logical schemas. schemaBarrierTs also
	// orders same-version runtime property snapshots such as ttl_field.
	_, _, err := collection.applySchemaUpdate(schema, logicalSchemaVersion, schemaBarrierTs)
	return err
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
	if logicalSchemaVersion < currentVersion {
		return collectionSchemaUpdatePlan{}, false
	}
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
	schema               *schemapb.CollectionSchema
	loadPolicy           *schemapb.CollectionSchema
	loadFields           []int64
	logicalSchemaVersion uint64
	schemaBarrierTs      uint64
	segcoreSchemaVersion uint64
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
	schema     atomic.Pointer[collectionSchemaSnapshot]
	isGpuIndex bool

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
	c.schemaTransitionMu.RLock()
	defer c.schemaTransitionMu.RUnlock()

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.ccollection == nil {
		return nil, merr.WrapErrServiceInternal("create segment on released collection")
	}
	loadState := c.schema.Load()
	if loadState == nil {
		return nil, merr.WrapErrServiceInternal("create segment without collection schema")
	}
	req.Collection = c.ccollection
	req.LoadFields = append([]int64(nil), loadState.loadFields...)
	if req.SegmentType == segcore.SegmentTypeSealed {
		req.LoadSchema = loadState.loadPolicy
	} else {
		// Growing segments only need the persisted load metadata used by flush;
		// mmap and warmup policies apply to sealed loading and must not retain a
		// collection schema for every growing segment.
		req.LoadSchema = nil
	}
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

func (c *Collection) updateSchema(schema *schemapb.CollectionSchema, segcoreSchemaVersion uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ccollection == nil {
		return merr.WrapErrServiceInternal("update schema on released collection")
	}
	return c.ccollection.UpdateSchema(schema, segcoreSchemaVersion)
}

func (c *Collection) applySchemaUpdate(schema *schemapb.CollectionSchema, logicalSchemaVersion uint64, schemaBarrierTs uint64) (collectionSchemaUpdatePlan, bool, error) {
	c.lockSchemaTransitionForUpdate()
	defer c.unlockSchemaTransitionForUpdate()

	return c.applySchemaUpdateLocked(schema, nil, nil, logicalSchemaVersion, schemaBarrierTs)
}

func (c *Collection) applyLoadUpdate(logicalSchema *schemapb.CollectionSchema, loadSchema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadFields []int64, logicalSchemaVersion uint64, schemaBarrierTs uint64) (collectionSchemaUpdatePlan, bool, error) {
	c.lockSchemaTransitionForUpdate()
	defer c.unlockSchemaTransitionForUpdate()

	plan, shouldUpdate, err := c.applySchemaUpdateLocked(logicalSchema, loadSchema, loadFields, logicalSchemaVersion, schemaBarrierTs)
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

func (c *Collection) applySchemaUpdateLocked(schema *schemapb.CollectionSchema, loadSchema *schemapb.CollectionSchema, loadFields []int64, logicalSchemaVersion uint64, schemaBarrierTs uint64) (collectionSchemaUpdatePlan, bool, error) {
	plan, shouldUpdate := prepareCollectionSchemaUpdate(c, logicalSchemaVersion, schemaBarrierTs)
	if !shouldUpdate {
		return collectionSchemaUpdatePlan{}, false, nil
	}
	current := c.schema.Load()
	if current != nil && (loadSchema == nil || schemaBarrierTs < current.schemaBarrierTs) {
		loadSchema = current.loadPolicy
		loadFields = current.loadFields
	} else if loadSchema == nil {
		loadSchema = schema
	}
	if err := c.updateSchema(schema, plan.segcoreSchemaVersion); err != nil {
		return collectionSchemaUpdatePlan{}, false, err
	}
	c.setSchema(schema, loadSchema, loadFields, plan.logicalSchemaVersion, plan.schemaBarrierTs, plan.segcoreSchemaVersion)
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

func (c *Collection) setSchema(schema *schemapb.CollectionSchema, loadSchema *schemapb.CollectionSchema, loadFields []int64, logicalSchemaVersion uint64, schemaBarrierTs uint64, segcoreSchemaVersion uint64) {
	c.schema.Store(&collectionSchemaSnapshot{
		schema:               schema,
		loadPolicy:           loadPolicyFromSchema(loadSchema),
		loadFields:           append([]int64(nil), loadFields...),
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

// Schema returns the schema of collection
func (c *Collection) Schema() *schemapb.CollectionSchema {
	schema, _ := c.SchemaAndVersion()
	return schema
}

// LoadPolicy returns the compact mmap/warmup policy captured from QueryCoord's
// load schema. Logical schema updates preserve it until a newer load result
// replaces it.
func (c *Collection) LoadPolicy() *schemapb.CollectionSchema {
	snapshot := c.schema.Load()
	if snapshot == nil {
		return nil
	}
	return snapshot.loadPolicy
}

func (c *Collection) segmentLoadState() (*schemapb.CollectionSchema, *schemapb.CollectionSchema, []int64, uint64) {
	snapshot := c.schema.Load()
	if snapshot == nil {
		return nil, nil, nil, 0
	}
	return snapshot.schema, snapshot.loadPolicy, append([]int64(nil), snapshot.loadFields...), snapshot.segcoreSchemaVersion
}

// loadPolicyFromSchema keeps only the load settings consumed by segcore. The
// CollectionSchema type remains the wire carrier for compatibility, but names,
// fields, functions, and unrelated properties are never copied per segment.
func loadPolicyFromSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	if schema == nil {
		return nil
	}

	filterParams := func(params []*commonpb.KeyValuePair, collectionLevel bool) []*commonpb.KeyValuePair {
		filtered := make([]*commonpb.KeyValuePair, 0, len(params))
		for _, param := range params {
			if param == nil {
				continue
			}
			isWarmup := common.IsFieldWarmupKey(param.GetKey())
			if collectionLevel {
				isWarmup = common.IsCollectionWarmupKey(param.GetKey())
			}
			if param.GetKey() == common.MmapEnabledKey || isWarmup {
				filtered = append(filtered, proto.Clone(param).(*commonpb.KeyValuePair))
			}
		}
		return filtered
	}

	policy := &schemapb.CollectionSchema{
		Properties: filterParams(schema.GetProperties(), true),
	}
	for _, field := range schema.GetFields() {
		params := filterParams(field.GetTypeParams(), false)
		if len(params) > 0 {
			policy.Fields = append(policy.Fields, &schemapb.FieldSchema{
				FieldID:    field.GetFieldID(),
				TypeParams: params,
			})
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		compact := &schemapb.StructArrayFieldSchema{
			FieldID:    structField.GetFieldID(),
			TypeParams: filterParams(structField.GetTypeParams(), false),
		}
		for _, field := range structField.GetFields() {
			params := filterParams(field.GetTypeParams(), false)
			if len(params) > 0 {
				compact.Fields = append(compact.Fields, &schemapb.FieldSchema{
					FieldID:    field.GetFieldID(),
					TypeParams: params,
				})
			}
		}
		if len(compact.GetTypeParams()) > 0 || len(compact.GetFields()) > 0 {
			policy.StructArrayFields = append(policy.StructArrayFields, compact)
		}
	}
	return policy
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
func NewCollection(collectionID int64, loadSchema *schemapb.CollectionSchema, indexMeta *segcorepb.CollectionIndexMeta, loadMetaInfo *querypb.LoadMetaInfo) (*Collection, error) {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);
	*/

	logicalSchema := loadMetaInfo.GetLogicalSchema()
	if logicalSchema == nil {
		logicalSchema = loadSchema
	}
	logicalSchemaVersion := getLoadMetaSchemaVersion(logicalSchema, loadMetaInfo)
	schemaBarrierTs := loadMetaInfo.GetSchemaBarrierTs()
	segcoreSchemaVersion := initialSegcoreSchemaVersion(logicalSchemaVersion, schemaBarrierTs)
	isGpuIndex := false
	req := &segcore.CreateCCollectionRequest{
		CollectionID:  collectionID,
		SchemaVersion: segcoreSchemaVersion,
		Schema:        logicalSchema,
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
	}
	for _, partitionID := range loadMetaInfo.GetPartitionIDs() {
		coll.partitions.Insert(partitionID)
	}
	coll.setSchema(logicalSchema, loadSchema, loadMetaInfo.GetLoadFields(), logicalSchemaVersion, schemaBarrierTs, segcoreSchemaVersion)

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
	col.setSchema(schema, schema, nil, 0, 0, initialSegcoreSchemaVersion(0, 0))
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
	coll.setSchema(schema, schema, nil, logicalSchemaVersion, 0, initialSegcoreSchemaVersion(logicalSchemaVersion, 0))
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
