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
	"fmt"
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
	PutOrRefWithSchemaState(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) (*CollectionSchemaState, error)
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
	// schemaBarrierTs fences stale load results independently from schema identity.
	schemaBarrierTs uint64
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
	state, err := m.PutOrRefWithSchemaState(collectionID, schema, meta, loadMeta)
	if state != nil {
		state.Release()
	}
	return err
}

// PutOrRefWithSchemaState updates the collection and returns the exact state
// carried by this load request. Repeated segment loads clone the already
// published state, so all segments from one load snapshot share the same native
// logical and effective-load schemas. A request older than the collection-wide
// aggregate still gets its own state because different vchannels may advance at
// different times.
func (m *collectionManager) PutOrRefWithSchemaState(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) (*CollectionSchemaState, error) {
	if collection, ok := m.acquireCollectionLease(collectionID); ok {
		defer m.Unref(collectionID, 1)
		return m.putOrRefExisting(collectionID, collection, schema, meta, loadMeta)
	}

	m.mut.Lock()
	if collection, ok := m.collections[collectionID]; ok {
		collection.refCount.Inc()
		m.mut.Unlock()
		defer m.Unref(collectionID, 1)
		return m.putOrRefExisting(collectionID, collection, schema, meta, loadMeta)
	}
	defer m.mut.Unlock()

	mlog.Info(context.TODO(), "put new collection", mlog.Int64("collectionID", collectionID), mlog.FieldSchema(schema))
	requestState, err := NewSchemaStateFromLoad(collectionID, schema, loadMeta)
	if err != nil {
		return nil, err
	}
	collectionState, err := requestState.Clone()
	if err != nil {
		requestState.Release()
		return nil, err
	}
	collection, err := newCollectionWithSchemaState(collectionID, collectionState, meta, loadMeta)
	mlog.Info(context.TODO(), "new collection created", mlog.Int64("collectionID", collectionID), mlog.FieldSchema(schema), mlog.Err(err))
	if err != nil {
		requestState.Release()
		return nil, err
	}

	collection.Ref(1)
	m.collections[collectionID] = collection
	m.updateMetric()
	return requestState, nil
}

func (m *collectionManager) putOrRefExisting(collectionID int64, collection *Collection, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) (*CollectionSchemaState, error) {
	requestState, plan, shouldUpdate, err := collection.applyLoadUpdate(schema, loadMeta, meta)
	if err != nil {
		return nil, err
	}
	if shouldUpdate {
		mlog.Info(context.TODO(), "update collection schema",
			mlog.Int64("collectionID", collectionID),
			mlog.Uint64("schemaVersion", plan.logicalSchemaVersion),
			mlog.Uint64("schemaBarrierTs", plan.schemaBarrierTs),
			mlog.FieldSchema(requestState.Schema()),
		)
	}
	return requestState, nil
}

func (m *collectionManager) UpdateSchema(collectionID int64, schema *schemapb.CollectionSchema, schemaBarrierTs uint64) error {
	collection, ok := m.acquireCollectionLease(collectionID)
	if !ok {
		return merr.WrapErrCollectionNotFound(collectionID, "collection not found in querynode collection manager")
	}
	defer m.Unref(collectionID, 1)

	schema = typeutil.Clone(schema)
	logicalSchemaVersion := getUpdateSchemaVersion(schema, schemaBarrierTs)
	// A schema update carries two ordering domains:
	// - schema.Version is the logical collection schema version and prevents
	//   older schema payloads from overwriting newer fields/functions.
	// - schemaBarrierTs is the DDL barrier timestamp and advances visibility.
	_, _, err := collection.applySchemaUpdate(schema, logicalSchemaVersion, schemaBarrierTs)
	return err
}

func prepareCollectionSchemaUpdate(collection *Collection, logicalSchemaVersion uint64, schemaBarrierTs uint64) (collectionSchemaUpdatePlan, bool) {
	_, currentVersion, currentBarrierTs := collection.SchemaSnapshot()
	// Never allow logical schema version rollback, even if the incoming message
	// has a larger timestamp. This preserves the fix for out-of-order schema
	// messages across replay/channel delivery.
	if logicalSchemaVersion < currentVersion {
		return collectionSchemaUpdatePlan{}, false
	}
	// An equal-version event may advance the DDL barrier, but it cannot replace
	// the logical schema identified by that version.
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
	}, true
}

// prepareCollectionLoadUpdate accepts an equal version/barrier because load
// fields and effective mmap/warmup policy can change without a RootCoord schema
// timestamp change. Only an actual version or barrier rollback is stale.
func prepareCollectionLoadUpdate(collection *Collection, logicalSchemaVersion uint64, schemaBarrierTs uint64) (collectionSchemaUpdatePlan, bool) {
	_, currentVersion, currentBarrierTs := collection.SchemaSnapshot()
	if logicalSchemaVersion < currentVersion ||
		(logicalSchemaVersion == currentVersion && schemaBarrierTs < currentBarrierTs) {
		return collectionSchemaUpdatePlan{}, false
	}
	if schemaBarrierTs < currentBarrierTs {
		schemaBarrierTs = currentBarrierTs
	}
	return collectionSchemaUpdatePlan{
		logicalSchemaVersion: logicalSchemaVersion,
		schemaBarrierTs:      schemaBarrierTs,
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

func getLogicalLoadSchema(loadSchema *schemapb.CollectionSchema, loadMeta *querypb.LoadMetaInfo) (*schemapb.CollectionSchema, bool) {
	if logicalSchema := loadMeta.GetLogicalSchema(); logicalSchema != nil {
		return logicalSchema, true
	}
	// Compatibility with an older QueryCoord. Its request-level schema may be
	// decorated with mmap/warmup policy, so it must stay on the legacy path and
	// must not enter the process-wide logical schema cache.
	return loadSchema, false
}

func getLoadFieldIDs(schema *schemapb.CollectionSchema, loadMeta *querypb.LoadMetaInfo) typeutil.Set[int64] {
	loadFields := typeutil.NewSet(loadMeta.GetLoadFields()...)
	// An empty set is segcore's canonical full-load representation. Normalize an
	// explicit list containing every loadable field to the same form so future
	// fields added by schema evolution are loaded as well.
	if loadFields.Len() == 0 || isFullLoad(schema, loadFields) {
		return typeutil.NewSet[int64]()
	}
	return loadFields
}

// isFullLoad recognizes both encodings QueryCoord can persist for full-load:
// an empty list expanded by QueryNode, and the concrete set of all fields that
// existed when the collection was loaded. System and skip-load fields do not
// participate in the decision.
func isFullLoad(schema *schemapb.CollectionSchema, loadFields typeutil.Set[int64]) bool {
	if schema == nil {
		return false
	}
	isLoaded := func(field *schemapb.FieldSchema) bool {
		if common.IsSystemField(field.GetFieldID()) {
			return true
		}
		shouldLoad, err := common.ShouldFieldBeLoaded(field.GetTypeParams())
		if err != nil {
			return loadFields.Contain(field.GetFieldID())
		}
		if !shouldLoad {
			return true
		}
		return loadFields.Contain(field.GetFieldID())
	}
	for _, field := range schema.GetFields() {
		if !isLoaded(field) {
			return false
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			if !isLoaded(field) {
				return false
			}
		}
	}
	return true
}

func equalFieldSets(left, right typeutil.Set[int64]) bool {
	return left.Len() == right.Len() && left.Contain(right.Collect()...)
}

var collectionLoadPolicyKeys = []string{
	common.MmapEnabledKey,
	common.WarmupScalarFieldKey,
	common.WarmupVectorFieldKey,
	common.WarmupScalarIndexKey,
	common.WarmupVectorIndexKey,
}

func keyValue(pairs []*commonpb.KeyValuePair, key string) (string, bool) {
	for _, pair := range pairs {
		if pair.GetKey() == key {
			return pair.GetValue(), true
		}
	}
	return "", false
}

func setKeyValue(pairs []*commonpb.KeyValuePair, key, value string) []*commonpb.KeyValuePair {
	for _, pair := range pairs {
		if pair.GetKey() == key {
			pair.Value = value
			return pairs
		}
	}
	return append(pairs, &commonpb.KeyValuePair{Key: key, Value: value})
}

// evolveLoadSchema applies the previous QueryCoord-only load policy to a new
// logical schema. Logical fields/functions/properties always come from next;
// only mmap and warmup settings that differed from the previous logical schema
// are carried forward.
func evolveLoadSchema(next *schemapb.CollectionSchema, previous *CollectionSchemaState) *schemapb.CollectionSchema {
	result := typeutil.Clone(next)
	if previous != nil && previous.logicalSchema != nil && previous.loadSchema != nil {
		for _, key := range collectionLoadPolicyKeys {
			loadValue, inLoad := keyValue(previous.loadSchema.GetProperties(), key)
			logicalValue, inLogical := keyValue(previous.logicalSchema.GetProperties(), key)
			nextValue, inNext := keyValue(result.GetProperties(), key)
			logicalChanged := inNext != inLogical || (inNext && nextValue != logicalValue)
			if inLoad && !logicalChanged && (!inLogical || loadValue != logicalValue) {
				result.Properties = setKeyValue(result.GetProperties(), key, loadValue)
			}
		}
	}
	materializeLoadPolicy(result)
	return result
}

func materializeLoadPolicy(schema *schemapb.CollectionSchema) {
	collectionMmap, hasCollectionMmap := common.IsMmapDataEnabled(schema.GetProperties()...)
	scalarWarmup, hasScalarWarmup := common.GetWarmupPolicyByKey(common.WarmupScalarFieldKey, schema.GetProperties()...)
	vectorWarmup, hasVectorWarmup := common.GetWarmupPolicyByKey(common.WarmupVectorFieldKey, schema.GetProperties()...)

	apply := func(field *schemapb.FieldSchema, parentParams []*commonpb.KeyValuePair) {
		if _, ok := keyValue(field.GetTypeParams(), common.MmapEnabledKey); !ok {
			if value, exists := keyValue(parentParams, common.MmapEnabledKey); exists {
				field.TypeParams = setKeyValue(field.GetTypeParams(), common.MmapEnabledKey, value)
			} else if hasCollectionMmap {
				field.TypeParams = setKeyValue(field.GetTypeParams(), common.MmapEnabledKey, fmt.Sprint(collectionMmap))
			}
		}

		if _, ok := keyValue(field.GetTypeParams(), common.WarmupKey); ok {
			return
		}
		if value, exists := keyValue(parentParams, common.WarmupKey); exists {
			field.TypeParams = setKeyValue(field.GetTypeParams(), common.WarmupKey, value)
		} else if typeutil.IsVectorType(field.GetDataType()) && hasVectorWarmup {
			field.TypeParams = setKeyValue(field.GetTypeParams(), common.WarmupKey, vectorWarmup)
		} else if !typeutil.IsVectorType(field.GetDataType()) && hasScalarWarmup {
			field.TypeParams = setKeyValue(field.GetTypeParams(), common.WarmupKey, scalarWarmup)
		}
	}

	for _, field := range schema.GetFields() {
		apply(field, nil)
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			apply(field, structField.GetTypeParams())
		}
	}
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

// CollectionSchemaState is one immutable collection schema snapshot. The
// logical schema and its native cache reference always describe the same
// (collectionID, schema.Version). LoadSchema and LoadFields are the effective
// per-load policy captured with that logical schema.
//
// A captured state owns SchemaRef and must be released by its caller.
type CollectionSchemaState struct {
	logicalSchema *schemapb.CollectionSchema
	loadSchema    *schemapb.CollectionSchema
	schemaRef     *segcore.SchemaRef
	loadSchemaRef *segcore.SchemaRef
	loadFields    typeutil.Set[int64]
	// entityTTLFieldID is query-runtime state ordered by schemaBarrierTs.  It
	// intentionally does not participate in the process-wide logical schema
	// cache key: altering ttl_field is a properties-only update and RootCoord
	// keeps schema.Version unchanged.
	entityTTLFieldID int64
	schemaBarrierTs  uint64
}

func (s *CollectionSchemaState) Schema() *schemapb.CollectionSchema {
	if s == nil {
		return nil
	}
	return s.logicalSchema
}

func (s *CollectionSchemaState) LoadSchema() *schemapb.CollectionSchema {
	if s == nil {
		return nil
	}
	return s.loadSchema
}

func (s *CollectionSchemaState) SchemaRef() *segcore.SchemaRef {
	if s == nil {
		return nil
	}
	return s.schemaRef
}

func (s *CollectionSchemaState) LoadSchemaRef() *segcore.SchemaRef {
	if s == nil {
		return nil
	}
	return s.loadSchemaRef
}

func (s *CollectionSchemaState) Version() uint64 {
	if s == nil || s.logicalSchema == nil {
		return 0
	}
	return uint64(s.logicalSchema.GetVersion())
}

func (s *CollectionSchemaState) BarrierTs() uint64 {
	if s == nil {
		return 0
	}
	return s.schemaBarrierTs
}

func (s *CollectionSchemaState) LoadFields() []int64 {
	if s == nil || s.loadFields == nil {
		return nil
	}
	return s.loadFields.Collect()
}

// EntityTTLFieldID returns the active entity TTL field, or -1 when entity TTL
// is disabled. Field IDs are non-negative, so -1 is also the C API sentinel.
func (s *CollectionSchemaState) EntityTTLFieldID() int64 {
	if s == nil {
		return -1
	}
	return s.entityTTLFieldID
}

func resolveEntityTTLFieldID(schema *schemapb.CollectionSchema) (int64, error) {
	if schema == nil {
		return -1, merr.WrapErrServiceInternalMsg("cannot resolve entity TTL field from a nil schema")
	}

	var fieldName string
	for _, property := range schema.GetProperties() {
		if property.GetKey() == common.CollectionTTLFieldKey {
			fieldName = property.GetValue()
			break
		}
	}
	if fieldName == "" {
		return -1, nil
	}

	field := typeutil.GetFieldByName(schema, fieldName)
	if field == nil || field.GetDataType() != schemapb.DataType_Timestamptz {
		// The schema payload is produced and validated by Milvus itself. Reaching
		// this branch is an internal protocol violation, not bad user input at
		// the QueryNode boundary.
		return -1, merr.WrapErrServiceInternalMsg("entity TTL field %q is missing or is not a TIMESTAMPTZ field", fieldName)
	}
	return field.GetFieldID(), nil
}

// Clone returns an independently owned reference to the same immutable state.
func (s *CollectionSchemaState) Clone() (*CollectionSchemaState, error) {
	if s == nil {
		return nil, merr.WrapErrParameterInvalidMsg("schema state is nil")
	}
	var schemaRef, loadSchemaRef *segcore.SchemaRef
	var err error
	if s.schemaRef != nil {
		schemaRef, err = s.schemaRef.Clone()
		if err != nil {
			return nil, err
		}
	}
	if s.loadSchemaRef != nil {
		loadSchemaRef, err = s.loadSchemaRef.Clone()
		if err != nil {
			schemaRef.Release()
			return nil, err
		}
	}
	return &CollectionSchemaState{
		logicalSchema:    s.logicalSchema,
		loadSchema:       s.loadSchema,
		schemaRef:        schemaRef,
		loadSchemaRef:    loadSchemaRef,
		loadFields:       s.loadFields,
		entityTTLFieldID: s.entityTTLFieldID,
		schemaBarrierTs:  s.schemaBarrierTs,
	}, nil
}

// NewSchemaStateFromLoad builds the exact logical schema and effective load
// policy carried by one LoadSegments request. The returned state is independent
// from later collection-wide updates and must be released by the caller.
func NewSchemaStateFromLoad(collectionID int64, loadSchema *schemapb.CollectionSchema, loadMeta *querypb.LoadMetaInfo) (*CollectionSchemaState, error) {
	if loadSchema == nil {
		return nil, merr.WrapErrParameterInvalidMsg("load schema is nil")
	}
	effectiveSchema := typeutil.Clone(loadSchema)
	logicalSchema, useSchemaCache := getLogicalLoadSchema(effectiveSchema, loadMeta)
	logicalSchema = typeutil.Clone(logicalSchema)
	loadFields := getLoadFieldIDs(effectiveSchema, loadMeta)
	barrierTs := loadMeta.GetSchemaBarrierTs()
	entityTTLFieldID, err := resolveEntityTTLFieldID(logicalSchema)
	if err != nil {
		return nil, err
	}
	var schemaRef *segcore.SchemaRef
	if useSchemaCache {
		schemaRef, err = segcore.AcquireSchemaRef(collectionID, logicalSchema)
	} else {
		schemaRef, err = segcore.AcquireLoadSchemaRef(logicalSchema, nil)
	}
	if err != nil {
		return nil, err
	}
	loadSchemaRef, err := segcore.AcquireLoadSchemaRef(effectiveSchema, loadFields.Collect())
	if err != nil {
		schemaRef.Release()
		return nil, err
	}

	return &CollectionSchemaState{
		logicalSchema:    logicalSchema,
		loadSchema:       effectiveSchema,
		schemaRef:        schemaRef,
		loadSchemaRef:    loadSchemaRef,
		loadFields:       loadFields,
		entityTTLFieldID: entityTTLFieldID,
		schemaBarrierTs:  barrierTs,
	}, nil
}

// NewSchemaStateForUpdate builds a collection state from a schema event while
// retaining QueryCoord's effective load policy.
func NewSchemaStateForUpdate(collectionID int64, schema *schemapb.CollectionSchema, schemaBarrierTs uint64, previous *CollectionSchemaState) (*CollectionSchemaState, error) {
	if schema == nil {
		return nil, merr.WrapErrParameterInvalidMsg("schema is nil")
	}
	incomingSchema := typeutil.Clone(schema)
	logicalSchema := incomingSchema
	logicalVersion := uint64(incomingSchema.GetVersion())
	if previous != nil && logicalVersion < previous.Version() {
		return nil, merr.WrapErrParameterInvalidMsg("schema version %d is older than current version %d", logicalVersion, previous.Version())
	}
	entityTTLFieldID, err := resolveEntityTTLFieldID(incomingSchema)
	if err != nil {
		return nil, err
	}
	// (collectionID, schema.Version) uniquely identifies the logical schema.
	// An equal-version event may advance only its barrier, so retain the exact
	// cached schema already owned by this vchannel.
	equalVersion := previous != nil && logicalVersion == previous.Version()
	if equalVersion {
		logicalSchema = previous.logicalSchema
	}
	// RootCoord keeps schema.Version unchanged for runtime/load properties such
	// as ttl_field and external source/spec. Keep the cached logical schema for
	// field semantics, but carry the incoming properties in the effective load
	// snapshot used by segment storage paths.
	loadSchema := evolveLoadSchema(incomingSchema, previous)
	loadFields := getLoadFieldIDs(logicalSchema, nil)
	if previous != nil {
		// Empty is the canonical full-load policy and therefore includes fields
		// introduced by this schema. A non-empty partial-load set remains fixed.
		loadFields = typeutil.NewSet(previous.loadFields.Collect()...)
		if previous.schemaBarrierTs > schemaBarrierTs {
			schemaBarrierTs = previous.schemaBarrierTs
		}
	}

	var schemaRef *segcore.SchemaRef
	if equalVersion && previous.schemaRef != nil {
		schemaRef, err = previous.schemaRef.Clone()
	} else {
		schemaRef, err = segcore.AcquireSchemaRef(collectionID, logicalSchema)
	}
	if err != nil {
		return nil, err
	}
	loadSchemaRef, err := segcore.AcquireLoadSchemaRef(loadSchema, loadFields.Collect())
	if err != nil {
		schemaRef.Release()
		return nil, err
	}
	return &CollectionSchemaState{
		logicalSchema:    logicalSchema,
		loadSchema:       loadSchema,
		schemaRef:        schemaRef,
		loadSchemaRef:    loadSchemaRef,
		loadFields:       loadFields,
		entityTTLFieldID: entityTTLFieldID,
		schemaBarrierTs:  schemaBarrierTs,
	}, nil
}

func (s *CollectionSchemaState) Release() {
	if s != nil && s.schemaRef != nil {
		s.schemaRef.Release()
		s.schemaRef = nil
	}
	if s != nil && s.loadSchemaRef != nil {
		s.loadSchemaRef.Release()
		s.loadSchemaRef = nil
	}
}

// Collection is a wrapper of the underlying C-structure C.CCollection
// In a query node, `Collection` is a replica info of a collection in these query node.
type Collection struct {
	mu            sync.RWMutex // protects colllectionPtr
	schemaMu      sync.RWMutex // protects schema-state clone, swap, and handle release
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
	schema     atomic.Pointer[CollectionSchemaState]
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
	c.schemaMu.RLock()
	defer c.schemaMu.RUnlock()
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.ccollection == nil {
		return nil, merr.WrapErrServiceInternal("create search request on released collection")
	}
	state := c.schema.Load()
	if state == nil || state.Schema() == nil {
		return nil, merr.WrapErrServiceInternal("collection schema is unavailable")
	}
	return segcore.NewSearchRequestWithSchema(
		c.ccollection,
		state.SchemaRef(),
		state.Schema(),
		state.EntityTTLFieldID(),
		req,
		placeholderGroup,
	)
}

func (c *Collection) NewRetrievePlan(req *querypb.QueryRequest) (*segcore.RetrievePlan, error) {
	c.schemaMu.RLock()
	defer c.schemaMu.RUnlock()
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.ccollection == nil {
		return nil, merr.WrapErrServiceInternal("create retrieve plan on released collection")
	}
	state := c.schema.Load()
	if state == nil || state.Schema() == nil {
		return nil, merr.WrapErrServiceInternal("collection schema is unavailable")
	}
	return segcore.NewRetrievePlanWithSchema(
		c.ccollection,
		state.SchemaRef(),
		state.Schema(),
		state.EntityTTLFieldID(),
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

// UpdateIndexMeta refreshes only the native index metadata. It is used by
// compatibility load requests that do not carry a complete logical/effective
// schema pair and therefore cannot safely replace collection schema state.
func (c *Collection) UpdateIndexMeta(meta *segcorepb.CollectionIndexMeta) error {
	return c.updateIndexMeta(meta)
}

func (c *Collection) updateSchema(schema *schemapb.CollectionSchema, schemaRef *segcore.SchemaRef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ccollection == nil {
		return merr.WrapErrServiceInternal("update schema on released collection")
	}
	if schemaRef != nil {
		return c.ccollection.UpdateSchemaWithRef(schema, schemaRef)
	}
	return c.ccollection.UpdateSchema(schema)
}

func (c *Collection) applySchemaUpdate(schema *schemapb.CollectionSchema, logicalSchemaVersion uint64, schemaBarrierTs uint64) (collectionSchemaUpdatePlan, bool, error) {
	c.schemaMu.Lock()
	defer c.schemaMu.Unlock()

	return c.applySchemaUpdateLocked(schema, logicalSchemaVersion, schemaBarrierTs)
}

func (c *Collection) applyLoadUpdate(loadSchema *schemapb.CollectionSchema, loadMeta *querypb.LoadMetaInfo, meta *segcorepb.CollectionIndexMeta) (*CollectionSchemaState, collectionSchemaUpdatePlan, bool, error) {
	c.schemaMu.Lock()
	defer c.schemaMu.Unlock()

	if loadSchema == nil {
		return nil, collectionSchemaUpdatePlan{}, false, merr.WrapErrParameterInvalidMsg("load schema is nil")
	}
	logicalSchema, _ := getLogicalLoadSchema(loadSchema, loadMeta)
	logicalSchemaVersion := getLoadMetaSchemaVersion(logicalSchema, loadMeta)
	schemaBarrierTs := loadMeta.GetSchemaBarrierTs()
	loadFields := getLoadFieldIDs(loadSchema, loadMeta)
	current := c.schema.Load()
	plan, shouldUpdate := prepareCollectionLoadUpdate(c, logicalSchemaVersion, schemaBarrierTs)
	// Split load requests carry identical schema metadata for every segment.
	// Reuse the published state instead of parsing and retaining one complete
	// effective load schema per segment.
	if shouldUpdate && current != nil &&
		current.Version() == plan.logicalSchemaVersion &&
		current.schemaBarrierTs == plan.schemaBarrierTs &&
		proto.Equal(current.loadSchema, loadSchema) && equalFieldSets(current.loadFields, loadFields) {
		if err := c.updateIndexMeta(meta); err != nil {
			return nil, collectionSchemaUpdatePlan{}, false, err
		}
		requestState, err := current.Clone()
		if err != nil {
			return nil, collectionSchemaUpdatePlan{}, false, err
		}
		c.Ref(1)
		return requestState, plan, false, nil
	}

	requestState, err := NewSchemaStateFromLoad(c.id, loadSchema, loadMeta)
	if err != nil {
		return nil, collectionSchemaUpdatePlan{}, false, err
	}
	releaseRequestState := true
	defer func() {
		if releaseRequestState {
			requestState.Release()
		}
	}()

	if !shouldUpdate {
		// Index metadata can advance independently from the schema/load-policy
		// snapshot. The request state remains usable by an independently lagging
		// vchannel, but it must not roll back this aggregate collection state.
		if err := c.updateIndexMeta(meta); err != nil {
			return nil, collectionSchemaUpdatePlan{}, false, err
		}
		c.Ref(1)
		releaseRequestState = false
		return requestState, collectionSchemaUpdatePlan{}, false, nil
	}

	logicalChanged := current == nil || current.Version() != plan.logicalSchemaVersion
	// Equal-version/equal-barrier PutOrRef calls may refresh load policy but
	// must not roll back runtime DDL state. Only a newer barrier (or a new
	// logical schema version) may replace the active TTL field.
	if current != nil && !logicalChanged && plan.schemaBarrierTs <= current.schemaBarrierTs {
		requestState.entityTTLFieldID = current.entityTTLFieldID
	}
	requestState.schemaBarrierTs = plan.schemaBarrierTs
	if !logicalChanged && current != nil && current.schemaRef != nil {
		// Equal schema version can only refresh load metadata or its barrier. Keep the
		// logical proto paired with the exact native reference already published.
		schemaRef, err := current.schemaRef.Clone()
		if err != nil {
			return nil, collectionSchemaUpdatePlan{}, false, err
		}
		requestState.schemaRef.Release()
		requestState.schemaRef = schemaRef
		requestState.logicalSchema = current.logicalSchema
	}
	collectionState, err := requestState.Clone()
	if err != nil {
		return nil, collectionSchemaUpdatePlan{}, false, err
	}
	releaseCollectionState := true
	defer func() {
		if releaseCollectionState {
			collectionState.Release()
		}
	}()

	// Always update index meta to ensure newly indexed fields are visible
	// for search plan creation (CollectionIndexMeta::HasField check).
	if err := c.updateIndexMeta(meta); err != nil {
		return nil, collectionSchemaUpdatePlan{}, false, err
	}
	if logicalChanged {
		if err := c.updateSchema(collectionState.Schema(), collectionState.SchemaRef()); err != nil {
			return nil, collectionSchemaUpdatePlan{}, false, err
		}
	}
	c.setSchema(collectionState)
	releaseCollectionState = false
	// The temporary manager lease keeps the collection alive while this update
	// waits. Publish the caller-visible ref only after the schema and index meta
	// that determine its storage context are applied.
	c.Ref(1)
	releaseRequestState = false
	return requestState, plan, shouldUpdate, nil
}

func (c *Collection) applySchemaUpdateLocked(schema *schemapb.CollectionSchema, logicalSchemaVersion uint64, schemaBarrierTs uint64) (collectionSchemaUpdatePlan, bool, error) {
	if schema == nil {
		return collectionSchemaUpdatePlan{}, false, merr.WrapErrParameterInvalidMsg("schema is nil")
	}
	plan, shouldUpdate := prepareCollectionSchemaUpdate(c, logicalSchemaVersion, schemaBarrierTs)
	current := c.schema.Load()
	if !shouldUpdate {
		return collectionSchemaUpdatePlan{}, false, nil
	}
	state, err := NewSchemaStateForUpdate(c.id, schema, plan.schemaBarrierTs, current)
	if err != nil {
		return collectionSchemaUpdatePlan{}, false, err
	}
	if current == nil || state.Version() != current.Version() {
		if err := c.updateSchema(state.Schema(), state.SchemaRef()); err != nil {
			state.Release()
			return collectionSchemaUpdatePlan{}, false, err
		}
	}
	c.setSchema(state)
	return plan, true, nil
}

func (c *Collection) setSchema(state *CollectionSchemaState) {
	previous := c.schema.Swap(state)
	if previous != nil {
		previous.Release()
	}
}

func (c *Collection) SchemaSnapshot() (*schemapb.CollectionSchema, uint64, uint64) {
	snapshot := c.schema.Load()
	if snapshot == nil {
		return nil, 0, 0
	}
	return snapshot.logicalSchema, snapshot.Version(), snapshot.schemaBarrierTs
}

// CaptureSchemaState returns one self-contained schema/load-policy snapshot.
// Its native schema reference remains valid after later collection updates.
func (c *Collection) CaptureSchemaState() (*CollectionSchemaState, error) {
	c.schemaMu.RLock()
	defer c.schemaMu.RUnlock()
	return c.captureSchemaStateLocked()
}

func (c *Collection) captureSchemaStateLocked() (*CollectionSchemaState, error) {
	snapshot := c.schema.Load()
	if snapshot == nil || snapshot.logicalSchema == nil {
		return nil, merr.WrapErrServiceInternal("collection schema is unavailable")
	}
	return snapshot.Clone()
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
	state, err := NewSchemaStateFromLoad(collectionID, schema, loadMetaInfo)
	if err != nil {
		return nil, err
	}
	return newCollectionWithSchemaState(collectionID, state, indexMeta, loadMetaInfo)
}

// newCollectionWithSchemaState takes ownership of state, including on error.
func newCollectionWithSchemaState(collectionID int64, state *CollectionSchemaState, indexMeta *segcorepb.CollectionIndexMeta, loadMetaInfo *querypb.LoadMetaInfo) (*Collection, error) {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);
	*/
	if state == nil || state.Schema() == nil {
		if state != nil {
			state.Release()
		}
		return nil, merr.WrapErrParameterInvalidMsg("schema state is nil")
	}
	stateOwned := true
	defer func() {
		if stateOwned {
			state.Release()
		}
	}()

	isGpuIndex := false
	req := &segcore.CreateCCollectionRequest{
		CollectionID:  collectionID,
		Schema:        state.Schema(),
		SchemaRef:     state.SchemaRef(),
		LoadFieldList: state.LoadFields(),
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
	coll.setSchema(state)
	stateOwned = false

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
	loadFields := getLoadFieldIDs(schema, nil)
	col.setSchema(&CollectionSchemaState{
		logicalSchema: schema,
		loadSchema:    schema,
		loadFields:    loadFields,
	})
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
	loadFields := getLoadFieldIDs(schema, nil)
	coll.setSchema(&CollectionSchemaState{
		logicalSchema: schema,
		loadSchema:    schema,
		loadFields:    loadFields,
	})
	return coll
}

// deleteCollection delete collection and free the collection memory
func DeleteCollection(collection *Collection) {
	/*
		void
		deleteCollection(CCollection collection);
	*/
	collection.schemaMu.Lock()
	defer collection.schemaMu.Unlock()
	collection.mu.Lock()
	defer collection.mu.Unlock()
	defer func() {
		if snapshot := collection.schema.Swap(nil); snapshot != nil {
			snapshot.schemaRef.Release()
			snapshot.loadSchemaRef.Release()
		}
	}()

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
