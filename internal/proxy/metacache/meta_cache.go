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

package metacache

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Cache is the interface for system metadata cache.
type Cache interface {
	// GetCollectionID get collection's id by name.
	GetCollectionID(ctx context.Context, database, collectionName string) (typeutil.UniqueID, error)
	// GetCollectionName get collection's name and database by id
	GetCollectionName(ctx context.Context, database string, collectionID int64) (string, error)
	// GetCollectionInfo get collection's information by name or collection id, such as Schema, and etc.
	GetCollectionInfo(ctx context.Context, database, collectionName string, collectionID int64) (*CollectionInfo, error)
	// GetPartitionID get partition's identifier of specific collection.
	GetPartitionID(ctx context.Context, database, collectionName string, partitionName string) (typeutil.UniqueID, error)
	// GetPartitionName get partition's name by id
	GetPartitionName(ctx context.Context, database, collectionName string, PartitionID int64) (string, error)
	// GetPartitions get all partitions' id of specific collection.
	GetPartitions(ctx context.Context, database, collectionName string) (map[string]typeutil.UniqueID, error)
	// GetPartitionInfo get partition's info.
	GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*PartitionInfo, error)
	// GetPartitionsIndex returns a partition names in partition key indexed order.
	GetPartitionsIndex(ctx context.Context, database, collectionName string) ([]string, error)
	// GetCollectionSchema get collection's Schema.
	GetCollectionSchema(ctx context.Context, database, collectionName string) (*SchemaInfo, error)
	// ResolveCollectionAlias returns the actual collection name if input is an alias,
	// or returns the input name if it's already a collection name.
	ResolveCollectionAlias(ctx context.Context, database, nameOrAlias string) (string, error)
	// GetShard(ctx context.Context, withCache bool, database, collectionName string, collectionID int64, channel string) ([]nodeInfo, error)
	// GetShardLeaderList(ctx context.Context, database, collectionName string, collectionID int64, withCache bool) ([]string, error)
	// DeprecateShardCache(database, collectionName string)
	// InvalidateShardLeaderCache(collections []int64)
	// ListShardLocation() map[int64]nodeInfo
	RemoveCollection(ctx context.Context, database, collectionName string)
	RemoveCollectionsByID(ctx context.Context, collectionID typeutil.UniqueID) []string
	// InvalidateCollectionMeta atomically applies the O(1) collection-cache
	// invalidations carried by one expiration request: resolution by real name
	// or alias, eviction by forwarded id, and optional alias-hint removal.
	// O(N) fallbacks such as RemoveAliasHolders remain separate operations.
	InvalidateCollectionMeta(ctx context.Context, database, collectionName string, collectionID typeutil.UniqueID, removeAlias bool) []string

	// RemoveAlias removes a cached alias entry.
	RemoveAlias(ctx context.Context, database, alias string)
	// RemoveAliasHolders evicts every cached entry of database whose declared
	// Aliases contain alias. FALLBACK for alias-DDL broadcasts that carry no
	// target id (id==0) -- i.e. when the old target could not be precisely
	// located. Two sources reach here, so it is a PERMANENT safety net, NOT
	// upgrade-window-only: (a) an old rootcoord that predates old_collection_id,
	// and (b) a new rootcoord that could not resolve the pre-alter AlterAlias
	// target (a meta inconsistency). Do not delete it as post-upgrade dead code.
	RemoveAliasHolders(ctx context.Context, database, alias string)
	// RemoveDatabase evicts every cached entry of the database. Ordering against
	// in-flight fills is handled by fillMu, so no version floor is needed.
	RemoveDatabase(ctx context.Context, database string)
	// RemoveDatabaseInfo invalidates only database-level metadata. AlterDatabase
	// changes database Properties but does not invalidate collection metadata, so
	// it must not pay RemoveDatabase's O(number of cached collections) cleanup.
	RemoveDatabaseInfo(ctx context.Context, database string)
	HasDatabase(ctx context.Context, database string) bool
	GetDatabaseInfo(ctx context.Context, database string) (*DatabaseInfo, error)
	// AllocID is only using on requests that need to skip timestamp allocation, don't overuse it.
	AllocID(ctx context.Context) (int64, error)

	RemovePartition(ctx context.Context, database string, collectionID typeutil.UniqueID, collectionName string, partitionName string)
	Close()
}

type CollectionInfo struct {
	CollID                typeutil.UniqueID
	DBID                  typeutil.UniqueID
	DBName                string
	Schema                *SchemaInfo
	CreatedTimestamp      uint64
	CreatedUtcTimestamp   uint64
	ConsistencyLevel      commonpb.ConsistencyLevel
	PartitionKeyIsolation bool
	QueryMode             string
	UpdateTimestamp       uint64
	CollectionTTL         uint64
	NumPartitions         int64
	VChannels             []string
	PChannels             []string
	ShardsNum             int32
	Aliases               []string
	Properties            []*commonpb.KeyValuePair
	// RoutingModulus is the modulus the shards' residues are taken against, and
	// ShardBy names the value that gets hashed. Both are zero/empty on a
	// collection that has never been split, which routes by hash % ShardsNum.
	RoutingModulus uint64
	ShardBy        string
	// RoutingTable is the collection's derived routing table. Built once here so
	// the write path does no per-request derivation. Nil when the collection has
	// never been split, and also when its routing meta is malformed -- the write
	// path then falls back to the legacy modulo rather than risk mis-routing.
	RoutingTable *routing.Table
}

type DatabaseInfo struct {
	DBID             typeutil.UniqueID
	Properties       []*commonpb.KeyValuePair
	CreatedTimestamp uint64
}

// SchemaInfo is a helper function wraps *schemapb.CollectionSchema
// with extra fields mapping and methods
type SchemaInfo struct {
	*schemapb.CollectionSchema
	FieldMap              *typeutil.ConcurrentMap[string, int64] // field name to id mapping
	HasPartitionKeyField  bool
	PkField               *schemapb.FieldSchema
	MultiAnalyzerFieldMap *typeutil.ConcurrentMap[int64, int64] // multi analzyer field id to dependent field id mapping
	SchemaHelper          *typeutil.SchemaHelper
}

func NewSchemaInfo(schema *schemapb.CollectionSchema) (*SchemaInfo, error) {
	fieldMap := typeutil.NewConcurrentMap[string, int64]()
	hasPartitionkey := false
	var pkField *schemapb.FieldSchema
	for _, field := range schema.GetFields() {
		fieldMap.Insert(field.GetName(), field.GetFieldID())
		if field.GetIsPartitionKey() {
			hasPartitionkey = true
		}
		if field.GetIsPrimaryKey() {
			pkField = field
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		fieldMap.Insert(structField.GetName(), structField.GetFieldID())
		for _, field := range structField.GetFields() {
			fieldMap.Insert(field.GetName(), field.GetFieldID())
		}
	}
	// skip load fields logic for now
	// partial load shall be processed as hint after tiered storage feature
	// A malformed Schema (nil, duplicate field name/id, multiple primary keys)
	// only reaches here on a RootCoord bug, so this is defensive; propagate it as
	// a retriable internal error rather than leaving SchemaHelper nil for a later
	// dereference to panic on.
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to create Schema helper")
	}
	return &SchemaInfo{
		CollectionSchema:      schema,
		FieldMap:              fieldMap,
		HasPartitionKeyField:  hasPartitionkey,
		PkField:               pkField,
		MultiAnalyzerFieldMap: typeutil.NewConcurrentMap[int64, int64](),
		SchemaHelper:          schemaHelper,
	}, nil
}

func (s *SchemaInfo) MapFieldID(name string) (int64, bool) {
	return s.FieldMap.Get(name)
}

func (s *SchemaInfo) IsPartitionKeyCollection() bool {
	return s.HasPartitionKeyField
}

func (s *SchemaInfo) GetPkField() (*schemapb.FieldSchema, error) {
	if s.PkField == nil {
		return nil, merr.WrapErrFieldNotFound("pk field")
	}
	return s.PkField, nil
}

func (s *SchemaInfo) GetMultiAnalyzerNameFieldID(id int64) (int64, error) {
	if id, ok := s.MultiAnalyzerFieldMap.Get(id); ok {
		return id, nil
	}

	field, err := s.SchemaHelper.GetFieldFromID(id)
	if err != nil {
		return 0, err
	}

	helper := typeutil.CreateFieldSchemaHelper(field)

	params, ok := helper.GetMultiAnalyzerParams()
	if !ok {
		s.MultiAnalyzerFieldMap.Insert(id, 0)
		return 0, nil
	}

	var raw map[string]json.RawMessage
	err = json.Unmarshal([]byte(params), &raw)
	if err != nil {
		return 0, err
	}

	jsonFieldID, ok := raw["by_field"]
	if !ok {
		return 0, merr.WrapErrServiceInternal("multi_analyzer_params missing required 'by_field' key")
	}
	var analyzerFieldName string
	err = json.Unmarshal(jsonFieldID, &analyzerFieldName)
	if err != nil {
		return 0, err
	}
	analyzerField, err := s.SchemaHelper.GetFieldFromName(analyzerFieldName)
	if err != nil {
		return 0, err
	}

	s.MultiAnalyzerFieldMap.Insert(id, analyzerField.GetFieldID())
	return analyzerField.GetFieldID(), nil
}

// GetLoadFieldIDs returns field id for load field list.
// If input `loadFields` is empty, use collection Schema definition.
// Otherwise, perform load field list constraint check then return field id.
func (s *SchemaInfo) GetLoadFieldIDs(loadFields []string, skipDynamicField bool) ([]int64, error) {
	if len(loadFields) == 0 {
		// skip check logic since create collection already did the rule check already
		return common.GetCollectionLoadFields(s.CollectionSchema, skipDynamicField), nil
	}

	fieldIDs := typeutil.NewSet[int64]()
	// fieldIDs := make([]int64, 0, len(loadFields))
	fields := make([]*schemapb.FieldSchema, 0, len(loadFields))
	for _, name := range loadFields {
		// todo(SpadeA): check struct field
		if structArrayField := s.SchemaHelper.GetStructArrayFieldFromName(name); structArrayField != nil {
			for _, field := range structArrayField.GetFields() {
				fields = append(fields, field)
				fieldIDs.Insert(field.GetFieldID())
			}
			continue
		}

		fieldSchema, err := s.SchemaHelper.GetFieldFromName(name)
		if err != nil {
			return nil, err
		}

		fields = append(fields, fieldSchema)
		fieldIDs.Insert(fieldSchema.GetFieldID())
	}

	// only append dynamic field when skipFlag == false
	if !skipDynamicField {
		// find dynamic field
		dynamicField := lo.FindOrElse(s.Fields, nil, func(field *schemapb.FieldSchema) bool {
			return field.IsDynamic
		})

		// if dynamic field not nil
		if dynamicField != nil {
			fieldIDs.Insert(dynamicField.GetFieldID())
			fields = append(fields, dynamicField)
		}
	}

	// validate load fields list
	if err := s.validateLoadFields(loadFields, fields); err != nil {
		return nil, err
	}

	return fieldIDs.Collect(), nil
}

func (s *SchemaInfo) validateLoadFields(names []string, fields []*schemapb.FieldSchema) error {
	// ignore error if not found
	partitionKeyField, _ := s.SchemaHelper.GetPartitionKeyField()
	clusteringKeyField, _ := s.SchemaHelper.GetClusteringKeyField()

	var hasPrimaryKey, hasPartitionKey, hasClusteringKey, hasVector bool
	for _, field := range fields {
		if field.GetFieldID() == s.PkField.GetFieldID() {
			hasPrimaryKey = true
		}
		if typeutil.IsVectorType(field.GetDataType()) {
			hasVector = true
		}
		if field.IsPartitionKey {
			hasPartitionKey = true
		}
		if field.IsClusteringKey {
			hasClusteringKey = true
		}
	}

	if !hasPrimaryKey {
		return merr.WrapErrParameterInvalidMsg("load field list %v does not contain primary key field %s", names, s.PkField.GetName())
	}
	if !hasVector {
		return merr.WrapErrParameterInvalidMsg("load field list %v does not contain vector field", names)
	}
	if partitionKeyField != nil && !hasPartitionKey {
		return merr.WrapErrParameterInvalidMsg("load field list %v does not contain partition key field %s", names, partitionKeyField.GetName())
	}
	if clusteringKeyField != nil && !hasClusteringKey {
		return merr.WrapErrParameterInvalidMsg("load field list %v does not contain clustering key field %s", names, clusteringKeyField.GetName())
	}
	return nil
}

func (s *SchemaInfo) CanRetrieveRawFieldData(field *schemapb.FieldSchema) bool {
	return s.SchemaHelper.CanRetrieveRawFieldData(field)
}

// PartitionInfos contains the cached collection partition informations.
type PartitionInfos struct {
	PartitionInfos        []*PartitionInfo
	Name2Info             map[string]*PartitionInfo // map[int64]*PartitionInfo
	Name2ID               map[string]int64          // map[int64]*PartitionInfo
	IndexedPartitionNames []string
}

// PartitionInfo single model for partition information.
type PartitionInfo struct {
	Name                string
	PartitionID         typeutil.UniqueID
	CreatedTimestamp    uint64
	CreatedUtcTimestamp uint64
	IsDefault           bool
}

func (info *CollectionInfo) isCollectionCached() bool {
	return info != nil && info.CollID != typeutil.UniqueID(0) && info.Schema != nil
}

// make sure MetaCache implements Cache.
var _ Cache = (*MetaCache)(nil)

// MetaCache implements Cache, provides collection meta cache based on internal RootCoord
type MetaCache struct {
	mixCoord types.MixCoordClient

	dbInfo map[string]*DatabaseInfo // database -> db_info

	// collections is the PRIMARY store and the single source of truth: an entry
	// is live if and only if its cluster-unique collection id is in this map --
	// liveness IS presence; every invalidation deletes explicitly through
	// evictCollectionEntryLocked, which removes the entry TOGETHER with
	// everything it owns (its name hint, its declared alias hints, its
	// partition list).
	collections map[typeutil.UniqueID]*CollectionInfo // collection id -> entry
	// nameIdx and aliasInfo are HINTS: they resolve a (db, name) or (db, alias)
	// to the primary store and are validated against it on every read (the id
	// must exist and the entry's identity -- name, database -- must still
	// match). A stale hint is at worst one extra describe, never a stale read.
	// Hints are written ONLY together with their entry (update(): the name, the
	// entry's declared Aliases) and evictions clean exactly that set -- so every
	// hint is discoverable from its target entry, nothing can dangle, and there
	// is NO background GC over these maps.
	nameIdx   map[string]map[string]typeutil.UniqueID // database -> real collection name -> id
	aliasInfo map[string]map[string]string            // database -> alias -> target real name (positive entries only)

	mu sync.RWMutex

	sfGlobal conc.Singleflight[*CollectionInfo]
	sfDB     conc.Singleflight[*DatabaseInfo]

	idStart int64
	idCount int64
	idIndex int64
	idLock  sync.RWMutex

	// testHookRemoveDatabaseMidWindow, when non-nil (tests only), runs between
	// RemoveDatabase's bucket drop (fill lock released) and its batched primary
	// walk -- the exact window where a racing fill can rebuild an entry with
	// fresh hints that the walk must then evict through the unified routine.
	testHookRemoveDatabaseMidWindow func()

	// fillMu orders cache fills against invalidations, replacing the previous
	// per-collection version floor (collectionCacheVersion). A fill holds the READ
	// side for the whole singleflight lifecycle: RPC, write-back, result delivery,
	// and removal of the completed flight. An invalidation takes the WRITE side,
	// so it drains every in-flight fill before evicting. Any fill that read a
	// pre-DDL snapshot has therefore finished serving its result BEFORE the
	// eviction runs (and its write-back is cleaned by it), and any fill issued
	// after the eviction reads post-DDL state (rootcoord commits before
	// broadcasting) -- no timestamp comparison needed, and it also covers
	// write-backs the floor could not (alias resolution entries, RequestTime==0
	// responses from older rootcoords).
	// Costs, accepted by design: an invalidation waits for the slowest in-flight
	// fill. Fill RPCs preserve the caller's context and do not impose a shorter
	// MetaCache-specific deadline, so callers must provide an appropriate
	// deadline (or rely on the coordinator client's transport policy). Fills
	// issued while an invalidation is pending briefly block behind it.
	// Cache HITS never touch this lock; fills don't block each other.
	// Lock order: fillMu before m.mu. Deadlock-free against rootcoord: ack
	// callbacks release the meta lock before the expiration fan-out, so a fill's
	// describe RPC is never blocked by the DDL whose invalidation is waiting on
	// that fill.
	fillMu sync.RWMutex

	// testHookBeforeSingleflightReturn, when non-nil (tests only), runs after a
	// collection fill has written back but before its singleflight callback
	// returns. fillMu.RLock is still held across this hook and the subsequent
	// singleflight cleanup.
	testHookBeforeSingleflightReturn func()

	// testHookInvalidateCollectionMetaMidMutation, when non-nil (tests only),
	// runs after the batch has evicted every resolved collection but before it
	// removes the explicit alias hint. Both fillMu and m.mu are still held.
	testHookInvalidateCollectionMetaMidMutation func()

	// partitionCache: collection id (as a string key) -> the collection's FULL
	// partition list (plus name->info map and the partition-key routing order).
	// One cache for both access shapes: by-name point lookups resolve through
	// Name2Info, and list consumers (partition-key routing, ShowPartitions) take
	// it whole -- the filler always fetched the full list anyway (ShowPartitions
	// has no name filter), so a separate per-partition cache duplicated this data.
	//
	// A plain map guarded by m.mu and ordered against invalidations by fillMu,
	// exactly like the primary store: a fill writes it back under fillMu.RLock,
	// an invalidation deletes it under fillMu.Lock after draining in-flight
	// fills. The list values are immutable once built and are handed out by
	// pointer (Go's GC keeps a returned list alive independent of map
	// membership), so no reference counting, stale-marker lifecycle, version
	// floor, or background reclamation is needed -- an invalidation's plain
	// delete is safe even while a request still routes over a list it already
	// read.
	partitionCache   map[string]*PartitionInfos
	sfPartitionCache conc.Singleflight[*PartitionInfos]

	stopCh    chan struct{}
	closeOnce sync.Once
}

// NewMetaCache creates a MetaCache with provided RootCoord and QueryNode
func NewMetaCache(mixCoord types.MixCoordClient) (*MetaCache, error) {
	metaCache := &MetaCache{
		mixCoord:       mixCoord,
		dbInfo:         map[string]*DatabaseInfo{},
		aliasInfo:      map[string]map[string]string{},
		collections:    map[typeutil.UniqueID]*CollectionInfo{},
		nameIdx:        map[string]map[string]typeutil.UniqueID{},
		partitionCache: map[string]*PartitionInfos{},
		stopCh:         make(chan struct{}),
		closeOnce:      sync.Once{},
	}
	return metaCache, nil
}

// liveLocked returns the primary entry for id. Liveness IS presence: every
// invalidation (including RemoveDatabase) deletes primary entries explicitly.
// Caller must hold m.mu (read or write).
func (m *MetaCache) liveLocked(collectionID typeutil.UniqueID) (*CollectionInfo, bool) {
	entry, ok := m.collections[collectionID]
	return entry, ok
}

func (m *MetaCache) getCollection(database, collectionName string, collectionID typeutil.UniqueID) (*CollectionInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if collectionName == "" {
		// By-id lookup: collection ids are cluster-unique and rootcoord ignores
		// the db name when describing by id, so serve straight from the primary.
		if entry, ok := m.liveLocked(collectionID); ok {
			return entry, entry.isCollectionCached()
		}
		return nil, false
	}

	database = normalizeDBName(database)
	// Name hint -> primary, validated: the entry must still carry this exact
	// name in this database (after a rename the old-name hint still points at
	// the live id but the name no longer matches -- it must miss; the hint is
	// removed when its owner is evicted, or overwritten by a same-name refill).
	// A failed validation is just a miss.
	if ids, ok := m.nameIdx[database]; ok {
		if id, ok := ids[collectionName]; ok {
			if entry, ok := m.liveLocked(id); ok &&
				entry.Schema.GetName() == collectionName && normalizeDBName(entry.DBName) == database {
				return entry, entry.isCollectionCached()
			}
		}
	}
	// Alias hint -> real name -> name hint -> primary, same validation chain.
	if aliasDB, ok := m.aliasInfo[database]; ok {
		if target, ok := aliasDB[collectionName]; ok && target != "" {
			if ids, ok := m.nameIdx[database]; ok {
				if id, ok := ids[target]; ok {
					if entry, ok := m.liveLocked(id); ok &&
						entry.Schema.GetName() == target && normalizeDBName(entry.DBName) == database {
						return entry, entry.isCollectionCached()
					}
				}
			}
		}
	}

	return nil, false
}

func (m *MetaCache) update(ctx context.Context, database, collectionName string, collectionID typeutil.UniqueID) (*CollectionInfo, error) {
	if collInfo, ok := m.getCollection(database, collectionName, collectionID); ok {
		return collInfo, nil
	}

	collection, err := m.describeCollection(ctx, database, collectionName, collectionID)
	if err != nil {
		return nil, err
	}

	realName := collection.Schema.GetName()
	// the caller may have addressed the collection by an alias; cache under the
	// real name (the alias hint comes from the declared-Aliases loop below)
	if collectionName == "" || (realName != "" && realName != collectionName) {
		collectionName = realName
	}
	isolation, err := common.IsPartitionKeyIsolationKvEnabled(collection.Properties...)
	if err != nil {
		return nil, err
	}
	queryMode := common.GetQueryMode(collection.Properties...)

	schemaInfo, err := NewSchemaInfo(collection.Schema)
	if err != nil {
		return nil, err
	}

	bucketDB := collection.GetDbName()
	if bucketDB == "" {
		if collectionID != 0 {
			// Old-rootcoord id-only describe: the response omits the db, so the
			// entry's database is unknown and NO database DDL could ever
			// reach it (no database event could ever name its db). Serve
			// the response uncached instead of inventing a compat mechanism for
			// an upgrade-window-only entry class -- repeat lookups just
			// re-describe until the rootcoords are upgraded.
			// Routing is attached here too, not only on the cached path below: an
			// entry served without it would route a split collection by the legacy
			// modulo, sending part of every write to a shard that does not own its
			// keys.
			return attachRouting(ctx, newCollectionInfo(collection, schemaInfo, isolation, queryMode), collection), nil
		}
		// By-name lookup: the request database is authoritative.
		bucketDB = normalizeDBName(database)
	}

	info := attachRouting(ctx, newCollectionInfo(collection, schemaInfo, isolation, queryMode), collection)
	if info.DBName == "" {
		info.DBName = bucketDB // authoritative for by-name fills
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// A fill can observe a rename before the rename broadcast arrives: the same
	// id re-fills under a new canonical (database, name) location. Delete the
	// previous name hint when either coordinate changed so nothing dangles (no
	// background GC exists to catch it later).
	if prev, ok := m.collections[collection.GetCollectionID()]; ok && prev.Schema != nil {
		prevName := prev.Schema.GetName()
		prevDB := normalizeDBName(prev.DBName)
		if prevName != "" && (prevDB != bucketDB || prevName != collectionName) {
			if ids, ok := m.nameIdx[prevDB]; ok && ids[prevName] == collection.GetCollectionID() {
				delete(ids, prevName)
				if len(ids) == 0 {
					delete(m.nameIdx, prevDB)
				}
			}
		}
	}
	m.collections[collection.GetCollectionID()] = info

	if bucketDB != "" {
		// name hint; overwrites any stale hint for this name (rename, or a
		// drop+recreate reusing it) -- old hints self-invalidate on read
		ids, ok := m.nameIdx[bucketDB]
		if !ok {
			ids = make(map[string]typeutil.UniqueID)
			m.nameIdx[bucketDB] = ids
		}
		ids[collectionName] = collection.GetCollectionID()

		// Alias hints are written ONLY from the entry's own declared Aliases, so
		// every hint is discoverable from its target entry and eviction can
		// always clean them -- no stray hints can exist (a stray would validate
		// again after a same-name recreate and resurrect a dead alias). This
		// also covers the alias the caller used, and lets an alias DDL broadcast
		// from an older rootcoord (alias name only, CollectionID=0) resolve the
		// alias to the primary entry to evict it.
		for _, alias := range collection.Aliases {
			if alias != "" && alias != collectionName {
				m.setAliasLocked(bucketDB, alias, collectionName)
			}
		}
	}

	mlog.Info(ctx, "meta update success", mlog.String("requestDatabase", database), mlog.String("database", bucketDB),
		mlog.String("collectionName", collectionName),
		mlog.String("actual collection Name", collection.Schema.GetName()), mlog.Int64("collectionID", collection.CollectionID),
		mlog.Uint64("version", collection.GetRequestTime()), mlog.Any("Aliases", collection.Aliases),
		mlog.Bool("partition key isolation", isolation), mlog.String("QueryMode", queryMode),
	)

	return info, nil
}

// normalizeDBName maps an empty database name to the default database,
// mirroring rootcoord's backward-compat normalization for name lookups
// (meta_table.getCollectionByNameInternal).
func normalizeDBName(database string) string {
	if database == "" {
		return util.DefaultDBName
	}
	return database
}

func NormalizeDBName(database string) string {
	return normalizeDBName(database)
}

// newCollectionInfo builds a CollectionInfo from a describe response. It does
// not touch any cache map, so it is safe to use for entries that are returned
// without being cached.
func newCollectionInfo(collection *milvuspb.DescribeCollectionResponse, schemaInfo *SchemaInfo, isolation bool, queryMode string) *CollectionInfo {
	return &CollectionInfo{
		CollID:                collection.CollectionID,
		DBID:                  collection.GetDbId(),
		DBName:                collection.GetDbName(),
		Schema:                schemaInfo,
		CreatedTimestamp:      collection.CreatedTimestamp,
		CreatedUtcTimestamp:   collection.CreatedUtcTimestamp,
		ConsistencyLevel:      collection.ConsistencyLevel,
		PartitionKeyIsolation: isolation,
		QueryMode:             queryMode,
		UpdateTimestamp:       collection.UpdateTimestamp,
		CollectionTTL:         GetCollectionTTL(schemaInfo.GetProperties()),
		VChannels:             collection.VirtualChannelNames,
		PChannels:             collection.PhysicalChannelNames,
		NumPartitions:         collection.NumPartitions,
		ShardsNum:             collection.ShardsNum,
		Aliases:               collection.Aliases,
		Properties:            collection.Properties,
		RoutingModulus:        collection.GetRoutingModulus(),
		ShardBy:               collection.GetShardBy(),
	}
}

// buildRoutingTable derives a collection's routing table from the routing
// modulus and the per-shard residues DescribeCollection returns parallel to
// virtual_channel_names.
//
// A collection with no shard infos at all has never been split; deriving from
// the channel list alone gives exactly the legacy hash % shardNum placement.
// A non-nil error means the routing meta is malformed (a gap, an overlap, or a
// residue outside the modulus).
func buildRoutingTable(modulus uint64, vChannels []string, shardInfos []*schemapb.CollectionShardInfo) (*routing.Table, error) {
	if len(shardInfos) == 0 {
		return routing.Derive(modulus, vChannels, nil)
	}
	shards, err := routing.ShardsFromMeta(vChannels, shardInfos)
	if err != nil {
		return nil, err
	}
	return routing.Derive(modulus, vChannels, shards)
}

// attachRouting derives the routing table onto a freshly built entry.
//
// A malformed routing meta logs and leaves the table nil; the write path then
// keeps the legacy modulo. That is the safe fallback only because a collection
// that has never been split is exactly the case the legacy modulo is correct
// for -- for a split collection the write is rejected instead, because its
// table is what tells insert and delete which shard owns a key.
func attachRouting(ctx context.Context, info *CollectionInfo, collection *milvuspb.DescribeCollectionResponse) *CollectionInfo {
	table, err := buildRoutingTable(collection.GetRoutingModulus(), collection.GetVirtualChannelNames(), collection.GetShardInfos())
	if err != nil {
		mlog.Warn(ctx, "build routing table failed, fall back to no explicit routing",
			mlog.FieldCollectionName(collection.GetCollectionName()),
			mlog.FieldCollectionID(collection.GetCollectionID()),
			mlog.Err(err))
		return info
	}
	info.RoutingTable = table
	return info
}

func buildSfKeyByName(database, collectionName string) string {
	return database + "-" + collectionName
}

// GetCollectionTTL returns ttl if collection's ttl is specified.
// Return 0 when ttl is unspecified or malformed, preserving the previous proxy behavior.
func GetCollectionTTL(pairs []*commonpb.KeyValuePair) uint64 {
	ttl, err := common.GetCollectionTTL(pairs)
	if err != nil || ttl <= 0 {
		return 0
	}
	return uint64(ttl)
}

func buildSfKeyById(database string, collectionID typeutil.UniqueID) string {
	return database + "--" + fmt.Sprint(collectionID)
}

// The partition cache is keyed by the collection id: the primary store is
// id-keyed, ids are cluster-unique and never reused, so a drop+recreate can
// never alias into old partition entries, and no name/alias expansion is ever
// needed to invalidate them.
func buildPartitionCacheKey(collectionID typeutil.UniqueID) string {
	return fmt.Sprint(collectionID)
}

func BuildPartitionCacheKey(collectionID typeutil.UniqueID) string {
	return buildPartitionCacheKey(collectionID)
}

func (m *MetaCache) updateWithSingleflight(
	ctx context.Context,
	key string,
	database string,
	collectionName string,
	collectionID typeutil.UniqueID,
) (*CollectionInfo, error) {
	// The read lock must cover the entire singleflight.Do call, not only update().
	// singleflight keeps a completed callback in its map until its own cleanup
	// runs. Releasing fillMu inside update would let an invalidation evict the
	// write-back while a post-invalidation caller could still join that old flight
	// and receive its pre-DDL result directly, bypassing the cache maps.
	m.fillMu.RLock()
	defer m.fillMu.RUnlock()

	collection, err, _ := m.sfGlobal.Do(key, func() (*CollectionInfo, error) {
		info, err := m.update(ctx, database, collectionName, collectionID)
		if h := m.testHookBeforeSingleflightReturn; h != nil {
			h()
		}
		return info, err
	})
	return collection, err
}

func (m *MetaCache) UpdateByName(ctx context.Context, database, collectionName string) (*CollectionInfo, error) {
	database = normalizeDBName(database)
	collection, err := m.updateWithSingleflight(
		ctx,
		buildSfKeyByName(database, collectionName),
		database,
		collectionName,
		0,
	)
	// Name resolution failed -> the caller named a collection/db that does not
	// exist, which is the user's input error, not a system fault. Mark it here,
	// the single name-resolution chokepoint shared by every name-based
	// GetCollection* path (data-plane and control-plane proxy tasks), so the
	// error_type is Input. The id-based UpdateByID path is deliberately left as
	// SystemError: a by-id lookup miss is an internal/component query (e.g.
	// rootcoord answering another component by collectionID), not user input.
	// The sentinel itself stays SystemError so datacoord's internal retry.Do
	// recovery loops still retry a transient not-found.
	return collection, merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
}

func (m *MetaCache) UpdateByID(ctx context.Context, database string, collectionID typeutil.UniqueID) (*CollectionInfo, error) {
	// Do not normalize an empty db to default here: update() needs to tell an
	// external id-only lookup (no db context) apart from an internal by-id
	// refresh that carries a real db, to decide whether it may cache by name.
	return m.updateWithSingleflight(
		ctx,
		buildSfKeyById(database, collectionID),
		database,
		"",
		collectionID,
	)
}

// GetCollectionID returns the corresponding collection id for provided collection name
func (m *MetaCache) GetCollectionID(ctx context.Context, database, collectionName string) (typeutil.UniqueID, error) {
	method := "GetCollectionID"
	collInfo, ok := m.getCollection(database, collectionName, 0)
	if !ok {
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")

		collInfo, err := m.UpdateByName(ctx, database, collectionName)
		if err != nil {
			return typeutil.UniqueID(0), err
		}

		metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return collInfo.CollID, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheHitLabel).Inc()

	return collInfo.CollID, nil
}

// GetCollectionName returns the corresponding collection name for provided collection id
func (m *MetaCache) GetCollectionName(ctx context.Context, database string, collectionID int64) (string, error) {
	method := "GetCollectionName"
	collInfo, ok := m.getCollection(database, "", collectionID)

	if !ok {
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")

		collInfo, err := m.UpdateByID(ctx, database, collectionID)
		if err != nil {
			return "", err
		}

		metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return collInfo.Schema.Name, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheHitLabel).Inc()

	return collInfo.Schema.Name, nil
}

func (m *MetaCache) GetCollectionInfo(ctx context.Context, database string, collectionName string, collectionID int64) (*CollectionInfo, error) {
	// Pass collectionID through so an id-only lookup (collectionName == "") hits
	// the cluster-wide id index instead of probing id 0, which always misses and
	// forces a needless UpdateByID/singleflight round plus a spurious cache-miss
	// metric. For name lookups getCollection ignores the id.
	collInfo, ok := m.getCollection(database, collectionName, collectionID)

	method := "GetCollectionInfo"
	// if collInfo.CollID != collectionID, means that the cache is not trustable
	// try to get collection according to collectionID
	// Why use collectionID? Because the collectionID is not always provided in the proxy.
	if !ok || (collectionID != 0 && collInfo.CollID != collectionID) {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheMissLabel).Inc()

		if collectionID == 0 {
			collInfo, err := m.UpdateByName(ctx, database, collectionName)
			if err != nil {
				return nil, err
			}
			metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
			return collInfo, nil
		}
		collInfo, err := m.UpdateByID(ctx, database, collectionID)
		if err != nil {
			return nil, err
		}
		metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return collInfo, nil
	}

	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheHitLabel).Inc()
	return collInfo, nil
}

func (m *MetaCache) GetCollectionSchema(ctx context.Context, database, collectionName string) (*SchemaInfo, error) {
	collInfo, ok := m.getCollection(database, collectionName, 0)

	method := "GetCollectionSchema"
	if !ok {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheMissLabel).Inc()

		collInfo, err := m.UpdateByName(ctx, database, collectionName)
		if err != nil {
			return nil, err
		}
		metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		mlog.Debug(ctx, "Reload collection from root coordinator ",
			mlog.String("collectionName", collectionName),
			mlog.Int64("time (milliseconds) take ", tr.ElapseSpan().Milliseconds()))
		return collInfo.Schema, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheHitLabel).Inc()

	return collInfo.Schema, nil
}

// setAliasLocked sets an alias hint (alias -> real name). Caller must hold m.mu
// write lock.
func (m *MetaCache) setAliasLocked(database, alias, realName string) {
	database = normalizeDBName(database)
	if _, ok := m.aliasInfo[database]; !ok {
		m.aliasInfo[database] = make(map[string]string)
	}
	m.aliasInfo[database][alias] = realName
}

// removeAliasLocked removes an alias hint. Caller must hold m.mu write lock.
func (m *MetaCache) removeAliasLocked(database, alias string) {
	if db, ok := m.aliasInfo[normalizeDBName(database)]; ok {
		delete(db, alias)
	}
}

func (m *MetaCache) RemoveAlias(ctx context.Context, database, alias string) {
	// Invalidation: drain in-flight fills (e.g. a DescribeAlias write-back racing
	// this alias DDL) before removing, see fillMu.
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeAliasLocked(database, alias)
	mlog.Debug(ctx, "remove alias from cache", mlog.String("db", database), mlog.String("alias", alias))
}

// RemoveAliasHolders evicts every cached entry of database whose declared
// Aliases contain alias, by scanning the primary store. FALLBACK gated by the
// caller to alias-DDL broadcasts that carry no target id (id==0) -- i.e. when
// the old target could not be precisely located. This is a PERMANENT safety
// net, not upgrade-window compat: id==0 arrives both from an old rootcoord that
// predates old_collection_id AND from a new rootcoord that could not resolve
// the pre-alter AlterAlias target (a meta inconsistency). It closes the
// otherwise-unhealable window case: a concurrent describe re-points the alias
// hint to the new target before the broadcast arrives, so name-based resolution
// misses the OLD target, whose stale Aliases list would then persist until an
// unrelated eviction (a stable, name-addressed collection might never get one).
// O(cached collections), paid only when the old target id is absent.
func (m *MetaCache) RemoveAliasHolders(ctx context.Context, database, alias string) {
	// Invalidation: drain in-flight fills first, see fillMu.
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	db := normalizeDBName(database)
	var ids []typeutil.UniqueID
	for id, entry := range m.collections {
		if normalizeDBName(entry.DBName) != db {
			continue
		}
		for _, a := range entry.Aliases {
			if a == alias {
				ids = append(ids, id)
				break
			}
		}
	}
	for _, id := range ids {
		m.removeCollectionByID(ctx, id)
	}
	mlog.Debug(ctx, "removed alias holders (old-rootcoord fallback)", mlog.String("db", database),
		mlog.String("alias", alias), mlog.Int("holders", len(ids)))
}

// ResolveCollectionAlias normalizes a caller-supplied name -- which may be an
// alias -- to the collection's real name, for RBAC object resolution. It is
// just the collection cache with a fill on miss: no dedicated alias RPC, no
// separately-lifecycled alias state. The fill's describe resolves Aliases at
// rootcoord and caches the entry under its real name plus its declared alias
// hints, so the next request hits the cache (a resolve-and-forget RPC would
// re-ask forever). A name that does not resolve to any collection (junk, or an
// alias unknown to rootcoord) is returned unchanged: authorization proceeds
// against the literal name and the task fails not-found later, as before.
func (m *MetaCache) ResolveCollectionAlias(ctx context.Context, database, nameOrAlias string) (string, error) {
	if collInfo, ok := m.getCollection(database, nameOrAlias, 0); ok {
		if collInfo.Schema != nil && collInfo.Schema.GetName() != "" {
			return collInfo.Schema.GetName(), nil
		}
		return nameOrAlias, nil
	}

	info, err := m.GetCollectionInfo(ctx, database, nameOrAlias, 0)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) || errors.Is(err, merr.ErrAliasNotFound) {
			return nameOrAlias, nil
		}
		return "", err
	}
	if info.Schema != nil && info.Schema.GetName() != "" {
		return info.Schema.GetName(), nil
	}
	return nameOrAlias, nil
}

func (m *MetaCache) GetPartitionID(ctx context.Context, database, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	partInfo, err := m.GetPartitionInfo(ctx, database, collectionName, partitionName)
	if err != nil {
		return 0, err
	}
	return partInfo.PartitionID, nil
}

func (m *MetaCache) GetPartitionName(ctx context.Context, database, collectionName string, PartitionID int64) (string, error) {
	partitions, err := m.GetPartitionInfos(ctx, database, collectionName)
	if err != nil {
		return "", err
	}

	for _, info := range partitions.PartitionInfos {
		if info.PartitionID == PartitionID {
			return info.Name, nil
		}
	}

	return "", merr.WrapErrPartitionNotFound(PartitionID)
}

func (m *MetaCache) GetPartitions(ctx context.Context, database, collectionName string) (map[string]typeutil.UniqueID, error) {
	partitions, err := m.GetPartitionInfos(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}

	return partitions.Name2ID, nil
}

// resolvePartitionCollection resolves the caller-supplied (database, name) --
// which may be an alias and/or an empty db -- to the collection entry, filling
// the collection cache on a miss. Partition caches are keyed by the entry's
// cluster-unique id, so every collection has exactly ONE partition-cache
// location no matter how it is addressed, invalidation stales exact id-keys,
// and a drop+recreate can never alias into old entries (new id). It also
// returns the canonical (db, name) to address rootcoord with.
func (m *MetaCache) resolvePartitionCollection(ctx context.Context, database, collectionName string) (typeutil.UniqueID, string, string, error) {
	info, err := m.GetCollectionInfo(ctx, database, collectionName, 0)
	if err != nil {
		return 0, "", "", err
	}
	db := info.DBName
	if db == "" {
		db = normalizeDBName(database)
	}
	name := collectionName
	if info.Schema != nil && info.Schema.GetName() != "" {
		name = info.Schema.GetName()
	}
	return info.CollID, db, name, nil
}

func (m *MetaCache) GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*PartitionInfo, error) {
	// Handle empty partitionName - use default partition
	if partitionName == "" {
		partitionName = paramtable.Get().CommonCfg.DefaultPartitionName.GetValue()
	}
	// One partition cache for everything: resolve the full list (cached by
	// collection id) and answer the point lookup from its name map.
	partitions, err := m.GetPartitionInfos(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}
	if info, ok := partitions.Name2Info[partitionName]; ok {
		return info, nil
	}
	// partitionName is caller-supplied; a failed name resolution is the user's
	// input error, not a system fault. Mark it here, the single partition-name
	// chokepoint (GetPartitionID also routes through here), so the proxy reports
	// InputError without per-task wrappers. ErrPartitionNotFound stays
	// SystemError by default so id-based lookups (GetPartitionName) are unaffected.
	return nil, merr.WrapErrAsInputError(merr.WrapErrPartitionNotFound(partitionName))
}

func (m *MetaCache) GetPartitionsIndex(ctx context.Context, database, collectionName string) ([]string, error) {
	partitions, err := m.GetPartitionInfos(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}

	if partitions.IndexedPartitionNames == nil {
		return nil, merr.WrapErrServiceInternal("partitions not in partition key naming pattern")
	}

	return partitions.IndexedPartitionNames, nil
}

func (m *MetaCache) GetPartitionInfos(ctx context.Context, database, collectionName string) (*PartitionInfos, error) {
	method := "GetPartitionInfo"
	// See resolvePartitionCollection: entries are keyed by the collection id, so
	// they have exactly one location and invalidation stales exact id-keys.
	collectionID, rpcDB, rpcName, err := m.resolvePartitionCollection(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}
	key := buildPartitionCacheKey(collectionID)
	m.mu.RLock()
	cached, ok := m.partitionCache[key]
	m.mu.RUnlock()
	if ok && cached != nil {
		return cached, nil
	}
	tr := timerecord.NewTimeRecorder("UpdateCache")
	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheMissLabel).Inc()
	// Keep the fill lock through singleflight cleanup, not only its callback.
	// Otherwise a partition invalidation can finish after the callback unlocks
	// while a new caller still joins the completed pre-invalidation flight.
	m.fillMu.RLock()
	defer m.fillMu.RUnlock()
	partitionsInfo, err, _ := m.sfPartitionCache.Do(key, func() (*PartitionInfos, error) {
		// Describe/show BY ID (collectionID resolved above), not by name: the
		// cache key is that id, and resolving by name here would let a
		// concurrent same-name drop+recreate return the NEW collection's
		// partitions and cache them under the OLD id (wrong partition context,
		// and with no GC an entry nothing can clean). By id, a dropped old id
		// returns not-found and the fill fails (the caller re-resolves) rather
		// than caching a mismatch. The empty name forces id-only resolution.
		collection, err := m.describeCollection(ctx, rpcDB, "", collectionID)
		if err != nil {
			return nil, err
		}
		if collection.GetCollectionID() != collectionID {
			// defense: the id and the returned collection must be the same one
			return nil, merr.WrapErrCollectionNotFound(rpcName)
		}
		schemaInfo, err := NewSchemaInfo(collection.Schema)
		if err != nil {
			return nil, err
		}

		resp, err := m.showPartitions(ctx, rpcDB, "", collectionID)
		if err != nil {
			return nil, err
		}
		partitions := make([]*PartitionInfo, 0)
		for i, name := range resp.PartitionNames {
			partitions = append(partitions, &PartitionInfo{
				Name:                name,
				PartitionID:         resp.PartitionIDs[i],
				CreatedTimestamp:    resp.CreatedTimestamps[i],
				CreatedUtcTimestamp: resp.CreatedUtcTimestamps[i],
			})
		}
		partitionsInfo := parsePartitionsInfo(partitions, schemaInfo.IsPartitionKeyCollection())
		// Write-back under m.mu, ordered against invalidations by fillMu (held
		// across this whole closure): an invalidation drains in-flight fills
		// before deleting, so this insert either lands before the delete (and is
		// cleaned by it) or the fill was issued after the DDL. No version floor.
		//
		// Only cache the list while the collection's PRIMARY entry is still live.
		// collectionID was resolved before we took fillMu.RLock; an invalidation
		// in that window evicts the primary AND its partition list, but the fill
		// still describes/shows the collection successfully (it lives at rootcoord
		// for Load/Release/Alter). Writing anyway would leave a partition entry
		// with NO primary owner: a later Create/DropPartition invalidation routes
		// through evictCollectionEntryLocked, which no-ops when the primary is
		// absent, so the stale list would survive and be re-adopted when the
		// primary re-fills. fillMu.RLock is held here, so no invalidation can run
		// between this check and the write -- if the primary is present now it
		// stays present until we release, and any future eviction of it cleans
		// the list. Absent primary => skip the cache; the caller still gets the
		// correct list, just uncached (one extra fill next time).
		m.mu.Lock()
		if _, ok := m.collections[collectionID]; ok {
			m.partitionCache[key] = partitionsInfo
		}
		m.mu.Unlock()
		metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return partitionsInfo, nil
	})
	if err != nil {
		return nil, err
	}
	if partitionsInfo == nil {
		return nil, merr.WrapErrServiceInternal("partition info not found")
	}
	return partitionsInfo, nil
}

// Get the collection information from rootcoord.
func (m *MetaCache) describeCollection(ctx context.Context, database, collectionName string, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
	req := &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
		),
		DbName:         database,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	}
	coll, err := m.mixCoord.DescribeCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	err = merr.Error(coll.GetStatus())
	if err != nil {
		return nil, err
	}
	userFields := make([]*schemapb.FieldSchema, 0)
	for _, field := range coll.Schema.Fields {
		if field.FieldID >= common.StartOfUserFieldID {
			userFields = append(userFields, field)
		}
	}
	coll.Schema.Fields = userFields
	return coll, nil
}

func (m *MetaCache) showPartitions(ctx context.Context, DBName string, collectionName string, collectionID typeutil.UniqueID) (*milvuspb.ShowPartitionsResponse, error) {
	req := &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		DbName:         DBName,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	}

	partitions, err := m.mixCoord.ShowPartitions(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := merr.Error(partitions.GetStatus()); err != nil {
		return nil, err
	}

	// The response shape is produced by the coordinator, not the caller: a
	// misaligned array is backend metadata inconsistency, not user input.
	if len(partitions.PartitionIDs) != len(partitions.PartitionNames) {
		return nil, merr.WrapErrServiceInternalMsg("partition ids len: %d doesn't equal Partition name len %d",
			len(partitions.PartitionIDs), len(partitions.PartitionNames))
	}
	if len(partitions.PartitionNames) != len(partitions.CreatedTimestamps) ||
		len(partitions.PartitionNames) != len(partitions.CreatedUtcTimestamps) {
		return nil, merr.WrapErrServiceInternalMsg(
			"partition names and timestamps number is not aligned, response: %s",
			partitions.String(),
		)
	}

	return partitions, nil
}

func (m *MetaCache) describeDatabase(ctx context.Context, DBName string) (*rootcoordpb.DescribeDatabaseResponse, error) {
	req := &rootcoordpb.DescribeDatabaseRequest{
		DbName: DBName,
	}

	resp, err := m.mixCoord.DescribeDatabase(ctx, req)
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}

	return resp, nil
}

// parsePartitionsInfo parse PartitionInfo list to PartitionInfos struct.
// prepare all name to id & info map
// try parse partition names to partitionKey index.
func parsePartitionsInfo(infos []*PartitionInfo, hasPartitionKey bool) *PartitionInfos {
	Name2ID := lo.SliceToMap(infos, func(info *PartitionInfo) (string, int64) {
		return info.Name, info.PartitionID
	})
	Name2Info := lo.SliceToMap(infos, func(info *PartitionInfo) (string, *PartitionInfo) {
		return info.Name, info
	})

	result := &PartitionInfos{
		PartitionInfos: infos,
		Name2ID:        Name2ID,
		Name2Info:      Name2Info,
	}

	if !hasPartitionKey {
		return result
	}

	// Make sure the order of the partition names got every time is the same
	partitionNames := make([]string, len(infos))
	for _, info := range infos {
		partitionName := info.Name
		splits := strings.Split(partitionName, "_")
		if len(splits) < 2 {
			mlog.Info(context.TODO(), "partition group not in partitionKey pattern", mlog.String("partitionName", partitionName))
			return result
		}
		index, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
		if err != nil {
			mlog.Info(context.TODO(), "partition group not in partitionKey pattern", mlog.String("partitionName", partitionName), mlog.Err(err))
			return result
		}
		// Defense against anomalous ShowPartitions metadata (e.g. a "_3" suffix
		// with only two partitions): a raw index into partitionNames would panic.
		// Degrade to the unindexed result, like the sibling
		// typeutil.RearrangePartitionsForPartitionKey.
		if index < 0 || index >= int64(len(infos)) {
			mlog.Info(context.TODO(), "partition group index out of range for partitionKey pattern",
				mlog.String("partitionName", partitionName), mlog.Int64("index", index), mlog.Int("partitionCount", len(infos)))
			return result
		}
		partitionNames[index] = partitionName
	}

	result.IndexedPartitionNames = partitionNames
	return result
}

func ParsePartitionsInfo(infos []*PartitionInfo, hasPartitionKey bool) *PartitionInfos {
	return parsePartitionsInfo(infos, hasPartitionKey)
}

// removeCollectionByAliasLocked, when alias is a cached alias hint in database,
// invalidates the collection it currently resolves to (by id) so an alias DDL
// from an OLDER rootcoord (alias name only, CollectionID=0) still evicts the
// target's primary entry. Newer rootcoords forward the target ids explicitly --
// including both sides of an AlterAlias -- which does not depend on this hint.
// Returns true if it found and invalidated (or cleaned up) a target. Caller
// must hold m.mu write lock.
func (m *MetaCache) removeCollectionByAliasLocked(ctx context.Context, database, alias string) bool {
	Aliases, ok := m.aliasInfo[database]
	if !ok {
		return false
	}
	target, ok := Aliases[alias]
	if !ok || target == "" {
		return false
	}
	if ids, ok := m.nameIdx[database]; ok {
		if id, ok := ids[target]; ok {
			m.removeCollectionByID(ctx, id)
			return true
		}
	}
	// Unreachable under the invariant "an alias hint exists only while its
	// target entry lives and declares it" (hints are written solely from
	// update()'s declared-Aliases loop and cleaned at eviction); kept as a
	// one-line defense against future edits.
	delete(Aliases, alias)
	return true
}

func (m *MetaCache) RemoveCollection(ctx context.Context, database, collectionName string) {
	// Invalidation: drain in-flight fills first, see fillMu.
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	// All hint buckets are keyed by the normalized database.
	database = normalizeDBName(database)
	if ids, ok := m.nameIdx[database]; ok {
		if id, ok := ids[collectionName]; ok {
			m.removeCollectionByID(ctx, id)
		}
	}
	// The name may be an alias (alias DDL from an older rootcoord broadcasts
	// only the alias name); resolve it to its target and evict that primary
	// entry too. Either eviction runs the unified routine, which cleans the
	// entry's hints synchronously.
	m.removeCollectionByAliasLocked(ctx, database, collectionName)
	mlog.Debug(ctx, "remove collection", mlog.String("db", database), mlog.String("collection", collectionName))
}

// InvalidateCollectionMeta applies every O(1) mutation carried by one cache
// expiration request under one fillMu/m.mu critical section. Resolving all ids
// before the first eviction is important: evictCollectionEntryLocked removes
// the entry's name and alias hints, which may be needed to locate another target
// named by the same request.
//
// The caller keeps O(N) policies separate. In particular, an alias DDL whose
// old target id is unknown may follow this method with RemoveAliasHolders; that
// scan is intentionally not hidden inside this O(1) batch.
func (m *MetaCache) InvalidateCollectionMeta(
	ctx context.Context,
	database string,
	collectionName string,
	collectionID typeutil.UniqueID,
	removeAlias bool,
) []string {
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	database = normalizeDBName(database)
	ids := make([]typeutil.UniqueID, 0, 3)
	seenIDs := make(map[typeutil.UniqueID]struct{}, 3)
	addID := func(id typeutil.UniqueID) {
		if id == 0 {
			return
		}
		if _, ok := seenIDs[id]; ok {
			return
		}
		seenIDs[id] = struct{}{}
		ids = append(ids, id)
	}

	if collectionName != "" {
		if names, ok := m.nameIdx[database]; ok {
			addID(names[collectionName])
		}
		if Aliases, ok := m.aliasInfo[database]; ok {
			if target := Aliases[collectionName]; target != "" {
				if names, ok := m.nameIdx[database]; ok {
					addID(names[target])
				}
			}
		}
	}
	addID(collectionID)

	removedNames := make([]string, 0, len(ids))
	for _, id := range ids {
		if name, ok := m.evictCollectionEntryLocked(id); ok && name != "" {
			removedNames = append(removedNames, name)
		}
	}

	if h := m.testHookInvalidateCollectionMetaMidMutation; h != nil {
		h()
	}
	if removeAlias && collectionName != "" {
		m.removeAliasLocked(database, collectionName)
	}

	mlog.Debug(ctx, "invalidated collection meta cache",
		mlog.String("db", database),
		mlog.String("collection", collectionName),
		mlog.Int64("id", collectionID),
		mlog.Bool("removeAlias", removeAlias),
		mlog.Strings("removedCollections", removedNames))
	return removedNames
}

func (m *MetaCache) RemoveCollectionsByID(ctx context.Context, collectionID typeutil.UniqueID) []string {
	// Invalidation: drain in-flight fills first, see fillMu.
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removeCollectionByID(ctx, collectionID)
}

// evictCollectionEntryLocked is THE single eviction routine: it removes the
// primary entry and everything the entry owns, all O(1)-reachable from the
// entry in hand (no background GC exists):
//  1. its declared alias hints -- Aliases are cascade-dropped with their
//     collection at rootcoord but the DropCollection broadcast carries no
//     per-alias expiration, and a lazily-kept hint would VALIDATE again after
//     a same-name recreate (alias -> name -> the NEW id), permanently
//     resurrecting a deleted alias for RBAC and routing;
//  2. its name hint, only while it still points at this id (a same-name
//     recreate may have already overwritten it);
//  3. its partition list -- Load/Release/Alter share the eviction path and pay
//     one extra ShowPartitions on the next partition access.
//
// EVERY deletion from m.collections must go through here: a raw delete leaves
// the entry's hints behind with no owner, and a later drop + same-name
// recreate turns them into resurrected Aliases. Returns the entry's real name
// and whether an entry was evicted. Caller must hold m.mu write lock, with
// fills either drained (fillMu held) or racing tolerably (RemoveDatabase's
// post-drain walk: evicting a freshly re-filled entry removes it TOGETHER with
// its hints -- one refetch, never a dangling hint).
func (m *MetaCache) evictCollectionEntryLocked(collectionID typeutil.UniqueID) (string, bool) {
	entry, ok := m.collections[collectionID]
	if !ok {
		return "", false
	}
	name := ""
	if entry.Schema != nil {
		name = entry.Schema.GetName()
	}
	db := normalizeDBName(entry.DBName)
	if Aliases, ok := m.aliasInfo[db]; ok {
		for _, alias := range entry.Aliases {
			if Aliases[alias] == name {
				delete(Aliases, alias)
			}
		}
	}
	if ids, ok := m.nameIdx[db]; ok && ids[name] == collectionID {
		delete(ids, name)
		if len(ids) == 0 {
			delete(m.nameIdx, db)
		}
	}
	m.stalePartitionCacheLocked(collectionID)
	delete(m.collections, collectionID)
	return name, true
}

func (m *MetaCache) removeCollectionByID(ctx context.Context, collectionID typeutil.UniqueID) []string {
	var collNames []string
	if name, ok := m.evictCollectionEntryLocked(collectionID); ok && name != "" {
		collNames = append(collNames, name)
	}
	mlog.Debug(ctx, "remove collection by id", mlog.Int64("id", collectionID),
		mlog.Strings("collection", collNames))
	return collNames
}

func (m *MetaCache) RemoveDatabase(ctx context.Context, database string) {
	mlog.Debug(ctx, "remove database", mlog.String("name", database))
	// Invalidation: drain in-flight fills first, so a describe issued before the
	// DropDatabase broadcast cannot write its pre-DDL response back
	// afterwards and resurrect an entry of this database. See fillMu.
	// Hold the fill write lock ONLY for the drain plus the O(1) part (DETACH and
	// drop the hint buckets by reference), then release it BEFORE both collecting
	// the ids from the detached bucket and the O(N_db) primary walk -- a database
	// DDL must not block the whole proxy's fills for longer than any other
	// invalidation's drain. This stays safe without the
	// lock: any pre-DDL snapshot was either drained (its id is in the collected
	// list below and gets deleted) or its fill was blocked here and will issue
	// its RPC only after we release -- reading post-DDL state, since rootcoord
	// commits before broadcasting. A FRESH entry written by such a fill while
	// the batched walk runs may get over-deleted by it -- harmless, one extra
	// describe -- the "a wrong deletion only ever costs a refetch" principle.
	//
	// COMPLETENESS of the collected list: every live primary entry has a
	// validating name hint in its database's nameIdx bucket -- update() writes
	// both together, and a hint is only removed by the eviction that removes
	// its entry -- so the bucket reaches every entry of this db.
	m.fillMu.Lock()
	m.mu.Lock()
	// O(1) under the lock: DETACH the db's hint bucket by reference and drop it
	// from the maps -- do NOT iterate it here. Once detached, no fill or eviction
	// touches this map (a new fill for this db creates a fresh bucket; evictions
	// look up m.nameIdx[db], now absent), so it can be walked lock-free below.
	// This keeps the lock hold O(1) instead of O(the db's collections); the
	// earlier in-lock iteration was the scan the "O(1) hold" comment promised to
	// avoid.
	bucket := m.nameIdx[database]
	delete(m.dbInfo, database)
	delete(m.nameIdx, database)
	delete(m.aliasInfo, database)
	m.mu.Unlock()
	m.fillMu.Unlock()

	// Collect the doomed ids from the detached bucket OUTSIDE the locks.
	ids := make([]typeutil.UniqueID, 0, len(bucket))
	for _, id := range bucket {
		ids = append(ids, id)
	}

	if h := m.testHookRemoveDatabaseMidWindow; h != nil {
		h()
	}

	// O(the db's cached collections), rare op, batched so m.mu is held O(batch)
	// at a time; completes before returning, so the broadcast ack still implies
	// "no stale reads afterwards".
	const removeBatch = 1024
	evicted := 0
	for start := 0; start < len(ids); start += removeBatch {
		end := min(start+removeBatch, len(ids))
		m.mu.Lock()
		for _, id := range ids[start:end] {
			// Unified eviction, not a raw delete: if a fill racing this walk has
			// rebuilt the entry WITH fresh name/alias hints, deleting
			// only the primary would leave those hints dangling, and a later
			// drop + same-name recreate would resurrect a dead alias.
			if _, ok := m.evictCollectionEntryLocked(id); ok {
				evicted++
			}
		}
		m.mu.Unlock()
	}
	mlog.Debug(ctx, "removed database collections", mlog.String("db", database), mlog.Int("evicted", evicted))
}

// RemoveDatabaseInfo invalidates only the database metadata entry. Drain the
// complete database-info singleflight lifecycle before deleting the write-back,
// so a DescribeDatabase issued before AlterDatabase cannot resurrect stale
// Properties after this method returns. Collection metadata is deliberately
// left untouched: AlterDatabase changes database Properties only.
func (m *MetaCache) RemoveDatabaseInfo(ctx context.Context, database string) {
	m.fillMu.Lock()
	defer m.fillMu.Unlock()

	m.mu.Lock()
	delete(m.dbInfo, database)
	m.mu.Unlock()

	mlog.Debug(ctx, "removed database info", mlog.String("db", database))
}

func (m *MetaCache) HasDatabase(ctx context.Context, database string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.nameIdx[database]
	return ok
}

func (m *MetaCache) GetDatabaseInfo(ctx context.Context, database string) (*DatabaseInfo, error) {
	dbInfo := m.safeGetDBInfo(database)
	if dbInfo != nil {
		return dbInfo, nil
	}

	// Keep the fill lock through singleflight cleanup, matching collection and
	// partition fills. RemoveDatabase/RemoveDatabaseInfo must not finish while a
	// new caller can still join a completed pre-invalidation database flight.
	m.fillMu.RLock()
	defer m.fillMu.RUnlock()
	dbInfo, err, _ := m.sfDB.Do(database, func() (*DatabaseInfo, error) {
		resp, err := m.describeDatabase(ctx, database)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		dbInfo := &DatabaseInfo{
			DBID:             resp.GetDbID(),
			Properties:       resp.Properties,
			CreatedTimestamp: resp.GetCreatedTimestamp(),
		}
		m.dbInfo[database] = dbInfo
		return dbInfo, nil
	})

	// Symmetric with UpdateByName: a failed database-name resolution means the
	// caller named a database that does not exist — the user's input error, not a
	// system fault. Mark it here, the single database-name chokepoint, so every
	// caller (data-plane and control-plane proxy tasks) gets InputError without a
	// per-callsite wrapper. The sentinel stays SystemError so internal id-based
	// lookups and retry loops elsewhere are unaffected.
	return dbInfo, merr.WrapErrAsInputErrorWhen(err, merr.ErrDatabaseNotFound)
}

func (m *MetaCache) safeGetDBInfo(database string) *DatabaseInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	db, ok := m.dbInfo[database]
	if !ok {
		return nil
	}
	return db
}

func (m *MetaCache) AllocID(ctx context.Context) (int64, error) {
	m.idLock.Lock()
	defer m.idLock.Unlock()

	if m.idIndex == m.idCount {
		resp, err := m.mixCoord.AllocID(ctx, &rootcoordpb.AllocIDRequest{
			Count: 1000000,
		})
		if err != nil {
			mlog.Warn(context.TODO(), "Refreshing ID cache from rootcoord failed", mlog.Err(err))
			return 0, err
		}
		if resp.GetStatus().GetCode() != 0 {
			mlog.Warn(context.TODO(), "Refreshing ID cache from rootcoord failed", mlog.String("failed detail", resp.GetStatus().GetDetail()))
			return 0, merr.Error(resp.GetStatus())
		}
		m.idStart, m.idCount = resp.GetID(), int64(resp.GetCount())
		m.idIndex = 0
	}
	id := m.idStart + m.idIndex
	m.idIndex++
	return id, nil
}

func (m *MetaCache) RemovePartition(ctx context.Context, database string, collectionID typeutil.UniqueID, collectionName string, partitionName string) {
	// Invalidation: drain in-flight fills first, see fillMu.
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	// Partition entries are keyed by the collection id. Resolve the id from the
	// broadcast or the LOCAL hints only -- no RPC on the invalidation path.
	// This is complete: a partition list exists only while its collection's
	// primary entry lives (evictions clean both together), and a live entry
	// always has its name and declared-alias hints. So if the hints cannot
	// resolve the broadcast name, nothing of this collection is cached and
	// there is nothing to invalidate. (This also removes the old failure mode
	// where a name-only broadcast from an older rootcoord plus a transient
	// describe error silently dropped the invalidation.)
	id := collectionID
	if id == 0 && collectionName != "" {
		db := normalizeDBName(database)
		resolved := collectionName
		if aliasDB, ok := m.aliasInfo[db]; ok {
			if target, ok := aliasDB[collectionName]; ok && target != "" {
				resolved = target
			}
		}
		if ids, ok := m.nameIdx[db]; ok {
			id = ids[resolved]
		}
	}
	if id != 0 {
		// Partition DDL changes collection-level fields such as NumPartitions,
		// so evict the primary entry too (next lookup re-describes) -- through
		// the unified eviction routine: a raw delete would leave the entry's
		// alias hints behind, and a later drop + same-name recreate would
		// resurrect a dead alias through them (the drop's cleanup no-ops when
		// the entry is already gone).
		m.removeCollectionByID(ctx, id)
		// Drop the partition list unconditionally. evictCollectionEntryLocked
		// (inside removeCollectionByID) cleans it only when the primary exists;
		// if the primary is already gone but an owner-less partition entry
		// somehow lingers, a partition DDL means it is stale regardless, so evict
		// it directly. Idempotent when the primary path already cleaned it.
		m.stalePartitionCacheLocked(id)
	}

	mlog.Debug(ctx, "remove partition", mlog.String("db", database), mlog.Int64("collectionID", id), mlog.String("collection", collectionName), mlog.String("partition", partitionName))
}

// stalePartitionCacheLocked drops a collection's partition list. Called from
// evictCollectionEntryLocked under m.mu (with fills drained via fillMu), so a
// plain delete is safe: any in-flight fill has already written back and is
// removed here, and any request still routing over a list it already read holds
// it by pointer (Go's GC keeps it alive). Forget the singleflight key too, so a
// fill issued after this invalidation starts fresh instead of joining a
// pre-invalidation flight.
func (m *MetaCache) stalePartitionCacheLocked(collectionID typeutil.UniqueID) {
	key := buildPartitionCacheKey(collectionID)
	m.sfPartitionCache.Forget(key)
	delete(m.partitionCache, key)
}

func (m *MetaCache) Close() {
	m.closeOnce.Do(func() {
		close(m.stopCh)
	})
}
