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

package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	internalhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/expr"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Cache is the interface for system meta data cache
//
//go:generate mockery --name=Cache --filename=mock_cache_test.go --outpkg=proxy --output=. --inpackage --structname=MockCache --with-expecter
type Cache interface {
	// GetCollectionID get collection's id by name.
	GetCollectionID(ctx context.Context, database, collectionName string) (typeutil.UniqueID, error)
	// GetCollectionName get collection's name and database by id
	GetCollectionName(ctx context.Context, database string, collectionID int64) (string, error)
	// GetCollectionInfo get collection's information by name or collection id, such as schema, and etc.
	GetCollectionInfo(ctx context.Context, database, collectionName string, collectionID int64) (*collectionInfo, error)
	// GetPartitionID get partition's identifier of specific collection.
	GetPartitionID(ctx context.Context, database, collectionName string, partitionName string) (typeutil.UniqueID, error)
	// GetPartitionName get partition's name by id
	GetPartitionName(ctx context.Context, database, collectionName string, partitionID int64) (string, error)
	// GetPartitions get all partitions' id of specific collection.
	GetPartitions(ctx context.Context, database, collectionName string) (map[string]typeutil.UniqueID, error)
	// GetPartitionInfo get partition's info.
	GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*partitionInfo, error)
	// GetPartitionsIndex returns a partition names in partition key indexed order.
	GetPartitionsIndex(ctx context.Context, database, collectionName string) ([]string, error)
	// GetCollectionSchema get collection's schema.
	GetCollectionSchema(ctx context.Context, database, collectionName string) (*schemaInfo, error)
	// ResolveCollectionAlias returns the actual collection name if input is an alias,
	// or returns the input name if it's already a collection name.
	ResolveCollectionAlias(ctx context.Context, database, nameOrAlias string) (string, error)
	// GetShard(ctx context.Context, withCache bool, database, collectionName string, collectionID int64, channel string) ([]nodeInfo, error)
	// GetShardLeaderList(ctx context.Context, database, collectionName string, collectionID int64, withCache bool) ([]string, error)
	// DeprecateShardCache(database, collectionName string)
	// InvalidateShardLeaderCache(collections []int64)
	// ListShardLocation() map[int64]nodeInfo
	RemoveCollection(ctx context.Context, database, collectionName string, version uint64)
	RemoveCollectionsByID(ctx context.Context, collectionID UniqueID, version uint64) []string

	// GetCredentialInfo operate credential cache
	// GetCredentialInfo(ctx context.Context, username string) (*internalpb.CredentialInfo, error)
	// RemoveCredential(username string)
	// UpdateCredential(credInfo *internalpb.CredentialInfo)

	// GetPrivilegeInfo(ctx context.Context) []string
	// GetUserRole(username string) []string
	// RefreshPolicyInfo(op typeutil.CacheOp) error
	// InitPolicyInfo(info []string, userRoles []string)

	// RemoveAlias removes a cached alias entry.
	RemoveAlias(ctx context.Context, database, alias string)
	// RemoveDatabase evicts every cached entry of the database. version is the
	// invalidation broadcast's timestamp, used to raise the per-collection version
	// floor so an in-flight describe cannot resurrect an evicted entry; 0 skips
	// the floor (best-effort local cleanup).
	RemoveDatabase(ctx context.Context, database string, version uint64)
	HasDatabase(ctx context.Context, database string) bool
	GetDatabaseInfo(ctx context.Context, database string) (*databaseInfo, error)
	// AllocID is only using on requests that need to skip timestamp allocation, don't overuse it.
	AllocID(ctx context.Context) (int64, error)

	RemovePartition(ctx context.Context, database string, collectionID UniqueID, collectionName string, partitionName string, version uint64)
	Close()
}

type collectionInfo struct {
	collID                typeutil.UniqueID
	dbID                  typeutil.UniqueID
	dbName                string
	schema                *schemaInfo
	createdTimestamp      uint64
	createdUtcTimestamp   uint64
	consistencyLevel      commonpb.ConsistencyLevel
	partitionKeyIsolation bool
	queryMode             string
	updateTimestamp       uint64
	collectionTTL         uint64
	numPartitions         int64
	vChannels             []string
	pChannels             []string
	shardsNum             int32
	aliases               []string
	properties            []*commonpb.KeyValuePair
	dbGen                 uint64 // database generation this entry was cached at; stale when != dbGen[dbName]
}

const aliasCacheNegativeTTL = 30 * time.Second

type aliasEntry struct {
	collectionName string    // real collection name; "" means negative cache (not an alias)
	cachedAt       time.Time // when this entry was cached; used for TTL on negative entries
}

type databaseInfo struct {
	dbID             typeutil.UniqueID
	properties       []*commonpb.KeyValuePair
	createdTimestamp uint64
}

// schemaInfo is a helper function wraps *schemapb.CollectionSchema
// with extra fields mapping and methods
type schemaInfo struct {
	*schemapb.CollectionSchema
	fieldMap              *typeutil.ConcurrentMap[string, int64] // field name to id mapping
	hasPartitionKeyField  bool
	pkField               *schemapb.FieldSchema
	multiAnalyzerFieldMap *typeutil.ConcurrentMap[int64, int64] // multi analzyer field id to dependent field id mapping
	schemaHelper          *typeutil.SchemaHelper
}

func newSchemaInfo(schema *schemapb.CollectionSchema) *schemaInfo {
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
	schemaHelper, _ := typeutil.CreateSchemaHelper(schema)
	return &schemaInfo{
		CollectionSchema:      schema,
		fieldMap:              fieldMap,
		hasPartitionKeyField:  hasPartitionkey,
		pkField:               pkField,
		multiAnalyzerFieldMap: typeutil.NewConcurrentMap[int64, int64](),
		schemaHelper:          schemaHelper,
	}
}

func (s *schemaInfo) MapFieldID(name string) (int64, bool) {
	return s.fieldMap.Get(name)
}

func (s *schemaInfo) IsPartitionKeyCollection() bool {
	return s.hasPartitionKeyField
}

func (s *schemaInfo) GetPkField() (*schemapb.FieldSchema, error) {
	if s.pkField == nil {
		return nil, merr.WrapErrFieldNotFound("pk field")
	}
	return s.pkField, nil
}

func (s *schemaInfo) GetMultiAnalyzerNameFieldID(id int64) (int64, error) {
	if id, ok := s.multiAnalyzerFieldMap.Get(id); ok {
		return id, nil
	}

	field, err := s.schemaHelper.GetFieldFromID(id)
	if err != nil {
		return 0, err
	}

	helper := typeutil.CreateFieldSchemaHelper(field)

	params, ok := helper.GetMultiAnalyzerParams()
	if !ok {
		s.multiAnalyzerFieldMap.Insert(id, 0)
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
	analyzerField, err := s.schemaHelper.GetFieldFromName(analyzerFieldName)
	if err != nil {
		return 0, err
	}

	s.multiAnalyzerFieldMap.Insert(id, analyzerField.GetFieldID())
	return analyzerField.GetFieldID(), nil
}

// GetLoadFieldIDs returns field id for load field list.
// If input `loadFields` is empty, use collection schema definition.
// Otherwise, perform load field list constraint check then return field id.
func (s *schemaInfo) GetLoadFieldIDs(loadFields []string, skipDynamicField bool) ([]int64, error) {
	if len(loadFields) == 0 {
		// skip check logic since create collection already did the rule check already
		return common.GetCollectionLoadFields(s.CollectionSchema, skipDynamicField), nil
	}

	fieldIDs := typeutil.NewSet[int64]()
	// fieldIDs := make([]int64, 0, len(loadFields))
	fields := make([]*schemapb.FieldSchema, 0, len(loadFields))
	for _, name := range loadFields {
		// todo(SpadeA): check struct field
		if structArrayField := s.schemaHelper.GetStructArrayFieldFromName(name); structArrayField != nil {
			for _, field := range structArrayField.GetFields() {
				fields = append(fields, field)
				fieldIDs.Insert(field.GetFieldID())
			}
			continue
		}

		fieldSchema, err := s.schemaHelper.GetFieldFromName(name)
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

func (s *schemaInfo) validateLoadFields(names []string, fields []*schemapb.FieldSchema) error {
	// ignore error if not found
	partitionKeyField, _ := s.schemaHelper.GetPartitionKeyField()
	clusteringKeyField, _ := s.schemaHelper.GetClusteringKeyField()

	var hasPrimaryKey, hasPartitionKey, hasClusteringKey, hasVector bool
	for _, field := range fields {
		if field.GetFieldID() == s.pkField.GetFieldID() {
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
		return merr.WrapErrParameterInvalidMsg("load field list %v does not contain primary key field %s", names, s.pkField.GetName())
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

func (s *schemaInfo) CanRetrieveRawFieldData(field *schemapb.FieldSchema) bool {
	return s.schemaHelper.CanRetrieveRawFieldData(field)
}

// partitionInfos contains the cached collection partition informations.
type partitionInfos struct {
	partitionInfos        []*partitionInfo
	name2Info             map[string]*partitionInfo // map[int64]*partitionInfo
	name2ID               map[string]int64          // map[int64]*partitionInfo
	indexedPartitionNames []string
}

// partitionInfo single model for partition information.
type partitionInfo struct {
	name                string
	partitionID         typeutil.UniqueID
	createdTimestamp    uint64
	createdUtcTimestamp uint64
	isDefault           bool
}

func (info *collectionInfo) isCollectionCached() bool {
	return info != nil && info.collID != UniqueID(0) && info.schema != nil
}

// make sure MetaCache implements Cache.
var _ Cache = (*MetaCache)(nil)

// MetaCache implements Cache, provides collection meta cache based on internal RootCoord
type MetaCache struct {
	mixCoord types.MixCoordClient

	dbInfo map[string]*databaseInfo // database -> db_info

	// collections is the PRIMARY store and the single source of truth: an entry
	// exists if and only if its cluster-unique collection id is in this map, and
	// its dbGen matches the database's current generation. Invalidation by id is
	// one map delete; nothing else needs synchronous cleanup.
	collections map[UniqueID]*collectionInfo // collection id -> entry
	// nameIdx and aliasInfo are HINTS: they resolve a (db, name) or (db, alias)
	// to the primary store and are validated against it on every read (the id
	// must exist, the entry's identity must still match, the db generation must
	// be current). A stale hint is at worst one extra describe, never a stale
	// read, so hints are cleaned lazily by the background GC instead of being
	// kept transactionally in sync with evictions.
	nameIdx   map[string]map[string]UniqueID    // database -> real collection name -> id
	aliasInfo map[string]map[string]*aliasEntry // database -> alias -> target name ("" = negative, TTL'd)
	// dbGen is the database generation: RemoveDatabase (DropDatabase /
	// AlterDatabase) bumps it, instantly invalidating every primary entry cached
	// under that database in O(1) -- entries carry the generation they were
	// cached at and reads compare. Entries never deleted; one counter per db
	// name ever seen.
	dbGen map[string]uint64

	credMap        map[string]*internalpb.CredentialInfo // cache for credential, lazy load
	privilegeInfos map[string]struct{}                   // privileges cache
	userToRoles    map[string]map[string]struct{}        // user to role cache
	mu             sync.RWMutex
	credMut        sync.RWMutex

	sfGlobal conc.Singleflight[*collectionInfo]
	sfDB     conc.Singleflight[*databaseInfo]

	IDStart int64
	IDCount int64
	IDIndex int64
	IDLock  sync.RWMutex

	// fillMu orders cache fills against invalidations, replacing the previous
	// per-collection version floor (collectionCacheVersion). A fill (an RPC to
	// rootcoord plus its write-back) holds the READ side for its whole duration;
	// an invalidation takes the WRITE side, so it drains every in-flight fill
	// before evicting. Any fill that read a pre-DDL snapshot has therefore
	// already written back BEFORE the eviction runs (and is cleaned by it), and
	// any fill issued after the eviction reads post-DDL state (rootcoord commits
	// before broadcasting) -- no timestamp comparison needed, and it also covers
	// write-backs the floor could not (alias resolution entries, RequestTime==0
	// responses from older rootcoords).
	// Costs, accepted by design: an invalidation waits for the slowest in-flight
	// fill (bounded by the describe RPC timeout; invalidations are rare), and
	// fills issued while an invalidation is pending briefly block behind it.
	// Cache HITS never touch this lock; fills don't block each other.
	// Lock order: fillMu before m.mu. Deadlock-free against rootcoord: ack
	// callbacks release the meta lock before the expiration fan-out, so a fill's
	// describe RPC is never blocked by the DDL whose invalidation is waiting on
	// that fill.
	fillMu sync.RWMutex

	partitionCache          *VersionCache[string, *partitionInfo]  // partitionName -> partitionInfo
	collLevelPartitionCache *VersionCache[string, *partitionInfos] // collectionName -> partitionInfos

	sfPartitionCache          conc.Singleflight[*partitionInfo]
	sfCollLevelPartitionCache conc.Singleflight[*partitionInfos]

	stopCh    chan struct{}
	closeOnce sync.Once
}

// globalMetaCache is singleton instance of Cache
var globalMetaCache Cache

// InitMetaCache initializes globalMetaCache
func InitMetaCache(ctx context.Context, mixCoord types.MixCoordClient) error {
	var err error
	globalMetaCache, err = NewMetaCache(mixCoord)
	if err != nil {
		return err
	}
	expr.Register("cache", globalMetaCache)

	err = privilege.InitPrivilegeCache(ctx, mixCoord)
	if err != nil {
		mlog.Error(context.TODO(), "failed to init privilege cache", mlog.Err(err))
		return err
	}

	// Register password verify function for /expr endpoint authentication
	internalhttp.RegisterPasswordVerifyFunc(PasswordVerify)

	// Register get user role function for /expr endpoint RBAC check
	internalhttp.RegisterGetUserRoleFunc(GetRole)

	return nil
}

// NewMetaCache creates a MetaCache with provided RootCoord and QueryNode
func NewMetaCache(mixCoord types.MixCoordClient) (*MetaCache, error) {
	metaCache := &MetaCache{
		mixCoord:                mixCoord,
		dbInfo:                  map[string]*databaseInfo{},
		aliasInfo:               map[string]map[string]*aliasEntry{},
		collections:             map[UniqueID]*collectionInfo{},
		nameIdx:                 map[string]map[string]UniqueID{},
		dbGen:                   map[string]uint64{},
		credMap:                 map[string]*internalpb.CredentialInfo{},
		privilegeInfos:          map[string]struct{}{},
		userToRoles:             map[string]map[string]struct{}{},
		partitionCache:          NewVersionCache[string, *partitionInfo](),
		collLevelPartitionCache: NewVersionCache[string, *partitionInfos](),
		stopCh:                  make(chan struct{}),
		closeOnce:               sync.Once{},
	}
	metaCache.backgroundGCLoop(metaCache.stopCh)
	return metaCache, nil
}

// liveLocked returns the primary entry for id if it exists and its database
// generation is current. Caller must hold m.mu (read or write).
func (m *MetaCache) liveLocked(collectionID UniqueID) (*collectionInfo, bool) {
	entry, ok := m.collections[collectionID]
	if !ok {
		return nil, false
	}
	if entry.dbGen != m.dbGen[entry.dbName] {
		// the database was dropped or altered after this entry was cached
		return nil, false
	}
	return entry, true
}

func (m *MetaCache) getCollection(database, collectionName string, collectionID UniqueID) (*collectionInfo, bool) {
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
	// name (a rename leaves the old-name hint dangling and it must miss) in this
	// database. A failed validation is just a miss; the GC cleans the hint.
	if ids, ok := m.nameIdx[database]; ok {
		if id, ok := ids[collectionName]; ok {
			if entry, ok := m.liveLocked(id); ok &&
				entry.schema.GetName() == collectionName && normalizeDBName(entry.dbName) == database {
				return entry, entry.isCollectionCached()
			}
		}
	}
	// Alias hint -> real name -> name hint -> primary, same validation chain.
	if aliasDB, ok := m.aliasInfo[database]; ok {
		if aliasHint, ok := aliasDB[collectionName]; ok && aliasHint.collectionName != "" {
			if ids, ok := m.nameIdx[database]; ok {
				if id, ok := ids[aliasHint.collectionName]; ok {
					if entry, ok := m.liveLocked(id); ok &&
						entry.schema.GetName() == aliasHint.collectionName && normalizeDBName(entry.dbName) == database {
						return entry, entry.isCollectionCached()
					}
				}
			}
		}
	}

	return nil, false
}

func (m *MetaCache) update(ctx context.Context, database, collectionName string, collectionID UniqueID) (*collectionInfo, error) {
	if collInfo, ok := m.getCollection(database, collectionName, collectionID); ok {
		return collInfo, nil
	}

	// Fill: hold the fill lock across the RPC AND the write-back, so an
	// invalidation (write side) drains this fill first and its eviction runs
	// strictly after our write -- a pre-DDL snapshot can never outlive the DDL's
	// eviction. See the fillMu field comment.
	m.fillMu.RLock()
	defer m.fillMu.RUnlock()

	collection, err := m.describeCollection(ctx, database, collectionName, collectionID)
	if err != nil {
		return nil, err
	}

	realName := collection.Schema.GetName()
	originalName := collectionName
	isAlias := collectionName != "" && realName != "" && realName != collectionName
	if collectionName == "" || isAlias {
		collectionName = realName
	}
	isolation, err := common.IsPartitionKeyIsolationKvEnabled(collection.Properties...)
	if err != nil {
		return nil, err
	}
	queryMode := common.GetQueryMode(collection.Properties...)

	schemaInfo := newSchemaInfo(collection.Schema)

	// The primary store is keyed by the cluster-unique collection id, so EVERY
	// describe response is cacheable -- including an id lookup whose response
	// omits the db (older rootcoord): it is served by id and simply gets no name
	// hint until a by-name lookup supplies the authoritative database.
	bucketDB := collection.GetDbName()
	if bucketDB == "" && collectionID == 0 {
		// By-name lookup: the request database is authoritative.
		bucketDB = normalizeDBName(database)
	}

	info := newCollectionInfo(collection, schemaInfo, isolation, queryMode)
	if info.dbName == "" {
		// authoritative for by-name fills; stays "" only for old-rootcoord id
		// fills, whose db is genuinely unknown
		info.dbName = bucketDB
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	info.dbGen = m.dbGen[info.dbName]
	m.collections[collection.GetCollectionID()] = info

	if bucketDB != "" {
		// name hint; overwrites any stale hint for this name (rename, or a
		// drop+recreate reusing it) -- old hints self-invalidate on read
		ids, ok := m.nameIdx[bucketDB]
		if !ok {
			ids = make(map[string]UniqueID)
			m.nameIdx[bucketDB] = ids
		}
		ids[collectionName] = collection.GetCollectionID()

		if isAlias {
			// Caller passed an alias; record the alias→realName hint so
			// subsequent ResolveCollectionAlias calls hit Level 2 cache.
			m.setAliasLocked(bucketDB, originalName, &aliasEntry{collectionName: realName, cachedAt: time.Now()})
		}
		// Hint this collection's own aliases too, so an alias DDL broadcast from
		// an older rootcoord (alias name only, CollectionID=0) can still resolve
		// the alias to the primary entry and evict it. Newer rootcoords forward
		// the target collection ids explicitly, which does not depend on this.
		for _, alias := range collection.Aliases {
			if alias != "" && alias != collectionName {
				m.setAliasLocked(bucketDB, alias, &aliasEntry{collectionName: collectionName, cachedAt: time.Now()})
			}
		}
	}

	mlog.Info(ctx, "meta update success", mlog.String("requestDatabase", database), mlog.String("database", bucketDB),
		mlog.String("collectionName", collectionName),
		mlog.String("actual collection Name", collection.Schema.GetName()), mlog.Int64("collectionID", collection.CollectionID),
		mlog.Uint64("version", collection.GetRequestTime()), mlog.Any("aliases", collection.Aliases),
		mlog.Bool("partition key isolation", isolation), mlog.String("queryMode", queryMode),
	)

	return info, nil
}

// normalizeDBName maps an empty database name to the default database,
// mirroring rootcoord's backward-compat normalization for name lookups
// (meta_table.getCollectionByNameInternal).
func normalizeDBName(database string) string {
	if database == "" {
		return defaultDB
	}
	return database
}

// newCollectionInfo builds a collectionInfo from a describe response. It does
// not touch any cache map, so it is safe to use for entries that are returned
// without being cached.
func newCollectionInfo(collection *milvuspb.DescribeCollectionResponse, schemaInfo *schemaInfo, isolation bool, queryMode string) *collectionInfo {
	return &collectionInfo{
		collID:                collection.CollectionID,
		dbID:                  collection.GetDbId(),
		dbName:                collection.GetDbName(),
		schema:                schemaInfo,
		createdTimestamp:      collection.CreatedTimestamp,
		createdUtcTimestamp:   collection.CreatedUtcTimestamp,
		consistencyLevel:      collection.ConsistencyLevel,
		partitionKeyIsolation: isolation,
		queryMode:             queryMode,
		updateTimestamp:       collection.UpdateTimestamp,
		collectionTTL:         getCollectionTTL(schemaInfo.GetProperties()),
		vChannels:             collection.VirtualChannelNames,
		pChannels:             collection.PhysicalChannelNames,
		numPartitions:         collection.NumPartitions,
		shardsNum:             collection.ShardsNum,
		aliases:               collection.Aliases,
		properties:            collection.Properties,
	}
}

func buildSfKeyByName(database, collectionName string) string {
	return database + "-" + collectionName
}

func buildSfKeyById(database string, collectionID UniqueID) string {
	return database + "--" + fmt.Sprint(collectionID)
}

// Partition caches are keyed by the collection id: the primary store is
// id-keyed, ids are cluster-unique and never reused, so a drop+recreate can
// never alias into old partition entries, and no name/alias expansion is ever
// needed to invalidate them.
func buildPartitionKey(collectionID UniqueID, partitionName string) string {
	return fmt.Sprint(collectionID) + "-" + partitionName
}

func buildCollLevelPartitionKey(collectionID UniqueID) string {
	return fmt.Sprint(collectionID)
}

// partitionKeyOwner extracts the collection id prefix of a partition-cache key
// (both key shapes above); used by the GC to reclaim entries whose collection
// left the primary store. Returns 0 when unparsable.
func partitionKeyOwner(key string) UniqueID {
	idPart := key
	if i := strings.IndexByte(key, '-'); i >= 0 {
		idPart = key[:i]
	}
	id, err := strconv.ParseInt(idPart, 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func (m *MetaCache) UpdateByName(ctx context.Context, database, collectionName string) (*collectionInfo, error) {
	database = normalizeDBName(database)
	collection, err, _ := m.sfGlobal.Do(buildSfKeyByName(database, collectionName), func() (*collectionInfo, error) {
		return m.update(ctx, database, collectionName, 0)
	})
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

func (m *MetaCache) UpdateByID(ctx context.Context, database string, collectionID UniqueID) (*collectionInfo, error) {
	// Do not normalize an empty db to default here: update() needs to tell an
	// external id-only lookup (no db context) apart from an internal by-id
	// refresh that carries a real db, to decide whether it may cache by name.
	collection, err, _ := m.sfGlobal.Do(buildSfKeyById(database, collectionID), func() (*collectionInfo, error) {
		return m.update(ctx, database, "", collectionID)
	})
	return collection, err
}

// GetCollectionID returns the corresponding collection id for provided collection name
func (m *MetaCache) GetCollectionID(ctx context.Context, database, collectionName string) (UniqueID, error) {
	method := "GetCollectionID"
	collInfo, ok := m.getCollection(database, collectionName, 0)
	if !ok {
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")

		collInfo, err := m.UpdateByName(ctx, database, collectionName)
		if err != nil {
			return UniqueID(0), err
		}

		metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return collInfo.collID, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheHitLabel).Inc()

	return collInfo.collID, nil
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
		return collInfo.schema.Name, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheHitLabel).Inc()

	return collInfo.schema.Name, nil
}

func (m *MetaCache) GetCollectionInfo(ctx context.Context, database string, collectionName string, collectionID int64) (*collectionInfo, error) {
	// Pass collectionID through so an id-only lookup (collectionName == "") hits
	// the cluster-wide id index instead of probing id 0, which always misses and
	// forces a needless UpdateByID/singleflight round plus a spurious cache-miss
	// metric. For name lookups getCollection ignores the id.
	collInfo, ok := m.getCollection(database, collectionName, collectionID)

	method := "GetCollectionInfo"
	// if collInfo.collID != collectionID, means that the cache is not trustable
	// try to get collection according to collectionID
	// Why use collectionID? Because the collectionID is not always provided in the proxy.
	if !ok || (collectionID != 0 && collInfo.collID != collectionID) {
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

func (m *MetaCache) GetCollectionSchema(ctx context.Context, database, collectionName string) (*schemaInfo, error) {
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
		return collInfo.schema, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheHitLabel).Inc()

	return collInfo.schema, nil
}

func (m *MetaCache) getAlias(database, alias string) (*aliasEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if db, ok := m.aliasInfo[normalizeDBName(database)]; ok {
		if entry, ok := db[alias]; ok {
			// Expire negative cache entries after TTL
			if entry.collectionName == "" && time.Since(entry.cachedAt) > aliasCacheNegativeTTL {
				return nil, false
			}
			return entry, true
		}
	}
	return nil, false
}

// setAliasLocked sets an alias hint. Caller must hold m.mu write lock.
func (m *MetaCache) setAliasLocked(database, alias string, entry *aliasEntry) {
	database = normalizeDBName(database)
	if _, ok := m.aliasInfo[database]; !ok {
		m.aliasInfo[database] = make(map[string]*aliasEntry)
	}
	m.aliasInfo[database][alias] = entry
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

func (m *MetaCache) ResolveCollectionAlias(ctx context.Context, database, nameOrAlias string) (string, error) {
	// Level 1: Found in collection cache. update() keys entries by the real
	// collection name, but getCollection may have resolved nameOrAlias through
	// the alias map, so compare with the schema's real name to return it.
	if collInfo, ok := m.getCollection(database, nameOrAlias, 0); ok {
		if collInfo.schema != nil {
			if realName := collInfo.schema.GetName(); realName != "" && realName != nameOrAlias {
				return realName, nil
			}
		}
		return nameOrAlias, nil
	}

	// Level 2: Found in alias cache
	if entry, ok := m.getAlias(database, nameOrAlias); ok {
		if entry.collectionName == "" {
			// Negative cache: not an alias, return as-is
			return nameOrAlias, nil
		}
		return entry.collectionName, nil
	}

	// Level 3: Cache miss, call DescribeAlias RPC. This is a fill (RPC +
	// write-back into aliasInfo), so hold the fill read lock across it: an alias
	// DDL's invalidation then drains this resolution before removing the alias
	// entry, and a pre-DDL DescribeAlias response can never outlive the removal.
	m.fillMu.RLock()
	defer m.fillMu.RUnlock()
	resp, err := m.mixCoord.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{
		DbName: database,
		Alias:  nameOrAlias,
	})
	if err != nil {
		return "", err
	}
	if err = merr.CheckRPCCall(resp, nil); err != nil {
		if errors.Is(err, merr.ErrAliasNotFound) || errors.Is(err, merr.ErrCollectionNotFound) {
			// Negative cache: this name is not an alias
			m.mu.Lock()
			m.setAliasLocked(database, nameOrAlias, &aliasEntry{collectionName: "", cachedAt: time.Now()})
			m.mu.Unlock()
			return nameOrAlias, nil
		}
		return "", err
	}

	if resp.GetCollection() == "" {
		// Negative cache
		m.mu.Lock()
		m.setAliasLocked(database, nameOrAlias, &aliasEntry{collectionName: "", cachedAt: time.Now()})
		m.mu.Unlock()
		return nameOrAlias, nil
	}

	// Positive cache: alias -> real collection name
	m.mu.Lock()
	m.setAliasLocked(database, nameOrAlias, &aliasEntry{collectionName: resp.GetCollection(), cachedAt: time.Now()})
	m.mu.Unlock()
	return resp.GetCollection(), nil
}

func (m *MetaCache) GetPartitionID(ctx context.Context, database, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	partInfo, err := m.GetPartitionInfo(ctx, database, collectionName, partitionName)
	if err != nil {
		return 0, err
	}
	return partInfo.partitionID, nil
}

func (m *MetaCache) GetPartitionName(ctx context.Context, database, collectionName string, partitionID int64) (string, error) {
	partitions, err := m.GetPartitionInfos(ctx, database, collectionName)
	if err != nil {
		return "", err
	}

	for _, info := range partitions.partitionInfos {
		if info.partitionID == partitionID {
			return info.name, nil
		}
	}

	return "", merr.WrapErrPartitionNotFound(partitionID)
}

func (m *MetaCache) GetPartitions(ctx context.Context, database, collectionName string) (map[string]typeutil.UniqueID, error) {
	partitions, err := m.GetPartitionInfos(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}

	return partitions.name2ID, nil
}

// resolvePartitionCollection resolves the caller-supplied (database, name) --
// which may be an alias and/or an empty db -- to the collection entry, filling
// the collection cache on a miss. Partition caches are keyed by the entry's
// cluster-unique id, so every collection has exactly ONE partition-cache
// location no matter how it is addressed, invalidation stales exact id-keys,
// and a drop+recreate can never alias into old entries (new id). It also
// returns the canonical (db, name) to address rootcoord with.
func (m *MetaCache) resolvePartitionCollection(ctx context.Context, database, collectionName string) (UniqueID, string, string, error) {
	info, err := m.GetCollectionInfo(ctx, database, collectionName, 0)
	if err != nil {
		return 0, "", "", err
	}
	db := info.dbName
	if db == "" {
		db = normalizeDBName(database)
	}
	name := collectionName
	if info.schema != nil && info.schema.GetName() != "" {
		name = info.schema.GetName()
	}
	return info.collID, db, name, nil
}

func (m *MetaCache) GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*partitionInfo, error) {
	// Handle empty partitionName - use default partition
	if partitionName == "" {
		partitionName = Params.CommonCfg.DefaultPartitionName.GetValue()
	}
	collectionID, rpcDB, rpcName, err := m.resolvePartitionCollection(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}

	key := buildPartitionKey(collectionID, partitionName)
	entry, ok, release := m.partitionCache.Lookup(key)
	defer release(entry)
	if ok && entry.state == EntryStateActive && entry.value != nil {
		return entry.value, nil
	}

	collectionKey := buildCollLevelPartitionKey(collectionID)
	_, err, _ = m.sfPartitionCache.Do(collectionKey, func() (*partitionInfo, error) {
		// Fill: hold the fill read lock across the RPC and the batch insert, so
		// an invalidation drains this fill before staling the partition entries.
		m.fillMu.RLock()
		defer m.fillMu.RUnlock()
		// as rootcoord does not support show partitions by partition name, we need to get all partitions first.
		resp, err := m.showPartitions(ctx, rpcDB, rpcName, 0)
		if err != nil {
			return nil, err
		}
		keys := make([]string, 0)
		values := make([]*partitionInfo, 0)
		versions := make([]uint64, 0)
		var ret *partitionInfo
		for i := range resp.PartitionNames {
			keys = append(keys, buildPartitionKey(collectionID, resp.PartitionNames[i]))
			values = append(values, &partitionInfo{
				name:                resp.PartitionNames[i],
				partitionID:         resp.PartitionIDs[i],
				createdTimestamp:    resp.CreatedTimestamps[i],
				createdUtcTimestamp: resp.CreatedUtcTimestamps[i],
			})
			versions = append(versions, resp.CreatedTimestamps[i])
			if resp.PartitionNames[i] == partitionName {
				ret = values[i]
			}
		}
		m.partitionCache.InsertBatchWithoutRef(keys, values, versions)
		return ret, nil
	})
	if err != nil {
		return nil, err
	}

	entry, ok, release = m.partitionCache.Lookup(key)
	defer release(entry)
	if ok && entry.state == EntryStateActive && entry.value != nil {
		return entry.value, nil
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

	if partitions.indexedPartitionNames == nil {
		return nil, merr.WrapErrServiceInternal("partitions not in partition key naming pattern")
	}

	return partitions.indexedPartitionNames, nil
}

func (m *MetaCache) GetPartitionInfos(ctx context.Context, database, collectionName string) (*partitionInfos, error) {
	method := "GetPartitionInfo"
	// See resolvePartitionCollection: entries are keyed by the collection id, so
	// they have exactly one location and invalidation stales exact id-keys.
	collectionID, rpcDB, rpcName, err := m.resolvePartitionCollection(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}
	key := buildCollLevelPartitionKey(collectionID)
	entry, ok, release := m.collLevelPartitionCache.Lookup(key)
	defer release(entry)
	if ok && entry.state == EntryStateActive && entry.value != nil {
		return entry.value, nil
	}
	tr := timerecord.NewTimeRecorder("UpdateCache")
	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheMissLabel).Inc()
	partitionsInfo, err, _ := m.sfCollLevelPartitionCache.Do(key, func() (*partitionInfos, error) {
		// Fill: see fillMu -- covers the describe + show RPCs and the insert.
		m.fillMu.RLock()
		defer m.fillMu.RUnlock()
		collection, err := m.describeCollection(ctx, rpcDB, rpcName, 0)
		if err != nil {
			return nil, err
		}
		schemaInfo := newSchemaInfo(collection.Schema)

		resp, err := m.showPartitions(ctx, rpcDB, rpcName, 0)
		if err != nil {
			return nil, err
		}
		partitions := make([]*partitionInfo, 0)
		for i, name := range resp.PartitionNames {
			partitions = append(partitions, &partitionInfo{
				name:                name,
				partitionID:         resp.PartitionIDs[i],
				createdTimestamp:    resp.CreatedTimestamps[i],
				createdUtcTimestamp: resp.CreatedUtcTimestamps[i],
			})
		}
		partitionsInfo := parsePartitionsInfo(partitions, schemaInfo.IsPartitionKeyCollection())
		entry, release := m.collLevelPartitionCache.Insert(key, partitionsInfo, collection.RequestTime)
		defer release(entry)
		metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return entry.value, nil
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

func (m *MetaCache) showPartitions(ctx context.Context, dbName string, collectionName string, collectionID UniqueID) (*milvuspb.ShowPartitionsResponse, error) {
	req := &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		DbName:         dbName,
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

func (m *MetaCache) describeDatabase(ctx context.Context, dbName string) (*rootcoordpb.DescribeDatabaseResponse, error) {
	req := &rootcoordpb.DescribeDatabaseRequest{
		DbName: dbName,
	}

	resp, err := m.mixCoord.DescribeDatabase(ctx, req)
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}

	return resp, nil
}

// parsePartitionsInfo parse partitionInfo list to partitionInfos struct.
// prepare all name to id & info map
// try parse partition names to partitionKey index.
func parsePartitionsInfo(infos []*partitionInfo, hasPartitionKey bool) *partitionInfos {
	name2ID := lo.SliceToMap(infos, func(info *partitionInfo) (string, int64) {
		return info.name, info.partitionID
	})
	name2Info := lo.SliceToMap(infos, func(info *partitionInfo) (string, *partitionInfo) {
		return info.name, info
	})

	result := &partitionInfos{
		partitionInfos: infos,
		name2ID:        name2ID,
		name2Info:      name2Info,
	}

	if !hasPartitionKey {
		return result
	}

	// Make sure the order of the partition names got every time is the same
	partitionNames := make([]string, len(infos))
	for _, info := range infos {
		partitionName := info.name
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
		partitionNames[index] = partitionName
	}

	result.indexedPartitionNames = partitionNames
	return result
}

// removeCollectionByAliasLocked, when alias is a cached alias hint in database,
// invalidates the collection it currently resolves to (by id) so an alias DDL
// from an OLDER rootcoord (alias name only, CollectionID=0) still evicts the
// target's primary entry. Newer rootcoords forward the target ids explicitly --
// including both sides of an AlterAlias -- which does not depend on this hint.
// Returns true if it found and invalidated (or cleaned up) a target. Caller
// must hold m.mu write lock.
func (m *MetaCache) removeCollectionByAliasLocked(ctx context.Context, database, alias string, version uint64) bool {
	aliases, ok := m.aliasInfo[database]
	if !ok {
		return false
	}
	entry, ok := aliases[alias]
	if !ok || entry == nil || entry.collectionName == "" {
		return false
	}
	if ids, ok := m.nameIdx[database]; ok {
		if id, ok := ids[entry.collectionName]; ok {
			m.removeCollectionByID(ctx, id, version)
			return true
		}
	}
	// dangling hint (its target was never cached by name, or already evicted):
	// drop it so it cannot mis-resolve a later ResolveCollectionAlias (whose
	// Level-2 hit feeds RBAC object resolution).
	delete(aliases, alias)
	return true
}

func (m *MetaCache) RemoveCollection(ctx context.Context, database, collectionName string, version uint64) {
	// Invalidation: drain in-flight fills first, see fillMu.
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	// All hint buckets are keyed by the normalized database.
	database = normalizeDBName(database)
	if ids, ok := m.nameIdx[database]; ok {
		if id, ok := ids[collectionName]; ok {
			m.removeCollectionByID(ctx, id, version)
		}
	}
	// The name may be an alias (alias DDL from an older rootcoord broadcasts
	// only the alias name); resolve it to its target and evict that primary
	// entry too. No further cleanup is needed either way: hints are validated
	// against the primary on read and reclaimed by the GC.
	m.removeCollectionByAliasLocked(ctx, database, collectionName, version)
	mlog.Debug(ctx, "remove collection", mlog.String("db", database), mlog.String("collection", collectionName))
}

func (m *MetaCache) RemoveCollectionsByID(ctx context.Context, collectionID UniqueID, version uint64) []string {
	// Invalidation: drain in-flight fills first, see fillMu.
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removeCollectionByID(ctx, collectionID, version)
}

// removeCollectionByID deletes the primary entry -- the single source of truth
// -- which atomically invalidates every hint and partition entry referencing the
// id (they validate against the primary on read). Nothing else needs synchronous
// cleanup; the GC reclaims dangling hints and orphaned partition entries.
// Partition entries deliberately survive collection-level invalidation: partition
// DDL has its own broadcast, and after a drop the recreated collection gets a new
// id, so old entries are unreachable. No staleness check either: fills are
// serialized against invalidations by fillMu, so eviction is always safe; a
// duplicate or late broadcast at worst causes one extra re-describe.
// Caller must hold m.mu write lock (and fillMu write lock).
func (m *MetaCache) removeCollectionByID(ctx context.Context, collectionID UniqueID, version uint64) []string {
	var collNames []string
	if entry, ok := m.collections[collectionID]; ok {
		if entry.schema != nil && entry.schema.GetName() != "" {
			collNames = append(collNames, entry.schema.GetName())
		}
		delete(m.collections, collectionID)
	}
	mlog.Debug(ctx, "remove collection by id", mlog.Int64("id", collectionID),
		mlog.Strings("collection", collNames), mlog.Uint64("version", version))
	return collNames
}

func (m *MetaCache) RemoveDatabase(ctx context.Context, database string, version uint64) {
	mlog.Debug(ctx, "remove database", mlog.String("name", database), mlog.Uint64("version", version))
	// Invalidation: drain in-flight fills first, so a describe issued before the
	// DropDatabase/AlterDatabase broadcast cannot write its pre-DDL response back
	// afterwards and resurrect an entry of this database. See fillMu.
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	// O(1): bumping the database generation makes every primary entry cached
	// under this database invisible to reads (entries carry the generation they
	// were cached at). The entries, their hints and their partition entries are
	// reclaimed lazily by the GC.
	m.dbGen[database]++
	delete(m.dbInfo, database)
	delete(m.nameIdx, database)
	delete(m.aliasInfo, database)
}

func (m *MetaCache) HasDatabase(ctx context.Context, database string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.nameIdx[database]
	return ok
}

func (m *MetaCache) GetDatabaseInfo(ctx context.Context, database string) (*databaseInfo, error) {
	dbInfo := m.safeGetDBInfo(database)
	if dbInfo != nil {
		return dbInfo, nil
	}

	dbInfo, err, _ := m.sfDB.Do(database, func() (*databaseInfo, error) {
		// Fill: see fillMu -- RemoveDatabase drains this before evicting dbInfo.
		m.fillMu.RLock()
		defer m.fillMu.RUnlock()
		resp, err := m.describeDatabase(ctx, database)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		dbInfo := &databaseInfo{
			dbID:             resp.GetDbID(),
			properties:       resp.Properties,
			createdTimestamp: resp.GetCreatedTimestamp(),
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

func (m *MetaCache) safeGetDBInfo(database string) *databaseInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	db, ok := m.dbInfo[database]
	if !ok {
		return nil
	}
	return db
}

func (m *MetaCache) AllocID(ctx context.Context) (int64, error) {
	m.IDLock.Lock()
	defer m.IDLock.Unlock()

	if m.IDIndex == m.IDCount {
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
		m.IDStart, m.IDCount = resp.GetID(), int64(resp.GetCount())
		m.IDIndex = 0
	}
	id := m.IDStart + m.IDIndex
	m.IDIndex++
	return id, nil
}

func (m *MetaCache) RemovePartition(ctx context.Context, database string, collectionID UniqueID, collectionName string, partitionName string, version uint64) {
	// The ensure step is itself a fill (it may describe), so it MUST run before
	// taking the fill write lock below or it would self-deadlock.
	ensured := m.ensureCollectionForPartitionInvalidation(ctx, database, collectionID, collectionName)

	// Invalidation: drain in-flight fills first, see fillMu.
	m.fillMu.Lock()
	defer m.fillMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	// Partition entries are keyed by the collection id, so a partition DDL
	// invalidates exact id-keys -- no name or alias expansion. Resolve the id
	// from the broadcast, the ensured describe, or the name hints.
	id := collectionID
	if id == 0 && ensured != nil {
		id = ensured.collID
	}
	if id == 0 && collectionName != "" {
		db := normalizeDBName(database)
		resolved := collectionName
		if aliasDB, ok := m.aliasInfo[db]; ok {
			if entry, ok := aliasDB[collectionName]; ok && entry.collectionName != "" {
				resolved = entry.collectionName
			}
		}
		if ids, ok := m.nameIdx[db]; ok {
			id = ids[resolved]
		}
	}
	if id != 0 {
		// Partition DDL changes collection-level fields such as NumPartitions,
		// so evict the primary entry too (next lookup re-describes). No
		// staleness check: fills are serialized against invalidations by fillMu,
		// so a duplicate or late broadcast at worst causes one extra re-describe.
		delete(m.collections, id)
		m.stalePartitionCacheLocked(id, partitionName, version)
	}

	mlog.Debug(ctx, "remove partition", mlog.String("db", database), mlog.Int64("collectionID", id), mlog.String("collection", collectionName), mlog.String("partition", partitionName), mlog.Uint64("version", version))
}

// ensureCollectionForPartitionInvalidation makes sure the collection metadata
// needed to expand a partition invalidation (real name + aliases) is available,
// and returns it. The returned *collectionInfo may NOT be in the cache: an
// id-only lookup whose describe response omits DbName (older rootcoord during a
// rolling upgrade) is deliberately not cached by update(), yet the caller still
// needs its schema/aliases to invalidate the real-name partition entries — so
// we hand the fetched info back rather than relying on it being in collInfo.
// Returns nil only when the collection could not be resolved at all.
func (m *MetaCache) ensureCollectionForPartitionInvalidation(ctx context.Context, database string, collectionID UniqueID, collectionName string) *collectionInfo {
	if collectionID != 0 {
		// getCollection serves id lookups straight from the id index, db-agnostic.
		if info, ok := m.getCollection(database, "", collectionID); ok {
			return info
		}

		fetchDB := database
		if fetchDB == "" {
			fetchDB = defaultDB
		}
		info, err := m.UpdateByID(ctx, fetchDB, collectionID)
		if err != nil {
			mlog.Debug(ctx, "failed to refresh collection cache before partition invalidation",
				mlog.String("db", fetchDB),
				mlog.Int64("collectionID", collectionID),
				mlog.Err(err))
			return nil
		}
		return info
	}

	if collectionName == "" {
		return nil
	}

	// getCollection normalizes an empty db to default internally.
	if info, ok := m.getCollection(database, collectionName, 0); ok {
		return info
	}

	fetchDB := database
	if fetchDB == "" {
		fetchDB = defaultDB
	}
	info, err := m.UpdateByName(ctx, fetchDB, collectionName)
	if err != nil {
		mlog.Debug(ctx, "failed to refresh collection cache by name before partition invalidation",
			mlog.String("db", fetchDB),
			mlog.String("collection", collectionName),
			mlog.Err(err))
		return nil
	}
	return info
}

func (m *MetaCache) stalePartitionCacheLocked(collectionID UniqueID, partitionName string, version uint64) {
	collectionKey := buildCollLevelPartitionKey(collectionID)
	m.sfPartitionCache.Forget(collectionKey)
	m.partitionCache.Stale(buildPartitionKey(collectionID, partitionName), version)
	m.sfCollLevelPartitionCache.Forget(collectionKey)
	m.collLevelPartitionCache.Stale(collectionKey, version)
}

func (m *MetaCache) backgroundGCLoop(stopCh <-chan struct{}) {
	go func() {
		interval := Params.ProxyCfg.MetaCacheGCTimeInterval.GetAsDuration(time.Second)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.partitionCache.Prune()
				m.collLevelPartitionCache.Prune()
				m.sweep()

				newInterval := Params.ProxyCfg.MetaCacheGCTimeInterval.GetAsDuration(time.Second)
				if newInterval != interval {
					interval = newInterval
					ticker.Reset(interval)
				}
			case <-stopCh:
				return
			}
		}
	}()
}

// sweep reclaims memory the invalidation paths leave behind on purpose: primary
// entries whose database generation moved on, hints whose target no longer
// validates, and partition entries whose collection left the primary store.
// Correctness never depends on this -- reads validate against the primary -- so
// it runs in the background GC loop: collect candidates under the read lock,
// re-verify and delete under a short write lock.
func (m *MetaCache) sweep() {
	m.mu.RLock()
	deadIDs := make([]UniqueID, 0)
	for id, entry := range m.collections {
		if entry.dbGen != m.dbGen[entry.dbName] {
			deadIDs = append(deadIDs, id)
		}
	}
	type dbName struct{ db, name string }
	deadNames := make([]dbName, 0)
	for db, ids := range m.nameIdx {
		for name, id := range ids {
			entry, ok := m.collections[id]
			if !ok || entry.dbGen != m.dbGen[entry.dbName] ||
				entry.schema.GetName() != name || normalizeDBName(entry.dbName) != db {
				deadNames = append(deadNames, dbName{db: db, name: name})
			}
		}
	}
	deadAliases := make([]dbName, 0)
	for db, aliases := range m.aliasInfo {
		for alias, hint := range aliases {
			if hint == nil {
				continue
			}
			if hint.collectionName == "" {
				// expired negative entries
				if time.Since(hint.cachedAt) > aliasCacheNegativeTTL {
					deadAliases = append(deadAliases, dbName{db: db, name: alias})
				}
				continue
			}
			// positive hints whose target name no longer resolves to a live
			// entry: dropping them is always safe (at worst one extra
			// DescribeAlias), and it bounds the hint maps
			if ids, ok := m.nameIdx[db]; ok {
				if id, ok := ids[hint.collectionName]; ok {
					if entry, ok := m.collections[id]; ok && entry.dbGen == m.dbGen[entry.dbName] {
						continue
					}
				}
			}
			deadAliases = append(deadAliases, dbName{db: db, name: alias})
		}
	}
	liveIDs := make(map[UniqueID]struct{}, len(m.collections))
	for id, entry := range m.collections {
		if entry.dbGen == m.dbGen[entry.dbName] {
			liveIDs[id] = struct{}{}
		}
	}
	m.mu.RUnlock()

	if len(deadIDs)+len(deadNames)+len(deadAliases) > 0 {
		m.mu.Lock()
		for _, id := range deadIDs {
			if entry, ok := m.collections[id]; ok && entry.dbGen != m.dbGen[entry.dbName] {
				delete(m.collections, id)
			}
		}
		for _, dn := range deadNames {
			if ids, ok := m.nameIdx[dn.db]; ok {
				if id, ok := ids[dn.name]; ok {
					entry, live := m.collections[id]
					if !live || entry.dbGen != m.dbGen[entry.dbName] ||
						entry.schema.GetName() != dn.name || normalizeDBName(entry.dbName) != dn.db {
						delete(ids, dn.name)
						if len(ids) == 0 {
							delete(m.nameIdx, dn.db)
						}
					}
				}
			}
		}
		for _, da := range deadAliases {
			// re-verification is not worth it here: deleting a hint is always safe
			if aliases, ok := m.aliasInfo[da.db]; ok {
				delete(aliases, da.name)
				if len(aliases) == 0 {
					delete(m.aliasInfo, da.db)
				}
			}
		}
		m.mu.Unlock()
	}

	// partition entries whose collection left the primary (drop, db drop/alter)
	orphan := func(key string) bool {
		_, ok := liveIDs[partitionKeyOwner(key)]
		return !ok
	}
	m.partitionCache.PruneIf(orphan)
	m.collLevelPartitionCache.PruneIf(orphan)
}

func (m *MetaCache) Close() {
	m.closeOnce.Do(func() {
		close(m.stopCh)
	})
}
