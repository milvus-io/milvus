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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	GetCollectionInfo(ctx context.Context, database, collectionName string, collectionID int64) (*collectionBasicInfo, error)
	// GetCollectionNamesByID get collection name and database name by collection id
	GetCollectionNamesByID(ctx context.Context, collectionID []UniqueID) ([]string, []string, error)
	// GetPartitionID get partition's identifier of specific collection.
	GetPartitionID(ctx context.Context, database, collectionName string, partitionName string) (typeutil.UniqueID, error)
	// GetPartitions get all partitions' id of specific collection.
	GetPartitions(ctx context.Context, database, collectionName string) (map[string]typeutil.UniqueID, error)
	// GetPartitionInfo get partition's info.
	GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*partitionInfo, error)
	// GetPartitionsIndex returns a partition names in partition key indexed order.
	GetPartitionsIndex(ctx context.Context, database, collectionName string) ([]string, error)
	// GetCollectionSchema get collection's schema.
	GetCollectionSchema(ctx context.Context, database, collectionName string) (*schemaInfo, error)
	GetShards(ctx context.Context, withCache bool, database, collectionName string, collectionID int64) (map[string][]nodeInfo, error)
	DeprecateShardCache(database, collectionName string)
	InvalidateShardLeaderCache(collections []int64)
	RemoveCollection(ctx context.Context, database, collectionName string)
	RemoveCollectionsByID(ctx context.Context, collectionID UniqueID) []string
	RemovePartition(ctx context.Context, database, collectionName string, partitionName string)

	// GetCredentialInfo operate credential cache
	GetCredentialInfo(ctx context.Context, username string) (*internalpb.CredentialInfo, error)
	RemoveCredential(username string)
	UpdateCredential(credInfo *internalpb.CredentialInfo)

	GetPrivilegeInfo(ctx context.Context) []string
	GetUserRole(username string) []string
	RefreshPolicyInfo(op typeutil.CacheOp) error
	InitPolicyInfo(info []string, userRoles []string)

	RemoveDatabase(ctx context.Context, database string)
	HasDatabase(ctx context.Context, database string) bool
	GetDatabaseInfo(ctx context.Context, database string) (*databaseInfo, error)
	// AllocID is only using on requests that need to skip timestamp allocation, don't overuse it.
	AllocID(ctx context.Context) (int64, error)
}
type collectionBasicInfo struct {
	collID                typeutil.UniqueID
	createdTimestamp      uint64
	createdUtcTimestamp   uint64
	consistencyLevel      commonpb.ConsistencyLevel
	partitionKeyIsolation bool
}

type collectionInfo struct {
	collID                typeutil.UniqueID
	schema                *schemaInfo
	partInfo              *partitionInfos
	createdTimestamp      uint64
	createdUtcTimestamp   uint64
	consistencyLevel      commonpb.ConsistencyLevel
	partitionKeyIsolation bool
}

type databaseInfo struct {
	dbID             typeutil.UniqueID
	createdTimestamp uint64
	properties       map[string]string
}

// schemaInfo is a helper function wraps *schemapb.CollectionSchema
// with extra fields mapping and methods
type schemaInfo struct {
	*schemapb.CollectionSchema
	fieldMap             *typeutil.ConcurrentMap[string, int64] // field name to id mapping
	hasPartitionKeyField bool
	pkField              *schemapb.FieldSchema
	schemaHelper         *typeutil.SchemaHelper
}

func newSchemaInfoWithLoadFields(schema *schemapb.CollectionSchema, loadFields []int64) *schemaInfo {
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
	// schema shall be verified before
	schemaHelper, _ := typeutil.CreateSchemaHelperWithLoadFields(schema, loadFields)
	return &schemaInfo{
		CollectionSchema:     schema,
		fieldMap:             fieldMap,
		hasPartitionKeyField: hasPartitionkey,
		pkField:              pkField,
		schemaHelper:         schemaHelper,
	}
}

func newSchemaInfo(schema *schemapb.CollectionSchema) *schemaInfo {
	return newSchemaInfoWithLoadFields(schema, nil)
}

func (s *schemaInfo) MapFieldID(name string) (int64, bool) {
	return s.fieldMap.Get(name)
}

func (s *schemaInfo) IsPartitionKeyCollection() bool {
	return s.hasPartitionKeyField
}

func (s *schemaInfo) GetPkField() (*schemapb.FieldSchema, error) {
	if s.pkField == nil {
		return nil, merr.WrapErrServiceInternal("pk field not found")
	}
	return s.pkField, nil
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

	var hasPrimaryKey, hasPartitionKey, hasVector bool
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
	return nil
}

func (s *schemaInfo) IsFieldLoaded(fieldID int64) bool {
	return s.schemaHelper.IsFieldLoaded(fieldID)
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
}

// getBasicInfo get a basic info by deep copy.
func (info *collectionInfo) getBasicInfo() *collectionBasicInfo {
	// Do a deep copy for all fields.
	basicInfo := &collectionBasicInfo{
		collID:                info.collID,
		createdTimestamp:      info.createdTimestamp,
		createdUtcTimestamp:   info.createdUtcTimestamp,
		consistencyLevel:      info.consistencyLevel,
		partitionKeyIsolation: info.partitionKeyIsolation,
	}

	return basicInfo
}

func (info *collectionInfo) isCollectionCached() bool {
	return info != nil && info.collID != UniqueID(0) && info.schema != nil
}

// shardLeaders wraps shard leader mapping for iteration.
type shardLeaders struct {
	idx        *atomic.Int64
	deprecated *atomic.Bool

	collectionID int64
	shardLeaders map[string][]nodeInfo
}

type shardLeadersReader struct {
	leaders *shardLeaders
	idx     int64
}

// Shuffle returns the shuffled shard leader list.
func (it shardLeadersReader) Shuffle() map[string][]nodeInfo {
	result := make(map[string][]nodeInfo)
	for channel, leaders := range it.leaders.shardLeaders {
		l := len(leaders)
		// shuffle all replica at random order
		shuffled := make([]nodeInfo, l)
		for i, randIndex := range rand.Perm(l) {
			shuffled[i] = leaders[randIndex]
		}

		// make each copy has same probability to be first replica
		for index, leader := range shuffled {
			if leader == leaders[int(it.idx)%l] {
				shuffled[0], shuffled[index] = shuffled[index], shuffled[0]
			}
		}

		result[channel] = shuffled
	}
	return result
}

// GetReader returns shuffer reader for shard leader.
func (sl *shardLeaders) GetReader() shardLeadersReader {
	idx := sl.idx.Inc()
	return shardLeadersReader{
		leaders: sl,
		idx:     idx,
	}
}

// make sure MetaCache implements Cache.
var _ Cache = (*MetaCache)(nil)

// MetaCache implements Cache, provides collection meta cache based on internal RootCoord
type MetaCache struct {
	rootCoord  types.RootCoordClient
	queryCoord types.QueryCoordClient

	dbInfo           map[string]*databaseInfo                // database -> db_info
	collInfo         map[string]map[string]*collectionInfo   // database -> collectionName -> collection_info
	collLeader       map[string]map[string]*shardLeaders     // database -> collectionName -> collection_leaders
	dbCollectionInfo map[string]map[typeutil.UniqueID]string // database -> collectionID -> collectionName
	credMap          map[string]*internalpb.CredentialInfo   // cache for credential, lazy load
	privilegeInfos   map[string]struct{}                     // privileges cache
	userToRoles      map[string]map[string]struct{}          // user to role cache
	mu               sync.RWMutex
	credMut          sync.RWMutex
	leaderMut        sync.RWMutex
	shardMgr         shardClientMgr
	sfGlobal         conc.Singleflight[*collectionInfo]
	sfDB             conc.Singleflight[*databaseInfo]

	IDStart int64
	IDCount int64
	IDIndex int64
	IDLock  sync.RWMutex
}

// globalMetaCache is singleton instance of Cache
var globalMetaCache Cache

// InitMetaCache initializes globalMetaCache
func InitMetaCache(ctx context.Context, rootCoord types.RootCoordClient, queryCoord types.QueryCoordClient, shardMgr shardClientMgr) error {
	var err error
	globalMetaCache, err = NewMetaCache(rootCoord, queryCoord, shardMgr)
	if err != nil {
		return err
	}

	// The privilege info is a little more. And to get this info, the query operation of involving multiple table queries is required.
	resp, err := rootCoord.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
	if err != nil {
		log.Error("fail to init meta cache", zap.Error(err))
		return err
	}
	globalMetaCache.InitPolicyInfo(resp.PolicyInfos, resp.UserRoles)
	log.Info("success to init meta cache", zap.Strings("policy_infos", resp.PolicyInfos))
	return nil
}

// NewMetaCache creates a MetaCache with provided RootCoord and QueryNode
func NewMetaCache(rootCoord types.RootCoordClient, queryCoord types.QueryCoordClient, shardMgr shardClientMgr) (*MetaCache, error) {
	return &MetaCache{
		rootCoord:        rootCoord,
		queryCoord:       queryCoord,
		dbInfo:           map[string]*databaseInfo{},
		collInfo:         map[string]map[string]*collectionInfo{},
		collLeader:       map[string]map[string]*shardLeaders{},
		dbCollectionInfo: map[string]map[typeutil.UniqueID]string{},
		credMap:          map[string]*internalpb.CredentialInfo{},
		shardMgr:         shardMgr,
		privilegeInfos:   map[string]struct{}{},
		userToRoles:      map[string]map[string]struct{}{},
	}, nil
}

func (m *MetaCache) getCollection(database, collectionName string, collectionID UniqueID) (*collectionInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	db, ok := m.collInfo[database]
	if !ok {
		return nil, false
	}
	if collectionName == "" {
		for _, collection := range db {
			if collection.collID == collectionID {
				return collection, collection.isCollectionCached()
			}
		}
	} else {
		if collection, ok := db[collectionName]; ok {
			return collection, collection.isCollectionCached()
		}
	}

	return nil, false
}

func (m *MetaCache) getCollectionShardLeader(database, collectionName string) (*shardLeaders, bool) {
	m.leaderMut.RLock()
	defer m.leaderMut.RUnlock()

	db, ok := m.collLeader[database]
	if !ok {
		return nil, false
	}

	if leaders, ok := db[collectionName]; ok {
		return leaders, !leaders.deprecated.Load()
	}
	return nil, false
}

func (m *MetaCache) update(ctx context.Context, database, collectionName string, collectionID UniqueID) (*collectionInfo, error) {
	if collInfo, ok := m.getCollection(database, collectionName, collectionID); ok {
		return collInfo, nil
	}

	collection, err := m.describeCollection(ctx, database, collectionName, collectionID)
	if err != nil {
		return nil, err
	}

	partitions, err := m.showPartitions(ctx, database, collectionName, collectionID)
	if err != nil {
		return nil, err
	}

	loadFields, err := m.getCollectionLoadFields(ctx, collection.CollectionID)
	if err != nil {
		return nil, err
	}

	// check partitionID, createdTimestamp and utcstamp has sam element numbers
	if len(partitions.PartitionNames) != len(partitions.CreatedTimestamps) || len(partitions.PartitionNames) != len(partitions.CreatedUtcTimestamps) {
		return nil, merr.WrapErrParameterInvalidMsg("partition names and timestamps number is not aligned, response: %s", partitions.String())
	}

	infos := lo.Map(partitions.GetPartitionIDs(), func(partitionID int64, idx int) *partitionInfo {
		return &partitionInfo{
			name:                partitions.PartitionNames[idx],
			partitionID:         partitions.PartitionIDs[idx],
			createdTimestamp:    partitions.CreatedTimestamps[idx],
			createdUtcTimestamp: partitions.CreatedUtcTimestamps[idx],
		}
	})

	collectionName = collection.Schema.GetName()
	m.mu.Lock()
	defer m.mu.Unlock()
	_, dbOk := m.collInfo[database]
	if !dbOk {
		m.collInfo[database] = make(map[string]*collectionInfo)
	}

	isolation, err := common.IsPartitionKeyIsolationKvEnabled(collection.Properties...)
	if err != nil {
		return nil, err
	}

	schemaInfo := newSchemaInfoWithLoadFields(collection.Schema, loadFields)
	m.collInfo[database][collectionName] = &collectionInfo{
		collID:                collection.CollectionID,
		schema:                schemaInfo,
		partInfo:              parsePartitionsInfo(infos, schemaInfo.hasPartitionKeyField),
		createdTimestamp:      collection.CreatedTimestamp,
		createdUtcTimestamp:   collection.CreatedUtcTimestamp,
		consistencyLevel:      collection.ConsistencyLevel,
		partitionKeyIsolation: isolation,
	}

	log.Info("meta update success", zap.String("database", database), zap.String("collectionName", collectionName), zap.Int64("collectionID", collection.CollectionID))
	return m.collInfo[database][collectionName], nil
}

func buildSfKeyByName(database, collectionName string) string {
	return database + "-" + collectionName
}

func buildSfKeyById(database string, collectionID UniqueID) string {
	return database + "--" + fmt.Sprint(collectionID)
}

func (m *MetaCache) UpdateByName(ctx context.Context, database, collectionName string) (*collectionInfo, error) {
	collection, err, _ := m.sfGlobal.Do(buildSfKeyByName(database, collectionName), func() (*collectionInfo, error) {
		return m.update(ctx, database, collectionName, 0)
	})
	return collection, err
}

func (m *MetaCache) UpdateByID(ctx context.Context, database string, collectionID UniqueID) (*collectionInfo, error) {
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
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")

		collInfo, err := m.UpdateByName(ctx, database, collectionName)
		if err != nil {
			return UniqueID(0), err
		}

		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return collInfo.collID, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()

	return collInfo.collID, nil
}

// GetCollectionName returns the corresponding collection name for provided collection id
func (m *MetaCache) GetCollectionName(ctx context.Context, database string, collectionID int64) (string, error) {
	method := "GetCollectionName"
	collInfo, ok := m.getCollection(database, "", collectionID)

	if !ok {
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")

		collInfo, err := m.UpdateByID(ctx, database, collectionID)
		if err != nil {
			return "", err
		}

		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return collInfo.schema.Name, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()

	return collInfo.schema.Name, nil
}

func (m *MetaCache) GetCollectionInfo(ctx context.Context, database string, collectionName string, collectionID int64) (*collectionBasicInfo, error) {
	collInfo, ok := m.getCollection(database, collectionName, 0)

	method := "GetCollectionInfo"
	// if collInfo.collID != collectionID, means that the cache is not trustable
	// try to get collection according to collectionID
	if !ok || collInfo.collID != collectionID {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()

		collInfo, err := m.UpdateByID(ctx, database, collectionID)
		if err != nil {
			return nil, err
		}
		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return collInfo.getBasicInfo(), nil
	}

	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()
	return collInfo.getBasicInfo(), nil
}

func (m *MetaCache) GetCollectionNamesByID(ctx context.Context, collectionIDs []UniqueID) ([]string, []string, error) {
	hasUpdate := false

	dbNames := make([]string, 0)
	collectionNames := make([]string, 0)
	for _, collectionID := range collectionIDs {
		dbName, collectionName := m.innerGetCollectionByID(collectionID)
		if dbName != "" {
			dbNames = append(dbNames, dbName)
			collectionNames = append(collectionNames, collectionName)
			continue
		}
		if hasUpdate {
			return nil, nil, errors.New("collection not found after meta cache has been updated")
		}
		hasUpdate = true
		err := m.updateDBInfo(ctx)
		if err != nil {
			return nil, nil, err
		}
		dbName, collectionName = m.innerGetCollectionByID(collectionID)
		if dbName == "" {
			return nil, nil, errors.New("collection not found")
		}
		dbNames = append(dbNames, dbName)
		collectionNames = append(collectionNames, collectionName)
	}

	return dbNames, collectionNames, nil
}

func (m *MetaCache) innerGetCollectionByID(collectionID int64) (string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for database, db := range m.dbCollectionInfo {
		name, ok := db[collectionID]
		if ok {
			return database, name
		}
	}
	return "", ""
}

func (m *MetaCache) updateDBInfo(ctx context.Context) error {
	databaseResp, err := m.rootCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases)),
	})

	if err := merr.CheckRPCCall(databaseResp, err); err != nil {
		log.Warn("failed to ListDatabases", zap.Error(err))
		return err
	}

	dbInfo := make(map[string]map[int64]string)
	for _, dbName := range databaseResp.DbNames {
		resp, err := m.rootCoord.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
			),
			DbName: dbName,
		})

		if err := merr.CheckRPCCall(resp, err); err != nil {
			log.Warn("failed to ShowCollections",
				zap.String("dbName", dbName),
				zap.Error(err))
			return err
		}

		collections := make(map[int64]string)
		for i, collection := range resp.CollectionNames {
			collections[resp.CollectionIds[i]] = collection
		}
		dbInfo[dbName] = collections
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.dbCollectionInfo = dbInfo

	return nil
}

// GetCollectionInfo returns the collection information related to provided collection name
// If the information is not found, proxy will try to fetch information for other source (RootCoord for now)
// TODO: may cause data race of this implementation, should be refactored in future.
func (m *MetaCache) getFullCollectionInfo(ctx context.Context, database, collectionName string, collectionID int64) (*collectionInfo, error) {
	collInfo, ok := m.getCollection(database, collectionName, collectionID)

	method := "GetCollectionInfo"
	// if collInfo.collID != collectionID, means that the cache is not trustable
	// try to get collection according to collectionID
	if !ok || collInfo.collID != collectionID {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()

		collInfo, err := m.UpdateByID(ctx, database, collectionID)
		if err != nil {
			return nil, err
		}
		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return collInfo, nil
	}

	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()
	return collInfo, nil
}

func (m *MetaCache) GetCollectionSchema(ctx context.Context, database, collectionName string) (*schemaInfo, error) {
	collInfo, ok := m.getCollection(database, collectionName, 0)

	method := "GetCollectionSchema"
	if !ok {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()

		collInfo, err := m.UpdateByName(ctx, database, collectionName)
		if err != nil {
			return nil, err
		}
		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		log.Debug("Reload collection from root coordinator ",
			zap.String("collectionName", collectionName),
			zap.Int64("time (milliseconds) take ", tr.ElapseSpan().Milliseconds()))
		return collInfo.schema, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()

	return collInfo.schema, nil
}

func (m *MetaCache) GetPartitionID(ctx context.Context, database, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	partInfo, err := m.GetPartitionInfo(ctx, database, collectionName, partitionName)
	if err != nil {
		return 0, err
	}
	return partInfo.partitionID, nil
}

func (m *MetaCache) GetPartitions(ctx context.Context, database, collectionName string) (map[string]typeutil.UniqueID, error) {
	partitions, err := m.GetPartitionInfos(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}

	return partitions.name2ID, nil
}

func (m *MetaCache) GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*partitionInfo, error) {
	partitions, err := m.GetPartitionInfos(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}

	info, ok := partitions.name2Info[partitionName]
	if !ok {
		return nil, merr.WrapErrPartitionNotFound(partitionName)
	}
	return info, nil
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
	collInfo, ok := m.getCollection(database, collectionName, 0)

	if !ok {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()

		collInfo, err := m.UpdateByName(ctx, database, collectionName)
		if err != nil {
			return nil, err
		}

		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return collInfo.partInfo, nil
	}
	return collInfo.partInfo, nil
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
	coll, err := m.rootCoord.DescribeCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	err = merr.Error(coll.GetStatus())
	if err != nil {
		return nil, err
	}
	resp := &milvuspb.DescribeCollectionResponse{
		Status: coll.Status,
		Schema: &schemapb.CollectionSchema{
			Name:               coll.Schema.Name,
			Description:        coll.Schema.Description,
			AutoID:             coll.Schema.AutoID,
			Fields:             make([]*schemapb.FieldSchema, 0),
			EnableDynamicField: coll.Schema.EnableDynamicField,
		},
		CollectionID:         coll.CollectionID,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		CreatedTimestamp:     coll.CreatedTimestamp,
		CreatedUtcTimestamp:  coll.CreatedUtcTimestamp,
		ConsistencyLevel:     coll.ConsistencyLevel,
		DbName:               coll.GetDbName(),
		Properties:           coll.Properties,
	}
	for _, field := range coll.Schema.Fields {
		if field.FieldID >= common.StartOfUserFieldID {
			resp.Schema.Fields = append(resp.Schema.Fields, field)
		}
	}
	return resp, nil
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

	partitions, err := m.rootCoord.ShowPartitions(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := merr.Error(partitions.GetStatus()); err != nil {
		return nil, err
	}

	if len(partitions.PartitionIDs) != len(partitions.PartitionNames) {
		return nil, fmt.Errorf("partition ids len: %d doesn't equal Partition name len %d",
			len(partitions.PartitionIDs), len(partitions.PartitionNames))
	}

	return partitions, nil
}

func (m *MetaCache) getCollectionLoadFields(ctx context.Context, collectionID UniqueID) ([]int64, error) {
	req := &querypb.ShowCollectionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionIDs: []int64{collectionID},
	}

	resp, err := m.queryCoord.ShowCollections(ctx, req)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotLoaded) {
			return []int64{}, nil
		}
		return nil, err
	}
	// backward compatility, ignore HPL logic
	if len(resp.GetLoadFields()) < 1 {
		return []int64{}, nil
	}
	return resp.GetLoadFields()[0].GetData(), nil
}

func (m *MetaCache) describeDatabase(ctx context.Context, dbName string) (*rootcoordpb.DescribeDatabaseResponse, error) {
	req := &rootcoordpb.DescribeDatabaseRequest{
		DbName: dbName,
	}

	resp, err := m.rootCoord.DescribeDatabase(ctx, req)
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
			log.Info("partition group not in partitionKey pattern", zap.String("partitionName", partitionName))
			return result
		}
		index, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
		if err != nil {
			log.Info("partition group not in partitionKey pattern", zap.String("partitionName", partitionName), zap.Error(err))
			return result
		}
		partitionNames[index] = partitionName
	}

	result.indexedPartitionNames = partitionNames
	return result
}

func (m *MetaCache) RemoveCollection(ctx context.Context, database, collectionName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, dbOk := m.collInfo[database]
	if dbOk {
		delete(m.collInfo[database], collectionName)
	}
}

func (m *MetaCache) RemoveCollectionsByID(ctx context.Context, collectionID UniqueID) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var collNames []string
	for database, db := range m.collInfo {
		for k, v := range db {
			if v.collID == collectionID {
				delete(m.collInfo[database], k)
				collNames = append(collNames, k)
			}
		}
	}
	return collNames
}

func (m *MetaCache) RemovePartition(ctx context.Context, database, collectionName, partitionName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var ok bool
	var collInfo *collectionInfo

	db, dbOk := m.collInfo[database]
	if dbOk {
		collInfo, ok = db[collectionName]
	}

	if !ok {
		return
	}

	partInfo := m.collInfo[database][collectionName].partInfo
	if partInfo == nil {
		return
	}
	filteredInfos := lo.Filter(partInfo.partitionInfos, func(info *partitionInfo, idx int) bool {
		return info.name != partitionName
	})

	m.collInfo[database][collectionName].partInfo = parsePartitionsInfo(filteredInfos, collInfo.schema.hasPartitionKeyField)
}

// GetCredentialInfo returns the credential related to provided username
// If the cache missed, proxy will try to fetch from storage
func (m *MetaCache) GetCredentialInfo(ctx context.Context, username string) (*internalpb.CredentialInfo, error) {
	m.credMut.RLock()
	var credInfo *internalpb.CredentialInfo
	credInfo, ok := m.credMap[username]
	m.credMut.RUnlock()

	if !ok {
		req := &rootcoordpb.GetCredentialRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_GetCredential),
			),
			Username: username,
		}
		resp, err := m.rootCoord.GetCredential(ctx, req)
		if err != nil {
			return &internalpb.CredentialInfo{}, err
		}
		credInfo = &internalpb.CredentialInfo{
			Username:          resp.Username,
			EncryptedPassword: resp.Password,
		}
	}

	return credInfo, nil
}

func (m *MetaCache) RemoveCredential(username string) {
	m.credMut.Lock()
	defer m.credMut.Unlock()
	// delete pair in credMap
	delete(m.credMap, username)
}

func (m *MetaCache) UpdateCredential(credInfo *internalpb.CredentialInfo) {
	m.credMut.Lock()
	defer m.credMut.Unlock()
	username := credInfo.Username
	_, ok := m.credMap[username]
	if !ok {
		m.credMap[username] = &internalpb.CredentialInfo{}
	}

	// Do not cache encrypted password content
	m.credMap[username].Username = username
	m.credMap[username].Sha256Password = credInfo.Sha256Password
}

// GetShards update cache if withCache == false
func (m *MetaCache) GetShards(ctx context.Context, withCache bool, database, collectionName string, collectionID int64) (map[string][]nodeInfo, error) {
	method := "GetShards"
	log := log.Ctx(ctx).With(
		zap.String("collectionName", collectionName),
		zap.Int64("collectionID", collectionID))

	info, err := m.getFullCollectionInfo(ctx, database, collectionName, collectionID)
	if err != nil {
		return nil, err
	}

	cacheShardLeaders, ok := m.getCollectionShardLeader(database, collectionName)
	if withCache {
		if ok {
			metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()
			iterator := cacheShardLeaders.GetReader()
			return iterator.Shuffle(), nil
		}

		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		log.Info("no shard cache for collection, try to get shard leaders from QueryCoord")
	}
	req := &querypb.GetShardLeadersRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetShardLeaders),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionID: info.collID,
	}

	tr := timerecord.NewTimeRecorder("UpdateShardCache")
	resp, err := m.queryCoord.GetShardLeaders(ctx, req)
	if err != nil {
		return nil, err
	}
	if err = merr.Error(resp.GetStatus()); err != nil {
		return nil, err
	}

	shards := parseShardLeaderList2QueryNode(resp.GetShards())
	newShardLeaders := &shardLeaders{
		collectionID: info.collID,
		shardLeaders: shards,
		deprecated:   atomic.NewBool(false),
		idx:          atomic.NewInt64(0),
	}

	// lock leader
	m.leaderMut.Lock()
	if _, ok := m.collLeader[database]; !ok {
		m.collLeader[database] = make(map[string]*shardLeaders)
	}

	m.collLeader[database][collectionName] = newShardLeaders
	m.leaderMut.Unlock()

	iterator := newShardLeaders.GetReader()
	ret := iterator.Shuffle()

	oldLeaders := make(map[string][]nodeInfo)
	if cacheShardLeaders != nil {
		oldLeaders = cacheShardLeaders.shardLeaders
	}
	// update refcnt in shardClientMgr
	// and create new client for new leaders
	_ = m.shardMgr.UpdateShardLeaders(oldLeaders, ret)

	metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return ret, nil
}

func parseShardLeaderList2QueryNode(shardsLeaders []*querypb.ShardLeadersList) map[string][]nodeInfo {
	shard2QueryNodes := make(map[string][]nodeInfo)

	for _, leaders := range shardsLeaders {
		qns := make([]nodeInfo, len(leaders.GetNodeIds()))

		for j := range qns {
			qns[j] = nodeInfo{leaders.GetNodeIds()[j], leaders.GetNodeAddrs()[j]}
		}

		shard2QueryNodes[leaders.GetChannelName()] = qns
	}

	return shard2QueryNodes
}

// DeprecateShardCache clear the shard leader cache of a collection
func (m *MetaCache) DeprecateShardCache(database, collectionName string) {
	log.Info("clearing shard cache for collection", zap.String("collectionName", collectionName))
	if shards, ok := m.getCollectionShardLeader(database, collectionName); ok {
		shards.deprecated.Store(true)
	}
}

func (m *MetaCache) InvalidateShardLeaderCache(collections []int64) {
	log.Info("Invalidate shard cache for collections", zap.Int64s("collectionIDs", collections))
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()

	collectionSet := typeutil.NewUniqueSet(collections...)
	for _, db := range m.collLeader {
		for _, shardLeaders := range db {
			if collectionSet.Contain(shardLeaders.collectionID) {
				shardLeaders.deprecated.Store(true)
			}
		}
	}
}

func (m *MetaCache) InitPolicyInfo(info []string, userRoles []string) {
	defer func() {
		err := getEnforcer().LoadPolicy()
		if err != nil {
			log.Error("failed to load policy after RefreshPolicyInfo", zap.Error(err))
		}
	}()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsafeInitPolicyInfo(info, userRoles)
}

func (m *MetaCache) unsafeInitPolicyInfo(info []string, userRoles []string) {
	m.privilegeInfos = util.StringSet(info)
	for _, userRole := range userRoles {
		user, role, err := funcutil.DecodeUserRoleCache(userRole)
		if err != nil {
			log.Warn("invalid user-role key", zap.String("user-role", userRole), zap.Error(err))
			continue
		}
		if m.userToRoles[user] == nil {
			m.userToRoles[user] = make(map[string]struct{})
		}
		m.userToRoles[user][role] = struct{}{}
	}
}

func (m *MetaCache) GetPrivilegeInfo(ctx context.Context) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return util.StringList(m.privilegeInfos)
}

func (m *MetaCache) GetUserRole(user string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return util.StringList(m.userToRoles[user])
}

func (m *MetaCache) RefreshPolicyInfo(op typeutil.CacheOp) (err error) {
	defer func() {
		if err == nil {
			le := getEnforcer().LoadPolicy()
			if le != nil {
				log.Error("failed to load policy after RefreshPolicyInfo", zap.Error(le))
			}
			CleanPrivilegeCache()
		}
	}()
	if op.OpType != typeutil.CacheRefresh {
		m.mu.Lock()
		defer m.mu.Unlock()
		if op.OpKey == "" {
			return errors.New("empty op key")
		}
	}

	switch op.OpType {
	case typeutil.CacheGrantPrivilege:
		m.privilegeInfos[op.OpKey] = struct{}{}
	case typeutil.CacheRevokePrivilege:
		delete(m.privilegeInfos, op.OpKey)
	case typeutil.CacheAddUserToRole:
		user, role, err := funcutil.DecodeUserRoleCache(op.OpKey)
		if err != nil {
			return fmt.Errorf("invalid opKey, fail to decode, op_type: %d, op_key: %s", int(op.OpType), op.OpKey)
		}
		if m.userToRoles[user] == nil {
			m.userToRoles[user] = make(map[string]struct{})
		}
		m.userToRoles[user][role] = struct{}{}
	case typeutil.CacheRemoveUserFromRole:
		user, role, err := funcutil.DecodeUserRoleCache(op.OpKey)
		if err != nil {
			return fmt.Errorf("invalid opKey, fail to decode, op_type: %d, op_key: %s", int(op.OpType), op.OpKey)
		}
		if m.userToRoles[user] != nil {
			delete(m.userToRoles[user], role)
		}
	case typeutil.CacheDeleteUser:
		delete(m.userToRoles, op.OpKey)
	case typeutil.CacheDropRole:
		for user := range m.userToRoles {
			delete(m.userToRoles[user], op.OpKey)
		}

		for policy := range m.privilegeInfos {
			if funcutil.PolicyCheckerWithRole(policy, op.OpKey) {
				delete(m.privilegeInfos, policy)
			}
		}
	case typeutil.CacheRefresh:
		resp, err := m.rootCoord.ListPolicy(context.Background(), &internalpb.ListPolicyRequest{})
		if err != nil {
			log.Error("fail to init meta cache", zap.Error(err))
			return err
		}

		if !merr.Ok(resp.GetStatus()) {
			log.Error("fail to init meta cache",
				zap.String("error_code", resp.GetStatus().GetErrorCode().String()),
				zap.String("reason", resp.GetStatus().GetReason()))
			return merr.Error(resp.Status)
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		m.userToRoles = make(map[string]map[string]struct{})
		m.privilegeInfos = make(map[string]struct{})
		m.unsafeInitPolicyInfo(resp.PolicyInfos, resp.UserRoles)
	default:
		return fmt.Errorf("invalid opType, op_type: %d, op_key: %s", int(op.OpType), op.OpKey)
	}
	return nil
}

func (m *MetaCache) RemoveDatabase(ctx context.Context, database string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.collInfo, database)
	delete(m.dbInfo, database)
}

func (m *MetaCache) HasDatabase(ctx context.Context, database string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.collInfo[database]
	return ok
}

func (m *MetaCache) GetDatabaseInfo(ctx context.Context, database string) (*databaseInfo, error) {
	dbInfo := m.safeGetDBInfo(database)
	if dbInfo != nil {
		return dbInfo, nil
	}

	dbInfo, err, _ := m.sfDB.Do(database, func() (*databaseInfo, error) {
		resp, err := m.describeDatabase(ctx, database)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		dbInfo := &databaseInfo{
			dbID:             resp.GetDbID(),
			createdTimestamp: resp.GetCreatedTimestamp(),
			properties:       funcutil.KeyValuePair2Map(resp.GetProperties()),
		}
		m.dbInfo[database] = dbInfo
		return dbInfo, nil
	})

	return dbInfo, err
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
		resp, err := m.rootCoord.AllocID(ctx, &rootcoordpb.AllocIDRequest{
			Count: 1000000,
		})
		if err != nil {
			log.Warn("Refreshing ID cache from rootcoord failed", zap.Error(err))
			return 0, err
		}
		if resp.GetStatus().GetCode() != 0 {
			log.Warn("Refreshing ID cache from rootcoord failed", zap.String("failed detail", resp.GetStatus().GetDetail()))
			return 0, merr.WrapErrServiceInternal(resp.GetStatus().GetDetail())
		}
		m.IDStart, m.IDCount = resp.GetID(), int64(resp.GetCount())
		m.IDIndex = 0
	}
	id := m.IDStart + m.IDIndex
	m.IDIndex++
	return id, nil
}
