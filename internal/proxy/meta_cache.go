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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	internalhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/expr"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	// GetShard(ctx context.Context, withCache bool, database, collectionName string, collectionID int64, channel string) ([]nodeInfo, error)
	// GetShardLeaderList(ctx context.Context, database, collectionName string, collectionID int64, withCache bool) ([]string, error)
	// DeprecateShardCache(database, collectionName string)
	// InvalidateShardLeaderCache(collections []int64)
	// ListShardLocation() map[int64]nodeInfo
	RemoveCollection(ctx context.Context, database, collectionName string, version uint64)
	RemoveCollectionsByID(ctx context.Context, collectionID UniqueID, version uint64, removeVersion bool) []string

	// GetCredentialInfo operate credential cache
	// GetCredentialInfo(ctx context.Context, username string) (*internalpb.CredentialInfo, error)
	// RemoveCredential(username string)
	// UpdateCredential(credInfo *internalpb.CredentialInfo)

	// GetPrivilegeInfo(ctx context.Context) []string
	// GetUserRole(username string) []string
	// RefreshPolicyInfo(op typeutil.CacheOp) error
	// InitPolicyInfo(info []string, userRoles []string)

	RemoveDatabase(ctx context.Context, database string)
	HasDatabase(ctx context.Context, database string) bool
	GetDatabaseInfo(ctx context.Context, database string) (*databaseInfo, error)
	// AllocID is only using on requests that need to skip timestamp allocation, don't overuse it.
	AllocID(ctx context.Context) (int64, error)
}

type collectionInfo struct {
	collID                typeutil.UniqueID
	schema                *schemaInfo
	partInfo              *partitionInfos
	createdTimestamp      uint64
	createdUtcTimestamp   uint64
	consistencyLevel      commonpb.ConsistencyLevel
	partitionKeyIsolation bool
	updateTimestamp       uint64
	collectionTTL         uint64
	numPartitions         int64
	vChannels             []string
	pChannels             []string
	shardsNum             int32
	aliases               []string
	properties            []*commonpb.KeyValuePair
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
		return nil, merr.WrapErrServiceInternal("pk field not found")
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

	dbInfo   map[string]*databaseInfo              // database -> db_info
	collInfo map[string]map[string]*collectionInfo // database -> collectionName -> collection_info

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

	collectionCacheVersion map[UniqueID]uint64 // collectionID -> cacheVersion
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
		log.Error("failed to init privilege cache", zap.Error(err))
		return err
	}

	// Register password verify function for /expr endpoint authentication
	internalhttp.RegisterPasswordVerifyFunc(PasswordVerify)

	return nil
}

// NewMetaCache creates a MetaCache with provided RootCoord and QueryNode
func NewMetaCache(mixCoord types.MixCoordClient) (*MetaCache, error) {
	return &MetaCache{
		mixCoord:               mixCoord,
		dbInfo:                 map[string]*databaseInfo{},
		collInfo:               map[string]map[string]*collectionInfo{},
		credMap:                map[string]*internalpb.CredentialInfo{},
		privilegeInfos:         map[string]struct{}{},
		userToRoles:            map[string]map[string]struct{}{},
		collectionCacheVersion: make(map[UniqueID]uint64),
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

	// check partitionID, createdTimestamp and utcstamp has sam element numbers
	if len(partitions.PartitionNames) != len(partitions.CreatedTimestamps) || len(partitions.PartitionNames) != len(partitions.CreatedUtcTimestamps) {
		return nil, merr.WrapErrParameterInvalidMsg("partition names and timestamps number is not aligned, response: %s", partitions.String())
	}

	defaultPartitionName := Params.CommonCfg.DefaultPartitionName.GetValue()
	infos := lo.Map(partitions.GetPartitionIDs(), func(partitionID int64, idx int) *partitionInfo {
		return &partitionInfo{
			name:                partitions.PartitionNames[idx],
			partitionID:         partitions.PartitionIDs[idx],
			createdTimestamp:    partitions.CreatedTimestamps[idx],
			createdUtcTimestamp: partitions.CreatedUtcTimestamps[idx],
			isDefault:           partitions.PartitionNames[idx] == defaultPartitionName,
		}
	})

	if collectionName == "" {
		collectionName = collection.Schema.GetName()
	}
	if database == "" {
		log.Ctx(ctx).Warn("database is empty, use default database name", zap.String("collectionName", collectionName), zap.Stack("stack"))
	}
	isolation, err := common.IsPartitionKeyIsolationKvEnabled(collection.Properties...)
	if err != nil {
		return nil, err
	}

	schemaInfo := newSchemaInfo(collection.Schema)

	m.mu.Lock()
	defer m.mu.Unlock()
	curVersion := m.collectionCacheVersion[collection.GetCollectionID()]
	// Compatibility logic: if the rootcoord version is lower(requestTime = 0), update the cache directly.
	if collection.GetRequestTime() < curVersion && collection.GetRequestTime() != 0 {
		log.Ctx(ctx).Debug("describe collection timestamp less than version, don't update cache",
			zap.String("collectionName", collectionName),
			zap.Uint64("version", collection.GetRequestTime()), zap.Uint64("cache version", curVersion))
		return &collectionInfo{
			collID:                collection.CollectionID,
			schema:                schemaInfo,
			partInfo:              parsePartitionsInfo(infos, schemaInfo.hasPartitionKeyField),
			createdTimestamp:      collection.CreatedTimestamp,
			createdUtcTimestamp:   collection.CreatedUtcTimestamp,
			consistencyLevel:      collection.ConsistencyLevel,
			partitionKeyIsolation: isolation,
			updateTimestamp:       collection.UpdateTimestamp,
			collectionTTL:         getCollectionTTL(schemaInfo.CollectionSchema.GetProperties()),
			vChannels:             collection.VirtualChannelNames,
			pChannels:             collection.PhysicalChannelNames,
			numPartitions:         collection.NumPartitions,
			shardsNum:             collection.ShardsNum,
			aliases:               collection.Aliases,
			properties:            collection.Properties,
		}, nil
	}
	_, dbOk := m.collInfo[database]
	if !dbOk {
		m.collInfo[database] = make(map[string]*collectionInfo)
	}

	m.collInfo[database][collectionName] = &collectionInfo{
		collID:                collection.CollectionID,
		schema:                schemaInfo,
		partInfo:              parsePartitionsInfo(infos, schemaInfo.hasPartitionKeyField),
		createdTimestamp:      collection.CreatedTimestamp,
		createdUtcTimestamp:   collection.CreatedUtcTimestamp,
		consistencyLevel:      collection.ConsistencyLevel,
		partitionKeyIsolation: isolation,
		updateTimestamp:       collection.UpdateTimestamp,
		collectionTTL:         getCollectionTTL(schemaInfo.CollectionSchema.GetProperties()),
		vChannels:             collection.VirtualChannelNames,
		pChannels:             collection.PhysicalChannelNames,
		numPartitions:         collection.NumPartitions,
		shardsNum:             collection.ShardsNum,
		aliases:               collection.Aliases,
		properties:            collection.Properties,
	}

	log.Ctx(ctx).Info("meta update success", zap.String("database", database), zap.String("collectionName", collectionName),
		zap.String("actual collection Name", collection.Schema.GetName()), zap.Int64("collectionID", collection.CollectionID),
		zap.Strings("partition", partitions.PartitionNames), zap.Uint64("currentVersion", curVersion),
		zap.Uint64("version", collection.GetRequestTime()), zap.Any("aliases", collection.Aliases),
	)

	m.collectionCacheVersion[collection.GetCollectionID()] = collection.GetRequestTime()
	collInfo := m.collInfo[database][collectionName]

	return collInfo, nil
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
	collInfo, ok := m.getCollection(database, collectionName, 0)

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
		log.Ctx(ctx).Debug("Reload collection from root coordinator ",
			zap.String("collectionName", collectionName),
			zap.Int64("time (milliseconds) take ", tr.ElapseSpan().Milliseconds()))
		return collInfo.schema, nil
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheHitLabel).Inc()

	return collInfo.schema, nil
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

func (m *MetaCache) GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*partitionInfo, error) {
	partitions, err := m.GetPartitionInfos(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}

	if partitionName == "" {
		for _, info := range partitions.partitionInfos {
			if info.isDefault {
				return info, nil
			}
		}
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
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), method, metrics.CacheMissLabel).Inc()

		collInfo, err := m.UpdateByName(ctx, database, collectionName)
		if err != nil {
			return nil, err
		}

		metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
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

	if len(partitions.PartitionIDs) != len(partitions.PartitionNames) {
		return nil, fmt.Errorf("partition ids len: %d doesn't equal Partition name len %d",
			len(partitions.PartitionIDs), len(partitions.PartitionNames))
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

func (m *MetaCache) RemoveCollection(ctx context.Context, database, collectionName string, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if db, dbOk := m.collInfo[database]; dbOk {
		if coll, ok := db[collectionName]; ok {
			m.removeCollectionByID(ctx, coll.collID, version, false)
		}
	}
	if database == "" {
		if db, dbOk := m.collInfo[defaultDB]; dbOk {
			if coll, ok := db[collectionName]; ok {
				m.removeCollectionByID(ctx, coll.collID, version, false)
			}
		}
	}
	log.Ctx(ctx).Debug("remove collection", zap.String("db", database), zap.String("collection", collectionName))
}

func (m *MetaCache) RemoveCollectionsByID(ctx context.Context, collectionID UniqueID, version uint64, removeVersion bool) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removeCollectionByID(ctx, collectionID, version, removeVersion)
}

func (m *MetaCache) removeCollectionByID(ctx context.Context, collectionID UniqueID, version uint64, removeVersion bool) []string {
	curVersion := m.collectionCacheVersion[collectionID]
	var collNames []string
	for database, db := range m.collInfo {
		for k, v := range db {
			if v.collID == collectionID {
				if version == 0 || curVersion <= version {
					delete(m.collInfo[database], k)
					collNames = append(collNames, k)
					m.sfGlobal.Forget(buildSfKeyByName(database, k))
					m.sfGlobal.Forget(buildSfKeyById(database, v.collID))
				}
			}
		}
	}
	if removeVersion {
		delete(m.collectionCacheVersion, collectionID)
	} else if version != 0 {
		m.collectionCacheVersion[collectionID] = version
	}
	log.Ctx(ctx).Debug("remove collection by id", zap.Int64("id", collectionID),
		zap.Strings("collection", collNames), zap.Uint64("currentVersion", curVersion),
		zap.Uint64("version", version), zap.Bool("removeVersion", removeVersion))
	return collNames
}

func (m *MetaCache) RemoveDatabase(ctx context.Context, database string) {
	log.Ctx(ctx).Debug("remove database", zap.String("name", database))
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
			properties:       resp.Properties,
			createdTimestamp: resp.GetCreatedTimestamp(),
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
		resp, err := m.mixCoord.AllocID(ctx, &rootcoordpb.AllocIDRequest{
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
