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
	"sync"
	"time"

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
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Cache is the interface for system meta data cache
//
//go:generate mockery --name=Cache --filename=mock_cache_test.go --outpkg=proxy --output=. --inpackage --structname=MockCache --with-expecter
type Cache interface {
	// GetCollectionID get collection's id by name.
	GetCollectionID(ctx context.Context, database, collectionName string) (typeutil.UniqueID, error)
	// GetDatabaseAndCollectionName get collection's name and database by id
	GetDatabaseAndCollectionName(ctx context.Context, collectionID int64) (string, string, error)
	// GetCollectionInfo get collection's information by name, such as collection id, schema, and etc.
	GetCollectionInfo(ctx context.Context, database, collectionName string) (*collectionInfo, error)
	// GetPartitionID get partition's identifier of specific collection.
	GetPartitionID(ctx context.Context, database, collectionName string, partitionName string) (typeutil.UniqueID, error)
	// GetPartitions get all partitions' id of specific collection.
	GetPartitions(ctx context.Context, database, collectionName string) (map[string]typeutil.UniqueID, error)
	// GetPartitionInfo get partition's info.
	GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*partitionInfo, error)
	// GetCollectionSchema get collection's schema.
	GetCollectionSchema(ctx context.Context, database, collectionName string) (*schemapb.CollectionSchema, error)
	GetShards(ctx context.Context, withCache bool, database, collectionName string) (map[string][]nodeInfo, error)
	DeprecateShardCache(database, collectionName string)
	expireShardLeaderCache(ctx context.Context)
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
}

type collectionInfo struct {
	collID              typeutil.UniqueID
	schema              *schemapb.CollectionSchema
	partInfo            map[string]*partitionInfo
	leaderMutex         sync.RWMutex
	shardLeaders        *shardLeaders
	createdTimestamp    uint64
	createdUtcTimestamp uint64
	consistencyLevel    commonpb.ConsistencyLevel
	database            string
}

func (info *collectionInfo) isCollectionCached() bool {
	return info != nil && info.collID != UniqueID(0) && info.schema != nil
}

func (info *collectionInfo) deprecateLeaderCache() {
	info.leaderMutex.RLock()
	defer info.leaderMutex.RUnlock()
	if info.shardLeaders != nil {
		info.shardLeaders.deprecated.Store(true)
	}
}

// shardLeaders wraps shard leader mapping for iteration.
type shardLeaders struct {
	idx        *atomic.Int64
	deprecated *atomic.Bool

	shardLeaders map[string][]nodeInfo
}

type shardLeadersReader struct {
	leaders *shardLeaders
	idx     int64
}

// Shuffle returns the shuffled shard leader list.
func (it shardLeadersReader) Shuffle() map[string][]nodeInfo {
	result := make(map[string][]nodeInfo)
	rand.Seed(time.Now().UnixNano())
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

type partitionInfo struct {
	partitionID         typeutil.UniqueID
	createdTimestamp    uint64
	createdUtcTimestamp uint64
}

// make sure MetaCache implements Cache.
var _ Cache = (*MetaCache)(nil)

// MetaCache implements Cache, provides collection meta cache based on internal RootCoord
type MetaCache struct {
	rootCoord  types.RootCoord
	queryCoord types.QueryCoord

	collInfo       map[string]map[string]*collectionInfo // database -> collection -> collection_info
	credMap        map[string]*internalpb.CredentialInfo // cache for credential, lazy load
	privilegeInfos map[string]struct{}                   // privileges cache
	userToRoles    map[string]map[string]struct{}        // user to role cache
	mu             sync.RWMutex
	credMut        sync.RWMutex
	privilegeMut   sync.RWMutex
	shardMgr       shardClientMgr
}

// globalMetaCache is singleton instance of Cache
var globalMetaCache Cache

// InitMetaCache initializes globalMetaCache
func InitMetaCache(ctx context.Context, rootCoord types.RootCoord, queryCoord types.QueryCoord, shardMgr shardClientMgr) error {
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
	globalMetaCache.expireShardLeaderCache(ctx)
	return nil
}

// NewMetaCache creates a MetaCache with provided RootCoord and QueryNode
func NewMetaCache(rootCoord types.RootCoord, queryCoord types.QueryCoord, shardMgr shardClientMgr) (*MetaCache, error) {
	return &MetaCache{
		rootCoord:      rootCoord,
		queryCoord:     queryCoord,
		collInfo:       map[string]map[string]*collectionInfo{},
		credMap:        map[string]*internalpb.CredentialInfo{},
		shardMgr:       shardMgr,
		privilegeInfos: map[string]struct{}{},
		userToRoles:    map[string]map[string]struct{}{},
	}, nil
}

// GetCollectionID returns the corresponding collection id for provided collection name
func (m *MetaCache) GetCollectionID(ctx context.Context, database, collectionName string) (typeutil.UniqueID, error) {
	m.mu.RLock()

	var ok bool
	var collInfo *collectionInfo

	db, dbOk := m.collInfo[database]
	if dbOk && db != nil {
		collInfo, ok = db[collectionName]
	}

	method := "GeCollectionID"
	if !ok || !collInfo.isCollectionCached() {
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")
		m.mu.RUnlock()
		coll, err := m.describeCollection(ctx, database, collectionName, 0)
		if err != nil {
			return 0, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()

		m.updateCollection(coll, database, collectionName)
		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		collInfo = m.collInfo[database][collectionName]
		return collInfo.collID, nil
	}
	defer m.mu.RUnlock()
	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()

	return collInfo.collID, nil
}

// GetDatabaseAndCollectionName returns the corresponding collection name for provided collection id
func (m *MetaCache) GetDatabaseAndCollectionName(ctx context.Context, collectionID int64) (string, string, error) {
	m.mu.RLock()
	var collInfo *collectionInfo
	for _, db := range m.collInfo {
		for _, coll := range db {
			if coll.collID == collectionID {
				collInfo = coll
				break
			}
		}
	}

	method := "GeCollectionName"
	if collInfo == nil || !collInfo.isCollectionCached() {
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")
		m.mu.RUnlock()
		coll, err := m.describeCollection(ctx, "", "", collectionID)
		if err != nil {
			return "", "", err
		}
		m.mu.Lock()
		defer m.mu.Unlock()

		m.updateCollection(coll, coll.GetDbName(), coll.Schema.Name)
		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		return coll.GetDbName(), coll.Schema.Name, nil
	}
	defer m.mu.RUnlock()
	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()

	return collInfo.database, collInfo.schema.Name, nil
}

// GetCollectionInfo returns the collection information related to provided collection name
// If the information is not found, proxy will try to fetch information for other source (RootCoord for now)
func (m *MetaCache) GetCollectionInfo(ctx context.Context, database, collectionName string) (*collectionInfo, error) {
	m.mu.RLock()
	var collInfo *collectionInfo
	var ok bool

	db, dbOk := m.collInfo[database]
	if dbOk {
		collInfo, ok = db[collectionName]
	}
	m.mu.RUnlock()

	method := "GetCollectionInfo"
	if !ok || !collInfo.isCollectionCached() {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		coll, err := m.describeCollection(ctx, database, collectionName, 0)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.updateCollection(coll, database, collectionName)
		collInfo = m.collInfo[database][collectionName]
		m.mu.Unlock()
		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	}

	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()
	return collInfo, nil
}

func (m *MetaCache) GetCollectionSchema(ctx context.Context, database, collectionName string) (*schemapb.CollectionSchema, error) {
	m.mu.RLock()
	var collInfo *collectionInfo
	var ok bool

	db, dbOk := m.collInfo[database]
	if dbOk {
		collInfo, ok = db[collectionName]
	}

	method := "GetCollectionSchema"
	if !ok || !collInfo.isCollectionCached() {
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")
		m.mu.RUnlock()
		coll, err := m.describeCollection(ctx, database, collectionName, 0)
		if err != nil {
			log.Warn("Failed to load collection from rootcoord ",
				zap.String("collection name ", collectionName),
				zap.Error(err))
			return nil, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()

		m.updateCollection(coll, database, collectionName)
		collInfo = m.collInfo[database][collectionName]
		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		log.Debug("Reload collection from root coordinator ",
			zap.String("collection name", collectionName),
			zap.Any("time (milliseconds) take ", tr.ElapseSpan().Milliseconds()))
		return collInfo.schema, nil
	}
	defer m.mu.RUnlock()
	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()

	return collInfo.schema, nil
}

func (m *MetaCache) updateCollection(coll *milvuspb.DescribeCollectionResponse, database, collectionName string) {
	_, dbOk := m.collInfo[database]
	if !dbOk {
		m.collInfo[database] = make(map[string]*collectionInfo)
	}

	_, ok := m.collInfo[database][collectionName]
	if !ok {
		m.collInfo[database][collectionName] = &collectionInfo{}
	}
	m.collInfo[database][collectionName].schema = coll.Schema
	m.collInfo[database][collectionName].collID = coll.CollectionID
	m.collInfo[database][collectionName].createdTimestamp = coll.CreatedTimestamp
	m.collInfo[database][collectionName].createdUtcTimestamp = coll.CreatedUtcTimestamp
	m.collInfo[database][collectionName].consistencyLevel = coll.ConsistencyLevel
}

func (m *MetaCache) GetPartitionID(ctx context.Context, database, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	partInfo, err := m.GetPartitionInfo(ctx, database, collectionName, partitionName)
	if err != nil {
		return 0, err
	}
	return partInfo.partitionID, nil
}

func (m *MetaCache) GetPartitions(ctx context.Context, database, collectionName string) (map[string]typeutil.UniqueID, error) {
	_, err := m.GetCollectionID(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}

	method := "GetPartitions"
	m.mu.RLock()

	var collInfo *collectionInfo
	var ok bool
	db, dbOk := m.collInfo[database]
	if dbOk {
		collInfo, ok = db[collectionName]
	}

	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("can't find collection name:%s", collectionName)
	}

	if collInfo.partInfo == nil || len(collInfo.partInfo) == 0 {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		m.mu.RUnlock()

		partitions, err := m.showPartitions(ctx, database, collectionName)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		err = m.updatePartitions(partitions, database, collectionName)
		if err != nil {
			return nil, err
		}
		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		log.Debug("proxy", zap.Any("GetPartitions:partitions after update", partitions), zap.String("collectionName", collectionName))
		ret := make(map[string]typeutil.UniqueID)
		partInfo := m.collInfo[database][collectionName].partInfo
		for k, v := range partInfo {
			ret[k] = v.partitionID
		}
		return ret, nil

	}

	defer m.mu.RUnlock()
	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()

	ret := make(map[string]typeutil.UniqueID)
	partInfo := collInfo.partInfo
	for k, v := range partInfo {
		ret[k] = v.partitionID
	}

	return ret, nil
}

func (m *MetaCache) GetPartitionInfo(ctx context.Context, database, collectionName string, partitionName string) (*partitionInfo, error) {
	_, err := m.GetCollectionID(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()

	var collInfo *collectionInfo
	var ok bool
	db, dbOk := m.collInfo[database]
	if dbOk {
		collInfo, ok = db[collectionName]
	}

	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("can't find collection name:%s", collectionName)
	}

	var partInfo *partitionInfo
	partInfo, ok = collInfo.partInfo[partitionName]
	m.mu.RUnlock()

	method := "GetPartitionInfo"
	if !ok {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		partitions, err := m.showPartitions(ctx, database, collectionName)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		err = m.updatePartitions(partitions, database, collectionName)
		if err != nil {
			return nil, err
		}
		metrics.ProxyUpdateCacheLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
		log.Debug("proxy", zap.Any("GetPartitionID:partitions after update", partitions), zap.String("collectionName", collectionName))
		partInfo, ok = m.collInfo[database][collectionName].partInfo[partitionName]
		if !ok {
			return nil, merr.WrapErrPartitionNotFound(partitionName)
		}
	}
	metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()
	return &partitionInfo{
		partitionID:         partInfo.partitionID,
		createdTimestamp:    partInfo.createdTimestamp,
		createdUtcTimestamp: partInfo.createdUtcTimestamp,
	}, nil
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
	if coll.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, common.NewStatusError(coll.GetStatus().GetErrorCode(), coll.GetStatus().GetReason())
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
	}
	for _, field := range coll.Schema.Fields {
		if field.FieldID >= common.StartOfUserFieldID {
			resp.Schema.Fields = append(resp.Schema.Fields, field)
		}
	}
	return resp, nil
}

func (m *MetaCache) showPartitions(ctx context.Context, dbName string, collectionName string) (*milvuspb.ShowPartitionsResponse, error) {
	req := &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		DbName:         dbName,
		CollectionName: collectionName,
	}

	partitions, err := m.rootCoord.ShowPartitions(ctx, req)
	if err != nil {
		return nil, err
	}
	if partitions.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, fmt.Errorf("%s", partitions.Status.Reason)
	}

	if len(partitions.PartitionIDs) != len(partitions.PartitionNames) {
		return nil, fmt.Errorf("partition ids len: %d doesn't equal Partition name len %d",
			len(partitions.PartitionIDs), len(partitions.PartitionNames))
	}

	return partitions, nil
}

func (m *MetaCache) updatePartitions(partitions *milvuspb.ShowPartitionsResponse, database, collectionName string) error {
	_, dbOk := m.collInfo[database]
	if !dbOk {
		m.collInfo[database] = make(map[string]*collectionInfo)
	}

	_, ok := m.collInfo[database][collectionName]
	if !ok {
		m.collInfo[database][collectionName] = &collectionInfo{
			partInfo: map[string]*partitionInfo{},
		}
	}
	partInfo := m.collInfo[database][collectionName].partInfo
	if partInfo == nil {
		partInfo = map[string]*partitionInfo{}
	}

	// check partitionID, createdTimestamp and utcstamp has sam element numbers
	if len(partitions.PartitionNames) != len(partitions.CreatedTimestamps) || len(partitions.PartitionNames) != len(partitions.CreatedUtcTimestamps) {
		return errors.New("partition names and timestamps number is not aligned, response " + partitions.String())
	}

	for i := 0; i < len(partitions.PartitionIDs); i++ {
		if _, ok := partInfo[partitions.PartitionNames[i]]; !ok {
			partInfo[partitions.PartitionNames[i]] = &partitionInfo{
				partitionID:         partitions.PartitionIDs[i],
				createdTimestamp:    partitions.CreatedTimestamps[i],
				createdUtcTimestamp: partitions.CreatedUtcTimestamps[i],
			}
		}
	}
	m.collInfo[database][collectionName].partInfo = partInfo
	return nil
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

	db, dbOk := m.collInfo[database]
	if dbOk {
		_, ok = db[collectionName]
	}

	if !ok {
		return
	}

	partInfo := m.collInfo[database][collectionName].partInfo
	if partInfo == nil {
		return
	}
	delete(partInfo, partitionName)
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
func (m *MetaCache) GetShards(ctx context.Context, withCache bool, database, collectionName string) (map[string][]nodeInfo, error) {
	info, err := m.GetCollectionInfo(ctx, database, collectionName)
	if err != nil {
		return nil, err
	}

	method := "GetShards"
	if withCache {
		var shardLeaders *shardLeaders
		info.leaderMutex.RLock()
		shardLeaders = info.shardLeaders
		info.leaderMutex.RUnlock()

		if shardLeaders != nil && !shardLeaders.deprecated.Load() {
			metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheHitLabel).Inc()
			iterator := shardLeaders.GetReader()
			return iterator.Shuffle(), nil
		}

		metrics.ProxyCacheStatsCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), method, metrics.CacheMissLabel).Inc()
		log.Info("no shard cache for collection, try to get shard leaders from QueryCoord",
			zap.String("collectionName", collectionName))
	}
	req := &querypb.GetShardLeadersRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetShardLeaders),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionID: info.collID,
	}

	// retry until service available or context timeout
	var resp *querypb.GetShardLeadersResponse
	childCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	tr := timerecord.NewTimeRecorder("UpdateShardCache")
	err = retry.Do(childCtx, func() error {
		resp, err = m.queryCoord.GetShardLeaders(ctx, req)
		if err != nil {
			return retry.Unrecoverable(err)
		}
		if resp.Status.ErrorCode == commonpb.ErrorCode_Success {
			return nil
		}
		// do not retry unless got NoReplicaAvailable from querycoord
		if resp.Status.ErrorCode != commonpb.ErrorCode_NoReplicaAvailable {
			return retry.Unrecoverable(fmt.Errorf("fail to get shard leaders from QueryCoord: %s", resp.Status.Reason))
		}
		return fmt.Errorf("fail to get shard leaders from QueryCoord: %s", resp.Status.Reason)
	})
	if err != nil {
		return nil, err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, fmt.Errorf("fail to get shard leaders from QueryCoord: %s", resp.Status.Reason)
	}

	shards := parseShardLeaderList2QueryNode(resp.GetShards())

	info, err = m.GetCollectionInfo(ctx, database, collectionName)
	if err != nil {
		return nil, fmt.Errorf("failed to get shards, collection %s not found", collectionName)
	}
	// lock leader
	info.leaderMutex.Lock()
	oldShards := info.shardLeaders
	info.shardLeaders = &shardLeaders{
		shardLeaders: shards,
		deprecated:   atomic.NewBool(false),
		idx:          atomic.NewInt64(0),
	}
	iterator := info.shardLeaders.GetReader()
	info.leaderMutex.Unlock()

	ret := iterator.Shuffle()
	oldLeaders := make(map[string][]nodeInfo)
	if oldShards != nil {
		oldLeaders = oldShards.shardLeaders
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
	m.mu.RLock()
	var info *collectionInfo
	var ok bool
	db, dbOk := m.collInfo[database]
	if !dbOk {
		m.mu.RUnlock()
		log.Warn("not found database", zap.String("dbName", database))
		return
	}
	info, ok = db[collectionName]
	m.mu.RUnlock()
	if ok {
		info.deprecateLeaderCache()
	}

}

func (m *MetaCache) expireShardLeaderCache(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(params.Params.ProxyCfg.ShardLeaderCacheInterval.GetAsDuration(time.Second))
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Info("stop periodically update meta cache")
				return
			case <-ticker.C:
				m.mu.RLock()
				for database, db := range m.collInfo {
					log.Info("expire all shard leader cache",
						zap.String("database", database),
						zap.Strings("collections", lo.Keys(db)))
					for _, info := range db {
						info.deprecateLeaderCache()
					}
				}
				m.mu.RUnlock()
			}
		}
	}()
}

func (m *MetaCache) InitPolicyInfo(info []string, userRoles []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

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

func (m *MetaCache) RefreshPolicyInfo(op typeutil.CacheOp) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if op.OpKey == "" {
		return errors.New("empty op key")
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
	default:
		return fmt.Errorf("invalid opType, op_type: %d, op_key: %s", int(op.OpType), op.OpKey)
	}
	return nil
}

func (m *MetaCache) RemoveDatabase(ctx context.Context, database string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.collInfo, database)
}
