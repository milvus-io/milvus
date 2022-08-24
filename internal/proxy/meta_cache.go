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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Cache is the interface for system meta data cache
type Cache interface {
	// GetCollectionID get collection's id by name.
	GetCollectionID(ctx context.Context, collectionName string) (typeutil.UniqueID, error)
	// GetCollectionInfo get collection's information by name, such as collection id, schema, and etc.
	GetCollectionInfo(ctx context.Context, collectionName string) (*collectionInfo, error)
	// GetPartitionID get partition's identifier of specific collection.
	GetPartitionID(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error)
	// GetPartitions get all partitions' id of specific collection.
	GetPartitions(ctx context.Context, collectionName string) (map[string]typeutil.UniqueID, error)
	// GetPartitionInfo get partition's info.
	GetPartitionInfo(ctx context.Context, collectionName string, partitionName string) (*partitionInfo, error)
	// GetCollectionSchema get collection's schema.
	GetCollectionSchema(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error)
	GetShards(ctx context.Context, withCache bool, collectionName string) (map[string][]nodeInfo, error)
	ClearShards(collectionName string)
	RemoveCollection(ctx context.Context, collectionName string)
	RemoveCollectionsByID(ctx context.Context, collectionID UniqueID)
	RemovePartition(ctx context.Context, collectionName string, partitionName string)

	// GetCredentialInfo operate credential cache
	GetCredentialInfo(ctx context.Context, username string) (*internalpb.CredentialInfo, error)
	RemoveCredential(username string)
	UpdateCredential(credInfo *internalpb.CredentialInfo)

	GetPrivilegeInfo(ctx context.Context) []string
	GetUserRole(username string) []string
	RefreshPolicyInfo(op typeutil.CacheOp) error
	InitPolicyInfo(info []string, userRoles []string)
}

type collectionInfo struct {
	collID              typeutil.UniqueID
	schema              *schemapb.CollectionSchema
	partInfo            map[string]*partitionInfo
	shardLeaders        map[string][]nodeInfo
	leaderMutex         sync.Mutex
	createdTimestamp    uint64
	createdUtcTimestamp uint64
	isLoaded            bool
}

// CloneShardLeaders returns a copy of shard leaders
// leaderMutex shall be accuired before invoking this method
func (c *collectionInfo) CloneShardLeaders() map[string][]nodeInfo {
	m := make(map[string][]nodeInfo)
	for channel, leaders := range c.shardLeaders {
		l := make([]nodeInfo, len(leaders))
		copy(l, leaders)
		m[channel] = l
	}
	return m
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

	collInfo       map[string]*collectionInfo
	credMap        map[string]*internalpb.CredentialInfo // cache for credential, lazy load
	privilegeInfos map[string]struct{}                   // privileges cache
	userToRoles    map[string]map[string]struct{}        // user to role cache
	mu             sync.RWMutex
	credMut        sync.RWMutex
	privilegeMut   sync.RWMutex
	shardMgr       *shardClientMgr
}

// globalMetaCache is singleton instance of Cache
var globalMetaCache Cache

// InitMetaCache initializes globalMetaCache
func InitMetaCache(ctx context.Context, rootCoord types.RootCoord, queryCoord types.QueryCoord, shardMgr *shardClientMgr) error {
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
	log.Debug("success to init meta cache", zap.Strings("policy_infos", resp.PolicyInfos))
	return nil
}

// NewMetaCache creates a MetaCache with provided RootCoord and QueryNode
func NewMetaCache(rootCoord types.RootCoord, queryCoord types.QueryCoord, shardMgr *shardClientMgr) (*MetaCache, error) {
	return &MetaCache{
		rootCoord:      rootCoord,
		queryCoord:     queryCoord,
		collInfo:       map[string]*collectionInfo{},
		credMap:        map[string]*internalpb.CredentialInfo{},
		shardMgr:       shardMgr,
		privilegeInfos: map[string]struct{}{},
		userToRoles:    map[string]map[string]struct{}{},
	}, nil
}

// GetCollectionID returns the corresponding collection id for provided collection name
func (m *MetaCache) GetCollectionID(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
	m.mu.RLock()
	collInfo, ok := m.collInfo[collectionName]

	if !ok {
		metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GeCollectionID", metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")
		m.mu.RUnlock()
		coll, err := m.describeCollection(ctx, collectionName)
		if err != nil {
			return 0, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.updateCollection(coll, collectionName)
		metrics.ProxyUpdateCacheLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
		collInfo = m.collInfo[collectionName]
		return collInfo.collID, nil
	}
	defer m.mu.RUnlock()
	metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GetCollectionID", metrics.CacheHitLabel).Inc()

	return collInfo.collID, nil
}

// GetCollectionInfo returns the collection information related to provided collection name
// If the information is not found, proxy will try to fetch information for other source (RootCoord for now)
func (m *MetaCache) GetCollectionInfo(ctx context.Context, collectionName string) (*collectionInfo, error) {
	m.mu.RLock()
	var collInfo *collectionInfo
	collInfo, ok := m.collInfo[collectionName]
	m.mu.RUnlock()

	if !ok {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GetCollectionInfo", metrics.CacheMissLabel).Inc()
		coll, err := m.describeCollection(ctx, collectionName)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.updateCollection(coll, collectionName)
		collInfo = m.collInfo[collectionName]
		m.mu.Unlock()
		metrics.ProxyUpdateCacheLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	}

	if !collInfo.isLoaded {
		// check if collection was loaded
		showResp, err := m.queryCoord.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_ShowCollections,
				SourceID: Params.ProxyCfg.GetNodeID(),
			},
		})
		if err != nil {
			return nil, err
		}
		if showResp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, errors.New(showResp.Status.Reason)
		}
		log.Debug("QueryCoord show collections",
			zap.Int64("collID", collInfo.collID),
			zap.Int64s("collections", showResp.GetCollectionIDs()),
			zap.Int64s("collectionsInMemoryPercentages", showResp.GetInMemoryPercentages()),
		)
		loaded := false
		for index, collID := range showResp.CollectionIDs {
			if collID == collInfo.collID && showResp.GetInMemoryPercentages()[index] >= int64(100) {
				loaded = true
				break
			}
		}
		if loaded {
			m.mu.Lock()
			m.collInfo[collectionName].isLoaded = true
			m.mu.Unlock()
		}
	}

	metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GetCollectionInfo", metrics.CacheHitLabel).Inc()
	return collInfo, nil
}

func (m *MetaCache) GetCollectionSchema(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
	m.mu.RLock()
	collInfo, ok := m.collInfo[collectionName]

	if !ok {
		metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GetCollectionSchema", metrics.CacheMissLabel).Inc()
		tr := timerecord.NewTimeRecorder("UpdateCache")
		m.mu.RUnlock()
		coll, err := m.describeCollection(ctx, collectionName)
		if err != nil {
			log.Warn("Failed to load collection from rootcoord ",
				zap.String("collection name ", collectionName),
				zap.Error(err))
			return nil, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.updateCollection(coll, collectionName)
		collInfo = m.collInfo[collectionName]
		metrics.ProxyUpdateCacheLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
		log.Debug("Reload collection from root coordinator ",
			zap.String("collection name ", collectionName),
			zap.Any("time (milliseconds) take ", tr.ElapseSpan().Milliseconds()))
		return collInfo.schema, nil
	}
	defer m.mu.RUnlock()
	metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GetCollectionSchema", metrics.CacheHitLabel).Inc()

	return collInfo.schema, nil
}

func (m *MetaCache) updateCollection(coll *milvuspb.DescribeCollectionResponse, collectionName string) {
	_, ok := m.collInfo[collectionName]
	if !ok {
		m.collInfo[collectionName] = &collectionInfo{}
	}
	m.collInfo[collectionName].schema = coll.Schema
	m.collInfo[collectionName].collID = coll.CollectionID
	m.collInfo[collectionName].createdTimestamp = coll.CreatedTimestamp
	m.collInfo[collectionName].createdUtcTimestamp = coll.CreatedUtcTimestamp
}

func (m *MetaCache) GetPartitionID(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	partInfo, err := m.GetPartitionInfo(ctx, collectionName, partitionName)
	if err != nil {
		return 0, err
	}
	return partInfo.partitionID, nil
}

func (m *MetaCache) GetPartitions(ctx context.Context, collectionName string) (map[string]typeutil.UniqueID, error) {
	_, err := m.GetCollectionID(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("can't find collection name:%s", collectionName)
	}

	if collInfo.partInfo == nil || len(collInfo.partInfo) == 0 {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GetPartitions", metrics.CacheMissLabel).Inc()
		m.mu.RUnlock()

		partitions, err := m.showPartitions(ctx, collectionName)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		err = m.updatePartitions(partitions, collectionName)
		if err != nil {
			return nil, err
		}
		metrics.ProxyUpdateCacheLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
		log.Debug("proxy", zap.Any("GetPartitions:partitions after update", partitions), zap.Any("collectionName", collectionName))
		ret := make(map[string]typeutil.UniqueID)
		partInfo := m.collInfo[collectionName].partInfo
		for k, v := range partInfo {
			ret[k] = v.partitionID
		}
		return ret, nil

	}
	defer m.mu.RUnlock()
	metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GetPartitions", metrics.CacheHitLabel).Inc()

	ret := make(map[string]typeutil.UniqueID)
	partInfo := m.collInfo[collectionName].partInfo
	for k, v := range partInfo {
		ret[k] = v.partitionID
	}

	return ret, nil
}

func (m *MetaCache) GetPartitionInfo(ctx context.Context, collectionName string, partitionName string) (*partitionInfo, error) {
	_, err := m.GetCollectionID(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("can't find collection name:%s", collectionName)
	}

	var partInfo *partitionInfo
	partInfo, ok = collInfo.partInfo[partitionName]
	m.mu.RUnlock()

	if !ok {
		tr := timerecord.NewTimeRecorder("UpdateCache")
		metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GetPartitionInfo", metrics.CacheMissLabel).Inc()
		partitions, err := m.showPartitions(ctx, collectionName)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		err = m.updatePartitions(partitions, collectionName)
		if err != nil {
			return nil, err
		}
		metrics.ProxyUpdateCacheLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
		log.Debug("proxy", zap.Any("GetPartitionID:partitions after update", partitions), zap.Any("collectionName", collectionName))
		partInfo, ok = m.collInfo[collectionName].partInfo[partitionName]
		if !ok {
			return nil, fmt.Errorf("partitionID of partitionName:%s can not be find", partitionName)
		}
	}
	metrics.ProxyCacheHitCounter.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.GetNodeID(), 10), "GetPartitionInfo", metrics.CacheHitLabel).Inc()
	return &partitionInfo{
		partitionID:         partInfo.partitionID,
		createdTimestamp:    partInfo.createdTimestamp,
		createdUtcTimestamp: partInfo.createdUtcTimestamp,
	}, nil
}

// Get the collection information from rootcoord.
func (m *MetaCache) describeCollection(ctx context.Context, collectionName string) (*milvuspb.DescribeCollectionResponse, error) {
	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeCollection,
		},
		CollectionName: collectionName,
	}
	coll, err := m.rootCoord.DescribeCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	if coll.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(coll.Status.Reason)
	}
	resp := &milvuspb.DescribeCollectionResponse{
		Status: coll.Status,
		Schema: &schemapb.CollectionSchema{
			Name:        coll.Schema.Name,
			Description: coll.Schema.Description,
			AutoID:      coll.Schema.AutoID,
			Fields:      make([]*schemapb.FieldSchema, 0),
		},
		CollectionID:         coll.CollectionID,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		CreatedTimestamp:     coll.CreatedTimestamp,
		CreatedUtcTimestamp:  coll.CreatedUtcTimestamp,
	}
	for _, field := range coll.Schema.Fields {
		if field.FieldID >= common.StartOfUserFieldID {
			resp.Schema.Fields = append(resp.Schema.Fields, field)
		}
	}
	return resp, nil
}

func (m *MetaCache) showPartitions(ctx context.Context, collectionName string) (*milvuspb.ShowPartitionsResponse, error) {
	req := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
		},
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

func (m *MetaCache) updatePartitions(partitions *milvuspb.ShowPartitionsResponse, collectionName string) error {
	_, ok := m.collInfo[collectionName]
	if !ok {
		m.collInfo[collectionName] = &collectionInfo{
			partInfo: map[string]*partitionInfo{},
		}
	}
	partInfo := m.collInfo[collectionName].partInfo
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
	m.collInfo[collectionName].partInfo = partInfo
	return nil
}

func (m *MetaCache) RemoveCollection(ctx context.Context, collectionName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.collInfo, collectionName)
}

func (m *MetaCache) RemoveCollectionsByID(ctx context.Context, collectionID UniqueID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.collInfo {
		if v.collID == collectionID {
			delete(m.collInfo, k)
		}
	}
}

func (m *MetaCache) RemovePartition(ctx context.Context, collectionName, partitionName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.collInfo[collectionName]
	if !ok {
		return
	}
	partInfo := m.collInfo[collectionName].partInfo
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
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_GetCredential,
			},
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
func (m *MetaCache) GetShards(ctx context.Context, withCache bool, collectionName string) (map[string][]nodeInfo, error) {
	info, err := m.GetCollectionInfo(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	if withCache {
		if len(info.shardLeaders) > 0 {
			info.leaderMutex.Lock()
			updateShardsWithRoundRobin(info.shardLeaders)

			shards := info.CloneShardLeaders()
			info.leaderMutex.Unlock()
			return shards, nil
		}
		log.Info("no shard cache for collection, try to get shard leaders from QueryCoord",
			zap.String("collectionName", collectionName))
	}
	req := &querypb.GetShardLeadersRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_GetShardLeaders,
			SourceID: Params.ProxyCfg.GetNodeID(),
		},
		CollectionID: info.collID,
	}

	// retry until service available or context timeout
	var resp *querypb.GetShardLeadersResponse
	childCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
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

	// manipulate info in map, get map returns a copy of the information
	m.mu.RLock()
	info = m.collInfo[collectionName]
	// lock leader
	info.leaderMutex.Lock()
	oldShards := info.shardLeaders
	info.shardLeaders = shards
	info.leaderMutex.Unlock()
	m.mu.RUnlock()

	// update refcnt in shardClientMgr
	ret := info.CloneShardLeaders()
	_ = m.shardMgr.UpdateShardLeaders(oldShards, ret)
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

// ClearShards clear the shard leader cache of a collection
func (m *MetaCache) ClearShards(collectionName string) {
	log.Info("clearing shard cache for collection", zap.String("collectionName", collectionName))
	m.mu.Lock()
	info, ok := m.collInfo[collectionName]
	if ok {
		m.collInfo[collectionName].shardLeaders = nil
	}
	m.mu.Unlock()
	// delete refcnt in shardClientMgr
	if ok {
		_ = m.shardMgr.UpdateShardLeaders(info.shardLeaders, nil)
	}
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
