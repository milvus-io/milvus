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

package querycoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

const (
	collectionMetaPrefix   = "queryCoord-collectionMeta"
	dmChannelMetaPrefix    = "queryCoord-dmChannelWatchInfo"
	deltaChannelMetaPrefix = "queryCoord-deltaChannel"
	ReplicaMetaPrefix      = "queryCoord-ReplicaMeta"
)

type col2SegmentInfos = map[UniqueID][]*querypb.SegmentInfo
type col2SealedSegmentChangeInfos = map[UniqueID]*querypb.SealedSegmentsChangeInfo

// Meta contains information about all loaded collections and partitions, including segment information and vchannel information
type Meta interface {
	reloadFromKV() error
	setKvClient(kv kv.MetaKv)

	showCollections() []*querypb.CollectionInfo
	hasCollection(collectionID UniqueID) bool
	getCollectionInfoByID(collectionID UniqueID) (*querypb.CollectionInfo, error)
	addCollection(collectionID UniqueID, loadType querypb.LoadType, schema *schemapb.CollectionSchema) error
	releaseCollection(collectionID UniqueID) error

	addPartitions(collectionID UniqueID, partitionIDs []UniqueID) error
	showPartitions(collectionID UniqueID) ([]*querypb.PartitionStates, error)
	hasPartition(collectionID UniqueID, partitionID UniqueID) bool
	hasReleasePartition(collectionID UniqueID, partitionID UniqueID) bool
	releasePartitions(collectionID UniqueID, partitionIDs []UniqueID) error

	showSegmentInfos(collectionID UniqueID, partitionIDs []UniqueID) []*querypb.SegmentInfo
	getSegmentInfoByID(segmentID UniqueID) (*querypb.SegmentInfo, error)
	getSegmentInfosByNode(nodeID int64) []*querypb.SegmentInfo
	getSegmentInfosByNodeAndCollection(nodeID, collectionID int64) []*querypb.SegmentInfo
	saveSegmentInfo(segment *querypb.SegmentInfo) error

	getPartitionStatesByID(collectionID UniqueID, partitionID UniqueID) (*querypb.PartitionStates, error)

	getDmChannelInfosByNodeID(nodeID int64) []*querypb.DmChannelWatchInfo
	setDmChannelInfos(channelInfos []*querypb.DmChannelWatchInfo) error

	getDeltaChannelsByCollectionID(collectionID UniqueID) ([]*datapb.VchannelInfo, error)
	setDeltaChannel(collectionID UniqueID, info []*datapb.VchannelInfo) error

	getQueryChannelInfoByID(collectionID UniqueID) *querypb.QueryChannelInfo

	setLoadType(collectionID UniqueID, loadType querypb.LoadType) error
	setLoadPercentage(collectionID UniqueID, partitionID UniqueID, percentage int64, loadType querypb.LoadType) error
	//printMeta()
	saveGlobalSealedSegInfos(saves col2SegmentInfos) (col2SealedSegmentChangeInfos, error)
	removeGlobalSealedSegInfos(collectionID UniqueID, partitionIDs []UniqueID) (col2SealedSegmentChangeInfos, error)

	getWatchedChannelsByNodeID(nodeID int64) *querypb.UnsubscribeChannelInfo

	generateReplica(collectionID int64, partitionIds []int64) (*milvuspb.ReplicaInfo, error)
	addReplica(replica *milvuspb.ReplicaInfo) error
	setReplicaInfo(info *milvuspb.ReplicaInfo) error
	getReplicaByID(replicaID int64) (*milvuspb.ReplicaInfo, error)
	getReplicasByCollectionID(collectionID int64) ([]*milvuspb.ReplicaInfo, error)
	getReplicasByNodeID(nodeID int64) ([]*milvuspb.ReplicaInfo, error)
	applyReplicaBalancePlan(p *balancePlan) error
	updateShardLeader(replicaID UniqueID, dmChannel string, leaderID UniqueID, leaderAddr string) error
}

// MetaReplica records the current load information on all querynodes
type MetaReplica struct {
	ctx    context.Context
	cancel context.CancelFunc
	client kv.MetaKv // client of a reliable kv service, i.e. etcd client
	//DDL lock
	clientMutex sync.Mutex
	factory     dependency.Factory
	idAllocator func() (UniqueID, error)

	//sync.RWMutex
	collectionInfos   map[UniqueID]*querypb.CollectionInfo
	collectionMu      sync.RWMutex
	queryChannelInfos map[UniqueID]*querypb.QueryChannelInfo
	channelMu         sync.RWMutex
	deltaChannelInfos map[UniqueID][]*datapb.VchannelInfo
	deltaChannelMu    sync.RWMutex
	dmChannelInfos    map[string]*querypb.DmChannelWatchInfo
	dmChannelMu       sync.RWMutex

	segmentsInfo *segmentsInfo
	//partitionStates map[UniqueID]*querypb.PartitionStates
	replicas *ReplicaInfos
}

func newMeta(ctx context.Context, kv kv.MetaKv, factory dependency.Factory, idAllocator func() (UniqueID, error)) (Meta, error) {
	childCtx, cancel := context.WithCancel(ctx)
	collectionInfos := make(map[UniqueID]*querypb.CollectionInfo)
	queryChannelInfos := make(map[UniqueID]*querypb.QueryChannelInfo)
	deltaChannelInfos := make(map[UniqueID][]*datapb.VchannelInfo)
	dmChannelInfos := make(map[string]*querypb.DmChannelWatchInfo)

	m := &MetaReplica{
		ctx:         childCtx,
		cancel:      cancel,
		factory:     factory,
		idAllocator: idAllocator,

		collectionInfos:   collectionInfos,
		queryChannelInfos: queryChannelInfos,
		deltaChannelInfos: deltaChannelInfos,
		dmChannelInfos:    dmChannelInfos,

		segmentsInfo: newSegmentsInfo(kv),
		replicas:     NewReplicaInfos(),
	}
	m.setKvClient(kv)

	err := m.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *MetaReplica) reloadFromKV() error {
	log.Info("recovery collections...")
	collectionKeys, collectionValues, err := m.getKvClient().LoadWithPrefix(collectionMetaPrefix)
	if err != nil {
		return err
	}
	for index := range collectionKeys {
		collectionID, err := strconv.ParseInt(filepath.Base(collectionKeys[index]), 10, 64)
		if err != nil {
			return err
		}
		collectionInfo := &querypb.CollectionInfo{}
		err = proto.Unmarshal([]byte(collectionValues[index]), collectionInfo)
		if err != nil {
			return err
		}
		m.collectionInfos[collectionID] = collectionInfo

		log.Debug("recovery collection",
			zap.Int64("collectionID", collectionID))
	}
	metrics.QueryCoordNumCollections.WithLabelValues().Set(float64(len(m.collectionInfos)))

	if err := m.segmentsInfo.loadSegments(); err != nil {
		return err
	}

	deltaChannelKeys, deltaChannelValues, err := m.getKvClient().LoadWithPrefix(deltaChannelMetaPrefix)
	if err != nil {
		return nil
	}
	for index, value := range deltaChannelValues {
		pathStrings := strings.Split(deltaChannelKeys[index], "/")
		collectionID, err := strconv.ParseInt(pathStrings[len(pathStrings)-2], 10, 64)
		if err != nil {
			return err
		}
		deltaChannelInfo := &datapb.VchannelInfo{}
		err = proto.Unmarshal([]byte(value), deltaChannelInfo)
		if err != nil {
			return err
		}
		m.deltaChannelInfos[collectionID] = append(m.deltaChannelInfos[collectionID], deltaChannelInfo)
	}

	dmChannelKeys, dmChannelValues, err := m.getKvClient().LoadWithPrefix(dmChannelMetaPrefix)
	if err != nil {
		return err
	}
	for index := range dmChannelKeys {
		dmChannel := filepath.Base(dmChannelKeys[index])
		dmChannelWatchInfo := &querypb.DmChannelWatchInfo{}
		err = proto.Unmarshal([]byte(dmChannelValues[index]), dmChannelWatchInfo)
		if err != nil {
			return err
		}
		if len(dmChannelWatchInfo.NodeIds) == 0 {
			dmChannelWatchInfo.NodeIds = []int64{dmChannelWatchInfo.NodeIDLoaded}
		}
		m.dmChannelInfos[dmChannel] = dmChannelWatchInfo
	}

	// Compatibility for old meta format
	// For collections that don't have replica(s), create 1 replica for them
	// Add replica into meta storage and rewrite collection
	dmChannels := make(map[UniqueID][]*querypb.DmChannelWatchInfo) // CollectionID -> []*DmChannelWatchInfo
	for _, dmc := range m.dmChannelInfos {
		dmChannels[dmc.CollectionID] = append(dmChannels[dmc.CollectionID], dmc)
	}
	for _, collectionInfo := range m.collectionInfos {
		if len(collectionInfo.ReplicaIds) == 0 {
			replica, err := m.generateReplica(collectionInfo.CollectionID, collectionInfo.PartitionIDs)
			if err != nil {
				return err
			}

			segments := m.showSegmentInfos(collectionInfo.CollectionID, collectionInfo.PartitionIDs)
			// remove duplicates
			nodes := make(map[UniqueID]struct{})
			for _, segment := range segments {
				for _, nodeID := range segment.NodeIds {
					nodes[nodeID] = struct{}{}
				}
			}
			for nodeID := range nodes {
				replica.NodeIds = append(replica.NodeIds, nodeID)
			}

			shardReplicas := make([]*milvuspb.ShardReplica, 0, len(dmChannels[collectionInfo.CollectionID]))
			for _, dmc := range dmChannels[collectionInfo.CollectionID] {
				shardReplicas = append(shardReplicas, &milvuspb.ShardReplica{
					LeaderID: dmc.NodeIDLoaded,
					// LeaderAddr: Will set it after the cluster is reloaded
					DmChannelName: dmc.DmChannel,
				})
			}

			err = m.addReplica(replica)
			if err != nil {
				log.Error("failed to add replica for old collection info format without replicas info",
					zap.Int64("collectionID", replica.CollectionID),
					zap.Error(err))
				return err
			}

			// DO NOT insert the replica into m.replicas
			// it will be recovered below
		}
	}

	replicaKeys, replicaValues, err := m.getKvClient().LoadWithPrefix(ReplicaMetaPrefix)
	if err != nil {
		return err
	}
	for i := range replicaKeys {
		replicaInfo := &milvuspb.ReplicaInfo{}
		err = proto.Unmarshal([]byte(replicaValues[i]), replicaInfo)
		if err != nil {
			return err
		}
		m.replicas.Insert(replicaInfo)
	}

	//TODO::update partition states
	log.Info("reload from kv finished")

	return nil
}

// Compatibility for old meta format, this retrieves node address from cluster.
// The leader address is not always valid
func reloadShardLeaderAddress(meta Meta, cluster Cluster) error {
	collections := meta.showCollections()

	reloadShardLeaderAddressFunc := func(idx int) error {
		collection := collections[idx]
		replicas, err := meta.getReplicasByCollectionID(collection.CollectionID)
		if err != nil {
			return err
		}

		for _, replica := range replicas {
			isModified := false
			for _, shard := range replica.ShardReplicas {
				if len(shard.LeaderAddr) == 0 {
					nodeInfo, err := cluster.getNodeInfoByID(shard.LeaderID)
					if err != nil {
						log.Warn("failed to retrieve the node's address",
							zap.Int64("nodeID", shard.LeaderID),
							zap.Error(err))
						continue
					}
					shard.LeaderAddr = nodeInfo.(*queryNode).address
					isModified = true
				}
			}

			if isModified {
				err := meta.setReplicaInfo(replica)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	concurrencyLevel := len(collections)
	if concurrencyLevel > runtime.NumCPU() {
		concurrencyLevel = runtime.NumCPU()
	}
	return funcutil.ProcessFuncParallel(len(collections), concurrencyLevel,
		reloadShardLeaderAddressFunc, "reloadShardLeaderAddressFunc")
}

func (m *MetaReplica) setKvClient(kv kv.MetaKv) {
	m.clientMutex.Lock()
	defer m.clientMutex.Unlock()
	m.client = kv
}

func (m *MetaReplica) getKvClient() kv.MetaKv {
	m.clientMutex.Lock()
	defer m.clientMutex.Unlock()
	return m.client
}

func (m *MetaReplica) showCollections() []*querypb.CollectionInfo {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	collections := make([]*querypb.CollectionInfo, 0)
	for _, info := range m.collectionInfos {
		collections = append(collections, proto.Clone(info).(*querypb.CollectionInfo))
	}
	return collections
}

func (m *MetaReplica) showPartitions(collectionID UniqueID) ([]*querypb.PartitionStates, error) {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	//TODO::should update after load collection
	results := make([]*querypb.PartitionStates, 0)
	if info, ok := m.collectionInfos[collectionID]; ok {
		for _, state := range info.PartitionStates {
			results = append(results, proto.Clone(state).(*querypb.PartitionStates))
		}
		return results, nil
	}

	return nil, errors.New("showPartitions: can't find collection in collectionInfos")
}

func (m *MetaReplica) hasCollection(collectionID UniqueID) bool {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	if _, ok := m.collectionInfos[collectionID]; ok {
		return true
	}

	return false
}

func (m *MetaReplica) hasPartition(collectionID UniqueID, partitionID UniqueID) bool {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		for _, id := range info.PartitionIDs {
			if partitionID == id {
				return true
			}
		}
	}

	return false
}

func (m *MetaReplica) hasReleasePartition(collectionID UniqueID, partitionID UniqueID) bool {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		for _, id := range info.ReleasedPartitionIDs {
			if partitionID == id {
				return true
			}
		}
	}

	return false
}

func (m *MetaReplica) addCollection(collectionID UniqueID, loadType querypb.LoadType, schema *schemapb.CollectionSchema) error {
	hasCollection := m.hasCollection(collectionID)
	if !hasCollection {
		var partitionIDs []UniqueID
		var partitionStates []*querypb.PartitionStates
		newCollection := &querypb.CollectionInfo{
			CollectionID:    collectionID,
			PartitionIDs:    partitionIDs,
			PartitionStates: partitionStates,
			LoadType:        loadType,
			Schema:          schema,
			ReplicaIds:      make([]int64, 0),
			ReplicaNumber:   0,
		}
		err := saveGlobalCollectionInfo(collectionID, newCollection, m.getKvClient())
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		m.collectionMu.Lock()
		m.collectionInfos[collectionID] = newCollection
		metrics.QueryCoordNumCollections.WithLabelValues().Set(float64(len(m.collectionInfos)))
		m.collectionMu.Unlock()
	}

	return nil
}

func (m *MetaReplica) addPartitions(collectionID UniqueID, partitionIDs []UniqueID) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		collectionInfo := proto.Clone(info).(*querypb.CollectionInfo)
		loadedPartitionID2State := make(map[UniqueID]*querypb.PartitionStates)
		for _, partitionID := range partitionIDs {
			loadedPartitionID2State[partitionID] = &querypb.PartitionStates{
				PartitionID: partitionID,
				State:       querypb.PartitionState_NotPresent,
			}
		}

		for offset, partitionID := range collectionInfo.PartitionIDs {
			loadedPartitionID2State[partitionID] = collectionInfo.PartitionStates[offset]
		}

		newPartitionIDs := make([]UniqueID, 0)
		newPartitionStates := make([]*querypb.PartitionStates, 0)
		for partitionID, state := range loadedPartitionID2State {
			newPartitionIDs = append(newPartitionIDs, partitionID)
			newPartitionStates = append(newPartitionStates, state)
		}

		newReleasedPartitionIDs := make([]UniqueID, 0)
		for _, releasedPartitionID := range collectionInfo.ReleasedPartitionIDs {
			if _, ok = loadedPartitionID2State[releasedPartitionID]; !ok {
				newReleasedPartitionIDs = append(newReleasedPartitionIDs, releasedPartitionID)
			}
		}

		collectionInfo.PartitionIDs = newPartitionIDs
		collectionInfo.PartitionStates = newPartitionStates
		collectionInfo.ReleasedPartitionIDs = newReleasedPartitionIDs

		log.Info("add a partition to MetaReplica", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", collectionInfo.PartitionIDs))
		err := saveGlobalCollectionInfo(collectionID, collectionInfo, m.getKvClient())
		if err != nil {
			log.Error("save collectionInfo error", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", collectionInfo.PartitionIDs), zap.Any("error", err.Error()))
			return err
		}
		m.collectionInfos[collectionID] = collectionInfo
		return nil
	}
	return fmt.Errorf("addPartition: can't find collection %d when add partition", collectionID)
}

func (m *MetaReplica) releaseCollection(collectionID UniqueID) error {
	collection, err := m.getCollectionInfoByID(collectionID)
	if err != nil {
		log.Warn("the collection has been released",
			zap.Int64("collectionID", collectionID))
		return nil
	}

	err = removeCollectionMeta(collectionID, collection.ReplicaIds, m.getKvClient())
	if err != nil {
		log.Warn("remove collectionInfo from etcd failed", zap.Int64("collectionID", collectionID), zap.Any("error", err.Error()))
		return err
	}

	m.collectionMu.Lock()
	delete(m.collectionInfos, collectionID)
	metrics.QueryCoordNumCollections.WithLabelValues().Set(float64(len(m.collectionInfos)))
	m.collectionMu.Unlock()

	m.deltaChannelMu.Lock()
	delete(m.deltaChannelInfos, collectionID)
	m.deltaChannelMu.Unlock()

	m.dmChannelMu.Lock()
	for dmChannel, info := range m.dmChannelInfos {
		if info.CollectionID == collectionID {
			delete(m.dmChannelInfos, dmChannel)
		}
	}
	m.dmChannelMu.Unlock()

	m.replicas.Remove(collection.ReplicaIds...)

	return nil
}

func (m *MetaReplica) releasePartitions(collectionID UniqueID, releasedPartitionIDs []UniqueID) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()
	info, ok := m.collectionInfos[collectionID]
	if !ok {
		return nil
	}
	collectionInfo := proto.Clone(info).(*querypb.CollectionInfo)

	releasedPartitionMap := make(map[UniqueID]struct{})
	for _, partitionID := range releasedPartitionIDs {
		releasedPartitionMap[partitionID] = struct{}{}
	}
	for _, partitionID := range collectionInfo.ReleasedPartitionIDs {
		releasedPartitionMap[partitionID] = struct{}{}
	}

	newPartitionIDs := make([]UniqueID, 0)
	newPartitionStates := make([]*querypb.PartitionStates, 0)
	for offset, partitionID := range collectionInfo.PartitionIDs {
		if _, ok = releasedPartitionMap[partitionID]; !ok {
			newPartitionIDs = append(newPartitionIDs, partitionID)
			newPartitionStates = append(newPartitionStates, collectionInfo.PartitionStates[offset])
		}
	}

	newReleasedPartitionIDs := make([]UniqueID, 0)
	for partitionID := range releasedPartitionMap {
		newReleasedPartitionIDs = append(newReleasedPartitionIDs, partitionID)
	}

	collectionInfo.PartitionIDs = newPartitionIDs
	collectionInfo.PartitionStates = newPartitionStates
	collectionInfo.ReleasedPartitionIDs = newReleasedPartitionIDs

	err := saveGlobalCollectionInfo(collectionID, collectionInfo, m.getKvClient())
	if err != nil {
		log.Error("releasePartition: remove partition infos error", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", releasedPartitionIDs), zap.Any("error", err.Error()))
		return err
	}

	m.collectionInfos[collectionID] = collectionInfo

	return nil
}

func (m *MetaReplica) saveGlobalSealedSegInfos(saves col2SegmentInfos) (col2SealedSegmentChangeInfos, error) {
	if len(saves) == 0 {
		return nil, nil
	}
	// generate segment change info according segment info to updated
	col2SegmentChangeInfos := make(col2SealedSegmentChangeInfos)

	segmentsCompactionFrom := make([]*querypb.SegmentInfo, 0)
	// get segmentInfos to colSegmentInfos
	for collectionID, onlineInfos := range saves {
		segmentsChangeInfo := &querypb.SealedSegmentsChangeInfo{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SealedSegmentsChangeInfo,
			},
			Infos: []*querypb.SegmentChangeInfo{},
		}
		for _, info := range onlineInfos {
			segmentID := info.SegmentID
			onlineNodeID := info.NodeID
			changeInfo := &querypb.SegmentChangeInfo{
				OnlineNodeID:   onlineNodeID,
				OnlineSegments: []*querypb.SegmentInfo{info},
			}
			offlineInfo, err := m.getSegmentInfoByID(segmentID)
			if err == nil {
				offlineNodeID := offlineInfo.NodeID
				// if the offline segment state is growing, it will not impact the global sealed segments
				if offlineInfo.SegmentState == commonpb.SegmentState_Sealed {
					changeInfo.OfflineNodeID = offlineNodeID
					changeInfo.OfflineSegments = []*querypb.SegmentInfo{offlineInfo}
				}
			}
			segmentsChangeInfo.Infos = append(segmentsChangeInfo.Infos, changeInfo)

			// generate offline segment change info if the loaded segment is compacted from other sealed segments
			for _, compactionSegmentID := range info.CompactionFrom {
				compactionSegmentInfo, err := m.getSegmentInfoByID(compactionSegmentID)
				if err == nil && compactionSegmentInfo.SegmentState == commonpb.SegmentState_Sealed {
					segmentsChangeInfo.Infos = append(segmentsChangeInfo.Infos, &querypb.SegmentChangeInfo{
						OfflineNodeID:   compactionSegmentInfo.NodeID,
						OfflineSegments: []*querypb.SegmentInfo{compactionSegmentInfo},
					})
					segmentsCompactionFrom = append(segmentsCompactionFrom, compactionSegmentInfo)
				} else {
					return nil, fmt.Errorf("saveGlobalSealedSegInfos: the compacted segment %d has not been loaded into memory", compactionSegmentID)
				}
			}
		}
		col2SegmentChangeInfos[collectionID] = segmentsChangeInfo
	}
	queryChannelInfosMap := make(map[UniqueID]*querypb.QueryChannelInfo)
	for collectionID, infos := range saves {
		// TODO silverxia change QueryChannelInfo struct to simplifed one
		// queryChannelInfo contains the GlobalSealedSegment list
		queryChannelInfo := m.getQueryChannelInfoByID(collectionID)

		// merge save segment info and existing GlobalSealedSegments
		seg2Info := make(map[UniqueID]*querypb.SegmentInfo)
		for _, segmentInfo := range queryChannelInfo.GlobalSealedSegments {
			segmentID := segmentInfo.SegmentID
			seg2Info[segmentID] = segmentInfo
		}
		for _, segmentInfo := range infos {
			segmentID := segmentInfo.SegmentID
			seg2Info[segmentID] = segmentInfo
		}

		globalSealedSegmentInfos := make([]*querypb.SegmentInfo, len(seg2Info))
		for _, info := range seg2Info {
			globalSealedSegmentInfos = append(globalSealedSegmentInfos, info)
		}
		queryChannelInfo.GlobalSealedSegments = globalSealedSegmentInfos
	}

	// save segmentInfo to etcd
	for _, infos := range saves {
		for _, info := range infos {
			if err := m.segmentsInfo.saveSegment(info); err != nil {
				panic(err)
			}
		}
	}

	// remove compacted segment info from etcd
	for _, segmentInfo := range segmentsCompactionFrom {
		if err := m.segmentsInfo.removeSegment(segmentInfo); err != nil {
			panic(err)
		}
	}

	// save sealedSegmentsChangeInfo to etcd
	saveKvs := make(map[string]string)
	// save segmentChangeInfo into etcd, query node will deal the changeInfo if the msgID key exist in etcd
	// avoid the produce process success but save meta to etcd failed
	// then the msgID key will not exist, and changeIndo will be ignored by query node
	for _, changeInfos := range col2SegmentChangeInfos {
		changeInfoBytes, err := proto.Marshal(changeInfos)
		if err != nil {
			return col2SegmentChangeInfos, err
		}
		// TODO:: segmentChangeInfo clear in etcd with coord gc and queryNode watch the changeInfo meta to deal changeInfoMsg
		changeInfoKey := fmt.Sprintf("%s/%d", util.ChangeInfoMetaPrefix, changeInfos.Base.MsgID)
		saveKvs[changeInfoKey] = string(changeInfoBytes)
	}

	err := m.getKvClient().MultiSave(saveKvs)
	if err != nil {
		panic(err)
	}

	// Write back to cache
	m.channelMu.Lock()
	for collectionID, channelInfo := range queryChannelInfosMap {
		m.queryChannelInfos[collectionID] = channelInfo
	}
	m.channelMu.Unlock()

	return col2SegmentChangeInfos, nil
}

func (m *MetaReplica) removeGlobalSealedSegInfos(collectionID UniqueID, partitionIDs []UniqueID) (col2SealedSegmentChangeInfos, error) {
	removes := m.showSegmentInfos(collectionID, partitionIDs)
	if len(removes) == 0 {
		return nil, nil
	}
	// get segmentInfos to remove
	segmentChangeInfos := &querypb.SealedSegmentsChangeInfo{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_SealedSegmentsChangeInfo,
		},
		Infos: []*querypb.SegmentChangeInfo{},
	}
	for _, info := range removes {
		offlineNodeID := info.NodeID
		changeInfo := &querypb.SegmentChangeInfo{
			OfflineNodeID:   offlineNodeID,
			OfflineSegments: []*querypb.SegmentInfo{info},
		}

		segmentChangeInfos.Infos = append(segmentChangeInfos.Infos, changeInfo)
	}

	// produce sealedSegmentChangeInfos to query channel
	queryChannelInfo := m.getQueryChannelInfoByID(collectionID)

	// update segmentInfo, queryChannelInfo meta to cache and etcd
	seg2Info := make(map[UniqueID]*querypb.SegmentInfo)
	for _, segmentInfo := range queryChannelInfo.GlobalSealedSegments {
		segmentID := segmentInfo.SegmentID
		seg2Info[segmentID] = segmentInfo
	}

	for _, segmentInfo := range removes {
		segmentID := segmentInfo.SegmentID
		delete(seg2Info, segmentID)
	}

	globalSealedSegmentInfos := make([]*querypb.SegmentInfo, 0)
	for _, info := range seg2Info {
		globalSealedSegmentInfos = append(globalSealedSegmentInfos, info)
	}
	queryChannelInfo.GlobalSealedSegments = globalSealedSegmentInfos

	// remove meta from etcd
	for _, info := range removes {
		if err := m.segmentsInfo.removeSegment(info); err != nil {
			panic(err)
		}
	}

	saveKvs := make(map[string]string)
	// save segmentChangeInfo into etcd, query node will deal the changeInfo if the msgID key exist in etcd
	// avoid the produce process success but save meta to etcd failed
	// then the msgID key will not exist, and changeIndo will be ignored by query node
	changeInfoBytes, err := proto.Marshal(segmentChangeInfos)
	if err != nil {
		return col2SealedSegmentChangeInfos{collectionID: segmentChangeInfos}, err
	}
	// TODO:: segmentChangeInfo clear in etcd with coord gc and queryNode watch the changeInfo meta to deal changeInfoMsg
	changeInfoKey := fmt.Sprintf("%s/%d", util.ChangeInfoMetaPrefix, segmentChangeInfos.Base.MsgID)
	saveKvs[changeInfoKey] = string(changeInfoBytes)

	err = m.getKvClient().MultiSave(saveKvs)
	if err != nil {
		panic(err)
	}

	m.channelMu.Lock()
	m.queryChannelInfos[collectionID] = queryChannelInfo
	m.channelMu.Unlock()

	return col2SealedSegmentChangeInfos{collectionID: segmentChangeInfos}, nil
}

func (m *MetaReplica) showSegmentInfos(collectionID UniqueID, partitionIDs []UniqueID) []*querypb.SegmentInfo {
	ignorePartitionCmp := len(partitionIDs) == 0
	partitionFilter := make(map[int64]struct{})
	for _, pid := range partitionIDs {
		partitionFilter[pid] = struct{}{}
	}

	segments := m.segmentsInfo.getSegments()
	var res []*querypb.SegmentInfo
	for _, segment := range segments {
		_, ok := partitionFilter[segment.GetPartitionID()]
		if (ignorePartitionCmp || ok) && segment.GetCollectionID() == collectionID {
			res = append(res, segment)
		}
	}
	return res
}

func (m *MetaReplica) getSegmentInfoByID(segmentID UniqueID) (*querypb.SegmentInfo, error) {
	segment := m.segmentsInfo.getSegment(segmentID)
	if segment != nil {
		return segment, nil
	}

	return nil, errors.New("getSegmentInfoByID: can't find segmentID in segmentInfos")
}

func (m *MetaReplica) getSegmentInfosByNode(nodeID int64) []*querypb.SegmentInfo {
	var res []*querypb.SegmentInfo
	segments := m.segmentsInfo.getSegments()
	for _, segment := range segments {
		if funcutil.SliceContain(segment.NodeIds, nodeID) {
			res = append(res, segment)
		}
	}
	return res
}
func (m *MetaReplica) getSegmentInfosByNodeAndCollection(nodeID, collectionID int64) []*querypb.SegmentInfo {
	var res []*querypb.SegmentInfo
	segments := m.segmentsInfo.getSegments()
	for _, segment := range segments {
		if segment.GetCollectionID() == collectionID && funcutil.SliceContain(segment.NodeIds, nodeID) {
			res = append(res, segment)
		}
	}
	return res
}

func (m *MetaReplica) saveSegmentInfo(segment *querypb.SegmentInfo) error {
	return m.segmentsInfo.saveSegment(segment)
}

func (m *MetaReplica) getCollectionInfoByID(collectionID UniqueID) (*querypb.CollectionInfo, error) {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		return proto.Clone(info).(*querypb.CollectionInfo), nil
	}

	return nil, fmt.Errorf("collection not found, maybe not loaded")
}

func (m *MetaReplica) getPartitionStatesByID(collectionID UniqueID, partitionID UniqueID) (*querypb.PartitionStates, error) {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		for offset, id := range info.PartitionIDs {
			if id == partitionID {
				return proto.Clone(info.PartitionStates[offset]).(*querypb.PartitionStates), nil
			}
		}
		return nil, errors.New("getPartitionStateByID: can't find partitionID in partitionStates")
	}

	return nil, errors.New("getPartitionStateByID: can't find collectionID in collectionInfo")
}

func (m *MetaReplica) getDmChannelInfosByNodeID(nodeID int64) []*querypb.DmChannelWatchInfo {
	m.dmChannelMu.RLock()
	defer m.dmChannelMu.RUnlock()

	var watchedDmChannelWatchInfo []*querypb.DmChannelWatchInfo
	for _, channelInfo := range m.dmChannelInfos {
		if funcutil.SliceContain(channelInfo.NodeIds, nodeID) {
			watchedDmChannelWatchInfo = append(watchedDmChannelWatchInfo, proto.Clone(channelInfo).(*querypb.DmChannelWatchInfo))
		}
	}

	return watchedDmChannelWatchInfo
}

func (m *MetaReplica) setDmChannelInfos(dmChannelWatchInfos []*querypb.DmChannelWatchInfo) error {
	m.dmChannelMu.Lock()
	defer m.dmChannelMu.Unlock()

	for _, channelInfo := range dmChannelWatchInfos {
		if old, ok := m.dmChannelInfos[channelInfo.DmChannel]; ok {
			channelInfo.NodeIds = append(channelInfo.NodeIds, old.NodeIds...)
		}
	}

	err := saveDmChannelWatchInfos(dmChannelWatchInfos, m.getKvClient())
	if err != nil {
		return err
	}
	for _, channelInfo := range dmChannelWatchInfos {
		m.dmChannelInfos[channelInfo.DmChannel] = channelInfo
	}

	return nil
}

// createQueryChannel creates topic names for search channel and search result channel
// Search channel's suffix is fixed with "-0"
// Search result channel's suffix is fixed with "-0"
func (m *MetaReplica) createQueryChannel(collectionID UniqueID) *querypb.QueryChannelInfo {
	allocatedQueryChannel := fmt.Sprintf("%s-0", Params.CommonCfg.QueryCoordSearch)
	allocatedQueryResultChannel := fmt.Sprintf("%s-0", Params.CommonCfg.QueryCoordSearchResult)

	log.Info("query coordinator is creating query channel",
		zap.String("query channel name", allocatedQueryChannel),
		zap.String("query result channel name", allocatedQueryResultChannel))

	seekPosition := &internalpb.MsgPosition{
		ChannelName: allocatedQueryChannel,
	}
	segmentInfos := m.showSegmentInfos(collectionID, nil)
	info := &querypb.QueryChannelInfo{
		CollectionID:         collectionID,
		QueryChannel:         allocatedQueryChannel,
		QueryResultChannel:   allocatedQueryResultChannel,
		GlobalSealedSegments: segmentInfos,
		SeekPosition:         seekPosition,
	}

	return info
}

// Get delta channel info for collection, so far all the collection share the same query channel 0
func (m *MetaReplica) getDeltaChannelsByCollectionID(collectionID UniqueID) ([]*datapb.VchannelInfo, error) {
	m.deltaChannelMu.RLock()
	defer m.deltaChannelMu.RUnlock()
	if infos, ok := m.deltaChannelInfos[collectionID]; ok {
		return infos, nil
	}

	return nil, fmt.Errorf("delta channel not exist in meta, collectionID = %d", collectionID)
}

func (m *MetaReplica) setDeltaChannel(collectionID UniqueID, infos []*datapb.VchannelInfo) error {
	m.deltaChannelMu.Lock()
	defer m.deltaChannelMu.Unlock()
	_, ok := m.deltaChannelInfos[collectionID]
	if ok {
		log.Debug("delta channel already exist", zap.Any("collectionID", collectionID))
		return nil
	}

	err := saveDeltaChannelInfo(collectionID, infos, m.getKvClient())
	if err != nil {
		log.Error("save delta channel info error", zap.Int64("collectionID", collectionID), zap.Error(err))
		return err
	}
	log.Info("save delta channel infos to meta", zap.Any("collectionID", collectionID))
	m.deltaChannelInfos[collectionID] = infos
	return nil
}

// Get Query channel info for collection, so far all the collection share the same query channel 0
func (m *MetaReplica) getQueryChannelInfoByID(collectionID UniqueID) *querypb.QueryChannelInfo {
	m.channelMu.Lock()
	defer m.channelMu.Unlock()

	var channelInfo *querypb.QueryChannelInfo
	if info, ok := m.queryChannelInfos[collectionID]; ok {
		channelInfo = proto.Clone(info).(*querypb.QueryChannelInfo)
	} else {
		channelInfo = m.createQueryChannel(collectionID)
		m.queryChannelInfos[collectionID] = channelInfo
	}

	return proto.Clone(channelInfo).(*querypb.QueryChannelInfo)
}

func (m *MetaReplica) setLoadType(collectionID UniqueID, loadType querypb.LoadType) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()

	if _, ok := m.collectionInfos[collectionID]; ok {
		info := proto.Clone(m.collectionInfos[collectionID]).(*querypb.CollectionInfo)
		info.LoadType = loadType
		err := saveGlobalCollectionInfo(collectionID, info, m.getKvClient())
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}

		m.collectionInfos[collectionID] = info
		return nil
	}

	return errors.New("setLoadType: can't find collection in collectionInfos")
}

func (m *MetaReplica) setLoadPercentage(collectionID UniqueID, partitionID UniqueID, percentage int64, loadType querypb.LoadType) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()

	if _, ok := m.collectionInfos[collectionID]; !ok {
		return errors.New("setLoadPercentage: can't find collection in collectionInfos")
	}

	info := proto.Clone(m.collectionInfos[collectionID]).(*querypb.CollectionInfo)
	if loadType == querypb.LoadType_LoadCollection {
		info.InMemoryPercentage = percentage
		for _, partitionState := range info.PartitionStates {
			if percentage >= 100 {
				partitionState.State = querypb.PartitionState_InMemory
			} else {
				partitionState.State = querypb.PartitionState_PartialInMemory
			}
			partitionState.InMemoryPercentage = percentage
		}
		err := saveGlobalCollectionInfo(collectionID, info, m.getKvClient())
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
	} else {
		findPartition := false
		info.InMemoryPercentage = 0
		for _, partitionState := range info.PartitionStates {
			if partitionState.PartitionID == partitionID {
				findPartition = true
				if percentage >= 100 {
					partitionState.State = querypb.PartitionState_InMemory
				} else {
					partitionState.State = querypb.PartitionState_PartialInMemory
				}
				partitionState.InMemoryPercentage = percentage
			}
			info.InMemoryPercentage += partitionState.InMemoryPercentage
		}
		if !findPartition {
			return errors.New("setLoadPercentage: can't find partitionID in collectionInfos")
		}

		info.InMemoryPercentage /= int64(len(info.PartitionIDs))
		err := saveGlobalCollectionInfo(collectionID, info, m.getKvClient())
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
	}

	m.collectionInfos[collectionID] = info
	return nil
}

func (m *MetaReplica) getWatchedChannelsByNodeID(nodeID int64) *querypb.UnsubscribeChannelInfo {
	// 1. find all the search/dmChannel/deltaChannel the node has watched
	colID2DmChannels := make(map[UniqueID][]string)
	colID2DeltaChannels := make(map[UniqueID][]string)
	// TODO remove colID2QueryChannel since it's not used
	colID2QueryChannel := make(map[UniqueID]string)
	dmChannelInfos := m.getDmChannelInfosByNodeID(nodeID)
	// get dmChannel/search channel the node has watched
	for _, channelInfo := range dmChannelInfos {
		collectionID := channelInfo.CollectionID
		dmChannel := funcutil.ToPhysicalChannel(channelInfo.DmChannel)
		if _, ok := colID2DmChannels[collectionID]; !ok {
			colID2DmChannels[collectionID] = []string{}
		}
		colID2DmChannels[collectionID] = append(colID2DmChannels[collectionID], dmChannel)
		if _, ok := colID2QueryChannel[collectionID]; !ok {
			queryChannelInfo := m.getQueryChannelInfoByID(collectionID)
			colID2QueryChannel[collectionID] = queryChannelInfo.QueryChannel
		}
	}
	segmentInfos := m.getSegmentInfosByNode(nodeID)
	colIDs := make(map[UniqueID]bool)
	// iterate through segments to find unique collection ids
	for _, segmentInfo := range segmentInfos {
		colIDs[segmentInfo.CollectionID] = true
	}
	// get delta/search channel the node has watched
	for collectionID := range colIDs {
		if _, ok := colID2DeltaChannels[collectionID]; !ok {
			deltaChanelInfos, err := m.getDeltaChannelsByCollectionID(collectionID)
			if err != nil {
				// all nodes succeeded in releasing the Data, but queryCoord hasn't cleaned up the meta in time, and a Node went down
				// and meta was cleaned after m.getSegmentInfosByNode(nodeID)
				continue
			}
			deltaChannels := make([]string, len(deltaChanelInfos))
			for offset, channelInfo := range deltaChanelInfos {
				deltaChannels[offset] = funcutil.ToPhysicalChannel(channelInfo.ChannelName)
			}
			colID2DeltaChannels[collectionID] = deltaChannels
		}
		if _, ok := colID2QueryChannel[collectionID]; !ok {
			queryChannelInfo := m.getQueryChannelInfoByID(collectionID)
			colID2QueryChannel[collectionID] = queryChannelInfo.QueryChannel
		}
	}

	// creating unsubscribeChannelInfo, which will be written to etcd
	colID2Channels := make(map[UniqueID][]string)
	for collectionID, channels := range colID2DmChannels {
		colID2Channels[collectionID] = append(colID2Channels[collectionID], channels...)
	}
	for collectionID, channels := range colID2DeltaChannels {
		colID2Channels[collectionID] = append(colID2Channels[collectionID], channels...)
	}
	for collectionID, channel := range colID2QueryChannel {
		colID2Channels[collectionID] = append(colID2Channels[collectionID], channel)
	}

	unsubscribeChannelInfo := &querypb.UnsubscribeChannelInfo{
		NodeID: nodeID,
	}

	for collectionID, channels := range colID2Channels {
		unsubscribeChannelInfo.CollectionChannels = append(unsubscribeChannelInfo.CollectionChannels,
			&querypb.UnsubscribeChannels{
				CollectionID: collectionID,
				Channels:     channels,
			})
	}

	return unsubscribeChannelInfo
}

func (m *MetaReplica) generateReplica(collectionID int64, partitionIds []int64) (*milvuspb.ReplicaInfo, error) {
	id, err := m.idAllocator()
	if err != nil {
		return nil, err
	}

	return &milvuspb.ReplicaInfo{
		ReplicaID:     id,
		CollectionID:  collectionID,
		PartitionIds:  partitionIds,
		ShardReplicas: make([]*milvuspb.ShardReplica, 0),
		NodeIds:       make([]int64, 0),
	}, nil
}

func (m *MetaReplica) addReplica(replica *milvuspb.ReplicaInfo) error {
	collectionInfo, err := m.getCollectionInfoByID(replica.CollectionID)
	if err != nil {
		return err
	}

	collectionInfo.ReplicaIds = append(collectionInfo.ReplicaIds, replica.ReplicaID)
	collectionInfo.ReplicaNumber++

	err = saveGlobalCollectionInfo(collectionInfo.CollectionID, collectionInfo, m.getKvClient())
	if err != nil {
		return err
	}

	m.collectionMu.Lock()
	m.collectionInfos[collectionInfo.CollectionID] = collectionInfo
	m.collectionMu.Unlock()

	err = saveReplicaInfo(replica, m.getKvClient())
	if err != nil {
		return err
	}

	m.replicas.Insert(replica)
	return nil
}

func (m *MetaReplica) setReplicaInfo(info *milvuspb.ReplicaInfo) error {
	err := saveReplicaInfo(info, m.getKvClient())
	if err != nil {
		return err
	}

	m.replicas.Insert(info)
	return nil
}

func (m *MetaReplica) getReplicaByID(replicaID int64) (*milvuspb.ReplicaInfo, error) {
	replica, ok := m.replicas.Get(replicaID)
	if !ok {
		return nil, errors.New("replica not found")
	}

	return replica, nil
}

func (m *MetaReplica) getReplicasByNodeID(nodeID int64) ([]*milvuspb.ReplicaInfo, error) {
	replicas := m.replicas.GetReplicasByNodeID(nodeID)
	return replicas, nil
}

func (m *MetaReplica) getReplicasByCollectionID(collectionID int64) ([]*milvuspb.ReplicaInfo, error) {
	collection, err := m.getCollectionInfoByID(collectionID)
	if err != nil {
		return nil, err
	}

	replicas := make([]*milvuspb.ReplicaInfo, 0, len(collection.ReplicaIds))
	for _, replicaID := range collection.ReplicaIds {
		replica, err := m.getReplicaByID(replicaID)
		if err != nil {
			return nil, err
		}

		replicas = append(replicas, replica)
	}

	rand.Shuffle(len(replicas), func(i, j int) {
		replicas[i], replicas[j] = replicas[j], replicas[i]
	})

	return replicas, nil
}

// applyReplicaBalancePlan applies replica balance plan to replica info.
func (m *MetaReplica) applyReplicaBalancePlan(p *balancePlan) error {
	return m.replicas.ApplyBalancePlan(p, m.getKvClient())
}

func (m *MetaReplica) updateShardLeader(replicaID UniqueID, dmChannel string, leaderID UniqueID, leaderAddr string) error {
	return m.replicas.UpdateShardLeader(replicaID, dmChannel, leaderID, leaderAddr, m.getKvClient())
}

//func (m *MetaReplica) printMeta() {
//	m.RLock()
//	defer m.RUnlock()
//	for id, info := range m.collectionInfos {
//		log.Debug("query coordinator MetaReplica: collectionInfo", zap.Int64("collectionID", id), zap.Any("info", info))
//	}
//
//	for id, info := range m.segmentInfos {
//		log.Debug("query coordinator MetaReplica: segmentInfo", zap.Int64("segmentID", id), zap.Any("info", info))
//	}
//
//	for id, info := range m.queryChannelInfos {
//		log.Debug("query coordinator MetaReplica: queryChannelInfo", zap.Int64("collectionID", id), zap.Any("info", info))
//	}
//}

func saveGlobalCollectionInfo(collectionID UniqueID, info *querypb.CollectionInfo, kv kv.MetaKv) error {
	infoBytes, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%d", collectionMetaPrefix, collectionID)
	return kv.Save(key, string(infoBytes))
}

func saveDeltaChannelInfo(collectionID UniqueID, infos []*datapb.VchannelInfo, kv kv.MetaKv) error {
	kvs := make(map[string]string)
	for _, info := range infos {
		infoBytes, err := proto.Marshal(info)
		if err != nil {
			return err
		}

		key := fmt.Sprintf("%s/%d/%s", deltaChannelMetaPrefix, collectionID, info.ChannelName)
		kvs[key] = string(infoBytes)
	}
	return kv.MultiSave(kvs)
}

func saveDmChannelWatchInfos(infos []*querypb.DmChannelWatchInfo, kv kv.MetaKv) error {
	kvs := make(map[string]string)
	for _, info := range infos {
		infoBytes, err := proto.Marshal(info)
		if err != nil {
			return err
		}

		key := fmt.Sprintf("%s/%d/%s", dmChannelMetaPrefix, info.CollectionID, info.DmChannel)
		kvs[key] = string(infoBytes)
	}
	return kv.MultiSave(kvs)
}

func saveReplicaInfo(info *milvuspb.ReplicaInfo, kv kv.MetaKv) error {
	infoBytes, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%d", ReplicaMetaPrefix, info.ReplicaID)
	return kv.Save(key, string(infoBytes))
}

func removeCollectionMeta(collectionID UniqueID, replicas []UniqueID, kv kv.MetaKv) error {
	var prefixes []string
	collectionInfosPrefix := fmt.Sprintf("%s/%d", collectionMetaPrefix, collectionID)
	prefixes = append(prefixes, collectionInfosPrefix)

	dmChannelInfosPrefix := fmt.Sprintf("%s/%d", dmChannelMetaPrefix, collectionID)
	prefixes = append(prefixes, dmChannelInfosPrefix)

	deltaChannelInfosPrefix := fmt.Sprintf("%s/%d", deltaChannelMetaPrefix, collectionID)
	prefixes = append(prefixes, deltaChannelInfosPrefix)

	for _, replicaID := range replicas {
		replicaPrefix := fmt.Sprintf("%s/%d", ReplicaMetaPrefix, replicaID)
		prefixes = append(prefixes, replicaPrefix)
	}

	return kv.MultiRemoveWithPrefix(prefixes)
}

func getShardNodes(collectionID UniqueID, meta Meta) map[string]map[UniqueID]struct{} {
	shardNodes := make(map[string]map[UniqueID]struct{})
	segments := meta.showSegmentInfos(collectionID, nil)
	for _, segment := range segments {
		nodes, ok := shardNodes[segment.DmChannel]
		if !ok {
			nodes = make(map[UniqueID]struct{})
		}

		for _, nodeID := range segment.NodeIds {
			nodes[nodeID] = struct{}{}
		}

		shardNodes[segment.DmChannel] = nodes
	}

	return shardNodes
}
