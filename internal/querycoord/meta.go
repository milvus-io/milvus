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
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/mqclient"
)

const (
	collectionMetaPrefix          = "queryCoord-collectionMeta"
	queryChannelMetaPrefix        = "queryCoord-queryChannel"
	deltaChannelMetaPrefix        = "queryCoord-deltaChannel"
	globalQuerySeekPositionPrefix = "queryCoord-globalQuerySeekPosition"
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
	addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error
	releaseCollection(collectionID UniqueID) error

	addPartition(collectionID UniqueID, partitionID UniqueID) error
	showPartitions(collectionID UniqueID) ([]*querypb.PartitionStates, error)
	hasPartition(collectionID UniqueID, partitionID UniqueID) bool
	hasReleasePartition(collectionID UniqueID, partitionID UniqueID) bool
	releasePartition(collectionID UniqueID, partitionID UniqueID) error

	deleteSegmentInfoByNodeID(nodeID UniqueID) error
	setSegmentInfos(segmentInfos map[UniqueID]*querypb.SegmentInfo) error
	showSegmentInfos(collectionID UniqueID, partitionIDs []UniqueID) []*querypb.SegmentInfo
	getSegmentInfoByID(segmentID UniqueID) (*querypb.SegmentInfo, error)
	getSegmentInfosByNode(nodeID int64) []*querypb.SegmentInfo

	getPartitionStatesByID(collectionID UniqueID, partitionID UniqueID) (*querypb.PartitionStates, error)

	getDmChannelWatchInfosByNodeID(nodeID int64) []*querypb.DmChannelWatchInfo
	addDmChannel(nodeID int64, info *querypb.DmChannelWatchInfo) error
	removeDmChannel(nodeID int64, info *querypb.DmChannelWatchInfo) error
	removeDmChannelWatchInfosByNodeID(nodeID int64) error

	getDeltaChannelsByCollectionID(collectionID UniqueID) ([]*datapb.VchannelInfo, error)
	setDeltaChannel(collectionID UniqueID, info []*datapb.VchannelInfo) error

	getQueryChannelInfoByID(collectionID UniqueID) (*querypb.QueryChannelInfo, error)
	getQueryStreamByID(collectionID UniqueID) (msgstream.MsgStream, error)

	setLoadType(collectionID UniqueID, loadType querypb.LoadType) error
	getLoadType(collectionID UniqueID) (querypb.LoadType, error)
	setLoadPercentage(collectionID UniqueID, partitionID UniqueID, percentage int64, loadType querypb.LoadType) error
	//printMeta()
	saveGlobalSealedSegInfos(saves col2SegmentInfos) (col2SealedSegmentChangeInfos, error)
	removeGlobalSealedSegInfos(collectionID UniqueID, partitionIDs []UniqueID) (col2SealedSegmentChangeInfos, error)
	sendSealedSegmentChangeInfos(collectionID UniqueID, changeInfos *querypb.SealedSegmentsChangeInfo) (*querypb.QueryChannelInfo, map[string][]mqclient.MessageID, error)
}

// MetaReplica records the current load information on all querynodes
type MetaReplica struct {
	ctx         context.Context
	cancel      context.CancelFunc
	client      kv.MetaKv // client of a reliable kv service, i.e. etcd client
	msFactory   msgstream.Factory
	idAllocator func() (UniqueID, error)

	//sync.RWMutex
	collectionInfos   map[UniqueID]*querypb.CollectionInfo
	collectionMu      sync.RWMutex
	segmentInfos      map[UniqueID]*querypb.SegmentInfo
	segmentMu         sync.RWMutex
	queryChannelInfos map[UniqueID]*querypb.QueryChannelInfo
	channelMu         sync.RWMutex
	deltaChannelInfos map[UniqueID][]*datapb.VchannelInfo
	deltaChannelMu    sync.RWMutex
	queryStreams      map[UniqueID]msgstream.MsgStream
	streamMu          sync.RWMutex

	globalSeekPosition *internalpb.MsgPosition
	//partitionStates map[UniqueID]*querypb.PartitionStates
}

func newMeta(ctx context.Context, kv kv.MetaKv, factory msgstream.Factory, idAllocator func() (UniqueID, error)) (Meta, error) {
	childCtx, cancel := context.WithCancel(ctx)
	collectionInfos := make(map[UniqueID]*querypb.CollectionInfo)
	segmentInfos := make(map[UniqueID]*querypb.SegmentInfo)
	queryChannelInfos := make(map[UniqueID]*querypb.QueryChannelInfo)
	deltaChannelInfos := make(map[UniqueID][]*datapb.VchannelInfo)
	queryMsgStream := make(map[UniqueID]msgstream.MsgStream)
	position := &internalpb.MsgPosition{}

	m := &MetaReplica{
		ctx:         childCtx,
		cancel:      cancel,
		client:      kv,
		msFactory:   factory,
		idAllocator: idAllocator,

		collectionInfos:    collectionInfos,
		segmentInfos:       segmentInfos,
		queryChannelInfos:  queryChannelInfos,
		deltaChannelInfos:  deltaChannelInfos,
		queryStreams:       queryMsgStream,
		globalSeekPosition: position,
	}

	err := m.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *MetaReplica) reloadFromKV() error {
	log.Debug("start reload from kv")
	collectionKeys, collectionValues, err := m.client.LoadWithPrefix(collectionMetaPrefix)
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
		collectionInfo.ChannelInfos = upgradeDmChannelInfos(collectionID, collectionInfo.ChannelInfos)
		m.collectionInfos[collectionID] = collectionInfo
	}

	segmentKeys, segmentValues, err := m.client.LoadWithPrefix(util.SegmentMetaPrefix)
	if err != nil {
		return err
	}
	for index := range segmentKeys {
		segmentID, err := strconv.ParseInt(filepath.Base(segmentKeys[index]), 10, 64)
		if err != nil {
			return err
		}
		segmentInfo := &querypb.SegmentInfo{}
		err = proto.Unmarshal([]byte(segmentValues[index]), segmentInfo)
		if err != nil {
			return err
		}
		m.segmentInfos[segmentID] = segmentInfo
	}

	queryChannelKeys, queryChannelValues, err := m.client.LoadWithPrefix(queryChannelMetaPrefix)
	if err != nil {
		return nil
	}
	for index := range queryChannelKeys {
		collectionID, err := strconv.ParseInt(filepath.Base(queryChannelKeys[index]), 10, 64)
		if err != nil {
			return err
		}
		queryChannelInfo := &querypb.QueryChannelInfo{}
		err = proto.Unmarshal([]byte(queryChannelValues[index]), queryChannelInfo)
		if err != nil {
			return err
		}
		m.queryChannelInfos[collectionID] = queryChannelInfo
	}

	deltaChannelKeys, deltaChannelValues, err := m.client.LoadWithPrefix(deltaChannelMetaPrefix)
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

	globalSeekPosValue, err := m.client.Load(globalQuerySeekPositionPrefix)
	if err == nil {
		position := &internalpb.MsgPosition{}
		err = proto.Unmarshal([]byte(globalSeekPosValue), position)
		if err != nil {
			return err
		}
		m.globalSeekPosition = position
	}
	//TODO::update partition states
	log.Debug("reload from kv finished")

	return nil
}

func (m *MetaReplica) setKvClient(kv kv.MetaKv) {
	m.client = kv
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

func (m *MetaReplica) addCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) error {
	hasCollection := m.hasCollection(collectionID)
	if !hasCollection {
		partitions := make([]UniqueID, 0)
		partitionStates := make([]*querypb.PartitionStates, 0)
		channels := make([]*querypb.DmChannelInfo, 0)
		newCollection := &querypb.CollectionInfo{
			CollectionID:    collectionID,
			PartitionIDs:    partitions,
			PartitionStates: partitionStates,
			ChannelInfos:    channels,
			Schema:          schema,
		}
		err := saveGlobalCollectionInfo(collectionID, newCollection, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		m.collectionMu.Lock()
		m.collectionInfos[collectionID] = newCollection
		m.collectionMu.Unlock()
	}

	return nil
}

func (m *MetaReplica) addPartition(collectionID UniqueID, partitionID UniqueID) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		col := proto.Clone(info).(*querypb.CollectionInfo)
		log.Debug("add a  partition to MetaReplica...", zap.Int64s("partitionIDs", col.PartitionIDs))
		for _, id := range col.PartitionIDs {
			if id == partitionID {
				return nil
			}
		}
		col.PartitionIDs = append(col.PartitionIDs, partitionID)
		releasedPartitionIDs := make([]UniqueID, 0)
		for _, id := range col.ReleasedPartitionIDs {
			if id != partitionID {
				releasedPartitionIDs = append(releasedPartitionIDs, id)
			}
		}
		col.ReleasedPartitionIDs = releasedPartitionIDs
		col.PartitionStates = append(col.PartitionStates, &querypb.PartitionStates{
			PartitionID: partitionID,
			State:       querypb.PartitionState_NotPresent,
		})

		log.Debug("add a  partition to MetaReplica", zap.Int64s("partitionIDs", col.PartitionIDs))
		err := saveGlobalCollectionInfo(collectionID, col, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		m.collectionInfos[collectionID] = col
		return nil
	}
	return errors.New("addPartition: can't find collection when add partition")
}

func (m *MetaReplica) deleteSegmentInfoByNodeID(nodeID UniqueID) error {
	m.segmentMu.Lock()
	defer m.segmentMu.Unlock()

	segmentIDsToRemove := make([]UniqueID, 0)
	for segmentID, info := range m.segmentInfos {
		if info.NodeID == nodeID {
			segmentIDsToRemove = append(segmentIDsToRemove, segmentID)
		}
	}

	err := multiRemoveSegmentInfo(segmentIDsToRemove, m.client)
	if err != nil {
		log.Error("remove segmentInfo from etcd error", zap.Any("error", err.Error()), zap.Int64s("segmentIDs", segmentIDsToRemove))
		return err
	}
	for _, segmentID := range segmentIDsToRemove {
		delete(m.segmentInfos, segmentID)
	}

	return nil
}

func (m *MetaReplica) setSegmentInfos(segmentInfos map[UniqueID]*querypb.SegmentInfo) error {
	m.segmentMu.Lock()
	defer m.segmentMu.Unlock()

	err := multiSaveSegmentInfos(segmentInfos, m.client)
	if err != nil {
		log.Error("save segmentInfos error", zap.Any("segmentInfos", segmentInfos), zap.Error(err))
		return err
	}

	for segmentID, info := range segmentInfos {
		m.segmentInfos[segmentID] = info
	}

	return nil
}

func (m *MetaReplica) saveGlobalSealedSegInfos(saves col2SegmentInfos) (col2SealedSegmentChangeInfos, error) {
	if len(saves) == 0 {
		return nil, nil
	}
	// generate segment change info according segment info to updated
	col2SegmentChangeInfos := make(col2SealedSegmentChangeInfos)

	segmentsCompactionFrom := make([]UniqueID, 0)
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
				if offlineInfo.SegmentState == querypb.SegmentState_sealed {
					changeInfo.OfflineNodeID = offlineNodeID
					changeInfo.OfflineSegments = []*querypb.SegmentInfo{offlineInfo}
				}
			}
			segmentsChangeInfo.Infos = append(segmentsChangeInfo.Infos, changeInfo)

			// generate offline segment change info if the loaded segment is compacted from other sealed segments
			for _, compactionSegmentID := range info.CompactionFrom {
				compactionSegmentInfo, err := m.getSegmentInfoByID(compactionSegmentID)
				if err == nil && compactionSegmentInfo.SegmentState == querypb.SegmentState_sealed {
					segmentsChangeInfo.Infos = append(segmentsChangeInfo.Infos, &querypb.SegmentChangeInfo{
						OfflineNodeID:   compactionSegmentInfo.NodeID,
						OfflineSegments: []*querypb.SegmentInfo{compactionSegmentInfo},
					})
					segmentsCompactionFrom = append(segmentsCompactionFrom, compactionSegmentID)
				} else {
					return nil, fmt.Errorf("saveGlobalSealedSegInfos: the compacted segment %d has not been loaded into memory", compactionSegmentID)
				}
			}
		}
		col2SegmentChangeInfos[collectionID] = segmentsChangeInfo
	}

	queryChannelInfosMap := make(map[UniqueID]*querypb.QueryChannelInfo)
	var globalSeekPositionTmp *internalpb.MsgPosition
	for collectionID, segmentChangeInfos := range col2SegmentChangeInfos {
		// get msgStream to produce sealedSegmentChangeInfos to query channel
		queryChannelInfo, messageIDInfos, err := m.sendSealedSegmentChangeInfos(collectionID, segmentChangeInfos)
		if err != nil {
			return nil, err
		}
		// len(messageIDs) == 1
		messageIDs, ok := messageIDInfos[queryChannelInfo.QueryChannelID]
		if !ok || len(messageIDs) == 0 {
			return col2SegmentChangeInfos, errors.New("updateGlobalSealedSegmentInfos: send sealed segment change info failed")
		}

		if queryChannelInfo.SeekPosition == nil {
			queryChannelInfo.SeekPosition = &internalpb.MsgPosition{
				ChannelName: queryChannelInfo.QueryChannelID,
			}
		}

		queryChannelInfo.SeekPosition.MsgID = messageIDs[0].Serialize()

		// update segmentInfo, queryChannelInfo meta to cache and etcd
		seg2Info := make(map[UniqueID]*querypb.SegmentInfo)
		for _, segmentInfo := range queryChannelInfo.GlobalSealedSegments {
			segmentID := segmentInfo.SegmentID
			seg2Info[segmentID] = segmentInfo
		}
		if infos, ok := saves[collectionID]; ok {
			for _, segmentInfo := range infos {
				segmentID := segmentInfo.SegmentID
				seg2Info[segmentID] = segmentInfo
			}
		}

		globalSealedSegmentInfos := make([]*querypb.SegmentInfo, 0)
		for _, info := range seg2Info {
			globalSealedSegmentInfos = append(globalSealedSegmentInfos, info)
		}
		queryChannelInfo.GlobalSealedSegments = globalSealedSegmentInfos
		queryChannelInfosMap[collectionID] = queryChannelInfo
		globalSeekPositionTmp = queryChannelInfo.SeekPosition
	}

	// save segmentInfo to etcd
	segmentInfoKvs := make(map[string]string)
	for _, infos := range saves {
		for _, info := range infos {
			segmentInfoBytes, err := proto.Marshal(info)
			if err != nil {
				return col2SegmentChangeInfos, err
			}
			segmentKey := fmt.Sprintf("%s/%d", util.SegmentMetaPrefix, info.SegmentID)
			segmentInfoKvs[segmentKey] = string(segmentInfoBytes)
		}
	}
	for key, value := range segmentInfoKvs {
		err := m.client.Save(key, value)
		if err != nil {
			panic(err)
		}
	}

	// remove compacted segment info from etcd
	for _, segmentID := range segmentsCompactionFrom {
		segmentKey := fmt.Sprintf("%s/%d", util.SegmentMetaPrefix, segmentID)
		err := m.client.Remove(segmentKey)
		if err != nil {
			panic(err)
		}
	}

	// save queryChannelInfo and sealedSegmentsChangeInfo to etcd
	saveKvs := make(map[string]string)
	for collectionID, queryChannelInfo := range queryChannelInfosMap {
		channelInfoBytes, err := proto.Marshal(queryChannelInfo)
		if err != nil {
			return col2SegmentChangeInfos, err
		}
		channelKey := fmt.Sprintf("%s/%d", queryChannelMetaPrefix, collectionID)
		saveKvs[channelKey] = string(channelInfoBytes)
	}
	seekPos, err := proto.Marshal(globalSeekPositionTmp)
	if err != nil {
		return col2SegmentChangeInfos, err
	}
	saveKvs[globalQuerySeekPositionPrefix] = string(seekPos)

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

	err = m.client.MultiSave(saveKvs)
	if err != nil {
		panic(err)
	}

	m.segmentMu.Lock()
	for _, segmentInfos := range saves {
		for _, info := range segmentInfos {
			segmentID := info.SegmentID
			m.segmentInfos[segmentID] = info
		}
	}
	for _, segmentID := range segmentsCompactionFrom {
		delete(m.segmentInfos, segmentID)
	}
	m.segmentMu.Unlock()

	m.channelMu.Lock()
	for collectionID, channelInfo := range queryChannelInfosMap {
		m.queryChannelInfos[collectionID] = channelInfo
	}
	m.globalSeekPosition = globalSeekPositionTmp
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

	// get msgStream to produce sealedSegmentChangeInfos to query channel
	queryChannelInfo, messageIDInfos, err := m.sendSealedSegmentChangeInfos(collectionID, segmentChangeInfos)
	if err != nil {
		return nil, err
	}
	// len(messageIDs) = 1
	messageIDs, ok := messageIDInfos[queryChannelInfo.QueryChannelID]
	if !ok || len(messageIDs) == 0 {
		return col2SealedSegmentChangeInfos{collectionID: segmentChangeInfos}, errors.New("updateGlobalSealedSegmentInfos: send sealed segment change info failed")
	}

	if queryChannelInfo.SeekPosition == nil {
		queryChannelInfo.SeekPosition = &internalpb.MsgPosition{
			ChannelName: queryChannelInfo.QueryChannelID,
		}
	}
	queryChannelInfo.SeekPosition.MsgID = messageIDs[0].Serialize()

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
		segmentKey := fmt.Sprintf("%s/%d", util.SegmentMetaPrefix, info.SegmentID)
		err = m.client.Remove(segmentKey)
		if err != nil {
			panic(err)
		}
	}

	// save meta to etcd
	saveKvs := make(map[string]string)
	channelInfoBytes, err := proto.Marshal(queryChannelInfo)
	if err != nil {
		return col2SealedSegmentChangeInfos{collectionID: segmentChangeInfos}, err
	}
	channelKey := fmt.Sprintf("%s/%d", queryChannelMetaPrefix, collectionID)
	saveKvs[channelKey] = string(channelInfoBytes)
	seekPos, err := proto.Marshal(queryChannelInfo.SeekPosition)
	if err != nil {
		return col2SealedSegmentChangeInfos{collectionID: segmentChangeInfos}, err
	}
	saveKvs[globalQuerySeekPositionPrefix] = string(seekPos)

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

	err = m.client.MultiSave(saveKvs)
	if err != nil {
		panic(err)
	}

	m.segmentMu.Lock()
	for _, info := range removes {
		delete(m.segmentInfos, info.SegmentID)
	}
	m.segmentMu.Unlock()

	m.channelMu.Lock()
	m.queryChannelInfos[collectionID] = queryChannelInfo
	m.globalSeekPosition = queryChannelInfo.SeekPosition
	m.channelMu.Unlock()

	return col2SealedSegmentChangeInfos{collectionID: segmentChangeInfos}, nil
}

// send sealed segment change infos into query channels
func (m *MetaReplica) sendSealedSegmentChangeInfos(collectionID UniqueID, changeInfos *querypb.SealedSegmentsChangeInfo) (*querypb.QueryChannelInfo, map[string][]mqclient.MessageID, error) {
	// get msgStream to produce sealedSegmentChangeInfos to query channel
	queryChannelInfo, err := m.getQueryChannelInfoByID(collectionID)
	if err != nil {
		log.Error("updateGlobalSealedSegmentInfos: get query channel info failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, nil, err
	}

	queryStream, err := m.getQueryStreamByID(collectionID)
	if err != nil {
		log.Error("updateGlobalSealedSegmentInfos: get query stream failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, nil, err
	}

	var msgPack = &msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{},
	}
	id, err := m.idAllocator()
	if err != nil {
		log.Error("allocator trigger taskID failed", zap.Error(err))
		return nil, nil, err
	}
	changeInfos.Base.MsgID = id
	segmentChangeMsg := &msgstream.SealedSegmentsChangeInfoMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SealedSegmentsChangeInfo: *changeInfos,
	}
	msgPack.Msgs = append(msgPack.Msgs, segmentChangeMsg)

	messageIDInfos, err := queryStream.ProduceMark(msgPack)
	if err != nil {
		log.Error("updateGlobalSealedSegmentInfos: send sealed segment change info failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, nil, err
	}
	log.Debug("updateGlobalSealedSegmentInfos: send sealed segment change info to queryChannel", zap.Any("msgPack", msgPack))

	return queryChannelInfo, messageIDInfos, nil
}

func (m *MetaReplica) showSegmentInfos(collectionID UniqueID, partitionIDs []UniqueID) []*querypb.SegmentInfo {
	m.segmentMu.RLock()
	defer m.segmentMu.RUnlock()

	results := make([]*querypb.SegmentInfo, 0)
	segmentInfos := make([]*querypb.SegmentInfo, 0)
	for _, info := range m.segmentInfos {
		if info.CollectionID == collectionID {
			segmentInfos = append(segmentInfos, proto.Clone(info).(*querypb.SegmentInfo))
		}
	}
	if len(partitionIDs) == 0 {
		return segmentInfos
	}
	for _, info := range segmentInfos {
		for _, partitionID := range partitionIDs {
			if info.PartitionID == partitionID {
				results = append(results, info)
			}
		}
	}
	return results
}

func (m *MetaReplica) getSegmentInfoByID(segmentID UniqueID) (*querypb.SegmentInfo, error) {
	m.segmentMu.RLock()
	defer m.segmentMu.RUnlock()

	if info, ok := m.segmentInfos[segmentID]; ok {
		return proto.Clone(info).(*querypb.SegmentInfo), nil
	}

	return nil, errors.New("getSegmentInfoByID: can't find segmentID in segmentInfos")
}
func (m *MetaReplica) getSegmentInfosByNode(nodeID int64) []*querypb.SegmentInfo {
	m.segmentMu.RLock()
	defer m.segmentMu.RUnlock()

	segmentInfos := make([]*querypb.SegmentInfo, 0)
	for _, info := range m.segmentInfos {
		if info.NodeID == nodeID {
			segmentInfos = append(segmentInfos, proto.Clone(info).(*querypb.SegmentInfo))
		}
	}

	return segmentInfos
}

func (m *MetaReplica) getCollectionInfoByID(collectionID UniqueID) (*querypb.CollectionInfo, error) {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	return m.getCollectionInfoByIDPrivate(collectionID)
}

func (m *MetaReplica) getCollectionInfoByIDPrivate(collectionID UniqueID) (*querypb.CollectionInfo, error) {
	if info, ok := m.collectionInfos[collectionID]; ok {
		return proto.Clone(info).(*querypb.CollectionInfo), nil
	}

	return nil, errors.New("getCollectionInfoByID: can't find collectionID in collectionInfo")
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

func (m *MetaReplica) releaseCollection(collectionID UniqueID) error {
	err := removeGlobalCollectionInfo(collectionID, m.client)
	if err != nil {
		log.Warn("remove collectionInfo from etcd failed", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
		return err
	}

	m.collectionMu.Lock()
	delete(m.collectionInfos, collectionID)
	m.collectionMu.Unlock()

	return nil
}

func (m *MetaReplica) releasePartition(collectionID UniqueID, partitionID UniqueID) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()
	info, err := m.getCollectionInfoByIDPrivate(collectionID)
	if err == nil {
		newPartitionIDs := make([]UniqueID, 0)
		newPartitionStates := make([]*querypb.PartitionStates, 0)
		for offset, id := range info.PartitionIDs {
			if id != partitionID {
				newPartitionIDs = append(newPartitionIDs, id)
				newPartitionStates = append(newPartitionStates, info.PartitionStates[offset])
			}
		}
		info.PartitionIDs = newPartitionIDs
		info.PartitionStates = newPartitionStates

		releasedPartitionIDs := make([]UniqueID, 0)
		for _, id := range info.ReleasedPartitionIDs {
			if id != partitionID {
				releasedPartitionIDs = append(releasedPartitionIDs, id)
			}
		}
		releasedPartitionIDs = append(releasedPartitionIDs, partitionID)
		info.ReleasedPartitionIDs = releasedPartitionIDs

		// If user loaded a collectionA, and release a partitionB which belongs to collectionA,
		// and then load collectionA again, if we don't set the inMemoryPercentage to 0 when releasing
		// partitionB, the second loading of collectionA would directly return because
		// the inMemoryPercentage in ShowCollection response is still the old value -- 100.
		// So if releasing partition, inMemoryPercentage should be set to 0.
		info.InMemoryPercentage = 0

		// delete watchPartitionInfo by partitionID
		removeDmChannelByPartitionID(info, partitionID)

		err = saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("releasePartition: save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID))
			return err
		}

		m.collectionInfos[collectionID] = info
		return nil
	}

	return err
}

func (m *MetaReplica) getDmChannelWatchInfosByNodeID(nodeID int64) []*querypb.DmChannelWatchInfo {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	var result []*querypb.DmChannelWatchInfo
	for _, collectionInfo := range m.collectionInfos {
		dmChannelInfos := collectionInfo.ChannelInfos
		for _, channelInfo := range dmChannelInfos {
			if channelInfo.NodeIDLoaded == nodeID {
				result = append(result, channelInfo.WatchInfos...)
			}
		}
	}

	return result
}

func (m *MetaReplica) removeDmChannelWatchInfosByNodeID(nodeID int64) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()

	for collectionID, info := range m.collectionInfos {
		collectionInfo := proto.Clone(info).(*querypb.CollectionInfo)
		var newDmChannelInfos []*querypb.DmChannelInfo
		needUpdate := false
		dmChannelInfos := collectionInfo.ChannelInfos
		for _, channelInfo := range dmChannelInfos {
			if channelInfo.NodeIDLoaded != nodeID {
				needUpdate = true
				newDmChannelInfos = append(newDmChannelInfos, channelInfo)
			}
		}
		collectionInfo.ChannelInfos = newDmChannelInfos

		if needUpdate {
			err := saveGlobalCollectionInfo(collectionID, collectionInfo, m.client)
			if err != nil {
				log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
				return err
			}
			m.collectionInfos[collectionID] = collectionInfo
		}
	}

	return nil
}

func (m *MetaReplica) addDmChannel(nodeID int64, watchInfo *querypb.DmChannelWatchInfo) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()
	//before add channel, should ensure toAddedChannels not in MetaReplica
	collectionID := watchInfo.CollectionID
	info, err := m.getCollectionInfoByIDPrivate(collectionID)
	if err == nil {
		addDmChannel(nodeID, info, watchInfo)
		err = saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		m.collectionInfos[collectionID] = info
		return nil
	}

	return errors.New("addDmChannels: can't find collection in collectionInfos")
}

func (m *MetaReplica) removeDmChannel(nodeID int64, watchInfo *querypb.DmChannelWatchInfo) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()
	collectionID := watchInfo.CollectionID
	info, err := m.getCollectionInfoByIDPrivate(collectionID)
	if err == nil {
		removeDmChannel(nodeID, info, watchInfo)
		err = saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}

		m.collectionInfos[collectionID] = info
		return nil
	}

	return errors.New("removeDmChannel: can't find collection in collectionInfos")
}

func createQueryChannel(collectionID UniqueID) *querypb.QueryChannelInfo {
	searchPrefix := Params.SearchChannelPrefix
	searchResultPrefix := Params.SearchResultChannelPrefix
	allocatedQueryChannel := searchPrefix + "-" + strconv.FormatInt(collectionID, 10)
	allocatedQueryResultChannel := searchResultPrefix + "-" + strconv.FormatInt(collectionID, 10)
	log.Debug("query coordinator create query channel", zap.String("queryChannelName", allocatedQueryChannel), zap.String("queryResultChannelName", allocatedQueryResultChannel))

	seekPosition := &internalpb.MsgPosition{
		ChannelName: allocatedQueryChannel,
	}
	info := &querypb.QueryChannelInfo{
		CollectionID:         collectionID,
		QueryChannelID:       allocatedQueryChannel,
		QueryResultChannelID: allocatedQueryResultChannel,
		GlobalSealedSegments: []*querypb.SegmentInfo{},
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

	err := saveDeltaChannelInfo(collectionID, infos, m.client)
	if err != nil {
		log.Error("save delta channel info error", zap.Error(err))
		return err
	}
	log.Debug("save delta channel infos to meta", zap.Any("collectionID", collectionID), zap.Any("infos", infos))
	m.deltaChannelInfos[collectionID] = infos
	return nil
}

// Get Query channel info for collection, so far all the collection share the same query channel 0
func (m *MetaReplica) getQueryChannelInfoByID(collectionID UniqueID) (*querypb.QueryChannelInfo, error) {
	m.channelMu.Lock()
	defer m.channelMu.Unlock()

	if info, ok := m.queryChannelInfos[collectionID]; ok {
		return proto.Clone(info).(*querypb.QueryChannelInfo), nil
	}

	// TODO::to remove
	// all collection use the same query channel
	colIDForAssignChannel := UniqueID(0)
	info := createQueryChannel(colIDForAssignChannel)
	err := saveQueryChannelInfo(collectionID, info, m.client)
	if err != nil {
		log.Error("getQueryChannel: save channel to etcd error", zap.Error(err))
		return nil, err
	}
	// set info.collectionID from 0 to realID
	info.CollectionID = collectionID
	m.queryChannelInfos[collectionID] = info
	info.SeekPosition = m.globalSeekPosition
	if info.SeekPosition != nil {
		info.SeekPosition.ChannelName = info.QueryChannelID
	}
	return proto.Clone(info).(*querypb.QueryChannelInfo), nil
}

func (m *MetaReplica) getQueryStreamByID(collectionID UniqueID) (msgstream.MsgStream, error) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()

	info, err := m.getQueryChannelInfoByID(collectionID)
	if err != nil {
		return nil, err
	}

	stream, ok := m.queryStreams[collectionID]
	if !ok {
		stream, err = m.msFactory.NewMsgStream(m.ctx)
		if err != nil {
			log.Error("updateGlobalSealedSegmentInfos: create msgStream failed", zap.Error(err))
			return nil, err
		}

		queryChannel := info.QueryChannelID
		stream.AsProducer([]string{queryChannel})
		m.queryStreams[collectionID] = stream
		log.Debug("getQueryStreamByID: create query msgStream for collection", zap.Int64("collectionID", collectionID))
	}

	return stream, nil
}

func (m *MetaReplica) setLoadType(collectionID UniqueID, loadType querypb.LoadType) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()
	info, err := m.getCollectionInfoByIDPrivate(collectionID)
	if err == nil {
		info.LoadType = loadType
		err := saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
		m.collectionInfos[collectionID] = info
		return nil
	}

	return errors.New("setLoadType: can't find collection in collectionInfos")
}

func (m *MetaReplica) getLoadType(collectionID UniqueID) (querypb.LoadType, error) {
	m.collectionMu.RLock()
	defer m.collectionMu.RUnlock()

	if info, ok := m.collectionInfos[collectionID]; ok {
		return info.LoadType, nil
	}

	return 0, errors.New("getLoadType: can't find collection in collectionInfos")
}

func (m *MetaReplica) setLoadPercentage(collectionID UniqueID, partitionID UniqueID, percentage int64, loadType querypb.LoadType) error {
	m.collectionMu.Lock()
	defer m.collectionMu.Unlock()
	info, err := m.getCollectionInfoByIDPrivate(collectionID)
	if err != nil {
		return errors.New("setLoadPercentage: can't find collection in collectionInfos")
	}

	if loadType == querypb.LoadType_loadCollection {
		info.InMemoryPercentage = percentage
		for _, partitionState := range info.PartitionStates {
			if percentage >= 100 {
				partitionState.State = querypb.PartitionState_InMemory
			} else {
				partitionState.State = querypb.PartitionState_PartialInMemory
			}
			partitionState.InMemoryPercentage = percentage
		}
		err := saveGlobalCollectionInfo(collectionID, info, m.client)
		if err != nil {
			log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
			return err
		}
	} else {
		findPartition := false
		for _, partitionState := range info.PartitionStates {
			if partitionState.PartitionID == partitionID {
				findPartition = true
				if percentage >= 100 {
					partitionState.State = querypb.PartitionState_InMemory
				} else {
					partitionState.State = querypb.PartitionState_PartialInMemory
				}
				partitionState.InMemoryPercentage = percentage
				err := saveGlobalCollectionInfo(collectionID, info, m.client)
				if err != nil {
					log.Error("save collectionInfo error", zap.Any("error", err.Error()), zap.Int64("collectionID", collectionID))
					return err
				}
			}
		}
		if !findPartition {
			return errors.New("setLoadPercentage: can't find partitionID in collectionInfos")
		}
	}

	m.collectionInfos[collectionID] = info
	return nil
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

func removeGlobalCollectionInfo(collectionID UniqueID, kv kv.MetaKv) error {
	key := fmt.Sprintf("%s/%d", collectionMetaPrefix, collectionID)
	return kv.Remove(key)
}

func multiSaveSegmentInfos(segmentInfos map[UniqueID]*querypb.SegmentInfo, kv kv.MetaKv) error {
	kvs := make(map[string]string)
	for segmentID, info := range segmentInfos {
		infoBytes, err := proto.Marshal(info)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s/%d", util.SegmentMetaPrefix, segmentID)
		kvs[key] = string(infoBytes)
	}

	return kv.MultiSave(kvs)
}

func multiRemoveSegmentInfo(segmentIDs []UniqueID, kv kv.MetaKv) error {
	keys := make([]string, 0)
	for _, segmentID := range segmentIDs {
		key := fmt.Sprintf("%s/%d", util.SegmentMetaPrefix, segmentID)
		keys = append(keys, key)
	}

	return kv.MultiRemove(keys)
}

func saveQueryChannelInfo(collectionID UniqueID, info *querypb.QueryChannelInfo, kv kv.MetaKv) error {
	infoBytes, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%d", queryChannelMetaPrefix, collectionID)
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

func upgradeDmChannelInfos(collectionID UniqueID, channelInfos []*querypb.DmChannelInfo) []*querypb.DmChannelInfo {
	nodeID2WatchInfos := make(map[int64]map[string]*querypb.DmChannelWatchInfo)

	for _, channelInfo := range channelInfos {
		nodeID := channelInfo.NodeIDLoaded

		// in case after update milvus, can't find watch type from dmChannelInfo
		// different queryNode may watch same dmChannel and create same growing segment
		// deduplicate result when reduce, the correctness of search has no effect
		// and growing segment will be removed after handoff
		for _, channel := range channelInfo.ChannelIDs {
			watchInfo := &querypb.DmChannelWatchInfo{
				CollectionID: collectionID,
				ChannelName:  channel,
				WatchType:    querypb.WatchType_WatchCollection,
			}
			channelInfo.WatchInfos = append(channelInfo.WatchInfos, watchInfo)
		}

		for _, watchInfo := range channelInfo.WatchInfos {
			if _, ok := nodeID2WatchInfos[nodeID]; !ok {
				nodeID2WatchInfos[nodeID] = make(map[string]*querypb.DmChannelWatchInfo)
			}

			channelName := watchInfo.ChannelName
			if _, ok := nodeID2WatchInfos[nodeID][channelName]; !ok {
				nodeID2WatchInfos[nodeID][channelName] = watchInfo
				continue
			}

			watchedChannelInfo := nodeID2WatchInfos[nodeID][channelName]
			// watchedChannelInfo.WatchType == watchCollection
			if watchedChannelInfo.WatchType == querypb.WatchType_WatchCollection {
				continue
			}

			// watchedChannelInfo.WatchType == watchPartition && watchInfo.WatchType == watchCollection
			if watchInfo.WatchType == querypb.WatchType_WatchCollection {
				nodeID2WatchInfos[nodeID][channelName] = watchInfo
				continue
			}

			// watchedChannelInfo.WatchType == watchPartition && watchInfo.WatchType == watchPartition
			for _, partitionID := range watchInfo.PartitionIDs {
				hasWatchedPartition := false
				for _, watchedPartitionID := range watchedChannelInfo.PartitionIDs {
					if watchedPartitionID == partitionID {
						hasWatchedPartition = true
						break
					}
				}

				if !hasWatchedPartition {
					watchedChannelInfo.PartitionIDs = append(watchedChannelInfo.PartitionIDs, partitionID)
				}
			}
		}
	}

	result := make([]*querypb.DmChannelInfo, 0)
	for nodeID, watchInfos := range nodeID2WatchInfos {
		dmChannelInfo := &querypb.DmChannelInfo{
			NodeIDLoaded: nodeID,
		}
		for _, watchInfo := range watchInfos {
			dmChannelInfo.WatchInfos = append(dmChannelInfo.WatchInfos, watchInfo)
		}
		result = append(result, dmChannelInfo)
	}

	return result
}
