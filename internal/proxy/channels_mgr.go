// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/querypb"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"go.uber.org/zap"
)

// channelsMgr manages the pchans, vchans and related message stream of collections.
type channelsMgr interface {
	getChannels(collectionID UniqueID) ([]pChan, error)
	getVChannels(collectionID UniqueID) ([]vChan, error)
	createDQLStream(collectionID UniqueID) error
	getDQLStream(collectionID UniqueID) (msgstream.MsgStream, error)
	removeDQLStream(collectionID UniqueID) error
	removeAllDQLStream() error
	createDMLMsgStream(collectionID UniqueID) error
	getDMLStream(collectionID UniqueID) (msgstream.MsgStream, error)
	removeDMLStream(collectionID UniqueID) error
	removeAllDMLStream() error
}

type getChannelsFuncType = func(collectionID UniqueID) (map[vChan]pChan, error)
type repackFuncType = func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error)

func getDmlChannelsFunc(ctx context.Context, rc types.RootCoord) getChannelsFuncType {
	return func(collectionID UniqueID) (map[vChan]pChan, error) {
		req := &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     0, // todo
				Timestamp: 0, // todo
				SourceID:  0, // todo
			},
			DbName:         "", // todo
			CollectionName: "", // todo
			CollectionID:   collectionID,
			TimeStamp:      0, // todo
		}
		resp, err := rc.DescribeCollection(ctx, req)
		if err != nil {
			log.Warn("DescribeCollection", zap.Error(err))
			return nil, err
		}
		if resp.Status.ErrorCode != 0 {
			log.Warn("DescribeCollection",
				zap.Any("ErrorCode", resp.Status.ErrorCode),
				zap.Any("Reason", resp.Status.Reason))
			return nil, err
		}
		if len(resp.VirtualChannelNames) != len(resp.PhysicalChannelNames) {
			err := fmt.Errorf(
				"len(VirtualChannelNames): %v, len(PhysicalChannelNames): %v",
				len(resp.VirtualChannelNames),
				len(resp.PhysicalChannelNames))
			log.Warn("GetDmlChannels", zap.Error(err))
			return nil, err
		}

		ret := make(map[vChan]pChan)
		for idx, name := range resp.VirtualChannelNames {
			if _, ok := ret[name]; ok {
				err := fmt.Errorf(
					"duplicated virtual channel found, vchan: %v, pchan: %v",
					name,
					resp.PhysicalChannelNames[idx])
				return nil, err
			}
			ret[name] = resp.PhysicalChannelNames[idx]
		}

		return ret, nil
	}
}

func getDqlChannelsFunc(ctx context.Context, proxyID int64, qc createQueryChannelInterface) getChannelsFuncType {
	return func(collectionID UniqueID) (map[vChan]pChan, error) {
		req := &querypb.CreateQueryChannelRequest{
			CollectionID: collectionID,
			ProxyID:      proxyID,
		}
		resp, err := qc.CreateQueryChannel(ctx, req)
		if err != nil {
			return nil, err
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, errors.New(resp.Status.Reason)
		}

		m := make(map[vChan]pChan)
		m[resp.RequestChannel] = resp.RequestChannel

		return m, nil
	}
}

type streamType int

const (
	dmlStreamType streamType = iota
	dqlStreamType
)

type singleTypeChannelsMgr struct {
	collectionID2VIDs map[UniqueID][]int // id are sorted
	collMtx           sync.RWMutex

	id2vchans    map[int][]vChan
	id2vchansMtx sync.RWMutex

	id2Stream                 map[int]msgstream.MsgStream
	id2UsageHistogramOfStream map[int]int
	streamMtx                 sync.RWMutex

	vchans2pchans    map[vChan]pChan
	vchans2pchansMtx sync.RWMutex

	getChannelsFunc getChannelsFuncType

	repackFunc repackFuncType

	singleStreamType streamType

	msgStreamFactory msgstream.Factory
}

func getAllKeys(m map[vChan]pChan) []vChan {
	keys := make([]vChan, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func getAllValues(m map[vChan]pChan) []pChan {
	values := make([]pChan, 0, len(m))
	for _, value := range m {
		values = append(values, value)
	}
	return values
}

func (mgr *singleTypeChannelsMgr) getLatestVID(collectionID UniqueID) (int, error) {
	mgr.collMtx.RLock()
	defer mgr.collMtx.RUnlock()

	ids, ok := mgr.collectionID2VIDs[collectionID]
	if !ok || len(ids) <= 0 {
		return 0, fmt.Errorf("collection %d not found", collectionID)
	}

	return ids[len(ids)-1], nil
}

func (mgr *singleTypeChannelsMgr) getAllVIDs(collectionID UniqueID) ([]int, error) {
	mgr.collMtx.RLock()
	defer mgr.collMtx.RUnlock()

	ids, exist := mgr.collectionID2VIDs[collectionID]
	if !exist {
		return nil, fmt.Errorf("collection %d not found", collectionID)
	}

	return ids, nil
}

func (mgr *singleTypeChannelsMgr) getVChansByVID(vid int) ([]vChan, error) {
	mgr.id2vchansMtx.RLock()
	defer mgr.id2vchansMtx.RUnlock()

	vchans, ok := mgr.id2vchans[vid]
	if !ok {
		return nil, fmt.Errorf("vid %d not found", vid)
	}

	return vchans, nil
}

func (mgr *singleTypeChannelsMgr) getPChansByVChans(vchans []vChan) ([]pChan, error) {
	mgr.vchans2pchansMtx.RLock()
	defer mgr.vchans2pchansMtx.RUnlock()

	pchans := make([]pChan, 0)
	for _, vchan := range vchans {
		pchan, ok := mgr.vchans2pchans[vchan]
		if !ok {
			return nil, fmt.Errorf("vchan %v not found", vchan)
		}
		pchans = append(pchans, pchan)
	}

	return pchans, nil
}

func (mgr *singleTypeChannelsMgr) updateVChans(vid int, vchans []vChan) {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	mgr.id2vchans[vid] = vchans
}

func (mgr *singleTypeChannelsMgr) deleteVChansByVID(vid int) {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	delete(mgr.id2vchans, vid)
}

func (mgr *singleTypeChannelsMgr) deleteVChansByVIDs(vids []int) {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	for _, vid := range vids {
		delete(mgr.id2vchans, vid)
	}
}

func (mgr *singleTypeChannelsMgr) deleteStreamByVID(vid int) {
	mgr.streamMtx.Lock()
	defer mgr.streamMtx.Unlock()

	delete(mgr.id2Stream, vid)
}

func (mgr *singleTypeChannelsMgr) deleteStreamByVIDs(vids []int) {
	mgr.streamMtx.Lock()
	defer mgr.streamMtx.Unlock()

	for _, vid := range vids {
		delete(mgr.id2Stream, vid)
	}
}

func (mgr *singleTypeChannelsMgr) updateChannels(channels map[vChan]pChan) {
	mgr.vchans2pchansMtx.Lock()
	defer mgr.vchans2pchansMtx.Unlock()

	for vchan, pchan := range channels {
		mgr.vchans2pchans[vchan] = pchan
	}
}

func (mgr *singleTypeChannelsMgr) deleteAllChannels() {
	mgr.vchans2pchansMtx.Lock()
	defer mgr.vchans2pchansMtx.Unlock()

	mgr.vchans2pchans = nil
}

func (mgr *singleTypeChannelsMgr) deleteAllStream() {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	mgr.id2UsageHistogramOfStream = nil
	mgr.id2Stream = nil
}

func (mgr *singleTypeChannelsMgr) deleteAllVChans() {
	mgr.id2vchansMtx.Lock()
	defer mgr.id2vchansMtx.Unlock()

	mgr.id2vchans = nil
}

func (mgr *singleTypeChannelsMgr) deleteAllCollection() {
	mgr.collMtx.Lock()
	defer mgr.collMtx.Unlock()

	mgr.collectionID2VIDs = nil
}

func (mgr *singleTypeChannelsMgr) addStream(vid int, stream msgstream.MsgStream) {
	mgr.streamMtx.Lock()
	defer mgr.streamMtx.Unlock()

	mgr.id2Stream[vid] = stream
	mgr.id2UsageHistogramOfStream[vid] = 0
}

func (mgr *singleTypeChannelsMgr) updateCollection(collectionID UniqueID, id int) {
	mgr.collMtx.Lock()
	defer mgr.collMtx.Unlock()

	vids, ok := mgr.collectionID2VIDs[collectionID]
	if !ok {
		mgr.collectionID2VIDs[collectionID] = make([]int, 1)
		mgr.collectionID2VIDs[collectionID][0] = id
	} else {
		vids = append(vids, id)
		sort.Slice(vids, func(i, j int) bool {
			return vids[i] < vids[j]
		})
		mgr.collectionID2VIDs[collectionID] = vids
	}
}

func (mgr *singleTypeChannelsMgr) getChannels(collectionID UniqueID) ([]pChan, error) {
	id, err := mgr.getLatestVID(collectionID)
	if err == nil {
		vchans, err := mgr.getVChansByVID(id)
		if err != nil {
			return nil, err
		}

		return mgr.getPChansByVChans(vchans)
	}

	// TODO(dragondriver): return error or update channel information from master?
	return nil, err
}

func (mgr *singleTypeChannelsMgr) getVChannels(collectionID UniqueID) ([]vChan, error) {
	id, err := mgr.getLatestVID(collectionID)
	if err == nil {
		return mgr.getVChansByVID(id)
	}

	// TODO(dragondriver): return error or update channel information from master?
	return nil, err
}

func (mgr *singleTypeChannelsMgr) createMsgStream(collectionID UniqueID) error {
	channels, err := mgr.getChannelsFunc(collectionID)
	if err != nil {
		log.Warn("failed to create message stream",
			zap.Int64("collection_id", collectionID),
			zap.Error(err))
		return err
	}
	log.Debug("singleTypeChannelsMgr",
		zap.Int64("collection_id", collectionID),
		zap.Any("createMsgStream.getChannels", channels))

	mgr.updateChannels(channels)

	id := uniquegenerator.GetUniqueIntGeneratorIns().GetInt()

	vchans, pchans := make([]string, 0, len(channels)), make([]string, 0, len(channels))
	for k, v := range channels {
		vchans = append(vchans, k)
		pchans = append(pchans, v)
	}
	mgr.updateVChans(id, vchans)

	var stream msgstream.MsgStream
	if mgr.singleStreamType == dqlStreamType {
		stream, err = mgr.msgStreamFactory.NewQueryMsgStream(context.Background())
	} else {
		stream, err = mgr.msgStreamFactory.NewMsgStream(context.Background())
	}
	if err != nil {
		return err
	}
	stream.AsProducer(pchans)
	if mgr.repackFunc != nil {
		stream.SetRepackFunc(mgr.repackFunc)
	}
	runtime.SetFinalizer(stream, func(stream msgstream.MsgStream) {
		stream.Close()
	})
	mgr.addStream(id, stream)

	mgr.updateCollection(collectionID, id)

	return nil
}

func (mgr *singleTypeChannelsMgr) getStream(collectionID UniqueID) (msgstream.MsgStream, error) {
	mgr.streamMtx.RLock()
	defer mgr.streamMtx.RUnlock()

	vid, err := mgr.getLatestVID(collectionID)
	if err != nil {
		return nil, err
	}

	stream, ok := mgr.id2Stream[vid]
	if !ok {
		return nil, fmt.Errorf("no dml stream for collection %v", collectionID)
	}

	return stream, nil
}

func (mgr *singleTypeChannelsMgr) removeStream(collectionID UniqueID) error {
	ids, err := mgr.getAllVIDs(collectionID)
	if err != nil {
		return err
	}

	mgr.deleteVChansByVIDs(ids)
	mgr.deleteStreamByVIDs(ids)

	return nil
}

func (mgr *singleTypeChannelsMgr) removeAllStream() error {
	mgr.deleteAllChannels()
	mgr.deleteAllStream()
	mgr.deleteAllVChans()
	mgr.deleteAllCollection()

	return nil
}

func newSingleTypeChannelsMgr(
	getChannelsFunc getChannelsFuncType,
	msgStreamFactory msgstream.Factory,
	repackFunc repackFuncType,
	singleStreamType streamType,
) *singleTypeChannelsMgr {
	return &singleTypeChannelsMgr{
		collectionID2VIDs:         make(map[UniqueID][]int),
		id2vchans:                 make(map[int][]vChan),
		id2Stream:                 make(map[int]msgstream.MsgStream),
		id2UsageHistogramOfStream: make(map[int]int),
		vchans2pchans:             make(map[vChan]pChan),
		getChannelsFunc:           getChannelsFunc,
		repackFunc:                repackFunc,
		singleStreamType:          singleStreamType,
		msgStreamFactory:          msgStreamFactory,
	}
}

type channelsMgrImpl struct {
	dmlChannelsMgr *singleTypeChannelsMgr
	dqlChannelsMgr *singleTypeChannelsMgr
}

func (mgr *channelsMgrImpl) getChannels(collectionID UniqueID) ([]pChan, error) {
	return mgr.dmlChannelsMgr.getChannels(collectionID)
}

func (mgr *channelsMgrImpl) getVChannels(collectionID UniqueID) ([]vChan, error) {
	return mgr.dmlChannelsMgr.getVChannels(collectionID)
}

func (mgr *channelsMgrImpl) createDQLStream(collectionID UniqueID) error {
	return mgr.dqlChannelsMgr.createMsgStream(collectionID)
}

func (mgr *channelsMgrImpl) getDQLStream(collectionID UniqueID) (msgstream.MsgStream, error) {
	return mgr.dqlChannelsMgr.getStream(collectionID)
}

func (mgr *channelsMgrImpl) removeDQLStream(collectionID UniqueID) error {
	return mgr.dqlChannelsMgr.removeStream(collectionID)
}

func (mgr *channelsMgrImpl) removeAllDQLStream() error {
	return mgr.dqlChannelsMgr.removeAllStream()
}

func (mgr *channelsMgrImpl) createDMLMsgStream(collectionID UniqueID) error {
	return mgr.dmlChannelsMgr.createMsgStream(collectionID)
}

func (mgr *channelsMgrImpl) getDMLStream(collectionID UniqueID) (msgstream.MsgStream, error) {
	return mgr.dmlChannelsMgr.getStream(collectionID)
}

func (mgr *channelsMgrImpl) removeDMLStream(collectionID UniqueID) error {
	return mgr.dmlChannelsMgr.removeStream(collectionID)
}

func (mgr *channelsMgrImpl) removeAllDMLStream() error {
	return mgr.dmlChannelsMgr.removeAllStream()
}

func newChannelsMgrImpl(
	getDmlChannelsFunc getChannelsFuncType,
	dmlRepackFunc repackFuncType,
	getDqlChannelsFunc getChannelsFuncType,
	dqlRepackFunc repackFuncType,
	msgStreamFactory msgstream.Factory,
) *channelsMgrImpl {
	return &channelsMgrImpl{
		dmlChannelsMgr: newSingleTypeChannelsMgr(getDmlChannelsFunc, msgStreamFactory, dmlRepackFunc, dmlStreamType),
		dqlChannelsMgr: newSingleTypeChannelsMgr(getDqlChannelsFunc, msgStreamFactory, dqlRepackFunc, dqlStreamType),
	}
}
