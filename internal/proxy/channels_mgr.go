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
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// channelsMgr manages the pchans, vchans and related message stream of collections.
type channelsMgr interface {
	getChannels(collectionID UniqueID) ([]pChan, error)
	getVChannels(collectionID UniqueID) ([]vChan, error)
	getOrCreateDmlStream(ctx context.Context, collectionID UniqueID) (msgstream.MsgStream, error)
	removeDMLStream(collectionID UniqueID)
	removeAllDMLStream()
}

type channelInfos struct {
	// It seems that there is no need to maintain relationships between vchans & pchans.
	vchans []vChan
	pchans []pChan
}

type streamInfos struct {
	channelInfos channelInfos
	stream       msgstream.MsgStream
}

func removeDuplicate(ss []string) []string {
	m := make(map[string]struct{})
	filtered := make([]string, 0, len(ss))
	for _, s := range ss {
		if _, ok := m[s]; !ok {
			filtered = append(filtered, s)
			m[s] = struct{}{}
		}
	}
	return filtered
}

func newChannels(vchans []vChan, pchans []pChan) (channelInfos, error) {
	if len(vchans) != len(pchans) {
		log.Error("physical channels mismatch virtual channels", zap.Int("len(VirtualChannelNames)", len(vchans)), zap.Int("len(PhysicalChannelNames)", len(pchans)))
		return channelInfos{}, fmt.Errorf("physical channels mismatch virtual channels, len(VirtualChannelNames): %v, len(PhysicalChannelNames): %v", len(vchans), len(pchans))
	}
	/*
		// remove duplicate physical channels.
		return channelInfos{vchans: vchans, pchans: removeDuplicate(pchans)}, nil
	*/
	return channelInfos{vchans: vchans, pchans: pchans}, nil
}

// getChannelsFuncType returns the channel information according to the collection id.
type getChannelsFuncType = func(collectionID UniqueID) (channelInfos, error)

// repackFuncType repacks message into message pack.
type repackFuncType = func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error)

// getDmlChannelsFunc returns a function about how to get dml channels of a collection.
func getDmlChannelsFunc(ctx context.Context, rc types.RootCoordClient) getChannelsFuncType {
	return func(collectionID UniqueID) (channelInfos, error) {
		req := &milvuspb.DescribeCollectionRequest{
			Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection)),
			CollectionID: collectionID,
		}

		resp, err := rc.DescribeCollection(ctx, req)
		if err != nil {
			log.Error("failed to describe collection", zap.Error(err), zap.Int64("collection", collectionID))
			return channelInfos{}, err
		}

		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Error("failed to describe collection",
				zap.String("error_code", resp.GetStatus().GetErrorCode().String()),
				zap.String("reason", resp.GetStatus().GetReason()))
			return channelInfos{}, merr.Error(resp.GetStatus())
		}

		return newChannels(resp.GetVirtualChannelNames(), resp.GetPhysicalChannelNames())
	}
}

type singleTypeChannelsMgr struct {
	infos map[UniqueID]streamInfos // collection id -> stream infos
	mu    sync.RWMutex

	getChannelsFunc  getChannelsFuncType
	repackFunc       repackFuncType
	msgStreamFactory msgstream.Factory
}

func (mgr *singleTypeChannelsMgr) getAllChannels(collectionID UniqueID) (channelInfos, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	infos, ok := mgr.infos[collectionID]
	if ok {
		return infos.channelInfos, nil
	}

	return channelInfos{}, fmt.Errorf("collection not found in channels manager: %d", collectionID)
}

func (mgr *singleTypeChannelsMgr) getPChans(collectionID UniqueID) ([]pChan, error) {
	channelInfos, err := mgr.getChannelsFunc(collectionID)
	if err != nil {
		return nil, err
	}
	return channelInfos.pchans, nil
}

func (mgr *singleTypeChannelsMgr) getVChans(collectionID UniqueID) ([]vChan, error) {
	channelInfos, err := mgr.getChannelsFunc(collectionID)
	if err != nil {
		return nil, err
	}
	return channelInfos.vchans, nil
}

// getChannels returns the physical channels.
func (mgr *singleTypeChannelsMgr) getChannels(collectionID UniqueID) ([]pChan, error) {
	var channelInfos channelInfos
	channelInfos, err := mgr.getAllChannels(collectionID)
	if err != nil {
		return mgr.getPChans(collectionID)
	}
	return channelInfos.pchans, nil
}

// getVChannels returns the virtual channels.
func (mgr *singleTypeChannelsMgr) getVChannels(collectionID UniqueID) ([]vChan, error) {
	var channelInfos channelInfos
	channelInfos, err := mgr.getAllChannels(collectionID)
	if err != nil {
		return mgr.getVChans(collectionID)
	}
	return channelInfos.vchans, nil
}

func (mgr *singleTypeChannelsMgr) streamExistPrivate(collectionID UniqueID) bool {
	streamInfos, ok := mgr.infos[collectionID]
	return ok && streamInfos.stream != nil
}

func createStream(ctx context.Context, factory msgstream.Factory, pchans []pChan, repack repackFuncType) (msgstream.MsgStream, error) {
	var stream msgstream.MsgStream
	var err error

	stream, err = factory.NewMsgStream(context.Background())
	if err != nil {
		return nil, err
	}

	stream.AsProducer(ctx, pchans)
	if repack != nil {
		stream.SetRepackFunc(repack)
	}
	return stream, nil
}

func incPChansMetrics(pchans []pChan) {
	for _, pc := range pchans {
		metrics.ProxyMsgStreamObjectsForPChan.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), pc).Inc()
	}
}

func decPChanMetrics(pchans []pChan) {
	for _, pc := range pchans {
		metrics.ProxyMsgStreamObjectsForPChan.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), pc).Dec()
	}
}

// createMsgStream create message stream for specified collection. Idempotent.
// If stream already exists, directly return it and no error will be returned.
func (mgr *singleTypeChannelsMgr) createMsgStream(ctx context.Context, collectionID UniqueID) (msgstream.MsgStream, error) {
	mgr.mu.RLock()
	infos, ok := mgr.infos[collectionID]
	if ok && infos.stream != nil {
		// already exist.
		mgr.mu.RUnlock()
		return infos.stream, nil
	}
	mgr.mu.RUnlock()

	channelInfos, err := mgr.getChannelsFunc(collectionID)
	if err != nil {
		// What if stream created by other goroutines?
		log.Error("failed to get channels", zap.Error(err), zap.Int64("collection", collectionID))
		return nil, err
	}

	stream, err := createStream(ctx, mgr.msgStreamFactory, channelInfos.pchans, mgr.repackFunc)
	if err != nil {
		// What if stream created by other goroutines?
		log.Error("failed to create message stream", zap.Error(err), zap.Int64("collection", collectionID))
		return nil, err
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if !mgr.streamExistPrivate(collectionID) {
		log.Info("create message stream", zap.Int64("collection", collectionID),
			zap.Strings("virtual_channels", channelInfos.vchans),
			zap.Strings("physical_channels", channelInfos.pchans))
		mgr.infos[collectionID] = streamInfos{channelInfos: channelInfos, stream: stream}
		incPChansMetrics(channelInfos.pchans)
	} else {
		stream.Close()
	}

	return mgr.infos[collectionID].stream, nil
}

func (mgr *singleTypeChannelsMgr) lockGetStream(collectionID UniqueID) (msgstream.MsgStream, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	streamInfos, ok := mgr.infos[collectionID]
	if ok {
		return streamInfos.stream, nil
	}
	return nil, fmt.Errorf("collection not found: %d", collectionID)
}

// getOrCreateStream get message stream of specified collection.
// If stream doesn't exist, call createMsgStream to create for it.
func (mgr *singleTypeChannelsMgr) getOrCreateStream(ctx context.Context, collectionID UniqueID) (msgstream.MsgStream, error) {
	if stream, err := mgr.lockGetStream(collectionID); err == nil {
		return stream, nil
	}

	return mgr.createMsgStream(ctx, collectionID)
}

// removeStream remove the corresponding stream of the specified collection. Idempotent.
// If stream already exists, remove it, otherwise do nothing.
func (mgr *singleTypeChannelsMgr) removeStream(collectionID UniqueID) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if info, ok := mgr.infos[collectionID]; ok {
		decPChanMetrics(info.channelInfos.pchans)
		info.stream.Close()
		delete(mgr.infos, collectionID)
	}
	log.Info("dml stream removed", zap.Int64("collection_id", collectionID))
}

// removeAllStream remove all message stream.
func (mgr *singleTypeChannelsMgr) removeAllStream() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for _, info := range mgr.infos {
		info.stream.Close()
		decPChanMetrics(info.channelInfos.pchans)
	}
	mgr.infos = make(map[UniqueID]streamInfos)
	log.Info("all dml stream removed")
}

func newSingleTypeChannelsMgr(
	getChannelsFunc getChannelsFuncType,
	msgStreamFactory msgstream.Factory,
	repackFunc repackFuncType,
) *singleTypeChannelsMgr {
	return &singleTypeChannelsMgr{
		infos:            make(map[UniqueID]streamInfos),
		getChannelsFunc:  getChannelsFunc,
		repackFunc:       repackFunc,
		msgStreamFactory: msgStreamFactory,
	}
}

// implementation assertion
var _ channelsMgr = (*channelsMgrImpl)(nil)

// channelsMgrImpl implements channelsMgr.
type channelsMgrImpl struct {
	dmlChannelsMgr *singleTypeChannelsMgr
}

func (mgr *channelsMgrImpl) getChannels(collectionID UniqueID) ([]pChan, error) {
	return mgr.dmlChannelsMgr.getChannels(collectionID)
}

func (mgr *channelsMgrImpl) getVChannels(collectionID UniqueID) ([]vChan, error) {
	return mgr.dmlChannelsMgr.getVChannels(collectionID)
}

func (mgr *channelsMgrImpl) getOrCreateDmlStream(ctx context.Context, collectionID UniqueID) (msgstream.MsgStream, error) {
	return mgr.dmlChannelsMgr.getOrCreateStream(ctx, collectionID)
}

func (mgr *channelsMgrImpl) removeDMLStream(collectionID UniqueID) {
	mgr.dmlChannelsMgr.removeStream(collectionID)
}

func (mgr *channelsMgrImpl) removeAllDMLStream() {
	mgr.dmlChannelsMgr.removeAllStream()
}

// newChannelsMgrImpl constructs a channels manager.
func newChannelsMgrImpl(
	getDmlChannelsFunc getChannelsFuncType,
	dmlRepackFunc repackFuncType,
	msgStreamFactory msgstream.Factory,
) *channelsMgrImpl {
	return &channelsMgrImpl{
		dmlChannelsMgr: newSingleTypeChannelsMgr(getDmlChannelsFunc, msgStreamFactory, dmlRepackFunc),
	}
}
