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

package channelmgr

import (
	"context"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ChannelsMgr manages the pchans, vchans and related message stream of collections.
type ChannelsMgr interface {
	// GetChannels returns the physical channels of a collection.
	GetChannels(collectionID typeutil.UniqueID) ([]string, error)
	// GetVChannels returns the virtual channels of a collection.
	GetVChannels(collectionID typeutil.UniqueID) ([]string, error)
	// RemoveStream removes the corresponding stream of the specified collection. Idempotent.
	RemoveStream(collectionID typeutil.UniqueID)
}

// ChannelInfo holds the virtual and physical channels of a collection.
type ChannelInfo struct {
	VChans []string
	PChans []string
}

// GetChannelsFunc resolves the channels of a collection. Implementations may
// cache lazily; the returned info must keep vchan/pchan aligned.
type GetChannelsFunc func(collectionID typeutil.UniqueID) (ChannelInfo, error)

// RepackFunc repacks messages into a message pack.
type RepackFunc func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error)

type streamInfos struct {
	channelInfo ChannelInfo
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

func newChannels(vchans []string, pchans []string) (ChannelInfo, error) {
	if len(vchans) != len(pchans) {
		mlog.Error(context.TODO(), "physical channels mismatch virtual channels", mlog.Int("len(VirtualChannelNames)", len(vchans)), mlog.Int("len(PhysicalChannelNames)", len(pchans)))
		// Channel lists come from DescribeCollection (coordinator-allocated
		// metadata), never from the caller: a mismatch is a server-side bug.
		return ChannelInfo{}, merr.WrapErrServiceInternalMsg("physical channels mismatch virtual channels, len(VirtualChannelNames): %v, len(PhysicalChannelNames): %v", len(vchans), len(pchans))
	}
	return ChannelInfo{VChans: vchans, PChans: pchans}, nil
}

type channelsMgrImpl struct {
	infos map[typeutil.UniqueID]streamInfos // collection id -> stream infos
	mu    sync.RWMutex

	getChannelsFunc GetChannelsFunc
	repackFunc      RepackFunc
}

func (mgr *channelsMgrImpl) getAllChannels(collectionID typeutil.UniqueID) (ChannelInfo, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	infos, ok := mgr.infos[collectionID]
	if ok {
		return infos.channelInfo, nil
	}

	return ChannelInfo{}, merr.WrapErrParameterInvalidMsg("collection not found in channels manager: %d", collectionID)
}

func (mgr *channelsMgrImpl) ensureChannels(collectionID typeutil.UniqueID) (ChannelInfo, error) {
	if infos, err := mgr.getAllChannels(collectionID); err == nil {
		return infos, nil
	}

	channelInfos, err := mgr.getChannelsFunc(collectionID)
	if err != nil {
		return ChannelInfo{}, err
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if infos, ok := mgr.infos[collectionID]; ok {
		return infos.channelInfo, nil
	}
	mgr.infos[collectionID] = streamInfos{channelInfo: channelInfos}
	incPChansMetrics(channelInfos.PChans)
	return channelInfos, nil
}

// GetChannels returns the physical channels.
func (mgr *channelsMgrImpl) GetChannels(collectionID typeutil.UniqueID) ([]string, error) {
	channelInfos, err := mgr.ensureChannels(collectionID)
	if err != nil {
		return nil, err
	}
	return channelInfos.PChans, nil
}

// GetVChannels returns the virtual channels.
func (mgr *channelsMgrImpl) GetVChannels(collectionID typeutil.UniqueID) ([]string, error) {
	channelInfos, err := mgr.ensureChannels(collectionID)
	if err != nil {
		return nil, err
	}
	return channelInfos.VChans, nil
}

func incPChansMetrics(pchans []string) {
	for _, pc := range pchans {
		metrics.ProxyMsgStreamObjectsForPChan.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), pc).Inc()
	}
}

func decPChanMetrics(pchans []string) {
	for _, pc := range pchans {
		metrics.ProxyMsgStreamObjectsForPChan.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), pc).Dec()
	}
}

// RemoveStream removes the corresponding stream of the specified collection. Idempotent.
// If stream already exists, remove it, otherwise do nothing.
func (mgr *channelsMgrImpl) RemoveStream(collectionID typeutil.UniqueID) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if info, ok := mgr.infos[collectionID]; ok {
		decPChanMetrics(info.channelInfo.PChans)
		delete(mgr.infos, collectionID)
	}
	mlog.Info(context.TODO(), "dml stream removed", mlog.Int64("collection_id", collectionID))
}

// NewChannelsMgr constructs a channels manager backed by the given resolver.
// getChannelsFunc resolves collection channels; repackFunc repacks messages.
func NewChannelsMgr(
	getChannelsFunc GetChannelsFunc,
	repackFunc RepackFunc,
) ChannelsMgr {
	return &channelsMgrImpl{
		infos:           make(map[typeutil.UniqueID]streamInfos),
		getChannelsFunc: getChannelsFunc,
		repackFunc:      repackFunc,
	}
}
