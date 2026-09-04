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

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ChannelsMgr resolves the DML channels of collections.
type ChannelsMgr interface {
	// GetChannels returns the physical channels of a collection.
	GetChannels(collectionID typeutil.UniqueID) ([]string, error)
	// GetVChannels returns the virtual channels of a collection.
	GetVChannels(collectionID typeutil.UniqueID) ([]string, error)
}

// ChannelInfo holds the virtual and physical channels of a collection.
type ChannelInfo struct {
	VChans []string
	PChans []string
}

// GetChannelsFunc resolves the channels of a collection. The returned info
// must keep vchan/pchan aligned.
type GetChannelsFunc func(collectionID typeutil.UniqueID) (ChannelInfo, error)

// newChannels validates the alignment between virtual and physical channels
// and returns them as a ChannelInfo.
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
	getChannelsFunc GetChannelsFunc
}

// resolve validates and returns the channels of a collection, delegating the
// actual lookup to the injected resolver. The resolver owns any caching (e.g.
// it may read the meta cache), so this package keeps no channel cache of its
// own and therefore never serves stale channel metadata.
func (mgr *channelsMgrImpl) resolve(collectionID typeutil.UniqueID) (ChannelInfo, error) {
	channelInfos, err := mgr.getChannelsFunc(collectionID)
	if err != nil {
		return ChannelInfo{}, err
	}
	// Re-validate the alignment for every resolver result, since the resolver
	// is injected and may not run the len(vchans)==len(pchans) guard itself
	// (e.g. when it reads the meta cache, which copies the two lists verbatim).
	return newChannels(channelInfos.VChans, channelInfos.PChans)
}

// GetChannels returns the physical channels.
func (mgr *channelsMgrImpl) GetChannels(collectionID typeutil.UniqueID) ([]string, error) {
	channelInfos, err := mgr.resolve(collectionID)
	if err != nil {
		return nil, err
	}
	return channelInfos.PChans, nil
}

// GetVChannels returns the virtual channels.
func (mgr *channelsMgrImpl) GetVChannels(collectionID typeutil.UniqueID) ([]string, error) {
	channelInfos, err := mgr.resolve(collectionID)
	if err != nil {
		return nil, err
	}
	return channelInfos.VChans, nil
}

// NewChannelsMgr constructs a channels manager backed by the given resolver.
// getChannelsFunc resolves collection channels.
func NewChannelsMgr(
	getChannelsFunc GetChannelsFunc,
) ChannelsMgr {
	return &channelsMgrImpl{
		getChannelsFunc: getChannelsFunc,
	}
}
