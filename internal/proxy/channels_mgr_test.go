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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
)

func Test_removeDuplicate(t *testing.T) {
	s1 := []string{"11", "11"}
	filtered1 := removeDuplicate(s1)
	assert.ElementsMatch(t, filtered1, []string{"11"})
}

func Test_newChannels(t *testing.T) {
	t.Run("length mismatch", func(t *testing.T) {
		_, err := newChannels([]string{"111", "222"}, []string{"111"})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		got, err := newChannels([]string{"111", "222"}, []string{"111", "111"})
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got.vchans)
		// assert.ElementsMatch(t, []string{"111"}, got.pchans)
		assert.ElementsMatch(t, []string{"111", "111"}, got.pchans)
	})
}

func Test_getDmlChannelsFunc(t *testing.T) {
	t.Run("failed to describe collection", func(t *testing.T) {
		ctx := context.Background()
		rc := mocks.NewMockMixCoordClient(t)
		rc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, errors.New("mock"))

		f := getDmlChannelsFunc(ctx, rc)
		_, err := f(100)
		assert.Error(t, err)
	})

	t.Run("error code not success", func(t *testing.T) {
		ctx := context.Background()
		rc := mocks.NewMockMixCoordClient(t)
		rc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil)
		f := getDmlChannelsFunc(ctx, rc)
		_, err := f(100)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		rc := mocks.NewMockMixCoordClient(t)
		rc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames:  []string{"111", "222"},
			PhysicalChannelNames: []string{"111", "111"},
			Status:               &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)

		f := getDmlChannelsFunc(ctx, rc)
		got, err := f(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got.vchans)
		// assert.ElementsMatch(t, []string{"111"}, got.pchans)
		assert.ElementsMatch(t, []string{"111", "111"}, got.pchans)
	})
}

func Test_singleTypeChannelsMgr_getAllChannels(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{
				100: {channelInfos: channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}},
			},
		}
		got, err := m.getAllChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got.vchans)
		assert.ElementsMatch(t, []string{"111"}, got.pchans)
	})

	t.Run("not found", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{},
		}
		_, err := m.getAllChannels(100)
		assert.Error(t, err)
	})
}

func Test_singleTypeChannelsMgr_getPChans(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}, nil
			},
		}
		got, err := m.getPChans(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.getPChans(100)
		assert.Error(t, err)
	})
}

func Test_singleTypeChannelsMgr_getVChans(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}, nil
			},
		}
		got, err := m.getVChans(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.getVChans(100)
		assert.Error(t, err)
	})
}

func Test_singleTypeChannelsMgr_getChannels(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{
				100: {channelInfos: channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}},
			},
		}
		got, err := m.getChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.getChannels(100)
		assert.Error(t, err)
	})
}

func Test_singleTypeChannelsMgr_getVChannels(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			infos: map[UniqueID]streamInfos{
				100: {channelInfos: channelInfos{vchans: []string{"111", "222"}, pchans: []string{"111"}}},
			},
		}
		got, err := m.getVChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &singleTypeChannelsMgr{
			getChannelsFunc: func(collectionID UniqueID) (channelInfos, error) {
				return channelInfos{}, errors.New("mock")
			},
		}
		_, err := m.getVChannels(100)
		assert.Error(t, err)
	})
}

func Test_singleTypeChannelsMgr_removeStream(t *testing.T) {
	m := &singleTypeChannelsMgr{
		infos: map[UniqueID]streamInfos{
			100: {},
		},
	}
	m.removeStream(100)
}
