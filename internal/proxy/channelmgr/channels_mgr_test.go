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
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
		assert.ElementsMatch(t, []string{"111", "222"}, got.VChans)
		assert.ElementsMatch(t, []string{"111", "111"}, got.PChans)
	})
}

func Test_channelsMgrImpl_getAllChannels(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &channelsMgrImpl{
			infos: map[typeutil.UniqueID]streamInfos{
				100: {channelInfo: ChannelInfo{VChans: []string{"111", "222"}, PChans: []string{"111"}}},
			},
		}
		got, err := m.getAllChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got.VChans)
		assert.ElementsMatch(t, []string{"111"}, got.PChans)
	})

	t.Run("not found", func(t *testing.T) {
		m := &channelsMgrImpl{
			infos: map[typeutil.UniqueID]streamInfos{},
		}
		_, err := m.getAllChannels(100)
		assert.Error(t, err)
	})
}

func Test_channelsMgrImpl_ensureChannels(t *testing.T) {
	t.Run("hit cache", func(t *testing.T) {
		m := &channelsMgrImpl{
			infos: map[typeutil.UniqueID]streamInfos{
				100: {channelInfo: ChannelInfo{VChans: []string{"111"}, PChans: []string{"p111"}}},
			},
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				return ChannelInfo{}, errors.New("should not be called")
			},
		}
		got, err := m.ensureChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111"}, got.VChans)
		assert.ElementsMatch(t, []string{"p111"}, got.PChans)
	})

	t.Run("load and cache", func(t *testing.T) {
		called := atomic.Int32{}
		m := &channelsMgrImpl{
			infos: make(map[typeutil.UniqueID]streamInfos),
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				called.Add(1)
				return ChannelInfo{VChans: []string{"111", "222"}, PChans: []string{"p111", "p222"}}, nil
			},
		}
		got, err := m.ensureChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got.VChans)
		assert.ElementsMatch(t, []string{"p111", "p222"}, got.PChans)
		assert.Equal(t, int32(1), called.Load())

		// ensure the cached value is returned without extra fetches.
		got, err = m.ensureChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got.VChans)
		assert.Equal(t, int32(1), called.Load())
	})

	t.Run("propagate error", func(t *testing.T) {
		expErr := errors.New("mock")
		m := &channelsMgrImpl{
			infos: make(map[typeutil.UniqueID]streamInfos),
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				return ChannelInfo{}, expErr
			},
		}
		_, err := m.ensureChannels(1)
		assert.ErrorIs(t, err, expErr)
	})
}

func Test_channelsMgrImpl_GetChannels(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &channelsMgrImpl{
			infos: map[typeutil.UniqueID]streamInfos{
				100: {channelInfo: ChannelInfo{VChans: []string{"111", "222"}, PChans: []string{"111"}}},
			},
		}
		got, err := m.GetChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &channelsMgrImpl{
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				return ChannelInfo{}, errors.New("mock")
			},
		}
		_, err := m.GetChannels(100)
		assert.Error(t, err)
	})
}

func Test_channelsMgrImpl_GetVChannels(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		m := &channelsMgrImpl{
			infos: map[typeutil.UniqueID]streamInfos{
				100: {channelInfo: ChannelInfo{VChans: []string{"111", "222"}, PChans: []string{"111"}}},
			},
		}
		got, err := m.GetVChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got)
	})

	t.Run("error case", func(t *testing.T) {
		m := &channelsMgrImpl{
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				return ChannelInfo{}, errors.New("mock")
			},
		}
		_, err := m.GetVChannels(100)
		assert.Error(t, err)
	})
}

func Test_channelsMgrImpl_RemoveStream(t *testing.T) {
	m := &channelsMgrImpl{
		infos: map[typeutil.UniqueID]streamInfos{
			100: {},
		},
	}
	m.RemoveStream(100)
}
