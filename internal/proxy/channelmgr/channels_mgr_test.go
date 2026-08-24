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

func Test_channelsMgrImpl_GetChannels(t *testing.T) {
	t.Run("delegates to resolver", func(t *testing.T) {
		called := atomic.Int32{}
		m := &channelsMgrImpl{
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				called.Add(1)
				assert.Equal(t, typeutil.UniqueID(100), collectionID)
				return ChannelInfo{VChans: []string{"111", "222"}, PChans: []string{"p111", "p222"}}, nil
			},
		}
		got, err := m.GetChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"p111", "p222"}, got)
		// no internal cache: every call hits the resolver.
		_, err = m.GetChannels(100)
		assert.NoError(t, err)
		assert.Equal(t, int32(2), called.Load())
	})

	t.Run("propagate resolver error", func(t *testing.T) {
		expErr := errors.New("mock")
		m := &channelsMgrImpl{
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				return ChannelInfo{}, expErr
			},
		}
		_, err := m.GetChannels(100)
		assert.ErrorIs(t, err, expErr)
	})

	t.Run("reject misaligned resolver result", func(t *testing.T) {
		m := &channelsMgrImpl{
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				return ChannelInfo{VChans: []string{"111", "222"}, PChans: []string{"p111"}}, nil
			},
		}
		_, err := m.GetChannels(100)
		assert.Error(t, err)
	})
}

func Test_channelsMgrImpl_GetVChannels(t *testing.T) {
	t.Run("delegates to resolver", func(t *testing.T) {
		m := &channelsMgrImpl{
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				return ChannelInfo{VChans: []string{"111", "222"}, PChans: []string{"p111", "p222"}}, nil
			},
		}
		got, err := m.GetVChannels(100)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"111", "222"}, got)
	})

	t.Run("propagate resolver error", func(t *testing.T) {
		expErr := errors.New("mock")
		m := &channelsMgrImpl{
			getChannelsFunc: func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
				return ChannelInfo{}, expErr
			},
		}
		_, err := m.GetVChannels(100)
		assert.ErrorIs(t, err, expErr)
	})
}

func TestNewChannelsMgr(t *testing.T) {
	m := NewChannelsMgr(func(collectionID typeutil.UniqueID) (ChannelInfo, error) {
		return ChannelInfo{VChans: []string{"v"}, PChans: []string{"p"}}, nil
	})
	got, err := m.GetVChannels(100)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"v"}, got)
}
