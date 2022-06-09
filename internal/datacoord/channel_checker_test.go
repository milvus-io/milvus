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

package datacoord

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/golang/protobuf/proto"
)

func TestChannelStateTimer(t *testing.T) {
	kv := getMetaKv(t)
	defer kv.Close()

	prefix := Params.DataCoordCfg.ChannelWatchSubPath

	t.Run("test getWatcher", func(t *testing.T) {
		timer := newChannelStateTimer(kv)

		etcdCh, timeoutCh := timer.getWatchers(prefix)
		assert.NotNil(t, etcdCh)
		assert.NotNil(t, timeoutCh)

		timer.getWatchers(prefix)
		assert.NotNil(t, etcdCh)
		assert.NotNil(t, timeoutCh)
	})

	t.Run("test startOne", func(t *testing.T) {
		normalTimeoutTs := time.Now().Add(20 * time.Second).UnixNano()
		nowTimeoutTs := time.Now().UnixNano()
		zeroTimeoutTs := int64(0)
		tests := []struct {
			channelName string
			timeoutTs   int64

			description string
		}{
			{"channel-1", normalTimeoutTs, "test stop"},
			{"channel-2", nowTimeoutTs, "test timeout"},
			{"channel-3", zeroTimeoutTs, "not start"},
		}

		timer := newChannelStateTimer(kv)

		_, timeoutCh := timer.getWatchers(prefix)

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				timer.startOne(datapb.ChannelWatchState_ToWatch, test.channelName, 1, test.timeoutTs)
				if test.timeoutTs == nowTimeoutTs {
					e := <-timeoutCh
					assert.Equal(t, watchTimeoutAck, e.ackType)
					assert.Equal(t, test.channelName, e.channelName)
				} else {
					timer.stopIfExsit(&ackEvent{watchSuccessAck, test.channelName, 1})
				}
			})
		}

		timer.startOne(datapb.ChannelWatchState_ToWatch, "channel-remove", 1, normalTimeoutTs)
		timer.removeTimers([]string{"channel-remove"})
	})
}

func TestChannelStateTimer_parses(t *testing.T) {
	const (
		ValidTest   = true
		InValidTest = false
	)

	t.Run("test parseWatchInfo", func(t *testing.T) {
		validWatchInfo := datapb.ChannelWatchInfo{
			Vchan:     &datapb.VchannelInfo{},
			StartTs:   time.Now().Unix(),
			State:     datapb.ChannelWatchState_ToWatch,
			TimeoutTs: time.Now().Add(20 * time.Millisecond).UnixNano(),
		}
		validData, err := proto.Marshal(&validWatchInfo)
		require.NoError(t, err)

		invalidDataUnableToMarshal := []byte("invalidData")

		invalidWatchInfoNilVchan := validWatchInfo
		invalidWatchInfoNilVchan.Vchan = nil
		invalidDataNilVchan, err := proto.Marshal(&invalidWatchInfoNilVchan)
		require.NoError(t, err)

		tests := []struct {
			inKey  string
			inData []byte

			isValid     bool
			description string
		}{
			{"key", validData, ValidTest, "test with valid watchInfo"},
			{"key", invalidDataUnableToMarshal, InValidTest, "test with watchInfo unable to marshal"},
			{"key", invalidDataNilVchan, InValidTest, "test with watchInfo with nil Vchan"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				info, err := parseWatchInfo(test.inKey, test.inData)
				if test.isValid {
					assert.NoError(t, err)
					assert.NotNil(t, info)
					assert.Equal(t, info.GetState(), validWatchInfo.GetState())
					assert.Equal(t, info.GetStartTs(), validWatchInfo.GetStartTs())
					assert.Equal(t, info.GetTimeoutTs(), validWatchInfo.GetTimeoutTs())
				} else {
					assert.Nil(t, info)
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("test getAckType", func(t *testing.T) {
		tests := []struct {
			inState    datapb.ChannelWatchState
			outAckType ackType
		}{
			{datapb.ChannelWatchState_WatchSuccess, watchSuccessAck},
			{datapb.ChannelWatchState_WatchFailure, watchFailAck},
			{datapb.ChannelWatchState_ToWatch, watchTimeoutAck},
			{datapb.ChannelWatchState_Uncomplete, watchTimeoutAck},
			{datapb.ChannelWatchState_ReleaseSuccess, releaseSuccessAck},
			{datapb.ChannelWatchState_ReleaseFailure, releaseFailAck},
			{datapb.ChannelWatchState_ToRelease, releaseTimeoutAck},
			{100, invalidAck},
		}

		for _, test := range tests {
			assert.Equal(t, test.outAckType, getAckType(test.inState))
		}

	})
}
