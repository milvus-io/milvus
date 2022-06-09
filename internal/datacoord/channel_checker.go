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
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type channelStateTimer struct {
	watchkv        kv.MetaKv
	runningTimers  sync.Map // channel name to timer stop channels
	etcdWatcher    clientv3.WatchChan
	timeoutWatcher chan *ackEvent
}

func newChannelStateTimer(kv kv.MetaKv) *channelStateTimer {
	return &channelStateTimer{
		watchkv:        kv,
		timeoutWatcher: make(chan *ackEvent, 20),
	}
}

func (c *channelStateTimer) getWatchers(prefix string) (clientv3.WatchChan, chan *ackEvent) {
	if c.etcdWatcher == nil {
		c.etcdWatcher = c.watchkv.WatchWithPrefix(prefix)

	}
	return c.etcdWatcher, c.timeoutWatcher
}

// startOne can write ToWatch or ToRelease states.
func (c *channelStateTimer) startOne(watchState datapb.ChannelWatchState, channelName string, nodeID UniqueID, timeoutTs int64) {
	if timeoutTs == 0 {
		log.Debug("zero timeoutTs, skip starting timer",
			zap.String("watch state", watchState.String()),
			zap.Int64("nodeID", nodeID),
			zap.String("channel name", channelName),
		)
		return
	}
	stop := make(chan struct{})
	c.runningTimers.Store(channelName, stop)
	timeoutT := time.Unix(0, timeoutTs)
	go func() {
		log.Debug("timer started",
			zap.String("watch state", watchState.String()),
			zap.Int64("nodeID", nodeID),
			zap.String("channel name", channelName),
			zap.Time("timeout time", timeoutT))
		select {
		case <-time.NewTimer(time.Until(timeoutT)).C:
			log.Info("timeout and stop timer: wait for channel ACK timeout",
				zap.String("watch state", watchState.String()),
				zap.Int64("nodeID", nodeID),
				zap.String("channel name", channelName),
				zap.Time("timeout time", timeoutT))
			ackType := getAckType(watchState)
			c.notifyTimeoutWatcher(&ackEvent{ackType, channelName, nodeID})
		case <-stop:
			log.Debug("stop timer before timeout",
				zap.String("watch state", watchState.String()),
				zap.Int64("nodeID", nodeID),
				zap.String("channel name", channelName),
				zap.Time("timeout time", timeoutT))
		}
	}()
}

func (c *channelStateTimer) notifyTimeoutWatcher(e *ackEvent) {
	c.timeoutWatcher <- e
}

func (c *channelStateTimer) removeTimers(channels []string) {
	for _, channel := range channels {
		if stop, ok := c.runningTimers.LoadAndDelete(channel); ok {
			close(stop.(chan struct{}))
		}
	}
}

func (c *channelStateTimer) stopIfExsit(e *ackEvent) {
	stop, ok := c.runningTimers.LoadAndDelete(e.channelName)
	if ok && e.ackType != watchTimeoutAck && e.ackType != releaseTimeoutAck {
		close(stop.(chan struct{}))
	}
}

func parseWatchInfo(key string, data []byte) (*datapb.ChannelWatchInfo, error) {
	watchInfo := datapb.ChannelWatchInfo{}
	if err := proto.Unmarshal(data, &watchInfo); err != nil {
		return nil, fmt.Errorf("invalid event data: fail to parse ChannelWatchInfo, key: %s, err: %v", key, err)

	}
	if watchInfo.Vchan == nil {
		return nil, fmt.Errorf("invalid event: ChannelWatchInfo with nil VChannelInfo, key: %s", key)
	}

	return &watchInfo, nil
}

// parseAckEvent transfers key-values from etcd into ackEvent
func parseAckEvent(nodeID UniqueID, info *datapb.ChannelWatchInfo) *ackEvent {
	ret := &ackEvent{
		ackType:     getAckType(info.GetState()),
		channelName: info.GetVchan().GetChannelName(),
		nodeID:      nodeID,
	}
	return ret
}

func getAckType(state datapb.ChannelWatchState) ackType {
	switch state {
	case datapb.ChannelWatchState_WatchSuccess, datapb.ChannelWatchState_Complete:
		return watchSuccessAck
	case datapb.ChannelWatchState_WatchFailure:
		return watchFailAck
	case datapb.ChannelWatchState_ReleaseSuccess:
		return releaseSuccessAck
	case datapb.ChannelWatchState_ReleaseFailure:
		return releaseFailAck
	case datapb.ChannelWatchState_ToWatch, datapb.ChannelWatchState_Uncomplete: // unchange watch states generates timeout acks
		return watchTimeoutAck
	case datapb.ChannelWatchState_ToRelease: // unchange watch states generates timeout acks
		return releaseTimeoutAck
	default:
		return invalidAck
	}
}
