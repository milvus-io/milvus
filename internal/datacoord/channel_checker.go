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
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

type channelStateTimer struct {
	watchkv kv.WatchKV

	runningTimers     sync.Map
	runningTimerStops sync.Map // channel name to timer stop channels
	etcdWatcher       clientv3.WatchChan
	timeoutWatcher    chan *ackEvent
	//Modifies afterwards must guarantee that runningTimerCount is updated synchronized with runningTimers
	//in order to keep consistency
	runningTimerCount atomic.Int32
}

func newChannelStateTimer(kv kv.WatchKV) *channelStateTimer {
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

func (c *channelStateTimer) getWatchersWithRevision(prefix string, revision int64) (clientv3.WatchChan, chan *ackEvent) {
	c.etcdWatcher = c.watchkv.WatchWithRevision(prefix, revision)
	return c.etcdWatcher, c.timeoutWatcher
}

func (c *channelStateTimer) loadAllChannels(nodeID UniqueID) ([]*datapb.ChannelWatchInfo, error) {
	prefix := path.Join(Params.CommonCfg.DataCoordWatchSubPath.GetValue(), strconv.FormatInt(nodeID, 10))

	// TODO: change to LoadWithPrefixBytes
	keys, values, err := c.watchkv.LoadWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	var ret []*datapb.ChannelWatchInfo

	for i, k := range keys {
		watchInfo, err := parseWatchInfo(k, []byte(values[i]))
		if err != nil {
			// TODO: delete this kv later
			log.Warn("invalid watchInfo loaded", zap.Error(err))
			continue
		}

		ret = append(ret, watchInfo)
	}

	return ret, nil
}

// startOne can write ToWatch or ToRelease states.
func (c *channelStateTimer) startOne(watchState datapb.ChannelWatchState, channelName string, nodeID UniqueID, timeout time.Duration) {
	if timeout == 0 {
		log.Info("zero timeoutTs, skip starting timer",
			zap.String("watch state", watchState.String()),
			zap.Int64("nodeID", nodeID),
			zap.String("channelName", channelName),
		)
		return
	}

	stop := make(chan struct{})
	ticker := time.NewTimer(timeout)
	c.removeTimers([]string{channelName})
	c.runningTimerStops.Store(channelName, stop)
	c.runningTimers.Store(channelName, ticker)
	c.runningTimerCount.Inc()
	go func() {
		log.Info("timer started",
			zap.String("watch state", watchState.String()),
			zap.Int64("nodeID", nodeID),
			zap.String("channelName", channelName),
			zap.Duration("check interval", timeout))
		defer ticker.Stop()

		select {
		case <-ticker.C:
			// check tickle at path as :tickle/[prefix]/{channel_name}
			c.removeTimers([]string{channelName})
			log.Warn("timeout and stop timer: wait for channel ACK timeout",
				zap.String("watch state", watchState.String()),
				zap.Int64("nodeID", nodeID),
				zap.String("channelName", channelName),
				zap.Duration("timeout interval", timeout),
				zap.Int32("runningTimerCount", c.runningTimerCount.Load()))
			ackType := getAckType(watchState)
			c.notifyTimeoutWatcher(&ackEvent{ackType, channelName, nodeID})
			return
		case <-stop:
			log.Info("stop timer before timeout",
				zap.String("watch state", watchState.String()),
				zap.Int64("nodeID", nodeID),
				zap.String("channelName", channelName),
				zap.Duration("timeout interval", timeout),
				zap.Int32("runningTimerCount", c.runningTimerCount.Load()))
			return
		}
	}()
}

func (c *channelStateTimer) notifyTimeoutWatcher(e *ackEvent) {
	c.timeoutWatcher <- e
}

func (c *channelStateTimer) removeTimers(channels []string) {
	for _, channel := range channels {
		if stop, ok := c.runningTimerStops.LoadAndDelete(channel); ok {
			close(stop.(chan struct{}))
			c.runningTimers.Delete(channel)
			c.runningTimerCount.Dec()
			log.Info("remove timer for channel", zap.String("channel", channel),
				zap.Int32("timerCount", c.runningTimerCount.Load()))
		}
	}
}

func (c *channelStateTimer) stopIfExist(e *ackEvent) {
	stop, ok := c.runningTimerStops.LoadAndDelete(e.channelName)
	if ok && e.ackType != watchTimeoutAck && e.ackType != releaseTimeoutAck {
		close(stop.(chan struct{}))
		c.runningTimers.Delete(e.channelName)
		c.runningTimerCount.Dec()
		log.Info("stop timer for channel", zap.String("channel", e.channelName),
			zap.Int32("timerCount", c.runningTimerCount.Load()))
	}
}

func (c *channelStateTimer) resetIfExist(channel string, interval time.Duration) {
	if value, ok := c.runningTimers.Load(channel); ok {
		timer := value.(*time.Timer)
		timer.Reset(interval)
	}
}

// Note here the reading towards c.running are not protected by mutex
// because it's meaningless, since we cannot guarantee the following add/delete node operations
func (c *channelStateTimer) hasRunningTimers() bool {
	return c.runningTimerCount.Load() != 0
}

func parseWatchInfo(key string, data []byte) (*datapb.ChannelWatchInfo, error) {
	watchInfo := datapb.ChannelWatchInfo{}
	if err := proto.Unmarshal(data, &watchInfo); err != nil {
		return nil, fmt.Errorf("invalid event data: fail to parse ChannelWatchInfo, key: %s, err: %v", key, err)

	}

	if watchInfo.Vchan == nil {
		return nil, fmt.Errorf("invalid event: ChannelWatchInfo with nil VChannelInfo, key: %s", key)
	}
	reviseVChannelInfo(watchInfo.GetVchan())

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
