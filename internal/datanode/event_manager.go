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

package datanode

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

const retryWatchInterval = 20 * time.Second

type event struct {
	eventType int
	vChanName string
	version   int64
	info      *datapb.ChannelWatchInfo
}

type channelEventManager struct {
	sync.Once
	wg                sync.WaitGroup
	eventChan         chan event
	closeChan         chan struct{}
	handlePutEvent    func(watchInfo *datapb.ChannelWatchInfo, version int64) error // node.handlePutEvent
	handleDeleteEvent func(vChanName string)                                        // node.handleDeleteEvent
	retryInterval     time.Duration
}

const (
	putEventType    = 1
	deleteEventType = 2
)

func newChannelEventManager(handlePut func(*datapb.ChannelWatchInfo, int64) error,
	handleDel func(string), retryInterval time.Duration) *channelEventManager {
	return &channelEventManager{
		eventChan:         make(chan event, 10),
		closeChan:         make(chan struct{}),
		handlePutEvent:    handlePut,
		handleDeleteEvent: handleDel,
		retryInterval:     retryInterval,
	}
}

func (e *channelEventManager) Run() {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case event := <-e.eventChan:
				switch event.eventType {
				case putEventType:
					err := e.handlePutEvent(event.info, event.version)
					if err != nil {
						// logging the error is convenient for follow-up investigation of problems
						log.Warn("handle put event failed", zap.String("vChanName", event.vChanName), zap.Error(err))
					}
				case deleteEventType:
					e.handleDeleteEvent(event.vChanName)
				}
			case <-e.closeChan:
				return
			}
		}
	}()
}

func (e *channelEventManager) handleEvent(event event) {
	e.eventChan <- event
}

func (e *channelEventManager) Close() {
	e.Do(func() {
		close(e.closeChan)
		e.wg.Wait()
	})
}

func isEndWatchState(state datapb.ChannelWatchState) bool {
	return state != datapb.ChannelWatchState_ToWatch && // start watch
		state != datapb.ChannelWatchState_ToRelease && // start release
		state != datapb.ChannelWatchState_Uncomplete // legacy state, equal to ToWatch
}

type tickler struct {
	progress *atomic.Int32
	version  int64

	kv        kv.MetaKv
	path      string
	watchInfo *datapb.ChannelWatchInfo

	interval time.Duration
	closeCh  chan struct{}
	closeWg  sync.WaitGroup
}

func (t *tickler) inc() {
	t.progress.Inc()
}

func (t *tickler) watch() {
	if t.interval == 0 {
		log.Info("zero interval, close ticler watch",
			zap.String("channel name", t.watchInfo.GetVchan().GetChannelName()),
		)
		return
	}

	t.closeWg.Add(1)
	go func() {
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				nowProgress := t.progress.Load()
				if t.watchInfo.Progress == nowProgress {
					continue
				}

				t.watchInfo.Progress = nowProgress
				v, err := proto.Marshal(t.watchInfo)
				if err != nil {
					log.Error("fail to marshal watchInfo with progress at tickler",
						zap.String("vChanName", t.watchInfo.Vchan.ChannelName),
						zap.Int32("progree", nowProgress),
						zap.Error(err))
					return
				}
				success, err := t.kv.CompareVersionAndSwap(t.path, t.version, string(v))
				if err != nil {
					log.Error("tickler update failed", zap.Error(err))
					continue
				}

				if !success {
					log.Error("tickler update failed: failed to compare version and swap",
						zap.String("key", t.path), zap.Int32("progress", nowProgress), zap.Int64("version", t.version),
						zap.String("vChanName", t.watchInfo.GetVchan().ChannelName))
					return
				}
				log.Debug("tickler update success", zap.Int32("progress", nowProgress), zap.Int64("version", t.version),
					zap.String("vChanName", t.watchInfo.GetVchan().ChannelName))
				t.version++
			case <-t.closeCh:
				t.closeWg.Done()
				return
			}
		}
	}()
}

func (t *tickler) stop() {
	close(t.closeCh)
	t.closeWg.Wait()
}

func newTickler(version int64, path string, watchInfo *datapb.ChannelWatchInfo, kv kv.MetaKv, interval time.Duration) *tickler {
	return &tickler{
		progress:  atomic.NewInt32(0),
		path:      path,
		kv:        kv,
		watchInfo: watchInfo,
		version:   version,
		interval:  interval,
		closeCh:   make(chan struct{}),
	}
}
