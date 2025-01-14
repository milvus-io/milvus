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
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/logutil"
)

const retryWatchInterval = 20 * time.Second

func (node *DataNode) startWatchChannelsAtBackground(ctx context.Context) {
	node.stopWaiter.Add(1)
	go node.StartWatchChannels(ctx)
}

// StartWatchChannels start loop to watch channel allocation status via kv(etcd for now)
func (node *DataNode) StartWatchChannels(ctx context.Context) {
	defer node.stopWaiter.Done()
	defer logutil.LogPanic()
	// REF MEP#7 watch path should be [prefix]/channel/{node_id}/{channel_name}
	// TODO, this is risky, we'd better watch etcd with revision rather simply a path
	watchPrefix := path.Join(Params.CommonCfg.DataCoordWatchSubPath.GetValue(), fmt.Sprintf("%d", node.GetSession().ServerID))
	log.Info("Start watch channel", zap.String("prefix", watchPrefix))
	evtChan := node.watchKv.WatchWithPrefix(watchPrefix)
	// after watch, first check all exists nodes first
	err := node.checkWatchedList()
	if err != nil {
		log.Warn("StartWatchChannels failed", zap.Error(err))
		return
	}
	for {
		select {
		case <-ctx.Done():
			log.Info("watch etcd loop quit")
			return
		case event, ok := <-evtChan:
			if !ok {
				log.Warn("datanode failed to watch channel, return")
				node.startWatchChannelsAtBackground(ctx)
				return
			}

			if err := event.Err(); err != nil {
				log.Warn("datanode watch channel canceled", zap.Error(event.Err()))
				// https://github.com/etcd-io/etcd/issues/8980
				if event.Err() == v3rpc.ErrCompacted {
					node.startWatchChannelsAtBackground(ctx)
					return
				}
				// if watch loop return due to event canceled, the datanode is not functional anymore
				log.Panic("datanode is not functional for event canceled", zap.Error(err))
				return
			}
			for _, evt := range event.Events {
				// We need to stay in order until events enqueued
				node.handleChannelEvt(evt)
			}
		}
	}
}

// checkWatchedList list all nodes under [prefix]/channel/{node_id} and make sure all nodes are watched
// serves the corner case for etcd connection lost and missing some events
func (node *DataNode) checkWatchedList() error {
	// REF MEP#7 watch path should be [prefix]/channel/{node_id}/{channel_name}
	prefix := path.Join(Params.CommonCfg.DataCoordWatchSubPath.GetValue(), fmt.Sprintf("%d", node.GetNodeID()))
	keys, values, err := node.watchKv.LoadWithPrefix(prefix)
	if err != nil {
		return err
	}
	for i, val := range values {
		node.handleWatchInfo(&event{eventType: putEventType}, keys[i], []byte(val))
	}
	return nil
}

func (node *DataNode) handleWatchInfo(e *event, key string, data []byte) {
	switch e.eventType {
	case putEventType:
		watchInfo, err := parsePutEventData(data)
		if err != nil {
			log.Warn("fail to handle watchInfo", zap.Int("event type", e.eventType), zap.String("key", key), zap.Error(err))
			return
		}

		if isEndWatchState(watchInfo.State) {
			log.Info("DataNode received a PUT event with an end State", zap.String("state", watchInfo.State.String()))
			return
		}

		if watchInfo.Progress != 0 {
			log.Info("DataNode received a PUT event with tickler update progress", zap.String("channel", watchInfo.Vchan.ChannelName), zap.Int64("version", e.version))
			return
		}

		e.info = watchInfo
		e.vChanName = watchInfo.GetVchan().GetChannelName()
		log.Info("DataNode is handling watchInfo PUT event", zap.String("key", key), zap.String("watch state", watchInfo.GetState().String()))
	case deleteEventType:
		e.vChanName = parseDeleteEventKey(key)
		log.Info("DataNode is handling watchInfo DELETE event", zap.String("key", key))
	}

	actualManager := node.eventManager.GetOrInsert(e.vChanName, newChannelEventManager(
		node.handlePutEvent, node.handleDeleteEvent, retryWatchInterval,
	))

	actualManager.handleEvent(*e)

	// Whenever a delete event comes, this eventManager will be removed from map
	if e.eventType == deleteEventType {
		node.eventManager.Remove(e.vChanName)
	}
}

func parsePutEventData(data []byte) (*datapb.ChannelWatchInfo, error) {
	watchInfo := datapb.ChannelWatchInfo{}
	err := proto.Unmarshal(data, &watchInfo)
	if err != nil {
		return nil, fmt.Errorf("invalid event data: fail to parse ChannelWatchInfo, err: %v", err)
	}

	if watchInfo.Vchan == nil {
		return nil, fmt.Errorf("invalid event: ChannelWatchInfo with nil VChannelInfo")
	}
	reviseVChannelInfo(watchInfo.GetVchan())
	return &watchInfo, nil
}

func parseDeleteEventKey(key string) string {
	parts := strings.Split(key, "/")
	vChanName := parts[len(parts)-1]
	return vChanName
}

func (node *DataNode) handlePutEvent(watchInfo *datapb.ChannelWatchInfo, version int64) (err error) {
	vChanName := watchInfo.GetVchan().GetChannelName()
	key := path.Join(Params.CommonCfg.DataCoordWatchSubPath.GetValue(), fmt.Sprintf("%d", node.GetSession().ServerID), vChanName)
	tickler := newEtcdTickler(version, key, watchInfo, node.watchKv, Params.DataNodeCfg.WatchEventTicklerInterval.GetAsDuration(time.Second))

	switch watchInfo.State {
	case datapb.ChannelWatchState_Uncomplete, datapb.ChannelWatchState_ToWatch:
		if err := node.flowgraphManager.AddandStartWithEtcdTickler(node, watchInfo.GetVchan(), watchInfo.GetSchema(), tickler); err != nil {
			log.Warn("handle put event: new data sync service failed", zap.String("vChanName", vChanName), zap.Error(err))
			watchInfo.State = datapb.ChannelWatchState_WatchFailure
		} else {
			log.Info("handle put event: new data sync service success", zap.String("vChanName", vChanName))
			watchInfo.State = datapb.ChannelWatchState_WatchSuccess
		}
	case datapb.ChannelWatchState_ToRelease:
		// there is no reason why we release fail
		node.tryToReleaseFlowgraph(vChanName)
		watchInfo.State = datapb.ChannelWatchState_ReleaseSuccess
	}

	liteInfo := GetLiteChannelWatchInfo(watchInfo)
	v, err := proto.Marshal(liteInfo)
	if err != nil {
		return fmt.Errorf("fail to marshal watchInfo with state, vChanName: %s, state: %s ,err: %w", vChanName, liteInfo.State.String(), err)
	}

	success, err := node.watchKv.CompareVersionAndSwap(key, tickler.version, string(v))
	// etcd error
	if err != nil {
		// flow graph will leak if not release, causing new datanode failed to subscribe
		node.tryToReleaseFlowgraph(vChanName)
		log.Warn("fail to update watch state to etcd", zap.String("vChanName", vChanName),
			zap.String("state", watchInfo.State.String()), zap.Error(err))
		return err
	}
	// etcd valid but the states updated.
	if !success {
		log.Info("handle put event: failed to compare version and swap, release flowgraph",
			zap.String("key", key), zap.String("state", watchInfo.State.String()),
			zap.String("vChanName", vChanName))
		// flow graph will leak if not release, causing new datanode failed to subscribe
		node.tryToReleaseFlowgraph(vChanName)
		return nil
	}
	log.Info("handle put event success", zap.String("key", key),
		zap.String("state", watchInfo.State.String()), zap.String("vChanName", vChanName))
	return nil
}

func (node *DataNode) handleDeleteEvent(vChanName string) {
	node.tryToReleaseFlowgraph(vChanName)
}

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
	handleDel func(string), retryInterval time.Duration,
) *channelEventManager {
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

type etcdTickler struct {
	progress *atomic.Int32
	version  int64

	kv        kv.WatchKV
	path      string
	watchInfo *datapb.ChannelWatchInfo

	interval      time.Duration
	closeCh       chan struct{}
	closeWg       sync.WaitGroup
	isWatchFailed *atomic.Bool
}

func (t *etcdTickler) inc() {
	t.progress.Inc()
}

func (t *etcdTickler) watch() {
	if t.interval == 0 {
		log.Info("zero interval, close ticler watch",
			zap.String("channelName", t.watchInfo.GetVchan().GetChannelName()),
		)
		return
	}

	t.closeWg.Add(1)
	go func() {
		defer t.closeWg.Done()
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
					t.isWatchFailed.Store(true)
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
					t.isWatchFailed.Store(true)
					return
				}
				log.Debug("tickler update success", zap.Int32("progress", nowProgress), zap.Int64("version", t.version),
					zap.String("vChanName", t.watchInfo.GetVchan().ChannelName))
				t.version++
			case <-t.closeCh:
				return
			}
		}
	}()
}

func (t *etcdTickler) stop() {
	close(t.closeCh)
	t.closeWg.Wait()
}

func newEtcdTickler(version int64, path string, watchInfo *datapb.ChannelWatchInfo, kv kv.WatchKV, interval time.Duration) *etcdTickler {
	liteWatchInfo := GetLiteChannelWatchInfo(watchInfo)
	return &etcdTickler{
		progress:      atomic.NewInt32(0),
		path:          path,
		kv:            kv,
		watchInfo:     liteWatchInfo,
		version:       version,
		interval:      interval,
		closeCh:       make(chan struct{}),
		isWatchFailed: atomic.NewBool(false),
	}
}

// GetLiteChannelWatchInfo clones watchInfo without segmentIDs to reduce the size of the message
func GetLiteChannelWatchInfo(watchInfo *datapb.ChannelWatchInfo) *datapb.ChannelWatchInfo {
	return &datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{
			CollectionID: watchInfo.GetVchan().GetCollectionID(),
			ChannelName:  watchInfo.GetVchan().GetChannelName(),
			SeekPosition: watchInfo.GetVchan().GetSeekPosition(),
		},
		StartTs:   watchInfo.GetStartTs(),
		State:     watchInfo.GetState(),
		TimeoutTs: watchInfo.GetTimeoutTs(),
		Schema:    watchInfo.GetSchema(),
		Progress:  watchInfo.GetProgress(),
	}
}

type EventManager struct {
	channelGuard    sync.Mutex
	channelManagers map[string]*channelEventManager
}

func NewEventManager() *EventManager {
	return &EventManager{
		channelManagers: make(map[string]*channelEventManager),
	}
}

func (m *EventManager) GetOrInsert(channel string, newManager *channelEventManager) *channelEventManager {
	m.channelGuard.Lock()
	defer m.channelGuard.Unlock()

	eManager, got := m.channelManagers[channel]
	if !got {
		newManager.Run()
		m.channelManagers[channel] = newManager
		return newManager
	}

	return eManager
}

func (m *EventManager) Remove(channel string) {
	m.channelGuard.Lock()
	eManager, got := m.channelManagers[channel]
	delete(m.channelManagers, channel)
	m.channelGuard.Unlock()

	if got {
		eManager.Close()
	}
}

func (m *EventManager) CloseAll() {
	m.channelGuard.Lock()
	defer m.channelGuard.Unlock()

	for channel, eManager := range m.channelManagers {
		delete(m.channelManagers, channel)
		eManager.Close()
	}
}
