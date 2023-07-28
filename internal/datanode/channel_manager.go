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
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ChannelManager struct {
	dn *DataNode

	guard           sync.RWMutex
	opInQueue       []*datapb.ChannelWatchInfo
	watchInProgress map[string]*tickler // channel name -> tickler

	inProgressWaiter sync.WaitGroup

	runningFlowgraphs *flowgraphManager

	closeCh     chan struct{}
	closeOnce   sync.Once
	closeWaiter sync.WaitGroup
}

func NewChannelManager(dn *DataNode) *ChannelManager {
	return &ChannelManager{
		dn:                dn,
		runningFlowgraphs: newFlowgraphManager(),

		opInQueue:       make([]*datapb.ChannelWatchInfo, 10),
		watchInProgress: make(map[string]*tickler),
		closeCh:         make(chan struct{}),
	}
}

func (m *ChannelManager) Start() {
	m.closeWaiter.Add(2)

	go m.runningFlowgraphs.start(&m.closeWaiter)
	go func() {
		defer m.closeWaiter.Done()

		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		log.Info("ChannelManager start")
		for {
			select {
			case <-ticker.C:
				m.Execute()
			case <-m.closeCh:
				log.Info("DataNode ChannelManager exit")
				return
			}
		}
	}()
}

func (m *ChannelManager) Close() {
	m.guard.Lock()
	for _, tk := range m.watchInProgress {
		tk.close()
	}
	m.inProgressWaiter.Wait()
	m.guard.Unlock()

	m.runningFlowgraphs.close()
	m.closeOnce.Do(func() {
		close(m.closeCh)
		m.closeWaiter.Wait()
	})
}

func (m *ChannelManager) Enqueue(info *datapb.ChannelWatchInfo) {
	m.guard.Lock()
	defer m.guard.Unlock()

	channelName := info.GetVchan().GetChannelName()
	if m.shouldEnqueue(channelName, info.GetState()) {
		m.opInQueue = append(m.opInQueue, info)
	}
}

func (m *ChannelManager) shouldEnqueue(channel string, state datapb.ChannelWatchState) bool {
	if state == datapb.ChannelWatchState_ToWatch &&
		!m.hasOp(channel, state) &&
		!m.runningFlowgraphs.exist(channel) {
		return true
	}

	if state == datapb.ChannelWatchState_ToRelease && m.runningFlowgraphs.exist(channel) {
		return true
	}

	log.Warn("Channel operation exist or done, skip enqueue",
		zap.String("channel", channel),
		zap.String("operation", state.String()))
	return false
}

func (m *ChannelManager) hasOp(channel string, state datapb.ChannelWatchState) bool {
	if _, ok := m.watchInProgress[channel]; ok && state == datapb.ChannelWatchState_ToWatch {
		return true
	}

	for _, info := range m.opInQueue {
		if info.GetVchan().GetChannelName() == channel && state == info.GetState() {
			return true
		}
	}
	return false
}

func (m *ChannelManager) Execute() {
	m.guard.Lock()
	defer m.guard.Unlock()

	info := m.pop()
	if info == nil {
		return
	}

	if info.GetState() == datapb.ChannelWatchState_ToWatch {
		tickler := newTickler()
		m.inProgressWaiter.Add(1)
		m.watchInProgress[info.GetVchan().GetChannelName()] = tickler
		go m.ExecuteWatchOperation(info, tickler)
		return
	}

	if info.GetState() == datapb.ChannelWatchState_ToRelease {
		go m.ExecuteReleaseOperation(info)
		return
	}

	log.Warn("Unknown channel operation, skip.")
}

func (m *ChannelManager) pop() *datapb.ChannelWatchInfo {
	if len(m.opInQueue) == 0 {
		return nil
	}

	info := m.opInQueue[0]
	m.opInQueue = m.opInQueue[1:len(m.opInQueue):len(m.opInQueue)]
	return info
}

func (m *ChannelManager) ExecuteWatchOperation(info *datapb.ChannelWatchInfo, tickler *tickler) {
	log.Info("Execute channel watch operation", zap.String("channel", info.GetVchan().GetChannelName()))
	defer m.inProgressWaiter.Done()
	dataSyncService := getDataSyncService(m.dn, info, tickler)
	if dataSyncService == nil {
		return
	}

	dataSyncService.start()
	tickler.close()

	m.guard.Lock()
	delete(m.watchInProgress, info.GetVchan().GetChannelName())
	m.runningFlowgraphs.Add(dataSyncService)
	m.guard.Unlock()

	metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
}

func getDataSyncService(node *DataNode, info *datapb.ChannelWatchInfo, tickler *tickler) *dataSyncService {
	channelName := info.GetVchan().GetChannelName()
	log := log.With(zap.String("channel", channelName))

	channel := newChannel(
		info.GetVchan().GetChannelName(),
		info.GetVchan().GetCollectionID(),
		info.GetSchema(),
		node.rootCoord,
		node.chunkManager,
	)

	dataSyncService, err := newDataSyncService(
		node.ctx,
		make(chan flushMsg, 100),
		make(chan resendTTMsg, 100),
		channel,
		node.allocator,
		node.dispClient,
		node.factory,
		info.GetVchan(),
		node.clearSignal,
		node.dataCoord,
		node.segmentCache,
		node.chunkManager,
		node.compactionExecutor,
		tickler,
		node.GetSession().ServerID,
		node.timeTickSender,
	)
	if err != nil {
		log.Warn("fail to create new datasyncservice", zap.Error(err))
		return nil
	}

	return dataSyncService
}

func (m *ChannelManager) ExecuteReleaseOperation(info *datapb.ChannelWatchInfo) {
	m.guard.Lock()
	defer m.guard.Unlock()
	log.Info("Execute channel release operation", zap.String("channel", info.GetVchan().GetChannelName()))
	channel := info.GetVchan().GetChannelName()
	if m.runningFlowgraphs.exist(channel) {
		m.runningFlowgraphs.release(channel)
		return
	}

	// If request release for a not running flowgraph
	if m.hasOp(channel, datapb.ChannelWatchState_ToWatch) {
		// if inProgress, stop and delete from the manager
		if t, ok := m.watchInProgress[channel]; ok {
			t.close()
			delete(m.watchInProgress, channel)
			return
		}

		// if inQueue, remove from the queue
		for i, info := range m.opInQueue {
			if info.GetVchan().GetChannelName() == channel {
				m.opInQueue = append(m.opInQueue[:i], m.opInQueue[i+1:]...)
				return
			}
		}
	}
}

func (m *ChannelManager) GetOperationProgress(channel string, state datapb.ChannelWatchState) int32 {
	m.guard.RLock()
	defer m.guard.RUnlock()
	for _, info := range m.opInQueue {
		if info.GetVchan().GetChannelName() == channel && info.GetState() == state {
			return 0
		}
	}

	if state == datapb.ChannelWatchState_ToWatch {
		if m.runningFlowgraphs.exist(channel) {
			return 100
		}

		if t, ok := m.watchInProgress[channel]; ok {
			return t.progress.Load()
		}
	}

	if state == datapb.ChannelWatchState_ToRelease {
		if !m.runningFlowgraphs.exist(channel) && !m.hasOp(channel, datapb.ChannelWatchState_ToRelease) {
			return 100
		}
	}

	return 0
}

func (m *ChannelManager) Release(channel string) {
	log.Info("try to release flowgraph", zap.String("channel", channel))
	m.runningFlowgraphs.release(channel)
}

type tickler struct {
	progress *atomic.Int32 // TODO use percentage
	interval time.Duration
	sig      *atomic.Bool
}

func (t *tickler) inc() {
	t.progress.Inc()
}

func (t *tickler) close() {
	t.sig.CompareAndSwap(false, true)
}

func (t *tickler) closed() bool {
	return t.sig.Load()
}

func newTickler() *tickler {
	return &tickler{
		progress: atomic.NewInt32(0),
		sig:      atomic.NewBool(false),
		interval: Params.DataNodeCfg.WatchEventTicklerInterval.GetAsDuration(time.Second),
	}
}
