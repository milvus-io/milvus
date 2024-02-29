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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const (
	defaultUpdateChanCPMaxParallel = 10
)

type channelCPUpdateTask struct {
	pos      *msgpb.MsgPosition
	callback func()
}

type channelCheckpointUpdater struct {
	dn         *DataNode
	workerPool *conc.Pool[any]

	mu    sync.RWMutex
	tasks map[string]*channelCPUpdateTask

	closeCh   chan struct{}
	closeOnce sync.Once
}

func newChannelCheckpointUpdater(dn *DataNode) *channelCheckpointUpdater {
	updateChanCPMaxParallel := paramtable.Get().DataNodeCfg.UpdateChannelCheckpointMaxParallel.GetAsInt()
	if updateChanCPMaxParallel <= 0 {
		updateChanCPMaxParallel = defaultUpdateChanCPMaxParallel
	}
	return &channelCheckpointUpdater{
		dn:         dn,
		workerPool: conc.NewPool[any](updateChanCPMaxParallel, conc.WithPreAlloc(true)),
		tasks:      make(map[string]*channelCPUpdateTask),
		closeCh:    make(chan struct{}),
	}
}

func (ccu *channelCheckpointUpdater) start() {
	log.Info("channel checkpoint updater start")
	ticker := time.NewTicker(paramtable.Get().DataNodeCfg.ChannelCheckpointUpdaterTick.GetAsDuration(time.Second))
	for {
		select {
		case <-ccu.closeCh:
			log.Info("channel checkpoint updater exit")
			return
		case <-ticker.C:
			ccu.execute()
		}
	}
}

func (ccu *channelCheckpointUpdater) execute() {
	ccu.mu.RLock()
	taskGroups := lo.Chunk(lo.Values(ccu.tasks), paramtable.Get().DataNodeCfg.MaxChannelCheckpointsPerRPC.GetAsInt())
	ccu.mu.RUnlock()

	futures := make([]*conc.Future[any], 0)
	for _, tasks := range taskGroups {
		tasks := tasks
		future := ccu.workerPool.Submit(func() (any, error) {
			timeout := paramtable.Get().DataNodeCfg.UpdateChannelCheckpointRPCTimeout.GetAsDuration(time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			channelCPs := lo.Map(tasks, func(t *channelCPUpdateTask, _ int) *msgpb.MsgPosition {
				return t.pos
			})
			err := ccu.dn.broker.UpdateChannelCheckpoint(ctx, channelCPs)
			if err != nil {
				return nil, err
			}
			for _, task := range tasks {
				task.callback()
			}
			ccu.mu.Lock()
			defer ccu.mu.Unlock()
			for _, task := range tasks {
				channel := task.pos.GetChannelName()
				if ccu.tasks[channel].pos.GetTimestamp() <= task.pos.GetTimestamp() {
					delete(ccu.tasks, channel)
				}
			}
			return nil, nil
		})
		futures = append(futures, future)
	}
	err := conc.AwaitAll(futures...)
	if err != nil {
		log.Warn("update channel checkpoint failed", zap.Error(err))
	}
}

func (ccu *channelCheckpointUpdater) addTask(channelPos *msgpb.MsgPosition, callback func()) {
	if channelPos == nil || channelPos.GetMsgID() == nil || channelPos.GetChannelName() == "" {
		log.Warn("illegal checkpoint", zap.Any("pos", channelPos))
		return
	}
	channel := channelPos.GetChannelName()
	ccu.mu.RLock()
	if ccu.tasks[channel] != nil && channelPos.GetTimestamp() <= ccu.tasks[channel].pos.GetTimestamp() {
		ccu.mu.RUnlock()
		return
	}
	ccu.mu.RUnlock()

	ccu.mu.Lock()
	defer ccu.mu.Unlock()
	ccu.tasks[channel] = &channelCPUpdateTask{
		pos:      channelPos,
		callback: callback,
	}
}

func (ccu *channelCheckpointUpdater) taskNum() int {
	ccu.mu.RLock()
	defer ccu.mu.RUnlock()
	return len(ccu.tasks)
}

func (ccu *channelCheckpointUpdater) close() {
	ccu.closeOnce.Do(func() {
		close(ccu.closeCh)
		ccu.workerPool.Release()
	})
}
