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

package querycoord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

type ChannelCleaner struct {
	ctx      context.Context
	cancel   context.CancelFunc
	kvClient *etcdkv.EtcdKV
	factory  msgstream.Factory

	taskMutex sync.RWMutex // mutex for channelInfos, since container/list is not goroutine-safe
	// nodeID, UnsubscribeChannelInfo
	tasks  map[int64]*querypb.UnsubscribeChannelInfo
	notify chan struct{}
	closed bool

	wg sync.WaitGroup
}

// newChannelUnsubscribeHandler create a new handler service to unsubscribe channels
func NewChannelCleaner(ctx context.Context, kv *etcdkv.EtcdKV, factory dependency.Factory) (*ChannelCleaner, error) {
	childCtx, cancel := context.WithCancel(ctx)
	handler := &ChannelCleaner{
		ctx:      childCtx,
		cancel:   cancel,
		kvClient: kv,
		factory:  factory,

		tasks:  make(map[int64]*querypb.UnsubscribeChannelInfo, 1024),
		notify: make(chan struct{}, 1024),
	}

	err := handler.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return handler, nil
}

// reloadFromKV reload unsolved channels to unsubscribe
func (cleaner *ChannelCleaner) reloadFromKV() error {
	log.Info("start reload unsubscribe channelInfo from kv")
	cleaner.taskMutex.Lock()
	defer cleaner.taskMutex.Unlock()
	_, channelInfoValues, err := cleaner.kvClient.LoadWithPrefix(unsubscribeChannelInfoPrefix)
	if err != nil {
		return err
	}
	for _, value := range channelInfoValues {
		info := &querypb.UnsubscribeChannelInfo{}
		err = proto.Unmarshal([]byte(value), info)
		if err != nil {
			return err
		}
		cleaner.tasks[info.NodeID] = info
	}
	cleaner.notify <- struct{}{}
	log.Info("successufully reload unsubscribe channelInfo from kv", zap.Int("unhandled", len(channelInfoValues)))
	return nil
}

// addUnsubscribeChannelInfo add channel info to handler service, and persistent to etcd
func (cleaner *ChannelCleaner) addUnsubscribeChannelInfo(info *querypb.UnsubscribeChannelInfo) {
	if len(info.CollectionChannels) == 0 {
		return
	}
	nodeID := info.NodeID
	cleaner.taskMutex.Lock()
	defer cleaner.taskMutex.Unlock()
	if cleaner.closed {
		return
	}

	_, ok := cleaner.tasks[nodeID]
	if ok {
		log.Info("duplicate add unsubscribe channel, ignore..", zap.Int64("nodeID", nodeID))
		return
	}

	channelInfoValue, err := proto.Marshal(info)
	if err != nil {
		panic(err)
	}

	//TODO, we don't even need unsubscribeChannelInfoPrefix, each time we just call addUnsubscribeChannelInfo when querycoord restard
	channelInfoKey := fmt.Sprintf("%s/%d", unsubscribeChannelInfoPrefix, nodeID)
	err = cleaner.kvClient.Save(channelInfoKey, string(channelInfoValue))
	if err != nil {
		panic(err)
	}
	cleaner.tasks[info.NodeID] = info
	cleaner.notify <- struct{}{}
	log.Info("successfully add unsubscribeChannelInfo to handler", zap.Int64("nodeID", info.NodeID), zap.Any("channels", info.CollectionChannels))
}

// handleChannelUnsubscribeLoop handle the unsubscription of channels which query node has watched
func (cleaner *ChannelCleaner) handleChannelCleanLoop() {
	defer cleaner.wg.Done()

	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	for {
		select {
		case <-cleaner.ctx.Done():
			log.Info("channelUnsubscribeHandler ctx done, handleChannelCleanLoop end")
			return
		case _, ok := <-cleaner.notify:
			if ok {
				cleaner.taskMutex.Lock()
				for segmentID := range cleaner.tasks {
					cleaner.process(segmentID)
				}
				cleaner.taskMutex.Unlock()
			}
		case <-ticker.C:
			cleaner.taskMutex.Lock()
			for segmentID := range cleaner.tasks {
				cleaner.process(segmentID)
			}
			cleaner.taskMutex.Unlock()
		}
	}
}

func (cleaner *ChannelCleaner) process(nodeID int64) error {
	log.Info("start to handle channel clean", zap.Int64("nodeID", nodeID))
	channelInfo := cleaner.tasks[nodeID]
	for _, collectionChannels := range channelInfo.CollectionChannels {
		collectionID := collectionChannels.CollectionID
		subName := funcutil.GenChannelSubName(Params.CommonCfg.QueryNodeSubName, collectionID, nodeID)
		// should be ok if we call unsubscribe multiple times
		msgstream.UnsubscribeChannels(cleaner.ctx, cleaner.factory, subName, collectionChannels.Channels)
	}
	channelInfoKey := fmt.Sprintf("%s/%d", unsubscribeChannelInfoPrefix, nodeID)
	err := cleaner.kvClient.Remove(channelInfoKey)
	if err != nil {
		log.Warn("remove unsubscribe channelInfo from etcd failed", zap.Int64("nodeID", nodeID))
		return err
	}
	delete(cleaner.tasks, nodeID)
	log.Info("unsubscribe channels success", zap.Int64("nodeID", nodeID))
	return nil
}

// check if there exists any unsubscribe task for specified channel
func (cleaner *ChannelCleaner) isNodeChannelCleanHandled(nodeID UniqueID) bool {
	cleaner.taskMutex.RLock()
	defer cleaner.taskMutex.RUnlock()
	_, ok := cleaner.tasks[nodeID]
	return !ok
}

func (cleaner *ChannelCleaner) start() {
	cleaner.wg.Add(1)
	go cleaner.handleChannelCleanLoop()
}

func (cleaner *ChannelCleaner) close() {
	cleaner.taskMutex.Lock()
	cleaner.closed = true
	close(cleaner.notify)
	cleaner.taskMutex.Unlock()
	cleaner.cancel()
	cleaner.wg.Wait()
}
