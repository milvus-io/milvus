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
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

const (
	unsubscribeChannelInfoPrefix = "queryCoord-unsubscribeChannelInfo"
)

type channelUnsubscribeHandler struct {
	ctx      context.Context
	cancel   context.CancelFunc
	kvClient *etcdkv.EtcdKV
	factory  msgstream.Factory

	mut          sync.RWMutex // mutex for channelInfos, since container/list is not goroutine-safe
	channelInfos *list.List
	downNodeChan chan int64

	wg sync.WaitGroup
}

// newChannelUnsubscribeHandler create a new handler service to unsubscribe channels
func newChannelUnsubscribeHandler(ctx context.Context, kv *etcdkv.EtcdKV, factory dependency.Factory) (*channelUnsubscribeHandler, error) {
	childCtx, cancel := context.WithCancel(ctx)
	handler := &channelUnsubscribeHandler{
		ctx:      childCtx,
		cancel:   cancel,
		kvClient: kv,
		factory:  factory,

		channelInfos: list.New(),
		//TODO:: if the query nodes that are down exceed 1024, query coord will not be able to restart
		downNodeChan: make(chan int64, 1024),
	}

	err := handler.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return handler, nil
}

// appendUnsubInfo pushes unsub info safely
func (csh *channelUnsubscribeHandler) appendUnsubInfo(info *querypb.UnsubscribeChannelInfo) {
	csh.mut.Lock()
	defer csh.mut.Unlock()
	csh.channelInfos.PushBack(info)
}

// reloadFromKV reload unsolved channels to unsubscribe
func (csh *channelUnsubscribeHandler) reloadFromKV() error {
	log.Info("start reload unsubscribe channelInfo from kv")
	_, channelInfoValues, err := csh.kvClient.LoadWithPrefix(unsubscribeChannelInfoPrefix)
	if err != nil {
		return err
	}
	for _, value := range channelInfoValues {
		channelInfo := &querypb.UnsubscribeChannelInfo{}
		err = proto.Unmarshal([]byte(value), channelInfo)
		if err != nil {
			return err
		}
		csh.appendUnsubInfo(channelInfo)
		csh.downNodeChan <- channelInfo.NodeID
	}

	return nil
}

// addUnsubscribeChannelInfo add channel info to handler service, and persistent to etcd
func (csh *channelUnsubscribeHandler) addUnsubscribeChannelInfo(info *querypb.UnsubscribeChannelInfo) {
	nodeID := info.NodeID
	channelInfoValue, err := proto.Marshal(info)
	if err != nil {
		panic(err)
	}
	// when queryCoord is restarted multiple times, the nodeID of added channelInfo may be the same
	hasEnqueue := false
	// reduce the lock range to iteration here, since `addUnsubscribeChannelInfo` is called one by one
	csh.mut.RLock()
	for e := csh.channelInfos.Back(); e != nil; e = e.Prev() {
		if e.Value.(*querypb.UnsubscribeChannelInfo).NodeID == nodeID {
			hasEnqueue = true
		}
	}
	csh.mut.RUnlock()

	if !hasEnqueue {
		channelInfoKey := fmt.Sprintf("%s/%d", unsubscribeChannelInfoPrefix, nodeID)
		err = csh.kvClient.Save(channelInfoKey, string(channelInfoValue))
		if err != nil {
			panic(err)
		}
		csh.appendUnsubInfo(info)
		csh.downNodeChan <- info.NodeID
		log.Info("add unsubscribeChannelInfo to handler", zap.Int64("nodeID", info.NodeID))
	}
}

// handleChannelUnsubscribeLoop handle the unsubscription of channels which query node has watched
func (csh *channelUnsubscribeHandler) handleChannelUnsubscribeLoop() {
	defer csh.wg.Done()
	for {
		select {
		case <-csh.ctx.Done():
			log.Info("channelUnsubscribeHandler ctx done, handleChannelUnsubscribeLoop end")
			return
		case <-csh.downNodeChan:
			csh.mut.RLock()
			e := csh.channelInfos.Front()
			channelInfo := csh.channelInfos.Front().Value.(*querypb.UnsubscribeChannelInfo)
			csh.mut.RUnlock()
			nodeID := channelInfo.NodeID
			for _, collectionChannels := range channelInfo.CollectionChannels {
				collectionID := collectionChannels.CollectionID
				subName := funcutil.GenChannelSubName(Params.CommonCfg.QueryNodeSubName, collectionID, nodeID)
				msgstream.UnsubscribeChannels(csh.ctx, csh.factory, subName, collectionChannels.Channels)
			}
			channelInfoKey := fmt.Sprintf("%s/%d", unsubscribeChannelInfoPrefix, nodeID)
			err := csh.kvClient.Remove(channelInfoKey)
			if err != nil {
				log.Error("remove unsubscribe channelInfo from etcd failed", zap.Int64("nodeID", nodeID))
				panic(err)
			}

			csh.mut.Lock()
			csh.channelInfos.Remove(e)
			csh.mut.Unlock()
			log.Info("unsubscribe channels success", zap.Int64("nodeID", nodeID))
		}
	}
}

func (csh *channelUnsubscribeHandler) start() {
	csh.wg.Add(1)
	go csh.handleChannelUnsubscribeLoop()
}

func (csh *channelUnsubscribeHandler) close() {
	csh.cancel()
	csh.wg.Wait()
}
