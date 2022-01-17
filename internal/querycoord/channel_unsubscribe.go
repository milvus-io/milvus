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
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

const (
	unsubscribeChannelInfoPrefix    = "queryCoord-unsubscribeChannelInfo"
	unsubscribeChannelCheckInterval = time.Second
)

type channelUnsubscribeHandler struct {
	ctx      context.Context
	cancel   context.CancelFunc
	kvClient *etcdkv.EtcdKV
	factory  msgstream.Factory

	channelInfos *list.List
	downNodeChan chan int64

	wg sync.WaitGroup
}

// newChannelUnsubscribeHandler create a new handler service to unsubscribe channels
func newChannelUnsubscribeHandler(ctx context.Context, kv *etcdkv.EtcdKV, factory msgstream.Factory) (*channelUnsubscribeHandler, error) {
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

// reloadFromKV reload unsolved channels to unsubscribe
func (csh *channelUnsubscribeHandler) reloadFromKV() error {
	log.Debug("start reload unsubscribe channelInfo from kv")
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
		csh.channelInfos.PushBack(channelInfo)
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
	for e := csh.channelInfos.Back(); e != nil; e = e.Prev() {
		if e.Value.(*querypb.UnsubscribeChannelInfo).NodeID == nodeID {
			hasEnqueue = true
		}
	}

	if !hasEnqueue {
		channelInfoKey := fmt.Sprintf("%s/%d", unsubscribeChannelInfoPrefix, nodeID)
		err = csh.kvClient.Save(channelInfoKey, string(channelInfoValue))
		if err != nil {
			panic(err)
		}
		csh.channelInfos.PushBack(info)
		csh.downNodeChan <- info.NodeID
		log.Debug("add unsubscribeChannelInfo to handler", zap.Int64("nodeID", info.NodeID))
	}
}

// handleChannelUnsubscribeLoop handle the unsubscription of channels which query node has watched
func (csh *channelUnsubscribeHandler) handleChannelUnsubscribeLoop() {
	defer csh.wg.Done()
	for {
		select {
		case <-csh.ctx.Done():
			log.Debug("channelUnsubscribeHandler ctx done, handleChannelUnsubscribeLoop end")
			return
		case <-csh.downNodeChan:
			channelInfo := csh.channelInfos.Front().Value.(*querypb.UnsubscribeChannelInfo)
			nodeID := channelInfo.NodeID
			for _, collectionChannels := range channelInfo.CollectionChannels {
				collectionID := collectionChannels.CollectionID
				subName := funcutil.GenChannelSubName(Params.QueryNodeCfg.MsgChannelSubName, collectionID, nodeID)
				err := unsubscribeChannels(csh.ctx, csh.factory, subName, collectionChannels.Channels)
				if err != nil {
					log.Debug("unsubscribe channels failed", zap.Int64("nodeID", nodeID))
					panic(err)
				}
			}

			channelInfoKey := fmt.Sprintf("%s/%d", unsubscribeChannelInfoPrefix, nodeID)
			err := csh.kvClient.Remove(channelInfoKey)
			if err != nil {
				log.Error("remove unsubscribe channelInfo from etcd failed", zap.Int64("nodeID", nodeID))
				panic(err)
			}
			log.Debug("unsubscribe channels success", zap.Int64("nodeID", nodeID))
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

// unsubscribeChannels create consumer fist, and unsubscribe channel through msgStream.close()
func unsubscribeChannels(ctx context.Context, factory msgstream.Factory, subName string, channels []string) error {
	msgStream, err := factory.NewMsgStream(ctx)
	if err != nil {
		return err
	}

	msgStream.AsConsumer(channels, subName)
	msgStream.Close()
	return nil
}
