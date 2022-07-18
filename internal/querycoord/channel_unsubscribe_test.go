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
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

func Test_HandlerReloadFromKV(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	channelInfoKey := fmt.Sprintf("%s/%d", unsubscribeChannelInfoPrefix, defaultQueryNodeID)
	unsubscribeChannelInfo := &querypb.UnsubscribeChannelInfo{
		NodeID: defaultQueryNodeID,
	}
	channelInfoBytes, err := proto.Marshal(unsubscribeChannelInfo)
	assert.Nil(t, err)

	err = kv.Save(channelInfoKey, string(channelInfoBytes))
	assert.Nil(t, err)

	factory := dependency.NewDefaultFactory(true)
	cleaner, err := NewChannelCleaner(baseCtx, kv, factory)
	assert.Nil(t, err)

	assert.False(t, cleaner.isNodeChannelCleanHandled(defaultQueryNodeID))

	cancel()
}

func Test_AddUnsubscribeChannelInfo(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	factory := dependency.NewDefaultFactory(true)
	cleaner, err := NewChannelCleaner(baseCtx, kv, factory)
	assert.Nil(t, err)

	collectionChannels := &querypb.UnsubscribeChannels{
		CollectionID: defaultCollectionID,
		Channels:     []string{"test-channel"},
	}
	unsubscribeChannelInfo := &querypb.UnsubscribeChannelInfo{
		NodeID:             defaultQueryNodeID,
		CollectionChannels: []*querypb.UnsubscribeChannels{collectionChannels},
	}

	cleaner.addUnsubscribeChannelInfo(unsubscribeChannelInfo)
	assert.Equal(t, len(cleaner.tasks), 1)

	// repeat nodeID which has down
	cleaner.addUnsubscribeChannelInfo(unsubscribeChannelInfo)
	assert.Equal(t, len(cleaner.tasks), 1)

	cancel()
}

func Test_HandleChannelUnsubscribeLoop(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	factory := dependency.NewDefaultFactory(true)
	handler, err := NewChannelCleaner(baseCtx, kv, factory)
	assert.Nil(t, err)

	collectionChannels := &querypb.UnsubscribeChannels{
		CollectionID: defaultCollectionID,
		Channels:     []string{"test-channel"},
	}
	unsubscribeChannelInfo := &querypb.UnsubscribeChannelInfo{
		NodeID:             defaultQueryNodeID,
		CollectionChannels: []*querypb.UnsubscribeChannels{collectionChannels},
	}

	handler.addUnsubscribeChannelInfo(unsubscribeChannelInfo)
	channelInfoKey := fmt.Sprintf("%s/%d", unsubscribeChannelInfoPrefix, defaultQueryNodeID)
	_, err = kv.Load(channelInfoKey)
	assert.Nil(t, err)

	handler.start()

	for {
		_, err := kv.Load(channelInfoKey)
		if err != nil {
			break
		}
	}

	cancel()
}
