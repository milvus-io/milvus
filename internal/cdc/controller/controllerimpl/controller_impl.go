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

package controllerimpl

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type controller struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewController() *controller {
	ctx, cancel := context.WithCancel(context.Background())
	return &controller{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (c *controller) Start() error {
	if err := c.recoverReplicatePChannelMeta(); err != nil {
		return err
	}
	c.startWatchLoop()
	return nil
}

func (c *controller) recoverReplicatePChannelMeta() error {
	replicataMetas, err := resource.Resource().ReplicationCatalog().ListReplicatePChannels(c.ctx)
	if err != nil {
		return err
	}
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	for _, replicate := range replicataMetas {
		if !strings.Contains(replicate.GetSourceChannelName(), currentClusterID) {
			// current cluster is not source cluster, skip create replicator
			continue
		}
		log.Info("recover replicate pchannel meta",
			zap.String("sourceChannel", replicate.GetSourceChannelName()),
			zap.String("targetChannel", replicate.GetTargetChannelName()),
		)
		resource.Resource().ReplicateManagerClient().CreateReplicator(replicate)
	}
	return nil
}

func (c *controller) startWatchLoop() {
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	prefix := streamingcoord.ReplicatePChannelMetaPrefix
	eventCh := resource.Resource().WatchKV().WatchWithPrefix(c.ctx, prefix)

	c.wg.Add(1)
	go func() {
		log.Info("start to watch replicate pchannel meta", zap.String("prefix", prefix))
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			case event, ok := <-eventCh:
				if !ok {
					panic("etcd event channel closed")
				}
				if err := event.Err(); err != nil {
					if err == rpctypes.ErrCompacted {
						c.startWatchLoop() // restart watch loop if etcd compacted
						return
					}
					panic(fmt.Sprintf("failed to handle etcd event: %v", err))
				}
				for _, e := range event.Events {
					replicate := c.mustParseReplicatePChannelMeta(e)
					log.Info("handle replicate pchannel event",
						zap.String("sourceChannel", replicate.GetSourceChannelName()),
						zap.String("targetChannel", replicate.GetTargetChannelName()),
						zap.String("eventType", e.Type.String()),
					)
					switch e.Type {
					case mvccpb.PUT:
						if !strings.Contains(replicate.GetSourceChannelName(), currentClusterID) {
							// current cluster is not source cluster, skip create replicator
							continue
						}
						resource.Resource().ReplicateManagerClient().CreateReplicator(replicate)
					case mvccpb.DELETE:
						resource.Resource().ReplicateManagerClient().RemoveReplicator(replicate)
					}
				}
			}
		}
	}()
}

func (c *controller) mustParseReplicatePChannelMeta(e *clientv3.Event) *streamingpb.ReplicatePChannelMeta {
	meta := &streamingpb.ReplicatePChannelMeta{}
	err := proto.Unmarshal(e.Kv.Value, meta)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal replicate pchannel meta: %v", err))
	}
	return meta
}

func (c *controller) Stop() {
	log.Ctx(c.ctx).Info("stop CDC controller...")
	c.cancel()
	c.wg.Wait()
	resource.Resource().ReplicateManagerClient().Close()
	log.Ctx(c.ctx).Info("CDC controller stopped")
}
