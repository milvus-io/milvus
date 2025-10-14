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
	"path"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

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

	prefix string
}

func NewController() *controller {
	ctx, cancel := context.WithCancel(context.Background())
	return &controller{
		ctx:    ctx,
		cancel: cancel,
		prefix: path.Join(
			paramtable.Get().EtcdCfg.MetaRootPath.GetValue(),
			streamingcoord.ReplicatePChannelMetaPrefix,
		),
	}
}

func (c *controller) Start() error {
	replicatePChannels, err := ListReplicatePChannels(c.ctx, resource.Resource().ETCD(), c.prefix)
	if err != nil {
		return err
	}
	if err := c.recoverReplicatePChannelMeta(replicatePChannels.Channels); err != nil {
		return err
	}
	initRevision := replicatePChannels.Revision
	eventCh := c.watchEvents(initRevision)
	c.startWatchLoop(eventCh)
	return nil
}

func (c *controller) recoverReplicatePChannelMeta(replicateChannels []*streamingpb.ReplicatePChannelMeta) error {
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	for _, replicate := range replicateChannels {
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

func (c *controller) watchEvents(revision int64) clientv3.WatchChan {
	eventCh := resource.Resource().ETCD().Watch(
		c.ctx,
		c.prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(revision),
	)
	log.Ctx(c.ctx).Info("succeed to watch replicate pchannel meta events",
		zap.Int64("revision", revision), zap.String("prefix", c.prefix))
	return eventCh
}

func (c *controller) startWatchLoop(eventCh clientv3.WatchChan) {
	c.wg.Add(1)
	go func() {
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
					// restart watch loop if error occurred
					log.Warn("etcd event error, will restart watch loop", zap.Error(err))
					newRevision := MustGetRevision(c.ctx, resource.Resource().ETCD(), c.prefix)
					eventCh := c.watchEvents(newRevision)
					c.startWatchLoop(eventCh)
					return
				}
				for _, e := range event.Events {
					replicate := MustParseReplicateChannelFromEvent(e)
					log.Info("handle replicate pchannel event",
						zap.String("sourceChannel", replicate.GetSourceChannelName()),
						zap.String("targetChannel", replicate.GetTargetChannelName()),
						zap.String("eventType", e.Type.String()),
					)
					switch e.Type {
					case mvccpb.PUT:
						currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
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

func (c *controller) Stop() {
	log.Ctx(c.ctx).Info("stop CDC controller...")
	c.cancel()
	c.wg.Wait()
	resource.Resource().ReplicateManagerClient().Close()
	log.Ctx(c.ctx).Info("CDC controller stopped")
}
