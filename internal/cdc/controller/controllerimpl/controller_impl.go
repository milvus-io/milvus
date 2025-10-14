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
	c.startWatchLoop()
	return nil
}

func (c *controller) recoverReplicatePChannelMeta(replicatePChannels []*streamingpb.ReplicatePChannelMeta) {
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	for _, replicate := range replicatePChannels {
		if !strings.Contains(replicate.GetSourceChannelName(), currentClusterID) {
			// current cluster is not source cluster, skip create replicator
			continue
		}
		log.Info("recover replicate pchannel meta",
			zap.String("sourceChannel", replicate.GetSourceChannelName()),
			zap.String("targetChannel", replicate.GetTargetChannelName()),
		)
		// Replicate manager ensures the idempotency of the creation.
		resource.Resource().ReplicateManagerClient().CreateReplicator(replicate)
	}
	resource.Resource().ReplicateManagerClient().RemoveOutdatedReplicators(replicatePChannels)
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

func (c *controller) startWatchLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			channels, revision, err := ListReplicatePChannels(c.ctx, resource.Resource().ETCD(), c.prefix)
			if err != nil && c.ctx.Err() == nil {
				log.Ctx(c.ctx).Warn("failed to list replicate pchannels", zap.Error(err))
				continue
			}
			c.recoverReplicatePChannelMeta(channels)
			eventCh := c.watchEvents(revision)
			err = c.watchLoop(eventCh)
			if err == nil {
				break
			}
		}
	}()
}

func (c *controller) watchLoop(eventCh clientv3.WatchChan) error {
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case event, ok := <-eventCh:
			if !ok {
				panic("etcd event channel closed")
			}
			if err := event.Err(); err != nil {
				log.Warn("etcd event error", zap.Error(err))
				return err
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
}

func (c *controller) Stop() {
	log.Ctx(c.ctx).Info("stop CDC controller...")
	c.cancel()
	c.wg.Wait()
	resource.Resource().ReplicateManagerClient().Close()
	log.Ctx(c.ctx).Info("CDC controller stopped")
}
