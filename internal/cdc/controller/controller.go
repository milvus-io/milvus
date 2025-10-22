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

package controller

import (
	"context"
	"path"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// Controller controls and schedules the CDC process.
// It will periodically update the replications by the replicate configuration.
type Controller interface {
	Start() error
	Stop()
}

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

func (c *controller) recoverReplicatePChannelMeta(channels []*meta.ReplicateChannel) {
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	for _, channelMeta := range channels {
		if !strings.Contains(channelMeta.Value.GetSourceChannelName(), currentClusterID) {
			// current cluster is not source cluster, skip create replicator
			continue
		}
		log.Info("recover replicate pchannel meta",
			zap.String("key", channelMeta.Key),
			zap.Int64("revision", channelMeta.ModRevision),
		)
		channel := &meta.ReplicateChannel{
			Key:         channelMeta.Key,
			Value:       channelMeta.Value,
			ModRevision: channelMeta.ModRevision,
		}
		resource.Resource().ReplicateManagerClient().CreateReplicator(channel)
	}
	resource.Resource().ReplicateManagerClient().RemoveOutdatedReplicators(channels)
}

func (c *controller) watchEvents(revision int64) clientv3.WatchChan {
	eventCh := resource.Resource().ETCD().Watch(
		c.ctx,
		c.prefix,
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
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
			m, err := meta.ListReplicatePChannels(c.ctx, resource.Resource().ETCD(), c.prefix)
			if err != nil && c.ctx.Err() == nil {
				log.Ctx(c.ctx).Warn("failed to list replicate pchannels", zap.Error(err))
				continue
			}
			c.recoverReplicatePChannelMeta(m.Channels)
			eventCh := c.watchEvents(m.Revision + 1)
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
				switch e.Type {
				case mvccpb.PUT:
					log.Info("handle replicate pchannel PUT event",
						zap.String("key", string(e.Kv.Key)),
						zap.Int64("modRevision", e.Kv.ModRevision),
					)
					currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
					replicate := meta.MustParseReplicateChannelFromEvent(e)
					if !strings.Contains(replicate.GetSourceChannelName(), currentClusterID) {
						// current cluster is not source cluster, skip create replicator
						continue
					}
					channel := &meta.ReplicateChannel{
						Key:         string(e.Kv.Key),
						Value:       replicate,
						ModRevision: e.Kv.ModRevision,
					}
					resource.Resource().ReplicateManagerClient().CreateReplicator(channel)
				case mvccpb.DELETE:
					log.Info("handle replicate pchannel DELETE event",
						zap.String("key", string(e.Kv.Key)),
						zap.Int64("prevModRevision", e.PrevKv.ModRevision),
					)
					key := string(e.Kv.Key)
					revision := e.PrevKv.ModRevision
					resource.Resource().ReplicateManagerClient().RemoveReplicator(key, revision)
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
