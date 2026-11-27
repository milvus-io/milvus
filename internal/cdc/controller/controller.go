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
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// listRetryInterval is how long the controller waits before listing the
// replicate pchannels again after a transient etcd error.
const listRetryInterval = time.Second

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
		mlog.Info(c.ctx, "recover replicate pchannel meta",
			mlog.String("key", channelMeta.Key),
			mlog.Int64("revision", channelMeta.ModRevision),
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
	mlog.Info(c.ctx, "succeed to watch replicate pchannel meta events",
		mlog.Int64("revision", revision), mlog.String("prefix", c.prefix))
	return eventCh
}

func (c *controller) startWatchLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			m, err := c.listReplicatePChannels()
			if err != nil {
				if c.ctx.Err() != nil {
					return
				}
				panic(fmt.Sprintf("failed to list replicate pchannels: %v", err))
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

// listReplicatePChannels lists the replicate pchannels, retrying transient etcd
// errors (e.g. leader changed while etcd restarts). Any other error, including
// a request timeout, is returned.
func (c *controller) listReplicatePChannels() (*meta.ReplicateChannels, error) {
	for {
		m, err := meta.ListReplicatePChannels(c.ctx, resource.Resource().ETCD(), c.prefix)
		if err == nil {
			return m, nil
		}
		if c.ctx.Err() != nil || !etcd.IsRetriableEtcdErr(err) {
			return nil, err
		}
		mlog.Warn(c.ctx, "failed to list replicate pchannels, retry", mlog.Err(err))
		select {
		case <-time.After(listRetryInterval):
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		}
	}
}

func (c *controller) watchLoop(eventCh clientv3.WatchChan) error {
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case event, ok := <-eventCh:
			if !ok {
				if c.ctx.Err() != nil {
					return nil
				}
				panic("etcd event channel closed")
			}
			if err := event.Err(); err != nil {
				mlog.Warn(c.ctx, "etcd event error", mlog.Err(err))
				return err
			}
			for _, e := range event.Events {
				switch e.Type {
				case mvccpb.PUT:
					mlog.Info(c.ctx, "handle replicate pchannel PUT event",
						mlog.String("key", string(e.Kv.Key)),
						mlog.Int64("modRevision", e.Kv.ModRevision),
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
					mlog.Info(c.ctx, "handle replicate pchannel DELETE event",
						mlog.String("key", string(e.Kv.Key)),
						mlog.Int64("prevModRevision", e.PrevKv.ModRevision),
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
	mlog.Info(c.ctx, "stop CDC controller...")
	c.cancel()
	c.wg.Wait()
	resource.Resource().ReplicateManagerClient().Close()
	mlog.Info(c.ctx, "CDC controller stopped")
}
