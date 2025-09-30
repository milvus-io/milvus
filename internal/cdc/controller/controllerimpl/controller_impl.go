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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const checkInterval = 10 * time.Second

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

func (c *controller) Start() {
	c.startWatchLoop()
}

func (c *controller) startWatchLoop() {
	prefix := streamingcoord.ReplicatePChannelMetaPrefix
	eventCh := resource.Resource().WatchKV().WatchWithPrefix(c.ctx, prefix)
	c.wg.Add(1)
	go func() {
		log.Info("start to watch replicate pchannel meta")
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
					switch e.Type {
					case mvccpb.PUT:
						c.handlePutEvent(e)
					case mvccpb.DELETE:
						c.handleDeleteEvent(e)
					}
				}
			}
		}
	}()
}

func (c *controller) handlePutEvent(e *clientv3.Event) {
	newReplicate := c.mustParseReplicatePChannelMeta(e)
	resource.Resource().ReplicateManagerClient().CreateReplicator(newReplicate)
}

func (c *controller) handleDeleteEvent(e *clientv3.Event) {
	staleReplicate := c.mustParseReplicatePChannelMeta(e)
	resource.Resource().ReplicateManagerClient().RemoveReplicator(staleReplicate)
}

func (c *controller) mustParseReplicatePChannelMeta(e *clientv3.Event) *streamingpb.ReplicatePChannelMeta {
	meta := &streamingpb.ReplicatePChannelMeta{}
	err := json.Unmarshal(e.Kv.Value, meta)
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
