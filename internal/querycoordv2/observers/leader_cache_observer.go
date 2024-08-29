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

package observers

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type CollectionShardLeaderCache = map[string]*querypb.ShardLeadersList

// LeaderCacheObserver is to invalidate shard leader cache when leader location changes
type LeaderCacheObserver struct {
	wg           sync.WaitGroup
	proxyManager proxyutil.ProxyClientManagerInterface
	startOnce    sync.Once
	stopOnce     sync.Once
	closeCh      chan struct{}

	// collections which need to update event
	eventCh chan int64
}

func (o *LeaderCacheObserver) Start(ctx context.Context) {
	o.startOnce.Do(func() {
		o.wg.Add(1)
		go o.schedule(ctx)
	})
}

func (o *LeaderCacheObserver) Stop() {
	o.stopOnce.Do(func() {
		close(o.closeCh)
		o.wg.Wait()
	})
}

func (o *LeaderCacheObserver) RegisterEvent(events ...int64) {
	for _, event := range events {
		o.eventCh <- event
	}
}

func (o *LeaderCacheObserver) schedule(ctx context.Context) {
	defer o.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Info("stop leader cache observer due to context done")
			return
		case <-o.closeCh:
			log.Info("stop leader cache observer")
			return

		case event := <-o.eventCh:
			log.Info("receive event, trigger leader cache update", zap.Int64("event", event))
			ret := make([]int64, 0)
			ret = append(ret, event)

			// try batch submit events
			eventNum := len(o.eventCh)
			if eventNum > 0 {
				for eventNum > 0 {
					event := <-o.eventCh
					ret = append(ret, event)
					eventNum--
				}
			}
			o.HandleEvent(ctx, ret...)
		}
	}
}

func (o *LeaderCacheObserver) HandleEvent(ctx context.Context, collectionIDs ...int64) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Second))
	defer cancel()
	err := o.proxyManager.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{
		CollectionIDs: collectionIDs,
	})
	if err != nil {
		log.Warn("failed to invalidate proxy's shard leader cache", zap.Error(err))
		return
	}
}

func NewLeaderCacheObserver(
	proxyManager proxyutil.ProxyClientManagerInterface,
) *LeaderCacheObserver {
	return &LeaderCacheObserver{
		proxyManager: proxyManager,
		closeCh:      make(chan struct{}),
		eventCh:      make(chan int64, 1024),
	}
}
