// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package etcdkv

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type EtcdStatsWatcher struct {
	mu        sync.RWMutex
	client    *clientv3.Client
	helper    etcdStatsHelper
	size      int
	startTime time.Time
}

type etcdStatsHelper struct {
	eventAfterReceive    func()
	eventAfterStartWatch func()
}

func defaultHelper() etcdStatsHelper {
	return etcdStatsHelper{
		eventAfterReceive:    func() {},
		eventAfterStartWatch: func() {},
	}
}

func NewEtcdStatsWatcher(client *clientv3.Client) *EtcdStatsWatcher {
	return &EtcdStatsWatcher{
		client: client,
		helper: defaultHelper(),
	}
}

func (w *EtcdStatsWatcher) StartBackgroundLoop(ctx context.Context) {
	ch := w.client.Watch(ctx, "", clientv3.WithPrefix(), clientv3.WithCreatedNotify())
	w.mu.Lock()
	w.startTime = time.Now()
	w.mu.Unlock()
	w.helper.eventAfterStartWatch()
	for {
		select {
		case <-ctx.Done():
			log.Debug("etcd stats watcher shutdown")
			return
		case e := <-ch:
			if e.Err() != nil {
				log.Error("etcd stats watcher receive error response", zap.Error(e.Err()))
				continue
			}
			if len(e.Events) == 0 {
				continue
			}
			t := 0
			for _, event := range e.Events {
				if event.Kv.Version == 1 {
					t += len(event.Kv.Key) + len(event.Kv.Value)
				}
			}
			w.mu.Lock()
			w.size += t
			w.mu.Unlock()
			w.helper.eventAfterReceive()
		}
	}
}

func (w *EtcdStatsWatcher) GetSize() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.size
}

/*
func (w *EtcdStatsWatcher) GetStartTime() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.startTime
}

func (w *EtcdStatsWatcher) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.size = 0
	w.startTime = time.Now()
}
*/
