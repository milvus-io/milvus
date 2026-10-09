// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoordv2

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type collectionUsageManager struct {
	mu sync.Mutex

	enabled       bool
	lastActiveAt  map[int64]time.Time
	configWatcher config.EventHandler
	configChanged chan struct{}

	loadConfigs *loadmgr.LoadConfigStore
	release     func(context.Context, int64) error

	stop context.CancelFunc
	wg   sync.WaitGroup
}

func newCollectionUsageManager(
	loadConfigs *loadmgr.LoadConfigStore,
	release func(context.Context, int64) error,
) *collectionUsageManager {
	m := &collectionUsageManager{
		lastActiveAt:  make(map[int64]time.Time),
		loadConfigs:   loadConfigs,
		release:       release,
		configChanged: make(chan struct{}, 1),
	}
	loadConfigs.RegisterObserver(m.handleLoadConfigChange)
	m.configWatcher = config.NewHandler(fmt.Sprintf("collection-usage-config-%p", m), func(*config.Event) {
		m.refreshEnabled()
	})
	params := paramtable.Get()
	params.Watch(params.QueryCoordCfg.AutoReleaseEnabled.Key, m.configWatcher)
	params.Watch(params.ProxyCfg.EnableAutoLoad.Key, m.configWatcher)
	// Read after subscribing so an update during construction cannot be missed.
	m.refreshEnabled()
	return m
}

func (m *collectionUsageManager) refreshEnabled() {
	params := paramtable.Get()
	autoReleaseEnabled := params.QueryCoordCfg.AutoReleaseEnabled.GetAsBool()
	autoLoadEnabled := params.ProxyCfg.EnableAutoLoad.GetAsBool()
	misconfigured := autoReleaseEnabled && !autoLoadEnabled

	m.mu.Lock()
	m.enabled = autoReleaseEnabled && autoLoadEnabled
	if !m.enabled {
		clear(m.lastActiveAt)
	}
	m.mu.Unlock()
	if misconfigured {
		mlog.Warn(context.TODO(), "queryCoord auto release requires proxy auto load; auto release remains disabled")
	}
	m.notifyConfigChanged()
}

func (m *collectionUsageManager) notifyConfigChanged() {
	select {
	case m.configChanged <- struct{}{}:
	default:
	}
}

// start runs a config-driven scan loop: disabled means no ticker; enabling or
// changing the interval starts or resets the ticker with the latest value.
func (m *collectionUsageManager) start(ctx context.Context) {
	// The cancel function is owned by the manager and invoked by close().
	ctx, m.stop = context.WithCancel(ctx) //nolint:gosec
	params := paramtable.Get()
	interval := &params.QueryCoordCfg.AutoReleaseCheckInterval
	watcher := config.NewHandler(fmt.Sprintf("collection-usage-interval-%p", m), func(*config.Event) {
		m.notifyConfigChanged()
	})
	params.Watch(interval.Key, watcher)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer params.Unwatch(interval.Key, watcher)
		var ticker *time.Ticker
		var ticks <-chan time.Time
		defer func() {
			if ticker != nil {
				ticker.Stop()
			}
		}()
		updateTicker := func() {
			m.mu.Lock()
			enabled := m.enabled
			m.mu.Unlock()
			if !enabled {
				if ticker != nil {
					ticker.Stop()
					ticker = nil
				}
				ticks = nil
				return
			}
			duration := interval.GetAsDuration(time.Second)
			if ticker == nil {
				ticker = time.NewTicker(duration)
			} else {
				ticker.Reset(duration)
			}
			ticks = ticker.C
		}
		updateTicker()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.configChanged:
				updateTicker()
			case <-ticks:
				m.scan(ctx)
			}
		}
	}()
}

func (m *collectionUsageManager) close() {
	params := paramtable.Get()
	params.Unwatch(params.QueryCoordCfg.AutoReleaseEnabled.Key, m.configWatcher)
	params.Unwatch(params.ProxyCfg.EnableAutoLoad.Key, m.configWatcher)
	if m.stop != nil {
		m.stop()
	}
	m.wg.Wait()
}

// touch records a DQL attempt once the scanner has initialized this load's
// idle timer from its load config.
// Requests are not tracked individually; even a running query can outlive TTL.
func (m *collectionUsageManager) touch(collectionID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.enabled {
		return
	}
	if _, initialized := m.lastActiveAt[collectionID]; initialized {
		m.lastActiveAt[collectionID] = time.Now()
	}
}

func (m *collectionUsageManager) handleLoadConfigChange(collectionID int64, released bool) {
	if !released {
		return
	}
	m.mu.Lock()
	delete(m.lastActiveAt, collectionID)
	m.mu.Unlock()
}

func (m *collectionUsageManager) scan(ctx context.Context) {
	m.mu.Lock()
	enabled := m.enabled
	m.mu.Unlock()
	if !enabled {
		return
	}

	snapshot := m.loadConfigs.Snapshot()
	candidates := make([]int64, 0)
	for collectionID := range snapshot.ConfigsMap() {
		if ctx.Err() != nil {
			return
		}
		if m.shouldRelease(collectionID) {
			candidates = append(candidates, collectionID)
		}
	}
	m.releaseCollections(ctx, candidates)
}

func (m *collectionUsageManager) shouldRelease(collectionID int64) bool {
	m.mu.Lock()
	lastActiveAt, initialized := m.lastActiveAt[collectionID]
	m.mu.Unlock()

	// Start the idle TTL on the first scan and skip release for this round.
	if !initialized {
		m.initializeLastActiveAt(collectionID)
		return false
	}
	return time.Since(lastActiveAt) >= m.idleTTL()
}

func (m *collectionUsageManager) releaseCollections(ctx context.Context, collectionIDs []int64) {
	if len(collectionIDs) == 0 {
		return
	}
	var group errgroup.Group
	group.SetLimit(min(paramtable.Get().QueryCoordCfg.AutoReleaseConcurrency.GetAsInt(), len(collectionIDs)))
	for _, collectionID := range collectionIDs {
		if ctx.Err() != nil {
			break
		}
		group.Go(func() error {
			if ctx.Err() != nil {
				return nil
			}
			if err := m.release(ctx, collectionID); err != nil {
				mlog.Warn(ctx, "failed to auto release idle collection",
					mlog.FieldCollectionID(collectionID), mlog.Err(err))
			}
			return nil
		})
	}
	_ = group.Wait()
}

func (m *collectionUsageManager) initializeLastActiveAt(collectionID int64) {
	if !m.loadConfigs.Contains(collectionID) {
		return
	}
	m.mu.Lock()
	if !m.enabled {
		m.mu.Unlock()
		return
	}
	if _, initialized := m.lastActiveAt[collectionID]; !initialized {
		m.lastActiveAt[collectionID] = time.Now()
	}
	m.mu.Unlock()

	// Remove may finish between the first Contains and recording the timestamp.
	// In that case its observer may have run before this record was added.
	if !m.loadConfigs.Contains(collectionID) {
		m.mu.Lock()
		delete(m.lastActiveAt, collectionID)
		m.mu.Unlock()
	}
}

func (m *collectionUsageManager) idleTTL() time.Duration {
	return paramtable.Get().QueryCoordCfg.AutoReleaseIdleTTLSeconds.GetAsDuration(time.Second)
}

func (s *Server) autoReleaseCollection(ctx context.Context, collectionID int64) error {
	err := s.broadcastDropLoadConfigCollectionV2ForReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
		CollectionID: collectionID,
	})
	if err != nil {
		return err
	}
	mlog.Info(ctx, "broadcast auto release for idle collection", mlog.FieldCollectionID(collectionID))
	return nil
}
