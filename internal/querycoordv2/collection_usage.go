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
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	walbalance "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/internal/views/coord/viewfeedback"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type collectionReleaseReason string

const (
	releaseReasonTTI            collectionReleaseReason = "tti"
	releaseReasonMemoryPressure collectionReleaseReason = "memory_pressure"
)

type releaseCandidate struct {
	collectionID        int64
	lastActiveUnixNanos int64
	reason              collectionReleaseReason
}

type collectionMemoryUsage struct {
	used, total uint64
}

type collectionUsageManager struct {
	s  *Server
	mu sync.Mutex

	enabled             bool
	lastActiveUnixNanos map[int64]int64
	configWatcher       config.EventHandler
	configChanged       chan struct{}

	// Pressure is retained between the low and high watermarks, per pool.
	memoryPressure map[string]bool

	releaseQueue []releaseCandidate
	// Entries cover queued and running releases. Removing a load invalidates
	// its queued release, even if the collection is loaded again before dispatch.
	releasing        map[int64]bool
	releasesInFlight int
	// Counts LRU releases from admission until completion or discard.
	lruReleasesOutstanding int

	stop context.CancelFunc
	wg   sync.WaitGroup
}

func newCollectionUsageManager(s *Server) *collectionUsageManager {
	params := paramtable.Get()
	m := &collectionUsageManager{
		s:                   s,
		lastActiveUnixNanos: make(map[int64]int64),
		memoryPressure:      make(map[string]bool),
		releasing:           make(map[int64]bool),
		configChanged:       make(chan struct{}, 1),
	}
	s.qviewsRuntime.loadConfigStore.RegisterObserver(m.handleLoadConfigChange)
	m.configWatcher = config.NewHandler(fmt.Sprintf("collection-usage-config-%p", m), func(*config.Event) {
		m.refreshConfig()
	})
	params.Watch(params.QueryCoordCfg.AutoReleaseEnabled.Key, m.configWatcher)
	params.Watch(params.ProxyCfg.EnableAutoLoad.Key, m.configWatcher)
	// Read after subscribing so an update during construction cannot be missed.
	m.refreshConfig()
	return m
}

func (m *collectionUsageManager) refreshConfig() {
	params := paramtable.Get()
	m.mu.Lock()
	autoReleaseEnabled := params.QueryCoordCfg.AutoReleaseEnabled.GetAsBool()
	autoLoadEnabled := params.ProxyCfg.EnableAutoLoad.GetAsBool()
	misconfigured := autoReleaseEnabled && !autoLoadEnabled

	m.enabled = autoReleaseEnabled && autoLoadEnabled
	if !m.enabled {
		m.resetUsageLocked()
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
	m.mu.Lock()
	m.enabled = false
	m.resetUsageLocked()
	m.mu.Unlock()
	if m.stop != nil {
		m.stop()
	}
	m.wg.Wait()
}

func (m *collectionUsageManager) resetUsageLocked() {
	clear(m.lastActiveUnixNanos)
	clear(m.memoryPressure)
	for _, candidate := range m.releaseQueue {
		m.removeReleaseLocked(candidate)
	}
	m.releaseQueue = nil
}

func (m *collectionUsageManager) handleLoadConfigChange(collectionID int64, released bool) {
	if !released {
		return
	}
	m.mu.Lock()
	delete(m.lastActiveUnixNanos, collectionID)
	if _, scheduled := m.releasing[collectionID]; scheduled {
		m.releasing[collectionID] = false
	}
	m.mu.Unlock()
}

func (m *collectionUsageManager) scan(ctx context.Context) {
	m.mu.Lock()
	enabled := m.enabled
	m.mu.Unlock()
	if !enabled {
		return
	}

	roleCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	b, err := walbalance.GetWithContext(roleCtx)
	cancel()
	if err != nil {
		mlog.RatedWarn(ctx, 1.0, "failed to get cluster replication role, skip auto release scan", mlog.Err(err))
		return
	}
	if b.ReplicateRole() != replicateutil.RolePrimary {
		return
	}

	snapshot := m.s.qviewsRuntime.loadConfigStore.Snapshot()
	pressure := m.pressuredPools(ctx)
	feedback := m.s.qviewsRuntime.viewFeedback.GetPlanningSnapshot()
	lruLimit := paramtable.Get().QueryCoordCfg.AutoReleaseLRUMaxEvictions.GetAsInt()
	m.mu.Lock()
	lruBudget := lruLimit - m.lruReleasesOutstanding
	m.mu.Unlock()
	lru := typeutil.NewObjectArrayBasedMaximumHeap(nil, func(candidate releaseCandidate) int64 {
		return candidate.lastActiveUnixNanos
	})
	candidates := make([]releaseCandidate, 0)
	for collectionID, cfg := range snapshot.ConfigsMap() {
		if ctx.Err() != nil {
			return
		}
		if !cfg.AutoLoad || !m.collectionReady(collectionID) {
			continue
		}
		candidate, tti, ok := m.releaseCandidate(ctx, cfg, feedback)
		if !ok {
			continue
		}
		if tti < 0 {
			continue
		}
		if idleFor(candidate.lastActiveUnixNanos, tti) {
			candidate.reason = releaseReasonTTI
			candidates = append(candidates, candidate)
			continue
		}
		if lruBudget > 0 && underMemoryPressure(cfg, pressure) {
			candidate.reason = releaseReasonMemoryPressure
			if lru.Len() < lruBudget {
				lru.Push(candidate)
			} else if candidate.lastActiveUnixNanos < lru.Peek().lastActiveUnixNanos {
				lru.Pop()
				lru.Push(candidate)
			}
		}
	}
	firstLRU := len(candidates)
	for lru.Len() > 0 {
		candidates = append(candidates, lru.Pop())
	}
	// Oldest candidates get the remaining budget if it shrank during the scan.
	slices.Reverse(candidates[firstLRU:])
	m.queueReleases(ctx, candidates)
}

func (m *collectionUsageManager) pressuredPools(ctx context.Context) map[string]bool {
	params := &paramtable.Get().QueryCoordCfg
	high := params.AutoReleaseLRUHighWatermark.GetAsFloat() / 100
	low := params.AutoReleaseLRULowWatermark.GetAsFloat() / 100
	if low >= high {
		mlog.RatedWarn(ctx, 1.0, "collection LRU low watermark must be below high, skip LRU selection", mlog.Float64("low", low), mlog.Float64("high", high))
		return nil
	}
	usages := m.collectMemoryUsageByRG(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.enabled {
		return nil
	}
	pressure := make(map[string]bool, len(usages))
	for pool, usage := range usages {
		if usage.total == 0 {
			continue
		}
		ratio := float64(usage.used) / float64(usage.total)
		previous := m.memoryPressure[pool]
		if ratio >= high {
			m.memoryPressure[pool] = true
		} else if ratio <= low {
			m.memoryPressure[pool] = false
		}
		pressure[pool] = m.memoryPressure[pool]
		if previous != pressure[pool] {
			mlog.Info(ctx, "collection LRU memory pressure changed", mlog.String("resourceGroup", pool), mlog.Float64("memoryRatio", ratio), mlog.Bool("pressure", pressure[pool]))
		}
	}
	return pressure
}

func (m *collectionUsageManager) releaseCandidate(ctx context.Context, cfg *loadmgr.LoadConfig, feedback *viewfeedback.PlanningSnapshot) (releaseCandidate, time.Duration, bool) {
	collectionID := cfg.CollectionID
	m.mu.Lock()
	_, scheduled := m.releasing[collectionID]
	m.mu.Unlock()
	if scheduled {
		return releaseCandidate{}, 0, false
	}
	tti, err := m.tti(ctx, collectionID)
	if err != nil {
		mlog.RatedWarn(ctx, 1.0, "failed to get collection auto release TTI, skip this scan",
			mlog.FieldCollectionID(collectionID), mlog.Err(err))
		return releaseCandidate{}, 0, false
	}
	var lastReported int64
	for _, replica := range cfg.Replicas {
		if sample, _ := feedback.GetReplicaFeedback(replica.ReplicaID, replica.ResourceGroup); sample != nil {
			lastReported = max(lastReported, sample.LastAccessUnixNanos)
		}
	}
	m.mu.Lock()
	lastActiveUnixNanos, initialized := m.lastActiveUnixNanos[collectionID]
	if lastReported > lastActiveUnixNanos {
		lastActiveUnixNanos = lastReported
	} else if !initialized {
		lastActiveUnixNanos = time.Now().UnixNano()
	}
	m.lastActiveUnixNanos[collectionID] = lastActiveUnixNanos
	m.mu.Unlock()

	// A release may have removed the config before the timer was inserted.
	if !initialized && !m.s.qviewsRuntime.loadConfigStore.Contains(collectionID) {
		m.handleLoadConfigChange(collectionID, true)
		return releaseCandidate{}, 0, false
	}
	return releaseCandidate{collectionID: collectionID, lastActiveUnixNanos: lastActiveUnixNanos, reason: releaseReasonTTI}, tti, true
}

func idleFor(lastActiveUnixNanos int64, tti time.Duration) bool {
	return tti > 0 && time.Now().UnixNano()-lastActiveUnixNanos >= tti.Nanoseconds()
}

func underMemoryPressure(cfg *loadmgr.LoadConfig, pressure map[string]bool) bool {
	for _, replica := range cfg.Replicas {
		if pressure[replica.ResourceGroup] {
			return true
		}
	}
	return false
}

func (m *collectionUsageManager) collectionReady(collectionID int64) bool {
	coll, err := m.s.meta.GetCollectionInfo(collectionID)
	if err != nil {
		return false
	}
	runtime := m.s.qviewsRuntime
	return loadstatus.Get(runtime.loadConfigStore, runtime.shardViewRegistry, collectionID, coll.VChannelNames).Ready()
}

// Both TTI and memory-pressure decisions use the scan snapshot. Once queued,
// a release proceeds even after later queries, TTI changes or lower memory usage.
func (m *collectionUsageManager) queueReleases(ctx context.Context, candidates []releaseCandidate) {
	lruLimit := paramtable.Get().QueryCoordCfg.AutoReleaseLRUMaxEvictions.GetAsInt()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.enabled || ctx.Err() != nil {
		return
	}
	for _, candidate := range candidates {
		id := candidate.collectionID
		if _, loaded := m.lastActiveUnixNanos[id]; !loaded {
			continue
		}
		if _, scheduled := m.releasing[id]; scheduled {
			continue
		}
		if candidate.reason == releaseReasonMemoryPressure {
			if m.lruReleasesOutstanding >= lruLimit {
				continue
			}
			m.lruReleasesOutstanding++
		}
		m.releasing[id] = true
		m.releaseQueue = append(m.releaseQueue, candidate)
	}
	m.dispatchReleasesLocked(ctx)
}

func (m *collectionUsageManager) dispatchReleasesLocked(ctx context.Context) {
	limit := paramtable.Get().QueryCoordCfg.AutoReleaseConcurrency.GetAsInt()
	for len(m.releaseQueue) > 0 && m.releasesInFlight < limit && ctx.Err() == nil {
		candidate := m.releaseQueue[0]
		m.releaseQueue = m.releaseQueue[1:]
		if !m.releasing[candidate.collectionID] {
			m.removeReleaseLocked(candidate)
			continue
		}
		m.releasesInFlight++
		m.wg.Add(1)
		go m.runRelease(ctx, candidate)
	}
	if len(m.releaseQueue) == 0 {
		m.releaseQueue = nil
	}
}

func (m *collectionUsageManager) runRelease(ctx context.Context, candidate releaseCandidate) {
	defer m.wg.Done()
	if err := m.autoReleaseCollection(ctx, candidate.collectionID, candidate.reason); err != nil {
		mlog.Warn(ctx, "failed to auto release collection",
			mlog.FieldCollectionID(candidate.collectionID), mlog.String("releaseReason", string(candidate.reason)), mlog.Err(err))
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeReleaseLocked(candidate)
	m.releasesInFlight--
	m.dispatchReleasesLocked(ctx)
}

func (m *collectionUsageManager) removeReleaseLocked(candidate releaseCandidate) {
	delete(m.releasing, candidate.collectionID)
	if candidate.reason == releaseReasonMemoryPressure {
		m.lruReleasesOutstanding--
	}
}

func (m *collectionUsageManager) tti(ctx context.Context, collectionID int64) (time.Duration, error) {
	properties, err := m.s.broker.GetCollectionProperties(ctx, collectionID)
	if err != nil {
		return 0, err
	}
	tti, err := common.GetCollectionAutoReleaseTTI(properties)
	if err != nil {
		mlog.RatedWarn(ctx, 1.0, "invalid collection auto release TTI, use global default",
			mlog.FieldCollectionID(collectionID), mlog.Err(err))
	}
	if err != nil || tti == 0 {
		tti = paramtable.Get().QueryCoordCfg.AutoReleaseTTISeconds.GetAsDuration(time.Second)
	}
	return tti, nil
}

func (m *collectionUsageManager) autoReleaseCollection(ctx context.Context, collectionID int64, reason collectionReleaseReason) error {
	schema := m.s.meta.GetCollectionSchema(ctx, collectionID)
	released, err := m.s.broadcastDropLoadConfigCollectionV2ForReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
		CollectionID: collectionID,
	}, true)
	if released {
		mlog.Info(ctx, "broadcast auto release for collection", mlog.String("event", "collection_auto_released"),
			mlog.FieldCollectionID(collectionID), mlog.FieldDbName(schema.GetDbName()), mlog.FieldCollectionName(schema.GetName()),
			mlog.String("releaseReason", string(reason)))
	}
	return err
}

// collectMemoryUsageByRG reads node memory usage and capacity, independent of
// admission thresholds and cache accounting. Only balanceable Worker QNs count.
// A missing reading invalidates its entire pool for this scan.
func (m *collectionUsageManager) collectMemoryUsageByRG(ctx context.Context) map[string]collectionMemoryUsage {
	nodes := m.s.onlineQueryNodes()
	groups := make([]string, len(nodes))
	readings := make([]collectionMemoryUsage, len(nodes))
	for i, node := range nodes {
		groups[i] = m.s.meta.GetResourceGroupByNodeID(node.ID())
	}
	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.NodeMemoryMetrics)
	if err != nil {
		mlog.Warn(ctx, "failed to construct collection LRU memory metrics request", mlog.Err(err))
		return nil
	}
	metricsCtx := retry.WithMaxAttemptsContext(ctx, 1)
	var group errgroup.Group
	group.SetLimit(maxConcurrentQueryNodeMetrics)
	for i, node := range nodes {
		if groups[i] == "" {
			continue
		}
		group.Go(func() error {
			rpcCtx, cancel := context.WithTimeout(metricsCtx, queryNodeMetricsTimeout)
			defer cancel()
			resp, err := m.s.cluster.GetMetrics(rpcCtx, node.ID(), req)
			if err := merr.CheckRPCCall(resp, err); err != nil {
				mlog.RatedWarn(ctx, 1.0, "collection LRU memory metrics unavailable, skip resource group", mlog.FieldNodeID(node.ID()), mlog.Err(err))
				return nil
			}
			var info metricsinfo.NodeMemory
			if err := metricsinfo.UnmarshalComponentInfos(resp.GetResponse(), &info); err != nil {
				mlog.RatedWarn(ctx, 1.0, "invalid collection LRU memory metrics, skip resource group", mlog.FieldNodeID(node.ID()), mlog.Err(err))
				return nil
			}
			// GetUsedMemoryCount returns zero when the process reading fails.
			if info.Memory == 0 || info.MemoryUsage == 0 {
				mlog.RatedWarn(ctx, 1.0, "collection LRU memory reading unavailable, skip resource group", mlog.FieldNodeID(node.ID()))
				return nil
			}
			readings[i] = collectionMemoryUsage{used: info.MemoryUsage, total: info.Memory}
			return nil
		})
	}
	_ = group.Wait() // Individual failures invalidate their pools below.
	usage := make(map[string]collectionMemoryUsage)
	missing := make(map[string]bool)
	for i, reading := range readings {
		rg := groups[i]
		if rg == "" {
			continue
		}
		if reading.total == 0 {
			missing[rg] = true
			continue
		}
		pool := usage[rg]
		pool.used += reading.used
		pool.total += reading.total
		usage[rg] = pool
	}
	for rg := range missing {
		delete(usage, rg)
	}
	return usage
}
