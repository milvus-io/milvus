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

package msgdispatcher

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type DispatcherManager interface {
	Add(ctx context.Context, streamConfig *StreamConfig) (<-chan *MsgPack, error)
	Remove(vchannel string)
	NumTarget() int
	NumConsumer() int
	Run()
	Close()
}

var _ DispatcherManager = (*dispatcherManager)(nil)

type dispatcherManager struct {
	role     string
	nodeID   int64
	pchannel string

	registeredTargets *typeutil.ConcurrentMap[string, *target]

	mu                sync.RWMutex
	mainDispatcher    *Dispatcher
	deputyDispatchers map[int64]*Dispatcher // ID -> *Dispatcher

	factory   msgstream.Factory
	closeChan chan struct{}
	closeOnce sync.Once
}

func NewDispatcherManager(pchannel string, role string, nodeID int64, factory msgstream.Factory) DispatcherManager {
	log.Info("create new dispatcherManager", zap.String("role", role),
		zap.Int64("nodeID", nodeID), zap.String("pchannel", pchannel))
	c := &dispatcherManager{
		role:              role,
		nodeID:            nodeID,
		pchannel:          pchannel,
		registeredTargets: typeutil.NewConcurrentMap[string, *target](),
		deputyDispatchers: make(map[int64]*Dispatcher),
		factory:           factory,
		closeChan:         make(chan struct{}),
	}
	return c
}

func (c *dispatcherManager) Add(ctx context.Context, streamConfig *StreamConfig) (<-chan *MsgPack, error) {
	t := newTarget(streamConfig)
	if _, ok := c.registeredTargets.GetOrInsert(t.vchannel, t); ok {
		return nil, fmt.Errorf("vchannel %s already exists in the dispatcher", t.vchannel)
	}
	log.Info("target register done", zap.String("vchannel", t.vchannel))
	return t.ch, nil
}

func (c *dispatcherManager) Remove(vchannel string) {
	if _, ok := c.registeredTargets.GetAndRemove(vchannel); ok {
		log.Info("target unregister done", zap.String("vchannel", vchannel))
	}
}

func (c *dispatcherManager) NumTarget() int {
	return c.registeredTargets.Len()
}

func (c *dispatcherManager) NumConsumer() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	numConsumer := 0
	if c.mainDispatcher != nil {
		numConsumer++
	}
	numConsumer += len(c.deputyDispatchers)
	return numConsumer
}

func (c *dispatcherManager) Close() {
	c.closeOnce.Do(func() {
		c.closeChan <- struct{}{}
	})
}

func (c *dispatcherManager) Run() {
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("pchannel", c.pchannel))
	log.Info("dispatcherManager is running...")
	ticker1 := time.NewTicker(10 * time.Second)
	ticker2 := time.NewTicker(paramtable.Get().MQCfg.MergeCheckInterval.GetAsDuration(time.Second))
	defer ticker1.Stop()
	defer ticker2.Stop()
	for {
		select {
		case <-c.closeChan:
			log.Info("dispatcherManager exited")
			return
		case <-ticker1.C:
			c.uploadMetric()
		case <-ticker2.C:
			c.closeRedundantDispatchers()
			c.buildDispatcher()
			c.tryMerge()
		}
	}
}

func (c *dispatcherManager) buildDispatcher() {
	c.mu.Lock()
	defer c.mu.Unlock()

	tr := timerecord.NewTimeRecorder("")
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("pchannel", c.pchannel))

	allTargets := c.registeredTargets.Values()
	// get lack targets to perform subscription
	lackTargets := make([]*target, 0, len(allTargets))
OUTER:
	for _, t := range allTargets {
		if c.mainDispatcher != nil && c.mainDispatcher.HasTarget(t.vchannel) {
			continue
		}
		for _, dispatcher := range c.deputyDispatchers {
			if dispatcher.HasTarget(t.vchannel) {
				continue OUTER
			}
		}
		lackTargets = append(lackTargets, t)
	}

	if len(lackTargets) == 0 {
		return
	}
	vchannels := lo.Map(lackTargets, func(t *target, _ int) string {
		return t.vchannel
	})

	log.Info("start to build dispatchers", zap.Int("numTargets", len(vchannels)),
		zap.Strings("vchannels", vchannels))

	sort.Slice(lackTargets, func(i, j int) bool {
		return lackTargets[i].pos.GetTimestamp() < lackTargets[j].pos.GetTimestamp()
	})

	var (
		// pullback from the earliest position
		// to the latest position in lack targets
		earliestTarget = lackTargets[0]
		latestTarget   = lackTargets[len(lackTargets)-1]
	)

	isMain := c.mainDispatcher == nil

	// TODO: fix context
	d, err := NewDispatcher(context.Background(), c.factory, isMain, c.pchannel, earliestTarget.pos, earliestTarget.subPos, latestTarget.pos.GetTimestamp())
	if err != nil {
		panic(err)
	}
	for _, t := range lackTargets {
		d.AddTarget(t)
	}
	d.Handle(start)
	buildDur := tr.RecordSpan()

	// block util pullback to the latest target position
	if len(lackTargets) > 1 {
		d.BlockUtilPullbackDone()
	}

	var (
		pullbackBeginTs   = earliestTarget.pos.GetTimestamp()
		pullbackEndTs     = latestTarget.pos.GetTimestamp()
		pullbackBeginTime = tsoutil.PhysicalTime(pullbackBeginTs)
		pullbackEndTime   = tsoutil.PhysicalTime(pullbackEndTs)
	)
	log.Info("build dispatcher done",
		zap.Int64("id", d.ID()),
		zap.Int("numVchannels", len(vchannels)),
		zap.Uint64("pullbackBeginTs", pullbackBeginTs),
		zap.Uint64("pullbackEndTs", pullbackEndTs),
		zap.Duration("lag", pullbackEndTime.Sub(pullbackBeginTime)),
		zap.Time("pullbackBeginTime", pullbackBeginTime),
		zap.Time("pullbackEndTime", pullbackEndTime),
		zap.Duration("buildDur", buildDur),
		zap.Duration("pullbackDur", tr.RecordSpan()),
		zap.Strings("vchannels", vchannels),
	)

	if isMain {
		c.mainDispatcher = d
		log.Info("add main dispatcher", zap.Int64("id", d.ID()))
	} else {
		c.deputyDispatchers[d.ID()] = d
		log.Info("add deputy dispatcher", zap.Int64("id", d.ID()))
	}
}

func (c *dispatcherManager) closeRedundantDispatchers() {
	c.mu.Lock()
	defer c.mu.Unlock()

	log := log.With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID))
	redundantTargets := make(map[string]*target)
	if c.mainDispatcher != nil {
		for _, t := range c.mainDispatcher.GetTargets() {
			if !c.registeredTargets.Contain(t.vchannel) {
				redundantTargets[t.vchannel] = t
			}
		}
		if len(redundantTargets) > 0 {
			c.mainDispatcher.Handle(pause)
			for vchannel := range redundantTargets {
				c.mainDispatcher.CloseTarget(vchannel)
			}
			if c.mainDispatcher.TargetNum() == 0 && len(c.deputyDispatchers) == 0 {
				c.mainDispatcher.Handle(terminate)
				c.mainDispatcher = nil
			} else {
				c.mainDispatcher.Handle(resume)
			}
		}
	}
	for _, dispatcher := range c.deputyDispatchers {
		redundantTargets = make(map[string]*target)
		for _, t := range dispatcher.GetTargets() {
			if !c.registeredTargets.Contain(t.vchannel) {
				redundantTargets[t.vchannel] = t
			}
		}
		if len(redundantTargets) > 0 {
			dispatcher.Handle(pause)
			for vchannel := range redundantTargets {
				dispatcher.CloseTarget(vchannel)
			}
			if dispatcher.TargetNum() == 0 {
				dispatcher.Handle(terminate)
				delete(c.deputyDispatchers, dispatcher.ID())
				log.Info("remove deputy dispatcher done", zap.Int64("id", dispatcher.ID()))
			} else {
				dispatcher.Handle(resume)
			}
		}
	}
}

func (c *dispatcherManager) tryMerge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	start := time.Now()
	log := log.With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID))

	if c.mainDispatcher == nil || c.mainDispatcher.CurTs() == 0 {
		return
	}
	candidates := make([]*Dispatcher, 0, len(c.deputyDispatchers))
	for _, sd := range c.deputyDispatchers {
		if sd.CurTs() == c.mainDispatcher.CurTs() {
			candidates = append(candidates, sd)
		}
	}
	if len(candidates) == 0 {
		return
	}

	log.Info("start merging...", zap.Int64s("dispatchers", lo.Map(candidates, func(d *Dispatcher, _ int) int64 {
		return d.ID()
	})))
	mergeCandidates := make([]*Dispatcher, 0, len(candidates))
	c.mainDispatcher.Handle(pause)
	for _, dispatcher := range candidates {
		dispatcher.Handle(pause)
		// after pause, check alignment again, if not, evict it and try to merge next time
		if c.mainDispatcher.CurTs() != dispatcher.CurTs() {
			dispatcher.Handle(resume)
			continue
		}
		mergeCandidates = append(mergeCandidates, dispatcher)
	}
	mergeTs := c.mainDispatcher.CurTs()
	for _, dispatcher := range mergeCandidates {
		targets := dispatcher.GetTargets()
		for _, t := range targets {
			c.mainDispatcher.AddTarget(t)
		}
		dispatcher.Handle(terminate)
		delete(c.deputyDispatchers, dispatcher.ID())
	}
	c.mainDispatcher.Handle(resume)
	log.Info("merge done", zap.Int64s("dispatchers", lo.Map(candidates, func(d *Dispatcher, _ int) int64 {
		return d.ID()
	})),
		zap.Uint64("mergeTs", mergeTs),
		zap.Duration("dur", time.Since(start)))
}

// deleteMetric remove specific prometheus metric,
// Lock/RLock is required before calling this method.
func (c *dispatcherManager) deleteMetric(channel string) {
	nodeIDStr := fmt.Sprintf("%d", c.nodeID)
	if c.role == typeutil.DataNodeRole {
		metrics.DataNodeMsgDispatcherTtLag.DeleteLabelValues(nodeIDStr, channel)
		return
	}
	if c.role == typeutil.QueryNodeRole {
		metrics.QueryNodeMsgDispatcherTtLag.DeleteLabelValues(nodeIDStr, channel)
	}
}

func (c *dispatcherManager) uploadMetric() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeIDStr := fmt.Sprintf("%d", c.nodeID)
	fn := func(gauge *prometheus.GaugeVec) {
		if c.mainDispatcher == nil {
			return
		}
		for _, t := range c.mainDispatcher.GetTargets() {
			gauge.WithLabelValues(nodeIDStr, t.vchannel).Set(
				float64(time.Since(tsoutil.PhysicalTime(c.mainDispatcher.CurTs())).Milliseconds()))
		}
		for _, dispatcher := range c.deputyDispatchers {
			for _, t := range dispatcher.GetTargets() {
				gauge.WithLabelValues(nodeIDStr, t.vchannel).Set(
					float64(time.Since(tsoutil.PhysicalTime(dispatcher.CurTs())).Milliseconds()))
			}
		}
	}
	if c.role == typeutil.DataNodeRole {
		fn(metrics.DataNodeMsgDispatcherTtLag)
		return
	}
	if c.role == typeutil.QueryNodeRole {
		fn(metrics.QueryNodeMsgDispatcherTtLag)
	}
}
