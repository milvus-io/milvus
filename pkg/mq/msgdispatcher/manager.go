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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type DispatcherManager interface {
	Add(ctx context.Context, streamConfig *StreamConfig) (<-chan *MsgPack, error)
	Remove(vchannel string)
	Num() int
	Run()
	Close()
}

var _ DispatcherManager = (*dispatcherManager)(nil)

type dispatcherManager struct {
	role     string
	nodeID   int64
	pchannel string

	lagNotifyChan chan struct{}
	lagTargets    *typeutil.ConcurrentMap[string, *target] // vchannel -> *target

	mu              sync.RWMutex // guards mainDispatcher and soloDispatchers
	mainDispatcher  *Dispatcher
	soloDispatchers map[string]*Dispatcher // vchannel -> *Dispatcher

	factory   msgstream.Factory
	closeChan chan struct{}
	closeOnce sync.Once
}

func NewDispatcherManager(pchannel string, role string, nodeID int64, factory msgstream.Factory) DispatcherManager {
	log.Info("create new dispatcherManager", zap.String("role", role),
		zap.Int64("nodeID", nodeID), zap.String("pchannel", pchannel))
	c := &dispatcherManager{
		role:            role,
		nodeID:          nodeID,
		pchannel:        pchannel,
		lagNotifyChan:   make(chan struct{}, 1),
		lagTargets:      typeutil.NewConcurrentMap[string, *target](),
		soloDispatchers: make(map[string]*Dispatcher),
		factory:         factory,
		closeChan:       make(chan struct{}),
	}
	return c
}

func (c *dispatcherManager) constructSubName(vchannel string, isMain bool) string {
	return fmt.Sprintf("%s-%d-%s-%t", c.role, c.nodeID, vchannel, isMain)
}

func (c *dispatcherManager) Add(ctx context.Context, streamConfig *StreamConfig) (<-chan *MsgPack, error) {
	vchannel := streamConfig.VChannel
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.soloDispatchers[vchannel]; ok {
		// current dispatcher didn't allow multiple subscriptions on same vchannel at same time
		log.Warn("unreachable: solo vchannel dispatcher already exists")
		return nil, fmt.Errorf("solo vchannel dispatcher already exists")
	}
	if c.mainDispatcher != nil {
		if _, err := c.mainDispatcher.GetTarget(vchannel); err == nil {
			// current dispatcher didn't allow multiple subscriptions on same vchannel at same time
			log.Warn("unreachable: vchannel has been registered in main dispatcher, ")
			return nil, fmt.Errorf("vchannel has been registered in main dispatcher")
		}
	}

	isMain := c.mainDispatcher == nil
	d, err := NewDispatcher(ctx, c.factory, isMain, c.pchannel, streamConfig.Pos, c.constructSubName(vchannel, isMain), streamConfig.SubPos, c.lagNotifyChan, c.lagTargets, false)
	if err != nil {
		return nil, err
	}
	t := newTarget(vchannel, streamConfig.Pos, streamConfig.ReplicateConfig)
	d.AddTarget(t)
	if isMain {
		c.mainDispatcher = d
		log.Info("add main dispatcher")
	} else {
		c.soloDispatchers[vchannel] = d
		log.Info("add solo dispatcher")
	}
	d.Handle(start)
	return t.ch, nil
}

func (c *dispatcherManager) Remove(vchannel string) {
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mainDispatcher != nil {
		c.mainDispatcher.Handle(pause)
		c.mainDispatcher.CloseTarget(vchannel)
		if c.mainDispatcher.TargetNum() == 0 && len(c.soloDispatchers) == 0 {
			c.mainDispatcher.Handle(terminate)
			c.mainDispatcher = nil
		} else {
			c.mainDispatcher.Handle(resume)
		}
	}
	if _, ok := c.soloDispatchers[vchannel]; ok {
		c.soloDispatchers[vchannel].Handle(terminate)
		c.soloDispatchers[vchannel].CloseTarget(vchannel)
		delete(c.soloDispatchers, vchannel)
		c.deleteMetric(vchannel)
		log.Info("remove soloDispatcher done")
	}
	c.lagTargets.GetAndRemove(vchannel)
}

func (c *dispatcherManager) Num() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var res int
	if c.mainDispatcher != nil {
		res++
	}
	return res + len(c.soloDispatchers)
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
			c.tryMerge()
		case <-c.lagNotifyChan:
			c.mu.Lock()
			c.lagTargets.Range(func(vchannel string, t *target) bool {
				c.split(t)
				c.lagTargets.GetAndRemove(vchannel)
				return true
			})
			c.mu.Unlock()
		}
	}
}

func (c *dispatcherManager) tryMerge() {
	log := log.With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID))

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mainDispatcher == nil || c.mainDispatcher.CurTs() == 0 {
		return
	}
	candidates := make(map[string]struct{})
	for vchannel, sd := range c.soloDispatchers {
		if sd.CurTs() == c.mainDispatcher.CurTs() {
			candidates[vchannel] = struct{}{}
		}
	}
	if len(candidates) == 0 {
		return
	}

	log.Info("start merging...", zap.Any("vchannel", candidates))
	c.mainDispatcher.Handle(pause)
	for vchannel := range candidates {
		c.soloDispatchers[vchannel].Handle(pause)
		// after pause, check alignment again, if not, evict it and try to merge next time
		if c.mainDispatcher.CurTs() != c.soloDispatchers[vchannel].CurTs() {
			c.soloDispatchers[vchannel].Handle(resume)
			delete(candidates, vchannel)
		}
	}
	mergeTs := c.mainDispatcher.CurTs()
	for vchannel := range candidates {
		t, err := c.soloDispatchers[vchannel].GetTarget(vchannel)
		if err == nil {
			c.mainDispatcher.AddTarget(t)
		}
		c.soloDispatchers[vchannel].Handle(terminate)
		delete(c.soloDispatchers, vchannel)
		c.deleteMetric(vchannel)
	}
	c.mainDispatcher.Handle(resume)
	log.Info("merge done", zap.Any("vchannel", candidates), zap.Uint64("mergeTs", mergeTs))
}

func (c *dispatcherManager) split(t *target) {
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("vchannel", t.vchannel))
	log.Info("start splitting...")

	// remove stale soloDispatcher if it existed
	if _, ok := c.soloDispatchers[t.vchannel]; ok {
		c.soloDispatchers[t.vchannel].Handle(terminate)
		delete(c.soloDispatchers, t.vchannel)
		c.deleteMetric(t.vchannel)
	}

	var newSolo *Dispatcher
	err := retry.Do(context.Background(), func() error {
		var err error
		newSolo, err = NewDispatcher(context.Background(), c.factory, false, c.pchannel, t.pos, c.constructSubName(t.vchannel, false), common.SubscriptionPositionUnknown, c.lagNotifyChan, c.lagTargets, true)
		return err
	}, retry.Attempts(10))
	if err != nil {
		log.Error("split failed", zap.Error(err))
		panic(err)
	}
	newSolo.AddTarget(t)
	c.soloDispatchers[t.vchannel] = newSolo
	newSolo.Handle(start)
	log.Info("split done")
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
		// for main dispatcher, use pchannel as channel label
		gauge.WithLabelValues(nodeIDStr, c.pchannel).Set(
			float64(time.Since(tsoutil.PhysicalTime(c.mainDispatcher.CurTs())).Milliseconds()))
		// for solo dispatchers, use vchannel as channel label
		for vchannel, dispatcher := range c.soloDispatchers {
			gauge.WithLabelValues(nodeIDStr, vchannel).Set(
				float64(time.Since(tsoutil.PhysicalTime(dispatcher.CurTs())).Milliseconds()))
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
