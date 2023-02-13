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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/util/retry"
)

var (
	CheckPeriod = 1 * time.Second // TODO: dyh, move to config
)

type DispatcherManager interface {
	Add(vchannel string, pos *Pos, subPos SubPos) (<-chan *MsgPack, error)
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
	lagTargets    *sync.Map // vchannel -> *target

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
		lagTargets:      &sync.Map{},
		soloDispatchers: make(map[string]*Dispatcher),
		factory:         factory,
		closeChan:       make(chan struct{}),
	}
	return c
}

func (c *dispatcherManager) constructSubName(vchannel string, isMain bool) string {
	return fmt.Sprintf("%s-%d-%s-%t", c.role, c.nodeID, vchannel, isMain)
}

func (c *dispatcherManager) Add(vchannel string, pos *Pos, subPos SubPos) (<-chan *MsgPack, error) {
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))

	c.mu.Lock()
	defer c.mu.Unlock()
	isMain := c.mainDispatcher == nil
	d, err := NewDispatcher(c.factory, isMain, c.pchannel, pos,
		c.constructSubName(vchannel, isMain), subPos, c.lagNotifyChan, c.lagTargets)
	if err != nil {
		return nil, err
	}
	t := newTarget(vchannel, pos)
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
		log.Info("remove soloDispatcher done")
	}
	c.lagTargets.Delete(vchannel)
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
	ticker := time.NewTicker(CheckPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			log.Info("dispatcherManager exited")
			return
		case <-ticker.C:
			c.tryMerge()
		case <-c.lagNotifyChan:
			c.mu.Lock()
			c.lagTargets.Range(func(vchannel, t any) bool {
				c.split(t.(*target))
				c.lagTargets.Delete(vchannel)
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
	if c.mainDispatcher == nil {
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
	for vchannel := range candidates {
		t, err := c.soloDispatchers[vchannel].GetTarget(vchannel)
		if err == nil {
			c.mainDispatcher.AddTarget(t)
		}
		c.soloDispatchers[vchannel].Handle(terminate)
		delete(c.soloDispatchers, vchannel)
	}
	c.mainDispatcher.Handle(resume)
	log.Info("merge done", zap.Any("vchannel", candidates))
}

func (c *dispatcherManager) split(t *target) {
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("vchannel", t.vchannel))
	log.Info("start splitting...")

	// remove stale soloDispatcher if it existed
	if _, ok := c.soloDispatchers[t.vchannel]; ok {
		c.soloDispatchers[t.vchannel].Handle(terminate)
		delete(c.soloDispatchers, t.vchannel)
	}

	var newSolo *Dispatcher
	err := retry.Do(context.Background(), func() error {
		var err error
		newSolo, err = NewDispatcher(c.factory, false, c.pchannel, t.pos,
			c.constructSubName(t.vchannel, false), mqwrapper.SubscriptionPositionUnknown, c.lagNotifyChan, c.lagTargets)
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
