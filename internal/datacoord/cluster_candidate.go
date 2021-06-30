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

package datacoord

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
)

// candidateManager manages data node candidates
type candidateManager struct {
	candidatePool sync.Map                         // current processing candidates
	taskQueue     chan candidate                   // task queue to notify workers
	cancel        func()                           // global cancel func
	validate      func(*datapb.DataNodeInfo) error // candidate validation
	enable        func(*datapb.DataNodeInfo) error // enable operation if candidate validate
}

// candidate stands for datanode info from etcd
// it needs to be validated before put into cluster
// since etcd key has a lease timeout of 10 seconds
type candidate struct {
	key    string               // key to specify candidate, usually candidate address
	node   *datapb.DataNodeInfo // node info
	ctx    context.Context      //context obj to control validation process
	cancel func()               // cancel func to cancel single candidate
}

// newCandidateManager create candidate with specified worker number
func newCandidateManager(wn int, validate, enable func(*datapb.DataNodeInfo) error) *candidateManager {
	if wn <= 0 {
		wn = 20
	}
	ctx, cancel := context.WithCancel(context.Background())
	cm := &candidateManager{
		candidatePool: sync.Map{},
		cancel:        cancel,
		taskQueue:     make(chan candidate, wn), // wn * 2 cap, wn worker & wn buffer
		validate:      validate,
		enable:        enable,
	}
	for i := 0; i < wn; i++ {
		//start worker
		go cm.work(ctx)
	}
	return cm
}

// work processes the candidates from channel
// each task can be cancel by candidate contex or by global cancel fund
func (cm *candidateManager) work(ctx context.Context) {
	for {
		select {
		case cand := <-cm.taskQueue:
			ch := make(chan struct{})
			var err error
			go func() {
				err = cm.validate(cand.node)
				ch <- struct{}{}
			}()
			select {
			case <-ch:
				if err == nil {
					cm.enable(cand.node) // success, enable candidate
				} else {
					log.Warn("[CM] candidate failed", zap.String("addr", cand.node.Address))
				}
			case <-cand.ctx.Done():
			}
			cm.candidatePool.Delete(cand.key) // remove from candidatePool
		case <-ctx.Done():
			return
		}
	}
}

// add datanode into candidate pool
// the operation is non-blocking
func (cm *candidateManager) add(dn *datapb.DataNodeInfo) {
	log.Warn("[CM]add new candidate", zap.String("addr", dn.Address))
	key := dn.Address
	ctx, cancel := context.WithCancel(context.Background())
	cand := candidate{
		key:    key,
		node:   dn,
		ctx:    ctx,
		cancel: cancel,
	}
	_, loaded := cm.candidatePool.LoadOrStore(key, cand)
	if !loaded {
		go func() { // start goroutine to non-blocking add into queue
			cm.taskQueue <- cand
		}()
	}
}

// stop the candidate validation process if it exists in the pool
func (cm *candidateManager) stop(key string) {
	val, loaded := cm.candidatePool.LoadAndDelete(key)
	if loaded {
		cand, ok := val.(candidate)
		if ok {
			cand.cancel()
		}
	}
}

// dispose the manager for stopping app
func (cm *candidateManager) dispose() {
	cm.cancel()
}
