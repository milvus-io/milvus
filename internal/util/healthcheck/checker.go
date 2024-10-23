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

package healthcheck

import (
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type Result struct {
	unhealthyNodeMsgs       []UnhealthyNodeMsg
	unhealthyCollectionMsgs []UnhealthyCollectionMsg
}

func NewResult() Result {
	return Result{
		unhealthyNodeMsgs:       make([]UnhealthyNodeMsg, 0),
		unhealthyCollectionMsgs: make([]UnhealthyCollectionMsg, 0),
	}
}

func (r *Result) AppendUnhealthyNodeMsg(unm UnhealthyNodeMsg) {
	r.unhealthyNodeMsgs = append(r.unhealthyNodeMsgs, unm)
}

func (r *Result) AppendUnhealthyCollectionMsgs(udm UnhealthyCollectionMsg) {
	r.unhealthyCollectionMsgs = append(r.unhealthyCollectionMsgs, udm)
}

func (r *Result) IsHealthy() bool {
	return len(r.unhealthyNodeMsgs) == 0 && len(r.unhealthyCollectionMsgs) == 0
}

func (r *Result) collectUnhealthyReasons() []string {
	nodeUnHealthyMsgs := lo.Map(r.unhealthyNodeMsgs, func(unm UnhealthyNodeMsg, i int) string {
		return unm.UnhealthyReason()
	})
	collectionUnHealthyMsgs := lo.Map(r.unhealthyCollectionMsgs, func(udm UnhealthyCollectionMsg, i int) string {
		return udm.UnhealthyReason()
	})
	return append(nodeUnHealthyMsgs, collectionUnHealthyMsgs...)
}

type UnhealthyNodeMsg struct {
	Role         string
	NodeID       int64
	UnhealthyMsg string
}

func (unm *UnhealthyNodeMsg) UnhealthyReason() string {
	return fmt.Sprintf("role:%s, nodeID:%d, unhealthy reason:%s", unm.Role, unm.NodeID, unm.UnhealthyMsg)
}

// UnhealthyCollectionMsg TODO check and collect health information of the collection
type UnhealthyCollectionMsg struct {
	CollectionID int64
	UnhealthyMsg string
}

func (ucm *UnhealthyCollectionMsg) UnhealthyReason() string {
	return fmt.Sprintf("databaseID:%d, unhealthy reason:%s", ucm.CollectionID, ucm.UnhealthyMsg)
}

type Checker struct {
	sync.RWMutex
	interval     time.Duration
	done         chan struct{}
	checkFn      func() Result
	latestResult Result
	once         sync.Once
}

func NewChecker(interval time.Duration, checkFn func() Result) *Checker {
	checker := &Checker{
		interval:     interval,
		checkFn:      checkFn,
		latestResult: NewResult(),
		done:         make(chan struct{}, 1),
		once:         sync.Once{},
	}
	go checker.start()
	return checker
}

func (hc *Checker) start() {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()
	log.Info("start health checker")
	for {
		select {
		case <-ticker.C:
			hc.Lock()
			hc.latestResult = hc.checkFn()
			hc.Unlock()
		case <-hc.done:
			log.Info("stop health checker")
			return
		}
	}
}

func (hc *Checker) GetLatestCheckResult() Result {
	hc.RLock()
	defer hc.RUnlock()
	return hc.latestResult
}

func (hc *Checker) Close() {
	hc.once.Do(func() {
		close(hc.done)
	})
}

func GetCheckHealthResponseFrom(checkResult *Result) *milvuspb.CheckHealthResponse {
	return &milvuspb.CheckHealthResponse{
		Status:    merr.Success(),
		IsHealthy: checkResult.IsHealthy(),
		Reasons:   checkResult.collectUnhealthyReasons(),
	}
}
