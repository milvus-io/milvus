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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// UnHealthyLevel represents the health level of a system.
type UnHealthyLevel int

const (
	// Healthy means the system is operating normally.
	Healthy UnHealthyLevel = iota
	// Warning indicates minor issues that might escalate.
	Warning
	// Critical indicates major issues that need immediate attention.
	Critical
	// Fatal indicates system failure.
	Fatal
)

// String returns the string representation of the UnHealthyLevel.
func (u UnHealthyLevel) String() string {
	switch u {
	case Healthy:
		return "Healthy"
	case Warning:
		return "Warning"
	case Critical:
		return "Critical"
	case Fatal:
		return "Fatal"
	default:
		return "Unknown"
	}
}

type Item int

const (
	ChannelsWatched Item = iota
	CheckpointLagExceed
	CollectionQueryable
	TimeTickLagExceed
	NodeHealthCheck
)

func getUnhealthyLevel(item Item) UnHealthyLevel {
	switch item {
	case ChannelsWatched:
		return Fatal
	case CheckpointLagExceed:
		return Fatal
	case TimeTickLagExceed:
		return Fatal
	case NodeHealthCheck:
		return Fatal
	case CollectionQueryable:
		return Critical
	default:
		panic(fmt.Sprintf("unknown health check item: %d", int(item)))
	}
}

type Result struct {
	UnhealthyClusterMsgs    []*UnhealthyClusterMsg    `json:"unhealthy_cluster_msgs"`
	UnhealthyCollectionMsgs []*UnhealthyCollectionMsg `json:"unhealthy_collection_msgs"`
}

func NewResult() *Result {
	return &Result{}
}

func (r *Result) AppendUnhealthyClusterMsg(unm *UnhealthyClusterMsg) {
	r.UnhealthyClusterMsgs = append(r.UnhealthyClusterMsgs, unm)
}

func (r *Result) AppendUnhealthyCollectionMsgs(udm *UnhealthyCollectionMsg) {
	r.UnhealthyCollectionMsgs = append(r.UnhealthyCollectionMsgs, udm)
}

func (r *Result) AppendResult(other *Result) {
	if other == nil {
		return
	}
	r.UnhealthyClusterMsgs = append(r.UnhealthyClusterMsgs, other.UnhealthyClusterMsgs...)
	r.UnhealthyCollectionMsgs = append(r.UnhealthyCollectionMsgs, other.UnhealthyCollectionMsgs...)
}

func (r *Result) IsEmpty() bool {
	return len(r.UnhealthyClusterMsgs) == 0 && len(r.UnhealthyCollectionMsgs) == 0
}

func (r *Result) IsHealthy() bool {
	if len(r.UnhealthyClusterMsgs) == 0 && len(r.UnhealthyCollectionMsgs) == 0 {
		return true
	}

	for _, unm := range r.UnhealthyClusterMsgs {
		if unm.Reason.UnhealthyLevel == Fatal {
			return false
		}
	}

	for _, ucm := range r.UnhealthyCollectionMsgs {
		if ucm.Reason.UnhealthyLevel == Fatal {
			return false
		}
	}

	return true
}

type UnhealthyReason struct {
	UnhealthyMsg   string         `json:"unhealthy_msg"`
	UnhealthyLevel UnHealthyLevel `json:"unhealthy_level"`
}

type UnhealthyClusterMsg struct {
	Role   string           `json:"role"`
	NodeID int64            `json:"node_id"`
	Reason *UnhealthyReason `json:"reason"`
}

func NewUnhealthyClusterMsg(role string, nodeID int64, unhealthyMsg string, item Item) *UnhealthyClusterMsg {
	return &UnhealthyClusterMsg{
		Role:   role,
		NodeID: nodeID,
		Reason: &UnhealthyReason{
			UnhealthyMsg:   unhealthyMsg,
			UnhealthyLevel: getUnhealthyLevel(item),
		},
	}
}

type UnhealthyCollectionMsg struct {
	DatabaseID   int64            `json:"database_id"`
	CollectionID int64            `json:"collection_id"`
	Reason       *UnhealthyReason `json:"reason"`
}

func NewUnhealthyCollectionMsg(collectionID int64, unhealthyMsg string, item Item) *UnhealthyCollectionMsg {
	return &UnhealthyCollectionMsg{
		CollectionID: collectionID,
		Reason: &UnhealthyReason{
			UnhealthyMsg:   unhealthyMsg,
			UnhealthyLevel: getUnhealthyLevel(item),
		},
	}
}

type Checker struct {
	sync.RWMutex
	interval     time.Duration
	done         chan struct{}
	checkFn      func() *Result
	latestResult *Result
	once         sync.Once
}

func NewChecker(interval time.Duration, checkFn func() *Result) *Checker {
	checker := &Checker{
		interval:     interval,
		checkFn:      checkFn,
		latestResult: NewResult(),
		done:         make(chan struct{}, 1),
		once:         sync.Once{},
	}
	return checker
}

func (hc *Checker) Start() {
	go func() {
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
	}()
}

func (hc *Checker) GetLatestCheckResult() *Result {
	hc.RLock()
	defer hc.RUnlock()
	return hc.latestResult
}

func (hc *Checker) Close() {
	hc.once.Do(func() {
		close(hc.done)
	})
}

func GetHealthCheckResultFromResp(resp *milvuspb.CheckHealthResponse) *Result {
	var r Result
	if len(resp.Reasons) == 0 {
		return &r
	}
	if len(resp.Reasons) > 1 {
		log.Error("invalid check result", zap.Any("reasons", resp.Reasons))
		return &r
	}

	err := json.Unmarshal([]byte(resp.Reasons[0]), &r)
	if err != nil {
		log.Error("unmarshal check result error", zap.String("error", err.Error()))
	}
	return &r
}

func GetCheckHealthResponseFromClusterMsg(msg ...*UnhealthyClusterMsg) *milvuspb.CheckHealthResponse {
	r := &Result{UnhealthyClusterMsgs: msg}
	reasons, err := json.Marshal(r)
	if err != nil {
		log.Error("marshal check result error", zap.String("error", err.Error()))
	}
	return &milvuspb.CheckHealthResponse{
		Status:    merr.Success(),
		IsHealthy: r.IsHealthy(),
		Reasons:   []string{string(reasons)},
	}
}

func GetCheckHealthResponseFromResult(checkResult *Result) *milvuspb.CheckHealthResponse {
	if checkResult.IsEmpty() {
		return OK()
	}

	reason, err := json.Marshal(checkResult)
	if err != nil {
		log.Error("marshal check result error", zap.String("error", err.Error()))
	}

	return &milvuspb.CheckHealthResponse{
		Status:    merr.Success(),
		IsHealthy: checkResult.IsHealthy(),
		Reasons:   []string{string(reason)},
	}
}

func OK() *milvuspb.CheckHealthResponse {
	return &milvuspb.CheckHealthResponse{Status: merr.Success(), IsHealthy: true, Reasons: []string{}}
}
