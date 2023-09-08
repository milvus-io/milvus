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

package checkers

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

var (
	checkRoundTaskNumLimit = 256
)

var (
	Segment_Checker = "segment_checker"
	Channel_Checker = "channel_checker"
	Balance_Checker = "balance_checker"
	Index_Checker   = "index_checker"
)

type CheckerController struct {
	stopCh         chan struct{}
	manualCheckChs map[string]chan struct{}
	meta           *meta.Meta
	dist           *meta.DistributionManager
	targetMgr      *meta.TargetManager
	broker         meta.Broker
	nodeMgr        *session.NodeManager
	balancer       balance.Balance

	scheduler task.Scheduler
	checkers  map[string]Checker

	stopOnce sync.Once
}

func NewCheckerController(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	balancer balance.Balance,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	broker meta.Broker,
) *CheckerController {

	// CheckerController runs checkers with the order,
	// the former checker has higher priority
	checkers := map[string]Checker{
		Channel_Checker: NewChannelChecker(meta, dist, targetMgr, balancer),
		Segment_Checker: NewSegmentChecker(meta, dist, targetMgr, balancer, nodeMgr),
		Balance_Checker: NewBalanceChecker(meta, balancer, nodeMgr, scheduler),
		Index_Checker:   NewIndexChecker(meta, dist, broker),
	}

	id := 0
	for _, checker := range checkers {
		checker.SetID(int64(id + 1))
	}

	manualCheckChs := map[string]chan struct{}{
		Channel_Checker: make(chan struct{}, 1),
		Segment_Checker: make(chan struct{}, 1),
		Balance_Checker: make(chan struct{}, 1),
	}

	return &CheckerController{
		stopCh:         make(chan struct{}),
		manualCheckChs: manualCheckChs,
		meta:           meta,
		dist:           dist,
		targetMgr:      targetMgr,
		scheduler:      scheduler,
		checkers:       checkers,
		broker:         broker,
	}
}

func (controller *CheckerController) Start(ctx context.Context) {
	for checkerType := range controller.checkers {
		go controller.StartChecker(ctx, checkerType)
	}
}

func getCheckerInterval(checkerType string) time.Duration {
	switch checkerType {
	case Segment_Checker:
		return Params.QueryCoordCfg.SegmentCheckInterval.GetAsDuration(time.Millisecond)
	case Channel_Checker:
		return Params.QueryCoordCfg.ChannelCheckInterval.GetAsDuration(time.Millisecond)
	case Balance_Checker:
		return Params.QueryCoordCfg.BalanceCheckInterval.GetAsDuration(time.Millisecond)
	case Index_Checker:
		return Params.QueryCoordCfg.IndexCheckInterval.GetAsDuration(time.Millisecond)
	default:
		return Params.QueryCoordCfg.CheckInterval.GetAsDuration(time.Millisecond)
	}

}

func (controller *CheckerController) StartChecker(ctx context.Context, checkerType string) {
	interval := getCheckerInterval(checkerType)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Checker stopped due to context canceled",
				zap.String("type", checkerType))
			return

		case <-controller.stopCh:
			log.Info("Checker stopped",
				zap.String("type", checkerType))
			return

		case <-ticker.C:
			controller.check(ctx, checkerType)

		case <-controller.manualCheckChs[checkerType]:
			ticker.Stop()
			controller.check(ctx, checkerType)
			ticker.Reset(interval)
		}
	}
}

func (controller *CheckerController) Stop() {
	controller.stopOnce.Do(func() {
		close(controller.stopCh)
	})
}

func (controller *CheckerController) Check() {
	for _, checkCh := range controller.manualCheckChs {
		select {
		case checkCh <- struct{}{}:
		default:
		}
	}
}

// check is the real implementation of Check
func (controller *CheckerController) check(ctx context.Context, checkerType string) {
	checker := controller.checkers[checkerType]
	tasks := checker.Check(ctx)

	for _, task := range tasks {
		err := controller.scheduler.Add(task)
		if err != nil {
			task.Cancel(err)
			continue
		}
	}
}
