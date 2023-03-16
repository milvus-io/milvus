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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

var (
	checkRoundTaskNumLimit = 256
)

type CheckerController struct {
	stopCh    chan struct{}
	checkCh   chan struct{}
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	broker    *meta.CoordinatorBroker
	nodeMgr   *session.NodeManager
	balancer  balance.Balance

	scheduler task.Scheduler
	checkers  []Checker

	stopOnce sync.Once
}

func NewCheckerController(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	balancer balance.Balance,
	scheduler task.Scheduler) *CheckerController {

	// CheckerController runs checkers with the order,
	// the former checker has higher priority
	checkers := []Checker{
		NewChannelChecker(meta, dist, targetMgr, balancer),
		NewSegmentChecker(meta, dist, targetMgr, balancer),
		NewBalanceChecker(balancer),
	}
	for i, checker := range checkers {
		checker.SetID(int64(i + 1))
	}

	return &CheckerController{
		stopCh:    make(chan struct{}),
		checkCh:   make(chan struct{}),
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		scheduler: scheduler,
		checkers:  checkers,
	}
}

func (controller *CheckerController) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(Params.QueryCoordCfg.CheckInterval.GetAsDuration(time.Millisecond))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Info("CheckerController stopped due to context canceled")
				return

			case <-controller.stopCh:
				log.Info("CheckerController stopped")
				return

			case <-ticker.C:
				controller.check(ctx)

			case <-controller.checkCh:
				ticker.Stop()
				controller.check(ctx)
				ticker.Reset(Params.QueryCoordCfg.CheckInterval.GetAsDuration(time.Millisecond))
			}
		}
	}()
}

func (controller *CheckerController) Stop() {
	controller.stopOnce.Do(func() {
		close(controller.stopCh)
	})
}

func (controller *CheckerController) Check() {
	controller.checkCh <- struct{}{}
}

// check is the real implementation of Check
func (controller *CheckerController) check(ctx context.Context) {
	tasks := make([]task.Task, 0)
	for _, checker := range controller.checkers {
		tasks = append(tasks, checker.Check(ctx)...)
	}

	for _, task := range tasks {
		err := controller.scheduler.Add(task)
		if err != nil {
			task.Cancel(err)
			continue
		}
	}
}
