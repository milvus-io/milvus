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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

var errTypeNotFound = errors.New("checker type not found")

type GetBalancerFunc = func() balance.Balance

type CheckerController struct {
	cancel         context.CancelFunc
	manualCheckChs map[utils.CheckerType]chan struct{}
	meta           *meta.Meta
	dist           *meta.DistributionManager
	targetMgr      meta.TargetManagerInterface
	broker         meta.Broker
	nodeMgr        *session.NodeManager
	balancer       balance.Balance

	scheduler task.Scheduler
	checkers  map[utils.CheckerType]Checker

	stopOnce sync.Once
}

func NewCheckerController(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	broker meta.Broker,
	getBalancerFunc GetBalancerFunc,
) *CheckerController {
	// CheckerController runs checkers with the order,
	// the former checker has higher priority
	checkers := map[utils.CheckerType]Checker{
		utils.ChannelChecker: NewChannelChecker(meta, dist, targetMgr, nodeMgr, getBalancerFunc),
		utils.SegmentChecker: NewSegmentChecker(meta, dist, targetMgr, nodeMgr, getBalancerFunc),
		utils.BalanceChecker: NewBalanceChecker(meta, targetMgr, nodeMgr, scheduler, getBalancerFunc),
		utils.IndexChecker:   NewIndexChecker(meta, dist, broker, nodeMgr, targetMgr),
		// todo temporary work around must fix
		// utils.LeaderChecker:  NewLeaderChecker(meta, dist, targetMgr, nodeMgr, true),
		utils.LeaderChecker: NewLeaderChecker(meta, dist, targetMgr, nodeMgr),
	}

	manualCheckChs := map[utils.CheckerType]chan struct{}{
		utils.ChannelChecker: make(chan struct{}, 1),
		utils.SegmentChecker: make(chan struct{}, 1),
		utils.BalanceChecker: make(chan struct{}, 1),
	}

	return &CheckerController{
		manualCheckChs: manualCheckChs,
		meta:           meta,
		dist:           dist,
		targetMgr:      targetMgr,
		scheduler:      scheduler,
		checkers:       checkers,
		broker:         broker,
	}
}

func (controller *CheckerController) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	controller.cancel = cancel

	for checker := range controller.checkers {
		go controller.startChecker(ctx, checker)
	}
}

func getCheckerInterval(checker utils.CheckerType) time.Duration {
	switch checker {
	case utils.SegmentChecker:
		return Params.QueryCoordCfg.SegmentCheckInterval.GetAsDuration(time.Millisecond)
	case utils.ChannelChecker:
		return Params.QueryCoordCfg.ChannelCheckInterval.GetAsDuration(time.Millisecond)
	case utils.BalanceChecker:
		return Params.QueryCoordCfg.BalanceCheckInterval.GetAsDuration(time.Millisecond)
	case utils.IndexChecker:
		return Params.QueryCoordCfg.IndexCheckInterval.GetAsDuration(time.Millisecond)
	case utils.LeaderChecker:
		return Params.QueryCoordCfg.LeaderViewUpdateInterval.GetAsDuration(time.Second)
	default:
		return Params.QueryCoordCfg.CheckInterval.GetAsDuration(time.Millisecond)
	}
}

func (controller *CheckerController) startChecker(ctx context.Context, checker utils.CheckerType) {
	interval := getCheckerInterval(checker)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Checker stopped",
				zap.String("type", checker.String()))
			return

		case <-ticker.C:
			controller.check(ctx, checker)

		case <-controller.manualCheckChs[checker]:
			ticker.Stop()
			controller.check(ctx, checker)
			ticker.Reset(interval)
		}
	}
}

func (controller *CheckerController) Stop() {
	controller.stopOnce.Do(func() {
		if controller.cancel != nil {
			controller.cancel()
		}
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
func (controller *CheckerController) check(ctx context.Context, checkType utils.CheckerType) {
	checker := controller.checkers[checkType]
	tasks := checker.Check(ctx)

	for _, task := range tasks {
		err := controller.scheduler.Add(task)
		if err != nil {
			task.Cancel(err)
			continue
		}
	}
}

func (controller *CheckerController) Deactivate(typ utils.CheckerType) error {
	for _, checker := range controller.checkers {
		if checker.ID() == typ {
			checker.Deactivate()
			return nil
		}
	}
	return errTypeNotFound
}

func (controller *CheckerController) Activate(typ utils.CheckerType) error {
	for _, checker := range controller.checkers {
		if checker.ID() == typ {
			checker.Activate()
			return nil
		}
	}
	return errTypeNotFound
}

func (controller *CheckerController) IsActive(typ utils.CheckerType) (bool, error) {
	for _, checker := range controller.checkers {
		if checker.ID() == typ {
			return checker.IsActive(), nil
		}
	}
	return false, errTypeNotFound
}

func (controller *CheckerController) Checkers() []Checker {
	checkers := make([]Checker, 0, len(controller.checkers))
	for _, checker := range controller.checkers {
		checkers = append(checkers, checker)
	}
	return checkers
}
