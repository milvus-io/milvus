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
	"github.com/milvus-io/milvus/pkg/log"
)

const (
	segmentCheckerName = "segment_checker"
	channelCheckerName = "channel_checker"
	balanceCheckerName = "balance_checker"
	indexCheckerName   = "index_checker"
)

type CheckerType int32

const (
	channelChecker CheckerType = iota + 1
	segmentChecker
	balanceChecker
	indexChecker
)

var (
	checkRoundTaskNumLimit = 256
	checkerOrder           = []string{channelCheckerName, segmentCheckerName, balanceCheckerName, indexCheckerName}
	checkerNames           = map[CheckerType]string{
		segmentChecker: segmentCheckerName,
		channelChecker: channelCheckerName,
		balanceChecker: balanceCheckerName,
		indexChecker:   indexCheckerName,
	}
	errTypeNotFound = errors.New("checker type not found")
)

func (s CheckerType) String() string {
	return checkerNames[s]
}

type CheckerController struct {
	cancel         context.CancelFunc
	manualCheckChs map[CheckerType]chan struct{}
	meta           *meta.Meta
	dist           *meta.DistributionManager
	targetMgr      *meta.TargetManager
	broker         meta.Broker
	nodeMgr        *session.NodeManager
	balancer       balance.Balance

	scheduler task.Scheduler
	checkers  map[CheckerType]Checker

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
	checkers := map[CheckerType]Checker{
		channelChecker: NewChannelChecker(meta, dist, targetMgr, balancer),
		segmentChecker: NewSegmentChecker(meta, dist, targetMgr, balancer, nodeMgr),
		balanceChecker: NewBalanceChecker(meta, balancer, nodeMgr, scheduler),
		indexChecker:   NewIndexChecker(meta, dist, broker, nodeMgr),
	}

	manualCheckChs := map[CheckerType]chan struct{}{
		channelChecker: make(chan struct{}, 1),
		segmentChecker: make(chan struct{}, 1),
		balanceChecker: make(chan struct{}, 1),
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

func getCheckerInterval(checker CheckerType) time.Duration {
	switch checker {
	case segmentChecker:
		return Params.QueryCoordCfg.SegmentCheckInterval.GetAsDuration(time.Millisecond)
	case channelChecker:
		return Params.QueryCoordCfg.ChannelCheckInterval.GetAsDuration(time.Millisecond)
	case balanceChecker:
		return Params.QueryCoordCfg.BalanceCheckInterval.GetAsDuration(time.Millisecond)
	case indexChecker:
		return Params.QueryCoordCfg.IndexCheckInterval.GetAsDuration(time.Millisecond)
	default:
		return Params.QueryCoordCfg.CheckInterval.GetAsDuration(time.Millisecond)
	}
}

func (controller *CheckerController) startChecker(ctx context.Context, checker CheckerType) {
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
func (controller *CheckerController) check(ctx context.Context, checkType CheckerType) {
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

func (controller *CheckerController) Deactivate(typ CheckerType) error {
	for _, checker := range controller.checkers {
		if checker.ID() == typ {
			checker.Deactivate()
			return nil
		}
	}
	return errTypeNotFound
}

func (controller *CheckerController) Activate(typ CheckerType) error {
	for _, checker := range controller.checkers {
		if checker.ID() == typ {
			checker.Activate()
			return nil
		}
	}
	return errTypeNotFound
}

func (controller *CheckerController) IsActive(typ CheckerType) (bool, error) {
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
