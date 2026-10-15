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

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

	wg         sync.WaitGroup
	stopOnce   sync.Once
	planningMu sync.Mutex
}

func NewCheckerController(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	broker meta.Broker,
) *CheckerController {
	// Placement runs in one ordered loop. Index and leader maintenance keep
	// independent timers because they do not reserve placement in this ledger.
	checkers := map[utils.CheckerType]Checker{
		utils.ChannelChecker: NewChannelChecker(meta, dist, targetMgr, nodeMgr, scheduler),
		utils.SegmentChecker: NewSegmentChecker(meta, dist, targetMgr, nodeMgr, scheduler),
		utils.BalanceChecker: NewBalanceChecker(meta, dist, targetMgr, nodeMgr, scheduler),
		utils.IndexChecker:   NewIndexChecker(meta, dist, broker, nodeMgr, targetMgr),
		utils.LeaderChecker:  NewLeaderChecker(meta, dist, targetMgr, nodeMgr),
	}

	placementCheckCh := make(chan struct{}, 1)
	manualCheckChs := map[utils.CheckerType]chan struct{}{
		utils.ChannelChecker: placementCheckCh,
		utils.SegmentChecker: placementCheckCh,
		utils.BalanceChecker: placementCheckCh,
	}

	return &CheckerController{
		manualCheckChs: manualCheckChs,
		meta:           meta,
		dist:           dist,
		targetMgr:      targetMgr,
		scheduler:      scheduler,
		checkers:       checkers,
		broker:         broker,
		nodeMgr:        nodeMgr,
	}
}

func (controller *CheckerController) Start() {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel is stored and called in Stop()
	controller.cancel = cancel

	for typ := range controller.checkers {
		if typ == utils.ChannelChecker || typ == utils.SegmentChecker || typ == utils.BalanceChecker {
			continue
		}
		controller.wg.Add(1)
		go func(checker utils.CheckerType) {
			defer controller.wg.Done()
			controller.startChecker(ctx, checker)
		}(typ)
	}
	controller.wg.Add(1)
	go func() {
		defer controller.wg.Done()
		controller.startPlacementChecker(ctx)
	}()
}

// The two recovery checks define the placement cadence. Balance checks are
// limited by their own configured interval, even if recovery checks run faster.
func getPlacementInterval() time.Duration {
	return min(getCheckerInterval(utils.ChannelChecker), getCheckerInterval(utils.SegmentChecker))
}

func (controller *CheckerController) startPlacementChecker(ctx context.Context) {
	interval := getPlacementInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastBalanceCheck time.Time
	manualCheckCh := controller.manualCheckChs[utils.ChannelChecker]
	for {
		manual := false
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-manualCheckCh:
			manual = true
		}
		if manual || time.Since(lastBalanceCheck) >= getCheckerInterval(utils.BalanceChecker) {
			controller.checkPlacement(ctx, true)
			lastBalanceCheck = time.Now()
		} else {
			controller.checkPlacement(ctx, false)
		}
		if updated := getPlacementInterval(); updated != interval {
			interval = updated
			ticker.Reset(interval)
		}
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

	handleCheck := func() {
		controller.check(ctx, checker)
		newInterval := getCheckerInterval(checker)
		if newInterval != interval {
			interval = newInterval
			// drain once to avoid immediate tick after Reset
			select {
			case <-ticker.C:
			default:
			}
			ticker.Reset(interval)
		}
	}

	for {
		select {
		case <-ctx.Done():
			mlog.Info(ctx, "Checker stopped",
				mlog.String("type", checker.String()))
			return

		case <-ticker.C:
			handleCheck()

		case <-controller.manualCheckChs[checker]:
			handleCheck()
		}
	}
}

func (controller *CheckerController) Stop() {
	controller.stopOnce.Do(func() {
		if controller.cancel != nil {
			controller.cancel()
		}

		controller.wg.Wait()
	})
}

func (controller *CheckerController) Check() {
	checkCh := controller.manualCheckChs[utils.ChannelChecker]
	select {
	case checkCh <- struct{}{}:
	default:
	}
}

// checkPlacement admits recovery in dependency order, then gives ordinary
// balance only the resource groups with no recovery demand or unfinished work.
// Keep planning and scheduler admission in one serialized invocation: each
// checker takes a fresh projected snapshot of the work accepted before it.
func (controller *CheckerController) checkPlacement(ctx context.Context, checkBalance bool) {
	controller.planningMu.Lock()
	defer controller.planningMu.Unlock()
	if controller.nodeMgr != nil && !controller.nodeMgr.LoadReportSyncReady() {
		return
	}

	blocked := make(map[string]struct{})
	for _, kind := range []utils.CheckerType{utils.ChannelChecker, utils.SegmentChecker} {
		checker := controller.checkers[kind]
		if checker == nil {
			continue
		}
		var works []task.Task
		if recovery, ok := checker.(interface {
			CheckWithRecovery(context.Context) ([]task.Task, map[string]struct{})
		}); ok {
			var pending map[string]struct{}
			works, pending = recovery.CheckWithRecovery(ctx)
			for rg := range pending {
				blocked[rg] = struct{}{}
			}
		} else {
			works = checker.Check(ctx)
		}
		controller.admit(works)
	}
	if !checkBalance {
		return
	}
	if inspector, ok := controller.scheduler.(interface {
		PendingRecoveryResourceGroups() map[string]struct{}
	}); ok {
		for rg := range inspector.PendingRecoveryResourceGroups() {
			blocked[rg] = struct{}{}
		}
	}
	for rg := range controller.remoteLoadingBlockedResourceGroups(ctx) {
		blocked[rg] = struct{}{}
	}
	if checker := controller.checkers[utils.BalanceChecker]; checker != nil {
		controller.admit(checker.Check(WithRecoveryBlockedRGs(ctx, blocked)))
	}
}

// A restarted coordinator has no local epoch for loads still running on QNs.
// Their source can remain in distribution, so the recovery diff alone cannot
// distinguish a settled RG from one with an unfinished remote Move. Also keep
// unknown reports out of ordinary balance until a fresh report or node removal
// resolves them. Recovery and stopping evacuation do not use this gate.
func (controller *CheckerController) remoteLoadingBlockedResourceGroups(ctx context.Context) map[string]struct{} {
	blocked := make(map[string]struct{})
	if controller.meta == nil || controller.nodeMgr == nil {
		return blocked
	}
	blockedNodes := make(map[int64]struct{})
	for _, node := range controller.nodeMgr.GetAll() {
		if !node.LoadAdmissionReady() || len(node.LoadResourceReport().GetLoadingSegments()) > 0 {
			blockedNodes[node.ID()] = struct{}{}
		}
	}
	if len(blockedNodes) == 0 {
		return blocked
	}
	// One physical node may still be an RO member of a previous RG while it
	// joins another replica/RG. Protect every current replica membership,
	// regardless of which collection owns the reported load.
	for _, collectionID := range controller.meta.GetAll(ctx) {
		for _, replica := range controller.meta.GetByCollection(ctx, collectionID) {
			for _, nodeID := range replica.GetNodes() {
				if _, pending := blockedNodes[nodeID]; pending {
					blocked[replica.GetResourceGroup()] = struct{}{}
					break
				}
			}
		}
	}
	// Legacy QNs become admission-ready from a full distribution without a
	// loading report. Their remote in-flight work cannot be reconstructed.
	return blocked
}

func (controller *CheckerController) admit(works []task.Task) {
	for _, work := range works {
		err := controller.scheduler.Add(work)
		if err != nil {
			assign.UndoTask(work.Context(), work)
			work.Cancel(err)
			continue
		}
		assign.CommitTask(work.Context(), work)
	}
}

// check is the real implementation of Check
func (controller *CheckerController) check(ctx context.Context, checkType utils.CheckerType) {
	// Placement checkers must observe each other's accepted work before taking
	// the next resource snapshot, so hold the lock through scheduler admission.
	// Index reopens and leader-view updates do not reserve placement in this
	// ledger; their metadata RPCs must not block segment/channel recovery.
	switch checkType {
	case utils.SegmentChecker, utils.ChannelChecker, utils.BalanceChecker:
		controller.planningMu.Lock()
		defer controller.planningMu.Unlock()
	}
	if controller.nodeMgr != nil && !controller.nodeMgr.LoadReportSyncReady() {
		return
	}
	checker := controller.checkers[checkType]
	controller.admit(checker.Check(ctx))
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
