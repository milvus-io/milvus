package checkers

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"go.uber.org/zap"
)

var (
	checkRoundTaskNumLimit = 128
)

type CheckerController struct {
	stopCh    chan struct{}
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	broker    *meta.CoordinatorBroker
	nodeMgr   *session.NodeManager
	balancer  balance.Balance

	scheduler task.Scheduler
	checkers  []Checker
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
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		scheduler: scheduler,
		checkers:  checkers,
	}
}

func (controller *CheckerController) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(Params.QueryCoordCfg.CheckInterval)
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
			}
		}
	}()
}

func (controller *CheckerController) Stop() {
	close(controller.stopCh)
}

// check is the real implementation of Check
func (controller *CheckerController) check(ctx context.Context) {
	tasks := make([]task.Task, 0)
	for id, checker := range controller.checkers {
		log := log.With(zap.Int("checkerID", id))

		tasks = append(tasks, checker.Check(ctx)...)
		if len(tasks) >= checkRoundTaskNumLimit {
			log.Info("checkers have spawn too many tasks, won't run subsequent checkers, and truncate the spawned tasks",
				zap.Int("taskNum", len(tasks)),
				zap.Int("taskNumLimit", checkRoundTaskNumLimit))
			tasks = tasks[:checkRoundTaskNumLimit]
			break
		}
	}

	for _, task := range tasks {
		controller.scheduler.Add(task)
	}
}
