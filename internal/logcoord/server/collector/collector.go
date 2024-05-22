package collector

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/logcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/lognode/client/manager"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/layout"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

func NewCollector(logNodeManager manager.ManagerClient, balancer balancer.Balancer) *Collector {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Collector{
		ctx:            ctx,
		cancel:         cancel,
		finish:         make(chan struct{}),
		trigger:        make(chan struct{}, 1),
		rater:          rate.NewLimiter(rate.Every(time.Second), 1),
		errCounter:     make(map[int64]int),
		balancer:       balancer,
		logNodeManager: logNodeManager,
	}
	return c
}

type Collector struct {
	ctx     context.Context
	cancel  context.CancelFunc
	finish  chan struct{}
	trigger chan struct{}
	rater   *rate.Limiter

	balancer       balancer.Balancer
	logNodeManager manager.ManagerClient
	errCounter     map[int64]int
}

func (c *Collector) Start() {
	go c.execute()
}

func (c *Collector) Stop() {
	c.cancel()
	<-c.finish
}

// Start a Collector operation right now.
func (c *Collector) Trigger() {
	select {
	case c.trigger <- struct{}{}:
	default:
	}
}

func (c *Collector) execute() {
	// TODO: configurable.
	ticker := time.NewTicker(10 * time.Second)
	defer func() {
		ticker.Stop()
		log.Info("collector stopped")
		close(c.finish)
	}()
	nodeChangeWatcher := c.logNodeManager.WatchNodeChanged(c.ctx)

	for {
		select {
		case <-nodeChangeWatcher:
			c.collectStatusAndUpdate()
		case <-ticker.C:
			c.collectStatusAndUpdate()
		case <-c.trigger:
			c.collectStatusAndUpdate()
		case <-c.ctx.Done():
			return
		}
	}
}

// collectStatusAndUpdate collects status from lognode and update the status to balancer.
func (c *Collector) collectStatusAndUpdate() {
	if err := c.rater.Wait(c.ctx); err != nil {
		return
	}

	// Collect all lognode status.
	status, err := c.logNodeManager.CollectAllStatus(c.ctx)
	if err != nil {
		log.Warn("collect all lognode status failed", zap.Error(err))
		return
	}

	// Collect all log node status error status.
	for serverID, s := range status {
		status[serverID] = c.checkErrCounter(serverID, s)
	}

	// remove counter for lognodes that not exists.
	for serverID := range c.errCounter {
		if _, ok := status[serverID]; !ok {
			delete(c.errCounter, serverID)
		}
	}

	// Update status to balancer.
	log.Debug("update lognode status to balancer", zap.Any("status", status))
	c.balancer.UpdateLogNodeStatus(status)
}

// checkErrCounter checks the error counter of lognode and return the modified status.
func (c *Collector) checkErrCounter(serverID int64, status *layout.NodeStatus) *layout.NodeStatus {
	if status.Error != nil {
		if _, ok := c.errCounter[serverID]; !ok {
			c.errCounter[serverID] = 0
		}
		c.errCounter[serverID]++
	} else {
		// cleanup error counter.
		c.errCounter[serverID] = 0
	}

	// If error counter is greater than 3, mark the node as freeze.
	// TODO: configurable.
	if c.errCounter[serverID] > 3 {
		status.Error = errors.Mark(status.Error, layout.ErrFreeze)
	}
	return status
}
