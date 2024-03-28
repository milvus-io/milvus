package metricsutil

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var schedulerOnce sync.Once

// StartScheduler starts the scheduler.
func StartScheduler() {
	schedulerOnce.Do(func() {
		newScheduler().Run()
	})
}

type metricManager interface {
	Apply(*sampler)
}

// scheduler is the scheduler of the metric package, organizing the metric managers and sampler together.
// Some metric is too large to be handled at prometheus,
// so we need to sample the metrics and aggregate them, then put them to prometheus.
type scheduler struct {
	ctx            context.Context
	cancel         context.CancelFunc
	metricManagers []metricManager
	sampler        *sampler
}

// newScheduler creates a new scheduler.
func newScheduler() *scheduler {
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	ctx, cancel := context.WithCancel(context.Background())
	return &scheduler{
		ctx:    ctx,
		cancel: cancel,
		metricManagers: []metricManager{
			newLoadedSegmentMetricManager(nodeID), // active segment metric aggregation.
			newActiveSegmentMetricManager(nodeID), // hot segment metric aggregation.
		},
		sampler: newSampler(),
	}
}

// Run runs the scheduler.
func (s *scheduler) Run() {
	go func() {
		for {
			interval := paramtable.Get().QueryNodeCfg.MetricScrapeInterval.GetAsDuration(time.Second)

			select {
			case <-s.ctx.Done():
				return
			case <-time.After(interval):
			}

			// scrape the metrics and apply to all metric managers.
			s.sampler.Scrape()
			for _, m := range s.metricManagers {
				m.Apply(s.sampler)
			}
		}
	}()
}

// Stop stops the scheduler.
// It seems that the scheduler need not to be stopped.
func (s *scheduler) Stop() {
	s.cancel()
}
