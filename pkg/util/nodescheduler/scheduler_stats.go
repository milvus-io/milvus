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

package nodescheduler

import (
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const schedulerStatsLogInterval = 5 * time.Second

type schedulerStats struct {
	mu         sync.Mutex
	capacity   int
	byTaskType map[string]*schedulerTaskStats
}

type schedulerTaskStats struct {
	queued  int64
	running int64

	submitted int64
	started   int64
	delayed   int64
	completed int64
	failed    int64
	canceled  int64

	queueWaitCount int64
	queueWaitTotal time.Duration
	queueWaitMax   time.Duration
	executeCount   int64
	executeTotal   time.Duration
	executeMax     time.Duration
}

type schedulerStatsSnapshot struct {
	capacity     int
	totalQueued  int64
	totalRunning int64
	tasks        []schedulerTaskStatsSnapshot
}

type schedulerTaskStatsSnapshot struct {
	taskType string
	queued   int64
	running  int64

	submitted int64
	started   int64
	delayed   int64
	completed int64
	failed    int64
	canceled  int64

	queueWaitCount int64
	queueWaitTotal time.Duration
	queueWaitMax   time.Duration
	executeCount   int64
	executeTotal   time.Duration
	executeMax     time.Duration
}

func newSchedulerStats(capacity int) *schedulerStats {
	return &schedulerStats{
		capacity:   capacity,
		byTaskType: make(map[string]*schedulerTaskStats),
	}
}

func (s *schedulerStats) setCapacity(capacity int) {
	s.mu.Lock()
	s.capacity = capacity
	s.mu.Unlock()
}

func (s *schedulerStats) submit(taskType string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.taskStats(taskType)
	stats.queued++
	stats.submitted++
}

func (s *schedulerStats) start(taskType string, queueWait time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.taskStats(taskType)
	stats.queued--
	stats.running++
	stats.started++
	stats.queueWaitCount++
	stats.queueWaitTotal += queueWait
	stats.queueWaitMax = max(stats.queueWaitMax, queueWait)
}

func (s *schedulerStats) finishDelayed(taskType string, executeDuration time.Duration, requeued bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.taskStats(taskType)
	stats.running--
	stats.delayed++
	stats.recordExecution(executeDuration)
	if requeued {
		stats.queued++
	} else {
		stats.canceled++
	}
}

func (s *schedulerStats) finishCompleted(taskType string, executeDuration time.Duration) {
	s.finish(taskType, executeDuration, func(stats *schedulerTaskStats) {
		stats.completed++
	})
}

func (s *schedulerStats) finishFailed(taskType string, executeDuration time.Duration) {
	s.finish(taskType, executeDuration, func(stats *schedulerTaskStats) {
		stats.failed++
	})
}

func (s *schedulerStats) finishCanceled(taskType string, executeDuration time.Duration) {
	s.finish(taskType, executeDuration, func(stats *schedulerTaskStats) {
		stats.canceled++
	})
}

func (s *schedulerStats) cancelStarted(taskType string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.taskStats(taskType)
	stats.running--
	stats.canceled++
}

func (s *schedulerStats) finish(taskType string, executeDuration time.Duration, update func(*schedulerTaskStats)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.taskStats(taskType)
	stats.running--
	stats.recordExecution(executeDuration)
	update(stats)
}

func (s *schedulerStats) cancelQueued(taskType string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.taskStats(taskType)
	stats.queued--
	stats.canceled++
}

func (s *schedulerStats) taskStats(taskType string) *schedulerTaskStats {
	stats := s.byTaskType[taskType]
	if stats == nil {
		stats = &schedulerTaskStats{}
		s.byTaskType[taskType] = stats
	}
	return stats
}

func (s *schedulerStats) snapshotAndReset() schedulerStatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	taskTypes := make([]string, 0, len(s.byTaskType))
	for taskType := range s.byTaskType {
		taskTypes = append(taskTypes, taskType)
	}
	sort.Strings(taskTypes)

	snapshot := schedulerStatsSnapshot{capacity: s.capacity}
	for _, taskType := range taskTypes {
		stats := s.byTaskType[taskType]
		snapshot.totalQueued += stats.queued
		snapshot.totalRunning += stats.running
		if !stats.hasActivity() {
			delete(s.byTaskType, taskType)
			continue
		}
		snapshot.tasks = append(snapshot.tasks, schedulerTaskStatsSnapshot{
			taskType:       taskType,
			queued:         stats.queued,
			running:        stats.running,
			submitted:      stats.submitted,
			started:        stats.started,
			delayed:        stats.delayed,
			completed:      stats.completed,
			failed:         stats.failed,
			canceled:       stats.canceled,
			queueWaitCount: stats.queueWaitCount,
			queueWaitTotal: stats.queueWaitTotal,
			queueWaitMax:   stats.queueWaitMax,
			executeCount:   stats.executeCount,
			executeTotal:   stats.executeTotal,
			executeMax:     stats.executeMax,
		})
		stats.resetWindow()
	}
	return snapshot
}

func (s *schedulerTaskStats) recordExecution(duration time.Duration) {
	s.executeCount++
	s.executeTotal += duration
	s.executeMax = max(s.executeMax, duration)
}

func (s *schedulerTaskStats) hasActivity() bool {
	return s.queued != 0 || s.running != 0 ||
		s.submitted != 0 || s.started != 0 || s.delayed != 0 ||
		s.completed != 0 || s.failed != 0 || s.canceled != 0
}

func (s *schedulerTaskStats) resetWindow() {
	s.submitted = 0
	s.started = 0
	s.delayed = 0
	s.completed = 0
	s.failed = 0
	s.canceled = 0
	s.queueWaitCount = 0
	s.queueWaitTotal = 0
	s.queueWaitMax = 0
	s.executeCount = 0
	s.executeTotal = 0
	s.executeMax = 0
}

func (s schedulerTaskStatsSnapshot) averageQueueWait() time.Duration {
	if s.queueWaitCount == 0 {
		return 0
	}
	return s.queueWaitTotal / time.Duration(s.queueWaitCount)
}

func (s schedulerTaskStatsSnapshot) averageExecution() time.Duration {
	if s.executeCount == 0 {
		return 0
	}
	return s.executeTotal / time.Duration(s.executeCount)
}

func (s *nodeScheduler) reportStats() {
	defer s.reporter.Done()

	ticker := time.NewTicker(schedulerStatsLogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.metrics.refreshNodeID()
			snapshot := s.stats.snapshotAndReset()
			for _, task := range snapshot.tasks {
				mlog.Info(
					s.ctx, "node scheduler stats",
					mlog.String("phase", "node_scheduler"),
					mlog.String("taskType", task.taskType),
					mlog.Int("capacity", snapshot.capacity),
					mlog.Int64("totalQueued", snapshot.totalQueued),
					mlog.Int64("totalRunning", snapshot.totalRunning),
					mlog.Int64("queued", task.queued),
					mlog.Int64("running", task.running),
					mlog.Int64("submitted", task.submitted),
					mlog.Int64("started", task.started),
					mlog.Int64("delayed", task.delayed),
					mlog.Int64("completed", task.completed),
					mlog.Int64("failed", task.failed),
					mlog.Int64("canceled", task.canceled),
					mlog.Duration("avgQueueWait", task.averageQueueWait()),
					mlog.Duration("maxQueueWait", task.queueWaitMax),
					mlog.Duration("avgExecute", task.averageExecution()),
					mlog.Duration("maxExecute", task.executeMax),
				)
			}
		}
	}
}
