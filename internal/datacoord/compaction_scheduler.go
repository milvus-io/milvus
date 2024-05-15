package datacoord

import (
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Scheduler interface {
	Submit(t ...*compactionTask)
	Schedule() []*compactionTask
	Finish(nodeID int64, plan *datapb.CompactionPlan)
	GetTaskCount() int
	LogStatus()

	// Start()
	// Stop()
	// IsFull() bool
	// GetCompactionTasksBySignalID(signalID int64) []compactionTask
}

type CompactionScheduler struct {
	taskNumber *atomic.Int32

	queuingTasks  []*compactionTask
	parallelTasks map[int64][]*compactionTask // parallel by nodeID
	taskGuard     lock.RWMutex

	planHandler *compactionPlanHandler
}

var _ Scheduler = (*CompactionScheduler)(nil)

func NewCompactionScheduler() *CompactionScheduler {
	return &CompactionScheduler{
		taskNumber:    atomic.NewInt32(0),
		queuingTasks:  make([]*compactionTask, 0),
		parallelTasks: make(map[int64][]*compactionTask),
	}
}

func (s *CompactionScheduler) Submit(tasks ...*compactionTask) {
	s.taskGuard.Lock()
	s.queuingTasks = append(s.queuingTasks, tasks...)
	s.taskGuard.Unlock()

	s.taskNumber.Add(int32(len(tasks)))
	lo.ForEach(tasks, func(t *compactionTask, _ int) {
		metrics.DataCoordCompactionTaskNum.
			WithLabelValues(fmt.Sprint(t.dataNodeID), t.plan.GetType().String(), metrics.Pending).Inc()
	})
	s.LogStatus()
}

// Schedule pick 1 or 0 tasks for 1 node
func (s *CompactionScheduler) Schedule() []*compactionTask {
	nodeTasks := make(map[int64][]*compactionTask) // nodeID

	s.taskGuard.Lock()
	defer s.taskGuard.Unlock()
	for _, task := range s.queuingTasks {
		if _, ok := nodeTasks[task.dataNodeID]; !ok {
			nodeTasks[task.dataNodeID] = make([]*compactionTask, 0)
		}

		nodeTasks[task.dataNodeID] = append(nodeTasks[task.dataNodeID], task)
	}

	executable := make(map[int64]*compactionTask)

	pickPriorPolicy := func(tasks []*compactionTask, exclusiveChannels []string, executing []string) *compactionTask {
		for _, task := range tasks {
			if lo.Contains(exclusiveChannels, task.plan.GetChannel()) {
				continue
			}

			if task.plan.GetType() == datapb.CompactionType_Level0DeleteCompaction {
				// Channel of LevelZeroCompaction task with no executing compactions
				if !lo.Contains(executing, task.plan.GetChannel()) {
					return task
				}

				// Don't schedule any tasks for channel with LevelZeroCompaction task
				// when there're executing compactions
				exclusiveChannels = append(exclusiveChannels, task.plan.GetChannel())
				continue
			}

			return task
		}

		return nil
	}

	// pick 1 or 0 task for 1 node
	for node, tasks := range nodeTasks {
		parallel := s.parallelTasks[node]
		if len(parallel) >= calculateParallel() {
			log.Info("Compaction parallel in DataNode reaches the limit", zap.Int64("nodeID", node), zap.Int("parallel", len(parallel)))
			continue
		}

		var (
			executing         = typeutil.NewSet[string]()
			channelsExecPrior = typeutil.NewSet[string]()
		)
		for _, t := range parallel {
			executing.Insert(t.plan.GetChannel())
			if t.plan.GetType() == datapb.CompactionType_Level0DeleteCompaction {
				channelsExecPrior.Insert(t.plan.GetChannel())
			}
		}

		picked := pickPriorPolicy(tasks, channelsExecPrior.Collect(), executing.Collect())
		if picked != nil {
			executable[node] = picked
		}
	}

	var pickPlans []int64
	for node, task := range executable {
		pickPlans = append(pickPlans, task.plan.PlanID)
		if _, ok := s.parallelTasks[node]; !ok {
			s.parallelTasks[node] = []*compactionTask{task}
		} else {
			s.parallelTasks[node] = append(s.parallelTasks[node], task)
		}
		metrics.DataCoordCompactionTaskNum.
			WithLabelValues(fmt.Sprint(node), task.plan.GetType().String(), metrics.Executing).Inc()
		metrics.DataCoordCompactionTaskNum.
			WithLabelValues(fmt.Sprint(node), task.plan.GetType().String(), metrics.Pending).Dec()
	}

	s.queuingTasks = lo.Filter(s.queuingTasks, func(t *compactionTask, _ int) bool {
		return !lo.Contains(pickPlans, t.plan.PlanID)
	})

	// clean parallelTasks with nodes of no running tasks
	for node, tasks := range s.parallelTasks {
		if len(tasks) == 0 {
			delete(s.parallelTasks, node)
		}
	}

	return lo.Values(executable)
}

func (s *CompactionScheduler) Finish(nodeID UniqueID, plan *datapb.CompactionPlan) {
	planID := plan.GetPlanID()
	log := log.With(zap.Int64("planID", planID), zap.Int64("nodeID", nodeID))

	s.taskGuard.Lock()
	if parallel, ok := s.parallelTasks[nodeID]; ok {
		tasks := lo.Filter(parallel, func(t *compactionTask, _ int) bool {
			return t.plan.PlanID != planID
		})
		s.parallelTasks[nodeID] = tasks
		s.taskNumber.Dec()
		metrics.DataCoordCompactionTaskNum.
			WithLabelValues(fmt.Sprint(nodeID), plan.GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.
			WithLabelValues(fmt.Sprint(nodeID), plan.GetType().String(), metrics.Done).Inc()
		log.Info("Compaction scheduler remove task from executing")
	}

	filtered := lo.Filter(s.queuingTasks, func(t *compactionTask, _ int) bool {
		return t.plan.PlanID != planID
	})
	if len(filtered) < len(s.queuingTasks) {
		s.queuingTasks = filtered
		s.taskNumber.Dec()
		metrics.DataCoordCompactionTaskNum.
			WithLabelValues(fmt.Sprint(nodeID), plan.GetType().String(), metrics.Pending).Dec()
		metrics.DataCoordCompactionTaskNum.
			WithLabelValues(fmt.Sprint(nodeID), plan.GetType().String(), metrics.Done).Inc()
		log.Info("Compaction scheduler remove task from queue")
	}

	s.taskGuard.Unlock()
	s.LogStatus()
}

func (s *CompactionScheduler) LogStatus() {
	s.taskGuard.RLock()
	defer s.taskGuard.RUnlock()

	if s.GetTaskCount() > 0 {
		waiting := lo.Map(s.queuingTasks, func(t *compactionTask, _ int) int64 {
			return t.plan.PlanID
		})

		var executing []int64
		for _, tasks := range s.parallelTasks {
			executing = append(executing, lo.Map(tasks, func(t *compactionTask, _ int) int64 {
				return t.plan.PlanID
			})...)
		}

		log.Info("Compaction scheduler status", zap.Int64s("waiting", waiting), zap.Int64s("executing", executing))
	}
}

func (s *CompactionScheduler) GetTaskCount() int {
	return int(s.taskNumber.Load())
}
