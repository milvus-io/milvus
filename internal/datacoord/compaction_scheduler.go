package datacoord

import (
	"sync"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Scheduler interface {
	Submit(t ...*compactionTask)

	// Start()
	// Stop()
	// IsFull() bool
	// GetCompactionTasksBySignalID(signalID int64) []compactionTask
}

type CompactionScheduler struct {
	taskNumber    *atomic.Int32
	queuingTasks  []*compactionTask
	parallelTasks map[int64][]*compactionTask // parallel by nodeID
	mu            sync.RWMutex

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
	s.mu.Lock()
	s.queuingTasks = append(s.queuingTasks, tasks...)
	s.mu.Unlock()

	s.taskNumber.Add(int32(len(tasks)))
	s.logStatus()
}

// schedule pick 1 or 0 tasks for 1 node
func (s *CompactionScheduler) schedule() []*compactionTask {
	nodeTasks := make(map[int64][]*compactionTask) // nodeID

	s.mu.Lock()
	defer s.mu.Unlock()
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

func (s *CompactionScheduler) finish(nodeID, planID UniqueID) {
	s.mu.Lock()
	if parallel, ok := s.parallelTasks[nodeID]; ok {
		tasks := lo.Filter(parallel, func(t *compactionTask, _ int) bool {
			return t.plan.PlanID != planID
		})
		s.parallelTasks[nodeID] = tasks
		s.taskNumber.Dec()
	}
	s.mu.Unlock()

	log.Info("Compaction finished", zap.Int64("planID", planID), zap.Int64("nodeID", nodeID))
	s.logStatus()
}

func (s *CompactionScheduler) logStatus() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	waiting := lo.Map(s.queuingTasks, func(t *compactionTask, _ int) int64 {
		return t.plan.PlanID
	})

	var executing []int64
	for _, tasks := range s.parallelTasks {
		executing = append(executing, lo.Map(tasks, func(t *compactionTask, _ int) int64 {
			return t.plan.PlanID
		})...)
	}

	if len(waiting) > 0 || len(executing) > 0 {
		log.Info("Compaction scheduler status", zap.Int64s("waiting", waiting), zap.Int64s("executing", executing))
	}
}

func (s *CompactionScheduler) getExecutingTaskNum() int {
	return int(s.taskNumber.Load())
}
