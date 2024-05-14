package datacoord

import (
	"fmt"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
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
	taskGuard     sync.RWMutex

	planHandler *compactionPlanHandler
	cluster     Cluster
}

var _ Scheduler = (*CompactionScheduler)(nil)

func NewCompactionScheduler(cluster Cluster) *CompactionScheduler {
	return &CompactionScheduler{
		taskNumber:    atomic.NewInt32(0),
		queuingTasks:  make([]*compactionTask, 0),
		parallelTasks: make(map[int64][]*compactionTask),
		cluster:       cluster,
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
	s.taskGuard.Lock()
	if len(s.queuingTasks) == 0 {
		s.taskGuard.Unlock()
		return nil // To mitigate the need for frequent slot querying
	}
	s.taskGuard.Unlock()

	nodeSlots := s.cluster.QuerySlots()

	l0ChannelExcludes := typeutil.NewSet[string]()
	mixChannelExcludes := typeutil.NewSet[string]()

	for _, tasks := range s.parallelTasks {
		for _, t := range tasks {
			switch t.plan.GetType() {
			case datapb.CompactionType_Level0DeleteCompaction:
				l0ChannelExcludes.Insert(t.plan.GetChannel())
			case datapb.CompactionType_MixCompaction:
				mixChannelExcludes.Insert(t.plan.GetChannel())
			}
		}
	}

	s.taskGuard.Lock()
	defer s.taskGuard.Unlock()

	picked := make([]*compactionTask, 0)
	for _, t := range s.queuingTasks {
		nodeID := s.pickAnyNode(nodeSlots)
		if nodeID == NullNodeID {
			log.Warn("cannot find datanode for compaction task",
				zap.Int64("planID", t.plan.PlanID), zap.String("vchannel", t.plan.Channel))
			continue
		}
		switch t.plan.GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			if l0ChannelExcludes.Contain(t.plan.GetChannel()) ||
				mixChannelExcludes.Contain(t.plan.GetChannel()) {
				continue
			}
			t.dataNodeID = nodeID
			picked = append(picked, t)
			l0ChannelExcludes.Insert(t.plan.GetChannel())
			nodeSlots[nodeID]--
		case datapb.CompactionType_MixCompaction:
			if l0ChannelExcludes.Contain(t.plan.GetChannel()) {
				continue
			}
			t.dataNodeID = nodeID
			picked = append(picked, t)
			mixChannelExcludes.Insert(t.plan.GetChannel())
			nodeSlots[nodeID]--
		}
	}

	var pickPlans []int64
	for _, task := range picked {
		node := task.dataNodeID
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

	return picked
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

func (s *CompactionScheduler) pickAnyNode(nodeSlots map[int64]int64) int64 {
	var (
		nodeID   int64 = NullNodeID
		maxSlots int64 = -1
	)
	for id, slots := range nodeSlots {
		if slots > 0 && slots > maxSlots {
			nodeID = id
			maxSlots = slots
		}
	}
	return nodeID
}

func (s *CompactionScheduler) pickShardNode(nodeID int64, nodeSlots map[int64]int64) int64 {
	if nodeSlots[nodeID] > 0 {
		return nodeID
	}
	return NullNodeID
}
