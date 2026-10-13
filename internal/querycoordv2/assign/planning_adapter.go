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

package assign

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type pendingSegmentLoadInspector interface {
	PendingSegmentLoads() map[int64][]*querypb.LoadingSegmentInfo
}

// NewPlanningLedger is captured once before generating plans. Real schedulers
// expose per-object pending loads; the aggregate fallback is for legacy mocks
// and cannot establish exact object-level deduplication of QN reports.
func NewPlanningLedger(nodes *session.NodeManager, dist *meta.DistributionManager, scheduler task.Scheduler, maxPending int) *PlanningLedger {
	states := make(map[int64]PlanningNode)
	collections := make(map[int64]struct{})
	var ids []int64
	var loaded []PlanningLoadKey
	var pending, reported []PlanningLoad
	for _, node := range nodes.GetAll() {
		ids = append(ids, node.ID())
		state := PlanningNode{Ready: node.GetState() == session.NodeStateNormal && node.LoadAdmissionReady() && !nodes.IsResourceExhausted(node.ID()),
			CapacityBytes: int64(node.MemCapacity() * 1024 * 1024), CollectionRows: make(map[int64]int64),
			PendingCollectionRows: make(map[int64]int64), CollectionChannels: make(map[int64]int64), PendingCollectionChannels: make(map[int64]int64)}
		if report := node.LoadResourceReport(); report != nil {
			state.ReclaimableMemory = report.GetTieredEvictionEnabled()
			state.MemoryUsageBytes = planningUnsignedBytes(report.GetMemoryUsageBytes())
			state.DiskUsageBytes = planningUnsignedBytes(report.GetDiskUsageBytes())
			state.ReportedLoadingMemory = planningUnsignedBytes(report.GetLoadingMemoryBytes())
			state.ReportedLoadingDisk = planningUnsignedBytes(report.GetLoadingDiskBytes())
			for _, info := range report.GetLoadingSegments() {
				reported = append(reported, planningReportedLoad(node.ID(), info))
				collections[info.GetCollectionId()] = struct{}{}
			}
		}
		states[node.ID()] = state
	}
	if dist != nil {
		for _, segment := range dist.SegmentDistManager.GetByFilter() {
			state, ok := states[segment.Node]
			if !ok {
				continue
			}
			state.Rows += segment.GetNumOfRows()
			state.CollectionRows[segment.GetCollectionID()] += segment.GetNumOfRows()
			states[segment.Node] = state
			loaded = append(loaded, PlanningLoadKey{NodeID: segment.Node, CollectionID: segment.GetCollectionID(), SegmentID: segment.GetID()})
			collections[segment.GetCollectionID()] = struct{}{}
		}
		for _, channel := range dist.ChannelDistManager.GetByFilter() {
			state, ok := states[channel.Node]
			if !ok {
				continue
			}
			state.Channels++
			state.CollectionChannels[channel.GetCollectionID()]++
			if channel.View != nil {
				state.Rows += channel.View.NumOfGrowingRows
				state.CollectionRows[channel.GetCollectionID()] += channel.View.NumOfGrowingRows
			}
			states[channel.Node] = state
			collections[channel.GetCollectionID()] = struct{}{}
		}
	}
	if inspector, ok := scheduler.(pendingSegmentLoadInspector); ok {
		for nodeID, infos := range inspector.PendingSegmentLoads() {
			for _, info := range infos {
				pending = append(pending, planningReportedLoad(nodeID, info))
				collections[info.GetCollectionId()] = struct{}{}
			}
		}
	}
	if scheduler != nil {
		global := scheduler.GetSegmentTaskDeltaSnapshot(ids, -1)
		for _, id := range ids {
			state := states[id]
			state.PendingRows = int64(global.GetByNode(id))
			state.PendingChannels = int64(scheduler.GetChannelTaskDelta(id, -1))
			states[id] = state
		}
		for collectionID := range collections {
			delta := scheduler.GetSegmentTaskDeltaSnapshot(ids, collectionID)
			for _, id := range ids {
				state := states[id]
				state.PendingCollectionRows[collectionID] = int64(delta.GetByNodeInCollection(id))
				state.PendingCollectionChannels[collectionID] = int64(scheduler.GetChannelTaskDelta(id, collectionID))
				states[id] = state
			}
		}
	}
	return NewPlanningLedgerState(states, loaded, pending, reported, maxPending)
}

func planningReportedLoad(nodeID int64, info *querypb.LoadingSegmentInfo) PlanningLoad {
	return PlanningLoad{Key: PlanningLoadKey{NodeID: nodeID, CollectionID: info.GetCollectionId(), SegmentID: info.GetSegmentId()},
		From: -1, Rows: info.GetNumRows(), MemoryBytes: planningUnsignedBytes(info.GetEstimatedMemoryBytes()), DiskBytes: planningUnsignedBytes(info.GetEstimatedDiskBytes())}
}

func planningUnsignedBytes(bytes uint64) int64 {
	return int64(min(bytes, uint64(math.MaxInt64)))
}

func ConvertSegmentNodeItems(ctx context.Context, policy ScoreAwareAssignPolicy, collectionID int64, nodes []int64) map[int64]*NodeItem {
	if aware, ok := policy.(interface {
		ConvertToNodeItemsBySegmentWithContext(context.Context, int64, []int64) map[int64]*NodeItem
	}); ok {
		return aware.ConvertToNodeItemsBySegmentWithContext(ctx, collectionID, nodes)
	}
	return policy.ConvertToNodeItemsBySegment(collectionID, nodes)
}

func ConvertChannelNodeItems(ctx context.Context, policy ScoreAwareAssignPolicy, collectionID int64, nodes []int64) map[int64]*NodeItem {
	if aware, ok := policy.(interface {
		ConvertToNodeItemsByChannelWithContext(context.Context, int64, []int64) map[int64]*NodeItem
	}); ok {
		return aware.ConvertToNodeItemsByChannelWithContext(ctx, collectionID, nodes)
	}
	return policy.ConvertToNodeItemsByChannel(collectionID, nodes)
}

func UndoTask(ctx context.Context, balanceTask task.Task) {
	changeTaskReservation(ctx, balanceTask, false)
}

func CommitTask(ctx context.Context, balanceTask task.Task) {
	changeTaskReservation(ctx, balanceTask, true)
}

func changeTaskReservation(ctx context.Context, balanceTask task.Task, commit bool) {
	ledger := PlanningLedgerFromContext(ctx)
	if ledger == nil {
		return
	}
	for _, action := range balanceTask.Actions() {
		if action.Type() != task.ActionTypeGrow {
			continue
		}
		switch grow := action.(type) {
		case *task.SegmentAction:
			key := PlanningLoadKey{NodeID: grow.Node(), CollectionID: balanceTask.CollectionID(), SegmentID: grow.GetSegmentID()}
			if commit {
				ledger.Commit(key)
			} else {
				ledger.Undo(key)
			}
		case *task.ChannelAction:
			ledger.mu.Lock()
			for key, reservation := range ledger.channels {
				if key.collection == balanceTask.CollectionID() && key.channel == grow.GetShard() && key.to == grow.Node() {
					if commit {
						reservation.committed = true
						ledger.channels[key] = reservation
					} else if !reservation.committed {
						delete(ledger.channels, key)
					}
				}
			}
			ledger.mu.Unlock()
		}
	}
}

func PlanningSegmentEstimate(segment *meta.Segment) int64 {
	memory, _ := planningSegmentResources(segment)
	return memory
}

// Available binlog metadata is advisory: it omits native transient peaks and
// may omit index/cache residency. The outstanding-Grow cap still bounds work
// when these estimates are unknown; QueryNode enforces its real admission.
func planningSegmentResources(segment *meta.Segment) (memory, disk int64) {
	if segment == nil {
		return 0, 0
	}
	for _, fields := range [][]*datapb.FieldBinlog{segment.GetBinlogs(), segment.GetStatslogs(), segment.GetDeltalogs()} {
		for _, field := range fields {
			for _, binlog := range field.GetBinlogs() {
				disk = saturatingPlanningAdd(disk, max(int64(0), binlog.GetLogSize()))
				memory = saturatingPlanningAdd(memory, max(int64(0), max(binlog.GetMemorySize(), binlog.GetLogSize())))
			}
		}
	}
	if stats := segment.GetStats(); stats != nil {
		memory = max(memory, saturatingPlanningAdd(max(int64(0), stats.GetInsertBinlogSize()), saturatingPlanningAdd(max(int64(0), stats.GetStatsBinlogSize()), max(int64(0), stats.GetDeltaBinlogSize()))))
	}
	return memory, disk
}

func segmentPlanningLoad(plan SegmentAssignPlan) PlanningLoad {
	from := plan.From
	if from <= 0 && plan.Segment.Node > 0 {
		from = plan.Segment.Node
	}
	memory, disk := planningSegmentResources(plan.Segment)
	return PlanningLoad{Key: PlanningLoadKey{NodeID: plan.To, CollectionID: plan.Segment.GetCollectionID(), SegmentID: plan.Segment.GetID()},
		From: from, Rows: plan.Segment.GetNumOfRows(), MemoryBytes: memory, DiskBytes: disk}
}

func (l *PlanningLedger) ReserveSegment(plan SegmentAssignPlan) bool {
	return l.Reserve(segmentPlanningLoad(plan))
}
func (l *PlanningLedger) UndoSegment(plan SegmentAssignPlan) bool {
	return l.Undo(segmentPlanningLoad(plan).Key)
}
func (l *PlanningLedger) CommitSegment(plan SegmentAssignPlan) {
	l.Commit(segmentPlanningLoad(plan).Key)
}

func (l *PlanningLedger) UndoSegmentByID(collectionID, segmentID, nodeID int64) bool {
	return l.Undo(PlanningLoadKey{NodeID: nodeID, CollectionID: collectionID, SegmentID: segmentID})
}

func (l *PlanningLedger) CommitSegmentByID(collectionID, segmentID, nodeID int64) {
	l.Commit(PlanningLoadKey{NodeID: nodeID, CollectionID: collectionID, SegmentID: segmentID})
}

func popPlanningSegmentTarget(ctx context.Context, queue *PriorityQueue, segment *meta.Segment) *NodeItem {
	ledger := PlanningLedgerFromContext(ctx)
	if ledger == nil {
		return queue.Pop().(*NodeItem)
	}
	var blocked []*NodeItem
	defer func() {
		for _, node := range blocked {
			queue.Push(node)
		}
	}()
	if ledger.HasLoading(segment.GetCollectionID(), segment.GetID(), planningQueueNodes(queue)) {
		return nil
	}
	for queue.Len() > 0 {
		node := queue.Pop().(*NodeItem)
		if ledger.CanReserve(segmentPlanningLoad(SegmentAssignPlan{Segment: segment, From: -1, To: node.NodeID})) {
			return node
		}
		blocked = append(blocked, node)
	}
	return nil
}

func planningQueueNodes(queue *PriorityQueue) []int64 {
	var nodes []int64
	for _, item := range queue.heapQueue {
		nodes = append(nodes, item.(*NodeItem).NodeID)
	}
	return nodes
}

func planningChannelIdentity(plan ChannelAssignPlan) planningChannelKey {
	from := plan.From
	if from <= 0 && plan.Channel.Node > 0 {
		from = plan.Channel.Node
	}
	return planningChannelKey{collection: plan.Channel.GetCollectionID(), from: from, to: plan.To, channel: plan.Channel.GetChannelName()}
}

func (l *PlanningLedger) ReserveChannel(plan ChannelAssignPlan) bool {
	key := planningChannelIdentity(plan)
	l.mu.Lock()
	defer l.mu.Unlock()
	if key.from == key.to || !l.nodes[key.to].Ready {
		return false
	}
	for existing := range l.channels {
		if existing.collection == key.collection && existing.channel == key.channel && existing.from == key.from {
			return false
		}
	}
	rows := int64(0)
	if plan.Channel.View != nil {
		rows = plan.Channel.View.NumOfGrowingRows
	}
	l.channels[key] = planningChannelReservation{rows: rows}
	return true
}

func (l *PlanningLedger) UndoChannel(plan ChannelAssignPlan) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := planningChannelIdentity(plan)
	reservation, ok := l.channels[key]
	if !ok || reservation.committed {
		return false
	}
	delete(l.channels, key)
	return true
}

func (l *PlanningLedger) CommitChannel(plan ChannelAssignPlan) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := planningChannelIdentity(plan)
	reservation, ok := l.channels[key]
	if ok {
		reservation.committed = true
		l.channels[key] = reservation
	}
}

// AssignRecoverySegments uses largest-first predicted rows/capacity and a hard
// outstanding-Grow cap. It intentionally does not rotate nodes round-robin.
func AssignRecoverySegments(_ context.Context, ledger *PlanningLedger, segments []*meta.Segment, nodes []int64, batch int) []SegmentAssignPlan {
	loads := make([]PlanningLoad, 0, len(segments))
	bySegment := make(map[PlanningLoadKey]*meta.Segment, len(segments))
	for _, segment := range segments {
		load := segmentPlanningLoad(SegmentAssignPlan{Segment: segment, From: -1, To: -1})
		loads = append(loads, load)
		bySegment[load.Key] = segment
	}
	reserved := ledger.PlanRecoveryLoads(loads, nodes, batch)
	plans := make([]SegmentAssignPlan, 0, len(reserved))
	for _, load := range reserved {
		key := load.Key
		key.NodeID = -1
		plans = append(plans, SegmentAssignPlan{Segment: bySegment[key], From: -1, To: load.Key.NodeID})
	}
	return plans
}
