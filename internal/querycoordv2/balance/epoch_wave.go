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

package balance

import (
	"fmt"
	"reflect"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type BalanceWaveBudget struct {
	MaxSegmentTasks       int
	MaxChannelTasks       int
	MaxTasksPerNode       int
	MaxTasksPerCollection int
}

type PlanKind int

const (
	PlanKindSegment PlanKind = iota + 1
	PlanKindChannel
)

type EpochPlan struct {
	Kind         PlanKind
	CollectionID int64
	ReplicaID    int64
	Shard        string
	SegmentID    int64
	Channel      string
	Scope        querypb.DataScope
	RowCount     int64
	From         int64
	To           int64
	Token        AdmissionToken
}

type BalanceObjectKey struct {
	Kind      BalanceObjectKind
	ReplicaID int64
	SegmentID int64
	Channel   string
	Scope     querypb.DataScope
}

type BalanceObjectKind int

const (
	BalanceObjectSegment BalanceObjectKind = iota + 1
	BalanceObjectChannel
)

func (p EpochPlan) ObjectKey() BalanceObjectKey {
	switch p.Kind {
	case PlanKindSegment:
		return BalanceObjectKey{
			Kind:      BalanceObjectSegment,
			ReplicaID: p.ReplicaID,
			SegmentID: p.SegmentID,
			Scope:     p.Scope,
		}
	case PlanKindChannel:
		return BalanceObjectKey{
			Kind:      BalanceObjectChannel,
			ReplicaID: p.ReplicaID,
			Channel:   p.Channel,
		}
	default:
		return BalanceObjectKey{}
	}
}

func (p EpochPlan) NodeActions() []int64 {
	return []int64{p.From, p.To}
}

type BalanceWave struct {
	Kind        PlanKind
	Plans       []EpochPlan
	PrefixAfter []ScorePotential
	Decisions   []BalancePlanDecision
	Before      ScorePotential
	After       ScorePotential
	Converged   bool
}

type BalancePlanDecision struct {
	Plan        EpochPlan
	Before      ScorePotential
	After       ScorePotential
	Result      string
	Explanation string
}

type ReservationClass int

const (
	ReservationQuarantineOnly ReservationClass = iota + 1
	ReservationActiveTask
	ReservationAmbiguousCapacity
)

type EpochObjectConstraint struct {
	Object          BalanceObjectKey
	CollectionID    int64
	From            int64
	To              int64
	Class           ReservationClass
	ChargedNodes    []int64
	QuarantineUntil time.Time
}

type EpochPlanningConstraints struct {
	Objects map[BalanceObjectKey]EpochObjectConstraint
}

type ScorePotential struct {
	Value float64
}

func (p ScorePotential) Improves(other ScorePotential) bool {
	const epsilon = 1e-9
	return p.Value+epsilon < other.Value
}

type WaveLedger struct {
	budget BalanceWaveBudget

	segmentTasks   int
	channelTasks   int
	collectionTask map[int64]int
	nodeActions    map[int64]int

	persistentObjects     map[BalanceObjectKey]struct{}
	persistentNodeActions map[int64]int
	reservations          map[BalanceObjectKey]EpochPlan
}

func NewWaveLedger(budget BalanceWaveBudget, constraints EpochPlanningConstraints) *WaveLedger {
	ledger := &WaveLedger{
		budget:                budget,
		collectionTask:        make(map[int64]int),
		nodeActions:           make(map[int64]int),
		persistentObjects:     make(map[BalanceObjectKey]struct{}, len(constraints.Objects)),
		persistentNodeActions: make(map[int64]int),
		reservations:          make(map[BalanceObjectKey]EpochPlan),
	}

	for key, constraint := range constraints.Objects {
		if key != constraint.Object {
			panic(fmt.Sprintf("balance constraint key %v does not match object %v", key, constraint.Object))
		}
		if !validBalanceObjectKey(constraint.Object) {
			panic(fmt.Sprintf("invalid balance constraint object %v", constraint.Object))
		}
		if constraint.Class < ReservationQuarantineOnly || constraint.Class > ReservationAmbiguousCapacity {
			panic(fmt.Sprintf("invalid reservation class %d", constraint.Class))
		}

		chargedNodes := make(map[int64]struct{}, len(constraint.ChargedNodes))
		for _, nodeID := range constraint.ChargedNodes {
			if nodeID <= 0 {
				panic(fmt.Sprintf("invalid charged node %d for object %v", nodeID, constraint.Object))
			}
			chargedNodes[nodeID] = struct{}{}
		}

		ledger.persistentObjects[constraint.Object] = struct{}{}
		if constraint.Class == ReservationActiveTask || constraint.Class == ReservationAmbiguousCapacity {
			for nodeID := range chargedNodes {
				ledger.persistentNodeActions[nodeID]++
			}
		}
	}

	return ledger
}

func (l *WaveLedger) TryReserve(plan EpochPlan) bool {
	if !validEpochPlan(plan) {
		return false
	}
	if l.objectLocked(plan.ObjectKey()) ||
		!l.withinKindLimit(plan) ||
		!l.withinCollectionLimit(plan.CollectionID) ||
		!l.withinNodeLimits(plan.NodeActions()) {
		return false
	}

	l.commit(plan)
	return true
}

func (l *WaveLedger) Release(plan EpochPlan) {
	key := plan.ObjectKey()
	reserved, ok := l.reservations[key]
	if !ok || !reflect.DeepEqual(reserved, plan) {
		panic(fmt.Sprintf("release of unreserved balance plan for object %v", key))
	}

	if l.collectionTask[plan.CollectionID] <= 0 {
		panic(fmt.Sprintf("negative collection reservation for collection %d", plan.CollectionID))
	}
	for _, nodeID := range plan.NodeActions() {
		if l.nodeActions[nodeID] <= 0 {
			panic(fmt.Sprintf("negative node reservation for node %d", nodeID))
		}
	}
	switch plan.Kind {
	case PlanKindSegment:
		if l.segmentTasks <= 0 {
			panic("negative segment reservation")
		}
	case PlanKindChannel:
		if l.channelTasks <= 0 {
			panic("negative channel reservation")
		}
	default:
		panic(fmt.Sprintf("invalid reserved plan kind %d", plan.Kind))
	}

	delete(l.reservations, key)
	l.collectionTask[plan.CollectionID]--
	for _, nodeID := range plan.NodeActions() {
		l.nodeActions[nodeID]--
	}
	if plan.Kind == PlanKindSegment {
		l.segmentTasks--
	} else {
		l.channelTasks--
	}
}

func (l *WaveLedger) SegmentTasks() int {
	return l.segmentTasks
}

func (l *WaveLedger) ChannelTasks() int {
	return l.channelTasks
}

func (l *WaveLedger) CollectionTasks(collectionID int64) int {
	return l.collectionTask[collectionID]
}

func (l *WaveLedger) NodeActions(nodeID int64) int {
	return l.persistentNodeActions[nodeID] + l.nodeActions[nodeID]
}

func (l *WaveLedger) objectLocked(key BalanceObjectKey) bool {
	if _, ok := l.persistentObjects[key]; ok {
		return true
	}
	_, ok := l.reservations[key]
	return ok
}

func (l *WaveLedger) withinKindLimit(plan EpochPlan) bool {
	switch plan.Kind {
	case PlanKindSegment:
		return l.budget.MaxSegmentTasks > 0 && l.segmentTasks < l.budget.MaxSegmentTasks
	case PlanKindChannel:
		return l.budget.MaxChannelTasks > 0 && l.channelTasks < l.budget.MaxChannelTasks
	default:
		return false
	}
}

func (l *WaveLedger) withinCollectionLimit(collectionID int64) bool {
	return l.budget.MaxTasksPerCollection > 0 &&
		l.collectionTask[collectionID] < l.budget.MaxTasksPerCollection
}

func (l *WaveLedger) withinNodeLimits(nodeIDs []int64) bool {
	if l.budget.MaxTasksPerNode <= 0 {
		return false
	}
	for _, nodeID := range nodeIDs {
		if l.NodeActions(nodeID) >= l.budget.MaxTasksPerNode {
			return false
		}
	}
	return true
}

func (l *WaveLedger) commit(plan EpochPlan) {
	key := plan.ObjectKey()
	l.reservations[key] = plan
	l.collectionTask[plan.CollectionID]++
	for _, nodeID := range plan.NodeActions() {
		l.nodeActions[nodeID]++
	}
	if plan.Kind == PlanKindSegment {
		l.segmentTasks++
	} else {
		l.channelTasks++
	}
}

func validEpochPlan(plan EpochPlan) bool {
	if plan.From <= 0 || plan.To <= 0 || plan.From == plan.To {
		return false
	}
	switch plan.Kind {
	case PlanKindSegment:
		return plan.SegmentID != 0 && plan.Channel == ""
	case PlanKindChannel:
		return plan.Channel != "" && plan.SegmentID == 0
	default:
		return false
	}
}

func validBalanceObjectKey(key BalanceObjectKey) bool {
	switch key.Kind {
	case BalanceObjectSegment:
		return key.SegmentID != 0 && key.Channel == ""
	case BalanceObjectChannel:
		return key.Channel != "" && key.SegmentID == 0 && key.Scope == 0
	default:
		return false
	}
}

type ProjectedPlacement struct {
	snapshot PlacementSnapshot
	applied  map[BalanceObjectKey]projectedApplyRecord
}

type projectedApplyRecord struct {
	plan           EpochPlan
	segmentsBefore []SegmentPlacement
	channelsBefore []ChannelPlacement
}

func NewProjectedPlacement(snapshot PlacementSnapshot) *ProjectedPlacement {
	return &ProjectedPlacement{
		snapshot: clonePlacementSnapshot(snapshot),
		applied:  make(map[BalanceObjectKey]projectedApplyRecord),
	}
}

func (p *ProjectedPlacement) Snapshot() PlacementSnapshot {
	return clonePlacementSnapshot(p.snapshot)
}

func (p *ProjectedPlacement) SegmentPlacements(key SegmentObjectKey) []SegmentPlacement {
	return cloneSlice(p.snapshot.Segments[key])
}

func (p *ProjectedPlacement) ChannelPlacements(key ChannelObjectKey) []ChannelPlacement {
	return cloneSlice(p.snapshot.Channels[key])
}

func (p *ProjectedPlacement) SegmentWorkload(nodeID int64) int {
	return p.snapshot.PendingWork.SegmentWorkloadByNode[nodeID]
}

func (p *ProjectedPlacement) ChannelWorkload(nodeID int64) int {
	return p.snapshot.PendingWork.ChannelWorkloadByNode[nodeID]
}

func (p *ProjectedPlacement) Apply(plan EpochPlan) error {
	if !validEpochPlan(plan) {
		return fmt.Errorf("invalid projected balance plan for object %v", plan.ObjectKey())
	}
	key := plan.ObjectKey()
	if _, ok := p.applied[key]; ok {
		return fmt.Errorf("balance object %v already has an outstanding projection", key)
	}

	switch plan.Kind {
	case PlanKindSegment:
		return p.applySegment(plan)
	case PlanKindChannel:
		return p.applyChannel(plan)
	default:
		return fmt.Errorf("invalid projected balance plan kind %d", plan.Kind)
	}
}

func (p *ProjectedPlacement) Undo(plan EpochPlan) {
	key := plan.ObjectKey()
	record, ok := p.applied[key]
	if !ok || !reflect.DeepEqual(record.plan, plan) {
		panic(fmt.Sprintf("undo of unapplied projected balance plan for object %v", key))
	}

	switch plan.Kind {
	case PlanKindSegment:
		segmentKey := SegmentObjectKey{
			ReplicaID: plan.ReplicaID,
			SegmentID: plan.SegmentID,
			Scope:     plan.Scope,
		}
		p.snapshot.Segments[segmentKey] = cloneSlice(record.segmentsBefore)
	case PlanKindChannel:
		channelKey := ChannelObjectKey{
			ReplicaID: plan.ReplicaID,
			Channel:   plan.Channel,
		}
		p.snapshot.Channels[channelKey] = cloneSlice(record.channelsBefore)
	default:
		panic(fmt.Sprintf("invalid applied projected balance plan kind %d", plan.Kind))
	}
	delete(p.applied, key)
}

func (p *ProjectedPlacement) applySegment(plan EpochPlan) error {
	segmentKey := SegmentObjectKey{
		ReplicaID: plan.ReplicaID,
		SegmentID: plan.SegmentID,
		Scope:     plan.Scope,
	}
	placements := p.snapshot.Segments[segmentKey]
	sourceIndex, err := validateSegmentProjection(placements, plan)
	if err != nil {
		return err
	}

	before := cloneSlice(placements)
	next := make([]SegmentPlacement, 0, len(placements))
	for index, placement := range placements {
		if index != sourceIndex {
			next = append(next, placement)
		}
	}
	moved := placements[sourceIndex]
	moved.NodeID = plan.To
	next = append(next, moved)

	p.applied[plan.ObjectKey()] = projectedApplyRecord{
		plan:           plan,
		segmentsBefore: before,
	}
	p.snapshot.Segments[segmentKey] = next
	return nil
}

func (p *ProjectedPlacement) applyChannel(plan EpochPlan) error {
	channelKey := ChannelObjectKey{
		ReplicaID: plan.ReplicaID,
		Channel:   plan.Channel,
	}
	placements := p.snapshot.Channels[channelKey]
	sourceIndex, err := validateChannelProjection(placements, plan)
	if err != nil {
		return err
	}

	before := cloneSlice(placements)
	next := make([]ChannelPlacement, 0, len(placements))
	for index, placement := range placements {
		if index != sourceIndex {
			next = append(next, placement)
		}
	}
	moved := placements[sourceIndex]
	moved.NodeID = plan.To
	next = append(next, moved)

	p.applied[plan.ObjectKey()] = projectedApplyRecord{
		plan:           plan,
		channelsBefore: before,
	}
	p.snapshot.Channels[channelKey] = next
	return nil
}

func validateSegmentProjection(placements []SegmentPlacement, plan EpochPlan) (int, error) {
	sourceIndex := -1
	sourceCount := 0
	for index, placement := range placements {
		if placement.CollectionID != plan.CollectionID {
			return -1, fmt.Errorf("segment %d collection mismatch: placement=%d plan=%d", plan.SegmentID, placement.CollectionID, plan.CollectionID)
		}
		if placement.NodeID == plan.To {
			return -1, fmt.Errorf("segment %d already exists on target node %d", plan.SegmentID, plan.To)
		}
		if placement.NodeID == plan.From {
			sourceCount++
			sourceIndex = index
			if !placement.Present {
				return -1, fmt.Errorf("segment %d source on node %d is not present", plan.SegmentID, plan.From)
			}
		}
	}
	if sourceCount != 1 {
		return -1, fmt.Errorf("segment %d must exist exactly once on source node %d, found %d", plan.SegmentID, plan.From, sourceCount)
	}
	return sourceIndex, nil
}

func validateChannelProjection(placements []ChannelPlacement, plan EpochPlan) (int, error) {
	sourceIndex := -1
	sourceCount := 0
	for index, placement := range placements {
		if placement.CollectionID != plan.CollectionID {
			return -1, fmt.Errorf("channel %q collection mismatch: placement=%d plan=%d", plan.Channel, placement.CollectionID, plan.CollectionID)
		}
		if placement.NodeID == plan.To {
			return -1, fmt.Errorf("channel %q already exists on target node %d", plan.Channel, plan.To)
		}
		if placement.NodeID == plan.From {
			sourceCount++
			sourceIndex = index
			if !placement.Present {
				return -1, fmt.Errorf("channel %q source on node %d is not present", plan.Channel, plan.From)
			}
		}
	}
	if sourceCount != 1 {
		return -1, fmt.Errorf("channel %q must exist exactly once on source node %d, found %d", plan.Channel, plan.From, sourceCount)
	}
	return sourceIndex, nil
}

func clonePlacementSnapshot(snapshot PlacementSnapshot) PlacementSnapshot {
	clone := snapshot
	clone.Token = cloneSnapshotToken(snapshot.Token)
	clone.Nodes = cloneNodes(snapshot.Nodes)
	clone.Replicas = cloneReplicas(snapshot.Replicas)
	clone.Segments = cloneSegments(snapshot.Segments)
	clone.Channels = cloneChannels(snapshot.Channels)
	clone.CollectionTargets = cloneCollectionTargets(snapshot.CollectionTargets)
	clone.PendingWork = clonePendingWork(snapshot.PendingWork)
	clone.EligibleReplicas = cloneEligibleReplicas(snapshot.EligibleReplicas)
	return clone
}

func cloneSnapshotToken(token SnapshotToken) SnapshotToken {
	clone := token
	clone.CurrentTargetVersion = cloneInt64Map(token.CurrentTargetVersion)
	clone.NextTargetVersion = cloneInt64Map(token.NextTargetVersion)
	if token.pendingEpochRevisions != nil {
		clone.pendingEpochRevisions = make(map[task.BalanceEpochMeta]uint64, len(token.pendingEpochRevisions))
		for epoch, revision := range token.pendingEpochRevisions {
			clone.pendingEpochRevisions[epoch] = revision
		}
	}
	return clone
}

func cloneInt64Map(values map[int64]int64) map[int64]int64 {
	if values == nil {
		return nil
	}
	clone := make(map[int64]int64, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func cloneNodes(nodes map[int64]NodeSnapshot) map[int64]NodeSnapshot {
	if nodes == nil {
		return nil
	}
	clone := make(map[int64]NodeSnapshot, len(nodes))
	for nodeID, node := range nodes {
		clone[nodeID] = node
	}
	return clone
}

func cloneReplicas(replicas map[int64]ReplicaSnapshot) map[int64]ReplicaSnapshot {
	if replicas == nil {
		return nil
	}
	clone := make(map[int64]ReplicaSnapshot, len(replicas))
	for replicaID, replica := range replicas {
		replica.RWNodes = cloneSlice(replica.RWNodes)
		replica.RONodes = cloneSlice(replica.RONodes)
		replica.RWSQNodes = cloneSlice(replica.RWSQNodes)
		replica.ROSQNodes = cloneSlice(replica.ROSQNodes)
		if replica.ChannelRWNodes != nil {
			channelNodes := make(map[string][]int64, len(replica.ChannelRWNodes))
			for channel, nodes := range replica.ChannelRWNodes {
				channelNodes[channel] = cloneSlice(nodes)
			}
			replica.ChannelRWNodes = channelNodes
		}
		clone[replicaID] = replica
	}
	return clone
}

func cloneSegments(segments map[SegmentObjectKey][]SegmentPlacement) map[SegmentObjectKey][]SegmentPlacement {
	if segments == nil {
		return nil
	}
	clone := make(map[SegmentObjectKey][]SegmentPlacement, len(segments))
	for key, placements := range segments {
		clone[key] = cloneSlice(placements)
	}
	return clone
}

func cloneChannels(channels map[ChannelObjectKey][]ChannelPlacement) map[ChannelObjectKey][]ChannelPlacement {
	if channels == nil {
		return nil
	}
	clone := make(map[ChannelObjectKey][]ChannelPlacement, len(channels))
	for key, placements := range channels {
		clone[key] = cloneSlice(placements)
	}
	return clone
}

func cloneCollectionTargets(targets map[int64]CollectionTargetSnapshot) map[int64]CollectionTargetSnapshot {
	if targets == nil {
		return nil
	}
	clone := make(map[int64]CollectionTargetSnapshot, len(targets))
	for collectionID, target := range targets {
		target.Current = cloneTargetScope(target.Current)
		target.Next = cloneTargetScope(target.Next)
		clone[collectionID] = target
	}
	return clone
}

func cloneTargetScope(scope TargetScopeSnapshot) TargetScopeSnapshot {
	clone := scope
	if scope.Segments != nil {
		clone.Segments = make(map[int64]TargetSegmentSnapshot, len(scope.Segments))
		for segmentID, segment := range scope.Segments {
			clone.Segments[segmentID] = segment
		}
	}
	if scope.Channels != nil {
		clone.Channels = make(map[string]TargetChannelSnapshot, len(scope.Channels))
		for channel, target := range scope.Channels {
			target.GrowingSegmentIDs = cloneSlice(target.GrowingSegmentIDs)
			clone.Channels[channel] = target
		}
	}
	return clone
}

func clonePendingWork(pending PendingWorkSnapshot) PendingWorkSnapshot {
	clone := pending
	if pending.Tasks != nil {
		clone.Tasks = make([]task.PendingBalanceTaskSnapshot, len(pending.Tasks))
		for index, pendingTask := range pending.Tasks {
			pendingTask.Actions = cloneSlice(pendingTask.Actions)
			clone.Tasks[index] = pendingTask
		}
	}
	clone.SegmentWorkloadByNode = cloneIntMap(pending.SegmentWorkloadByNode)
	clone.ChannelWorkloadByNode = cloneIntMap(pending.ChannelWorkloadByNode)
	return clone
}

func cloneIntMap(values map[int64]int) map[int64]int {
	if values == nil {
		return nil
	}
	clone := make(map[int64]int, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func cloneEligibleReplicas(replicas map[int64]struct{}) map[int64]struct{} {
	if replicas == nil {
		return nil
	}
	clone := make(map[int64]struct{}, len(replicas))
	for replicaID := range replicas {
		clone[replicaID] = struct{}{}
	}
	return clone
}

func cloneSlice[T any](values []T) []T {
	if values == nil {
		return nil
	}
	clone := make([]T, len(values))
	copy(clone, values)
	return clone
}
