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
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type epochManagerClock struct {
	mu  sync.Mutex
	now time.Time
}

func newEpochManagerClock() *epochManagerClock {
	return &epochManagerClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *epochManagerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *epochManagerClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	c.mu.Unlock()
}

type epochManagerSnapshotSource struct {
	mu        sync.Mutex
	snapshots map[string]*PlacementSnapshot
	builds    map[string]int
	carry     map[string][][]task.PendingBalanceTaskSnapshot
	validate  func(AdmissionToken) task.BalanceAdmissionReason
}

func newEpochManagerSnapshotSource(snapshots ...*PlacementSnapshot) *epochManagerSnapshotSource {
	source := &epochManagerSnapshotSource{
		snapshots: make(map[string]*PlacementSnapshot),
		builds:    make(map[string]int),
		carry:     make(map[string][][]task.PendingBalanceTaskSnapshot),
	}
	for _, snapshot := range snapshots {
		source.snapshots[snapshot.Token.ResourceGroup] = snapshot
	}
	return source
}

func (s *epochManagerSnapshotSource) Build(
	_ context.Context,
	resourceGroup string,
	_ []int64,
	carryOver []task.PendingBalanceTaskSnapshot,
) (*PlacementSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.builds[resourceGroup]++
	clonedCarry := make([]task.PendingBalanceTaskSnapshot, len(carryOver))
	for i, pending := range carryOver {
		clonedCarry[i] = pending
		clonedCarry[i].Actions = append([]task.PendingBalanceActionSnapshot(nil), pending.Actions...)
	}
	s.carry[resourceGroup] = append(s.carry[resourceGroup], clonedCarry)
	snapshot := s.snapshots[resourceGroup]
	if snapshot == nil {
		return nil, errors.New("snapshot not found")
	}
	cloned := clonePlacementSnapshot(*snapshot)
	return &cloned, nil
}

func (s *epochManagerSnapshotSource) Validate(token AdmissionToken) task.BalanceAdmissionReason {
	s.mu.Lock()
	validate := s.validate
	s.mu.Unlock()
	if validate != nil {
		return validate(token)
	}
	return task.BalanceAdmissionAccepted
}

func (s *epochManagerSnapshotSource) setSnapshot(snapshot *PlacementSnapshot) {
	s.mu.Lock()
	s.snapshots[snapshot.Token.ResourceGroup] = snapshot
	s.mu.Unlock()
}

func (s *epochManagerSnapshotSource) setValidate(validate func(AdmissionToken) task.BalanceAdmissionReason) {
	s.mu.Lock()
	s.validate = validate
	s.mu.Unlock()
}

func (s *epochManagerSnapshotSource) buildCount(resourceGroup string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.builds[resourceGroup]
}

func (s *epochManagerSnapshotSource) lastCarry(resourceGroup string) []task.PendingBalanceTaskSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	builds := s.carry[resourceGroup]
	if len(builds) == 0 {
		return nil
	}
	return append([]task.PendingBalanceTaskSnapshot(nil), builds[len(builds)-1]...)
}

type epochManagerPolicy struct {
	mu          sync.Mutex
	waves       map[string][]BalanceWave
	calls       map[string]int
	constraints map[string][]EpochPlanningConstraints
	entered     map[string]chan struct{}
	release     map[string]chan struct{}
}

func newEpochManagerPolicy() *epochManagerPolicy {
	return &epochManagerPolicy{
		waves:       make(map[string][]BalanceWave),
		calls:       make(map[string]int),
		constraints: make(map[string][]EpochPlanningConstraints),
		entered:     make(map[string]chan struct{}),
		release:     make(map[string]chan struct{}),
	}
}

func (p *epochManagerPolicy) Plan(
	snapshot *PlacementSnapshot,
	_ BalanceWaveBudget,
	constraints EpochPlanningConstraints,
	_ EpochPolicyConfig,
) BalanceWave {
	resourceGroup := snapshot.Token.ResourceGroup
	p.mu.Lock()
	call := p.calls[resourceGroup]
	p.calls[resourceGroup]++
	p.constraints[resourceGroup] = append(p.constraints[resourceGroup], cloneEpochManagerConstraints(constraints))
	entered := p.entered[resourceGroup]
	release := p.release[resourceGroup]
	waves := p.waves[resourceGroup]
	p.mu.Unlock()
	if entered != nil && call == 0 {
		close(entered)
		<-release
	}
	if len(waves) == 0 {
		return BalanceWave{Kind: PlanKindSegment, Converged: true}
	}
	if call >= len(waves) {
		return waves[len(waves)-1]
	}
	return waves[call]
}

func (p *epochManagerPolicy) Evaluate(
	_ *PlacementSnapshot,
	_ *ProjectedPlacement,
	_ PlanKind,
	_ EpochPolicyConfig,
) ScorePotential {
	return ScorePotential{}
}

func (p *epochManagerPolicy) setWaves(resourceGroup string, waves ...BalanceWave) {
	p.mu.Lock()
	p.waves[resourceGroup] = append([]BalanceWave(nil), waves...)
	p.mu.Unlock()
}

func (p *epochManagerPolicy) blockFirst(resourceGroup string) (<-chan struct{}, func()) {
	p.mu.Lock()
	entered := make(chan struct{})
	release := make(chan struct{})
	p.entered[resourceGroup] = entered
	p.release[resourceGroup] = release
	p.mu.Unlock()
	var once sync.Once
	return entered, func() { once.Do(func() { close(release) }) }
}

func (p *epochManagerPolicy) callCount(resourceGroup string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[resourceGroup]
}

func (p *epochManagerPolicy) lastConstraints(resourceGroup string) EpochPlanningConstraints {
	p.mu.Lock()
	defer p.mu.Unlock()
	constraints := p.constraints[resourceGroup]
	if len(constraints) == 0 {
		return EpochPlanningConstraints{}
	}
	return cloneEpochManagerConstraints(constraints[len(constraints)-1])
}

func cloneEpochManagerConstraints(input EpochPlanningConstraints) EpochPlanningConstraints {
	output := EpochPlanningConstraints{Objects: make(map[BalanceObjectKey]EpochObjectConstraint, len(input.Objects))}
	for key, constraint := range input.Objects {
		constraint.ChargedNodes = append([]int64(nil), constraint.ChargedNodes...)
		output.Objects[key] = constraint
	}
	return output
}

type epochManagerAdmitter struct {
	mu       sync.Mutex
	nextID   int64
	reasons  []task.BalanceAdmissionReason
	tasks    []task.Task
	expected []task.BalancePendingRevision
}

func (a *epochManagerAdmitter) AdmitBalanceTaskAtPendingRevision(
	balanceTask task.Task,
	expected task.BalancePendingRevision,
	validate task.BalanceAdmissionValidator,
) task.BalanceAdmissionResult {
	a.mu.Lock()
	a.expected = append(a.expected, expected)
	a.mu.Unlock()
	if reason := validate(); reason != task.BalanceAdmissionAccepted {
		balanceTask.Cancel(errors.New(reason.String()))
		return task.BalanceAdmissionResult{Reason: reason, Err: errors.New(reason.String()), PendingRevision: expected}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.reasons) != 0 {
		reason := a.reasons[0]
		a.reasons = a.reasons[1:]
		if reason != task.BalanceAdmissionAccepted {
			balanceTask.Cancel(errors.New(reason.String()))
			return task.BalanceAdmissionResult{Reason: reason, Err: errors.New(reason.String()), PendingRevision: expected}
		}
	}
	a.nextID++
	balanceTask.SetID(a.nextID)
	a.tasks = append(a.tasks, balanceTask)
	return task.BalanceAdmissionResult{
		TaskID: balanceTask.ID(),
		Reason: task.BalanceAdmissionAccepted,
		PendingRevision: task.BalancePendingRevision{
			ResourceGroup: expected.ResourceGroup,
			Epoch:         expected.Epoch,
			Revision:      expected.Revision + 1,
			EpochRevision: expected.EpochRevision + 1,
		},
	}
}

func (a *epochManagerAdmitter) GetPendingBalanceTasks() task.PendingBalanceSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	return task.PendingBalanceSnapshot{}
}

func (a *epochManagerAdmitter) acceptedTasks() []task.Task {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]task.Task(nil), a.tasks...)
}

func (a *epochManagerAdmitter) expectedRevisions() []task.BalancePendingRevision {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]task.BalancePendingRevision(nil), a.expected...)
}

type epochManagerTestFixture struct {
	placement *placementSnapshotFixture
	snapshot  *PlacementSnapshot
	other     *PlacementSnapshot
	source    *epochManagerSnapshotSource
	policy    *epochManagerPolicy
	admitter  *epochManagerAdmitter
	clock     *epochManagerClock
	manager   *BalanceEpochManager
}

func newEpochManagerTestFixture(t *testing.T) *epochManagerTestFixture {
	t.Helper()
	placement := newPlacementSnapshotFixture(t)
	snapshot := buildSnapshot(t, placement)
	other, err := placement.builder.Build(placement.ctx, testUnrelatedRG, []int64{13}, nil)
	require.NoError(t, err)
	source := newEpochManagerSnapshotSource(snapshot, other)
	policy := newEpochManagerPolicy()
	admitter := &epochManagerAdmitter{}
	clock := newEpochManagerClock()
	manager := NewBalanceEpochManager(
		placement.meta,
		placement.dist,
		placement.target,
		placement.nodes,
		nil,
		admitter,
		admitter,
		task.WrapIDSource(6),
		func() (EpochBalancePolicy, bool) { return policy, true },
		WithEpochClock(clock.Now),
		WithLeaderTerm(77),
		WithPlacementSnapshotSource(source),
	)
	return &epochManagerTestFixture{
		placement: placement,
		snapshot:  snapshot,
		other:     other,
		source:    source,
		policy:    policy,
		admitter:  admitter,
		clock:     clock,
		manager:   manager,
	}
}

func epochManagerRequest(resourceGroup string, replicaIDs ...int64) EpochRequest {
	return EpochRequest{
		ResourceGroup:      resourceGroup,
		EligibleReplicaIDs: append([]int64(nil), replicaIDs...),
		Budget: BalanceWaveBudget{
			MaxSegmentTasks: 10, MaxChannelTasks: 10,
			MaxTasksPerNode: 10, MaxTasksPerCollection: 10,
		},
		AllowNew:           true,
		Deadline:           time.Minute,
		NoProgressDeadline: time.Minute,
		SegmentTaskTimeout: time.Minute,
		ChannelTaskTimeout: time.Minute,
		MaxObjectRetries:   2,
		QuarantineBackoff:  time.Minute,
	}
}

func epochManagerWave(plans ...EpochPlan) BalanceWave {
	wave := BalanceWave{
		Kind:        PlanKindSegment,
		Plans:       append([]EpochPlan(nil), plans...),
		PrefixAfter: make([]ScorePotential, len(plans)),
		Before:      ScorePotential{Value: 100},
		After:       ScorePotential{Value: 100},
	}
	for i := range plans {
		wave.PrefixAfter[i] = ScorePotential{Value: float64(90 - i*10)}
	}
	if len(plans) != 0 {
		wave.After = wave.PrefixAfter[len(wave.PrefixAfter)-1]
	}
	return wave
}

func publishEpochManagerSegments(
	fixture *placementSnapshotFixture,
	node1SegmentIDs []int64,
	node3SegmentIDs []int64,
) {
	segments := func(node int64, ids []int64) []*meta.Segment {
		result := make([]*meta.Segment, 0, len(ids))
		for _, id := range ids {
			var collectionID, partitionID, rows int64
			var channel string
			switch id {
			case 101:
				collectionID, partitionID, rows, channel = 100, 10, 100, "channel-a"
			case 201:
				collectionID, partitionID, rows, channel = 200, 20, 200, "channel-b"
			default:
				panic("unknown test segment")
			}
			result = append(result, &meta.Segment{
				SegmentInfo: &datapb.SegmentInfo{
					ID: id, CollectionID: collectionID, PartitionID: partitionID,
					InsertChannel: channel, NumOfRows: rows,
				},
				Node:    node,
				Version: 100 + id,
			})
		}
		return result
	}
	fixture.dist.PublishNodeDistribution(1, segments(1, node1SegmentIDs), fixture.node1Channels)
	fixture.dist.PublishNodeDistribution(3, segments(3, node3SegmentIDs), nil)
}

func TestEpochManagerCoalescesRepeatedRGTriggers(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	fixture.policy.setWaves(testSnapshotRG, BalanceWave{Kind: PlanKindSegment, Converged: true})
	entered, release := fixture.policy.blockFirst(testSnapshotRG)
	defer release()

	first := make(chan EpochAdvanceResult, 1)
	go func() {
		first <- fixture.manager.Advance(context.Background(), epochManagerRequest(testSnapshotRG, testEligibleReplica))
	}()
	receiveBalanceTestSignal(t, entered, "blocked RG planning")

	second := make(chan EpochAdvanceResult, 1)
	go func() {
		second <- fixture.manager.Advance(context.Background(), epochManagerRequest(testSnapshotRG, testEligibleReplica))
	}()
	select {
	case result := <-second:
		require.Equal(t, EpochPlanning, result.State)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("same-RG coalesced advance waited for the in-flight planner")
	}
	require.Equal(t, 1, fixture.policy.callCount(testSnapshotRG))

	release()
	result := receiveBalanceTestSignal(t, first, "first RG result")
	require.Equal(t, EpochCompleted, result.State)
}

func TestEpochManagerAllowsIndependentRGs(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	fixture.policy.setWaves(testSnapshotRG, BalanceWave{Kind: PlanKindSegment, Converged: true})
	fixture.policy.setWaves(testUnrelatedRG, BalanceWave{Kind: PlanKindSegment, Converged: true})
	entered, release := fixture.policy.blockFirst(testSnapshotRG)
	defer release()

	first := make(chan EpochAdvanceResult, 1)
	go func() {
		first <- fixture.manager.Advance(context.Background(), epochManagerRequest(testSnapshotRG, testEligibleReplica))
	}()
	receiveBalanceTestSignal(t, entered, "blocked RG-A planning")

	resultCh := make(chan EpochAdvanceResult, 1)
	go func() {
		resultCh <- fixture.manager.Advance(context.Background(), epochManagerRequest(testUnrelatedRG, 13))
	}()
	select {
	case result := <-resultCh:
		require.Equal(t, EpochCompleted, result.State)
		require.Equal(t, testUnrelatedRG, result.ResourceGroup)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("independent RG advance was serialized behind RG-A")
	}

	release()
	receiveBalanceTestSignal(t, first, "RG-A result")
}

func TestEpochManagerCountsOnlyAcceptedAdmissions(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plans := []EpochPlan{
		epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3),
		epochSegmentPlan(*fixture.snapshot, testOtherReplica, 201, 1, 3),
	}
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plans...))
	fixture.admitter.reasons = []task.BalanceAdmissionReason{
		task.BalanceAdmissionAccepted,
		task.BalanceAdmissionDuplicate,
	}

	result := fixture.manager.Advance(context.Background(), epochManagerRequest(testSnapshotRG, testEligibleReplica, testOtherReplica))
	require.Equal(t, 2, result.Planned)
	require.Equal(t, 1, result.Admitted)
	require.Equal(t, 1, result.Rejected[task.BalanceAdmissionDuplicate])
	require.Equal(t, 90.0, result.ObjectiveAfter)
	require.Len(t, fixture.admitter.acceptedTasks(), 1)
	expected := fixture.admitter.expectedRevisions()
	require.Len(t, expected, 2)
	require.Equal(t, expected[0].EffectiveRevision(), expected[1].EffectiveRevision())

	accepted := fixture.admitter.acceptedTasks()[0]
	require.Equal(t, task.TaskPriorityLow, accepted.Priority())
	require.Equal(t, "segment unbalanced", accepted.GetReason())
	require.Equal(t, result.Epoch, accepted.BalanceEpoch())
	segmentTask, ok := accepted.(*task.SegmentTask)
	require.True(t, ok)
	require.Equal(t, commonpb.LoadPriority_LOW, segmentTask.LoadPriority())
	require.Len(t, accepted.Actions(), 2)
	require.Equal(t, task.ActionTypeGrow, accepted.Actions()[0].Type())
	require.Equal(t, task.ActionTypeReduce, accepted.Actions()[1].Type())
}

func TestEpochManagerReconcilesBeforeNextGeneration(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(
		testSnapshotRG,
		epochManagerWave(plan),
		BalanceWave{Kind: PlanKindSegment, Converged: true},
	)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)

	first := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, first.State)
	require.Equal(t, uint64(1), first.Epoch.Sequence)
	publishEpochManagerSegments(fixture.placement, []int64{201}, []int64{101})

	reconciled := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochCompleted, reconciled.State)
	require.True(t, reconciled.Terminal)
	require.Equal(t, 1, fixture.policy.callCount(testSnapshotRG))

	next := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochCompleted, next.State)
	require.Equal(t, uint64(2), next.Epoch.Sequence)
	require.Equal(t, 2, fixture.policy.callCount(testSnapshotRG))
}

func TestEpochManagerFailedGrowKeepsSource(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan), BalanceWave{Kind: PlanKindSegment, Converged: true})
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)

	fixture.manager.Advance(context.Background(), request)
	accepted := fixture.admitter.acceptedTasks()[0]
	accepted.SetStatus(task.TaskStatusFailed)
	select {
	case <-accepted.Done():
		t.Fatal("status-first fixture unexpectedly closed Done")
	default:
	}
	publishEpochManagerSegments(fixture.placement, []int64{101, 201}, nil)

	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochDegraded, result.State)
	require.True(t, result.Terminal)

	fixture.manager.Advance(context.Background(), request)
	require.Empty(t, fixture.source.lastCarry(testSnapshotRG))
	require.NotContains(t, fixture.policy.lastConstraints(testSnapshotRG).Objects, plan.ObjectKey())
}

func TestEpochManagerFailedReduceLeavesRedundantCopy(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan), BalanceWave{Kind: PlanKindSegment, Converged: true})
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)

	fixture.manager.Advance(context.Background(), request)
	accepted := fixture.admitter.acceptedTasks()[0]
	accepted.Fail(errors.New("reduce failed"))
	publishEpochManagerSegments(fixture.placement, []int64{101, 201}, []int64{101})

	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochDegraded, result.State)
	require.True(t, result.Terminal)

	fixture.manager.Advance(context.Background(), request)
	require.Empty(t, fixture.source.lastCarry(testSnapshotRG))
	require.NotContains(t, fixture.policy.lastConstraints(testSnapshotRG).Objects, plan.ObjectKey())
}

func TestEpochManagerDeadlineCarriesAmbiguousWork(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan), BalanceWave{Kind: PlanKindSegment, Converged: true})
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.Deadline = time.Second
	request.NoProgressDeadline = 0

	fixture.manager.Advance(context.Background(), request)
	fixture.clock.Advance(2 * time.Second)
	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochTimedOut, result.State)
	require.True(t, result.Terminal)

	fixture.manager.Advance(context.Background(), request)
	carry := fixture.source.lastCarry(testSnapshotRG)
	require.Len(t, carry, 1)
	require.Equal(t, plan.SegmentID, carry[0].Actions[0].SegmentID)
	constraint := fixture.policy.lastConstraints(testSnapshotRG).Objects[plan.ObjectKey()]
	require.Equal(t, ReservationAmbiguousCapacity, constraint.Class)
	require.Equal(t, []int64{1, 3}, constraint.ChargedNodes)

	fixture.source.setValidate(func(AdmissionToken) task.BalanceAdmissionReason {
		return task.BalanceAdmissionRGChanged
	})
	superseded := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochSuperseded, superseded.State)
	require.True(t, superseded.Terminal)
}

func TestEpochManagerNodeOrRGChangeSupersedesGeneration(t *testing.T) {
	for _, reason := range []task.BalanceAdmissionReason{
		task.BalanceAdmissionNodeIneligible,
		task.BalanceAdmissionRGChanged,
		task.BalanceAdmissionStaleEpoch,
	} {
		t.Run(reason.String(), func(t *testing.T) {
			fixture := newEpochManagerTestFixture(t)
			plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
			fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
			request := epochManagerRequest(testSnapshotRG, testEligibleReplica)

			fixture.manager.Advance(context.Background(), request)
			fixture.source.setValidate(func(AdmissionToken) task.BalanceAdmissionReason { return reason })
			result := fixture.manager.Advance(context.Background(), request)
			require.Equal(t, EpochSuperseded, result.State)
			require.True(t, result.Terminal)
			require.True(t, fixture.manager.HasActive(testSnapshotRG))
		})
	}
}

func TestEpochManagerTargetChangeScopesReconciliation(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	firstPlan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	secondPlan := epochSegmentPlan(*fixture.snapshot, testOtherReplica, 201, 1, 3)
	thirdPlan := firstPlan
	thirdPlan.SegmentID = 103
	thirdPlan.RowCount = 75
	thirdKey := SegmentObjectKey{ReplicaID: testEligibleReplica, SegmentID: 103, Scope: querypb.DataScope_Historical}
	thirdPlan.Token.Segment = &thirdKey
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(firstPlan, secondPlan, thirdPlan))
	validationCalls := 0
	fixture.source.setValidate(func(AdmissionToken) task.BalanceAdmissionReason {
		validationCalls++
		if validationCalls == 3 {
			return task.BalanceAdmissionTargetChanged
		}
		return task.BalanceAdmissionAccepted
	})
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica, testOtherReplica)

	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, 2, result.Admitted)
	require.Equal(t, 1, result.Rejected[task.BalanceAdmissionTargetChanged])
	require.Equal(t, EpochExecuting, result.State)

	tasks := fixture.admitter.acceptedTasks()
	require.Len(t, tasks, 2)
	tasks[1].Fail(errors.New("collection 200 grow failed"))
	publishEpochManagerSegments(fixture.placement, []int64{201}, []int64{101})
	fixture.source.setValidate(func(AdmissionToken) task.BalanceAdmissionReason {
		return task.BalanceAdmissionTargetChanged
	})

	reconciled := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochSuperseded, reconciled.State)
	require.True(t, reconciled.Terminal)
	require.Empty(t, fixture.source.lastCarry(testSnapshotRG))
	require.Equal(t, task.TaskStatusStarted, tasks[0].Status())
}

func TestEpochManagerQuarantinesRepeatedNonAmbiguousFailure(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(
		testSnapshotRG,
		epochManagerWave(plan),
		epochManagerWave(plan),
		BalanceWave{Kind: PlanKindSegment, Converged: true},
		epochManagerWave(plan),
	)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.MaxObjectRetries = 2
	request.QuarantineBackoff = time.Minute

	fixture.manager.Advance(context.Background(), request)
	fixture.admitter.acceptedTasks()[0].Fail(errors.New("first grow failure"))
	fixture.manager.Advance(context.Background(), request)
	fixture.manager.Advance(context.Background(), request)

	fixture.admitter.acceptedTasks()[1].Fail(errors.New("second grow failure"))
	fixture.manager.Advance(context.Background(), request)
	fixture.manager.Advance(context.Background(), request)

	constraint := fixture.policy.lastConstraints(testSnapshotRG).Objects[plan.ObjectKey()]
	require.Equal(t, ReservationQuarantineOnly, constraint.Class)
	require.Empty(t, constraint.ChargedNodes)
	require.Len(t, fixture.admitter.acceptedTasks(), 2)

	fixture.clock.Advance(2 * time.Minute)
	fixture.manager.Advance(context.Background(), request)
	require.Len(t, fixture.admitter.acceptedTasks(), 3)
}

func TestEpochManagerRestartRebuildsOnlyFromDistribution(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.Deadline = time.Second
	request.NoProgressDeadline = 0

	fixture.manager.Advance(context.Background(), request)
	fixture.clock.Advance(2 * time.Second)
	fixture.manager.Advance(context.Background(), request)

	restartedSource := newEpochManagerSnapshotSource(fixture.snapshot)
	restartedPolicy := newEpochManagerPolicy()
	restartedPolicy.setWaves(testSnapshotRG, epochManagerWave(plan))
	restartedAdmitter := &epochManagerAdmitter{}
	restarted := NewBalanceEpochManager(
		fixture.placement.meta,
		fixture.placement.dist,
		fixture.placement.target,
		fixture.placement.nodes,
		nil,
		restartedAdmitter,
		restartedAdmitter,
		task.WrapIDSource(6),
		func() (EpochBalancePolicy, bool) { return restartedPolicy, true },
		WithEpochClock(fixture.clock.Now),
		WithLeaderTerm(88),
		WithPlacementSnapshotSource(restartedSource),
	)

	result := restarted.Advance(context.Background(), request)
	require.Equal(t, 1, result.Admitted)
	require.Empty(t, restartedSource.lastCarry(testSnapshotRG))
	require.NotContains(t, restartedPolicy.lastConstraints(testSnapshotRG).Objects, plan.ObjectKey())
}

func TestEpochManagerRelevantDistributionProgressResetsNoProgressDeadline(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.Deadline = time.Minute
	request.NoProgressDeadline = time.Second

	first := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, first.State)
	fixture.clock.Advance(750 * time.Millisecond)
	publishEpochManagerSegments(fixture.placement, []int64{101, 201}, nil)
	progress := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, progress.State)

	fixture.clock.Advance(500 * time.Millisecond)
	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, result.State)
}

func TestEpochManagerReconcilesInheritedCarryWhenNewAdmissionStops(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	carriedPlan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	newPlan := epochSegmentPlan(*fixture.snapshot, testOtherReplica, 201, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(carriedPlan), epochManagerWave(newPlan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica, testOtherReplica)
	request.Deadline = time.Second
	request.NoProgressDeadline = 0

	fixture.manager.Advance(context.Background(), request)
	fixture.clock.Advance(2 * time.Second)
	fixture.manager.Advance(context.Background(), request)
	publishEpochManagerSegments(fixture.placement, []int64{201}, []int64{101})
	fixture.admitter.mu.Lock()
	fixture.admitter.reasons = []task.BalanceAdmissionReason{task.BalanceAdmissionDuplicate}
	fixture.admitter.mu.Unlock()

	stopped := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochDegraded, stopped.State)
	observe := request
	observe.AllowNew = false
	fixture.manager.Advance(context.Background(), observe)
	shadow := request
	shadow.Shadow = true
	fixture.manager.Advance(context.Background(), shadow)
	require.Empty(t, fixture.source.lastCarry(testSnapshotRG))
}

func TestEpochManagerShadowHonorsDeadlineWithoutActiveState(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	fixture.policy.setWaves(testSnapshotRG, BalanceWave{Kind: PlanKindSegment, Converged: true})
	entered, release := fixture.policy.blockFirst(testSnapshotRG)
	defer release()
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.Shadow = true
	request.Deadline = time.Second

	resultCh := make(chan EpochAdvanceResult, 1)
	go func() {
		resultCh <- fixture.manager.Advance(context.Background(), request)
	}()
	receiveBalanceTestSignal(t, entered, "shadow planning")
	fixture.clock.Advance(2 * time.Second)
	release()

	result := receiveBalanceTestSignal(t, resultCh, "shadow deadline result")
	require.Equal(t, EpochTimedOut, result.State)
	require.True(t, result.Terminal)
	require.False(t, result.Started)
	require.False(t, fixture.manager.HasActive(testSnapshotRG))
	require.Empty(t, fixture.admitter.acceptedTasks())
}

func TestEpochManagerLostPlacementOverridesTimeout(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.Deadline = time.Second
	request.NoProgressDeadline = 0

	fixture.manager.Advance(context.Background(), request)
	fixture.admitter.acceptedTasks()[0].Fail(errors.New("placement lost"))
	publishEpochManagerSegments(fixture.placement, []int64{201}, nil)
	fixture.clock.Advance(2 * time.Second)

	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochDegraded, result.State)
	require.True(t, result.Terminal)
}

func TestEpochManagerFailedUnreadyChannelTargetUsesPhysicalPresence(t *testing.T) {
	withSource := workObservation{
		sourcePresent: true,
		targetPresent: true,
		targetReady:   false,
		status:        task.TaskStatusFailed,
	}
	targetOnly := withSource
	targetOnly.sourcePresent = false

	require.Equal(t, terminalWorkReduceFailed, classifyTerminalWork(withSource))
	require.Equal(t, terminalWorkAmbiguous, classifyTerminalWork(targetOnly))
}

func TestSnapshotTokenWithPendingRevisionDeepCopies(t *testing.T) {
	epoch := task.BalanceEpochMeta{ResourceGroup: "rg", LeaderTerm: 7, Sequence: 9}
	original := SnapshotToken{
		ResourceGroup:        "rg",
		PendingTaskRevision:  3,
		CurrentTargetVersion: map[int64]int64{100: 10},
		NextTargetVersion:    map[int64]int64{100: 11},
		pendingEpochRevisions: map[task.BalanceEpochMeta]uint64{
			epoch: 2,
		},
	}
	revision := task.BalancePendingRevision{
		ResourceGroup: "rg", Epoch: epoch, Revision: 5, EpochRevision: 4,
	}

	updated := original.WithPendingRevision(revision)
	original.CurrentTargetVersion[100] = 20
	original.NextTargetVersion[100] = 21
	original.pendingEpochRevisions[epoch] = 8

	require.Equal(t, uint64(5), updated.PendingTaskRevision)
	require.Equal(t, int64(10), updated.CurrentTargetVersion[100])
	require.Equal(t, int64(11), updated.NextTargetVersion[100])
	require.Equal(t, uint64(4), updated.PendingRevision(epoch).EpochRevision)
}

func TestEpochManagerActiveResourceGroupsAreSortedAndRetainedUntilCleanup(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	fixture.policy.setWaves(testSnapshotRG, BalanceWave{Kind: PlanKindSegment, Converged: true})
	fixture.policy.setWaves(testUnrelatedRG, BalanceWave{Kind: PlanKindSegment, Converged: true})

	fixture.manager.Advance(context.Background(), epochManagerRequest(testUnrelatedRG, 13))
	fixture.manager.Advance(context.Background(), epochManagerRequest(testSnapshotRG, testEligibleReplica))
	require.Equal(t, []string{testSnapshotRG, testUnrelatedRG}, fixture.manager.ActiveResourceGroups())

	observe := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	observe.AllowNew = false
	fixture.manager.Advance(context.Background(), observe)
	require.False(t, fixture.manager.HasActive(testSnapshotRG))

	groups := fixture.manager.ActiveResourceGroups()
	sort.Strings(groups)
	require.Equal(t, []string{testUnrelatedRG}, groups)
}
