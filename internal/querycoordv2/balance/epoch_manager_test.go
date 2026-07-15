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
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientmodel "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
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

type epochManagerBlockingRejectAdmitter struct {
	entered chan struct{}
	release chan struct{}
	reason  task.BalanceAdmissionReason
}

type epochManagerRejectAdmitter struct {
	reason task.BalanceAdmissionReason
	err    error
}

func (a *epochManagerRejectAdmitter) AdmitBalanceTaskAtPendingRevision(
	balanceTask task.Task,
	expected task.BalancePendingRevision,
	validate task.BalanceAdmissionValidator,
) task.BalanceAdmissionResult {
	if reason := validate(); reason != task.BalanceAdmissionAccepted {
		err := errors.New(reason.String())
		balanceTask.Cancel(err)
		return task.BalanceAdmissionResult{Reason: reason, Err: err, PendingRevision: expected}
	}
	balanceTask.Cancel(a.err)
	return task.BalanceAdmissionResult{Reason: a.reason, Err: a.err, PendingRevision: expected}
}

func (a *epochManagerBlockingRejectAdmitter) AdmitBalanceTaskAtPendingRevision(
	balanceTask task.Task,
	expected task.BalancePendingRevision,
	validate task.BalanceAdmissionValidator,
) task.BalanceAdmissionResult {
	if reason := validate(); reason != task.BalanceAdmissionAccepted {
		balanceTask.Cancel(errors.New(reason.String()))
		return task.BalanceAdmissionResult{Reason: reason, Err: errors.New(reason.String()), PendingRevision: expected}
	}
	close(a.entered)
	<-a.release
	err := errors.New(a.reason.String())
	balanceTask.Cancel(err)
	return task.BalanceAdmissionResult{Reason: a.reason, Err: err, PendingRevision: expected}
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
		func(string, EpochPolicyConfig) (EpochBalancePolicy, bool) { return policy, true },
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
		Balancer:           meta.ScoreBasedBalancerName,
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

func newEpochManagerMetricsRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	resetEpochManagerMetrics()
	t.Cleanup(resetEpochManagerMetrics)
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		pkgmetrics.QueryCoordBalanceEpochActive,
		pkgmetrics.QueryCoordBalanceEpochTotal,
		pkgmetrics.QueryCoordBalanceEpochPlansTotal,
		pkgmetrics.QueryCoordBalanceEpochAdmissionTotal,
		pkgmetrics.QueryCoordBalanceEpochSnapshotRetriesTotal,
		pkgmetrics.QueryCoordBalanceEpochObjective,
		pkgmetrics.QueryCoordBalanceEpochCarryOver,
		pkgmetrics.QueryCoordBalanceEpochDurationSeconds,
	)
	return registry
}

func resetEpochManagerMetrics() {
	pkgmetrics.QueryCoordBalanceEpochActive.Reset()
	pkgmetrics.QueryCoordBalanceEpochTotal.Reset()
	pkgmetrics.QueryCoordBalanceEpochPlansTotal.Reset()
	pkgmetrics.QueryCoordBalanceEpochAdmissionTotal.Reset()
	pkgmetrics.QueryCoordBalanceEpochSnapshotRetriesTotal.Reset()
	pkgmetrics.QueryCoordBalanceEpochObjective.Reset()
	pkgmetrics.QueryCoordBalanceEpochCarryOver.Reset()
	pkgmetrics.QueryCoordBalanceEpochDurationSeconds.Reset()
}

func requireEpochMetric(
	t *testing.T,
	registry *prometheus.Registry,
	name string,
	labels map[string]string,
) *clientmodel.Metric {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			actual := make(map[string]string, len(metric.Label))
			for _, pair := range metric.Label {
				actual[pair.GetName()] = pair.GetValue()
			}
			if mapsEqual(actual, labels) {
				return metric
			}
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
	return nil
}

func requireEpochMetricAbsent(
	t *testing.T,
	registry *prometheus.Registry,
	name string,
	labels map[string]string,
) {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			actual := make(map[string]string, len(metric.Label))
			for _, pair := range metric.Label {
				actual[pair.GetName()] = pair.GetValue()
			}
			require.False(t, mapsEqual(actual, labels), "unexpected metric %s with labels %v", name, labels)
		}
	}
}

func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func requireEpochMetricFamilyAbsent(t *testing.T, registry *prometheus.Registry, name string) {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		require.NotEqual(t, name, family.GetName())
	}
}

func TestEpochManagerMetricsLifecycleExactlyOnce(t *testing.T) {
	t.Run("active generation publishes transitions and terminal once", func(t *testing.T) {
		registry := newEpochManagerMetricsRegistry(t)
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

		request := epochManagerRequest(testSnapshotRG, testEligibleReplica, testOtherReplica)
		started := fixture.manager.Advance(context.Background(), request)
		require.Equal(t, EpochExecuting, started.State)
		require.Equal(t, 1, started.Admitted)
		require.Equal(t, 1, started.Rejected[task.BalanceAdmissionDuplicate])

		active := requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_active", map[string]string{
			"resource_group": testSnapshotRG, "state": "executing",
		})
		require.Equal(t, float64(1), active.GetGauge().GetValue())
		requireEpochMetricAbsent(t, registry, "milvus_querycoord_balance_epoch_active", map[string]string{
			"resource_group": testSnapshotRG, "state": "planning",
		})
		requireEpochMetricAbsent(t, registry, "milvus_querycoord_balance_epoch_active", map[string]string{
			"resource_group": testSnapshotRG, "state": "admitting",
		})
		require.Equal(t, float64(2), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_plans_total", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment", "result": "planned",
		}).GetCounter().GetValue())
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_plans_total", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment", "result": "reserved",
		}).GetCounter().GetValue())
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_plans_total", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment", "result": "rejected",
		}).GetCounter().GetValue())
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_admission_total", map[string]string{
			"resource_group": testSnapshotRG, "reason": task.BalanceAdmissionAccepted.String(),
		}).GetCounter().GetValue())
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_admission_total", map[string]string{
			"resource_group": testSnapshotRG, "reason": task.BalanceAdmissionDuplicate.String(),
		}).GetCounter().GetValue())
		require.Equal(t, float64(100), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_objective", map[string]string{
			"resource_group": testSnapshotRG, "phase": "observed",
		}).GetGauge().GetValue())
		require.Equal(t, float64(80), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_objective", map[string]string{
			"resource_group": testSnapshotRG, "phase": "projected",
		}).GetGauge().GetValue())
		require.Equal(t, float64(90), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_objective", map[string]string{
			"resource_group": testSnapshotRG, "phase": "committed",
		}).GetGauge().GetValue())

		fixture.clock.Advance(2 * time.Second)
		publishEpochManagerSegments(fixture.placement, []int64{201}, []int64{101})
		terminal := fixture.manager.Advance(context.Background(), request)
		require.Equal(t, EpochDegraded, terminal.State)
		require.True(t, terminal.Terminal)
		requireEpochMetricFamilyAbsent(t, registry, "milvus_querycoord_balance_epoch_active")
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_total", map[string]string{
			"resource_group": testSnapshotRG, "result": "degraded",
		}).GetCounter().GetValue())
		duration := requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_duration_seconds", map[string]string{
			"resource_group": testSnapshotRG, "result": "degraded",
		}).GetHistogram()
		require.Equal(t, uint64(1), duration.GetSampleCount())
		require.Equal(t, float64(2), duration.GetSampleSum())

		runtime := fixture.manager.runtime(testSnapshotRG)
		runtime.mu.Lock()
		cached := fixture.manager.Advance(context.Background(), request)
		runtime.mu.Unlock()
		require.Equal(t, terminal, cached)
		runtime.mu.Lock()
		cached = fixture.manager.Advance(context.Background(), request)
		runtime.mu.Unlock()
		require.Equal(t, terminal, cached)
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_total", map[string]string{
			"resource_group": testSnapshotRG, "result": "degraded",
		}).GetCounter().GetValue())
		duration = requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_duration_seconds", map[string]string{
			"resource_group": testSnapshotRG, "result": "degraded",
		}).GetHistogram()
		require.Equal(t, uint64(1), duration.GetSampleCount())
	})

	t.Run("shadow publishes plans and objective without generation metrics", func(t *testing.T) {
		registry := newEpochManagerMetricsRegistry(t)
		fixture := newEpochManagerTestFixture(t)
		plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
		fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))

		request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
		request.Shadow = true
		request.AllowNew = false
		observed := fixture.manager.Advance(context.Background(), request)
		require.Equal(t, EpochIdle, observed.State)
		require.Zero(t, fixture.source.buildCount(testSnapshotRG))

		request.AllowNew = true
		result := fixture.manager.Advance(context.Background(), request)
		require.Equal(t, EpochCompleted, result.State)
		require.True(t, result.Terminal)
		require.Zero(t, result.Epoch)
		require.False(t, fixture.manager.HasActive(testSnapshotRG))
		require.Empty(t, fixture.admitter.acceptedTasks())
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_plans_total", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment", "result": "shadow",
		}).GetCounter().GetValue())
		require.Equal(t, float64(100), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_objective", map[string]string{
			"resource_group": testSnapshotRG, "phase": "observed",
		}).GetGauge().GetValue())
		require.Equal(t, float64(90), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_objective", map[string]string{
			"resource_group": testSnapshotRG, "phase": "projected",
		}).GetGauge().GetValue())
		require.Equal(t, float64(90), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_objective", map[string]string{
			"resource_group": testSnapshotRG, "phase": "shadow",
		}).GetGauge().GetValue())
		requireEpochMetricFamilyAbsent(t, registry, "milvus_querycoord_balance_epoch_active")
		requireEpochMetricFamilyAbsent(t, registry, "milvus_querycoord_balance_epoch_total")
		requireEpochMetricFamilyAbsent(t, registry, "milvus_querycoord_balance_epoch_duration_seconds")
		requireEpochMetricFamilyAbsent(t, registry, "milvus_querycoord_balance_epoch_admission_total")
	})

	t.Run("shadow timeout after planning retains the computed wave", func(t *testing.T) {
		registry := newEpochManagerMetricsRegistry(t)
		fixture := newEpochManagerTestFixture(t)
		plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
		fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
		entered, release := fixture.policy.blockFirst(testSnapshotRG)
		defer release()

		request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
		request.Shadow = true
		request.Deadline = time.Second
		resultCh := make(chan EpochAdvanceResult, 1)
		go func() {
			resultCh <- fixture.manager.Advance(context.Background(), request)
		}()
		receiveBalanceTestSignal(t, entered, "blocked shadow planner")
		fixture.clock.Advance(2 * time.Second)
		release()

		result := receiveBalanceTestSignal(t, resultCh, "timed-out shadow result")
		require.Equal(t, EpochTimedOut, result.State)
		require.True(t, result.Terminal)
		require.Equal(t, 1, result.Planned)
		require.Equal(t, float64(100), result.ObjectiveBefore)
		require.Equal(t, float64(90), result.ObjectiveAfter)
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_plans_total", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment", "result": "shadow",
		}).GetCounter().GetValue())
		require.Equal(t, float64(90), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_objective", map[string]string{
			"resource_group": testSnapshotRG, "phase": "shadow",
		}).GetGauge().GetValue())
		requireEpochMetricFamilyAbsent(t, registry, "milvus_querycoord_balance_epoch_active")
		requireEpochMetricFamilyAbsent(t, registry, "milvus_querycoord_balance_epoch_total")
		requireEpochMetricFamilyAbsent(t, registry, "milvus_querycoord_balance_epoch_admission_total")
	})

	t.Run("snapshot failure uses its stable terminal result exactly once", func(t *testing.T) {
		registry := newEpochManagerMetricsRegistry(t)
		fixture := newEpochManagerTestFixture(t)
		request := epochManagerRequest("missing-rg")

		terminal := fixture.manager.Advance(context.Background(), request)
		require.Equal(t, EpochDegraded, terminal.State)
		require.True(t, terminal.Terminal)
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_total", map[string]string{
			"resource_group": "missing-rg", "result": "snapshot_error",
		}).GetCounter().GetValue())
		duration := requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_duration_seconds", map[string]string{
			"resource_group": "missing-rg", "result": "snapshot_error",
		}).GetHistogram()
		require.Equal(t, uint64(1), duration.GetSampleCount())

		runtime := fixture.manager.runtime("missing-rg")
		runtime.mu.Lock()
		cached := fixture.manager.Advance(context.Background(), request)
		runtime.mu.Unlock()
		require.Equal(t, terminal, cached)
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_total", map[string]string{
			"resource_group": "missing-rg", "result": "snapshot_error",
		}).GetCounter().GetValue())
		duration = requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_duration_seconds", map[string]string{
			"resource_group": "missing-rg", "result": "snapshot_error",
		}).GetHistogram()
		require.Equal(t, uint64(1), duration.GetSampleCount())
	})

	t.Run("default snapshot builder publishes retry metrics", func(t *testing.T) {
		registry := newEpochManagerMetricsRegistry(t)
		fixture := newEpochManagerTestFixture(t)
		fixture.policy.setWaves(testSnapshotRG, BalanceWave{Kind: PlanKindSegment, Converged: true})
		manager := NewBalanceEpochManager(
			fixture.placement.meta,
			fixture.placement.dist,
			fixture.placement.target,
			fixture.placement.nodes,
			nil,
			fixture.admitter,
			fixture.admitter,
			task.WrapIDSource(6),
			func(string, EpochPolicyConfig) (EpochBalancePolicy, bool) { return fixture.policy, true },
			WithEpochClock(fixture.clock.Now),
			WithLeaderTerm(77),
		)

		mutated := false
		fixture.placement.targetState.mu.Lock()
		fixture.placement.targetState.segmentsHook = func(collectionID int64, scope int32) {
			if collectionID != 100 || scope != meta.CurrentTarget || mutated {
				return
			}
			mutated = true
			fixture.placement.targetState.mu.Lock()
			fixture.placement.targetState.currentVersion[collectionID]++
			fixture.placement.targetState.mu.Unlock()
		}
		fixture.placement.targetState.mu.Unlock()

		request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
		request.Shadow = true
		result := manager.Advance(context.Background(), request)
		require.Equal(t, EpochCompleted, result.State)
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_snapshot_retries_total", map[string]string{
			"resource_group": testSnapshotRG,
		}).GetCounter().GetValue())
	})
}

func TestEpochManagerCarryMetricOutlivesTerminal(t *testing.T) {
	registry := newEpochManagerMetricsRegistry(t)
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(
		testSnapshotRG,
		epochManagerWave(plan),
		BalanceWave{Kind: PlanKindSegment, Converged: true},
	)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.Deadline = time.Second
	request.NoProgressDeadline = 0

	started := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, started.State)
	require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
		"resource_group": testSnapshotRG, "kind": "segment",
	}).GetGauge().GetValue())
	requireEpochMetricAbsent(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
		"resource_group": testSnapshotRG, "kind": "channel",
	})

	fixture.clock.Advance(2 * time.Second)
	terminal := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochTimedOut, terminal.State)
	require.True(t, terminal.Terminal)
	require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
		"resource_group": testSnapshotRG, "kind": "segment",
	}).GetGauge().GetValue())

	request.AllowNew = false
	idle := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochIdle, idle.State)
	require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
		"resource_group": testSnapshotRG, "kind": "segment",
	}).GetGauge().GetValue())

	publishEpochManagerSegments(fixture.placement, []int64{201}, []int64{101})
	request.AllowNew = true
	restarted := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, restarted.State)
	resolved := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochCompleted, resolved.State)
	requireEpochMetricAbsent(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
		"resource_group": testSnapshotRG, "kind": "segment",
	})

	t.Run("quarantine remains after terminal and expires", func(t *testing.T) {
		registry := newEpochManagerMetricsRegistry(t)
		fixture := newEpochManagerTestFixture(t)
		plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
		fixture.policy.setWaves(
			testSnapshotRG,
			epochManagerWave(plan),
			epochManagerWave(plan),
		)
		request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
		request.MaxObjectRetries = 2
		request.QuarantineBackoff = time.Minute

		fixture.manager.Advance(context.Background(), request)
		fixture.admitter.acceptedTasks()[0].Fail(errors.New("first grow failure"))
		first := fixture.manager.Advance(context.Background(), request)
		require.Equal(t, EpochDegraded, first.State)

		fixture.manager.Advance(context.Background(), request)
		fixture.admitter.acceptedTasks()[1].Fail(errors.New("second grow failure"))
		second := fixture.manager.Advance(context.Background(), request)
		require.Equal(t, EpochDegraded, second.State)
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment",
		}).GetGauge().GetValue())

		request.AllowNew = false
		fixture.manager.Advance(context.Background(), request)
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment",
		}).GetGauge().GetValue())

		fixture.clock.Advance(2 * time.Minute)
		fixture.manager.Advance(context.Background(), request)
		requireEpochMetricAbsent(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment",
		})
	})

	t.Run("disabling retries clears the quarantine metric", func(t *testing.T) {
		registry := newEpochManagerMetricsRegistry(t)
		fixture := newEpochManagerTestFixture(t)
		plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
		fixture.policy.setWaves(
			testSnapshotRG,
			epochManagerWave(plan),
			epochManagerWave(plan),
			BalanceWave{Kind: PlanKindSegment, Converged: true},
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
		require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment",
		}).GetGauge().GetValue())

		request.MaxObjectRetries = 0
		request.QuarantineBackoff = 0
		result := fixture.manager.Advance(context.Background(), request)
		require.Equal(t, EpochCompleted, result.State)
		require.NotContains(t, fixture.policy.lastConstraints(testSnapshotRG).Objects, plan.ObjectKey())
		requireEpochMetricAbsent(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
			"resource_group": testSnapshotRG, "kind": "segment",
		})
	})
}

func TestEpochManagerObservationAppliesRetryDisablePolicy(t *testing.T) {
	registry := newEpochManagerMetricsRegistry(t)
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan), epochManagerWave(plan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)

	fixture.manager.Advance(context.Background(), request)
	fixture.admitter.acceptedTasks()[0].Fail(errors.New("first grow failure"))
	fixture.manager.Advance(context.Background(), request)
	fixture.manager.Advance(context.Background(), request)
	fixture.admitter.acceptedTasks()[1].Fail(errors.New("second grow failure"))
	fixture.manager.Advance(context.Background(), request)

	request.AllowNew = false
	fixture.manager.Advance(context.Background(), request)
	require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
		"resource_group": testSnapshotRG, "kind": "segment",
	}).GetGauge().GetValue())

	request.MaxObjectRetries = 0
	request.QuarantineBackoff = 0
	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochIdle, result.State)
	requireEpochMetricAbsent(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
		"resource_group": testSnapshotRG, "kind": "segment",
	})
	require.NotContains(t, fixture.manager.ResourceGroupsToObserve(), testSnapshotRG)
}

func TestEpochManagerObservationPrunesExpiredQuarantine(t *testing.T) {
	registry := newEpochManagerMetricsRegistry(t)
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan), epochManagerWave(plan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)

	fixture.manager.Advance(context.Background(), request)
	fixture.admitter.acceptedTasks()[0].Fail(errors.New("first grow failure"))
	fixture.manager.Advance(context.Background(), request)
	fixture.manager.Advance(context.Background(), request)
	fixture.admitter.acceptedTasks()[1].Fail(errors.New("second grow failure"))
	fixture.manager.Advance(context.Background(), request)

	request.AllowNew = false
	fixture.manager.Advance(context.Background(), request)
	require.Contains(t, fixture.manager.ResourceGroupsToObserve(), testSnapshotRG)
	require.Equal(t, float64(1), requireEpochMetric(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
		"resource_group": testSnapshotRG, "kind": "segment",
	}).GetGauge().GetValue())

	fixture.clock.Advance(2 * time.Minute)
	fixture.manager.Advance(context.Background(), request)
	requireEpochMetricAbsent(t, registry, "milvus_querycoord_balance_epoch_carry_over", map[string]string{
		"resource_group": testSnapshotRG, "kind": "segment",
	})
	runtime := fixture.manager.runtime(testSnapshotRG)
	runtime.mu.Lock()
	retryCount := len(runtime.retries)
	runtime.mu.Unlock()
	require.Zero(t, retryCount)
	require.NotContains(t, fixture.manager.ResourceGroupsToObserve(), testSnapshotRG)
}

func TestEpochManagerRetainedRetryHistoryRemainsObservableUntilDisabled(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)

	fixture.manager.Advance(context.Background(), request)
	fixture.admitter.acceptedTasks()[0].Fail(errors.New("first grow failure"))
	fixture.manager.Advance(context.Background(), request)

	request.AllowNew = false
	fixture.manager.Advance(context.Background(), request)
	require.Contains(t, fixture.manager.ResourceGroupsToObserve(), testSnapshotRG)

	request.MaxObjectRetries = 0
	request.QuarantineBackoff = 0
	fixture.manager.Advance(context.Background(), request)
	require.NotContains(t, fixture.manager.ResourceGroupsToObserve(), testSnapshotRG)
}

func TestEpochManagerUsesFrozenPolicySelection(t *testing.T) {
	t.Run("active generation", func(t *testing.T) {
		fixture := newEpochManagerTestFixture(t)
		plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
		fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))

		type selection struct {
			balancer string
			config   EpochPolicyConfig
		}
		selections := make([]selection, 0, 1)
		fixture.manager.policyProvider = func(balancer string, config EpochPolicyConfig) (EpochBalancePolicy, bool) {
			selections = append(selections, selection{balancer: balancer, config: config})
			return fixture.policy, true
		}

		request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
		request.Balancer = meta.ScoreBasedBalancerName
		request.PolicyConfig = EpochPolicyConfig{
			GlobalRowCountFactor:          0.11,
			DelegatorMemoryOverloadFactor: 0.22,
			CollectionChannelCountFactor:  0.33,
			AutoBalanceChannel:            true,
			StreamingServiceEnabled:       false,
		}
		result := fixture.manager.Advance(context.Background(), request)
		require.True(t, result.Started)
		require.Equal(t, 1, result.Admitted)

		changed := request
		changed.Balancer = meta.ChannelLevelScoreBalancerName
		changed.PolicyConfig.AutoBalanceChannel = false
		changed.PolicyConfig.StreamingServiceEnabled = true
		fixture.manager.Advance(context.Background(), changed)

		require.Equal(t, []selection{{balancer: request.Balancer, config: request.PolicyConfig}}, selections)
		require.Equal(t, request, fixture.manager.runtime(testSnapshotRG).active.request)
	})

	t.Run("shadow generation", func(t *testing.T) {
		fixture := newEpochManagerTestFixture(t)
		request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
		request.Shadow = true
		request.Balancer = meta.ChannelLevelScoreBalancerName
		request.PolicyConfig = EpochPolicyConfig{CollectionChannelCountFactor: 7}

		providerCalls := 0
		fixture.manager.policyProvider = func(balancer string, config EpochPolicyConfig) (EpochBalancePolicy, bool) {
			providerCalls++
			require.Equal(t, request.Balancer, balancer)
			require.Equal(t, request.PolicyConfig, config)
			return fixture.policy, true
		}

		result := fixture.manager.Advance(context.Background(), request)
		require.True(t, result.Terminal)
		require.Equal(t, 1, providerCalls)
	})
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
			case 103:
				collectionID, partitionID, rows, channel = 100, 10, 40, "channel-a"
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

func publishEpochManagerAcceptedSegmentMove(
	t *testing.T,
	fixture *placementSnapshotFixture,
	balanceTask task.Task,
) {
	t.Helper()
	segmentTask, ok := balanceTask.(*task.SegmentTask)
	require.True(t, ok)
	var source, target int64
	for _, action := range balanceTask.Actions() {
		switch action.Type() {
		case task.ActionTypeGrow:
			target = action.Node()
		case task.ActionTypeReduce:
			source = action.Node()
		}
	}
	require.NotZero(t, source)
	require.NotZero(t, target)

	captured := fixture.dist.Capture()
	byNode := map[int64][]*meta.Segment{1: {}, 3: {}}
	var moved *meta.Segment
	for _, record := range captured.Segments {
		if record.NodeID != 1 && record.NodeID != 3 {
			continue
		}
		segment := &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID: record.SegmentID, CollectionID: record.CollectionID,
				PartitionID: record.PartitionID, InsertChannel: record.Channel,
				NumOfRows: record.RowCount,
			},
			Node:    record.NodeID,
			Version: record.Version,
		}
		if record.SegmentID == segmentTask.SegmentID() && record.NodeID == source {
			moved = segment
			continue
		}
		byNode[record.NodeID] = append(byNode[record.NodeID], segment)
	}
	require.NotNil(t, moved)
	moved.Node = target
	moved.Version++
	byNode[target] = append(byNode[target], moved)
	fixture.dist.PublishNodeDistribution(1, byNode[1], fixture.node1Channels)
	fixture.dist.PublishNodeDistribution(3, byNode[3], nil)
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

func TestEpochManagerDeadlineDesiredPlacementCompletes(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan), BalanceWave{Kind: PlanKindSegment, Converged: true})
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.Deadline = time.Second
	request.NoProgressDeadline = 0

	started := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, started.State)
	fixture.clock.Advance(2 * time.Second)
	publishEpochManagerSegments(fixture.placement, []int64{201}, []int64{101})

	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochCompleted, result.State)
	require.True(t, result.Terminal)
	require.NoError(t, result.Err)

	fixture.manager.Advance(context.Background(), request)
	require.Empty(t, fixture.source.lastCarry(testSnapshotRG))
	require.NotContains(t, fixture.policy.lastConstraints(testSnapshotRG).Objects, plan.ObjectKey())
}

func TestEpochManagerNodeOrRGChangeSupersedesGeneration(t *testing.T) {
	for _, reason := range []task.BalanceAdmissionReason{
		task.BalanceAdmissionNodeIneligible,
		task.BalanceAdmissionRGChanged,
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

func TestEpochManagerOwnTaskRemovalDoesNotSupersede(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)

	started := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, started.State)
	accepted := fixture.admitter.acceptedTasks()[0]
	accepted.SetStatus(task.TaskStatusSucceeded)
	accepted.Cancel(nil)
	publishEpochManagerSegments(fixture.placement, []int64{201}, []int64{101})
	fixture.source.setValidate(func(AdmissionToken) task.BalanceAdmissionReason {
		return task.BalanceAdmissionStaleEpoch
	})

	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochCompleted, result.State)
	require.True(t, result.Terminal)
	require.Zero(t, result.Rejected[task.BalanceAdmissionStaleEpoch])
}

func TestEpochManagerAdmissionStaleStopsPrefix(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plans := []EpochPlan{
		epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3),
		epochSegmentPlan(*fixture.snapshot, testOtherReplica, 201, 1, 3),
	}
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plans...))
	fixture.source.setValidate(func(AdmissionToken) task.BalanceAdmissionReason {
		return task.BalanceAdmissionStaleEpoch
	})

	result := fixture.manager.Advance(
		context.Background(),
		epochManagerRequest(testSnapshotRG, testEligibleReplica, testOtherReplica),
	)
	require.Equal(t, EpochSuperseded, result.State)
	require.Equal(t, 2, result.Planned)
	require.Zero(t, result.Admitted)
	require.Equal(t, 1, result.Rejected[task.BalanceAdmissionStaleEpoch])
	require.Empty(t, fixture.admitter.acceptedTasks())
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
		func(string, EpochPolicyConfig) (EpochBalancePolicy, bool) { return restartedPolicy, true },
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

func TestEpochManagerNoProgressIgnoresUnrelatedOrReorderedRecords(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.Deadline = time.Minute
	request.NoProgressDeadline = time.Second

	started := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, started.State)
	accepted := fixture.admitter.acceptedTasks()[0]
	work := &admittedWork{plan: plan, task: accepted}
	from := meta.SegmentSnapshotRecord{
		SegmentID: 101, CollectionID: 100, PartitionID: 10, Channel: "channel-a",
		NodeID: 1, RowCount: 100, Version: 11, Scope: querypb.DataScope_Historical, Present: true,
	}
	to := from
	to.NodeID = 3
	to.Version = 12
	left := observeWork(work, meta.DistributionSnapshot{Segments: []meta.SegmentSnapshotRecord{from, to}})
	right := observeWork(work, meta.DistributionSnapshot{Segments: []meta.SegmentSnapshotRecord{to, from}})
	require.Equal(t, left.detailDigest, right.detailDigest)

	fixture.clock.Advance(750 * time.Millisecond)
	fixture.placement.dist.PublishNodeDistribution(4, []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID: 301, CollectionID: 300, PartitionID: 30,
				InsertChannel: "channel-c", NumOfRows: 300,
			},
			Node: 4, Version: 41,
		},
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID: 101, CollectionID: 100, PartitionID: 10,
				InsertChannel: "channel-a", NumOfRows: 100,
			},
			Node: 4, Version: 999,
		},
	}, nil)
	progress := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, progress.State)

	fixture.clock.Advance(500 * time.Millisecond)
	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochTimedOut, result.State)
}

func TestEpochManagerAmbiguousQuiescentIsDegraded(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)

	fixture.manager.Advance(context.Background(), request)
	fixture.admitter.acceptedTasks()[0].Cancel(errors.New("executor outcome ambiguous"))
	result := fixture.manager.Advance(context.Background(), request)

	require.Equal(t, EpochDegraded, result.State)
	require.True(t, result.Terminal)
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "ambiguous")
}

func TestEpochManagerAmbiguousCarryAddsDiagnosticAfterBudgetStop(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	carriedPlan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	newPlan := epochSegmentPlan(*fixture.snapshot, testOtherReplica, 201, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(carriedPlan), epochManagerWave(newPlan))
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica, testOtherReplica)
	request.Deadline = time.Second
	request.NoProgressDeadline = 0

	fixture.manager.Advance(context.Background(), request)
	fixture.clock.Advance(2 * time.Second)
	timedOut := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochTimedOut, timedOut.State)

	request.Deadline = time.Minute
	request.Budget.MaxSegmentTasks = 0
	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochDegraded, result.State)
	require.True(t, result.Terminal)
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "ambiguous")
}

func TestEpochManagerAmbiguousCarryPreservesAdmissionError(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	carriedPlan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	newPlan := epochSegmentPlan(*fixture.snapshot, testOtherReplica, 201, 1, 3)
	fixture.policy.setWaves(
		testSnapshotRG,
		epochManagerWave(carriedPlan),
		epochManagerWave(newPlan),
		BalanceWave{Kind: PlanKindSegment, Converged: true},
	)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica, testOtherReplica)
	request.Deadline = time.Second
	request.NoProgressDeadline = 0

	fixture.manager.Advance(context.Background(), request)
	fixture.clock.Advance(2 * time.Second)
	timedOut := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochTimedOut, timedOut.State)

	admissionErr := errors.New("ordinary admission rejection")
	fixture.manager.admitter = &epochManagerRejectAdmitter{
		reason: task.BalanceAdmissionDuplicate,
		err:    admissionErr,
	}
	request.Deadline = time.Minute
	result := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochDegraded, result.State)
	require.True(t, result.Terminal)
	require.Equal(t, 1, result.Rejected[task.BalanceAdmissionDuplicate])
	require.ErrorIs(t, result.Err, admissionErr)
	require.Contains(t, result.Err.Error(), "ambiguous")

	carrying := fixture.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, carrying.State)
	require.Len(t, fixture.source.lastCarry(testSnapshotRG), 1)
	constraint := fixture.policy.lastConstraints(testSnapshotRG).Objects[carriedPlan.ObjectKey()]
	require.Equal(t, ReservationAmbiguousCapacity, constraint.Class)
	require.Equal(t, []int64{1, 3}, constraint.ChargedNodes)
}

func TestEpochManagerDeadlineAfterRejectedAdmissionWinsOrdinaryFailure(t *testing.T) {
	fixture := newEpochManagerTestFixture(t)
	plan := epochSegmentPlan(*fixture.snapshot, testEligibleReplica, 101, 1, 3)
	fixture.policy.setWaves(testSnapshotRG, epochManagerWave(plan))
	blocking := &epochManagerBlockingRejectAdmitter{
		entered: make(chan struct{}), release: make(chan struct{}),
		reason: task.BalanceAdmissionDuplicate,
	}
	fixture.manager.admitter = blocking
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.Deadline = time.Second

	resultCh := make(chan EpochAdvanceResult, 1)
	go func() {
		resultCh <- fixture.manager.Advance(context.Background(), request)
	}()
	receiveBalanceTestSignal(t, blocking.entered, "blocked rejecting admission")
	fixture.clock.Advance(2 * time.Second)
	close(blocking.release)
	result := receiveBalanceTestSignal(t, resultCh, "post-deadline rejection")

	require.Equal(t, EpochTimedOut, result.State)
	require.True(t, result.Terminal)
	require.Equal(t, 1, result.Rejected[task.BalanceAdmissionDuplicate])
}

type balanceEpochRecordedMove struct {
	epoch      task.BalanceEpochMeta
	object     BalanceObjectKey
	collection int64
	from       int64
	to         int64
}

func (move balanceEpochRecordedMove) String() string {
	return fmt.Sprintf(
		"epoch=%d replica=%d object=%v collection=%d %d->%d",
		move.epoch.Sequence,
		move.object.ReplicaID,
		move.object,
		move.collection,
		move.from,
		move.to,
	)
}

type balanceEpochPotentialSample struct {
	epoch    uint64
	planned  int
	admitted int
	before   float64
	prefix   []float64
	after    float64
}

func (sample balanceEpochPotentialSample) String() string {
	return fmt.Sprintf(
		"epoch=%d planned=%d admitted=%d potential=%.6f prefix=%v ->%.6f",
		sample.epoch,
		sample.planned,
		sample.admitted,
		sample.before,
		sample.prefix,
		sample.after,
	)
}

type balanceEpochAdmissionGate struct {
	entered chan struct{}
	release chan struct{}
	reason  task.BalanceAdmissionReason
	fired   bool
}

type balanceEpochRecordingAdmitter struct {
	mu sync.Mutex

	nextID          int64
	revision        uint64
	rgRevisions     map[string]uint64
	epochRevisions  map[task.BalanceEpochMeta]uint64
	pending         map[int64]task.Task
	accepted        []task.Task
	moves           []balanceEpochRecordedMove
	concurrent      []BalanceObjectKey
	gates           map[string]*balanceEpochAdmissionGate
	pendingByObject map[BalanceObjectKey]int64
}

func newBalanceEpochRecordingAdmitter() *balanceEpochRecordingAdmitter {
	return &balanceEpochRecordingAdmitter{
		rgRevisions:     make(map[string]uint64),
		epochRevisions:  make(map[task.BalanceEpochMeta]uint64),
		pending:         make(map[int64]task.Task),
		gates:           make(map[string]*balanceEpochAdmissionGate),
		pendingByObject: make(map[BalanceObjectKey]int64),
	}
}

func (admitter *balanceEpochRecordingAdmitter) blockNext(
	resourceGroup string,
	reason task.BalanceAdmissionReason,
) (<-chan struct{}, func()) {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	gate := &balanceEpochAdmissionGate{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		reason:  reason,
	}
	admitter.gates[resourceGroup] = gate
	var once sync.Once
	return gate.entered, func() { once.Do(func() { close(gate.release) }) }
}

func (admitter *balanceEpochRecordingAdmitter) AdmitBalanceTaskAtPendingRevision(
	balanceTask task.Task,
	expected task.BalancePendingRevision,
	validate task.BalanceAdmissionValidator,
) task.BalanceAdmissionResult {
	if reason := validate(); reason != task.BalanceAdmissionAccepted {
		err := errors.New(reason.String())
		balanceTask.Cancel(err)
		return task.BalanceAdmissionResult{Reason: reason, Err: err, PendingRevision: expected}
	}
	if reason := validate(); reason != task.BalanceAdmissionAccepted {
		err := errors.New(reason.String())
		balanceTask.Cancel(err)
		return task.BalanceAdmissionResult{Reason: reason, Err: err, PendingRevision: expected}
	}

	admitter.mu.Lock()
	current := admitter.pendingRevisionLocked(expected.ResourceGroup, expected.Epoch)
	if current.EffectiveRevision() != expected.EffectiveRevision() {
		admitter.mu.Unlock()
		err := errors.New(task.BalanceAdmissionStaleEpoch.String())
		balanceTask.Cancel(err)
		return task.BalanceAdmissionResult{
			Reason: task.BalanceAdmissionStaleEpoch, Err: err, PendingRevision: current,
		}
	}
	gate := admitter.gates[expected.ResourceGroup]
	if gate != nil && !gate.fired {
		gate.fired = true
		admitter.mu.Unlock()
		close(gate.entered)
		<-gate.release
		err := errors.New(gate.reason.String())
		balanceTask.Cancel(err)
		return task.BalanceAdmissionResult{Reason: gate.reason, Err: err, PendingRevision: expected}
	}

	object, move := recordedBalanceEpochMove(balanceTask)
	if _, exists := admitter.pendingByObject[object]; exists {
		admitter.concurrent = append(admitter.concurrent, object)
		admitter.mu.Unlock()
		err := errors.New(task.BalanceAdmissionDuplicate.String())
		balanceTask.Cancel(err)
		return task.BalanceAdmissionResult{Reason: task.BalanceAdmissionDuplicate, Err: err, PendingRevision: current}
	}

	admitter.nextID++
	balanceTask.SetID(admitter.nextID)
	move.epoch = balanceTask.BalanceEpoch()
	admitter.pending[balanceTask.ID()] = balanceTask
	admitter.pendingByObject[object] = balanceTask.ID()
	admitter.accepted = append(admitter.accepted, balanceTask)
	admitter.moves = append(admitter.moves, move)
	admitter.revision++
	admitter.rgRevisions[expected.ResourceGroup]++
	admitter.epochRevisions[expected.Epoch]++
	current = admitter.pendingRevisionLocked(expected.ResourceGroup, expected.Epoch)
	admitter.mu.Unlock()

	return task.BalanceAdmissionResult{
		TaskID: balanceTask.ID(), Reason: task.BalanceAdmissionAccepted, PendingRevision: current,
	}
}

func (admitter *balanceEpochRecordingAdmitter) pendingRevisionLocked(
	resourceGroup string,
	epoch task.BalanceEpochMeta,
) task.BalancePendingRevision {
	return task.BalancePendingRevision{
		ResourceGroup: resourceGroup,
		Epoch:         epoch,
		Revision:      admitter.rgRevisions[resourceGroup],
		EpochRevision: admitter.epochRevisions[epoch],
	}
}

func (admitter *balanceEpochRecordingAdmitter) GetPendingBalanceTasks() task.PendingBalanceSnapshot {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	rgRevisions := make(map[string]uint64, len(admitter.rgRevisions))
	for resourceGroup, revision := range admitter.rgRevisions {
		rgRevisions[resourceGroup] = revision
	}
	epochRevisions := make(map[task.BalanceEpochMeta]uint64, len(admitter.epochRevisions))
	for epoch, revision := range admitter.epochRevisions {
		epochRevisions[epoch] = revision
	}
	tasks := make([]task.PendingBalanceTaskSnapshot, 0, len(admitter.pending))
	for _, pending := range admitter.pending {
		tasks = append(tasks, snapshotBalanceEpochTask(pending))
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].TaskID < tasks[j].TaskID })
	return task.PendingBalanceSnapshot{
		Revision:               admitter.revision,
		ResourceGroupRevisions: rgRevisions,
		EpochRevisions:         epochRevisions,
		Tasks:                  tasks,
	}
}

func (admitter *balanceEpochRecordingAdmitter) tasksForEpoch(epoch task.BalanceEpochMeta) []task.Task {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	tasks := make([]task.Task, 0)
	for _, accepted := range admitter.accepted {
		if accepted.BalanceEpoch() == epoch {
			tasks = append(tasks, accepted)
		}
	}
	return tasks
}

func (admitter *balanceEpochRecordingAdmitter) acceptedTasks() []task.Task {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	return append([]task.Task(nil), admitter.accepted...)
}

func (admitter *balanceEpochRecordingAdmitter) recordedMoves() []balanceEpochRecordedMove {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	return append([]balanceEpochRecordedMove(nil), admitter.moves...)
}

func (admitter *balanceEpochRecordingAdmitter) concurrentObjects() []BalanceObjectKey {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	return append([]BalanceObjectKey(nil), admitter.concurrent...)
}

func (admitter *balanceEpochRecordingAdmitter) remove(balanceTask task.Task) {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	if _, ok := admitter.pending[balanceTask.ID()]; !ok {
		return
	}
	delete(admitter.pending, balanceTask.ID())
	object, _ := recordedBalanceEpochMove(balanceTask)
	delete(admitter.pendingByObject, object)
	admitter.revision++
	resourceGroup := balanceTask.ResourceGroup()
	admitter.rgRevisions[resourceGroup]++
	epoch := balanceTask.BalanceEpoch()
	for _, pending := range admitter.pending {
		if pending.BalanceEpoch() == epoch {
			return
		}
	}
	delete(admitter.epochRevisions, epoch)
}

func (admitter *balanceEpochRecordingAdmitter) pendingNodeCounts() map[int64]int {
	admitter.mu.Lock()
	defer admitter.mu.Unlock()
	counts := make(map[int64]int)
	for _, pending := range admitter.pending {
		seen := make(map[int64]struct{})
		for _, action := range pending.Actions() {
			seen[action.Node()] = struct{}{}
		}
		for nodeID := range seen {
			counts[nodeID]++
		}
	}
	return counts
}

func snapshotBalanceEpochTask(balanceTask task.Task) task.PendingBalanceTaskSnapshot {
	snapshot := task.PendingBalanceTaskSnapshot{
		TaskID:        balanceTask.ID(),
		CollectionID:  balanceTask.CollectionID(),
		ReplicaID:     balanceTask.ReplicaID(),
		ResourceGroup: balanceTask.ResourceGroup(),
		Epoch:         balanceTask.BalanceEpoch(),
		Status:        balanceTask.Status(),
		Actions:       make([]task.PendingBalanceActionSnapshot, 0, len(balanceTask.Actions())),
	}
	for _, action := range balanceTask.Actions() {
		primitive := task.PendingBalanceActionSnapshot{
			NodeID: action.Node(), Type: action.Type(), Workload: action.WorkLoadEffect(),
		}
		switch action := action.(type) {
		case *task.SegmentAction:
			primitive.SegmentID = action.GetSegmentID()
			primitive.Shard = action.GetShard()
			primitive.Scope = action.GetScope()
		case *task.ChannelAction:
			primitive.Channel = action.ChannelName()
			primitive.Shard = action.GetShard()
		}
		snapshot.Actions = append(snapshot.Actions, primitive)
	}
	return snapshot
}

func recordedBalanceEpochMove(balanceTask task.Task) (BalanceObjectKey, balanceEpochRecordedMove) {
	move := balanceEpochRecordedMove{
		collection: balanceTask.CollectionID(),
	}
	for _, action := range balanceTask.Actions() {
		switch action.Type() {
		case task.ActionTypeGrow:
			move.to = action.Node()
		case task.ActionTypeReduce:
			move.from = action.Node()
		}
	}
	switch typed := balanceTask.(type) {
	case *task.SegmentTask:
		scope := querypb.DataScope_Historical
		for _, action := range balanceTask.Actions() {
			if segmentAction, ok := action.(*task.SegmentAction); ok {
				scope = segmentAction.GetScope()
				break
			}
		}
		move.object = BalanceObjectKey{
			Kind: BalanceObjectSegment, ReplicaID: balanceTask.ReplicaID(),
			SegmentID: typed.SegmentID(), Scope: scope,
		}
	case *task.ChannelTask:
		move.object = BalanceObjectKey{
			Kind: BalanceObjectChannel, ReplicaID: balanceTask.ReplicaID(), Channel: typed.Channel(),
		}
	}
	return move.object, move
}

type balanceEpochSegmentSpec struct {
	id           int64
	collectionID int64
	partitionID  int64
	channel      string
	rows         int64
}

type balanceEpochScenario struct {
	placement *placementSnapshotFixture
	admitter  *balanceEpochRecordingAdmitter
	clock     *epochManagerClock
	policy    EpochBalancePolicy
	manager   *BalanceEpochManager
}

func newBalanceEpochScenario(t *testing.T) *balanceEpochScenario {
	t.Helper()
	placement := newPlacementSnapshotFixture(t)
	placement.nodes.Get(1).UpdateStats(session.WithMemCapacity(1024))
	placement.nodes.Get(3).UpdateStats(session.WithMemCapacity(1024))
	admitter := newBalanceEpochRecordingAdmitter()
	clock := newEpochManagerClock()
	policy := newScoreEpochPolicy(false)
	manager := NewBalanceEpochManager(
		placement.meta,
		placement.dist,
		placement.target,
		placement.nodes,
		nil,
		admitter,
		admitter,
		task.WrapIDSource(6),
		func(string, EpochPolicyConfig) (EpochBalancePolicy, bool) { return policy, true },
		WithEpochClock(clock.Now),
		WithLeaderTerm(51244),
	)
	return &balanceEpochScenario{
		placement: placement,
		admitter:  admitter,
		clock:     clock,
		policy:    policy,
		manager:   manager,
	}
}

func balanceEpochSegments(collectionID, firstID int64, rows ...int64) []balanceEpochSegmentSpec {
	channel := "channel-a"
	partitionID := int64(10)
	if collectionID == 200 {
		channel = "channel-b"
		partitionID = 20
	}
	segments := make([]balanceEpochSegmentSpec, 0, len(rows))
	for index, rowCount := range rows {
		segments = append(segments, balanceEpochSegmentSpec{
			id: firstID + int64(index), collectionID: collectionID,
			partitionID: partitionID, channel: channel, rows: rowCount,
		})
	}
	return segments
}

func (scenario *balanceEpochScenario) setSegments(specs ...balanceEpochSegmentSpec) {
	byCollection := make(map[int64]map[int64]*datapb.SegmentInfo)
	segments := make([]*meta.Segment, 0, len(specs))
	for _, spec := range specs {
		info := &datapb.SegmentInfo{
			ID: spec.id, CollectionID: spec.collectionID, PartitionID: spec.partitionID,
			InsertChannel: spec.channel, NumOfRows: spec.rows,
		}
		if byCollection[spec.collectionID] == nil {
			byCollection[spec.collectionID] = make(map[int64]*datapb.SegmentInfo)
		}
		byCollection[spec.collectionID][spec.id] = info
		segments = append(segments, &meta.Segment{SegmentInfo: info, Node: 1, Version: 1000 + spec.id})
	}

	scenario.placement.targetState.mu.Lock()
	for collectionID, targets := range byCollection {
		current := make(map[int64]*datapb.SegmentInfo, len(targets))
		next := make(map[int64]*datapb.SegmentInfo, len(targets))
		for segmentID, info := range targets {
			current[segmentID] = info
			next[segmentID] = info
		}
		scenario.placement.targetState.currentSegments[collectionID] = current
		scenario.placement.targetState.nextSegments[collectionID] = next
	}
	scenario.placement.targetState.mu.Unlock()
	scenario.placement.dist.PublishNodeDistribution(1, segments, scenario.placement.node1Channels)
	scenario.placement.dist.PublishNodeDistribution(3, nil, nil)
}

func (scenario *balanceEpochScenario) setChannels(channelsByCollection map[int64][]string) {
	channels := make([]*meta.DmChannel, 0)
	scenario.placement.targetState.mu.Lock()
	for collectionID, names := range channelsByCollection {
		current := make(map[string]*meta.DmChannel, len(names))
		next := make(map[string]*meta.DmChannel, len(names))
		for _, name := range names {
			target := &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name}}
			current[name] = target
			next[name] = target
			version := scenario.placement.targetState.currentVersion[collectionID]
			channels = append(channels, &meta.DmChannel{
				VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name},
				Version:      version + int64(len(channels)) + 1,
				View: &meta.LeaderView{
					ID: 1, CollectionID: collectionID, Channel: name,
					Version: version + int64(len(channels)) + 1, TargetVersion: version,
					Status: &querypb.LeaderViewStatus{Serviceable: true},
				},
			})
		}
		scenario.placement.targetState.currentChannels[collectionID] = current
		scenario.placement.targetState.nextChannels[collectionID] = next
	}
	scenario.placement.targetState.mu.Unlock()

	captured := scenario.placement.dist.Capture()
	segmentsByNode, _ := primitiveBalanceEpochDistribution(captured)
	scenario.placement.dist.PublishNodeDistribution(1, segmentsByNode[1], channels)
	scenario.placement.dist.PublishNodeDistribution(3, segmentsByNode[3], nil)
}

func (scenario *balanceEpochScenario) publishGrow(balanceTask task.Task) {
	_, move := recordedBalanceEpochMove(balanceTask)
	captured := scenario.placement.dist.Capture()
	switch move.object.Kind {
	case BalanceObjectSegment:
		var source *meta.SegmentSnapshotRecord
		for index := range captured.Segments {
			record := &captured.Segments[index]
			if record.CollectionID == move.collection && record.SegmentID == move.object.SegmentID &&
				record.Scope == move.object.Scope && record.NodeID == move.from {
				source = record
				break
			}
		}
		if source == nil {
			panic(fmt.Sprintf("source segment missing for %s", move))
		}
		for _, record := range captured.Segments {
			if record.CollectionID == move.collection && record.SegmentID == move.object.SegmentID &&
				record.Scope == move.object.Scope && record.NodeID == move.to {
				return
			}
		}
		target := *source
		target.NodeID = move.to
		target.Version++
		captured.Segments = append(captured.Segments, target)
	case BalanceObjectChannel:
		var source *meta.ChannelSnapshotRecord
		for index := range captured.Channels {
			record := &captured.Channels[index]
			if record.CollectionID == move.collection && record.Channel == move.object.Channel && record.NodeID == move.from {
				source = record
				break
			}
		}
		if source == nil {
			panic(fmt.Sprintf("source channel missing for %s", move))
		}
		for _, record := range captured.Channels {
			if record.CollectionID == move.collection && record.Channel == move.object.Channel && record.NodeID == move.to {
				return
			}
		}
		target := *source
		target.NodeID = move.to
		target.Version++
		target.Serviceable = true
		target.LeaderID = move.to
		target.LeaderVersion++
		target.LeaderTargetVersion = scenario.frozenCurrentTargetVersion(balanceTask)
		captured.Channels = append(captured.Channels, target)
	}
	scenario.publish(captured)
}

func (scenario *balanceEpochScenario) frozenCurrentTargetVersion(balanceTask task.Task) int64 {
	runtime := scenario.manager.runtime(balanceTask.ResourceGroup())
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.active == nil {
		panic(fmt.Sprintf("active epoch missing for task %d", balanceTask.ID()))
	}
	work := runtime.active.admitted[balanceTask.ID()]
	if work == nil {
		for _, carried := range runtime.carryOver {
			if carried.task != nil && carried.task.ID() == balanceTask.ID() {
				work = carried
				break
			}
		}
	}
	if work == nil {
		panic(fmt.Sprintf("admitted work missing for task %d", balanceTask.ID()))
	}
	return work.plan.Token.Snapshot.CurrentTargetVersion[balanceTask.CollectionID()]
}

func (scenario *balanceEpochScenario) publishReduce(balanceTask task.Task) {
	_, move := recordedBalanceEpochMove(balanceTask)
	captured := scenario.placement.dist.Capture()
	switch move.object.Kind {
	case BalanceObjectSegment:
		segments := captured.Segments[:0]
		for _, record := range captured.Segments {
			if record.CollectionID == move.collection && record.SegmentID == move.object.SegmentID &&
				record.Scope == move.object.Scope && record.NodeID == move.from {
				continue
			}
			segments = append(segments, record)
		}
		captured.Segments = segments
	case BalanceObjectChannel:
		channels := captured.Channels[:0]
		for _, record := range captured.Channels {
			if record.CollectionID == move.collection && record.Channel == move.object.Channel && record.NodeID == move.from {
				continue
			}
			channels = append(channels, record)
		}
		captured.Channels = channels
	}
	scenario.publish(captured)
}

func (scenario *balanceEpochScenario) publish(captured meta.DistributionSnapshot) {
	segmentsByNode, channelsByNode := primitiveBalanceEpochDistribution(captured)
	for _, nodeID := range []int64{1, 3} {
		scenario.placement.dist.PublishNodeDistribution(nodeID, segmentsByNode[nodeID], channelsByNode[nodeID])
	}
}

func primitiveBalanceEpochDistribution(
	captured meta.DistributionSnapshot,
) (map[int64][]*meta.Segment, map[int64][]*meta.DmChannel) {
	segmentsByNode := make(map[int64][]*meta.Segment)
	for _, record := range captured.Segments {
		if record.NodeID != 1 && record.NodeID != 3 {
			continue
		}
		segmentsByNode[record.NodeID] = append(segmentsByNode[record.NodeID], &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID: record.SegmentID, CollectionID: record.CollectionID, PartitionID: record.PartitionID,
				InsertChannel: record.Channel, NumOfRows: record.RowCount,
			},
			Node: record.NodeID, Version: record.Version,
		})
	}
	channelsByNode := make(map[int64][]*meta.DmChannel)
	for _, record := range captured.Channels {
		if record.NodeID != 1 && record.NodeID != 3 {
			continue
		}
		growing := make(map[int64]*meta.Segment, len(record.GrowingSegments))
		for _, segment := range record.GrowingSegments {
			growing[segment.SegmentID] = &meta.Segment{
				SegmentInfo: &datapb.SegmentInfo{
					ID: segment.SegmentID, CollectionID: record.CollectionID,
					InsertChannel: record.Channel, NumOfRows: segment.RowCount,
				},
				Node: segment.NodeID,
			}
		}
		channelsByNode[record.NodeID] = append(channelsByNode[record.NodeID], &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: record.CollectionID, ChannelName: record.Channel},
			Version:      record.Version,
			View: &meta.LeaderView{
				ID: record.LeaderID, CollectionID: record.CollectionID, Channel: record.Channel,
				Version: record.LeaderVersion, TargetVersion: record.LeaderTargetVersion,
				Status:          &querypb.LeaderViewStatus{Serviceable: record.Serviceable},
				GrowingSegments: growing,
			},
		})
	}
	return segmentsByNode, channelsByNode
}

func (scenario *balanceEpochScenario) complete(balanceTask task.Task) {
	scenario.publishGrow(balanceTask)
	scenario.publishReduce(balanceTask)
	balanceTask.SetStatus(task.TaskStatusSucceeded)
	scenario.admitter.remove(balanceTask)
}

func (scenario *balanceEpochScenario) failGrow(balanceTask task.Task, err error) {
	balanceTask.Fail(err)
	scenario.admitter.remove(balanceTask)
}

func (scenario *balanceEpochScenario) failReduce(balanceTask task.Task, err error) {
	scenario.publishGrow(balanceTask)
	balanceTask.Fail(err)
	scenario.admitter.remove(balanceTask)
}

func requireBalanceEpochTaskBudgets(
	t *testing.T,
	tasks []task.Task,
	budget BalanceWaveBudget,
) {
	t.Helper()
	segments := 0
	channels := 0
	collections := make(map[int64]int)
	nodes := make(map[int64]int)
	for _, balanceTask := range tasks {
		switch balanceTask.(type) {
		case *task.SegmentTask:
			segments++
		case *task.ChannelTask:
			channels++
		}
		collections[balanceTask.CollectionID()]++
		seen := make(map[int64]struct{})
		for _, action := range balanceTask.Actions() {
			seen[action.Node()] = struct{}{}
		}
		for nodeID := range seen {
			nodes[nodeID]++
		}
	}
	require.LessOrEqual(t, segments, budget.MaxSegmentTasks)
	require.LessOrEqual(t, channels, budget.MaxChannelTasks)
	for collectionID, count := range collections {
		require.LessOrEqual(t, count, budget.MaxTasksPerCollection, "collection %d", collectionID)
	}
	for nodeID, count := range nodes {
		require.LessOrEqual(t, count, budget.MaxTasksPerNode, "node %d", nodeID)
	}
}

func requireBalanceEpochNoReverseMoves(t *testing.T, moves []balanceEpochRecordedMove) {
	t.Helper()
	last := make(map[BalanceObjectKey]balanceEpochRecordedMove)
	for _, move := range moves {
		if previous, ok := last[move.object]; ok {
			require.Falsef(
				t,
				previous.from == move.to && previous.to == move.from,
				"reverse cycle: %s then %s; sequence=%v",
				previous,
				move,
				moves,
			)
		}
		last[move.object] = move
	}
}

func requireBalanceEpochPlacement(
	t *testing.T,
	scenario *balanceEpochScenario,
	balanceTask task.Task,
	sourcePresent bool,
	targetPresent bool,
) {
	t.Helper()
	_, move := recordedBalanceEpochMove(balanceTask)
	observation := observeWork(&admittedWork{
		plan: EpochPlan{
			Kind: func() PlanKind {
				if move.object.Kind == BalanceObjectChannel {
					return PlanKindChannel
				}
				return PlanKindSegment
			}(),
			CollectionID: move.collection,
			ReplicaID:    move.object.ReplicaID,
			SegmentID:    move.object.SegmentID,
			Channel:      move.object.Channel,
			Scope:        move.object.Scope,
			From:         move.from,
			To:           move.to,
			Token: AdmissionToken{Snapshot: SnapshotToken{CurrentTargetVersion: map[int64]int64{
				move.collection: scenario.frozenCurrentTargetVersion(balanceTask),
			}}},
		},
		task: balanceTask,
	}, scenario.placement.dist.Capture())
	require.Equal(t, sourcePresent, observation.sourcePresent, move.String())
	require.Equal(t, targetPresent, observation.targetPresent, move.String())
	if move.object.Kind == BalanceObjectChannel && targetPresent {
		require.True(t, observation.targetReady, move.String())
	}
}

func totalBalanceEpochRejections(rejected map[task.BalanceAdmissionReason]int) int {
	total := 0
	for _, count := range rejected {
		total += count
	}
	return total
}

func balanceEpochPotentialForResult(
	t *testing.T,
	manager *BalanceEpochManager,
	result EpochAdvanceResult,
) balanceEpochPotentialSample {
	t.Helper()
	sample := balanceEpochPotentialSample{
		epoch: result.Epoch.Sequence, planned: result.Planned, admitted: result.Admitted,
		before: result.ObjectiveBefore, after: result.ObjectiveAfter,
	}
	if result.Admitted == 0 {
		return sample
	}
	runtime := manager.runtime(result.ResourceGroup)
	runtime.mu.Lock()
	active := runtime.active
	var epoch task.BalanceEpochMeta
	var prefix []ScorePotential
	if active != nil {
		epoch = active.epoch
		prefix = append([]ScorePotential(nil), active.wave.PrefixAfter...)
	}
	runtime.mu.Unlock()
	require.NotNil(t, active)
	require.Equal(t, result.Epoch, epoch)
	require.GreaterOrEqual(t, len(prefix), result.Admitted)
	for _, potential := range prefix[:result.Admitted] {
		sample.prefix = append(sample.prefix, potential.Value)
	}

	previous := sample.before
	for _, potential := range sample.prefix {
		require.LessOrEqual(t, potential, previous+1e-9, "%v", sample)
		previous = potential
	}
	require.InDelta(t, previous, sample.after, 1e-9, "%v", sample)
	return sample
}

func TestBalanceEpochIssue51244StaticTargetConverges(t *testing.T) {
	scenario := newBalanceEpochScenario(t)
	scenario.setSegments(balanceEpochSegments(100, 110, 10, 20, 30, 40, 50, 60, 70, 80)...)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.PolicyConfig = scorePolicyTestConfig(false)
	request.Budget = BalanceWaveBudget{
		MaxSegmentTasks: 2, MaxChannelTasks: 1,
		MaxTasksPerNode: 2, MaxTasksPerCollection: 2,
	}

	potentials := make([]balanceEpochPotentialSample, 0, 8)
	converged := false
	for generation := 0; generation < 12; generation++ {
		result := scenario.manager.Advance(context.Background(), request)
		potentials = append(potentials, balanceEpochPotentialForResult(t, scenario.manager, result))
		if result.Admitted == 0 {
			require.Equal(t, EpochCompleted, result.State)
			require.True(t, result.Converged)
			require.Zero(t, result.Planned)
			converged = true
			break
		}

		require.Equal(t, EpochExecuting, result.State)
		require.Equal(t, result.Planned, result.Admitted, "rejected suffix in %v", potentials)
		require.Zero(t, totalBalanceEpochRejections(result.Rejected), "rejected suffix in %v", potentials)
		require.LessOrEqual(t, result.ObjectiveAfter, result.ObjectiveBefore+1e-9, "%v", potentials)
		if len(potentials) > 1 {
			previous := potentials[len(potentials)-2]
			require.LessOrEqual(t, result.ObjectiveBefore, previous.after+1e-9, "%v", potentials)
		}
		tasks := scenario.admitter.tasksForEpoch(result.Epoch)
		require.Len(t, tasks, result.Admitted)
		requireBalanceEpochTaskBudgets(t, tasks, request.Budget)
		for _, accepted := range tasks {
			scenario.complete(accepted)
		}
		reconciled := scenario.manager.Advance(context.Background(), request)
		require.Equal(t, EpochCompleted, reconciled.State)
		require.True(t, reconciled.Terminal)
	}
	require.True(t, converged, "static target did not converge; potentials=%v moves=%v", potentials, scenario.admitter.recordedMoves())
	require.NotEmpty(t, scenario.admitter.recordedMoves())
	require.Empty(t, scenario.admitter.concurrentObjects())
	requireBalanceEpochNoReverseMoves(t, scenario.admitter.recordedMoves())
	require.Zero(t, potentials[len(potentials)-1].admitted, "move sequence never reached zero: %v", potentials)
	t.Logf("#51244 potentials=%v", potentials)
	t.Logf("#51244 moves=%v", scenario.admitter.recordedMoves())
}

func TestBalanceEpochDelayedGrowDistributionDoesNotReplanObject(t *testing.T) {
	scenario := newBalanceEpochScenario(t)
	scenario.setSegments(balanceEpochSegments(100, 120, 25, 25, 25, 25)...)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.PolicyConfig = scorePolicyTestConfig(false)
	request.Budget.MaxSegmentTasks = 1
	request.Budget.MaxTasksPerCollection = 1
	request.Budget.MaxTasksPerNode = 1

	started := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, started.State)
	require.Equal(t, 1, started.Admitted)
	tasks := scenario.admitter.tasksForEpoch(started.Epoch)
	require.Len(t, tasks, 1)
	moveCount := len(scenario.admitter.recordedMoves())

	withheld := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, withheld.State)
	require.Equal(t, started.Epoch, withheld.Epoch)
	require.Len(t, scenario.admitter.recordedMoves(), moveCount)
	require.Len(t, scenario.admitter.acceptedTasks(), 1)
	requireBalanceEpochPlacement(t, scenario, tasks[0], true, false)

	scenario.publishGrow(tasks[0])
	growObserved := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, growObserved.State)
	require.Len(t, scenario.admitter.recordedMoves(), moveCount)
	requireBalanceEpochPlacement(t, scenario, tasks[0], true, true)

	scenario.publishReduce(tasks[0])
	tasks[0].SetStatus(task.TaskStatusSucceeded)
	scenario.admitter.remove(tasks[0])
	completed := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochCompleted, completed.State)
	require.True(t, completed.Terminal)
	requireBalanceEpochPlacement(t, scenario, tasks[0], false, true)
	require.Empty(t, scenario.admitter.concurrentObjects())
}

func TestBalanceEpochOneFailedTaskDoesNotRollbackSuccessfulMove(t *testing.T) {
	scenario := newBalanceEpochScenario(t)
	scenario.setSegments(balanceEpochSegments(100, 130, 10, 10, 10, 10, 10, 10, 10, 10)...)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.PolicyConfig = scorePolicyTestConfig(false)
	request.Budget = BalanceWaveBudget{
		MaxSegmentTasks: 3, MaxChannelTasks: 1,
		MaxTasksPerNode: 3, MaxTasksPerCollection: 3,
	}

	started := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, started.State)
	require.Equal(t, 3, started.Admitted)
	require.Equal(t, started.Planned, started.Admitted)
	require.LessOrEqual(t, started.ObjectiveAfter, started.ObjectiveBefore+1e-9)
	tasks := scenario.admitter.tasksForEpoch(started.Epoch)
	require.Len(t, tasks, 3)
	requireBalanceEpochTaskBudgets(t, tasks, request.Budget)

	scenario.complete(tasks[0])
	scenario.failGrow(tasks[1], errors.New("grow failed"))
	scenario.failReduce(tasks[2], errors.New("reduce failed"))
	reconciled := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochDegraded, reconciled.State)
	require.True(t, reconciled.Terminal)
	requireBalanceEpochPlacement(t, scenario, tasks[0], false, true)
	requireBalanceEpochPlacement(t, scenario, tasks[1], true, false)
	requireBalanceEpochPlacement(t, scenario, tasks[2], true, true)
	require.Empty(t, scenario.admitter.concurrentObjects())
	requireBalanceEpochNoReverseMoves(t, scenario.admitter.recordedMoves())
}

func TestBalanceEpochStuckObjectAllowsUnrelatedCollectionProgress(t *testing.T) {
	scenario := newBalanceEpochScenario(t)
	scenario.placement.nodes.Get(1).UpdateStats(session.WithMemCapacity(1024))
	scenario.placement.nodes.Get(3).UpdateStats(session.WithMemCapacity(4096))
	segments := append(
		balanceEpochSegments(100, 140, 100),
		balanceEpochSegments(200, 240, 200)...,
	)
	scenario.setSegments(segments...)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica, testOtherReplica)
	request.PolicyConfig = scorePolicyTestConfig(false)
	request.NoProgressDeadline = time.Second
	request.Budget = BalanceWaveBudget{
		MaxSegmentTasks: 1, MaxChannelTasks: 1,
		MaxTasksPerNode: 2, MaxTasksPerCollection: 1,
	}

	first := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, first.State)
	require.Equal(t, 1, first.Admitted)
	firstTasks := scenario.admitter.tasksForEpoch(first.Epoch)
	require.Len(t, firstTasks, 1)
	scenario.publishGrow(firstTasks[0])
	requireBalanceEpochPlacement(t, scenario, firstTasks[0], true, true)
	require.Equal(t, EpochExecuting, scenario.manager.Advance(context.Background(), request).State)
	scenario.clock.Advance(2 * time.Second)
	timedOut := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochTimedOut, timedOut.State)
	require.True(t, timedOut.Terminal)

	second := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, second.State)
	require.Equal(t, 1, second.Admitted)
	secondTasks := scenario.admitter.tasksForEpoch(second.Epoch)
	require.Len(t, secondTasks, 1)
	require.NotEqual(t, firstTasks[0].CollectionID(), secondTasks[0].CollectionID())
	require.NotEqual(t, firstTasks[0].ReplicaID(), secondTasks[0].ReplicaID())
	requireBalanceEpochTaskBudgets(t, secondTasks, request.Budget)
	for nodeID, count := range scenario.admitter.pendingNodeCounts() {
		require.LessOrEqual(t, count, request.Budget.MaxTasksPerNode, "node %d", nodeID)
	}

	scenario.complete(secondTasks[0])
	requireBalanceEpochPlacement(t, scenario, secondTasks[0], false, true)
	require.Equal(t, EpochExecuting, scenario.manager.Advance(context.Background(), request).State)
	scenario.clock.Advance(2 * time.Second)
	secondTimedOut := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochTimedOut, secondTimedOut.State)
	require.True(t, secondTimedOut.Terminal)
	requireBalanceEpochPlacement(t, scenario, firstTasks[0], true, true)
	requireBalanceEpochPlacement(t, scenario, secondTasks[0], false, true)
	require.Empty(t, scenario.admitter.concurrentObjects())
}

func TestEpochManagerExecutionAllowsAdmittedChannelLeaderHandoff(t *testing.T) {
	scenario := newBalanceEpochScenario(t)
	scenario.setChannels(map[int64][]string{
		100: {"channel-a", "channel-a-2", "channel-a-3"},
	})
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.PolicyConfig = scorePolicyTestConfig(true)
	request.Budget = BalanceWaveBudget{
		MaxSegmentTasks: 1, MaxChannelTasks: 1,
		MaxTasksPerNode: 1, MaxTasksPerCollection: 1,
	}

	started := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, started.State)
	require.Equal(t, 1, started.Admitted)
	tasks := scenario.admitter.tasksForEpoch(started.Epoch)
	require.Len(t, tasks, 1)
	require.IsType(t, &task.ChannelTask{}, tasks[0])

	scenario.complete(tasks[0])
	reconciled := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochCompleted, reconciled.State)
	require.True(t, reconciled.Terminal)
	requireBalanceEpochPlacement(t, scenario, tasks[0], false, true)
}

func TestEpochManagerExecutionDoesNotSupersedeOnPostAdmissionLeaderPublication(t *testing.T) {
	scenario := newBalanceEpochScenario(t)
	scenario.setSegments(balanceEpochSegments(100, 145, 25, 25, 25, 25)...)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.PolicyConfig = scorePolicyTestConfig(false)
	request.Budget = BalanceWaveBudget{
		MaxSegmentTasks: 1, MaxChannelTasks: 1,
		MaxTasksPerNode: 1, MaxTasksPerCollection: 1,
	}

	started := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochExecuting, started.State)
	tasks := scenario.admitter.tasksForEpoch(started.Epoch)
	require.Len(t, tasks, 1)
	require.IsType(t, &task.SegmentTask{}, tasks[0])

	captured := scenario.placement.dist.Capture()
	changed := false
	for index := range captured.Channels {
		if captured.Channels[index].CollectionID == 200 && captured.Channels[index].Channel == "channel-b" {
			captured.Channels[index].LeaderVersion++
			changed = true
			break
		}
	}
	require.True(t, changed)
	scenario.publish(captured)
	scenario.complete(tasks[0])

	reconciled := scenario.manager.Advance(context.Background(), request)
	require.Equal(t, EpochCompleted, reconciled.State)
	require.True(t, reconciled.Terminal)
	require.Zero(t, reconciled.Rejected[task.BalanceAdmissionLeaderMissing])
	requireBalanceEpochPlacement(t, scenario, tasks[0], false, true)
}

func TestBalanceEpochOneRGFailureDoesNotBlockAnotherRG(t *testing.T) {
	scenario := newBalanceEpochScenario(t)
	scenario.setSegments(balanceEpochSegments(100, 150, 25, 25, 25, 25)...)
	requestA := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	requestA.PolicyConfig = scorePolicyTestConfig(false)
	requestA.Budget.MaxSegmentTasks = 1
	requestA.Budget.MaxTasksPerCollection = 1
	requestA.Budget.MaxTasksPerNode = 1
	entered, release := scenario.admitter.blockNext(testSnapshotRG, task.BalanceAdmissionDuplicate)
	defer release()

	resultA := make(chan EpochAdvanceResult, 1)
	go func() { resultA <- scenario.manager.Advance(context.Background(), requestA) }()
	receiveBalanceTestSignal(t, entered, "RG-A blocked admission")

	requestB := epochManagerRequest(testUnrelatedRG, 13)
	requestB.PolicyConfig = scorePolicyTestConfig(false)
	resultB := make(chan EpochAdvanceResult, 1)
	go func() { resultB <- scenario.manager.Advance(context.Background(), requestB) }()
	independent := receiveBalanceTestSignal(t, resultB, "RG-B independent epoch")
	require.Equal(t, EpochCompleted, independent.State)
	require.True(t, independent.Terminal)
	require.True(t, independent.Converged)

	release()
	failed := receiveBalanceTestSignal(t, resultA, "RG-A rejected admission")
	require.Equal(t, EpochDegraded, failed.State)
	require.True(t, failed.Terminal)
	require.Equal(t, 1, failed.Rejected[task.BalanceAdmissionDuplicate])
}

func TestBalanceEpochHardBudgetAcrossCollectionsAndReplicas(t *testing.T) {
	scenario := newBalanceEpochScenario(t)
	segments := append(
		balanceEpochSegments(100, 160, 10, 10, 10, 10),
		balanceEpochSegments(200, 260, 20, 20, 20, 20)...,
	)
	scenario.setSegments(segments...)
	scenario.setChannels(map[int64][]string{
		100: {"channel-a", "channel-a-2", "channel-a-3"},
		200: {"channel-b", "channel-b-2", "channel-b-3"},
	})
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica, testOtherReplica)
	request.PolicyConfig = scorePolicyTestConfig(true)
	request.Budget = BalanceWaveBudget{
		MaxSegmentTasks: 2, MaxChannelTasks: 2,
		MaxTasksPerNode: 2, MaxTasksPerCollection: 1,
	}

	sawSegments := false
	sawChannels := false
	converged := false
	potentials := make([]balanceEpochPotentialSample, 0, 8)
	for generation := 0; generation < 8; generation++ {
		result := scenario.manager.Advance(context.Background(), request)
		potentials = append(potentials, balanceEpochPotentialForResult(t, scenario.manager, result))
		if result.Admitted == 0 {
			require.Equal(t, EpochCompleted, result.State)
			require.True(t, result.Converged)
			converged = true
			break
		}
		require.Equal(t, EpochExecuting, result.State)
		require.Equal(t, result.Planned, result.Admitted)
		require.Zero(t, totalBalanceEpochRejections(result.Rejected))
		require.LessOrEqual(t, result.ObjectiveAfter, result.ObjectiveBefore+1e-9)
		tasks := scenario.admitter.tasksForEpoch(result.Epoch)
		require.Len(t, tasks, result.Admitted)
		requireBalanceEpochTaskBudgets(t, tasks, request.Budget)
		for _, balanceTask := range tasks {
			switch balanceTask.(type) {
			case *task.SegmentTask:
				sawSegments = true
			case *task.ChannelTask:
				sawChannels = true
			}
			scenario.complete(balanceTask)
		}
		reconciled := scenario.manager.Advance(context.Background(), request)
		require.Equal(t, EpochCompleted, reconciled.State, "potentials=%v moves=%v", potentials, scenario.admitter.recordedMoves())
	}
	require.True(t, converged, "hard-budget fixture did not converge; potentials=%v moves=%v", potentials, scenario.admitter.recordedMoves())
	require.True(t, sawChannels, "channel budget was not exercised; moves=%v", scenario.admitter.recordedMoves())
	require.True(t, sawSegments, "segment budget was not exercised; moves=%v", scenario.admitter.recordedMoves())
	require.Empty(t, scenario.admitter.concurrentObjects())
	requireBalanceEpochNoReverseMoves(t, scenario.admitter.recordedMoves())
	t.Logf("hard-budget potentials=%v", potentials)
	t.Logf("hard-budget moves=%v", scenario.admitter.recordedMoves())
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
