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

	"github.com/prometheus/client_golang/prometheus"
	clientmodel "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
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

func TestEpochManagerStaticTargetConvergesWithProductionPolicy(t *testing.T) {
	fixture := newPlacementSnapshotFixture(t)
	fixture.targetState.mu.Lock()
	fixture.targetState.currentSegments[100] = map[int64]*datapb.SegmentInfo{
		101: {ID: 101, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 10},
		103: {ID: 103, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 10},
		104: {ID: 104, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 10},
		105: {ID: 105, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 10},
	}
	fixture.targetState.nextSegments[100] = map[int64]*datapb.SegmentInfo{
		101: {ID: 101, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 10},
		103: {ID: 103, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 10},
		104: {ID: 104, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 10},
		105: {ID: 105, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-a", NumOfRows: 10},
	}
	fixture.targetState.mu.Unlock()
	node1Segments := []*meta.Segment{fixture.node1Segments[1]}
	for _, segmentID := range []int64{101, 103, 104, 105} {
		node1Segments = append(node1Segments, &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID: segmentID, CollectionID: 100, PartitionID: 10,
				InsertChannel: "channel-a", NumOfRows: 10,
			},
			Node: 1, Version: segmentID,
		})
	}
	fixture.dist.PublishNodeDistribution(1, node1Segments, fixture.node1Channels)
	fixture.dist.PublishNodeDistribution(3, nil, nil)

	admitter := &epochManagerAdmitter{}
	clock := newEpochManagerClock()
	policy := newScoreEpochPolicy(false)
	manager := NewBalanceEpochManager(
		fixture.meta,
		fixture.dist,
		fixture.target,
		fixture.nodes,
		nil,
		admitter,
		admitter,
		task.WrapIDSource(6),
		func(string, EpochPolicyConfig) (EpochBalancePolicy, bool) { return policy, true },
		WithEpochClock(clock.Now),
		WithLeaderTerm(99),
	)
	request := epochManagerRequest(testSnapshotRG, testEligibleReplica)
	request.PolicyConfig = scorePolicyTestConfig(false)
	request.Budget.MaxSegmentTasks = 1

	admittedByGeneration := make([]int, 0, 4)
	moves := make(map[int64][2]int64)
	consumedTasks := 0
	for generation := 0; generation < 10; generation++ {
		result := manager.Advance(context.Background(), request)
		admittedByGeneration = append(admittedByGeneration, result.Admitted)
		if result.Admitted == 0 {
			require.Equal(t, EpochCompleted, result.State)
			require.True(t, result.Converged)
			require.GreaterOrEqual(t, len(admittedByGeneration), 3)
			for _, admitted := range admittedByGeneration[:len(admittedByGeneration)-1] {
				require.Equal(t, 1, admitted)
			}
			require.Zero(t, admittedByGeneration[len(admittedByGeneration)-1])
			return
		}

		require.Equal(t, EpochExecuting, result.State)
		require.Equal(t, 1, result.Admitted)
		accepted := admitter.acceptedTasks()[consumedTasks]
		consumedTasks++
		segmentTask := accepted.(*task.SegmentTask)
		var source, target int64
		for _, action := range accepted.Actions() {
			if action.Type() == task.ActionTypeGrow {
				target = action.Node()
			} else if action.Type() == task.ActionTypeReduce {
				source = action.Node()
			}
		}
		if previous, ok := moves[segmentTask.SegmentID()]; ok {
			require.NotEqual(t, [2]int64{previous[1], previous[0]}, [2]int64{source, target})
		}
		moves[segmentTask.SegmentID()] = [2]int64{source, target}
		publishEpochManagerAcceptedSegmentMove(t, fixture, accepted)
		reconciled := manager.Advance(context.Background(), request)
		require.Equal(t, EpochCompleted, reconciled.State)
	}
	t.Fatal("static target did not converge within 10 generations")
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
