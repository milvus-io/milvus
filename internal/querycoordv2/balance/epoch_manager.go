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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type EpochState int

const (
	EpochIdle EpochState = iota
	EpochPlanning
	EpochAdmitting
	EpochExecuting
	EpochReconciling
	EpochCompleted
	EpochDegraded
	EpochSuperseded
	EpochTimedOut
)

type EpochRequest struct {
	ResourceGroup      string
	EligibleReplicaIDs []int64
	Budget             BalanceWaveBudget
	PolicyConfig       EpochPolicyConfig
	AllowNew           bool
	Shadow             bool
	Deadline           time.Duration
	NoProgressDeadline time.Duration
	SegmentTaskTimeout time.Duration
	ChannelTaskTimeout time.Duration
	MaxObjectRetries   int
	QuarantineBackoff  time.Duration
}

type EpochAdvanceResult struct {
	ResourceGroup   string
	Epoch           task.BalanceEpochMeta
	State           EpochState
	Planned         int
	Admitted        int
	Rejected        map[task.BalanceAdmissionReason]int
	Started         bool
	Terminal        bool
	Converged       bool
	ObjectiveBefore float64
	ObjectiveAfter  float64
	Err             error
}

type BalanceEpochController interface {
	Advance(context.Context, EpochRequest) EpochAdvanceResult
	HasActive(resourceGroup string) bool
	ActiveResourceGroups() []string
}

type EpochTaskFactory interface {
	Build(
		context.Context,
		EpochPlan,
		*meta.Replica,
		task.BalanceEpochMeta,
		time.Duration,
	) (task.Task, error)
}

type PlacementSnapshotSource interface {
	Build(context.Context, string, []int64, []task.PendingBalanceTaskSnapshot) (*PlacementSnapshot, error)
	Validate(AdmissionToken) task.BalanceAdmissionReason
}

type EpochManagerOption func(*BalanceEpochManager)

func WithEpochClock(now func() time.Time) EpochManagerOption {
	return func(manager *BalanceEpochManager) {
		if now != nil {
			manager.now = now
		}
	}
}

func WithLeaderTerm(term uint64) EpochManagerOption {
	return func(manager *BalanceEpochManager) {
		manager.leaderTerm = term
	}
}

func WithEpochTaskFactory(factory EpochTaskFactory) EpochManagerOption {
	return func(manager *BalanceEpochManager) {
		if factory != nil {
			manager.taskFactory = factory
		}
	}
}

func WithPlacementSnapshotSource(source PlacementSnapshotSource) EpochManagerOption {
	return func(manager *BalanceEpochManager) {
		if source != nil {
			manager.snapshotBuilder = source
		}
	}
}

type BalanceEpochManager struct {
	meta           *meta.Meta
	dist           *meta.DistributionManager
	targetMgr      meta.TargetManagerInterface
	nodeMgr        *session.NodeManager
	scheduler      task.Scheduler
	admitter       task.BalanceTaskGenerationAdmitter
	inspector      task.BalanceTaskInspector
	source         task.Source
	policyProvider func() (EpochBalancePolicy, bool)

	now             func() time.Time
	leaderTerm      uint64
	taskFactory     EpochTaskFactory
	snapshotBuilder PlacementSnapshotSource

	runtimesMu sync.Mutex
	runtimes   map[string]*rgRuntime
}

type rgRuntime struct {
	mu         sync.Mutex
	activeFlag atomic.Bool
	sequence   uint64
	active     *activeEpoch
	carryOver  map[BalanceObjectKey]*admittedWork
	retries    map[BalanceObjectKey]*objectRetryHistory
	last       atomic.Value
}

type activeEpoch struct {
	epoch          task.BalanceEpochMeta
	request        EpochRequest
	state          EpochState
	startedAt      time.Time
	lastProgressAt time.Time
	token          SnapshotToken
	wave           BalanceWave
	ledger         *WaveLedger
	admitted       map[int64]*admittedWork
	terminalIntent EpochState
	progressDigest uint64
	result         EpochAdvanceResult
}

type admittedWork struct {
	plan EpochPlan
	task task.Task
}

type objectRetryHistory struct {
	count           int
	quarantineUntil time.Time
	plan            EpochPlan
	token           SnapshotToken
}

type defaultEpochTaskFactory struct {
	source task.Source
}

func NewBalanceEpochManager(
	metadata *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	admitter task.BalanceTaskGenerationAdmitter,
	inspector task.BalanceTaskInspector,
	source task.Source,
	policyProvider func() (EpochBalancePolicy, bool),
	opts ...EpochManagerOption,
) *BalanceEpochManager {
	if admitter == nil {
		admitter, _ = scheduler.(task.BalanceTaskGenerationAdmitter)
	}
	if inspector == nil {
		inspector, _ = scheduler.(task.BalanceTaskInspector)
	}
	manager := &BalanceEpochManager{
		meta:           metadata,
		dist:           dist,
		targetMgr:      targetMgr,
		nodeMgr:        nodeMgr,
		scheduler:      scheduler,
		admitter:       admitter,
		inspector:      inspector,
		source:         source,
		policyProvider: policyProvider,
		now:            time.Now,
		runtimes:       make(map[string]*rgRuntime),
	}
	manager.taskFactory = &defaultEpochTaskFactory{source: source}
	if metadata != nil && dist != nil && nodeMgr != nil {
		manager.snapshotBuilder = NewPlacementSnapshotBuilder(metadata, dist, targetMgr, nodeMgr, inspector)
	}
	for _, opt := range opts {
		opt(manager)
	}
	if manager.leaderTerm == 0 {
		manager.leaderTerm = newEpochLeaderTerm()
	}
	return manager
}

func (factory *defaultEpochTaskFactory) Build(
	ctx context.Context,
	plan EpochPlan,
	replica *meta.Replica,
	epoch task.BalanceEpochMeta,
	timeout time.Duration,
) (task.Task, error) {
	var balanceTask task.Task
	var err error
	switch plan.Kind {
	case PlanKindSegment:
		balanceTask, err = task.NewSegmentTask(
			ctx,
			timeout,
			factory.source,
			plan.CollectionID,
			replica,
			commonpb.LoadPriority_LOW,
			task.NewSegmentActionWithScope(
				plan.To, task.ActionTypeGrow, plan.Shard, plan.SegmentID,
				querypb.DataScope_Historical, int(plan.RowCount),
			),
			task.NewSegmentActionWithScope(
				plan.From, task.ActionTypeReduce, plan.Shard, plan.SegmentID,
				querypb.DataScope_Historical, int(plan.RowCount),
			),
		)
		if err == nil {
			balanceTask.SetReason("segment unbalanced")
		}
	case PlanKindChannel:
		balanceTask, err = task.NewChannelTask(
			ctx,
			timeout,
			factory.source,
			plan.CollectionID,
			replica,
			task.NewChannelAction(plan.To, task.ActionTypeGrow, plan.Channel),
			task.NewChannelAction(plan.From, task.ActionTypeReduce, plan.Channel),
		)
		if err == nil {
			balanceTask.SetReason("channel unbalanced")
		}
	default:
		return nil, fmt.Errorf("unsupported epoch plan kind %d", plan.Kind)
	}
	if err != nil {
		return nil, err
	}
	balanceTask.SetPriority(task.TaskPriorityLow)
	balanceTask.SetBalanceEpoch(epoch)
	return balanceTask, nil
}

func (manager *BalanceEpochManager) Advance(ctx context.Context, request EpochRequest) EpochAdvanceResult {
	runtime := manager.runtime(request.ResourceGroup)
	if !runtime.mu.TryLock() {
		return runtime.loadLast(request.ResourceGroup)
	}
	defer runtime.mu.Unlock()

	if runtime.active != nil && isTerminalEpochState(runtime.active.state) {
		runtime.active = nil
		runtime.activeFlag.Store(false)
	}
	if runtime.active != nil {
		return manager.advanceActiveLocked(ctx, runtime)
	}
	if !request.AllowNew {
		return runtime.publish(EpochAdvanceResult{
			ResourceGroup: request.ResourceGroup,
			State:         EpochIdle,
			Rejected:      make(map[task.BalanceAdmissionReason]int),
		})
	}
	if request.Shadow {
		return manager.planShadowLocked(ctx, runtime, request)
	}
	return manager.startEpochLocked(ctx, runtime, request)
}

func (manager *BalanceEpochManager) HasActive(resourceGroup string) bool {
	manager.runtimesMu.Lock()
	runtime := manager.runtimes[resourceGroup]
	manager.runtimesMu.Unlock()
	return runtime != nil && runtime.activeFlag.Load()
}

func (manager *BalanceEpochManager) ActiveResourceGroups() []string {
	manager.runtimesMu.Lock()
	runtimes := make(map[string]*rgRuntime, len(manager.runtimes))
	for name, runtime := range manager.runtimes {
		runtimes[name] = runtime
	}
	manager.runtimesMu.Unlock()

	active := make([]string, 0, len(runtimes))
	for name, runtime := range runtimes {
		if runtime.activeFlag.Load() {
			active = append(active, name)
		}
	}
	sort.Strings(active)
	return active
}

func (manager *BalanceEpochManager) runtime(resourceGroup string) *rgRuntime {
	manager.runtimesMu.Lock()
	defer manager.runtimesMu.Unlock()
	if runtime := manager.runtimes[resourceGroup]; runtime != nil {
		return runtime
	}
	runtime := &rgRuntime{
		carryOver: make(map[BalanceObjectKey]*admittedWork),
		retries:   make(map[BalanceObjectKey]*objectRetryHistory),
	}
	runtime.last.Store(EpochAdvanceResult{
		ResourceGroup: resourceGroup,
		State:         EpochIdle,
		Rejected:      make(map[task.BalanceAdmissionReason]int),
	})
	manager.runtimes[resourceGroup] = runtime
	return runtime
}

func (manager *BalanceEpochManager) planShadowLocked(
	ctx context.Context,
	runtime *rgRuntime,
	request EpochRequest,
) EpochAdvanceResult {
	startedAt := manager.now()
	result := EpochAdvanceResult{
		ResourceGroup: request.ResourceGroup,
		State:         EpochPlanning,
		Rejected:      make(map[task.BalanceAdmissionReason]int),
	}
	if err := ctx.Err(); err != nil {
		result.State = EpochDegraded
		result.Terminal = true
		result.Err = err
		return runtime.publish(result)
	}
	if shadowDeadlineExpired(request, startedAt, manager.now()) {
		result.State = EpochTimedOut
		result.Terminal = true
		return runtime.publish(result)
	}
	policy, ok := manager.epochPolicy()
	if !ok {
		result.State = EpochDegraded
		result.Terminal = true
		result.Err = fmt.Errorf("epoch balance policy unavailable")
		return runtime.publish(result)
	}
	snapshot, err := manager.buildSnapshot(ctx, runtime, request)
	if err != nil {
		result.State = EpochDegraded
		result.Terminal = true
		result.Err = err
		return runtime.publish(result)
	}
	if err := ctx.Err(); err != nil {
		result.State = EpochDegraded
		result.Terminal = true
		result.Err = err
		return runtime.publish(result)
	}
	if shadowDeadlineExpired(request, startedAt, manager.now()) {
		result.State = EpochTimedOut
		result.Terminal = true
		return runtime.publish(result)
	}
	constraints := manager.planningConstraintsLocked(runtime, snapshot.Token, request, manager.now())
	wave := policy.Plan(snapshot, request.Budget, constraints, request.PolicyConfig)
	if err := ctx.Err(); err != nil {
		result.State = EpochDegraded
		result.Terminal = true
		result.Err = err
		return runtime.publish(result)
	}
	if shadowDeadlineExpired(request, startedAt, manager.now()) {
		result.State = EpochTimedOut
		result.Terminal = true
		return runtime.publish(result)
	}
	result.State = EpochCompleted
	result.Planned = len(wave.Plans)
	result.Terminal = true
	result.Converged = wave.Converged
	result.ObjectiveBefore = wave.Before.Value
	result.ObjectiveAfter = wave.After.Value
	return runtime.publish(result)
}

func (manager *BalanceEpochManager) startEpochLocked(
	ctx context.Context,
	runtime *rgRuntime,
	request EpochRequest,
) EpochAdvanceResult {
	now := manager.now()
	runtime.sequence++
	epoch := task.BalanceEpochMeta{
		ResourceGroup: request.ResourceGroup,
		LeaderTerm:    manager.leaderTerm,
		Sequence:      runtime.sequence,
	}
	active := &activeEpoch{
		epoch:          epoch,
		request:        cloneEpochRequest(request),
		state:          EpochPlanning,
		startedAt:      now,
		lastProgressAt: now,
		admitted:       make(map[int64]*admittedWork),
		result: EpochAdvanceResult{
			ResourceGroup: request.ResourceGroup,
			Epoch:         epoch,
			State:         EpochPlanning,
			Rejected:      make(map[task.BalanceAdmissionReason]int),
			Started:       true,
		},
	}
	runtime.active = active
	runtime.activeFlag.Store(true)
	runtime.publish(active.result)

	if err := ctx.Err(); err != nil {
		return manager.finishLocked(runtime, active, EpochDegraded, err)
	}
	if manager.deadlineExpired(active, manager.now()) {
		return manager.finishLocked(runtime, active, EpochTimedOut, nil)
	}
	policy, ok := manager.epochPolicy()
	if !ok {
		return manager.finishLocked(runtime, active, EpochDegraded, fmt.Errorf("epoch balance policy unavailable"))
	}
	snapshot, err := manager.buildSnapshot(ctx, runtime, request)
	if err != nil {
		return manager.finishLocked(runtime, active, EpochDegraded, err)
	}
	active.token = cloneSnapshotToken(snapshot.Token)
	if manager.deadlineExpired(active, manager.now()) {
		return manager.finishLocked(runtime, active, EpochTimedOut, nil)
	}
	constraints := manager.planningConstraintsLocked(runtime, snapshot.Token, request, manager.now())
	wave := policy.Plan(snapshot, request.Budget, constraints, request.PolicyConfig)
	active.wave = cloneBalanceWave(wave)
	active.ledger = NewWaveLedger(request.Budget, constraints)
	active.result.Planned = len(wave.Plans)
	active.result.ObjectiveBefore = wave.Before.Value
	active.result.ObjectiveAfter = wave.Before.Value
	active.result.Converged = wave.Converged
	if manager.deadlineExpired(active, manager.now()) {
		return manager.finishLocked(runtime, active, EpochTimedOut, nil)
	}
	if len(wave.Plans) == 0 {
		if len(runtime.carryOver) != 0 {
			active.state = EpochExecuting
			active.result.State = EpochExecuting
			active.result.Terminal = false
			active.result.Converged = false
			active.progressDigest = manager.progressDigest(runtime, active, manager.dist.Capture())
			return runtime.publish(active.result)
		}
		return manager.finishLocked(runtime, active, EpochCompleted, nil)
	}

	active.state = EpochAdmitting
	active.result.State = EpochAdmitting
	runtime.publish(active.result)
	manager.admitWaveLocked(ctx, runtime, active)
	if len(active.admitted) == 0 {
		state := active.terminalIntent
		if state == EpochIdle {
			state = EpochDegraded
		}
		if len(runtime.carryOver) != 0 {
			active.terminalIntent = state
			return manager.reconcileLocked(runtime, active, manager.dist.Capture())
		}
		return manager.finishLocked(runtime, active, state, active.result.Err)
	}
	active.state = EpochExecuting
	active.result.State = EpochExecuting
	active.result.Terminal = false
	active.progressDigest = manager.progressDigest(runtime, active, manager.dist.Capture())
	return runtime.publish(active.result)
}

func (manager *BalanceEpochManager) admitWaveLocked(
	ctx context.Context,
	runtime *rgRuntime,
	active *activeEpoch,
) {
	pendingRevision := active.token.PendingRevision(active.epoch)
	for index := range active.wave.Plans {
		if manager.deadlineExpired(active, manager.now()) {
			active.terminalIntent = EpochTimedOut
			break
		}
		plan := cloneEpochPolicyPlan(active.wave.Plans[index])
		plan.Token.Epoch = active.epoch
		plan.Token.Snapshot = plan.Token.Snapshot.WithPendingRevision(pendingRevision)
		active.token = plan.Token.Snapshot
		if !active.ledger.TryReserve(plan) {
			active.result.Rejected[task.BalanceAdmissionBudgetExhausted]++
			active.terminalIntent = EpochDegraded
			break
		}
		replica := manager.meta.ReplicaManager.Get(ctx, plan.ReplicaID)
		if replica == nil || replica.GetResourceGroup() != active.epoch.ResourceGroup || replica.GetCollectionID() != plan.CollectionID {
			active.ledger.Release(plan)
			active.result.Rejected[task.BalanceAdmissionReplicaChanged]++
			active.terminalIntent = EpochSuperseded
			break
		}
		timeout := active.request.SegmentTaskTimeout
		if plan.Kind == PlanKindChannel {
			timeout = active.request.ChannelTaskTimeout
		}
		balanceTask, err := manager.taskFactory.Build(ctx, plan, replica, active.epoch, timeout)
		if err != nil {
			active.ledger.Release(plan)
			active.result.Rejected[task.BalanceAdmissionInternalError]++
			active.result.Err = err
			active.terminalIntent = EpochSuperseded
			break
		}
		if manager.deadlineExpired(active, manager.now()) {
			active.ledger.Release(plan)
			balanceTask.Cancel(fmt.Errorf("balance epoch deadline exceeded before admission"))
			active.terminalIntent = EpochTimedOut
			break
		}
		if manager.admitter == nil {
			active.ledger.Release(plan)
			active.result.Rejected[task.BalanceAdmissionInternalError]++
			active.result.Err = fmt.Errorf("balance task generation admitter unavailable")
			active.terminalIntent = EpochSuperseded
			break
		}
		expected := plan.Token.Snapshot.PendingRevision(active.epoch)
		admission := manager.admitter.AdmitBalanceTaskAtPendingRevision(balanceTask, expected, func() task.BalanceAdmissionReason {
			if !manager.isCurrentLocked(runtime, active.epoch) {
				return task.BalanceAdmissionStaleEpoch
			}
			return manager.snapshotBuilder.Validate(plan.Token)
		})
		admissionDeadlineExpired := manager.deadlineExpired(active, manager.now())
		if admission.Reason != task.BalanceAdmissionAccepted {
			active.ledger.Release(plan)
			active.result.Rejected[admission.Reason]++
			active.result.Err = admission.Err
			if supersedesGeneration(admission.Reason) {
				active.terminalIntent = EpochSuperseded
			} else {
				active.terminalIntent = EpochDegraded
			}
			if admissionDeadlineExpired && active.terminalIntent != EpochSuperseded {
				active.terminalIntent = EpochTimedOut
			}
			break
		}
		pendingRevision = admission.PendingRevision
		active.token = active.token.WithPendingRevision(pendingRevision)
		active.admitted[admission.TaskID] = &admittedWork{plan: cloneEpochPolicyPlan(plan), task: balanceTask}
		active.result.Admitted++
		active.lastProgressAt = manager.now()
		if active.result.Admitted <= len(active.wave.PrefixAfter) {
			active.result.ObjectiveAfter = active.wave.PrefixAfter[active.result.Admitted-1].Value
		}
		if admissionDeadlineExpired {
			active.terminalIntent = EpochTimedOut
			break
		}
	}
}

func (manager *BalanceEpochManager) advanceActiveLocked(
	_ context.Context,
	runtime *rgRuntime,
) EpochAdvanceResult {
	active := runtime.active
	if active.state != EpochExecuting {
		return runtime.publish(active.result)
	}
	distribution := manager.dist.Capture()
	digest := manager.progressDigest(runtime, active, distribution)
	if digest != active.progressDigest {
		active.progressDigest = digest
		active.lastProgressAt = manager.now()
	}
	if active.terminalIntent == EpochIdle {
		if reason := manager.currentInvalidation(runtime, active); reason != task.BalanceAdmissionAccepted {
			active.terminalIntent = EpochSuperseded
			active.result.Rejected[reason]++
		}
	}
	if active.terminalIntent == EpochIdle && manager.deadlineExpired(active, manager.now()) {
		active.terminalIntent = EpochTimedOut
	}
	if active.terminalIntent == EpochIdle && active.request.NoProgressDeadline > 0 &&
		!manager.now().Before(active.lastProgressAt.Add(active.request.NoProgressDeadline)) {
		active.terminalIntent = EpochTimedOut
	}
	if active.terminalIntent == EpochIdle && !manager.allWorkQuiescent(runtime, active, distribution) {
		return runtime.publish(active.result)
	}
	return manager.reconcileLocked(runtime, active, distribution)
}

func (manager *BalanceEpochManager) reconcileLocked(
	runtime *rgRuntime,
	active *activeEpoch,
	distribution meta.DistributionSnapshot,
) EpochAdvanceResult {
	active.state = EpochReconciling
	active.result.State = EpochReconciling
	runtime.publish(active.result)

	all := make(map[BalanceObjectKey]*admittedWork, len(runtime.carryOver)+len(active.admitted))
	for key, work := range runtime.carryOver {
		all[key] = work
	}
	for _, work := range active.admitted {
		all[work.plan.ObjectKey()] = work
	}
	keys := sortedWorkKeys(all)
	nextCarry := make(map[BalanceObjectKey]*admittedWork)
	hadFailure := false
	hadLostPlacement := false
	for _, key := range keys {
		work := all[key]
		observation := observeWork(work, distribution)
		switch classifyTerminalWork(observation) {
		case terminalWorkCompleted:
			delete(runtime.retries, key)
		case terminalWorkAmbiguous:
			nextCarry[key] = work
		case terminalWorkGrowFailed, terminalWorkReduceFailed:
			hadFailure = true
			manager.recordFailureLocked(runtime, work.plan, active.request, manager.now())
			if active.result.Err == nil && work.task != nil {
				active.result.Err = work.task.Err()
			}
		case terminalWorkLost:
			hadFailure = true
			hadLostPlacement = true
			manager.recordFailureLocked(runtime, work.plan, active.request, manager.now())
			if active.result.Err == nil {
				active.result.Err = fmt.Errorf("balance object %v has no authoritative source or target placement", key)
			}
		}
	}
	runtime.carryOver = nextCarry

	state := active.terminalIntent
	if hadLostPlacement {
		state = EpochDegraded
	} else if state == EpochIdle {
		if hadFailure {
			state = EpochDegraded
		} else if len(nextCarry) != 0 {
			state = EpochDegraded
		} else {
			state = EpochCompleted
		}
	}
	if state == EpochTimedOut && len(nextCarry) == 0 && !hadFailure {
		state = EpochCompleted
	}
	if len(nextCarry) != 0 && state != EpochTimedOut && state != EpochSuperseded {
		state = EpochDegraded
		active.result.Err = errors.Join(
			active.result.Err,
			fmt.Errorf("%d balance object(s) remain ambiguous after reconciliation", len(nextCarry)),
		)
	}
	return manager.finishLocked(runtime, active, state, active.result.Err)
}

func (manager *BalanceEpochManager) finishLocked(
	runtime *rgRuntime,
	active *activeEpoch,
	state EpochState,
	err error,
) EpochAdvanceResult {
	active.state = state
	active.result.State = state
	active.result.Terminal = true
	if err != nil {
		active.result.Err = err
	}
	return runtime.publish(active.result)
}

func (manager *BalanceEpochManager) buildSnapshot(
	ctx context.Context,
	runtime *rgRuntime,
	request EpochRequest,
) (*PlacementSnapshot, error) {
	if manager.snapshotBuilder == nil {
		return nil, fmt.Errorf("placement snapshot source unavailable")
	}
	return manager.snapshotBuilder.Build(
		ctx,
		request.ResourceGroup,
		append([]int64(nil), request.EligibleReplicaIDs...),
		pendingCarryOver(runtime.carryOver),
	)
}

func (manager *BalanceEpochManager) planningConstraintsLocked(
	runtime *rgRuntime,
	token SnapshotToken,
	request EpochRequest,
	now time.Time,
) EpochPlanningConstraints {
	manager.clearRetryHistoryIfInvalidLocked(runtime, token, now)
	constraints := EpochPlanningConstraints{Objects: make(map[BalanceObjectKey]EpochObjectConstraint)}
	for key, work := range runtime.carryOver {
		constraints.Objects[key] = EpochObjectConstraint{
			Object:       key,
			CollectionID: work.plan.CollectionID,
			From:         work.plan.From,
			To:           work.plan.To,
			Class:        ReservationAmbiguousCapacity,
			ChargedNodes: positiveDistinctNodes(work.plan.From, work.plan.To),
		}
	}
	if request.MaxObjectRetries > 0 && request.QuarantineBackoff > 0 {
		for key, history := range runtime.retries {
			if history.quarantineUntil.IsZero() || !now.Before(history.quarantineUntil) {
				continue
			}
			constraints.Objects[key] = EpochObjectConstraint{
				Object:          key,
				CollectionID:    history.plan.CollectionID,
				From:            history.plan.From,
				To:              history.plan.To,
				Class:           ReservationQuarantineOnly,
				QuarantineUntil: history.quarantineUntil,
			}
		}
	}
	return constraints
}

func (manager *BalanceEpochManager) clearRetryHistoryIfInvalidLocked(
	runtime *rgRuntime,
	token SnapshotToken,
	now time.Time,
) {
	for _, history := range runtime.retries {
		if (!history.quarantineUntil.IsZero() && !now.Before(history.quarantineUntil)) ||
			!sameRetryTopology(history.token, token) {
			runtime.retries = make(map[BalanceObjectKey]*objectRetryHistory)
			return
		}
	}
}

func (manager *BalanceEpochManager) recordFailureLocked(
	runtime *rgRuntime,
	plan EpochPlan,
	request EpochRequest,
	now time.Time,
) {
	if request.MaxObjectRetries <= 0 {
		return
	}
	key := plan.ObjectKey()
	history := runtime.retries[key]
	if history == nil || !sameRetryTopology(history.token, plan.Token.Snapshot) {
		history = &objectRetryHistory{}
		runtime.retries[key] = history
	}
	history.count++
	history.plan = cloneEpochPolicyPlan(plan)
	history.token = cloneSnapshotToken(plan.Token.Snapshot)
	if history.count >= request.MaxObjectRetries && request.QuarantineBackoff > 0 {
		history.quarantineUntil = now.Add(request.QuarantineBackoff)
	}
}

func (manager *BalanceEpochManager) allWorkQuiescent(
	runtime *rgRuntime,
	active *activeEpoch,
	distribution meta.DistributionSnapshot,
) bool {
	for _, work := range runtime.carryOver {
		if !observeWork(work, distribution).quiescent() {
			return false
		}
	}
	for _, work := range active.admitted {
		if !observeWork(work, distribution).quiescent() {
			return false
		}
	}
	return true
}

func (manager *BalanceEpochManager) progressDigest(
	runtime *rgRuntime,
	active *activeEpoch,
	distribution meta.DistributionSnapshot,
) uint64 {
	all := make(map[BalanceObjectKey]*admittedWork, len(runtime.carryOver)+len(active.admitted))
	for key, work := range runtime.carryOver {
		all[key] = work
	}
	for _, work := range active.admitted {
		all[work.plan.ObjectKey()] = work
	}
	hash := fnv.New64a()
	for _, key := range sortedWorkKeys(all) {
		observation := observeWork(all[key], distribution)
		_, _ = fmt.Fprintf(
			hash,
			"%d/%d/%d/%s/%d/%t/%t/%t/%s/%t/%d;",
			key.Kind, key.ReplicaID, key.SegmentID, key.Channel, key.Scope,
			observation.sourcePresent, observation.targetPresent, observation.targetReady,
			observation.status, observation.done, observation.detailDigest,
		)
	}
	return hash.Sum64()
}

func (manager *BalanceEpochManager) currentInvalidation(
	runtime *rgRuntime,
	active *activeEpoch,
) task.BalanceAdmissionReason {
	if manager.snapshotBuilder == nil {
		return task.BalanceAdmissionInternalError
	}
	seen := make(map[[2]int64]struct{})
	all := make(map[BalanceObjectKey]*admittedWork, len(runtime.carryOver)+len(active.admitted))
	for key, work := range runtime.carryOver {
		all[key] = work
	}
	for _, work := range active.admitted {
		all[work.plan.ObjectKey()] = work
	}
	for _, key := range sortedWorkKeys(all) {
		plan := all[key].plan
		identity := [2]int64{plan.CollectionID, plan.ReplicaID}
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		reason := manager.snapshotBuilder.Validate(AdmissionToken{
			Snapshot:     active.token,
			Epoch:        active.epoch,
			CollectionID: plan.CollectionID,
			ReplicaID:    plan.ReplicaID,
		})
		switch reason {
		case task.BalanceAdmissionAccepted,
			task.BalanceAdmissionSourceGone,
			task.BalanceAdmissionStaleEpoch:
			continue
		default:
			return reason
		}
	}
	return task.BalanceAdmissionAccepted
}

func (manager *BalanceEpochManager) isCurrentLocked(runtime *rgRuntime, epoch task.BalanceEpochMeta) bool {
	return runtime.active != nil && runtime.active.epoch == epoch && !isTerminalEpochState(runtime.active.state)
}

func (manager *BalanceEpochManager) deadlineExpired(active *activeEpoch, now time.Time) bool {
	return active.request.Deadline > 0 && !now.Before(active.startedAt.Add(active.request.Deadline))
}

func (manager *BalanceEpochManager) epochPolicy() (EpochBalancePolicy, bool) {
	if manager.policyProvider == nil {
		return nil, false
	}
	policy, ok := manager.policyProvider()
	return policy, ok && policy != nil
}

func (runtime *rgRuntime) publish(result EpochAdvanceResult) EpochAdvanceResult {
	cloned := cloneEpochResult(result)
	if runtime.active != nil {
		runtime.active.result = cloneEpochResult(cloned)
	}
	runtime.last.Store(cloned)
	return cloneEpochResult(cloned)
}

func (runtime *rgRuntime) loadLast(resourceGroup string) EpochAdvanceResult {
	value := runtime.last.Load()
	if value == nil {
		return EpochAdvanceResult{
			ResourceGroup: resourceGroup,
			State:         EpochIdle,
			Rejected:      make(map[task.BalanceAdmissionReason]int),
		}
	}
	return cloneEpochResult(value.(EpochAdvanceResult))
}

func cloneEpochResult(result EpochAdvanceResult) EpochAdvanceResult {
	clone := result
	clone.Rejected = make(map[task.BalanceAdmissionReason]int, len(result.Rejected))
	for reason, count := range result.Rejected {
		clone.Rejected[reason] = count
	}
	return clone
}

func cloneEpochRequest(request EpochRequest) EpochRequest {
	request.EligibleReplicaIDs = append([]int64(nil), request.EligibleReplicaIDs...)
	return request
}

func cloneBalanceWave(wave BalanceWave) BalanceWave {
	clone := wave
	clone.Plans = make([]EpochPlan, len(wave.Plans))
	for i, plan := range wave.Plans {
		clone.Plans[i] = cloneEpochPolicyPlan(plan)
	}
	clone.PrefixAfter = append([]ScorePotential(nil), wave.PrefixAfter...)
	clone.Decisions = append([]BalancePlanDecision(nil), wave.Decisions...)
	return clone
}

func pendingCarryOver(carry map[BalanceObjectKey]*admittedWork) []task.PendingBalanceTaskSnapshot {
	keys := sortedWorkKeys(carry)
	result := make([]task.PendingBalanceTaskSnapshot, 0, len(keys))
	for _, key := range keys {
		work := carry[key]
		pending := task.PendingBalanceTaskSnapshot{
			CollectionID:  work.plan.CollectionID,
			ReplicaID:     work.plan.ReplicaID,
			ResourceGroup: work.plan.Token.Snapshot.ResourceGroup,
			Epoch:         work.plan.Token.Epoch,
			Status:        task.TaskStatusStarted,
		}
		if work.task != nil {
			pending.TaskID = work.task.ID()
			pending.Status = work.task.Status()
		}
		switch work.plan.Kind {
		case PlanKindSegment:
			pending.Actions = []task.PendingBalanceActionSnapshot{
				{
					NodeID: work.plan.To, Type: task.ActionTypeGrow,
					SegmentID: work.plan.SegmentID, Shard: work.plan.Shard,
					Scope: work.plan.Scope, Workload: int(work.plan.RowCount),
				},
				{
					NodeID: work.plan.From, Type: task.ActionTypeReduce,
					SegmentID: work.plan.SegmentID, Shard: work.plan.Shard,
					Scope: work.plan.Scope, Workload: -int(work.plan.RowCount),
				},
			}
		case PlanKindChannel:
			pending.Actions = []task.PendingBalanceActionSnapshot{
				{
					NodeID: work.plan.To, Type: task.ActionTypeGrow,
					Channel: work.plan.Channel, Shard: work.plan.Channel, Workload: 1,
				},
				{
					NodeID: work.plan.From, Type: task.ActionTypeReduce,
					Channel: work.plan.Channel, Shard: work.plan.Channel, Workload: -1,
				},
			}
		}
		result = append(result, pending)
	}
	return result
}

type terminalWorkClass int

const (
	terminalWorkCompleted terminalWorkClass = iota + 1
	terminalWorkAmbiguous
	terminalWorkGrowFailed
	terminalWorkReduceFailed
	terminalWorkLost
)

type workObservation struct {
	sourcePresent bool
	targetPresent bool
	targetReady   bool
	status        task.Status
	done          bool
	detailDigest  uint64
}

func (observation workObservation) desired() bool {
	return observation.targetPresent && observation.targetReady && !observation.sourcePresent
}

func (observation workObservation) quiescent() bool {
	return observation.desired() || observation.status != task.TaskStatusStarted || observation.done
}

func observeWork(work *admittedWork, distribution meta.DistributionSnapshot) workObservation {
	observation := workObservation{status: task.TaskStatusStarted}
	detail := fnv.New64a()
	if work.task != nil {
		observation.status = work.task.Status()
		select {
		case <-work.task.Done():
			observation.done = true
		default:
		}
	}
	plan := work.plan
	switch plan.Kind {
	case PlanKindSegment:
		source := segmentRecordsForPlanNode(distribution, plan, plan.From)
		target := segmentRecordsForPlanNode(distribution, plan, plan.To)
		observation.sourcePresent = len(source) != 0
		observation.targetPresent = len(target) != 0
		observation.targetReady = observation.targetPresent
		hashSegmentRecords(detail, "source", source)
		hashSegmentRecords(detail, "target", target)
	case PlanKindChannel:
		minimumTargetVersion := plan.Token.Snapshot.CurrentTargetVersion[plan.CollectionID]
		source := channelRecordsForPlanNode(distribution, plan, plan.From)
		target := channelRecordsForPlanNode(distribution, plan, plan.To)
		observation.sourcePresent = len(source) != 0
		observation.targetPresent = len(target) != 0
		for _, channel := range target {
			if channel.Serviceable && channel.LeaderID != 0 &&
				channel.LeaderID == plan.To &&
				(minimumTargetVersion == 0 || channel.LeaderTargetVersion >= minimumTargetVersion) {
				observation.targetReady = true
				break
			}
		}
		hashChannelRecords(detail, "source", source)
		hashChannelRecords(detail, "target", target)
	}
	observation.detailDigest = detail.Sum64()
	return observation
}

func segmentRecordsForPlanNode(
	distribution meta.DistributionSnapshot,
	plan EpochPlan,
	nodeID int64,
) []meta.SegmentSnapshotRecord {
	records := make([]meta.SegmentSnapshotRecord, 0, 1)
	for _, segment := range distribution.Segments {
		if segment.Present && segment.NodeID == nodeID &&
			segment.CollectionID == plan.CollectionID &&
			segment.SegmentID == plan.SegmentID && segment.Scope == plan.Scope {
			records = append(records, segment)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if left.Version != right.Version {
			return left.Version < right.Version
		}
		if left.PartitionID != right.PartitionID {
			return left.PartitionID < right.PartitionID
		}
		if left.Channel != right.Channel {
			return left.Channel < right.Channel
		}
		return left.RowCount < right.RowCount
	})
	return records
}

func hashSegmentRecords(detail hash.Hash64, role string, records []meta.SegmentSnapshotRecord) {
	_, _ = fmt.Fprintf(detail, "%s-segments/%d;", role, len(records))
	for _, segment := range records {
		_, _ = fmt.Fprintf(
			detail,
			"%d/%d/%d/%d/%s/%d/%d/%d/%t;",
			segment.NodeID, segment.CollectionID, segment.SegmentID, segment.PartitionID,
			segment.Channel, segment.RowCount, segment.Version, segment.Scope, segment.Present,
		)
	}
}

func channelRecordsForPlanNode(
	distribution meta.DistributionSnapshot,
	plan EpochPlan,
	nodeID int64,
) []meta.ChannelSnapshotRecord {
	records := make([]meta.ChannelSnapshotRecord, 0, 1)
	for _, channel := range distribution.Channels {
		if channel.Present && channel.NodeID == nodeID &&
			channel.CollectionID == plan.CollectionID && channel.Channel == plan.Channel {
			records = append(records, channel)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if left.Version != right.Version {
			return left.Version < right.Version
		}
		if left.LeaderID != right.LeaderID {
			return left.LeaderID < right.LeaderID
		}
		if left.LeaderVersion != right.LeaderVersion {
			return left.LeaderVersion < right.LeaderVersion
		}
		if left.LeaderTargetVersion != right.LeaderTargetVersion {
			return left.LeaderTargetVersion < right.LeaderTargetVersion
		}
		if left.Serviceable != right.Serviceable {
			return !left.Serviceable && right.Serviceable
		}
		return left.NumOfGrowingRows < right.NumOfGrowingRows
	})
	return records
}

func hashChannelRecords(detail hash.Hash64, role string, records []meta.ChannelSnapshotRecord) {
	_, _ = fmt.Fprintf(detail, "%s-channels/%d;", role, len(records))
	for _, channel := range records {
		_, _ = fmt.Fprintf(
			detail,
			"%d/%d/%s/%d/%t/%t/%d/%d/%d/%d;",
			channel.NodeID, channel.CollectionID, channel.Channel, channel.Version,
			channel.Present, channel.Serviceable, channel.LeaderID,
			channel.LeaderVersion, channel.LeaderTargetVersion, channel.NumOfGrowingRows,
		)
	}
}

func classifyTerminalWork(observation workObservation) terminalWorkClass {
	if observation.desired() {
		return terminalWorkCompleted
	}
	switch observation.status {
	case task.TaskStatusStarted, task.TaskStatusCanceled:
		return terminalWorkAmbiguous
	case task.TaskStatusFailed:
		if observation.sourcePresent && !observation.targetPresent {
			return terminalWorkGrowFailed
		}
		if observation.sourcePresent && observation.targetPresent {
			return terminalWorkReduceFailed
		}
		if !observation.sourcePresent && !observation.targetPresent {
			return terminalWorkLost
		}
		return terminalWorkAmbiguous
	case task.TaskStatusSucceeded:
		return terminalWorkAmbiguous
	default:
		return terminalWorkAmbiguous
	}
}

func sortedWorkKeys[V any](values map[BalanceObjectKey]V) []BalanceObjectKey {
	keys := make([]BalanceObjectKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left, right := keys[i], keys[j]
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.ReplicaID != right.ReplicaID {
			return left.ReplicaID < right.ReplicaID
		}
		if left.SegmentID != right.SegmentID {
			return left.SegmentID < right.SegmentID
		}
		if left.Channel != right.Channel {
			return left.Channel < right.Channel
		}
		return left.Scope < right.Scope
	})
	return keys
}

func positiveDistinctNodes(nodes ...int64) []int64 {
	unique := make(map[int64]struct{}, len(nodes))
	for _, node := range nodes {
		if node > 0 {
			unique[node] = struct{}{}
		}
	}
	result := make([]int64, 0, len(unique))
	for node := range unique {
		result = append(result, node)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func sameRetryTopology(left, right SnapshotToken) bool {
	return left.ResourceGroup == right.ResourceGroup &&
		left.RGHash == right.RGHash &&
		left.ReplicaHash == right.ReplicaHash &&
		left.NodeHash == right.NodeHash &&
		left.LeaderHash == right.LeaderHash &&
		equalInt64Map(left.CurrentTargetVersion, right.CurrentTargetVersion) &&
		equalInt64Map(left.NextTargetVersion, right.NextTargetVersion)
}

func supersedesGeneration(reason task.BalanceAdmissionReason) bool {
	switch reason {
	case task.BalanceAdmissionLeaderMissing,
		task.BalanceAdmissionReplicaChanged,
		task.BalanceAdmissionRGChanged,
		task.BalanceAdmissionTargetChanged,
		task.BalanceAdmissionNodeIneligible,
		task.BalanceAdmissionStaleEpoch,
		task.BalanceAdmissionInternalError:
		return true
	default:
		return false
	}
}

func isTerminalEpochState(state EpochState) bool {
	switch state {
	case EpochCompleted, EpochDegraded, EpochSuperseded, EpochTimedOut:
		return true
	default:
		return false
	}
}

func shadowDeadlineExpired(request EpochRequest, startedAt, now time.Time) bool {
	return request.Deadline > 0 && !now.Before(startedAt.Add(request.Deadline))
}

func newEpochLeaderTerm() uint64 {
	var buffer [8]byte
	if _, err := rand.Read(buffer[:]); err == nil {
		if term := binary.LittleEndian.Uint64(buffer[:]); term != 0 {
			return term
		}
	}
	term := uint64(time.Now().UnixNano())
	if term == 0 {
		return 1
	}
	return term
}
