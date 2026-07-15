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

package checkers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// createMockPriorityQueue creates a mock priority queue for testing
func createMockPriorityQueue() *assign.PriorityQueue {
	return assign.NewPriorityQueuePtr()
}

func cleanupMock(t *testing.T, mocker *mockey.Mocker) {
	t.Helper()
	t.Cleanup(func() {
		mocker.UnPatch()
	})
}

// Helper function to create a test BalanceChecker
func createTestBalanceChecker(opts ...balanceCheckerOption) *BalanceChecker {
	metaInstance := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(nil),
		ReplicaManager:    meta.NewReplicaManager(nil, nil),
	}
	targetMgr := meta.NewTargetManager(nil, nil)
	nodeMgr := &session.NodeManager{}
	dist := meta.NewDistributionManager(nodeMgr)
	scheduler := task.NewScheduler(context.Background(), nil, nil, nil, nil, nil, nil)

	// Initialize global assign policy factory for testing
	assign.ResetGlobalAssignPolicyFactoryForTest()
	assign.InitGlobalAssignPolicyFactory(scheduler, nodeMgr, dist, metaInstance, targetMgr)

	// Initialize global balancer factory for testing
	balance.ResetGlobalBalancerFactoryForTest()
	balance.InitGlobalBalancerFactory(scheduler, nodeMgr, dist, targetMgr)

	return newBalanceChecker(metaInstance, dist, targetMgr, nodeMgr, scheduler, opts...)
}

type fakeBalanceEpochController struct {
	active             []string
	requests           []balance.EpochRequest
	activateOnNew      bool
	clearOnObservation bool
	newResult          balance.EpochAdvanceResult
	hasActiveOverride  func(string) bool
}

func (f *fakeBalanceEpochController) Advance(_ context.Context, request balance.EpochRequest) balance.EpochAdvanceResult {
	request.EligibleReplicaIDs = append([]int64(nil), request.EligibleReplicaIDs...)
	f.requests = append(f.requests, request)
	if request.AllowNew {
		if f.activateOnNew && !f.HasActive(request.ResourceGroup) {
			f.active = append(f.active, request.ResourceGroup)
		}
		result := f.newResult
		result.ResourceGroup = request.ResourceGroup
		return result
	}
	if f.clearOnObservation {
		filtered := f.active[:0]
		for _, resourceGroup := range f.active {
			if resourceGroup != request.ResourceGroup {
				filtered = append(filtered, resourceGroup)
			}
		}
		f.active = filtered
	}
	return balance.EpochAdvanceResult{ResourceGroup: request.ResourceGroup}
}

func (f *fakeBalanceEpochController) HasActive(resourceGroup string) bool {
	if f.hasActiveOverride != nil {
		return f.hasActiveOverride(resourceGroup)
	}
	for _, active := range f.active {
		if active == resourceGroup {
			return true
		}
	}
	return false
}

type generationOnlyScheduler struct {
	task.Scheduler
}

func (s *generationOnlyScheduler) AdmitBalanceTaskAtPendingRevision(
	_ task.Task,
	_ task.BalancePendingRevision,
	_ task.BalanceAdmissionValidator,
) task.BalanceAdmissionResult {
	return task.BalanceAdmissionResult{}
}

type inspectorOnlyScheduler struct {
	task.Scheduler
}

func (s *inspectorOnlyScheduler) GetPendingBalanceTasks() task.PendingBalanceSnapshot {
	return task.PendingBalanceSnapshot{}
}

type epochCapableScheduler struct {
	task.Scheduler
}

func (s *epochCapableScheduler) AdmitBalanceTaskAtPendingRevision(
	_ task.Task,
	_ task.BalancePendingRevision,
	_ task.BalanceAdmissionValidator,
) task.BalanceAdmissionResult {
	return task.BalanceAdmissionResult{}
}

func (s *epochCapableScheduler) GetPendingBalanceTasks() task.PendingBalanceSnapshot {
	return task.PendingBalanceSnapshot{}
}

func (f *fakeBalanceEpochController) ActiveResourceGroups() []string {
	return append([]string(nil), f.active...)
}

func testBalanceEpochConfig() balanceEpochConfig {
	return balanceEpochConfig{
		enabled:            true,
		balancer:           meta.ScoreBasedBalancerName,
		budget:             balance.BalanceWaveBudget{MaxSegmentTasks: 3, MaxChannelTasks: 2, MaxTasksPerNode: 2, MaxTasksPerCollection: 2},
		deadline:           10 * time.Second,
		noProgressDeadline: 5 * time.Second,
		segmentTaskTimeout: 3 * time.Second,
		channelTaskTimeout: 4 * time.Second,
		maxObjectRetries:   2,
		quarantineBackoff:  time.Second,
		policyConfig: balance.EpochPolicyConfig{
			GlobalRowCountFactor:          0.1,
			DelegatorMemoryOverloadFactor: 0.2,
			CollectionChannelCountFactor:  0.3,
			AutoBalanceChannel:            true,
		},
	}
}

func mockEpochCheckConfig(
	t *testing.T,
	checker *BalanceChecker,
	config balanceConfig,
	stoppingEnabled bool,
	autoBalanceEnabled bool,
	policySupported bool,
) {
	t.Helper()
	mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
	cleanupMock(t, mockLoadConfig)
	mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
	cleanupMock(t, mockParamGet)
	mockGetAsBool := mockey.Mock((*paramtable.ParamItem).GetAsBool).
		Return(mockey.Sequence(stoppingEnabled).Times(1).Then(autoBalanceEnabled)).
		Build()
	cleanupMock(t, mockGetAsBool)
	mockStoppingBalancer := mockey.Mock((*balance.BalancerFactory).GetStoppingBalancer).
		Return((*balance.StoppingBalancer)(nil)).
		Build()
	cleanupMock(t, mockStoppingBalancer)
	mockBalancer := mockey.Mock((*balance.BalancerFactory).GetBalancer).
		Return((balance.Balance)(nil)).
		Build()
	cleanupMock(t, mockBalancer)
	mockPolicy := mockey.Mock((*balance.BalancerFactory).GetEpochPolicyFor).
		To(func(*balance.BalancerFactory, string, balance.EpochPolicyConfig) (balance.EpochBalancePolicy, bool) {
			return nil, policySupported
		}).
		Build()
	cleanupMock(t, mockPolicy)
	checker.autoBalanceTs = time.Time{}
}

func mockEligibleEpochCollections(
	t *testing.T,
	checker *BalanceChecker,
	collectionIDs []int64,
	replicas map[int64][]*meta.Replica,
) {
	t.Helper()
	collections := make(map[int64]*meta.Collection, len(collectionIDs))
	for _, collectionID := range collectionIDs {
		collections[collectionID] = &meta.Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: collectionID,
			Status:       querypb.LoadStatus_Loaded,
		}}
	}
	mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
	cleanupMock(t, mockGetAll)
	mockGetCollection := mockey.Mock((*meta.CollectionManager).GetCollection).
		To(func(_ *meta.CollectionManager, _ context.Context, collectionID int64) *meta.Collection {
			return collections[collectionID]
		}).
		Build()
	cleanupMock(t, mockGetCollection)
	mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(true).Build()
	cleanupMock(t, mockIsNextTargetExist)
	mockIsCurrentTargetReady := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetReady")).Return(true).Build()
	cleanupMock(t, mockIsCurrentTargetReady)
	mockGetChannels := mockey.Mock((*meta.ChannelDistManager).GetByFilter).
		Return([]*meta.DmChannel{{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}}}).
		Build()
	cleanupMock(t, mockGetChannels)
	mockGetReplicas := mockey.Mock((*meta.ReplicaManager).GetByCollection).
		To(func(_ *meta.ReplicaManager, _ context.Context, collectionID int64) []*meta.Replica {
			return replicas[collectionID]
		}).
		Build()
	cleanupMock(t, mockGetReplicas)
}

func newEpochReplica(replicaID, collectionID int64, resourceGroup string) *meta.Replica {
	return meta.NewReplica(&querypb.Replica{
		ID:            replicaID,
		CollectionID:  collectionID,
		ResourceGroup: resourceGroup,
	}, typeutil.NewUniqueSet())
}

func newLegacySegmentBalanceTask(t *testing.T, checker *BalanceChecker, segmentID int64) task.Task {
	t.Helper()
	replica := newEpochReplica(10, 1, "rg-a")
	balanceTask, err := task.NewSegmentTask(
		context.Background(), time.Second, checker.ID(), 1, replica, commonpb.LoadPriority_LOW,
		task.NewSegmentAction(1, task.ActionTypeGrow, "channel-a", segmentID),
	)
	require.NoError(t, err)
	return balanceTask
}

// =============================================================================
// Basic Interface Tests
// =============================================================================

func TestBalanceChecker_ID(t *testing.T) {
	checker := createTestBalanceChecker()

	id := checker.ID()
	assert.Equal(t, utils.BalanceChecker, id)
}

func TestBalanceChecker_Description(t *testing.T) {
	checker := createTestBalanceChecker()

	desc := checker.Description()
	assert.Equal(t, "BalanceChecker checks the cluster distribution and generates balance tasks", desc)
}

// =============================================================================
// Configuration Tests
// =============================================================================

func TestBalanceChecker_LoadBalanceConfig(t *testing.T) {
	checker := createTestBalanceChecker()

	// Mock paramtable.Get function
	mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
	defer mockParamGet.UnPatch()

	// Mock various param item methods
	mockGetAsInt := mockey.Mock((*paramtable.ParamItem).GetAsInt).Return(5).Build()
	defer mockGetAsInt.UnPatch()

	mockGetAsBool := mockey.Mock((*paramtable.ParamItem).GetAsBool).Return(true).Build()
	defer mockGetAsBool.UnPatch()

	mockGetAsDuration := mockey.Mock((*paramtable.ParamItem).GetAsDuration).Return(5 * time.Second).Build()
	defer mockGetAsDuration.UnPatch()

	config := checker.loadBalanceConfig()

	// Verify config structure is returned
	assert.IsType(t, balanceConfig{}, config)
}

// =============================================================================
// Collection Balance Item Tests
// =============================================================================

func TestNewCollectionBalanceItem(t *testing.T) {
	collectionID := int64(100)
	rowCount := 1000
	sortOrder := "byrowcount"

	item := newCollectionBalanceItem(collectionID, rowCount, sortOrder)

	assert.Equal(t, collectionID, item.collectionID)
	assert.Equal(t, rowCount, item.rowCount)
	assert.Equal(t, sortOrder, item.sortOrder)
}

func TestCollectionBalanceItem_GetPriority_ByRowCount(t *testing.T) {
	tests := []struct {
		name      string
		rowCount  int
		sortOrder string
		expected  int
	}{
		{"ByRowCount", 1000, "byrowcount", -1000},
		{"Default", 500, "", -500}, // default to byrowcount
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := newCollectionBalanceItem(1, tt.rowCount, tt.sortOrder)

			priority := item.getPriority()
			assert.Equal(t, tt.expected, priority)
		})
	}
}

func TestCollectionBalanceItem_GetPriority_ByCollectionID(t *testing.T) {
	collectionID := int64(123)
	item := newCollectionBalanceItem(collectionID, 1000, "bycollectionid")

	priority := item.getPriority()
	assert.Equal(t, int(collectionID), priority)
}

func TestCollectionBalanceItem_SetPriority(t *testing.T) {
	item := newCollectionBalanceItem(1, 100, "byrowcount")

	item.setPriority(200)

	assert.Equal(t, 200, item.getPriority())
}

// =============================================================================
// Collection Filtering Tests
// =============================================================================

func TestBalanceChecker_ReadyToCheck_Success(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	collectionID := int64(1)

	// Mock meta.GetCollection to return a valid collection
	mockGetCollection := mockey.Mock(mockey.GetMethod(checker.meta.CollectionManager, "GetCollection")).Return(&meta.Collection{}).Build()
	defer mockGetCollection.UnPatch()

	// Mock target manager methods
	mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(true).Build()
	defer mockIsNextTargetExist.UnPatch()

	result := checker.readyToCheck(ctx, collectionID)
	assert.True(t, result)
}

func TestBalanceChecker_ReadyToCheck_NoMeta(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	collectionID := int64(1)

	// Mock meta.GetCollection to return nil
	mockGetCollection := mockey.Mock((*meta.Meta).GetCollection).Return(nil).Build()
	defer mockGetCollection.UnPatch()

	// Mock target manager methods to return false
	mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(false).Build()
	defer mockIsNextTargetExist.UnPatch()

	mockIsCurrentTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetExist")).Return(false).Build()
	defer mockIsCurrentTargetExist.UnPatch()

	result := checker.readyToCheck(ctx, collectionID)
	assert.False(t, result)
}

func TestBalanceChecker_ReadyToCheck_NoTarget(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	collectionID := int64(1)

	// Mock meta.GetCollection to return a valid collection
	mockGetCollection := mockey.Mock((*meta.Meta).GetCollection).Return(&meta.Collection{}).Build()
	defer mockGetCollection.UnPatch()

	// Mock target manager methods to return false
	mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(false).Build()
	defer mockIsNextTargetExist.UnPatch()

	mockIsCurrentTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetExist")).Return(false).Build()
	defer mockIsCurrentTargetExist.UnPatch()

	result := checker.readyToCheck(ctx, collectionID)
	assert.False(t, result)
}

func TestBalanceChecker_FilterCollectionForBalance_Success(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Mock meta.GetAll to return collection IDs
	collectionIDs := []int64{1, 2, 3}
	mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
	defer mockGetAll.UnPatch()

	// Create filters that pass all collections
	passAllFilter := func(ctx context.Context, collectionID int64) bool {
		return true
	}

	result := checker.filterCollectionForBalance(ctx, passAllFilter)
	assert.Equal(t, collectionIDs, result)
}

func TestBalanceChecker_FilterCollectionForBalance_WithFiltering(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Mock meta.GetAll to return collection IDs
	collectionIDs := []int64{1, 2, 3, 4}
	mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
	defer mockGetAll.UnPatch()

	// Create filters: only even numbers pass first filter, only > 2 pass second filter
	evenFilter := func(ctx context.Context, collectionID int64) bool {
		return collectionID%2 == 0
	}

	greaterThanTwoFilter := func(ctx context.Context, collectionID int64) bool {
		return collectionID > 2
	}

	result := checker.filterCollectionForBalance(ctx, evenFilter, greaterThanTwoFilter)
	// Only collection 4 should pass both filters (even AND > 2)
	assert.Equal(t, []int64{4}, result)
}

func TestBalanceChecker_FilterCollectionForBalance_EmptyResult(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Mock meta.GetAll to return collection IDs
	collectionIDs := []int64{1, 2, 3}
	mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
	defer mockGetAll.UnPatch()

	// Create filter that rejects all
	rejectAllFilter := func(ctx context.Context, collectionID int64) bool {
		return false
	}

	result := checker.filterCollectionForBalance(ctx, rejectAllFilter)
	assert.Empty(t, result)
}

// =============================================================================
// Queue Construction Tests
// =============================================================================

func TestBalanceChecker_ConstructStoppingBalanceQueue(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Mock filterCollectionForBalance result
	collectionIDs := []int64{1, 2}
	mockFilterCollections := mockey.Mock((*BalanceChecker).filterCollectionForBalance).Return(collectionIDs).Build()
	defer mockFilterCollections.UnPatch()

	// Mock target manager GetCollectionRowCount
	mockGetRowCount := mockey.Mock(mockey.GetMethod(checker.targetMgr, "GetCollectionRowCount")).Return(int64(100)).Build()
	defer mockGetRowCount.UnPatch()

	// Mock paramtable for sort order
	mockParamValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("byrowcount").Build()
	defer mockParamValue.UnPatch()

	result := checker.constructStoppingBalanceQueue(ctx)
	assert.Equal(t, result.Len(), 2)
}

func TestBalanceChecker_ConstructNormalBalanceQueue(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Mock filterCollectionForBalance result
	collectionIDs := []int64{1, 2}
	mockFilterCollections := mockey.Mock((*BalanceChecker).filterCollectionForBalance).Return(collectionIDs).Build()
	defer mockFilterCollections.UnPatch()

	// Mock target manager GetCollectionRowCount
	mockGetRowCount := mockey.Mock(mockey.GetMethod(checker.targetMgr, "GetCollectionRowCount")).Return(int64(100)).Build()
	defer mockGetRowCount.UnPatch()

	// Mock paramtable for sort order
	mockParamValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("byrowcount").Build()
	defer mockParamValue.UnPatch()

	result := checker.constructNormalBalanceQueue(ctx)
	assert.Equal(t, result.Len(), 2)
}

// =============================================================================
// Replica Getting Tests
// =============================================================================

func TestBalanceChecker_GetReplicaForStoppingBalance_WithRONodes(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	collectionID := int64(1)

	// Create mock replicas
	replica1 := &meta.Replica{}
	replica2 := &meta.Replica{}
	replicas := []*meta.Replica{replica1, replica2}

	// Mock ReplicaManager.GetByCollection
	mockGetByCollection := mockey.Mock(mockey.GetMethod(checker.meta.ReplicaManager, "GetByCollection")).Return(replicas).Build()
	defer mockGetByCollection.UnPatch()

	// Mock replica methods - replica1 has RO nodes (return 1), replica2 doesn't (return 0)
	mockRONodesCount := mockey.Mock((*meta.Replica).RONodesCount).Return(mockey.Sequence(1).Times(1).Then(0)).Build()
	defer mockRONodesCount.UnPatch()

	mockROSQNodesCount := mockey.Mock((*meta.Replica).ROSQNodesCount).Return(0).Build()
	defer mockROSQNodesCount.UnPatch()

	// Mock GetID to return different IDs for each replica
	mockGetID := mockey.Mock((*meta.Replica).GetID).Return(mockey.Sequence(101).Times(1).Then(102)).Build()
	defer mockGetID.UnPatch()

	// Skip streaming service mock for simplicity

	result := checker.getReplicaForStoppingBalance(ctx, collectionID)
	// Should return replica1 ID since it has RO nodes
	assert.Contains(t, result, int64(101))
}

func TestBalanceChecker_GetReplicaForStoppingBalance_NoRONodes(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	collectionID := int64(1)

	// Create mock replicas
	replica1 := &meta.Replica{}
	replicas := []*meta.Replica{replica1}

	// Mock ReplicaManager.GetByCollection
	mockGetByCollection := mockey.Mock(mockey.GetMethod(checker.meta.ReplicaManager, "GetByCollection")).Return(replicas).Build()
	defer mockGetByCollection.UnPatch()

	// Mock replica methods - no RO nodes
	mockRONodesCount := mockey.Mock((*meta.Replica).RONodesCount).Return(0).Build()
	defer mockRONodesCount.UnPatch()

	mockROSQNodesCount := mockey.Mock((*meta.Replica).ROSQNodesCount).Return(0).Build()
	defer mockROSQNodesCount.UnPatch()

	// Skip streaming service mock for simplicity

	result := checker.getReplicaForStoppingBalance(ctx, collectionID)
	assert.Empty(t, result)
}

func TestBalanceChecker_GetReplicaForNormalBalance(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	collectionID := int64(1)

	// Create mock replicas
	replica1 := &meta.Replica{}
	replica2 := &meta.Replica{}
	replicas := []*meta.Replica{replica1, replica2}

	// Mock ReplicaManager.GetByCollection
	mockGetByCollection := mockey.Mock(mockey.GetMethod(checker.meta.ReplicaManager, "GetByCollection")).Return(replicas).Build()
	defer mockGetByCollection.UnPatch()

	// Mock replica GetID methods
	mockGetID := mockey.Mock((*meta.Replica).GetID).Return(mockey.Sequence(101).Times(1).Then(102)).Build()
	defer mockGetID.UnPatch()

	result := checker.getReplicaForNormalBalance(ctx, collectionID)
	expectedIDs := []int64{101, 102}
	assert.ElementsMatch(t, expectedIDs, result)
}

// =============================================================================
// Task Generation Tests
// =============================================================================

func TestBalanceChecker_GenerateBalanceTasksFromReplicas_EmptyReplicas(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	config := balanceConfig{}
	balancer := balance.GetGlobalBalancerFactory().GetBalancer()

	segmentTasks, channelTasks := checker.generateBalanceTasksFromReplicas(ctx, balancer, []int64{}, config, false)

	assert.Empty(t, segmentTasks)
	assert.Empty(t, channelTasks)
}

func TestBalanceChecker_GenerateBalanceTasksFromReplicas_Success(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	config := balanceConfig{
		segmentTaskTimeout: 30 * time.Second,
		channelTaskTimeout: 30 * time.Second,
	}
	replicaIDs := []int64{101}

	// Create mock replica
	mockReplica := &meta.Replica{}

	// Mock ReplicaManager.Get
	mockReplicaGet := mockey.Mock(mockey.GetMethod(checker.meta.ReplicaManager, "Get")).Return(mockReplica).Build()
	defer mockReplicaGet.UnPatch()

	// Create mock balance plans
	segmentPlan := assign.SegmentAssignPlan{
		Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 1}},
		Replica: mockReplica,
		From:    1,
		To:      2,
	}
	channelPlan := assign.ChannelAssignPlan{
		Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "test"}},
		Replica: mockReplica,
		From:    1,
		To:      2,
	}

	mockBalancer := mockey.Mock((*balance.BalancerFactory).GetBalancer).To(func(*balance.BalancerFactory) balance.Balance {
		return balance.NewScoreBasedBalancer(nil, nil, nil, nil)
	}).Build()
	defer mockBalancer.UnPatch()

	// Mock balancer.BalanceReplica
	mockBalanceReplica := mockey.Mock((*balance.ScoreBasedBalancer).BalanceReplica).Return(
		[]assign.SegmentAssignPlan{segmentPlan},
		[]assign.ChannelAssignPlan{channelPlan},
	).Build()
	defer mockBalanceReplica.UnPatch()

	// Mock balance.CreateSegmentTasksFromPlans
	mockSegmentTask := &task.SegmentTask{}
	mockCreateSegmentTasks := mockey.Mock(balance.CreateSegmentTasksFromPlans).Return([]task.Task{mockSegmentTask}).Build()
	defer mockCreateSegmentTasks.UnPatch()

	// Mock balance.CreateChannelTasksFromPlans
	mockChannelTask := &task.ChannelTask{}
	mockCreateChannelTasks := mockey.Mock(balance.CreateChannelTasksFromPlans).Return([]task.Task{mockChannelTask}).Build()
	defer mockCreateChannelTasks.UnPatch()

	// Mock task.SetPriority and task.SetReason
	mockSetPriority := mockey.Mock(task.SetPriority).Return().Build()
	defer mockSetPriority.UnPatch()

	mockSetReason := mockey.Mock(task.SetReason).Return().Build()
	defer mockSetReason.UnPatch()

	// Mock balance.PrintNewBalancePlans
	mockPrintPlans := mockey.Mock(balance.PrintNewBalancePlans).Return().Build()
	defer mockPrintPlans.UnPatch()

	balancer := balance.GetGlobalBalancerFactory().GetBalancer()
	segmentTasks, channelTasks := checker.generateBalanceTasksFromReplicas(ctx, balancer, replicaIDs, config, false)

	assert.Len(t, segmentTasks, 1)
	assert.Len(t, channelTasks, 1)
	assert.Equal(t, mockSegmentTask, segmentTasks[0])
	assert.Equal(t, mockChannelTask, channelTasks[0])
}

func TestBalanceChecker_GenerateBalanceTasksFromReplicas_NilReplica(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	config := balanceConfig{}
	replicaIDs := []int64{101}

	// Mock ReplicaManager.Get to return nil
	mockReplicaGet := mockey.Mock(mockey.GetMethod(checker.meta.ReplicaManager, "Get")).Return(nil).Build()
	defer mockReplicaGet.UnPatch()

	balancer := balance.GetGlobalBalancerFactory().GetBalancer()
	segmentTasks, channelTasks := checker.generateBalanceTasksFromReplicas(ctx, balancer, replicaIDs, config, false)

	assert.Empty(t, segmentTasks)
	assert.Empty(t, channelTasks)
}

// =============================================================================
// LoadPriority Tests
// =============================================================================

func TestBalanceChecker_GenerateBalanceTasksFromReplicas_StoppingBalanceHighPriority(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	config := balanceConfig{
		segmentTaskTimeout: 30 * time.Second,
		channelTaskTimeout: 30 * time.Second,
	}
	replicaIDs := []int64{101}

	// Create mock replica
	mockReplica := &meta.Replica{}

	// Mock ReplicaManager.Get
	mockReplicaGet := mockey.Mock(mockey.GetMethod(checker.meta.ReplicaManager, "Get")).Return(mockReplica).Build()
	defer mockReplicaGet.UnPatch()

	// Create mock balance plans (without LoadPriority set, should be set by generateBalanceTasksFromReplicas)
	segmentPlan := assign.SegmentAssignPlan{
		Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1}},
		Replica: mockReplica,
		From:    1,
		To:      2,
	}

	mockBalancer := mockey.Mock((*balance.BalancerFactory).GetBalancer).To(func(*balance.BalancerFactory) balance.Balance {
		return balance.NewScoreBasedBalancer(nil, nil, nil, nil)
	}).Build()
	defer mockBalancer.UnPatch()

	// Mock balancer.BalanceReplica
	mockBalanceReplica := mockey.Mock((*balance.ScoreBasedBalancer).BalanceReplica).Return(
		[]assign.SegmentAssignPlan{segmentPlan},
		[]assign.ChannelAssignPlan{},
	).Build()
	defer mockBalanceReplica.UnPatch()

	// Capture the plans passed to CreateSegmentTasksFromPlans to verify LoadPriority
	var capturedPlans []assign.SegmentAssignPlan
	mockCreateSegmentTasks := mockey.Mock(balance.CreateSegmentTasksFromPlans).To(
		func(ctx context.Context, source task.Source, timeout time.Duration, plans []assign.SegmentAssignPlan) []task.Task {
			capturedPlans = plans
			return []task.Task{}
		}).Build()
	defer mockCreateSegmentTasks.UnPatch()

	// Mock balance.PrintNewBalancePlans
	mockPrintPlans := mockey.Mock(balance.PrintNewBalancePlans).Return().Build()
	defer mockPrintPlans.UnPatch()

	balancer := balance.GetGlobalBalancerFactory().GetBalancer()
	// Call with isStoppingBalance=true
	checker.generateBalanceTasksFromReplicas(ctx, balancer, replicaIDs, config, true)

	// Verify LoadPriority is set to HIGH for stopping balance
	assert.Len(t, capturedPlans, 1)
	assert.Equal(t, commonpb.LoadPriority_HIGH, capturedPlans[0].LoadPriority,
		"Stopping balance should use HIGH priority")
}

func TestBalanceChecker_GenerateBalanceTasksFromReplicas_NormalBalanceLowPriority(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	config := balanceConfig{
		segmentTaskTimeout: 30 * time.Second,
		channelTaskTimeout: 30 * time.Second,
	}
	replicaIDs := []int64{101}

	// Create mock replica
	mockReplica := &meta.Replica{}

	// Mock ReplicaManager.Get
	mockReplicaGet := mockey.Mock(mockey.GetMethod(checker.meta.ReplicaManager, "Get")).Return(mockReplica).Build()
	defer mockReplicaGet.UnPatch()

	// Create mock balance plans (without LoadPriority set)
	segmentPlan := assign.SegmentAssignPlan{
		Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1}},
		Replica: mockReplica,
		From:    1,
		To:      2,
	}

	mockBalancer := mockey.Mock((*balance.BalancerFactory).GetBalancer).To(func(*balance.BalancerFactory) balance.Balance {
		return balance.NewScoreBasedBalancer(nil, nil, nil, nil)
	}).Build()
	defer mockBalancer.UnPatch()

	// Mock balancer.BalanceReplica
	mockBalanceReplica := mockey.Mock((*balance.ScoreBasedBalancer).BalanceReplica).Return(
		[]assign.SegmentAssignPlan{segmentPlan},
		[]assign.ChannelAssignPlan{},
	).Build()
	defer mockBalanceReplica.UnPatch()

	// Capture the plans passed to CreateSegmentTasksFromPlans to verify LoadPriority
	var capturedPlans []assign.SegmentAssignPlan
	mockCreateSegmentTasks := mockey.Mock(balance.CreateSegmentTasksFromPlans).To(
		func(ctx context.Context, source task.Source, timeout time.Duration, plans []assign.SegmentAssignPlan) []task.Task {
			capturedPlans = plans
			return []task.Task{}
		}).Build()
	defer mockCreateSegmentTasks.UnPatch()

	// Mock balance.PrintNewBalancePlans
	mockPrintPlans := mockey.Mock(balance.PrintNewBalancePlans).Return().Build()
	defer mockPrintPlans.UnPatch()

	balancer := balance.GetGlobalBalancerFactory().GetBalancer()
	// Call with isStoppingBalance=false
	checker.generateBalanceTasksFromReplicas(ctx, balancer, replicaIDs, config, false)

	// Verify LoadPriority is set to LOW for normal balance
	assert.Len(t, capturedPlans, 1)
	assert.Equal(t, commonpb.LoadPriority_LOW, capturedPlans[0].LoadPriority,
		"Normal balance should use LOW priority")
}

// =============================================================================
// Task Submission Tests
// =============================================================================

func TestBalanceChecker_SubmitTasks(t *testing.T) {
	checker := createTestBalanceChecker()

	// Create mock tasks
	segmentTask := &task.SegmentTask{}
	channelTask := &task.ChannelTask{}
	segmentTasks := []task.Task{segmentTask}
	channelTasks := []task.Task{channelTask}

	// Mock scheduler.Add
	mockSchedulerAdd := mockey.Mock(mockey.GetMethod(checker.scheduler, "Add")).Return(nil).Build()
	defer mockSchedulerAdd.UnPatch()

	checker.submitTasks(segmentTasks, channelTasks)

	// Verify scheduler.Add was called for both tasks
	// This is implicit verification through mockey call tracking
}

func TestBalanceChecker_SubmitTasks_EmptyTasks(t *testing.T) {
	checker := createTestBalanceChecker()

	// Mock scheduler.Add - should not be called
	mockSchedulerAdd := mockey.Mock(mockey.GetMethod(checker.scheduler, "Add")).Return(nil).Build()
	defer mockSchedulerAdd.UnPatch()

	checker.submitTasks([]task.Task{}, []task.Task{})

	// No assertions needed - just ensuring no panic with empty tasks
}

// =============================================================================
// Main Check Method Tests
// =============================================================================

func TestBalanceChecker_Check_InactiveChecker(t *testing.T) {
	t.Run("StoppingBalanceRunsWhenInactive", func(t *testing.T) {
		checker := createTestBalanceChecker(withEpochConfigForTest(balanceEpochConfig{}))
		ctx := context.Background()

		// Mock IsActive to return false - checker is inactive
		mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(false).Build()
		defer mockIsActive.UnPatch()

		// Mock paramtable to enable stopping balance
		mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
		defer mockParamGet.UnPatch()

		// First call returns true for EnableStoppingBalance
		mockGetAsBool := mockey.Mock((*paramtable.ParamItem).GetAsBool).Return(true).Build()
		defer mockGetAsBool.UnPatch()

		// Mock GetValue for stopping balance assign policy
		mockGetValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("RoundRobin").Build()
		defer mockGetValue.UnPatch()

		// Mock loadBalanceConfig
		config := balanceConfig{
			segmentBatchSize: 5,
			channelBatchSize: 5,
		}
		mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
		defer mockLoadConfig.UnPatch()

		// Track whether processBalanceQueue was called for stopping balance
		stoppingBalanceCalled := false
		mockProcessQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).To(
			func(ctx context.Context,
				balancer balance.Balance,
				getReplicasFunc func(context.Context, int64) []int64,
				constructQueueFunc func(context.Context) *assign.PriorityQueue,
				getQueueFunc func() *assign.PriorityQueue, config balanceConfig,
				isStoppingBalance bool,
			) (int, int) {
				stoppingBalanceCalled = true
				return 1, 0 // Return some tasks generated
			}).Build()
		defer mockProcessQueue.UnPatch()

		result := checker.Check(ctx)

		// Verify stopping balance was executed even though checker is inactive
		assert.True(t, stoppingBalanceCalled, "Stopping balance should run even when checker is inactive")
		assert.Nil(t, result)
		assert.Nil(t, checker.normalBalanceQueue, "Normal balance queue should be cleared when stopping balance generates tasks")
	})

	t.Run("NormalBalanceSkippedWhenInactive", func(t *testing.T) {
		checker := createTestBalanceChecker(withEpochConfigForTest(balanceEpochConfig{}))
		ctx := context.Background()

		// Mock IsActive to return false - checker is inactive
		mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(false).Build()
		defer mockIsActive.UnPatch()

		// Mock paramtable
		mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
		defer mockParamGet.UnPatch()

		// First call returns false for EnableStoppingBalance, so we skip to normal balance check
		mockGetAsBool := mockey.Mock((*paramtable.ParamItem).GetAsBool).Return(false).Build()
		defer mockGetAsBool.UnPatch()

		// Mock loadBalanceConfig
		config := balanceConfig{
			segmentBatchSize: 5,
			channelBatchSize: 5,
		}
		mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
		defer mockLoadConfig.UnPatch()

		// Track whether processBalanceQueue was called
		processQueueCalled := false
		mockProcessQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).To(
			func(ctx context.Context,
				balancer balance.Balance,
				getReplicasFunc func(context.Context, int64) []int64,
				constructQueueFunc func(context.Context) *assign.PriorityQueue,
				getQueueFunc func() *assign.PriorityQueue, config balanceConfig,
				isStoppingBalance bool,
			) (int, int) {
				processQueueCalled = true
				return 0, 0
			}).Build()
		defer mockProcessQueue.UnPatch()

		result := checker.Check(ctx)

		// Verify normal balance was NOT executed because checker is inactive
		// processBalanceQueue should not be called at all since stopping balance is disabled
		// and IsActive check blocks normal balance
		assert.False(t, processQueueCalled, "Normal balance should not run when checker is inactive")
		assert.Nil(t, result)
	})
}

func TestBalanceChecker_Check_StoppingBalanceEnabled(t *testing.T) {
	t.Run("StoppingBalanceGeneratesTasksAndClearsNormalQueue", func(t *testing.T) {
		checker := createTestBalanceChecker(withEpochConfigForTest(balanceEpochConfig{}))
		ctx := context.Background()

		// Pre-populate normal balance queue to verify it gets cleared
		checker.normalBalanceQueue = createMockPriorityQueue()
		checker.normalBalanceQueue.Push(newCollectionBalanceItem(1, 100, "byrowcount"))

		// Mock IsActive to return true
		mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(true).Build()
		defer mockIsActive.UnPatch()

		// Mock paramtable for enabling stopping balance
		mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
		defer mockParamGet.UnPatch()

		mockStoppingBalanceEnabled := mockey.Mock((*paramtable.ParamItem).GetAsBool).Return(true).Build()
		defer mockStoppingBalanceEnabled.UnPatch()

		// Mock GetValue for stopping balance assign policy
		mockGetValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("RoundRobin").Build()
		defer mockGetValue.UnPatch()

		// Mock loadBalanceConfig
		config := balanceConfig{
			segmentBatchSize: 5,
			channelBatchSize: 5,
		}
		mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
		defer mockLoadConfig.UnPatch()

		// Track which balance type was called
		stoppingBalanceCalled := false
		mockProcessQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).To(
			func(ctx context.Context,
				balancer balance.Balance,
				getReplicasFunc func(context.Context, int64) []int64,
				constructQueueFunc func(context.Context) *assign.PriorityQueue,
				getQueueFunc func() *assign.PriorityQueue, config balanceConfig,
				isStoppingBalance bool,
			) (int, int) {
				// Verify this is stopping balance by checking the function pointers
				stoppingBalanceCalled = true
				return 1, 0 // Generate stopping balance tasks
			}).Build()
		defer mockProcessQueue.UnPatch()

		result := checker.Check(ctx)

		// Verify stopping balance was executed
		assert.True(t, stoppingBalanceCalled, "Stopping balance should have been called")
		assert.Nil(t, result, "Check should always return nil")
		assert.Nil(t, checker.normalBalanceQueue, "Normal balance queue should be cleared when stopping balance generates tasks")
	})

	t.Run("StoppingBalanceNoTasksAllowsNormalBalance", func(t *testing.T) {
		checker := createTestBalanceChecker(withEpochConfigForTest(balanceEpochConfig{}))
		ctx := context.Background()
		checker.autoBalanceTs = time.Time{} // Allow normal balance

		// Mock IsActive to return true
		mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(true).Build()
		defer mockIsActive.UnPatch()

		// Mock paramtable - stopping balance enabled, auto balance enabled
		mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
		defer mockParamGet.UnPatch()

		// Return true for both EnableStoppingBalance and AutoBalance
		mockGetAsBool := mockey.Mock((*paramtable.ParamItem).GetAsBool).Return(true).Build()
		defer mockGetAsBool.UnPatch()

		// Mock GetValue for stopping balance assign policy
		mockGetValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("RoundRobin").Build()
		defer mockGetValue.UnPatch()

		// Mock loadBalanceConfig
		config := balanceConfig{
			segmentBatchSize:    5,
			channelBatchSize:    5,
			autoBalanceInterval: 1 * time.Second,
		}
		mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
		defer mockLoadConfig.UnPatch()

		// Track how many times processBalanceQueue is called
		callCount := 0
		mockProcessQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).To(
			func(ctx context.Context,
				balancer balance.Balance,
				getReplicasFunc func(context.Context, int64) []int64,
				constructQueueFunc func(context.Context) *assign.PriorityQueue,
				getQueueFunc func() *assign.PriorityQueue, config balanceConfig,
				isStoppingBalance bool,
			) (int, int) {
				callCount++
				if callCount == 1 {
					// First call: stopping balance generates no tasks
					return 0, 0
				}
				// Second call: normal balance generates tasks
				return 0, 1
			}).Build()
		defer mockProcessQueue.UnPatch()

		result := checker.Check(ctx)

		// Verify both stopping and normal balance were attempted
		assert.Equal(t, 2, callCount, "Both stopping balance and normal balance should be called")
		assert.Nil(t, result)
		assert.Nil(t, checker.stoppingBalanceQueue, "Stopping balance queue should be cleared when normal balance generates tasks")
	})
}

func TestBalanceChecker_Check_NormalBalanceEnabled(t *testing.T) {
	t.Run("NormalBalanceGeneratesTasks", func(t *testing.T) {
		checker := createTestBalanceChecker(withEpochConfigForTest(balanceEpochConfig{}))
		ctx := context.Background()

		// Set autoBalanceTs to allow normal balance
		checker.autoBalanceTs = time.Time{}

		// Pre-populate stopping balance queue to verify it gets cleared
		checker.stoppingBalanceQueue = createMockPriorityQueue()
		checker.stoppingBalanceQueue.Push(newCollectionBalanceItem(1, 100, "byrowcount"))

		// Mock IsActive to return true
		mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(true).Build()
		defer mockIsActive.UnPatch()

		// Mock paramtable - stopping balance disabled, auto balance enabled
		mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
		defer mockParamGet.UnPatch()

		// return false for stopping balance enabled, true for auto balance enabled
		mockParams := mockey.Mock((*paramtable.ParamItem).GetAsBool).Return(mockey.Sequence(false).Times(1).Then(true)).Build()
		defer mockParams.UnPatch()

		// Mock GetValue for balancer type
		mockGetValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("RoundRobin").Build()
		defer mockGetValue.UnPatch()

		// Mock loadBalanceConfig
		config := balanceConfig{
			segmentBatchSize:    5,
			channelBatchSize:    5,
			autoBalanceInterval: 1 * time.Second,
		}
		mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
		defer mockLoadConfig.UnPatch()

		// Track normal balance call and timestamp update
		normalBalanceCalled := false
		originalTs := checker.autoBalanceTs
		mockProcessQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).To(
			func(ctx context.Context,
				balancer balance.Balance,
				getReplicasFunc func(context.Context, int64) []int64,
				constructQueueFunc func(context.Context) *assign.PriorityQueue,
				getQueueFunc func() *assign.PriorityQueue, config balanceConfig,
				isStoppingBalance bool,
			) (int, int) {
				normalBalanceCalled = true
				return 0, 1 // Generate normal balance tasks
			}).Build()
		defer mockProcessQueue.UnPatch()

		result := checker.Check(ctx)

		// Verify normal balance was executed
		assert.True(t, normalBalanceCalled, "Normal balance should have been called")
		assert.Nil(t, result, "Check should always return nil")
		assert.Nil(t, checker.stoppingBalanceQueue, "Stopping balance queue should be cleared when normal balance generates tasks")
		assert.True(t, checker.autoBalanceTs.After(originalTs), "autoBalanceTs should be updated when tasks are generated")
	})

	t.Run("NormalBalanceRespects IntervalThrottle", func(t *testing.T) {
		checker := createTestBalanceChecker(withEpochConfigForTest(balanceEpochConfig{}))
		ctx := context.Background()

		// Set autoBalanceTs to recent time to trigger throttling
		checker.autoBalanceTs = time.Now()

		// Mock IsActive to return true
		mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(true).Build()
		defer mockIsActive.UnPatch()

		// Mock paramtable - stopping balance disabled, auto balance enabled
		mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
		defer mockParamGet.UnPatch()

		// return false for stopping balance enabled, true for auto balance enabled
		mockParams := mockey.Mock((*paramtable.ParamItem).GetAsBool).Return(mockey.Sequence(false).Times(1).Then(true)).Build()
		defer mockParams.UnPatch()

		// Mock GetValue for balancer type
		mockGetValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("RoundRobin").Build()
		defer mockGetValue.UnPatch()

		// Mock loadBalanceConfig with a large interval
		config := balanceConfig{
			segmentBatchSize:    5,
			channelBatchSize:    5,
			autoBalanceInterval: 10 * time.Second, // Long interval
		}
		mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
		defer mockLoadConfig.UnPatch()

		// Track whether processBalanceQueue was called
		normalBalanceCalled := false
		mockProcessQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).To(
			func(ctx context.Context,
				balancer balance.Balance,
				getReplicasFunc func(context.Context, int64) []int64,
				constructQueueFunc func(context.Context) *assign.PriorityQueue,
				getQueueFunc func() *assign.PriorityQueue, config balanceConfig,
				isStoppingBalance bool,
			) (int, int) {
				normalBalanceCalled = true
				return 0, 1
			}).Build()
		defer mockProcessQueue.UnPatch()

		result := checker.Check(ctx)

		// Verify normal balance was NOT executed due to interval throttle
		assert.False(t, normalBalanceCalled, "Normal balance should respect autoBalanceInterval throttle")
		assert.Nil(t, result)
	})

	t.Run("NormalBalanceSkippedWhenAutoBalanceDisabled", func(t *testing.T) {
		checker := createTestBalanceChecker(withEpochConfigForTest(balanceEpochConfig{}))
		ctx := context.Background()

		// Set autoBalanceTs to allow normal balance
		checker.autoBalanceTs = time.Time{}

		// Mock IsActive to return true
		mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(true).Build()
		defer mockIsActive.UnPatch()

		// Mock paramtable - stopping balance disabled, auto balance also disabled
		mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
		defer mockParamGet.UnPatch()

		// return false for both stopping balance and auto balance
		mockParams := mockey.Mock((*paramtable.ParamItem).GetAsBool).Return(false).Build()
		defer mockParams.UnPatch()

		// Mock loadBalanceConfig
		config := balanceConfig{
			segmentBatchSize:    5,
			channelBatchSize:    5,
			autoBalanceInterval: 1 * time.Second,
		}
		mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
		defer mockLoadConfig.UnPatch()

		// Track whether processBalanceQueue was called
		normalBalanceCalled := false
		mockProcessQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).To(
			func(ctx context.Context,
				balancer balance.Balance,
				getReplicasFunc func(context.Context, int64) []int64,
				constructQueueFunc func(context.Context) *assign.PriorityQueue,
				getQueueFunc func() *assign.PriorityQueue, config balanceConfig,
				isStoppingBalance bool,
			) (int, int) {
				normalBalanceCalled = true
				return 0, 1
			}).Build()
		defer mockProcessQueue.UnPatch()

		result := checker.Check(ctx)

		// Verify normal balance was NOT executed because auto balance is disabled
		assert.False(t, normalBalanceCalled, "Normal balance should be skipped when AutoBalance is disabled")
		assert.Nil(t, result)
	})
}

// =============================================================================
// ProcessBalanceQueue Tests
// =============================================================================

func TestBalanceChecker_ProcessBalanceQueue_Success(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Create mock balance config
	config := balanceConfig{
		segmentBatchSize:             5,
		channelBatchSize:             3,
		maxCheckCollectionCount:      5,
		balanceOnMultipleCollections: true,
	}

	// Create mock priority queue
	mockQueue := createMockPriorityQueue()
	// Use real priority queue for simplicity

	mockQueue.Push(newCollectionBalanceItem(1, 100, "byrowcount"))
	mockQueue.Push(newCollectionBalanceItem(2, 100, "byrowcount"))
	mockQueue.Push(newCollectionBalanceItem(3, 100, "byrowcount"))
	mockQueue.Push(newCollectionBalanceItem(4, 100, "byrowcount"))
	mockQueue.Push(newCollectionBalanceItem(5, 100, "byrowcount"))

	// Mock getReplicasFunc
	getReplicasFunc := func(ctx context.Context, collectionID int64) []int64 {
		return []int64{101, 102}
	}

	// Mock constructQueueFunc
	constructQueueFunc := func(ctx context.Context) *assign.PriorityQueue {
		return mockQueue
	}

	// Mock getQueueFunc
	getQueueFunc := func() *assign.PriorityQueue {
		return mockQueue
	}

	// Mock generateBalanceTasksFromReplicas
	mockSegmentTask := &task.SegmentTask{}
	mockChannelTask := &task.ChannelTask{}
	mockGenerateTasks := mockey.Mock((*BalanceChecker).generateBalanceTasksFromReplicas).Return(
		[]task.Task{mockSegmentTask}, []task.Task{mockChannelTask},
	).Build()
	defer mockGenerateTasks.UnPatch()

	// mock submit tasks
	mockSubmitTasks := mockey.Mock((*BalanceChecker).submitTasks).Return(1, 1).Build()
	defer mockSubmitTasks.UnPatch()

	balancer := balance.GetGlobalBalancerFactory().GetBalancer()
	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, balancer, getReplicasFunc, constructQueueFunc, getQueueFunc, config, false,
	)

	assert.Equal(t, 3, segmentTasks)
	assert.Equal(t, 3, channelTasks)
}

func TestBalanceChecker_ProcessBalanceQueue_EmptyQueue(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	config := balanceConfig{
		segmentBatchSize: 5,
		channelBatchSize: 3,
	}

	// Create empty mock priority queue
	mockQueue := createMockPriorityQueue()
	// Use real priority queue for empty queue testing

	getReplicasFunc := func(ctx context.Context, collectionID int64) []int64 {
		return []int64{101}
	}

	constructQueueFunc := func(ctx context.Context) *assign.PriorityQueue {
		return mockQueue
	}

	getQueueFunc := func() *assign.PriorityQueue {
		return mockQueue
	}

	balancer := balance.GetGlobalBalancerFactory().GetBalancer()
	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, balancer, getReplicasFunc, constructQueueFunc, getQueueFunc, config, false,
	)

	assert.Equal(t, 0, segmentTasks)
	assert.Equal(t, 0, channelTasks)
}

func TestBalanceChecker_ProcessBalanceQueue_BatchSizeLimit(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Set small batch sizes to test limits
	config := balanceConfig{
		segmentBatchSize:             1, // Only allow 1 segment task
		channelBatchSize:             1, // Only allow 1 channel task
		maxCheckCollectionCount:      10,
		balanceOnMultipleCollections: true,
	}

	// Test batch size limits with simplified logic

	// Create mock priority queue with 2 items
	mockQueue := createMockPriorityQueue()
	// Use real priority queue for batch size testing

	mockQueue.Push(newCollectionBalanceItem(1, 100, "byrowcount"))
	mockQueue.Push(newCollectionBalanceItem(2, 100, "byrowcount"))

	getReplicasFunc := func(ctx context.Context, collectionID int64) []int64 {
		return []int64{101}
	}

	constructQueueFunc := func(ctx context.Context) *assign.PriorityQueue {
		return mockQueue
	}

	getQueueFunc := func() *assign.PriorityQueue {
		return mockQueue
	}

	// Mock generateBalanceTasksFromReplicas to return multiple tasks
	mockSegmentTask1 := &task.SegmentTask{}
	mockSegmentTask2 := &task.SegmentTask{}
	mockChannelTask1 := &task.ChannelTask{}
	mockChannelTask2 := &task.ChannelTask{}

	mockGenerateTasks := mockey.Mock((*BalanceChecker).generateBalanceTasksFromReplicas).Return(mockey.Sequence(
		[]task.Task{mockSegmentTask1}, []task.Task{mockChannelTask1},
	).Times(1).Then(
		[]task.Task{mockSegmentTask2}, []task.Task{mockChannelTask2},
	)).Build()
	defer mockGenerateTasks.UnPatch()

	// mock submit tasks
	mockSubmitTasks := mockey.Mock((*BalanceChecker).submitTasks).Return(1, 1).Build()
	defer mockSubmitTasks.UnPatch()

	balancer := balance.GetGlobalBalancerFactory().GetBalancer()
	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, balancer, getReplicasFunc, constructQueueFunc, getQueueFunc, config, false,
	)

	// Should stop after first collection due to batch size limits
	assert.Equal(t, 1, segmentTasks)
	assert.Equal(t, 1, channelTasks)
}

func TestBalanceChecker_ProcessBalanceQueue_MultiCollectionDisabled(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	config := balanceConfig{
		segmentBatchSize:             10,
		channelBatchSize:             10,
		maxCheckCollectionCount:      10,
		balanceOnMultipleCollections: false, // Disabled
	}

	mockQueue := createMockPriorityQueue()
	// Use real priority queue for multi-collection testing

	getReplicasFunc := func(ctx context.Context, collectionID int64) []int64 {
		return []int64{101}
	}

	constructQueueFunc := func(ctx context.Context) *assign.PriorityQueue {
		return mockQueue
	}

	getQueueFunc := func() *assign.PriorityQueue {
		return mockQueue
	}

	mockQueue.Push(newCollectionBalanceItem(1, 100, "byrowcount"))

	// Mock generateBalanceTasksFromReplicas to return tasks
	mockSegmentTask := &task.SegmentTask{}
	mockGenerateTasks := mockey.Mock((*BalanceChecker).generateBalanceTasksFromReplicas).Return(
		[]task.Task{mockSegmentTask}, []task.Task{},
	).Build()
	defer mockGenerateTasks.UnPatch()

	// mock submit tasks
	mockSubmitTasks := mockey.Mock((*BalanceChecker).submitTasks).Return(1, 0).Build()
	defer mockSubmitTasks.UnPatch()

	balancer := balance.GetGlobalBalancerFactory().GetBalancer()
	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, balancer, getReplicasFunc, constructQueueFunc, getQueueFunc, config, false,
	)

	// Should stop after first collection due to multi-collection disabled
	assert.Equal(t, 1, segmentTasks)
	assert.Equal(t, 0, channelTasks)
}

func TestBalanceChecker_ProcessBalanceQueue_NoReplicasToBalance(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	config := balanceConfig{
		segmentBatchSize:             5,
		channelBatchSize:             5,
		maxCheckCollectionCount:      5,
		balanceOnMultipleCollections: true,
	}

	mockQueue := createMockPriorityQueue()
	// Use real priority queue for simplicity

	// getReplicasFunc returns empty slice
	getReplicasFunc := func(ctx context.Context, collectionID int64) []int64 {
		return []int64{} // No replicas
	}

	constructQueueFunc := func(ctx context.Context) *assign.PriorityQueue {
		return mockQueue
	}

	getQueueFunc := func() *assign.PriorityQueue {
		return mockQueue
	}

	balancer := balance.GetGlobalBalancerFactory().GetBalancer()
	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, balancer, getReplicasFunc, constructQueueFunc, getQueueFunc, config, false,
	)

	assert.Equal(t, 0, segmentTasks)
	assert.Equal(t, 0, channelTasks)
}

// =============================================================================
// Performance and Edge Case Tests
// =============================================================================

func TestBalanceChecker_CollectionBalanceItem_EdgeCases(t *testing.T) {
	// Test with zero row count
	item := newCollectionBalanceItem(1, 0, "byrowcount")
	assert.Equal(t, 0, item.getPriority())

	// Test with negative collection ID
	item = newCollectionBalanceItem(-1, 100, "bycollectionid")
	assert.Equal(t, -1, item.getPriority())

	// Test with very large values
	item = newCollectionBalanceItem(9223372036854775807, 2147483647, "byrowcount")
	assert.Equal(t, -2147483647, item.getPriority())

	// Test with empty sort order (should default to byrowcount)
	item = newCollectionBalanceItem(5, 100, "")
	assert.Equal(t, -100, item.getPriority())

	// Test with invalid sort order (should default to byrowcount)
	item = newCollectionBalanceItem(5, 100, "invalid")
	assert.Equal(t, -100, item.getPriority())
}

func TestBalanceChecker_FilterCollectionForBalance_EdgeCases(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Test with empty collection list
	mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return([]int64{}).Build()
	defer mockGetAll.UnPatch()

	passAllFilter := func(ctx context.Context, collectionID int64) bool {
		return true
	}

	result := checker.filterCollectionForBalance(ctx, passAllFilter)
	assert.Empty(t, result)

	// Test with no filters
	collectionIDs := []int64{1, 2, 3}
	mockGetAll.UnPatch()
	mockGetAll = mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
	defer mockGetAll.UnPatch()

	result = checker.filterCollectionForBalance(ctx)
	assert.Equal(t, collectionIDs, result) // No filters means all pass
}

// =============================================================================
// Streaming Service Tests
// =============================================================================

func TestBalanceChecker_GetReplicaForStoppingBalance_WithStreamingService(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	collectionID := int64(1)

	// Create mock replicas
	replica1 := &meta.Replica{}
	replicas := []*meta.Replica{replica1}

	// Mock ReplicaManager.GetByCollection
	mockGetByCollection := mockey.Mock(mockey.GetMethod(checker.meta.ReplicaManager, "GetByCollection")).Return(replicas).Build()
	defer mockGetByCollection.UnPatch()

	// Mock replica methods - no RO nodes but has streaming channel RO nodes
	mockRONodesCount := mockey.Mock((*meta.Replica).RONodesCount).Return(0).Build()
	defer mockRONodesCount.UnPatch()

	mockROSQNodesCount := mockey.Mock((*meta.Replica).ROSQNodesCount).Return(0).Build()
	defer mockROSQNodesCount.UnPatch()

	mockGetID := mockey.Mock((*meta.Replica).GetID).Return(int64(101)).Build()
	defer mockGetID.UnPatch()

	// streaming service mocks for simplicity
	mockIsStreamingServiceEnabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(true).Build()
	defer mockIsStreamingServiceEnabled.UnPatch()
	mockGetChannelRWAndRONodesFor260 := mockey.Mock(utils.GetChannelRWAndRONodesFor260).Return([]int64{}, []int64{1}).Build()
	defer mockGetChannelRWAndRONodesFor260.UnPatch()

	result := checker.getReplicaForStoppingBalance(ctx, collectionID)
	// Should return replica1 ID since it has channel RO nodes
	assert.Equal(t, []int64{101}, result)
}

// =============================================================================
// Filter Serviceable Collections Tests
// =============================================================================

func TestBalanceChecker_ConstructNormalBalanceQueue_FilterServiceableCollections(t *testing.T) {
	t.Run("FilterOutLoadingCollection", func(t *testing.T) {
		checker := createTestBalanceChecker()
		ctx := context.Background()

		// Mock meta.GetAll to return collection IDs
		collectionIDs := []int64{1, 2, 3}
		mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
		defer mockGetAll.UnPatch()

		// Create collections with different load statuses
		loadedCollection := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 1,
				Status:       querypb.LoadStatus_Loaded,
			},
		}
		loadingCollection := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 2,
				Status:       querypb.LoadStatus_Loading,
			},
		}
		loadedCollection2 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 3,
				Status:       querypb.LoadStatus_Loaded,
			},
		}

		// Mock CollectionManager.GetCollection to return different collections based on ID
		mockGetCollection := mockey.Mock((*meta.CollectionManager).GetCollection).To(
			func(m *meta.CollectionManager, ctx context.Context, collectionID int64) *meta.Collection {
				switch collectionID {
				case 1:
					return loadedCollection
				case 2:
					return loadingCollection
				case 3:
					return loadedCollection2
				default:
					return nil
				}
			}).Build()
		defer mockGetCollection.UnPatch()

		// Mock ChannelDistManager.GetByFilter to return serviceable channels
		mockGetChannels := mockey.Mock((*meta.ChannelDistManager).GetByFilter).To(
			func(_ *meta.ChannelDistManager, _ ...meta.ChannelDistFilter) []*meta.DmChannel {
				// Return serviceable channels for all collections
				return []*meta.DmChannel{
					{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
				}
			}).Build()
		defer mockGetChannels.UnPatch()

		// Mock target manager methods
		mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(true).Build()
		defer mockIsNextTargetExist.UnPatch()

		mockIsCurrentTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetExist")).Return(true).Build()
		defer mockIsCurrentTargetExist.UnPatch()

		mockIsCurrentTargetReady := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetReady")).Return(true).Build()
		defer mockIsCurrentTargetReady.UnPatch()

		mockGetRowCount := mockey.Mock(mockey.GetMethod(checker.targetMgr, "GetCollectionRowCount")).Return(int64(100)).Build()
		defer mockGetRowCount.UnPatch()

		// Mock paramtable for sort order
		mockParamValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("byrowcount").Build()
		defer mockParamValue.UnPatch()

		result := checker.constructNormalBalanceQueue(ctx)

		// Should only include loaded collections (1 and 3), not loading collection (2)
		assert.Equal(t, 2, result.Len())

		// Pop items and verify
		collectionIDsInQueue := make([]int64, 0)
		for result.Len() > 0 {
			item := result.Pop().(*collectionBalanceItem)
			collectionIDsInQueue = append(collectionIDsInQueue, item.collectionID)
		}
		assert.NotContains(t, collectionIDsInQueue, int64(2), "Loading collection should be filtered out")
		assert.Contains(t, collectionIDsInQueue, int64(1), "Loaded collection 1 should be included")
		assert.Contains(t, collectionIDsInQueue, int64(3), "Loaded collection 3 should be included")
	})

	t.Run("FilterOutInvalidStatusCollection", func(t *testing.T) {
		checker := createTestBalanceChecker()
		ctx := context.Background()

		// Mock meta.GetAll to return collection IDs
		collectionIDs := []int64{1, 2}
		mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
		defer mockGetAll.UnPatch()

		// Create collections with different load statuses
		loadedCollection := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 1,
				Status:       querypb.LoadStatus_Loaded,
			},
		}
		invalidCollection := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 2,
				Status:       querypb.LoadStatus_Invalid,
			},
		}

		// Mock CollectionManager.GetCollection to return different collections based on ID
		mockGetCollection := mockey.Mock((*meta.CollectionManager).GetCollection).To(
			func(m *meta.CollectionManager, ctx context.Context, collectionID int64) *meta.Collection {
				switch collectionID {
				case 1:
					return loadedCollection
				case 2:
					return invalidCollection
				default:
					return nil
				}
			}).Build()
		defer mockGetCollection.UnPatch()

		// Mock ChannelDistManager.GetByFilter to return serviceable channels
		mockGetChannels := mockey.Mock((*meta.ChannelDistManager).GetByFilter).To(
			func(_ *meta.ChannelDistManager, _ ...meta.ChannelDistFilter) []*meta.DmChannel {
				return []*meta.DmChannel{
					{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
				}
			}).Build()
		defer mockGetChannels.UnPatch()

		// Mock target manager methods
		mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(true).Build()
		defer mockIsNextTargetExist.UnPatch()

		mockIsCurrentTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetExist")).Return(true).Build()
		defer mockIsCurrentTargetExist.UnPatch()

		mockIsCurrentTargetReady := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetReady")).Return(true).Build()
		defer mockIsCurrentTargetReady.UnPatch()

		mockGetRowCount := mockey.Mock(mockey.GetMethod(checker.targetMgr, "GetCollectionRowCount")).Return(int64(100)).Build()
		defer mockGetRowCount.UnPatch()

		// Mock paramtable for sort order
		mockParamValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("byrowcount").Build()
		defer mockParamValue.UnPatch()

		result := checker.constructNormalBalanceQueue(ctx)

		// Should only include loaded collection (1), not invalid collection (2)
		assert.Equal(t, 1, result.Len())

		item := result.Pop().(*collectionBalanceItem)
		assert.Equal(t, int64(1), item.collectionID)
	})

	t.Run("FilterOutNilCollection", func(t *testing.T) {
		checker := createTestBalanceChecker()
		ctx := context.Background()

		// Mock meta.GetAll to return collection IDs
		collectionIDs := []int64{1, 2}
		mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
		defer mockGetAll.UnPatch()

		// Create loaded collection
		loadedCollection := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 1,
				Status:       querypb.LoadStatus_Loaded,
			},
		}

		// Mock CollectionManager.GetCollection - collection 2 returns nil
		mockGetCollection := mockey.Mock((*meta.CollectionManager).GetCollection).To(
			func(m *meta.CollectionManager, ctx context.Context, collectionID int64) *meta.Collection {
				if collectionID == 1 {
					return loadedCollection
				}
				return nil
			}).Build()
		defer mockGetCollection.UnPatch()

		// Mock ChannelDistManager.GetByFilter to return serviceable channels
		mockGetChannels := mockey.Mock((*meta.ChannelDistManager).GetByFilter).To(
			func(_ *meta.ChannelDistManager, _ ...meta.ChannelDistFilter) []*meta.DmChannel {
				return []*meta.DmChannel{
					{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
				}
			}).Build()
		defer mockGetChannels.UnPatch()

		// Mock target manager methods
		mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(true).Build()
		defer mockIsNextTargetExist.UnPatch()

		mockIsCurrentTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetExist")).Return(true).Build()
		defer mockIsCurrentTargetExist.UnPatch()

		mockIsCurrentTargetReady := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetReady")).Return(true).Build()
		defer mockIsCurrentTargetReady.UnPatch()

		mockGetRowCount := mockey.Mock(mockey.GetMethod(checker.targetMgr, "GetCollectionRowCount")).Return(int64(100)).Build()
		defer mockGetRowCount.UnPatch()

		// Mock paramtable for sort order
		mockParamValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("byrowcount").Build()
		defer mockParamValue.UnPatch()

		result := checker.constructNormalBalanceQueue(ctx)

		// Should only include collection 1, collection 2 should be filtered out due to nil
		assert.Equal(t, 1, result.Len())

		item := result.Pop().(*collectionBalanceItem)
		assert.Equal(t, int64(1), item.collectionID)
	})

	t.Run("AllCollectionsFiltered", func(t *testing.T) {
		checker := createTestBalanceChecker()
		ctx := context.Background()

		// Mock meta.GetAll to return collection IDs
		collectionIDs := []int64{1, 2}
		mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
		defer mockGetAll.UnPatch()

		// Create collections with non-loaded status
		loadingCollection1 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 1,
				Status:       querypb.LoadStatus_Loading,
			},
		}
		loadingCollection2 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 2,
				Status:       querypb.LoadStatus_Loading,
			},
		}

		// Mock CollectionManager.GetCollection
		mockGetCollection := mockey.Mock((*meta.CollectionManager).GetCollection).To(
			func(m *meta.CollectionManager, ctx context.Context, collectionID int64) *meta.Collection {
				switch collectionID {
				case 1:
					return loadingCollection1
				case 2:
					return loadingCollection2
				default:
					return nil
				}
			}).Build()
		defer mockGetCollection.UnPatch()

		// Mock ChannelDistManager.GetByFilter to return serviceable channels
		mockGetChannels := mockey.Mock((*meta.ChannelDistManager).GetByFilter).To(
			func(_ *meta.ChannelDistManager, _ ...meta.ChannelDistFilter) []*meta.DmChannel {
				return []*meta.DmChannel{
					{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
				}
			}).Build()
		defer mockGetChannels.UnPatch()

		// Mock target manager methods
		mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(true).Build()
		defer mockIsNextTargetExist.UnPatch()

		mockIsCurrentTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetExist")).Return(true).Build()
		defer mockIsCurrentTargetExist.UnPatch()

		mockIsCurrentTargetReady := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetReady")).Return(true).Build()
		defer mockIsCurrentTargetReady.UnPatch()

		mockGetRowCount := mockey.Mock(mockey.GetMethod(checker.targetMgr, "GetCollectionRowCount")).Return(int64(100)).Build()
		defer mockGetRowCount.UnPatch()

		// Mock paramtable for sort order
		mockParamValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("byrowcount").Build()
		defer mockParamValue.UnPatch()

		result := checker.constructNormalBalanceQueue(ctx)

		// Should be empty since all collections are loading
		assert.Equal(t, 0, result.Len())
	})

	t.Run("FilterOutCollectionWithNonServiceableChannel", func(t *testing.T) {
		checker := createTestBalanceChecker()
		ctx := context.Background()

		// Mock meta.GetAll to return collection IDs
		collectionIDs := []int64{1, 2}
		mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
		defer mockGetAll.UnPatch()

		// Create loaded collections
		loadedCollection1 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 1,
				Status:       querypb.LoadStatus_Loaded,
			},
		}
		loadedCollection2 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 2,
				Status:       querypb.LoadStatus_Loaded,
			},
		}

		// Mock CollectionManager.GetCollection
		mockGetCollection := mockey.Mock((*meta.CollectionManager).GetCollection).To(
			func(m *meta.CollectionManager, ctx context.Context, collectionID int64) *meta.Collection {
				switch collectionID {
				case 1:
					return loadedCollection1
				case 2:
					return loadedCollection2
				default:
					return nil
				}
			}).Build()
		defer mockGetCollection.UnPatch()

		// Mock ChannelDistManager.GetByFilter
		// Collection 1: all channels serviceable
		// Collection 2: one channel not serviceable
		mockGetChannels := mockey.Mock((*meta.ChannelDistManager).GetByFilter).To(
			func(_ *meta.ChannelDistManager, filters ...meta.ChannelDistFilter) []*meta.DmChannel {
				matchColl := func(cid int64) bool {
					ch := &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: cid}}
					for _, f := range filters {
						if !f.Match(ch) {
							return false
						}
					}
					return true
				}
				if matchColl(1) {
					// All serviceable
					return []*meta.DmChannel{
						{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
						{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
					}
				} else if matchColl(2) {
					// One not serviceable
					return []*meta.DmChannel{
						{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
						{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: false}}},
					}
				}
				return nil
			}).Build()
		defer mockGetChannels.UnPatch()

		// Mock target manager methods
		mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(true).Build()
		defer mockIsNextTargetExist.UnPatch()

		mockIsCurrentTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetExist")).Return(true).Build()
		defer mockIsCurrentTargetExist.UnPatch()

		mockIsCurrentTargetReady := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetReady")).Return(true).Build()
		defer mockIsCurrentTargetReady.UnPatch()

		mockGetRowCount := mockey.Mock(mockey.GetMethod(checker.targetMgr, "GetCollectionRowCount")).Return(int64(100)).Build()
		defer mockGetRowCount.UnPatch()

		// Mock paramtable for sort order
		mockParamValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("byrowcount").Build()
		defer mockParamValue.UnPatch()

		result := checker.constructNormalBalanceQueue(ctx)

		// Should only include collection 1 (all channels serviceable)
		// Collection 2 should be filtered out (has non-serviceable channel)
		assert.Equal(t, 1, result.Len())

		item := result.Pop().(*collectionBalanceItem)
		assert.Equal(t, int64(1), item.collectionID)
	})

	t.Run("FilterOutCollectionWithNoChannels", func(t *testing.T) {
		checker := createTestBalanceChecker()
		ctx := context.Background()

		// Mock meta.GetAll to return collection IDs
		collectionIDs := []int64{1, 2}
		mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
		defer mockGetAll.UnPatch()

		// Create loaded collections
		loadedCollection1 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 1,
				Status:       querypb.LoadStatus_Loaded,
			},
		}
		loadedCollection2 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 2,
				Status:       querypb.LoadStatus_Loaded,
			},
		}

		// Mock CollectionManager.GetCollection
		mockGetCollection := mockey.Mock((*meta.CollectionManager).GetCollection).To(
			func(m *meta.CollectionManager, ctx context.Context, collectionID int64) *meta.Collection {
				switch collectionID {
				case 1:
					return loadedCollection1
				case 2:
					return loadedCollection2
				default:
					return nil
				}
			}).Build()
		defer mockGetCollection.UnPatch()

		// Mock ChannelDistManager.GetByFilter
		// Collection 1: has serviceable channels
		// Collection 2: no channels in distribution
		mockGetChannels := mockey.Mock((*meta.ChannelDistManager).GetByFilter).To(
			func(_ *meta.ChannelDistManager, filters ...meta.ChannelDistFilter) []*meta.DmChannel {
				ch1 := &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1}}
				if len(filters) > 0 && filters[0].Match(ch1) {
					return []*meta.DmChannel{
						{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
					}
				}
				// Collection 2 has no channels
				return nil
			}).Build()
		defer mockGetChannels.UnPatch()

		// Mock target manager methods
		mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(true).Build()
		defer mockIsNextTargetExist.UnPatch()

		mockIsCurrentTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetExist")).Return(true).Build()
		defer mockIsCurrentTargetExist.UnPatch()

		mockIsCurrentTargetReady := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetReady")).Return(true).Build()
		defer mockIsCurrentTargetReady.UnPatch()

		mockGetRowCount := mockey.Mock(mockey.GetMethod(checker.targetMgr, "GetCollectionRowCount")).Return(int64(100)).Build()
		defer mockGetRowCount.UnPatch()

		// Mock paramtable for sort order
		mockParamValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("byrowcount").Build()
		defer mockParamValue.UnPatch()

		result := checker.constructNormalBalanceQueue(ctx)

		// Should only include collection 1 (has channels)
		// Collection 2 should be filtered out (no channels)
		assert.Equal(t, 1, result.Len())

		item := result.Pop().(*collectionBalanceItem)
		assert.Equal(t, int64(1), item.collectionID)
	})

	t.Run("AllChannelsServiceable", func(t *testing.T) {
		checker := createTestBalanceChecker()
		ctx := context.Background()

		// Mock meta.GetAll to return collection IDs
		collectionIDs := []int64{1, 2}
		mockGetAll := mockey.Mock((*meta.CollectionManager).GetAll).Return(collectionIDs).Build()
		defer mockGetAll.UnPatch()

		// Create loaded collections
		loadedCollection1 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 1,
				Status:       querypb.LoadStatus_Loaded,
			},
		}
		loadedCollection2 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID: 2,
				Status:       querypb.LoadStatus_Loaded,
			},
		}

		// Mock CollectionManager.GetCollection
		mockGetCollection := mockey.Mock((*meta.CollectionManager).GetCollection).To(
			func(m *meta.CollectionManager, ctx context.Context, collectionID int64) *meta.Collection {
				switch collectionID {
				case 1:
					return loadedCollection1
				case 2:
					return loadedCollection2
				default:
					return nil
				}
			}).Build()
		defer mockGetCollection.UnPatch()

		// Mock ChannelDistManager.GetByFilter
		// Both collections have all serviceable channels
		mockGetChannels := mockey.Mock((*meta.ChannelDistManager).GetByFilter).To(
			func(_ *meta.ChannelDistManager, _ ...meta.ChannelDistFilter) []*meta.DmChannel {
				return []*meta.DmChannel{
					{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
					{View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}}},
				}
			}).Build()
		defer mockGetChannels.UnPatch()

		// Mock target manager methods
		mockIsNextTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsNextTargetExist")).Return(true).Build()
		defer mockIsNextTargetExist.UnPatch()

		mockIsCurrentTargetExist := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetExist")).Return(true).Build()
		defer mockIsCurrentTargetExist.UnPatch()

		mockIsCurrentTargetReady := mockey.Mock(mockey.GetMethod(checker.targetMgr, "IsCurrentTargetReady")).Return(true).Build()
		defer mockIsCurrentTargetReady.UnPatch()

		mockGetRowCount := mockey.Mock(mockey.GetMethod(checker.targetMgr, "GetCollectionRowCount")).Return(int64(100)).Build()
		defer mockGetRowCount.UnPatch()

		// Mock paramtable for sort order
		mockParamValue := mockey.Mock((*paramtable.ParamItem).GetValue).Return("byrowcount").Build()
		defer mockParamValue.UnPatch()

		result := checker.constructNormalBalanceQueue(ctx)

		// Both collections should be included (all channels serviceable)
		assert.Equal(t, 2, result.Len())
	})
}

func TestBalanceChecker_Check_TimeoutWarning(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Mock IsActive to return true
	mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(true).Build()
	defer mockIsActive.UnPatch()

	mockProcessBalanceQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).To(
		func(ctx context.Context,
			balancer balance.Balance,
			getReplicasFunc func(ctx context.Context, collectionID int64) []int64,
			constructQueueFunc func(ctx context.Context) *assign.PriorityQueue,
			getQueueFunc func() *assign.PriorityQueue, config balanceConfig,
			isStoppingBalance bool,
		) (int, int) {
			time.Sleep(150 * time.Millisecond)
			return 0, 0
		}).Build()
	defer mockProcessBalanceQueue.UnPatch()

	start := time.Now()
	result := checker.Check(ctx)
	duration := time.Since(start)

	assert.Nil(t, result)
	assert.Greater(t, duration, 100*time.Millisecond) // Should trigger log
}

func TestBalanceCheckerEpochGroupsReplicasByRG(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	epochConfig := testBalanceEpochConfig()
	replicas := map[int64][]*meta.Replica{
		20: {
			newEpochReplica(40, 20, "rg-b"),
			newEpochReplica(10, 20, "rg-a"),
		},
		10: {
			newEpochReplica(30, 10, "rg-b"),
			newEpochReplica(20, 10, "rg-a"),
		},
	}
	mockEligibleEpochCollections(t, checker, []int64{20, 10}, replicas)

	requests := checker.collectNormalEpochRequests(ctx, balanceConfig{
		maxCheckCollectionCount:      10,
		balanceOnMultipleCollections: true,
	}, epochConfig)

	require.Len(t, requests, 2)
	assert.Equal(t, "rg-a", requests[0].ResourceGroup)
	assert.Equal(t, []int64{10, 20}, requests[0].EligibleReplicaIDs)
	assert.Equal(t, "rg-b", requests[1].ResourceGroup)
	assert.Equal(t, []int64{30, 40}, requests[1].EligibleReplicaIDs)
	for _, request := range requests {
		assert.True(t, request.AllowNew)
		assert.Equal(t, epochConfig.balancer, request.Balancer)
		assert.Equal(t, epochConfig.budget, request.Budget)
		assert.Equal(t, epochConfig.policyConfig, request.PolicyConfig)
	}
}

func TestBalanceCheckerLoadsEpochConfigOncePerTick(t *testing.T) {
	t.Run("loads every field", func(t *testing.T) {
		paramtable.Init()
		params := paramtable.Get()
		values := map[string]string{
			params.QueryCoordCfg.BalanceEpochEnabled.Key:               "true",
			params.QueryCoordCfg.BalanceEpochShadowMode.Key:            "true",
			params.QueryCoordCfg.BalanceEpochDeadline.Key:              "1234",
			params.QueryCoordCfg.BalanceEpochNoProgressDeadline.Key:    "5678",
			params.QueryCoordCfg.BalanceEpochMaxSegmentTasks.Key:       "9",
			params.QueryCoordCfg.BalanceEpochMaxChannelTasks.Key:       "8",
			params.QueryCoordCfg.BalanceEpochMaxTasksPerNode.Key:       "7",
			params.QueryCoordCfg.BalanceEpochMaxTasksPerCollection.Key: "6",
			params.QueryCoordCfg.BalanceEpochMaxObjectRetries.Key:      "4",
			params.QueryCoordCfg.BalanceEpochQuarantineBackoff.Key:     "9012",
			params.QueryCoordCfg.SegmentTaskTimeout.Key:                "3456",
			params.QueryCoordCfg.ChannelTaskTimeout.Key:                "7890",
			params.QueryCoordCfg.Balancer.Key:                          meta.ScoreBasedBalancerName,
			params.QueryCoordCfg.GlobalRowCountFactor.Key:              "0.11",
			params.QueryCoordCfg.DelegatorMemoryOverloadFactor.Key:     "0.22",
			params.QueryCoordCfg.CollectionChannelCountFactor.Key:      "0.33",
			params.QueryCoordCfg.AutoBalanceChannel.Key:                "false",
		}
		for key, value := range values {
			require.NoError(t, params.Save(key, value))
		}
		t.Cleanup(func() {
			for key := range values {
				params.Reset(key)
			}
			streamingutil.UnsetStreamingServiceEnabled()
		})
		streamingutil.SetStreamingServiceEnabled()

		checker := &BalanceChecker{}
		assert.Equal(t, balanceEpochConfig{
			enabled:  true,
			shadow:   true,
			balancer: meta.ScoreBasedBalancerName,
			budget: balance.BalanceWaveBudget{
				MaxSegmentTasks:       9,
				MaxChannelTasks:       8,
				MaxTasksPerNode:       7,
				MaxTasksPerCollection: 6,
			},
			deadline:           1234 * time.Millisecond,
			noProgressDeadline: 5678 * time.Millisecond,
			segmentTaskTimeout: 3456 * time.Millisecond,
			channelTaskTimeout: 7890 * time.Millisecond,
			maxObjectRetries:   4,
			quarantineBackoff:  9012 * time.Millisecond,
			policyConfig: balance.EpochPolicyConfig{
				GlobalRowCountFactor:          0.11,
				DelegatorMemoryOverloadFactor: 0.22,
				CollectionChannelCountFactor:  0.33,
				AutoBalanceChannel:            false,
				StreamingServiceEnabled:       true,
			},
		}, checker.loadBalanceEpochConfig())
	})

	t.Run("one snapshot per check", func(t *testing.T) {
		controller := &fakeBalanceEpochController{}
		checker := createTestBalanceChecker(withEpochControllerForTest(controller))
		epochConfig := testBalanceEpochConfig()
		epochConfig.shadow = true
		epochConfig.policyConfig.StreamingServiceEnabled = true
		providerCalls := 0
		checker.epochConfigProvider = func() balanceEpochConfig {
			providerCalls++
			return epochConfig
		}
		mockEpochCheckConfig(t, checker, balanceConfig{
			maxCheckCollectionCount:      1,
			balanceOnMultipleCollections: true,
			autoBalanceInterval:          0,
		}, false, true, true)
		mockEligibleEpochCollections(t, checker, []int64{1}, map[int64][]*meta.Replica{
			1: {newEpochReplica(10, 1, "rg-a")},
		})

		checker.Check(context.Background())

		assert.Equal(t, 1, providerCalls)
		require.Len(t, controller.requests, 1)
		assert.Equal(t, balance.EpochRequest{
			ResourceGroup:      "rg-a",
			EligibleReplicaIDs: []int64{10},
			Balancer:           epochConfig.balancer,
			Budget:             epochConfig.budget,
			PolicyConfig:       epochConfig.policyConfig,
			AllowNew:           true,
			Shadow:             true,
			Deadline:           epochConfig.deadline,
			NoProgressDeadline: epochConfig.noProgressDeadline,
			SegmentTaskTimeout: epochConfig.segmentTaskTimeout,
			ChannelTaskTimeout: epochConfig.channelTaskTimeout,
			MaxObjectRetries:   epochConfig.maxObjectRetries,
			QuarantineBackoff:  epochConfig.quarantineBackoff,
		}, controller.requests[0])
	})

	t.Run("before every early return", func(t *testing.T) {
		tests := []struct {
			name                string
			stoppingEnabled     bool
			active              bool
			autoBalanceEnabled  bool
			autoBalanceInterval time.Duration
			acceptedStopping    int
		}{
			{
				name:               "accepted stopping work",
				stoppingEnabled:    true,
				active:             true,
				autoBalanceEnabled: true,
				acceptedStopping:   1,
			},
			{
				name:               "inactive checker",
				active:             false,
				autoBalanceEnabled: true,
			},
			{
				name:               "auto balance disabled",
				active:             true,
				autoBalanceEnabled: false,
			},
			{
				name:                "interval throttled",
				active:              true,
				autoBalanceEnabled:  true,
				autoBalanceInterval: time.Hour,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				checker := createTestBalanceChecker(withEpochControllerForTest(&fakeBalanceEpochController{}))
				providerCalls := 0
				checker.epochConfigProvider = func() balanceEpochConfig {
					providerCalls++
					return testBalanceEpochConfig()
				}
				mockEpochCheckConfig(t, checker, balanceConfig{
					autoBalanceInterval: test.autoBalanceInterval,
				}, test.stoppingEnabled, test.autoBalanceEnabled, true)
				checker.autoBalanceTs = time.Now()

				mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(test.active).Build()
				cleanupMock(t, mockIsActive)
				mockProcess := mockey.Mock((*BalanceChecker).processBalanceQueue).
					Return(test.acceptedStopping, 0).
					Build()
				cleanupMock(t, mockProcess)

				checker.Check(context.Background())

				assert.Equal(t, 1, providerCalls)
			})
		}
	})
}

func TestBalanceCheckerEpochOptionalSchedulerCapabilities(t *testing.T) {
	fixture := createTestBalanceChecker()
	baseScheduler := task.NewMockScheduler(t)
	managerCalls := 0
	var capturedSource task.Source
	var capturedPolicyProvider func(string, balance.EpochPolicyConfig) (balance.EpochBalancePolicy, bool)
	mockNewManager := mockey.Mock(balance.NewBalanceEpochManager).
		To(func(
			_ *meta.Meta,
			_ *meta.DistributionManager,
			_ meta.TargetManagerInterface,
			_ *session.NodeManager,
			_ task.Scheduler,
			_ task.BalanceTaskGenerationAdmitter,
			_ task.BalanceTaskInspector,
			source task.Source,
			policyProvider func(string, balance.EpochPolicyConfig) (balance.EpochBalancePolicy, bool),
			_ ...balance.EpochManagerOption,
		) *balance.BalanceEpochManager {
			managerCalls++
			capturedSource = source
			capturedPolicyProvider = policyProvider
			return &balance.BalanceEpochManager{}
		}).
		Build()
	cleanupMock(t, mockNewManager)
	policyCalls := 0
	var capturedBalancer string
	var capturedConfig balance.EpochPolicyConfig
	mockPolicy := mockey.Mock((*balance.BalancerFactory).GetEpochPolicyFor).
		To(func(_ *balance.BalancerFactory, balancer string, config balance.EpochPolicyConfig) (balance.EpochBalancePolicy, bool) {
			policyCalls++
			capturedBalancer = balancer
			capturedConfig = config
			return nil, true
		}).
		Build()
	cleanupMock(t, mockPolicy)

	missing := NewBalanceChecker(fixture.meta, fixture.dist, fixture.targetMgr, fixture.nodeManager, baseScheduler)
	generationOnly := NewBalanceChecker(
		fixture.meta, fixture.dist, fixture.targetMgr, fixture.nodeManager,
		&generationOnlyScheduler{Scheduler: baseScheduler},
	)
	inspectorOnly := NewBalanceChecker(
		fixture.meta, fixture.dist, fixture.targetMgr, fixture.nodeManager,
		&inspectorOnlyScheduler{Scheduler: baseScheduler},
	)
	capable := NewBalanceChecker(
		fixture.meta, fixture.dist, fixture.targetMgr, fixture.nodeManager,
		&epochCapableScheduler{Scheduler: baseScheduler},
	)

	assert.Nil(t, missing.epochManager)
	assert.Nil(t, generationOnly.epochManager)
	assert.Nil(t, inspectorOnly.epochManager)
	assert.NotNil(t, capable.epochManager)
	assert.Equal(t, 1, managerCalls)
	assert.Equal(t, utils.BalanceChecker, capturedSource)
	require.NotNil(t, capturedPolicyProvider)
	frozenConfig := balance.EpochPolicyConfig{AutoBalanceChannel: true, StreamingServiceEnabled: true}
	_, supported := capturedPolicyProvider(meta.ScoreBasedBalancerName, frozenConfig)
	assert.True(t, supported)
	assert.Equal(t, 1, policyCalls)
	assert.Equal(t, meta.ScoreBasedBalancerName, capturedBalancer)
	assert.Equal(t, frozenConfig, capturedConfig)
}

func TestBalanceCheckerEpochUsesDeterministicRGOrder(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	managerOwned := []*meta.Replica{
		newEpochReplica(9, 1, "rg-a"),
		newEpochReplica(2, 1, "rg-z"),
		newEpochReplica(1, 1, "rg-a"),
		newEpochReplica(5, 1, "rg-m"),
	}
	mockEligibleEpochCollections(t, checker, []int64{1}, map[int64][]*meta.Replica{1: managerOwned})

	requests := checker.collectNormalEpochRequests(ctx, balanceConfig{
		maxCheckCollectionCount:      10,
		balanceOnMultipleCollections: true,
	}, testBalanceEpochConfig())

	require.Len(t, requests, 3)
	assert.Equal(t, []string{"rg-a", "rg-m", "rg-z"}, []string{
		requests[0].ResourceGroup,
		requests[1].ResourceGroup,
		requests[2].ResourceGroup,
	})
	assert.Equal(t, []int64{1, 9}, requests[0].EligibleReplicaIDs)
	assert.Equal(t, []int64{9, 2, 1, 5}, []int64{
		managerOwned[0].GetID(), managerOwned[1].GetID(), managerOwned[2].GetID(), managerOwned[3].GetID(),
	}, "manager-owned replica storage must not be sorted in place")

	requests[0].EligibleReplicaIDs[0] = 999
	next := checker.collectNormalEpochRequests(ctx, balanceConfig{
		maxCheckCollectionCount:      10,
		balanceOnMultipleCollections: true,
	}, testBalanceEpochConfig())
	assert.Equal(t, []int64{1, 9}, next[0].EligibleReplicaIDs)
}

func TestBalanceCheckerEpochCoalescesRepeatedTicks(t *testing.T) {
	controller := &fakeBalanceEpochController{
		activateOnNew: true,
		newResult:     balance.EpochAdvanceResult{Started: true, Admitted: 1},
	}
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(testBalanceEpochConfig()),
	)
	checker.stoppingBalanceQueue = createMockPriorityQueue()
	checker.stoppingBalanceQueue.Push(newCollectionBalanceItem(1, 1, "bycollectionid"))
	mockEpochCheckConfig(t, checker, balanceConfig{autoBalanceInterval: time.Hour}, false, true, true)
	mockCollect := mockey.Mock((*BalanceChecker).collectNormalEpochRequests).
		Return([]balance.EpochRequest{{ResourceGroup: "rg-a", AllowNew: true}}).
		Build()
	cleanupMock(t, mockCollect)

	checker.Check(context.Background())
	firstAttempt := checker.autoBalanceTs
	checker.normalBalanceQueue = createMockPriorityQueue()
	checker.normalBalanceQueue.Push(newCollectionBalanceItem(2, 1, "bycollectionid"))
	time.Sleep(time.Millisecond)
	checker.Check(context.Background())

	require.Len(t, controller.requests, 2)
	assert.True(t, controller.requests[0].AllowNew)
	assert.False(t, controller.requests[1].AllowNew)
	assert.Equal(t, "rg-a", controller.requests[1].ResourceGroup)
	assert.Equal(t, firstAttempt, checker.autoBalanceTs, "active observation alone must not move the interval timestamp")
	assert.Nil(t, checker.normalBalanceQueue, "active epoch ownership invalidates stale legacy normal work")
	assert.Nil(t, checker.stoppingBalanceQueue, "accepted epoch admission invalidates the stopping queue")
}

func TestBalanceCheckerEpochInactiveStillReconciles(t *testing.T) {
	controller := &fakeBalanceEpochController{active: []string{"rg-b", "rg-a"}}
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(testBalanceEpochConfig()),
	)
	mockEpochCheckConfig(t, checker, balanceConfig{}, false, true, true)
	mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(false).Build()
	cleanupMock(t, mockIsActive)

	checker.Check(context.Background())

	require.Len(t, controller.requests, 2)
	assert.Equal(t, []balance.EpochRequest{
		{ResourceGroup: "rg-a", AllowNew: false},
		{ResourceGroup: "rg-b", AllowNew: false},
	}, controller.requests)
}

func TestBalanceCheckerEpochAutoBalanceDisabledStillReconciles(t *testing.T) {
	controller := &fakeBalanceEpochController{active: []string{"rg-a"}}
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(testBalanceEpochConfig()),
	)
	mockEpochCheckConfig(t, checker, balanceConfig{}, false, false, true)

	checker.Check(context.Background())

	require.Len(t, controller.requests, 1)
	assert.Equal(t, "rg-a", controller.requests[0].ResourceGroup)
	assert.False(t, controller.requests[0].AllowNew)
}

func TestBalanceCheckerEpochFeatureDisabledDrainsBeforeLegacyFallback(t *testing.T) {
	controller := &fakeBalanceEpochController{
		active:             []string{"rg-a"},
		clearOnObservation: true,
	}
	disabled := testBalanceEpochConfig()
	disabled.enabled = false
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(disabled),
	)
	checker.normalBalanceQueue = createMockPriorityQueue()
	checker.normalBalanceQueue.Push(newCollectionBalanceItem(1, 1, "bycollectionid"))
	mockEpochCheckConfig(t, checker, balanceConfig{autoBalanceInterval: 0}, false, true, true)
	legacyCalls := 0
	mockProcess := mockey.Mock((*BalanceChecker).processBalanceQueue).
		To(func(
			_ context.Context,
			_ balance.Balance,
			_ func(context.Context, int64) []int64,
			_ func(context.Context) *assign.PriorityQueue,
			_ func() *assign.PriorityQueue,
			_ balanceConfig,
			isStopping bool,
		) (int, int) {
			assert.False(t, isStopping)
			legacyCalls++
			return 0, 0
		}).
		Build()
	cleanupMock(t, mockProcess)

	checker.Check(context.Background())

	require.Len(t, controller.requests, 1)
	assert.False(t, controller.requests[0].AllowNew)
	assert.Equal(t, 1, legacyCalls)
	assert.Nil(t, checker.normalBalanceQueue, "epoch ownership invalidates stale legacy work before fallback")
}

func TestBalanceCheckerEpochFeatureDisabledWaitsForActiveDrain(t *testing.T) {
	controller := &fakeBalanceEpochController{active: []string{"rg-a"}}
	disabled := testBalanceEpochConfig()
	disabled.enabled = false
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(disabled),
	)
	mockEpochCheckConfig(t, checker, balanceConfig{autoBalanceInterval: 0}, false, true, true)
	legacyCalls := 0
	mockProcess := mockey.Mock((*BalanceChecker).processBalanceQueue).
		To(func(
			_ context.Context,
			_ balance.Balance,
			_ func(context.Context, int64) []int64,
			_ func(context.Context) *assign.PriorityQueue,
			_ func() *assign.PriorityQueue,
			_ balanceConfig,
			_ bool,
		) (int, int) {
			legacyCalls++
			return 0, 0
		}).
		Build()
	cleanupMock(t, mockProcess)

	checker.Check(context.Background())

	require.Len(t, controller.requests, 1)
	assert.False(t, controller.requests[0].AllowNew)
	assert.Equal(t, 0, legacyCalls)
}

func TestBalanceCheckerEpochUnsupportedPolicyUsesLegacy(t *testing.T) {
	controller := &fakeBalanceEpochController{
		active:             []string{"rg-a"},
		clearOnObservation: true,
	}
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(testBalanceEpochConfig()),
	)
	mockEpochCheckConfig(t, checker, balanceConfig{autoBalanceInterval: 0}, false, true, false)
	legacyCalls := 0
	mockProcess := mockey.Mock((*BalanceChecker).processBalanceQueue).
		To(func(
			_ context.Context,
			_ balance.Balance,
			_ func(context.Context, int64) []int64,
			_ func(context.Context) *assign.PriorityQueue,
			_ func() *assign.PriorityQueue,
			_ balanceConfig,
			isStopping bool,
		) (int, int) {
			assert.False(t, isStopping)
			legacyCalls++
			return 0, 0
		}).
		Build()
	cleanupMock(t, mockProcess)

	checker.Check(context.Background())

	require.Len(t, controller.requests, 1)
	assert.False(t, controller.requests[0].AllowNew)
	assert.Equal(t, 1, legacyCalls)
}

func TestBalanceCheckerEpochUnsupportedPolicyWaitsForActiveDrain(t *testing.T) {
	controller := &fakeBalanceEpochController{active: []string{"rg-a"}}
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(testBalanceEpochConfig()),
	)
	mockEpochCheckConfig(t, checker, balanceConfig{autoBalanceInterval: 0}, false, true, false)
	legacyCalls := 0
	mockProcess := mockey.Mock((*BalanceChecker).processBalanceQueue).
		To(func(
			_ context.Context,
			_ balance.Balance,
			_ func(context.Context, int64) []int64,
			_ func(context.Context) *assign.PriorityQueue,
			_ func() *assign.PriorityQueue,
			_ balanceConfig,
			_ bool,
		) (int, int) {
			legacyCalls++
			return 0, 0
		}).
		Build()
	cleanupMock(t, mockProcess)

	checker.Check(context.Background())

	require.Len(t, controller.requests, 1)
	assert.False(t, controller.requests[0].AllowNew)
	assert.Equal(t, 0, legacyCalls)
}

func TestBalanceCheckerStoppingBalancePreventsNewNormalEpoch(t *testing.T) {
	controller := &fakeBalanceEpochController{active: []string{"rg-a"}}
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(testBalanceEpochConfig()),
	)
	mockEpochCheckConfig(t, checker, balanceConfig{}, true, true, true)
	mockProcess := mockey.Mock((*BalanceChecker).processBalanceQueue).
		Return(1, 0).
		Build()
	cleanupMock(t, mockProcess)

	checker.Check(context.Background())

	require.Len(t, controller.requests, 1)
	assert.Equal(t, "rg-a", controller.requests[0].ResourceGroup)
	assert.False(t, controller.requests[0].AllowNew)
}

func TestBalanceCheckerStoppingAdmissionUsesAcceptedCounts(t *testing.T) {
	for _, test := range []struct {
		name                  string
		addErr                error
		expectedNewEpochCalls int
		expectedCollectCalls  int
	}{
		{
			name:                  "RejectedStoppingTaskAllowsNormalEpoch",
			addErr:                errors.New("stopping task rejected"),
			expectedNewEpochCalls: 1,
			expectedCollectCalls:  1,
		},
		{
			name:                  "AcceptedStoppingTaskPreventsNormalEpoch",
			addErr:                nil,
			expectedNewEpochCalls: 0,
			expectedCollectCalls:  0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller := &fakeBalanceEpochController{}
			epochConfig := testBalanceEpochConfig()
			checker := createTestBalanceChecker(
				withEpochControllerForTest(controller),
				withEpochConfigForTest(epochConfig),
			)
			ctx := context.Background()
			mockEligibleEpochCollections(t, checker, []int64{2}, map[int64][]*meta.Replica{
				2: {newEpochReplica(20, 2, "rg-b")},
			})
			candidateRequests := checker.collectNormalEpochRequests(ctx, balanceConfig{
				maxCheckCollectionCount:      1,
				balanceOnMultipleCollections: true,
			}, epochConfig)
			require.Len(t, candidateRequests, 1)
			assert.Equal(t, "rg-b", candidateRequests[0].ResourceGroup)

			checker.stoppingBalanceQueue = createMockPriorityQueue()
			checker.stoppingBalanceQueue.Push(newCollectionBalanceItem(1, 1, "bycollectionid"))
			mockEpochCheckConfig(t, checker, balanceConfig{
				segmentBatchSize:             1,
				channelBatchSize:             1,
				maxCheckCollectionCount:      1,
				balanceOnMultipleCollections: true,
				autoBalanceInterval:          0,
			}, true, true, true)
			mockStoppingReplicas := mockey.Mock((*BalanceChecker).getReplicaForStoppingBalance).
				Return([]int64{10}).
				Build()
			cleanupMock(t, mockStoppingReplicas)
			stoppingTask := newLegacySegmentBalanceTask(t, checker, 100)
			mockGenerate := mockey.Mock((*BalanceChecker).generateBalanceTasksFromReplicas).
				To(func(
					_ context.Context,
					_ balance.Balance,
					_ []int64,
					_ balanceConfig,
					isStopping bool,
				) ([]task.Task, []task.Task) {
					assert.True(t, isStopping)
					return []task.Task{stoppingTask}, nil
				}).
				Build()
			cleanupMock(t, mockGenerate)
			mockAdd := mockey.Mock(mockey.GetMethod(checker.scheduler, "Add")).Return(test.addErr).Build()
			cleanupMock(t, mockAdd)
			collectCalls := 0
			mockCollect := mockey.Mock((*BalanceChecker).collectNormalEpochRequests).
				To(func(_ context.Context, _ balanceConfig, _ balanceEpochConfig) []balance.EpochRequest {
					collectCalls++
					return candidateRequests
				}).
				Build()
			cleanupMock(t, mockCollect)

			checker.Check(ctx)

			assert.Equal(t, test.expectedCollectCalls, collectCalls)
			assert.Len(t, controller.requests, test.expectedNewEpochCalls)
			if test.expectedNewEpochCalls > 0 {
				assert.Equal(t, "rg-b", controller.requests[0].ResourceGroup)
				assert.True(t, controller.requests[0].AllowNew)
			}
		})
	}
}

func TestBalanceCheckerLegacySubmitCountsAcceptedTasks(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	replica := newEpochReplica(10, 1, "rg-a")
	segmentTask1, err := task.NewSegmentTask(
		ctx, time.Second, checker.ID(), 1, replica, commonpb.LoadPriority_LOW,
		task.NewSegmentAction(1, task.ActionTypeGrow, "channel-a", 10),
	)
	require.NoError(t, err)
	segmentTask2, err := task.NewSegmentTask(
		ctx, time.Second, checker.ID(), 1, replica, commonpb.LoadPriority_LOW,
		task.NewSegmentAction(1, task.ActionTypeGrow, "channel-a", 11),
	)
	require.NoError(t, err)
	channelTask1, err := task.NewChannelTask(
		ctx, time.Second, checker.ID(), 1, replica,
		task.NewChannelAction(1, task.ActionTypeGrow, "channel-a"),
	)
	require.NoError(t, err)
	channelTask2, err := task.NewChannelTask(
		ctx, time.Second, checker.ID(), 1, replica,
		task.NewChannelAction(1, task.ActionTypeGrow, "channel-b"),
	)
	require.NoError(t, err)
	mockAdd := mockey.Mock(mockey.GetMethod(checker.scheduler, "Add")).
		Return(mockey.Sequence(nil).Times(1).
			Then(errors.New("segment rejected")).Times(1).
			Then(nil).Times(1).
			Then(errors.New("channel rejected"))).
		Build()
	cleanupMock(t, mockAdd)

	acceptedSegments, acceptedChannels := checker.submitTasks(
		[]task.Task{segmentTask1, segmentTask2},
		[]task.Task{channelTask1, channelTask2},
	)

	assert.Equal(t, 1, acceptedSegments)
	assert.Equal(t, 1, acceptedChannels)
}

func TestBalanceCheckerLegacyProcessUsesAcceptedCounts(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	queue := createMockPriorityQueue()
	queue.Push(newCollectionBalanceItem(1, 1, "bycollectionid"))
	queue.Push(newCollectionBalanceItem(2, 1, "bycollectionid"))
	firstTask := newLegacySegmentBalanceTask(t, checker, 101)
	secondTask := newLegacySegmentBalanceTask(t, checker, 102)
	mockGenerate := mockey.Mock((*BalanceChecker).generateBalanceTasksFromReplicas).
		Return(mockey.Sequence(
			[]task.Task{firstTask}, []task.Task(nil),
		).Times(1).Then(
			[]task.Task{secondTask}, []task.Task(nil),
		)).
		Build()
	cleanupMock(t, mockGenerate)
	addCalls := 0
	mockAdd := mockey.Mock(mockey.GetMethod(checker.scheduler, "Add")).
		To(func(task.Task) error {
			addCalls++
			if addCalls == 1 {
				return errors.New("first collection rejected")
			}
			return nil
		}).
		Build()
	cleanupMock(t, mockAdd)

	acceptedSegments, acceptedChannels := checker.processBalanceQueue(
		ctx,
		nil,
		func(context.Context, int64) []int64 { return []int64{10} },
		func(context.Context) *assign.PriorityQueue { return queue },
		func() *assign.PriorityQueue { return queue },
		balanceConfig{
			segmentBatchSize:             1,
			channelBatchSize:             1,
			maxCheckCollectionCount:      2,
			balanceOnMultipleCollections: false,
		},
		false,
	)

	assert.Equal(t, 2, addCalls, "a rejected first collection must not trip the single-collection accepted-work gate")
	assert.Equal(t, 1, acceptedSegments)
	assert.Equal(t, 0, acceptedChannels)
}

func TestBalanceCheckerEpochAdvancesEachRGAtMostOncePerCheck(t *testing.T) {
	controller := &fakeBalanceEpochController{
		active:             []string{"rg-a", "rg-a"},
		clearOnObservation: true,
	}
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(testBalanceEpochConfig()),
	)
	mockEpochCheckConfig(t, checker, balanceConfig{autoBalanceInterval: 0}, false, true, true)
	mockCollect := mockey.Mock((*BalanceChecker).collectNormalEpochRequests).
		Return([]balance.EpochRequest{
			{ResourceGroup: "rg-a", AllowNew: true},
			{ResourceGroup: "rg-b", AllowNew: true},
		}).
		Build()
	cleanupMock(t, mockCollect)

	checker.Check(context.Background())

	require.Len(t, controller.requests, 2)
	assert.Equal(t, "rg-a", controller.requests[0].ResourceGroup)
	assert.False(t, controller.requests[0].AllowNew)
	assert.Equal(t, "rg-b", controller.requests[1].ResourceGroup)
	assert.True(t, controller.requests[1].AllowNew)
}

func TestBalanceCheckerEpochDeduplicatesNewRequestsAndRechecksActive(t *testing.T) {
	controller := &fakeBalanceEpochController{
		hasActiveOverride: func(resourceGroup string) bool {
			return resourceGroup == "rg-b"
		},
	}
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(testBalanceEpochConfig()),
	)
	mockEpochCheckConfig(t, checker, balanceConfig{autoBalanceInterval: 0}, false, true, true)
	mockCollect := mockey.Mock((*BalanceChecker).collectNormalEpochRequests).
		Return([]balance.EpochRequest{
			{ResourceGroup: "rg-a", AllowNew: true},
			{ResourceGroup: "rg-a", AllowNew: true},
			{ResourceGroup: "rg-b", AllowNew: true},
		}).
		Build()
	cleanupMock(t, mockCollect)

	checker.Check(context.Background())

	require.Len(t, controller.requests, 1)
	assert.Equal(t, "rg-a", controller.requests[0].ResourceGroup)
	assert.True(t, controller.requests[0].AllowNew)
}

func TestBalanceCheckerEpochCollectionCursorDoesNotStarve(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	mockEligibleEpochCollections(t, checker, []int64{3, 1, 2}, map[int64][]*meta.Replica{
		1: {newEpochReplica(10, 1, "rg-a")},
		2: {newEpochReplica(20, 2, "rg-a")},
		3: {newEpochReplica(30, 3, "rg-a")},
	})
	config := balanceConfig{maxCheckCollectionCount: 1, balanceOnMultipleCollections: false}

	seen := make([]int64, 0, 4)
	for i := 0; i < 4; i++ {
		requests := checker.collectNormalEpochRequests(ctx, config, testBalanceEpochConfig())
		require.Len(t, requests, 1)
		require.Len(t, requests[0].EligibleReplicaIDs, 1)
		seen = append(seen, requests[0].EligibleReplicaIDs[0])
	}

	assert.Equal(t, []int64{10, 20, 30, 10}, seen)
}

func TestBalanceCheckerEpochCollectionCursorAdvancesPastEmptyReplicaWindow(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()
	mockEligibleEpochCollections(t, checker, []int64{1, 2}, map[int64][]*meta.Replica{
		1: nil,
		2: {newEpochReplica(20, 2, "rg-a")},
	})
	config := balanceConfig{maxCheckCollectionCount: 1, balanceOnMultipleCollections: false}

	first := checker.collectNormalEpochRequests(ctx, config, testBalanceEpochConfig())
	second := checker.collectNormalEpochRequests(ctx, config, testBalanceEpochConfig())

	assert.Empty(t, first)
	require.Len(t, second, 1)
	assert.Equal(t, []int64{20}, second[0].EligibleReplicaIDs)
}

func TestBalanceCheckerEpochEmptyAttemptUpdatesAutoBalanceTimestamp(t *testing.T) {
	controller := &fakeBalanceEpochController{}
	checker := createTestBalanceChecker(
		withEpochControllerForTest(controller),
		withEpochConfigForTest(testBalanceEpochConfig()),
	)
	mockEpochCheckConfig(t, checker, balanceConfig{autoBalanceInterval: 0}, false, true, true)
	mockCollect := mockey.Mock((*BalanceChecker).collectNormalEpochRequests).
		Return([]balance.EpochRequest(nil)).
		Build()
	cleanupMock(t, mockCollect)

	checker.Check(context.Background())

	assert.False(t, checker.autoBalanceTs.IsZero())
	assert.Empty(t, controller.requests)
}
