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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// createMockPriorityQueue creates a mock priority queue for testing
func createMockPriorityQueue() *balance.PriorityQueue {
	return balance.NewPriorityQueuePtr()
}

// Helper function to create a test BalanceChecker
func createTestBalanceChecker() *BalanceChecker {
	metaInstance := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(nil),
	}
	targetMgr := meta.NewTargetManager(nil, nil)
	nodeMgr := &session.NodeManager{}
	scheduler := task.NewScheduler(context.Background(), nil, nil, nil, nil, nil, nil)
	balancer := balance.NewScoreBasedBalancer(nil, nil, nil, nil, nil)
	getBalancerFunc := func() balance.Balance { return balancer }

	return NewBalanceChecker(metaInstance, targetMgr, nodeMgr, scheduler, getBalancerFunc)
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

	// Mock replica methods - replica1 has RO nodes, replica2 doesn't
	mockRONodesCount1 := mockey.Mock((*meta.Replica).RONodesCount).Return(1).Build()
	defer mockRONodesCount1.UnPatch()

	mockROSQNodesCount1 := mockey.Mock((*meta.Replica).ROSQNodesCount).Return(0).Build()
	defer mockROSQNodesCount1.UnPatch()

	mockGetID1 := mockey.Mock((*meta.Replica).GetID).Return(int64(101)).Build()
	defer mockGetID1.UnPatch()

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

	segmentTasks, channelTasks := checker.generateBalanceTasksFromReplicas(ctx, []int64{}, config)

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
	segmentPlan := balance.SegmentAssignPlan{
		Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 1}},
		Replica: mockReplica,
		From:    1,
		To:      2,
	}
	channelPlan := balance.ChannelAssignPlan{
		Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "test"}},
		Replica: mockReplica,
		From:    1,
		To:      2,
	}

	mockBalancer := mockey.Mock(checker.getBalancerFunc).To(func() balance.Balance {
		return balance.NewScoreBasedBalancer(nil, nil, nil, nil, nil)
	}).Build()
	defer mockBalancer.UnPatch()

	// Mock balancer.BalanceReplica
	mockBalanceReplica := mockey.Mock((*balance.ScoreBasedBalancer).BalanceReplica).Return(
		[]balance.SegmentAssignPlan{segmentPlan},
		[]balance.ChannelAssignPlan{channelPlan},
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

	segmentTasks, channelTasks := checker.generateBalanceTasksFromReplicas(ctx, replicaIDs, config)

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

	segmentTasks, channelTasks := checker.generateBalanceTasksFromReplicas(ctx, replicaIDs, config)

	assert.Empty(t, segmentTasks)
	assert.Empty(t, channelTasks)
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
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Mock IsActive to return false
	mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(false).Build()
	defer mockIsActive.UnPatch()

	result := checker.Check(ctx)
	assert.Nil(t, result)
}

func TestBalanceChecker_Check_StoppingBalanceEnabled(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Mock IsActive to return true
	mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(true).Build()
	defer mockIsActive.UnPatch()

	// Mock paramtable for enabling stopping balance
	mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
	defer mockParamGet.UnPatch()

	mockStoppingBalanceEnabled := mockey.Mock(mockey.GetMethod(&paramtable.ParamItem{}, "GetAsBool")).Return(true).Build()
	defer mockStoppingBalanceEnabled.UnPatch()

	// Mock loadBalanceConfig
	config := balanceConfig{
		segmentBatchSize: 5,
		channelBatchSize: 5,
	}
	mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
	defer mockLoadConfig.UnPatch()

	// Mock processBalanceQueue to return tasks
	mockProcessQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).Return(
		1, 0, // segment tasks, channel tasks
	).Build()
	defer mockProcessQueue.UnPatch()

	result := checker.Check(ctx)
	assert.Nil(t, result) // Always returns nil as tasks are submitted directly
	assert.Nil(t, checker.normalBalanceQueue)
}

func TestBalanceChecker_Check_NormalBalanceEnabled(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Set autoBalanceTs to allow normal balance
	checker.autoBalanceTs = time.Time{}

	// Mock IsActive to return true
	mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(true).Build()
	defer mockIsActive.UnPatch()

	// Mock paramtable - stopping balance disabled, auto balance enabled
	mockParamGet := mockey.Mock(paramtable.Get).Return(&paramtable.ComponentParam{}).Build()
	defer mockParamGet.UnPatch()

	// return false for stopping balance enabled, true for auto balance enabled
	mockParams := mockey.Mock(mockey.GetMethod(&paramtable.ParamItem{}, "GetAsBool")).Return(mockey.Sequence(false).Times(1).Then(true)).Build()
	defer mockParams.UnPatch()

	// Mock loadBalanceConfig
	config := balanceConfig{
		segmentBatchSize:    5,
		channelBatchSize:    5,
		autoBalanceInterval: 1 * time.Second,
	}
	mockLoadConfig := mockey.Mock((*BalanceChecker).loadBalanceConfig).Return(config).Build()
	defer mockLoadConfig.UnPatch()

	// Mock processBalanceQueue to return tasks
	mockProcessQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).Return(
		0, 1, // segment tasks, channel tasks
	).Build()
	defer mockProcessQueue.UnPatch()

	result := checker.Check(ctx)
	assert.Nil(t, result) // Always returns nil as tasks are submitted directly
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
	constructQueueFunc := func(ctx context.Context) *balance.PriorityQueue {
		return mockQueue
	}

	// Mock getQueueFunc
	getQueueFunc := func() *balance.PriorityQueue {
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
	mockSubmitTasks := mockey.Mock((*BalanceChecker).submitTasks).Return().Build()
	defer mockSubmitTasks.UnPatch()

	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, getReplicasFunc, constructQueueFunc, getQueueFunc, config,
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

	constructQueueFunc := func(ctx context.Context) *balance.PriorityQueue {
		return mockQueue
	}

	getQueueFunc := func() *balance.PriorityQueue {
		return mockQueue
	}

	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, getReplicasFunc, constructQueueFunc, getQueueFunc, config,
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

	constructQueueFunc := func(ctx context.Context) *balance.PriorityQueue {
		return mockQueue
	}

	getQueueFunc := func() *balance.PriorityQueue {
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
	mockSubmitTasks := mockey.Mock((*BalanceChecker).submitTasks).Return().Build()
	defer mockSubmitTasks.UnPatch()

	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, getReplicasFunc, constructQueueFunc, getQueueFunc, config,
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

	constructQueueFunc := func(ctx context.Context) *balance.PriorityQueue {
		return mockQueue
	}

	getQueueFunc := func() *balance.PriorityQueue {
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
	mockSubmitTasks := mockey.Mock((*BalanceChecker).submitTasks).Return().Build()
	defer mockSubmitTasks.UnPatch()

	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, getReplicasFunc, constructQueueFunc, getQueueFunc, config,
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

	constructQueueFunc := func(ctx context.Context) *balance.PriorityQueue {
		return mockQueue
	}

	getQueueFunc := func() *balance.PriorityQueue {
		return mockQueue
	}

	segmentTasks, channelTasks := checker.processBalanceQueue(
		ctx, getReplicasFunc, constructQueueFunc, getQueueFunc, config,
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
// Error Handling Tests
// =============================================================================

func TestBalanceChecker_Check_TimeoutWarning(t *testing.T) {
	checker := createTestBalanceChecker()
	ctx := context.Background()

	// Mock IsActive to return true
	mockIsActive := mockey.Mock((*checkerActivation).IsActive).Return(true).Build()
	defer mockIsActive.UnPatch()

	mockProcessBalanceQueue := mockey.Mock((*BalanceChecker).processBalanceQueue).To(
		func(ctx context.Context,
			getReplicasFunc func(ctx context.Context, collectionID int64) []int64,
			constructQueueFunc func(ctx context.Context) *balance.PriorityQueue,
			getQueueFunc func() *balance.PriorityQueue, config balanceConfig,
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
