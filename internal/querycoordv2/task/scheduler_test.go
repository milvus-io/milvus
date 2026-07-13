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

package task

import (
	"context"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

const balanceAdmissionShard = "balance-admission-shard"

var _ BalanceTaskAdmitter = (*taskScheduler)(nil)
var _ BalanceTaskInspector = (*taskScheduler)(nil)

func (suite *TaskSuite) newSegmentMoveTask(segmentID, sourceNode, targetNode int64) *SegmentTask {
	task, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(targetNode, ActionTypeGrow, balanceAdmissionShard, segmentID, querypb.DataScope_Historical, 100),
		NewSegmentActionWithScope(sourceNode, ActionTypeReduce, balanceAdmissionShard, segmentID, querypb.DataScope_Historical, 100),
	)
	suite.Require().NoError(err)
	return task
}

func (suite *TaskSuite) setSegmentMovePrerequisites(segmentID, sourceNode int64) {
	suite.dist.ChannelDistManager.Update(
		sourceNode,
		utils.CreateTestChannel(suite.collection, sourceNode, 1, balanceAdmissionShard),
	)
	suite.dist.SegmentDistManager.Update(
		sourceNode,
		utils.CreateTestSegment(suite.collection, 1, segmentID, sourceNode, 1, balanceAdmissionShard),
	)
}

func (suite *TaskSuite) assertBalanceTaskNotVisible(task Task) {
	suite.Zero(task.ID())
	suite.Zero(suite.scheduler.waitQueue.Len())
	suite.Zero(suite.scheduler.processQueue.Len())
	suite.Zero(suite.scheduler.tasks.Len())
	suite.Zero(suite.scheduler.segmentTasks.Len())
	suite.Zero(suite.scheduler.channelTasks.Len())
	suite.Zero(suite.scheduler.taskStats.Len())
	suite.True(suite.scheduler.lastUpdateMetricTime.Load().IsZero())
	suite.Empty(suite.scheduler.segmentTaskDelta.records)
	suite.Empty(suite.scheduler.channelTaskDelta.records)
}

func (suite *TaskSuite) assertBalanceTaskNotRegistered(task Task, expectedErr error) {
	suite.assertBalanceTaskNotVisible(task)
	suite.Equal(TaskStatusCanceled, task.Status())
	suite.Equal(expectedErr, task.Err())
	select {
	case <-task.Done():
	default:
		suite.Fail("rejected balance task did not signal completion")
	}
}

func (suite *TaskSuite) TestAdmitBalanceTaskAccepted() {
	segmentID := int64(1000)
	sourceNode := int64(1)
	targetNode := int64(2)
	moveGauge := metrics.QueryCoordTaskNum.WithLabelValues(metrics.SegmentMoveTaskLabel)
	previousGaugeValue := testutil.ToFloat64(moveGauge)
	suite.T().Cleanup(func() {
		moveGauge.Set(previousGaugeValue)
	})
	suite.setSegmentMovePrerequisites(segmentID, sourceNode)
	task := suite.newSegmentMoveTask(segmentID, sourceNode, targetNode)

	validationCount := 0
	result := suite.scheduler.AdmitBalanceTask(task, func() BalanceAdmissionReason {
		validationCount++
		return BalanceAdmissionAccepted
	})

	suite.Equal(BalanceAdmissionAccepted, result.Reason)
	suite.Equal(task.ID(), result.TaskID)
	suite.NotZero(result.TaskID)
	suite.NoError(result.Err)
	suite.Equal(2, validationCount)
	suite.Equal(1, suite.scheduler.waitQueue.Len())
	suite.Equal(1, suite.scheduler.tasks.Len())
	suite.Equal(1, suite.scheduler.segmentTasks.Len())
	suite.Equal(1, suite.scheduler.taskStats.Len())
	suite.False(suite.scheduler.lastUpdateMetricTime.Load().IsZero())
	suite.Len(suite.scheduler.segmentTaskDelta.records, 1)
	suite.Equal(float64(1), testutil.ToFloat64(moveGauge))
	snapshot := suite.scheduler.GetSegmentTaskDeltaSnapshot([]int64{sourceNode, targetNode}, suite.collection)
	suite.Equal(-100, snapshot.GetByNodeInCollection(sourceNode))
	suite.Equal(100, snapshot.GetByNodeInCollection(targetNode))
}

func (suite *TaskSuite) TestAdmitBalanceTaskReturnsTypedDuplicate() {
	segmentID := int64(1001)
	sourceNode := int64(1)
	suite.setSegmentMovePrerequisites(segmentID, sourceNode)
	first := suite.newSegmentMoveTask(segmentID, sourceNode, 2)
	suite.Require().NoError(suite.scheduler.Add(first))

	duplicate := suite.newSegmentMoveTask(segmentID, sourceNode, 3)
	validationCount := 0
	result := suite.scheduler.AdmitBalanceTask(duplicate, func() BalanceAdmissionReason {
		validationCount++
		return BalanceAdmissionAccepted
	})

	suite.Equal(BalanceAdmissionDuplicate, result.Reason)
	suite.Zero(result.TaskID)
	suite.NotNil(result.Err)
	suite.Equal(1, validationCount)
	suite.Zero(duplicate.ID())
	suite.Equal(TaskStatusCanceled, duplicate.Status())
	suite.Equal(1, suite.scheduler.waitQueue.Len())
	suite.Equal(1, suite.scheduler.tasks.Len())
	suite.Equal(1, suite.scheduler.segmentTasks.Len())
}

func (suite *TaskSuite) TestAdmitBalanceTaskReturnsTypedLeaderMissing() {
	segmentID := int64(1002)
	sourceNode := int64(1)
	suite.dist.SegmentDistManager.Update(
		sourceNode,
		utils.CreateTestSegment(suite.collection, 1, segmentID, sourceNode, 1, balanceAdmissionShard),
	)
	task := suite.newSegmentMoveTask(segmentID, sourceNode, 2)

	result := suite.scheduler.AdmitBalanceTask(task, func() BalanceAdmissionReason {
		return BalanceAdmissionAccepted
	})

	suite.Equal(BalanceAdmissionLeaderMissing, result.Reason)
	suite.Zero(result.TaskID)
	suite.assertBalanceTaskNotRegistered(task, result.Err)
}

func (suite *TaskSuite) TestAdmitBalanceTaskReturnsTypedSourceGone() {
	segmentID := int64(1003)
	sourceNode := int64(1)
	suite.dist.ChannelDistManager.Update(
		sourceNode,
		utils.CreateTestChannel(suite.collection, sourceNode, 1, balanceAdmissionShard),
	)
	task := suite.newSegmentMoveTask(segmentID, sourceNode, 2)

	result := suite.scheduler.AdmitBalanceTask(task, func() BalanceAdmissionReason {
		return BalanceAdmissionAccepted
	})

	suite.Equal(BalanceAdmissionSourceGone, result.Reason)
	suite.Zero(result.TaskID)
	suite.assertBalanceTaskNotRegistered(task, result.Err)
}

func (suite *TaskSuite) TestAdmitBalanceTaskRejectsBeforeRegistration() {
	task := suite.newSegmentMoveTask(1004, 1, 2)
	validationCount := 0

	result := suite.scheduler.AdmitBalanceTask(task, func() BalanceAdmissionReason {
		validationCount++
		return BalanceAdmissionReplicaChanged
	})

	suite.Equal(BalanceAdmissionReplicaChanged, result.Reason)
	suite.Zero(result.TaskID)
	suite.NotNil(result.Err)
	suite.Equal(1, validationCount)
	suite.assertBalanceTaskNotRegistered(task, result.Err)
}

func (suite *TaskSuite) TestAdmitBalanceTaskRejectsAtFinalHeldCheck() {
	segmentID := int64(1005)
	sourceNode := int64(1)
	suite.setSegmentMovePrerequisites(segmentID, sourceNode)
	task := suite.newSegmentMoveTask(segmentID, sourceNode, 2)
	allocatedIDs := 0
	suite.scheduler.idAllocator = func() int64 {
		allocatedIDs++
		return 1
	}
	moveGauge := metrics.QueryCoordTaskNum.WithLabelValues(metrics.SegmentMoveTaskLabel)
	previousGaugeValue := testutil.ToFloat64(moveGauge)
	moveGauge.Set(73)
	suite.T().Cleanup(func() {
		moveGauge.Set(previousGaugeValue)
	})
	validationCount := 0

	result := suite.scheduler.AdmitBalanceTask(task, func() BalanceAdmissionReason {
		validationCount++
		if validationCount == 1 {
			return BalanceAdmissionAccepted
		}
		suite.assertBalanceTaskNotVisible(task)
		suite.Equal(float64(73), testutil.ToFloat64(moveGauge))
		return BalanceAdmissionRGChanged
	})

	suite.Equal(BalanceAdmissionRGChanged, result.Reason)
	suite.Zero(result.TaskID)
	suite.NotNil(result.Err)
	suite.Equal(2, validationCount)
	suite.Zero(allocatedIDs)
	suite.Equal(float64(73), testutil.ToFloat64(moveGauge))
	suite.assertBalanceTaskNotRegistered(task, result.Err)
}

func (suite *TaskSuite) TestAdmitBalanceTaskFinalCheckKeepsLowerPriorityTask() {
	segmentID := int64(1006)
	sourceNode := int64(1)
	suite.setSegmentMovePrerequisites(segmentID, sourceNode)
	oldTask := suite.newSegmentMoveTask(segmentID, sourceNode, 2)
	oldTask.SetPriority(TaskPriorityNormal)
	suite.Require().NoError(suite.scheduler.Add(oldTask))
	metricUpdateTime := suite.scheduler.lastUpdateMetricTime.Load()

	newTask := suite.newSegmentMoveTask(segmentID, sourceNode, 3)
	newTask.SetPriority(TaskPriorityHigh)
	validationCount := 0
	result := suite.scheduler.AdmitBalanceTask(newTask, func() BalanceAdmissionReason {
		validationCount++
		if validationCount == 1 {
			return BalanceAdmissionAccepted
		}
		return BalanceAdmissionRGChanged
	})

	suite.Equal(BalanceAdmissionRGChanged, result.Reason)
	suite.Zero(result.TaskID)
	suite.Equal(2, validationCount)
	suite.Zero(newTask.ID())
	suite.Equal(TaskStatusCanceled, newTask.Status())
	suite.Equal(result.Err, newTask.Err())
	suite.Equal(TaskStatusStarted, oldTask.Status())
	suite.NoError(oldTask.Err())
	suite.Equal(1, suite.scheduler.waitQueue.Len())
	suite.Equal(1, suite.scheduler.tasks.Len())
	registered, ok := suite.scheduler.segmentTasks.Get(NewReplicaSegmentIndex(oldTask))
	suite.True(ok)
	suite.Same(oldTask, registered)
	suite.Equal(1, suite.scheduler.taskStats.Len())
	suite.Equal(metricUpdateTime, suite.scheduler.lastUpdateMetricTime.Load())
	suite.Len(suite.scheduler.segmentTaskDelta.records, 1)
}

func (suite *TaskSuite) TestAddHigherPrioritySegmentMoveSkipsTopologyRevalidation() {
	segmentID := int64(1007)
	sourceNode := int64(1)
	suite.setSegmentMovePrerequisites(segmentID, sourceNode)
	oldTask := suite.newSegmentMoveTask(segmentID, sourceNode, 2)
	oldTask.SetPriority(TaskPriorityNormal)
	suite.Require().NoError(suite.scheduler.Add(oldTask))

	// Preserve Add's historical replacement behavior: once a lower-priority
	// duplicate is found, the higher-priority task replaces it without running
	// the later move-topology checks.
	suite.dist.ChannelDistManager.Update(sourceNode)
	newTask := suite.newSegmentMoveTask(segmentID, sourceNode, 3)
	newTask.SetPriority(TaskPriorityHigh)

	suite.Require().NoError(suite.scheduler.Add(newTask))

	suite.Equal(TaskStatusCanceled, oldTask.Status())
	suite.Equal(TaskStatusStarted, newTask.Status())
	suite.NotZero(newTask.ID())
	suite.Equal(1, suite.scheduler.waitQueue.Len())
	suite.Equal(1, suite.scheduler.tasks.Len())
	registered, ok := suite.scheduler.segmentTasks.Get(NewReplicaSegmentIndex(newTask))
	suite.True(ok)
	suite.Same(newTask, registered)
}

func (suite *TaskSuite) TestAddHigherPriorityChannelMoveSkipsTopologyRevalidation() {
	channelName := "balance-admission-channel"
	sourceNode := int64(1)
	suite.dist.ChannelDistManager.Update(
		sourceNode,
		suite.channel(suite.collection, channelName, sourceNode),
	)
	oldTask := suite.newChannelMoveTask(
		suite.collection,
		suite.replica.GetID(),
		suite.replica.GetResourceGroup(),
		channelName,
		sourceNode,
		2,
	)
	oldTask.SetPriority(TaskPriorityNormal)
	suite.Require().NoError(suite.scheduler.Add(oldTask))

	// The source disappears after the old task was admitted. A higher-priority
	// duplicate still follows Add's historical immediate-replacement path.
	suite.dist.ChannelDistManager.Update(sourceNode)
	newTask := suite.newChannelMoveTask(
		suite.collection,
		suite.replica.GetID(),
		suite.replica.GetResourceGroup(),
		channelName,
		sourceNode,
		3,
	)
	newTask.SetPriority(TaskPriorityHigh)

	suite.Require().NoError(suite.scheduler.Add(newTask))

	suite.Equal(TaskStatusCanceled, oldTask.Status())
	suite.Equal(TaskStatusStarted, newTask.Status())
	suite.NotZero(newTask.ID())
	suite.Equal(1, suite.scheduler.waitQueue.Len())
	suite.Equal(1, suite.scheduler.tasks.Len())
	registered, ok := suite.scheduler.channelTasks.Get(replicaChannelIndex{newTask.ReplicaID(), newTask.Channel()})
	suite.True(ok)
	suite.Same(newTask, registered)
}

func (suite *TaskSuite) TestBalanceEpochMetaDiagnostics() {
	task := suite.newSegmentMoveTask(1008, 1, 2)
	suite.NotContains(task.String(), "[balanceSequence=")
	meta := BalanceEpochMeta{
		ResourceGroup: "rg1",
		LeaderTerm:    7,
		Sequence:      11,
	}

	task.SetBalanceEpoch(meta)

	suite.Equal(meta, task.BalanceEpoch())
	text := task.String()
	suite.True(strings.Contains(text, "[balanceResourceGroup=rg1]"))
	suite.True(strings.Contains(text, "[balanceLeaderTerm=7]"))
	suite.True(strings.Contains(text, "[balanceSequence=11]"))
}

func (suite *TaskSuite) TestPendingBalanceTaskInspectorCopiesPrimitiveActionsAndRevision() {
	segmentID := int64(2001)
	segmentSource := int64(1)
	suite.setSegmentMovePrerequisites(segmentID, segmentSource)
	segmentTask := suite.newSegmentMoveTask(segmentID, segmentSource, 2)
	segmentTask.SetBalanceEpoch(BalanceEpochMeta{
		ResourceGroup: suite.replica.GetResourceGroup(),
		LeaderTerm:    7,
		Sequence:      9,
	})
	suite.Require().NoError(suite.scheduler.Add(segmentTask))

	channelName := "pending-inspector-channel"
	suite.dist.ChannelDistManager.Update(
		segmentSource,
		suite.channel(suite.collection, channelName, segmentSource),
	)
	channelTask := suite.newChannelMoveTask(
		suite.collection,
		suite.replica.GetID(),
		suite.replica.GetResourceGroup(),
		channelName,
		segmentSource,
		3,
	)
	suite.Require().NoError(suite.scheduler.Add(channelTask))

	snapshot := suite.scheduler.GetPendingBalanceTasks()
	suite.Positive(snapshot.Revision)
	suite.Len(snapshot.Tasks, 2)
	byID := make(map[int64]PendingBalanceTaskSnapshot, len(snapshot.Tasks))
	for _, pending := range snapshot.Tasks {
		byID[pending.TaskID] = pending
	}

	segmentPending := byID[segmentTask.ID()]
	suite.Equal(suite.collection, segmentPending.CollectionID)
	suite.Equal(suite.replica.GetID(), segmentPending.ReplicaID)
	suite.Equal(suite.replica.GetResourceGroup(), segmentPending.ResourceGroup)
	suite.Equal(segmentTask.BalanceEpoch(), segmentPending.Epoch)
	suite.Equal(TaskStatusStarted, segmentPending.Status)
	suite.Len(segmentPending.Actions, 2)
	suite.Equal(ActionTypeGrow, segmentPending.Actions[0].Type)
	suite.Equal(int64(2), segmentPending.Actions[0].NodeID)
	suite.Equal(segmentID, segmentPending.Actions[0].SegmentID)
	suite.Equal(balanceAdmissionShard, segmentPending.Actions[0].Shard)
	suite.Equal(querypb.DataScope_Historical, segmentPending.Actions[0].Scope)
	suite.Equal(100, segmentPending.Actions[0].Workload)

	channelPending := byID[channelTask.ID()]
	suite.Len(channelPending.Actions, 2)
	suite.Equal(ActionTypeGrow, channelPending.Actions[0].Type)
	suite.Equal(int64(3), channelPending.Actions[0].NodeID)
	suite.Equal(channelName, channelPending.Actions[0].Channel)
	suite.Equal(channelName, channelPending.Actions[0].Shard)
	suite.Equal(1, channelPending.Actions[0].Workload)

	snapshot.Tasks[0].Actions[0].NodeID = 999
	secondCopy := suite.scheduler.GetPendingBalanceTasks()
	for _, pending := range secondCopy.Tasks {
		for _, action := range pending.Actions {
			suite.NotEqual(int64(999), action.NodeID)
		}
	}

	revisionBeforeRemove := secondCopy.Revision
	suite.scheduler.remove(segmentTask)
	afterRemove := suite.scheduler.GetPendingBalanceTasks()
	suite.Greater(afterRemove.Revision, revisionBeforeRemove)
	suite.Len(afterRemove.Tasks, 1)
	suite.Equal(channelTask.ID(), afterRemove.Tasks[0].TaskID)
}

func (suite *TaskSuite) TestBalanceAdmissionReasonSemantics() {
	suite.Equal("accepted", BalanceAdmissionAccepted.String())
	suite.Equal("duplicate", BalanceAdmissionDuplicate.String())
	suite.Equal("source_gone", BalanceAdmissionSourceGone.String())
	suite.Equal("leader_missing", BalanceAdmissionLeaderMissing.String())
	suite.Equal("replica_changed", BalanceAdmissionReplicaChanged.String())
	suite.Equal("resource_group_changed", BalanceAdmissionRGChanged.String())
	suite.Equal("target_changed", BalanceAdmissionTargetChanged.String())
	suite.Equal("node_ineligible", BalanceAdmissionNodeIneligible.String())
	suite.Equal("budget_exhausted", BalanceAdmissionBudgetExhausted.String())
	suite.Equal("stale_epoch", BalanceAdmissionStaleEpoch.String())
	suite.Equal("internal_error", BalanceAdmissionInternalError.String())
	suite.Equal("internal_error", BalanceAdmissionReason(100).String())

	for _, reason := range []BalanceAdmissionReason{
		BalanceAdmissionReplicaChanged,
		BalanceAdmissionRGChanged,
		BalanceAdmissionTargetChanged,
		BalanceAdmissionNodeIneligible,
		BalanceAdmissionStaleEpoch,
	} {
		suite.True(reason.InvalidatesScope(), reason.String())
	}
	for _, reason := range []BalanceAdmissionReason{
		BalanceAdmissionAccepted,
		BalanceAdmissionDuplicate,
		BalanceAdmissionSourceGone,
		BalanceAdmissionLeaderMissing,
		BalanceAdmissionBudgetExhausted,
		BalanceAdmissionInternalError,
	} {
		suite.False(reason.InvalidatesScope(), reason.String())
	}
}
