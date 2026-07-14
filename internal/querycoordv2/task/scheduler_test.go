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
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const balanceAdmissionShard = "balance-admission-shard"

var _ BalanceTaskAdmitter = (*taskScheduler)(nil)
var _ BalanceTaskGenerationAdmitter = (*taskScheduler)(nil)
var _ BalanceTaskInspector = (*taskScheduler)(nil)

func receiveTaskTestSignal[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		var zero T
		return zero
	}
}

func closeTaskTestChannelOnCleanup(t *testing.T, ch chan struct{}) func() {
	t.Helper()
	var once sync.Once
	closeChannel := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(closeChannel)
	return closeChannel
}

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

func (suite *TaskSuite) TestPendingGenerationAdmissionIsWaveAwareAndLinearized() {
	epoch := BalanceEpochMeta{
		ResourceGroup: suite.replica.GetResourceGroup(),
		LeaderTerm:    7,
		Sequence:      11,
	}
	expected := BalancePendingRevision{
		ResourceGroup: suite.replica.GetResourceGroup(),
		Epoch:         epoch,
	}
	newGrowTask := func(collectionID, segmentID, nodeID int64, replica *meta.Replica, taskEpoch BalanceEpochMeta) *SegmentTask {
		segmentTask, err := NewSegmentTask(
			context.Background(),
			time.Second,
			WrapIDSource(0),
			collectionID,
			replica,
			commonpb.LoadPriority_LOW,
			NewSegmentActionWithScope(nodeID, ActionTypeGrow, balanceAdmissionShard, segmentID, querypb.DataScope_Historical, 1),
		)
		suite.Require().NoError(err)
		segmentTask.SetBalanceEpoch(taskEpoch)
		return segmentTask
	}

	first := newGrowTask(suite.collection, 7001, 1, suite.replica, epoch)
	firstResult := suite.scheduler.AdmitBalanceTaskAtPendingRevision(first, expected, func() BalanceAdmissionReason {
		return BalanceAdmissionAccepted
	})
	suite.Equal(BalanceAdmissionAccepted, firstResult.Reason)

	second := newGrowTask(suite.collection, 7002, 2, suite.replica, epoch)
	secondResult := suite.scheduler.AdmitBalanceTaskAtPendingRevision(second, expected, func() BalanceAdmissionReason {
		return BalanceAdmissionAccepted
	})
	suite.Equal(BalanceAdmissionAccepted, secondResult.Reason)

	finalValidated := make(chan struct{})
	releaseCommit := make(chan struct{})
	releaseThirdCommit := closeTaskTestChannelOnCleanup(suite.T(), releaseCommit)
	third := newGrowTask(suite.collection, 7003, 3, suite.replica, epoch)
	thirdResultCh := make(chan BalanceAdmissionResult, 1)
	go func() {
		validationCount := 0
		thirdResultCh <- suite.scheduler.AdmitBalanceTaskAtPendingRevision(third, expected, func() BalanceAdmissionReason {
			validationCount++
			if validationCount == 2 {
				close(finalValidated)
				<-releaseCommit
			}
			return BalanceAdmissionAccepted
		})
	}()
	receiveTaskTestSignal(suite.T(), finalValidated, "third wave final validation")

	externalReplica := meta.NewReplica(&querypb.Replica{
		ID:            99,
		CollectionID:  suite.collection + 1,
		ResourceGroup: suite.replica.GetResourceGroup(),
		Nodes:         []int64{1, 2, 3},
	})
	suite.meta.Put(suite.ctx, externalReplica)
	external := newGrowTask(suite.collection+1, 8001, 1, externalReplica, BalanceEpochMeta{})
	suite.Require().NoError(suite.scheduler.Add(external))
	releaseThirdCommit()

	thirdResult := receiveTaskTestSignal(suite.T(), thirdResultCh, "third wave admission result")
	suite.Equal(BalanceAdmissionStaleEpoch, thirdResult.Reason)
	suite.Zero(third.ID())
	suite.Equal(TaskStatusCanceled, third.Status())
	suite.Equal(expected.EffectiveRevision()+1, thirdResult.PendingRevision.EffectiveRevision())
}

func (suite *TaskSuite) TestPendingGenerationDetectsSameEpochRemoval() {
	epoch := BalanceEpochMeta{
		ResourceGroup: suite.replica.GetResourceGroup(),
		LeaderTerm:    13,
		Sequence:      17,
	}
	expected := BalancePendingRevision{
		ResourceGroup: suite.replica.GetResourceGroup(),
		Epoch:         epoch,
	}
	newGrowTask := func(segmentID, nodeID int64) *SegmentTask {
		segmentTask, err := NewSegmentTask(
			context.Background(),
			time.Second,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentActionWithScope(nodeID, ActionTypeGrow, balanceAdmissionShard, segmentID, querypb.DataScope_Historical, 1),
		)
		suite.Require().NoError(err)
		segmentTask.SetBalanceEpoch(epoch)
		return segmentTask
	}
	accept := func() BalanceAdmissionReason { return BalanceAdmissionAccepted }

	first := newGrowTask(7101, 1)
	firstResult := suite.scheduler.AdmitBalanceTaskAtPendingRevision(first, expected, accept)
	suite.Equal(BalanceAdmissionAccepted, firstResult.Reason)

	second := newGrowTask(7102, 2)
	secondResult := suite.scheduler.AdmitBalanceTaskAtPendingRevision(second, expected, accept)
	suite.Equal(BalanceAdmissionAccepted, secondResult.Reason)

	suite.scheduler.remove(first)
	afterRemoval := suite.scheduler.GetPendingBalanceTasks().RevisionFor(expected.ResourceGroup, epoch)
	suite.Equal(uint64(3), afterRemoval.Revision)
	suite.Equal(uint64(2), afterRemoval.EpochRevision)
	suite.Equal(uint64(1), afterRemoval.EffectiveRevision())

	third := newGrowTask(7103, 3)
	thirdResult := suite.scheduler.AdmitBalanceTaskAtPendingRevision(third, expected, accept)
	suite.Equal(BalanceAdmissionStaleEpoch, thirdResult.Reason)
	suite.Equal(expected.EffectiveRevision()+1, thirdResult.PendingRevision.EffectiveRevision())
	suite.Zero(third.ID())
	suite.Equal(TaskStatusCanceled, third.Status())
}

func (suite *TaskSuite) TestPendingGenerationPrunesInactiveEpochRevision() {
	epoch := BalanceEpochMeta{
		ResourceGroup: suite.replica.GetResourceGroup(),
		LeaderTerm:    19,
		Sequence:      23,
	}
	expected := BalancePendingRevision{
		ResourceGroup: suite.replica.GetResourceGroup(),
		Epoch:         epoch,
	}
	task, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(1, ActionTypeGrow, balanceAdmissionShard, 7201, querypb.DataScope_Historical, 1),
	)
	suite.Require().NoError(err)
	task.SetBalanceEpoch(epoch)
	result := suite.scheduler.AdmitBalanceTaskAtPendingRevision(task, expected, func() BalanceAdmissionReason {
		return BalanceAdmissionAccepted
	})
	suite.Require().Equal(BalanceAdmissionAccepted, result.Reason)
	suite.Contains(suite.scheduler.GetPendingBalanceTasks().EpochRevisions, epoch)

	suite.scheduler.remove(task)

	afterRemoval := suite.scheduler.GetPendingBalanceTasks()
	suite.NotContains(afterRemoval.EpochRevisions, epoch)
	suite.Greater(afterRemoval.RevisionFor(expected.ResourceGroup, epoch).EffectiveRevision(), expected.EffectiveRevision())
}

func (suite *TaskSuite) TestLegacyAdmissionsDoNotAdvanceEpochRevision() {
	epoch := BalanceEpochMeta{
		ResourceGroup: suite.replica.GetResourceGroup(),
		LeaderTerm:    29,
		Sequence:      31,
	}
	newGrowTask := func(segmentID, nodeID int64) *SegmentTask {
		task, err := NewSegmentTask(
			context.Background(),
			time.Second,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentActionWithScope(nodeID, ActionTypeGrow, balanceAdmissionShard, segmentID, querypb.DataScope_Historical, 1),
		)
		suite.Require().NoError(err)
		task.SetBalanceEpoch(epoch)
		return task
	}

	added := newGrowTask(7301, 1)
	suite.Require().NoError(suite.scheduler.Add(added))
	afterAdd := suite.scheduler.GetPendingBalanceTasks().RevisionFor(epoch.ResourceGroup, epoch)
	suite.Equal(uint64(1), afterAdd.Revision)
	suite.Zero(afterAdd.EpochRevision)

	admitted := newGrowTask(7302, 2)
	result := suite.scheduler.AdmitBalanceTask(admitted, func() BalanceAdmissionReason {
		return BalanceAdmissionAccepted
	})
	suite.Require().Equal(BalanceAdmissionAccepted, result.Reason)
	afterAdmission := suite.scheduler.GetPendingBalanceTasks().RevisionFor(epoch.ResourceGroup, epoch)
	suite.Equal(uint64(2), afterAdmission.Revision)
	suite.Zero(afterAdmission.EpochRevision)
}

func (suite *TaskSuite) TestDispatchDoesNotHoldScheduleLockDuringTargetRefresh() {
	blockingTarget := meta.NewMockTargetManager(suite.T())
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	releaseTargetRefresh := closeTaskTestChannelOnCleanup(suite.T(), releaseRefresh)
	blockingTarget.EXPECT().UpdateCollectionNextTarget(mock.Anything, suite.collection).
		Run(func(context.Context, int64) {
			close(refreshStarted)
			<-releaseRefresh
		}).
		Return(nil).
		Once()
	suite.scheduler.targetMgr = blockingTarget

	failedTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(1, ActionTypeGrow, balanceAdmissionShard, 9001, querypb.DataScope_Historical, 1),
	)
	suite.Require().NoError(err)
	suite.Require().NoError(suite.scheduler.Add(failedTask))
	failedTask.Fail(merr.ErrSegmentNotFound)
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		suite.scheduler.Dispatch(1)
	}()
	receiveTaskTestSignal(suite.T(), refreshStarted, "target refresh start")

	addResourceGroup := "target-refresh-add-rg"
	externalReplica := meta.NewReplica(&querypb.Replica{
		ID:            100,
		CollectionID:  suite.collection + 1,
		ResourceGroup: addResourceGroup,
		Nodes:         []int64{1, 2, 3},
	})
	suite.Require().NoError(suite.meta.Put(suite.ctx, externalReplica))
	unrelatedTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection+1,
		externalReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(2, ActionTypeGrow, balanceAdmissionShard, 9002, querypb.DataScope_Historical, 1),
	)
	suite.Require().NoError(err)
	addDone := make(chan error, 1)
	go func() { addDone <- suite.scheduler.Add(unrelatedTask) }()

	generationResourceGroup := "target-refresh-admission-rg"
	generationEpoch := BalanceEpochMeta{ResourceGroup: generationResourceGroup, LeaderTerm: 9, Sequence: 1}
	generationReplica := meta.NewReplica(&querypb.Replica{
		ID:            101,
		CollectionID:  suite.collection + 2,
		ResourceGroup: generationResourceGroup,
		Nodes:         []int64{1, 2, 3},
	})
	suite.Require().NoError(suite.meta.Put(suite.ctx, generationReplica))
	generationTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection+2,
		generationReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(3, ActionTypeGrow, balanceAdmissionShard, 9003, querypb.DataScope_Historical, 1),
	)
	suite.Require().NoError(err)
	generationTask.SetBalanceEpoch(generationEpoch)
	expected := suite.scheduler.GetPendingBalanceTasks().RevisionFor(generationResourceGroup, generationEpoch)
	admitDone := make(chan BalanceAdmissionResult, 1)
	go func() {
		admitDone <- suite.scheduler.AdmitBalanceTaskAtPendingRevision(generationTask, expected, func() BalanceAdmissionReason {
			return BalanceAdmissionAccepted
		})
	}()

	inspectDone := make(chan PendingBalanceSnapshot, 1)
	go func() { inspectDone <- suite.scheduler.GetPendingBalanceTasks() }()

	addCompleted := false
	var addErr error
	select {
	case addErr = <-addDone:
		addCompleted = true
	case <-time.After(200 * time.Millisecond):
	}
	admitCompleted := false
	var admitResult BalanceAdmissionResult
	select {
	case admitResult = <-admitDone:
		admitCompleted = true
	case <-time.After(200 * time.Millisecond):
	}
	inspectCompleted := false
	select {
	case <-inspectDone:
		inspectCompleted = true
	case <-time.After(200 * time.Millisecond):
	}
	releaseTargetRefresh()
	receiveTaskTestSignal(suite.T(), dispatchDone, "dispatch completion")
	if !addCompleted {
		addErr = receiveTaskTestSignal(suite.T(), addDone, "unrelated Add completion")
	}
	if !admitCompleted {
		admitResult = receiveTaskTestSignal(suite.T(), admitDone, "generation admission completion")
	}
	if !inspectCompleted {
		receiveTaskTestSignal(suite.T(), inspectDone, "pending inspection completion")
	}

	suite.NoError(addErr)
	suite.Equal(BalanceAdmissionAccepted, admitResult.Reason)
	suite.True(addCompleted, "collection-B Add waited for collection-A target refresh")
	suite.True(admitCompleted, "generation admission waited for collection-A target refresh")
	suite.True(inspectCompleted, "pending inspection waited for collection-A target refresh")
}

func (suite *TaskSuite) TestResourceExhaustionPenaltyPrecedesDeferredFinalization() {
	finalizationStarted := make(chan struct{})
	releaseFinalization := make(chan struct{})
	releaseDeferredFinalization := closeTaskTestChannelOnCleanup(suite.T(), releaseFinalization)
	suite.scheduler.finalizationHook = func(task Task) {
		close(finalizationStarted)
		<-releaseFinalization
	}

	failedTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(1, ActionTypeGrow, balanceAdmissionShard, 9004, querypb.DataScope_Historical, 1),
	)
	suite.Require().NoError(err)
	suite.Require().NoError(suite.scheduler.Add(failedTask))
	failedTask.Fail(merr.ErrSegmentRequestResourceFailed)
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		suite.scheduler.Dispatch(1)
	}()
	receiveTaskTestSignal(suite.T(), finalizationStarted, "deferred finalization start")

	suite.True(suite.nodeMgr.IsResourceExhausted(1), "penalty was not visible before deferred finalization")
	externalReplica := meta.NewReplica(&querypb.Replica{
		ID:            102,
		CollectionID:  suite.collection + 3,
		ResourceGroup: "penalty-finalization-unrelated-rg",
		Nodes:         []int64{1, 2, 3},
	})
	suite.Require().NoError(suite.meta.Put(suite.ctx, externalReplica))
	unrelatedTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection+3,
		externalReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(2, ActionTypeGrow, balanceAdmissionShard, 9005, querypb.DataScope_Historical, 1),
	)
	suite.Require().NoError(err)
	addDone := make(chan error, 1)
	go func() { addDone <- suite.scheduler.Add(unrelatedTask) }()
	addCompleted := false
	var addErr error
	select {
	case addErr = <-addDone:
		addCompleted = true
	case <-time.After(200 * time.Millisecond):
	}

	releaseDeferredFinalization()
	receiveTaskTestSignal(suite.T(), dispatchDone, "penalty dispatch completion")
	if !addCompleted {
		addErr = receiveTaskTestSignal(suite.T(), addDone, "penalty test unrelated Add completion")
	}
	suite.NoError(addErr)
	suite.True(addCompleted, "unrelated Add waited for deferred finalization")
}

func (suite *TaskSuite) TestReplacementPreservesResourceExhaustionPenalty() {
	segmentID := int64(9006)
	suite.setSegmentMovePrerequisites(segmentID, 1)
	oldTask := suite.newSegmentMoveTask(segmentID, 1, 2)
	suite.Require().NoError(suite.scheduler.Add(oldTask))
	oldTask.Fail(merr.ErrSegmentRequestResourceFailed)

	replacement := suite.newSegmentMoveTask(segmentID, 1, 3)
	replacement.SetPriority(TaskPriorityHigh)
	suite.Require().NoError(suite.scheduler.Add(replacement))

	suite.True(suite.nodeMgr.IsResourceExhausted(2), "replacement skipped the failed task's resource penalty")
}

func (suite *TaskSuite) TestSynchronousRemoveWaitsForScheduleLock() {
	removeTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(1, ActionTypeGrow, balanceAdmissionShard, 9007, querypb.DataScope_Historical, 1),
	)
	suite.Require().NoError(err)
	suite.Require().NoError(suite.scheduler.Add(removeTask))

	suite.scheduler.scheduleMu.Lock()
	removeDone := make(chan struct{})
	go func() {
		defer close(removeDone)
		suite.scheduler.remove(removeTask)
	}()
	removedWhileLocked := false
	select {
	case <-removeDone:
		removedWhileLocked = true
	case <-time.After(200 * time.Millisecond):
	}
	suite.scheduler.scheduleMu.Unlock()
	if !removedWhileLocked {
		receiveTaskTestSignal(suite.T(), removeDone, "synchronous remove after schedule unlock")
	}

	suite.False(removedWhileLocked, "synchronous remove bypassed scheduleMu")
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

func (suite *TaskSuite) TestDelayedRemovalDoesNotDeleteReplacementIndex() {
	segmentID := int64(1010)
	sourceNode := int64(1)
	suite.setSegmentMovePrerequisites(segmentID, sourceNode)
	oldTask := suite.newSegmentMoveTask(segmentID, sourceNode, 2)
	oldTask.SetPriority(TaskPriorityNormal)
	suite.Require().NoError(suite.scheduler.Add(oldTask))

	replacement := suite.newSegmentMoveTask(segmentID, sourceNode, 3)
	replacement.SetPriority(TaskPriorityHigh)
	suite.Require().NoError(suite.scheduler.Add(replacement))
	suite.Equal(TaskStatusCanceled, oldTask.Status())
	beforeDelayedRemoval := suite.scheduler.GetPendingBalanceTasks()
	beforeDelta := suite.scheduler.GetSegmentTaskDeltaSnapshot([]int64{sourceNode, 3}, suite.collection)

	suite.scheduler.remove(oldTask)

	suite.Equal(TaskStatusStarted, replacement.Status())
	suite.Equal(1, suite.scheduler.waitQueue.Len())
	suite.Equal(1, suite.scheduler.tasks.Len())
	suite.Equal(1, suite.scheduler.segmentTasks.Len())
	registered, ok := suite.scheduler.segmentTasks.Get(NewReplicaSegmentIndex(replacement))
	suite.True(ok)
	suite.Same(replacement, registered)
	snapshot := suite.scheduler.GetPendingBalanceTasks()
	suite.Require().Len(snapshot.Tasks, 1)
	suite.Equal(replacement.ID(), snapshot.Tasks[0].TaskID)
	suite.Equal(beforeDelayedRemoval.Revision, snapshot.Revision)
	suite.Equal(beforeDelayedRemoval.ResourceGroupRevisions, snapshot.ResourceGroupRevisions)
	afterDelta := suite.scheduler.GetSegmentTaskDeltaSnapshot([]int64{sourceNode, 3}, suite.collection)
	suite.Equal(beforeDelta.GetByNodeInCollection(sourceNode), afterDelta.GetByNodeInCollection(sourceNode))
	suite.Equal(beforeDelta.GetByNodeInCollection(3), afterDelta.GetByNodeInCollection(3))
}

func (suite *TaskSuite) TestReplacementCancelsOldUnderScheduleLockOutsidePendingLock() {
	segmentID := int64(1011)
	sourceNode := int64(1)
	suite.setSegmentMovePrerequisites(segmentID, sourceNode)
	oldTask := suite.newSegmentMoveTask(segmentID, sourceNode, 2)
	oldTask.SetPriority(TaskPriorityNormal)
	suite.Require().NoError(suite.scheduler.Add(oldTask))
	originalCancel := oldTask.baseTask.cancel
	cancelStarted := make(chan struct{})
	releaseCancel := make(chan struct{})
	releaseOldCancel := closeTaskTestChannelOnCleanup(suite.T(), releaseCancel)
	oldTask.baseTask.cancel = func() {
		close(cancelStarted)
		<-releaseCancel
		originalCancel()
	}

	replacement := suite.newSegmentMoveTask(segmentID, sourceNode, 3)
	replacement.SetPriority(TaskPriorityHigh)
	addResult := make(chan error, 1)
	go func() {
		addResult <- suite.scheduler.Add(replacement)
	}()
	receiveTaskTestSignal(suite.T(), cancelStarted, "replacement old-task cancellation")

	scheduleLockAvailable := suite.scheduler.scheduleMu.TryLock()
	if scheduleLockAvailable {
		suite.scheduler.scheduleMu.Unlock()
	}
	suite.False(scheduleLockAvailable, "replacement cancellation must exclude dispatch")
	pendingLockAvailable := suite.scheduler.pendingMu.TryLock()
	if pendingLockAvailable {
		suite.scheduler.pendingMu.Unlock()
	}
	suite.True(pendingLockAvailable, "replacement cancellation must not hold the pending lock")

	releaseOldCancel()
	suite.NoError(receiveTaskTestSignal(suite.T(), addResult, "replacement Add result"))
	suite.Equal(TaskStatusCanceled, oldTask.Status())
	suite.Equal(TaskStatusStarted, replacement.Status())
}

func (suite *TaskSuite) TestReplacementAdditionBookkeepingPrecedesOldFinalization() {
	segmentID := int64(1012)
	sourceNode := int64(1)
	suite.setSegmentMovePrerequisites(segmentID, sourceNode)
	oldTask := suite.newSegmentMoveTask(segmentID, sourceNode, 2)
	suite.Require().NoError(suite.scheduler.Add(oldTask))

	finalizationStarted := make(chan struct{})
	releaseFinalization := make(chan struct{})
	releaseOldFinalization := closeTaskTestChannelOnCleanup(suite.T(), releaseFinalization)
	suite.scheduler.finalizationHook = func(task Task) {
		if task == oldTask {
			close(finalizationStarted)
			<-releaseFinalization
		}
	}

	replacement := suite.newSegmentMoveTask(segmentID, sourceNode, 3)
	replacement.SetPriority(TaskPriorityHigh)
	replacement.startTs.Store(time.Time{})
	addResult := make(chan error, 1)
	go func() { addResult <- suite.scheduler.Add(replacement) }()
	receiveTaskTestSignal(suite.T(), finalizationStarted, "replacement old-task finalization")

	suite.NotZero(replacement.ID())
	suite.True(suite.scheduler.taskStats.Contains(replacement.ID()), "replacement taskStats were not published before old finalization")
	suite.False(replacement.startTs.Load().IsZero(), "replacement start timestamp was not recorded before old finalization")
	scheduleLockAvailable := suite.scheduler.scheduleMu.TryLock()
	if scheduleLockAvailable {
		suite.scheduler.scheduleMu.Unlock()
	}
	suite.True(scheduleLockAvailable, "old finalization still held scheduleMu")

	releaseOldFinalization()
	suite.NoError(receiveTaskTestSignal(suite.T(), addResult, "replacement addition completion"))
	suite.Equal(TaskStatusCanceled, oldTask.Status())
	suite.Equal(TaskStatusStarted, replacement.Status())
}

func (suite *TaskSuite) TestGenerationReplacementAdditionBookkeepingPrecedesOldFinalization() {
	segmentID := int64(1013)
	sourceNode := int64(1)
	suite.setSegmentMovePrerequisites(segmentID, sourceNode)
	oldTask := suite.newSegmentMoveTask(segmentID, sourceNode, 2)
	suite.Require().NoError(suite.scheduler.Add(oldTask))

	finalizationStarted := make(chan struct{})
	releaseFinalization := make(chan struct{})
	releaseOldFinalization := closeTaskTestChannelOnCleanup(suite.T(), releaseFinalization)
	suite.scheduler.finalizationHook = func(task Task) {
		if task == oldTask {
			close(finalizationStarted)
			<-releaseFinalization
		}
	}

	epoch := BalanceEpochMeta{ResourceGroup: suite.replica.GetResourceGroup(), LeaderTerm: 17, Sequence: 19}
	replacement := suite.newSegmentMoveTask(segmentID, sourceNode, 3)
	replacement.SetPriority(TaskPriorityHigh)
	replacement.SetBalanceEpoch(epoch)
	replacement.startTs.Store(time.Time{})
	expected := suite.scheduler.GetPendingBalanceTasks().RevisionFor(epoch.ResourceGroup, epoch)
	admitResult := make(chan BalanceAdmissionResult, 1)
	go func() {
		admitResult <- suite.scheduler.AdmitBalanceTaskAtPendingRevision(replacement, expected, func() BalanceAdmissionReason {
			return BalanceAdmissionAccepted
		})
	}()
	receiveTaskTestSignal(suite.T(), finalizationStarted, "generation replacement old-task finalization")

	suite.NotZero(replacement.ID())
	suite.True(suite.scheduler.taskStats.Contains(replacement.ID()), "generation replacement taskStats were not published before old finalization")
	suite.False(replacement.startTs.Load().IsZero(), "generation replacement start timestamp was not recorded before old finalization")
	scheduleLockAvailable := suite.scheduler.scheduleMu.TryLock()
	if scheduleLockAvailable {
		suite.scheduler.scheduleMu.Unlock()
	}
	suite.True(scheduleLockAvailable, "generation replacement old finalization still held scheduleMu")

	releaseOldFinalization()
	result := receiveTaskTestSignal(suite.T(), admitResult, "generation replacement admission completion")
	suite.Equal(BalanceAdmissionAccepted, result.Reason)
	suite.Equal(TaskStatusCanceled, oldTask.Status())
	suite.Equal(TaskStatusStarted, replacement.Status())
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

func (suite *TaskSuite) TestPendingRevisionForNilReplicaUsesActionNodeResourceGroup() {
	resourceGroup := "nil-replica-rg"
	nodeID := int64(99)
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: nodeID,
		Labels: map[string]string{sessionutil.LabelResourceGroup: resourceGroup},
	}))
	cleanupTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		meta.NilReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(nodeID, ActionTypeReduce, balanceAdmissionShard, 9901, querypb.DataScope_Historical, -1),
	)
	suite.Require().NoError(err)
	suite.Require().NoError(suite.scheduler.Add(cleanupTask))

	afterAdd := suite.scheduler.GetPendingBalanceTasks()
	suite.Equal(uint64(1), afterAdd.ResourceGroupRevisions[resourceGroup])
	suite.Equal(uint64(2), afterAdd.RevisionFor(resourceGroup, BalanceEpochMeta{}).EffectiveRevision())

	suite.scheduler.remove(cleanupTask)
	afterRemove := suite.scheduler.GetPendingBalanceTasks()
	suite.Equal(uint64(2), afterRemove.ResourceGroupRevisions[resourceGroup])
	suite.Equal(uint64(4), afterRemove.RevisionFor(resourceGroup, BalanceEpochMeta{}).EffectiveRevision())
}

func (suite *TaskSuite) TestPendingRevisionForNilReplicaUsesPhysicalResourceGroupMembership() {
	resourceGroup := "nil-replica-physical-rg"
	nodeID := int64(95)
	suite.meta.ResourceManager = meta.NewResourceManager(suite.store, suite.nodeMgr)
	_, err := suite.meta.ResourceManager.AddResourceGroup(suite.ctx, resourceGroup, &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 1},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 1},
	})
	suite.Require().NoError(err)
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID}))
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, nodeID)
	suite.Require().True(suite.meta.ResourceManager.ContainsNode(suite.ctx, resourceGroup, nodeID))
	suite.Empty(suite.nodeMgr.Get(nodeID).ResourceGroupName())

	cleanupTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		meta.NilReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(nodeID, ActionTypeReduce, balanceAdmissionShard, 9905, querypb.DataScope_Historical, -1),
	)
	suite.Require().NoError(err)
	suite.Require().NoError(suite.scheduler.Add(cleanupTask))

	afterAdd := suite.scheduler.GetPendingBalanceTasks()
	suite.Equal(uint64(1), afterAdd.ResourceGroupRevisions[resourceGroup])
}

func (suite *TaskSuite) TestPendingRevisionForNilReplicaIncludesOutgoingReplicaResourceGroups() {
	currentResourceGroup := "nil-replica-current-rg"
	oldDataResourceGroup := "nil-replica-old-data-rg"
	oldStreamingResourceGroup := "nil-replica-old-streaming-rg"
	dataNode := int64(97)
	streamingNode := int64(98)
	for _, nodeID := range []int64{dataNode, streamingNode} {
		suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID: nodeID,
			Labels: map[string]string{sessionutil.LabelResourceGroup: currentResourceGroup},
		}))
	}
	suite.Require().NoError(suite.meta.Put(suite.ctx,
		meta.NewReplica(&querypb.Replica{
			ID:            97,
			CollectionID:  suite.collection,
			ResourceGroup: oldDataResourceGroup,
			RoNodes:       []int64{dataNode},
		}),
		meta.NewReplica(&querypb.Replica{
			ID:            98,
			CollectionID:  suite.collection,
			ResourceGroup: oldStreamingResourceGroup,
			RoSqNodes:     []int64{streamingNode},
		}),
	))
	newCleanupTask := func(segmentID, nodeID int64) *SegmentTask {
		cleanupTask, err := NewSegmentTask(
			context.Background(),
			time.Second,
			WrapIDSource(0),
			suite.collection,
			meta.NilReplica,
			commonpb.LoadPriority_LOW,
			NewSegmentActionWithScope(nodeID, ActionTypeReduce, balanceAdmissionShard, segmentID, querypb.DataScope_Historical, -1),
		)
		suite.Require().NoError(err)
		return cleanupTask
	}
	dataCleanup := newCleanupTask(9902, dataNode)
	streamingCleanup := newCleanupTask(9903, streamingNode)
	suite.Require().NoError(suite.scheduler.Add(dataCleanup))
	suite.Require().NoError(suite.scheduler.Add(streamingCleanup))

	afterAdd := suite.scheduler.GetPendingBalanceTasks()
	suite.Equal(uint64(2), afterAdd.ResourceGroupRevisions[currentResourceGroup])
	suite.Equal(uint64(1), afterAdd.ResourceGroupRevisions[oldDataResourceGroup])
	suite.Equal(uint64(1), afterAdd.ResourceGroupRevisions[oldStreamingResourceGroup])

	suite.scheduler.remove(dataCleanup)
	suite.scheduler.remove(streamingCleanup)
	afterRemove := suite.scheduler.GetPendingBalanceTasks()
	suite.Equal(uint64(4), afterRemove.ResourceGroupRevisions[currentResourceGroup])
	suite.Equal(uint64(2), afterRemove.ResourceGroupRevisions[oldDataResourceGroup])
	suite.Equal(uint64(2), afterRemove.ResourceGroupRevisions[oldStreamingResourceGroup])
}

func (suite *TaskSuite) TestNilReplicaRemovalIncludesNewOutgoingReplicaResourceGroup() {
	currentResourceGroup := "nil-replica-removal-current-rg"
	newOutgoingResourceGroup := "nil-replica-removal-outgoing-rg"
	nodeID := int64(96)
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: nodeID,
		Labels: map[string]string{sessionutil.LabelResourceGroup: currentResourceGroup},
	}))
	cleanupTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		meta.NilReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(nodeID, ActionTypeReduce, balanceAdmissionShard, 9904, querypb.DataScope_Historical, -1),
	)
	suite.Require().NoError(err)
	suite.Require().NoError(suite.scheduler.Add(cleanupTask))
	afterAdd := suite.scheduler.GetPendingBalanceTasks()
	suite.Equal(uint64(1), afterAdd.ResourceGroupRevisions[currentResourceGroup])
	suite.Zero(afterAdd.ResourceGroupRevisions[newOutgoingResourceGroup])

	suite.Require().NoError(suite.meta.Put(suite.ctx, meta.NewReplica(&querypb.Replica{
		ID:            96,
		CollectionID:  suite.collection,
		ResourceGroup: newOutgoingResourceGroup,
		RoNodes:       []int64{nodeID},
	})))
	suite.scheduler.remove(cleanupTask)

	afterRemove := suite.scheduler.GetPendingBalanceTasks()
	suite.Equal(uint64(2), afterRemove.ResourceGroupRevisions[currentResourceGroup])
	suite.Equal(uint64(1), afterRemove.ResourceGroupRevisions[newOutgoingResourceGroup])
}

func (suite *TaskSuite) TestNilReplicaRemovalUnscopedRevisionClosesScopeResolutionWindow() {
	currentResourceGroup := "nil-replica-linearized-current-rg"
	newOutgoingResourceGroup := "nil-replica-linearized-outgoing-rg"
	nodeID := int64(94)
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: nodeID,
		Labels: map[string]string{sessionutil.LabelResourceGroup: currentResourceGroup},
	}))
	cleanupTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		meta.NilReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(nodeID, ActionTypeReduce, balanceAdmissionShard, 9910, querypb.DataScope_Historical, -1),
	)
	suite.Require().NoError(err)
	suite.Require().NoError(suite.scheduler.Add(cleanupTask))
	admissionReplica := meta.NewReplica(&querypb.Replica{
		ID:            194,
		CollectionID:  suite.collection + 94,
		ResourceGroup: newOutgoingResourceGroup,
		Nodes:         []int64{1, 2, 3},
	})
	suite.Require().NoError(suite.meta.Put(suite.ctx, admissionReplica))
	admissionTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection+94,
		admissionReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(1, ActionTypeGrow, balanceAdmissionShard, 9912, querypb.DataScope_Historical, 1),
	)
	suite.Require().NoError(err)

	removalScopeResolved := make(chan struct{})
	releaseRemoval := make(chan struct{})
	admissionScopeResolved := make(chan struct{})
	releaseAdmission := make(chan struct{})
	releaseRemovalCommit := closeTaskTestChannelOnCleanup(suite.T(), releaseRemoval)
	releaseRemovalAdmission := closeTaskTestChannelOnCleanup(suite.T(), releaseAdmission)
	suite.scheduler.pendingMutationHook = func(stage pendingMutationStage, task Task) {
		switch {
		case stage == pendingMutationStageRemoveScopeResolved && task.ID() == cleanupTask.ID():
			close(removalScopeResolved)
			<-releaseRemoval
		case stage == pendingMutationStageAddScopeResolved && task == admissionTask:
			close(admissionScopeResolved)
			<-releaseAdmission
		}
	}
	removeDone := make(chan struct{})
	go func() {
		defer close(removeDone)
		suite.scheduler.remove(cleanupTask)
	}()
	receiveTaskTestSignal(suite.T(), removalScopeResolved, "NilReplica removal scope resolution")
	beforeRemoval := suite.scheduler.GetPendingBalanceTasks().RevisionFor(newOutgoingResourceGroup, BalanceEpochMeta{})
	suite.Equal(uint64(1), beforeRemoval.EffectiveRevision())
	admissionDone := make(chan BalanceAdmissionResult, 1)
	go func() {
		admissionDone <- suite.scheduler.AdmitBalanceTaskAtPendingRevision(admissionTask, beforeRemoval, func() BalanceAdmissionReason {
			return BalanceAdmissionAccepted
		})
	}()
	receiveTaskTestSignal(suite.T(), admissionScopeResolved, "removal-race admission scope resolution")

	suite.Require().NoError(suite.meta.Put(suite.ctx, meta.NewReplica(&querypb.Replica{
		ID:            94,
		CollectionID:  suite.collection,
		ResourceGroup: newOutgoingResourceGroup,
		RoNodes:       []int64{nodeID},
	})))

	releaseRemovalCommit()
	receiveTaskTestSignal(suite.T(), removeDone, "NilReplica removal commit")
	afterRemovalSnapshot := suite.scheduler.GetPendingBalanceTasks()
	afterRemoval := afterRemovalSnapshot.RevisionFor(newOutgoingResourceGroup, BalanceEpochMeta{})
	suite.Zero(afterRemovalSnapshot.ResourceGroupRevisions[newOutgoingResourceGroup],
		"the correctness fence must not depend on a late RG scope resolution")
	suite.Equal(uint64(2), afterRemoval.EffectiveRevision())

	releaseRemovalAdmission()
	result := receiveTaskTestSignal(suite.T(), admissionDone, "removal-race admission result")
	suite.Equal(BalanceAdmissionStaleEpoch, result.Reason)
	suite.Zero(admissionTask.ID())
}

func (suite *TaskSuite) TestNilReplicaAddUnscopedRevisionClosesScopeResolutionWindow() {
	newOutgoingResourceGroup := "nil-replica-add-linearized-outgoing-rg"
	nodeID := int64(93)
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID}))
	cleanupTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection,
		meta.NilReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(nodeID, ActionTypeReduce, balanceAdmissionShard, 9911, querypb.DataScope_Historical, -1),
	)
	suite.Require().NoError(err)
	admissionReplica := meta.NewReplica(&querypb.Replica{
		ID:            193,
		CollectionID:  suite.collection + 93,
		ResourceGroup: newOutgoingResourceGroup,
		Nodes:         []int64{1, 2, 3},
	})
	suite.Require().NoError(suite.meta.Put(suite.ctx, admissionReplica))
	admissionTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		suite.collection+93,
		admissionReplica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(1, ActionTypeGrow, balanceAdmissionShard, 9913, querypb.DataScope_Historical, 1),
	)
	suite.Require().NoError(err)

	cleanupScopeResolved := make(chan struct{})
	releaseAdd := make(chan struct{})
	admissionScopeResolved := make(chan struct{})
	releaseAdmission := make(chan struct{})
	releaseCleanupAdd := closeTaskTestChannelOnCleanup(suite.T(), releaseAdd)
	releaseAddAdmission := closeTaskTestChannelOnCleanup(suite.T(), releaseAdmission)
	suite.scheduler.pendingMutationHook = func(stage pendingMutationStage, task Task) {
		switch {
		case stage == pendingMutationStageAddScopeResolved && task == cleanupTask:
			close(cleanupScopeResolved)
			<-releaseAdd
		case stage == pendingMutationStageAddScopeResolved && task == admissionTask:
			close(admissionScopeResolved)
			<-releaseAdmission
		}
	}
	addDone := make(chan error, 1)
	go func() { addDone <- suite.scheduler.Add(cleanupTask) }()
	receiveTaskTestSignal(suite.T(), cleanupScopeResolved, "NilReplica add scope resolution")
	beforeAdd := suite.scheduler.GetPendingBalanceTasks().RevisionFor(newOutgoingResourceGroup, BalanceEpochMeta{})
	suite.Zero(beforeAdd.EffectiveRevision())
	admissionDone := make(chan BalanceAdmissionResult, 1)
	go func() {
		admissionDone <- suite.scheduler.AdmitBalanceTaskAtPendingRevision(admissionTask, beforeAdd, func() BalanceAdmissionReason {
			return BalanceAdmissionAccepted
		})
	}()
	receiveTaskTestSignal(suite.T(), admissionScopeResolved, "add-race admission scope resolution")

	suite.Require().NoError(suite.meta.Put(suite.ctx, meta.NewReplica(&querypb.Replica{
		ID:            93,
		CollectionID:  suite.collection,
		ResourceGroup: newOutgoingResourceGroup,
		RoNodes:       []int64{nodeID},
	})))

	releaseCleanupAdd()
	suite.Require().NoError(receiveTaskTestSignal(suite.T(), addDone, "NilReplica add commit"))
	afterAddSnapshot := suite.scheduler.GetPendingBalanceTasks()
	afterAdd := afterAddSnapshot.RevisionFor(newOutgoingResourceGroup, BalanceEpochMeta{})
	suite.Zero(afterAddSnapshot.ResourceGroupRevisions[newOutgoingResourceGroup],
		"the correctness fence must not depend on a late RG scope resolution")
	suite.Equal(uint64(1), afterAdd.EffectiveRevision())
	suite.Len(afterAddSnapshot.Tasks, 1)
	suite.Equal(cleanupTask.ID(), afterAddSnapshot.Tasks[0].TaskID)

	releaseAddAdmission()
	result := receiveTaskTestSignal(suite.T(), admissionDone, "add-race admission result")
	suite.Equal(BalanceAdmissionStaleEpoch, result.Reason)
	suite.Zero(admissionTask.ID())
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
