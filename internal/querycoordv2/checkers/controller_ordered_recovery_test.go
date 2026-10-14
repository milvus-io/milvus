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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type controllerRecoveryTestChecker struct {
	Checker
	check func(context.Context) ([]task.Task, map[string]struct{})
}

func (checker *controllerRecoveryTestChecker) CheckWithRecovery(ctx context.Context) ([]task.Task, map[string]struct{}) {
	return checker.check(ctx)
}

type controllerInFlightScheduler struct {
	*task.MockScheduler
	resourceGroups map[string]struct{}
}

func (scheduler *controllerInFlightScheduler) PendingRecoveryResourceGroups() map[string]struct{} {
	return scheduler.resourceGroups
}

func TestControllerPlacementChecksRecoveryInOrder(t *testing.T) {
	ctx := context.Background()
	channelReplica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 10, ResourceGroup: "channel-rg"})
	segmentReplica := meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 20, ResourceGroup: "segment-rg"})
	channelWork, err := task.NewChannelTask(ctx, time.Minute, utils.ChannelChecker, 10, channelReplica,
		task.NewChannelAction(11, task.ActionTypeGrow, "ch"))
	require.NoError(t, err)
	segmentWork, err := task.NewSegmentTask(ctx, time.Minute, utils.SegmentChecker, 20, segmentReplica, commonpb.LoadPriority_HIGH,
		task.NewSegmentAction(22, task.ActionTypeGrow, "ch", 100))
	require.NoError(t, err)
	t.Cleanup(func() {
		channelWork.Cancel(nil)
		segmentWork.Cancel(nil)
	})

	events := make([]string, 0, 5)
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().Add(mock.Anything).Run(func(work task.Task) {
		events = append(events, "admit-"+work.Source().String())
	}).Return(nil).Times(2)
	controller := &CheckerController{scheduler: scheduler, checkers: map[utils.CheckerType]Checker{
		utils.ChannelChecker: &controllerRecoveryTestChecker{check: func(context.Context) ([]task.Task, map[string]struct{}) {
			events = append(events, "check-channel")
			return []task.Task{channelWork}, map[string]struct{}{"channel-rg": {}}
		}},
		utils.SegmentChecker: &controllerRecoveryTestChecker{check: func(context.Context) ([]task.Task, map[string]struct{}) {
			events = append(events, "check-segment")
			require.Equal(t, "admit-channel_checker", events[1], "segment planning must observe admitted channel work")
			return []task.Task{segmentWork}, map[string]struct{}{"segment-rg": {}}
		}},
		utils.BalanceChecker: &controllerTestChecker{runCheck: func(ctx context.Context) []task.Task {
			events = append(events, "check-balance")
			blocked, ok := ctx.Value(recoveryBlockedRGsKey{}).(map[string]struct{})
			require.True(t, ok)
			require.Equal(t, map[string]struct{}{"channel-rg": {}, "segment-rg": {}}, blocked)
			return nil
		}},
	}}
	controller.checkPlacement(ctx, true)
	require.Equal(t, []string{
		"check-channel", "admit-channel_checker", "check-segment", "admit-segment_checker", "check-balance",
	}, events)
}

func TestControllerInFlightRecoveryBlocksOrdinaryBalanceWithoutNewTasks(t *testing.T) {
	scheduler := &controllerInFlightScheduler{
		MockScheduler:  task.NewMockScheduler(t),
		resourceGroups: map[string]struct{}{"still-loading": {}},
	}
	controller := &CheckerController{scheduler: scheduler, checkers: map[utils.CheckerType]Checker{
		utils.ChannelChecker: &controllerRecoveryTestChecker{check: func(context.Context) ([]task.Task, map[string]struct{}) {
			return nil, nil
		}},
		utils.SegmentChecker: &controllerRecoveryTestChecker{check: func(context.Context) ([]task.Task, map[string]struct{}) {
			return nil, nil
		}},
		utils.BalanceChecker: &controllerTestChecker{runCheck: func(ctx context.Context) []task.Task {
			blocked, ok := ctx.Value(recoveryBlockedRGsKey{}).(map[string]struct{})
			require.True(t, ok)
			require.Equal(t, map[string]struct{}{"still-loading": {}}, blocked)
			return nil
		}},
	}}
	controller.checkPlacement(context.Background(), true)
}

func TestControllerManualCheckRunsOneOrderedRound(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	for _, item := range []string{
		params.QueryCoordCfg.ChannelCheckInterval.Key,
		params.QueryCoordCfg.SegmentCheckInterval.Key,
	} {
		previous := paramtable.GetBaseTable().Get(item)
		require.NoError(t, params.Save(item, "60000"))
		t.Cleanup(func() { require.NoError(t, params.Save(item, previous)) })
	}

	manual := make(chan struct{}, 1)
	finished := make(chan struct{}, 2)
	var order []string
	controller := &CheckerController{
		manualCheckChs: map[utils.CheckerType]chan struct{}{
			utils.ChannelChecker: manual,
			utils.SegmentChecker: manual,
			utils.BalanceChecker: manual,
		},
		checkers: map[utils.CheckerType]Checker{
			utils.ChannelChecker: &controllerRecoveryTestChecker{check: func(context.Context) ([]task.Task, map[string]struct{}) {
				order = append(order, "channel")
				return nil, nil
			}},
			utils.SegmentChecker: &controllerRecoveryTestChecker{check: func(context.Context) ([]task.Task, map[string]struct{}) {
				order = append(order, "segment")
				return nil, nil
			}},
			utils.BalanceChecker: &controllerTestChecker{runCheck: func(context.Context) []task.Task {
				order = append(order, "balance")
				finished <- struct{}{}
				return nil
			}},
		},
	}
	controller.Start()
	defer controller.Stop()
	controller.Check()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("manual placement round did not complete")
	}
	require.Equal(t, []string{"channel", "segment", "balance"}, order)
	select {
	case <-finished:
		t.Fatal("one manual trigger produced more than one round")
	case <-time.After(50 * time.Millisecond):
	}
}
