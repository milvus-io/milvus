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
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/stretchr/testify/require"
)

func TestWaveLedgerHardBudgetLimits(t *testing.T) {
	baseBudget := BalanceWaveBudget{
		MaxSegmentTasks:       10,
		MaxChannelTasks:       10,
		MaxTasksPerNode:       10,
		MaxTasksPerCollection: 10,
	}

	t.Run("segment kind cap", func(t *testing.T) {
		budget := baseBudget
		budget.MaxSegmentTasks = 1
		ledger := NewWaveLedger(budget, EpochPlanningConstraints{})

		require.True(t, ledger.TryReserve(segmentPlan(100, 10, 1000, 1, 2)))
		require.False(t, ledger.TryReserve(segmentPlan(200, 20, 2000, 3, 4)))
		require.Equal(t, 1, ledger.SegmentTasks())
	})

	t.Run("channel kind cap", func(t *testing.T) {
		budget := baseBudget
		budget.MaxChannelTasks = 1
		ledger := NewWaveLedger(budget, EpochPlanningConstraints{})

		require.True(t, ledger.TryReserve(channelPlan("by-dev", 10, 1000, 1, 2)))
		require.False(t, ledger.TryReserve(channelPlan("by-prod", 20, 2000, 3, 4)))
		require.Equal(t, 1, ledger.ChannelTasks())
	})

	t.Run("collection cap", func(t *testing.T) {
		budget := baseBudget
		budget.MaxTasksPerCollection = 1
		ledger := NewWaveLedger(budget, EpochPlanningConstraints{})

		require.True(t, ledger.TryReserve(segmentPlan(100, 10, 1000, 1, 2)))
		require.False(t, ledger.TryReserve(channelPlan("by-dev", 10, 1000, 3, 4)))
		require.Equal(t, 1, ledger.CollectionTasks(10))
	})

	t.Run("duplicate object", func(t *testing.T) {
		ledger := NewWaveLedger(baseBudget, EpochPlanningConstraints{})

		require.True(t, ledger.TryReserve(segmentPlan(100, 10, 1000, 1, 2)))
		require.False(t, ledger.TryReserve(segmentPlan(100, 10, 1000, 3, 4)))
		require.Equal(t, 1, ledger.SegmentTasks())
		require.Equal(t, 0, ledger.NodeActions(3))
		require.Equal(t, 0, ledger.NodeActions(4))
	})
}

func TestWaveLedgerCountsBothMoveEndpoints(t *testing.T) {
	ledger := NewWaveLedger(BalanceWaveBudget{
		MaxSegmentTasks:       10,
		MaxChannelTasks:       10,
		MaxTasksPerNode:       1,
		MaxTasksPerCollection: 10,
	}, EpochPlanningConstraints{})

	require.True(t, ledger.TryReserve(segmentPlan(100, 10, 1000, 1, 2)))
	require.False(t, ledger.TryReserve(segmentPlan(200, 20, 2000, 3, 2)))
	require.False(t, ledger.TryReserve(segmentPlan(300, 30, 3000, 1, 4)))
	require.Equal(t, 1, ledger.NodeActions(1))
	require.Equal(t, 1, ledger.NodeActions(2))
	require.Equal(t, 0, ledger.NodeActions(3))
	require.Equal(t, 0, ledger.NodeActions(4))
}

func TestWaveLedgerChecksLimitForEveryPlan(t *testing.T) {
	ledger := NewWaveLedger(BalanceWaveBudget{
		MaxSegmentTasks:       5,
		MaxChannelTasks:       5,
		MaxTasksPerNode:       100,
		MaxTasksPerCollection: 100,
	}, EpochPlanningConstraints{})

	reserved := 0
	for i := 0; i < 100; i++ {
		if ledger.TryReserve(segmentPlan(int64(i+1), 10, 1000, int64(i*2+1), int64(i*2+2))) {
			reserved++
		}
	}
	require.Equal(t, 5, reserved)
	require.Equal(t, 5, ledger.SegmentTasks())
}

func TestWaveLedgerZeroOrNegativeLimitsAdmitNoWork(t *testing.T) {
	tests := []struct {
		name   string
		budget BalanceWaveBudget
		plan   EpochPlan
	}{
		{
			name: "zero segment cap",
			budget: BalanceWaveBudget{
				MaxChannelTasks:       1,
				MaxTasksPerNode:       1,
				MaxTasksPerCollection: 1,
			},
			plan: segmentPlan(100, 10, 1000, 1, 2),
		},
		{
			name: "negative channel cap",
			budget: BalanceWaveBudget{
				MaxSegmentTasks:       1,
				MaxChannelTasks:       -1,
				MaxTasksPerNode:       1,
				MaxTasksPerCollection: 1,
			},
			plan: channelPlan("by-dev", 10, 1000, 1, 2),
		},
		{
			name: "zero node cap",
			budget: BalanceWaveBudget{
				MaxSegmentTasks:       1,
				MaxChannelTasks:       1,
				MaxTasksPerCollection: 1,
			},
			plan: segmentPlan(100, 10, 1000, 1, 2),
		},
		{
			name: "negative collection cap",
			budget: BalanceWaveBudget{
				MaxSegmentTasks:       1,
				MaxChannelTasks:       1,
				MaxTasksPerNode:       1,
				MaxTasksPerCollection: -1,
			},
			plan: segmentPlan(100, 10, 1000, 1, 2),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger := NewWaveLedger(test.budget, EpochPlanningConstraints{})
			require.False(t, ledger.TryReserve(test.plan))
			require.Equal(t, 0, ledger.SegmentTasks())
			require.Equal(t, 0, ledger.ChannelTasks())
			require.Equal(t, 0, ledger.CollectionTasks(test.plan.CollectionID))
			require.Equal(t, 0, ledger.NodeActions(test.plan.From))
			require.Equal(t, 0, ledger.NodeActions(test.plan.To))
		})
	}
}

func TestWaveLedgerRejectsInvalidPlansWithoutMutation(t *testing.T) {
	validSegment := segmentPlan(100, 10, 1000, 1, 2)
	validChannel := channelPlan("by-dev", 10, 1000, 1, 2)
	tests := []struct {
		name string
		plan EpochPlan
	}{
		{name: "segment without id", plan: replaceSegmentID(validSegment, 0)},
		{name: "segment with channel", plan: replaceChannel(validSegment, "by-dev")},
		{name: "channel without name", plan: replaceChannel(validChannel, "")},
		{name: "channel with segment", plan: replaceSegmentID(validChannel, 100)},
		{name: "unknown kind", plan: replaceKind(validSegment, PlanKind(99))},
		{name: "zero source", plan: replaceEndpoints(validSegment, 0, 2)},
		{name: "negative target", plan: replaceEndpoints(validSegment, 1, -2)},
		{name: "same endpoints", plan: replaceEndpoints(validSegment, 1, 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger := NewWaveLedger(BalanceWaveBudget{
				MaxSegmentTasks:       1,
				MaxChannelTasks:       1,
				MaxTasksPerNode:       1,
				MaxTasksPerCollection: 1,
			}, EpochPlanningConstraints{})

			require.False(t, ledger.TryReserve(test.plan))
			require.Equal(t, 0, ledger.SegmentTasks())
			require.Equal(t, 0, ledger.ChannelTasks())
			require.Equal(t, 0, ledger.CollectionTasks(test.plan.CollectionID))
		})
	}
}

func TestWaveLedgerOldWorkDoesNotConsumeNewWaveKindOrCollectionCaps(t *testing.T) {
	oldPlan := segmentPlan(100, 10, 1000, 9, 10)
	constraints := EpochPlanningConstraints{Objects: map[BalanceObjectKey]EpochObjectConstraint{
		oldPlan.ObjectKey(): {
			Object:       oldPlan.ObjectKey(),
			CollectionID: oldPlan.CollectionID,
			Class:        ReservationActiveTask,
			ChargedNodes: []int64{9},
		},
	}}
	ledger := NewWaveLedger(BalanceWaveBudget{
		MaxSegmentTasks:       1,
		MaxChannelTasks:       1,
		MaxTasksPerNode:       1,
		MaxTasksPerCollection: 1,
	}, constraints)

	require.False(t, ledger.TryReserve(oldPlan))
	require.True(t, ledger.TryReserve(segmentPlan(200, 10, 1000, 1, 2)))
	require.False(t, ledger.TryReserve(segmentPlan(300, 10, 1000, 3, 4)))
	require.Equal(t, 1, ledger.SegmentTasks())
	require.Equal(t, 1, ledger.CollectionTasks(10))
}

func TestWaveLedgerAmbiguousWorkReservesOnlyItsObjectAndNodes(t *testing.T) {
	ambiguousPlan := segmentPlan(100, 10, 1000, 1, 2)
	constraints := EpochPlanningConstraints{Objects: map[BalanceObjectKey]EpochObjectConstraint{
		ambiguousPlan.ObjectKey(): {
			Object:       ambiguousPlan.ObjectKey(),
			CollectionID: ambiguousPlan.CollectionID,
			Class:        ReservationAmbiguousCapacity,
			ChargedNodes: []int64{1, 2, 2},
		},
	}}
	ledger := NewWaveLedger(BalanceWaveBudget{
		MaxSegmentTasks:       1,
		MaxChannelTasks:       1,
		MaxTasksPerNode:       1,
		MaxTasksPerCollection: 1,
	}, constraints)

	require.Equal(t, 1, ledger.NodeActions(1))
	require.Equal(t, 1, ledger.NodeActions(2))
	require.False(t, ledger.TryReserve(segmentPlan(100, 10, 1000, 3, 4)))
	require.False(t, ledger.TryReserve(segmentPlan(200, 10, 1000, 1, 4)))
	require.False(t, ledger.TryReserve(segmentPlan(300, 10, 1000, 3, 2)))
	require.True(t, ledger.TryReserve(segmentPlan(400, 10, 1000, 3, 4)))
	require.Equal(t, 1, ledger.SegmentTasks())
	require.Equal(t, 1, ledger.CollectionTasks(10))
}

func TestWaveLedgerQuarantineLocksOnlyTheObject(t *testing.T) {
	quarantinedPlan := segmentPlan(100, 10, 1000, 1, 2)
	constraints := EpochPlanningConstraints{Objects: map[BalanceObjectKey]EpochObjectConstraint{
		quarantinedPlan.ObjectKey(): {
			Object:       quarantinedPlan.ObjectKey(),
			CollectionID: quarantinedPlan.CollectionID,
			Class:        ReservationQuarantineOnly,
			ChargedNodes: []int64{1, 2},
		},
	}}
	ledger := NewWaveLedger(BalanceWaveBudget{
		MaxSegmentTasks:       1,
		MaxChannelTasks:       1,
		MaxTasksPerNode:       1,
		MaxTasksPerCollection: 1,
	}, constraints)

	require.False(t, ledger.TryReserve(quarantinedPlan))
	require.Equal(t, 0, ledger.NodeActions(1))
	require.Equal(t, 0, ledger.NodeActions(2))
	require.True(t, ledger.TryReserve(segmentPlan(200, 10, 1000, 1, 2)))
}

func TestWaveLedgerReleaseDoesNotReleasePersistentConstraints(t *testing.T) {
	oldPlan := segmentPlan(100, 10, 1000, 1, 2)
	constraints := EpochPlanningConstraints{Objects: map[BalanceObjectKey]EpochObjectConstraint{
		oldPlan.ObjectKey(): {
			Object:       oldPlan.ObjectKey(),
			CollectionID: oldPlan.CollectionID,
			Class:        ReservationAmbiguousCapacity,
			ChargedNodes: []int64{1, 2},
		},
	}}
	ledger := NewWaveLedger(BalanceWaveBudget{
		MaxSegmentTasks:       2,
		MaxChannelTasks:       2,
		MaxTasksPerNode:       1,
		MaxTasksPerCollection: 2,
	}, constraints)
	newPlan := segmentPlan(200, 10, 1000, 3, 4)

	require.True(t, ledger.TryReserve(newPlan))
	ledger.Release(newPlan)
	require.Equal(t, 0, ledger.SegmentTasks())
	require.Equal(t, 0, ledger.CollectionTasks(10))
	require.Equal(t, 1, ledger.NodeActions(1))
	require.Equal(t, 1, ledger.NodeActions(2))
	require.Equal(t, 0, ledger.NodeActions(3))
	require.Equal(t, 0, ledger.NodeActions(4))
	require.False(t, ledger.TryReserve(segmentPlan(300, 10, 1000, 1, 5)))
	require.Panics(t, func() { ledger.Release(oldPlan) })
}

func TestWaveLedgerReleaseExactlyReversesSuccessfulReservation(t *testing.T) {
	ledger := NewWaveLedger(BalanceWaveBudget{
		MaxSegmentTasks:       1,
		MaxChannelTasks:       1,
		MaxTasksPerNode:       1,
		MaxTasksPerCollection: 1,
	}, EpochPlanningConstraints{})
	plan := segmentPlan(100, 10, 1000, 1, 2)

	require.True(t, ledger.TryReserve(plan))
	ledger.Release(plan)
	require.Equal(t, 0, ledger.SegmentTasks())
	require.Equal(t, 0, ledger.CollectionTasks(10))
	require.Equal(t, 0, ledger.NodeActions(1))
	require.Equal(t, 0, ledger.NodeActions(2))
	require.True(t, ledger.TryReserve(plan))
	require.Panics(t, func() { ledger.Release(replaceEndpoints(plan, 1, 3)) })
	ledger.Release(plan)
	require.Panics(t, func() { ledger.Release(plan) })
}

func TestWaveLedgerRejectsInvalidConstraints(t *testing.T) {
	validPlan := segmentPlan(100, 10, 1000, 1, 2)
	tests := []struct {
		name       string
		key        BalanceObjectKey
		constraint EpochObjectConstraint
	}{
		{
			name: "map key mismatch",
			key:  segmentPlan(200, 10, 1000, 1, 2).ObjectKey(),
			constraint: EpochObjectConstraint{
				Object: validPlan.ObjectKey(), Class: ReservationActiveTask, ChargedNodes: []int64{1},
			},
		},
		{
			name: "invalid object",
			key:  BalanceObjectKey{Kind: BalanceObjectSegment, ReplicaID: 1000},
			constraint: EpochObjectConstraint{
				Object: BalanceObjectKey{Kind: BalanceObjectSegment, ReplicaID: 1000},
				Class:  ReservationQuarantineOnly,
			},
		},
		{
			name: "invalid class",
			key:  validPlan.ObjectKey(),
			constraint: EpochObjectConstraint{
				Object: validPlan.ObjectKey(), Class: ReservationClass(99),
			},
		},
		{
			name: "invalid charged node",
			key:  validPlan.ObjectKey(),
			constraint: EpochObjectConstraint{
				Object: validPlan.ObjectKey(), Class: ReservationActiveTask, ChargedNodes: []int64{0},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Panics(t, func() {
				NewWaveLedger(BalanceWaveBudget{}, EpochPlanningConstraints{Objects: map[BalanceObjectKey]EpochObjectConstraint{
					test.key: test.constraint,
				}})
			})
		})
	}
}

func TestProjectedPlacementDeepCopiesSnapshotAndReadResults(t *testing.T) {
	snapshot := projectedPlacementFixture()
	segmentKey := SegmentObjectKey{ReplicaID: 1000, SegmentID: 100, Scope: querypb.DataScope_Historical}
	channelKey := ChannelObjectKey{ReplicaID: 1000, Channel: "by-dev"}
	projection := NewProjectedPlacement(snapshot)

	snapshot.Segments[segmentKey][0].NodeID = 99
	snapshot.Channels[channelKey][0].NodeID = 99
	snapshot.PendingWork.SegmentWorkloadByNode[1] = 99
	snapshot.PendingWork.ChannelWorkloadByNode[1] = 99
	snapshot.PendingWork.Tasks[0].Actions[0].NodeID = 99
	snapshot.Replicas[1000].RWNodes[0] = 99
	snapshot.Replicas[1000].ChannelRWNodes["by-dev"][0] = 99
	snapshot.Token.CurrentTargetVersion[10] = 99
	targetChannel := snapshot.CollectionTargets[10].Current.Channels["by-dev"]
	targetChannel.GrowingSegmentIDs[0] = 99

	require.Equal(t, int64(1), projection.SegmentPlacements(segmentKey)[0].NodeID)
	require.Equal(t, int64(1), projection.ChannelPlacements(channelKey)[0].NodeID)
	require.Equal(t, 2, projection.SegmentWorkload(1))
	require.Equal(t, 3, projection.ChannelWorkload(1))
	require.Equal(t, int64(1), projection.Snapshot().PendingWork.Tasks[0].Actions[0].NodeID)

	segments := projection.SegmentPlacements(segmentKey)
	segments[0].NodeID = 77
	channels := projection.ChannelPlacements(channelKey)
	channels[0].NodeID = 77
	copySnapshot := projection.Snapshot()
	copySnapshot.Replicas[1000].RWNodes[0] = 77
	copySnapshot.Token.CurrentTargetVersion[10] = 77
	copySnapshot.PendingWork.Tasks[0].Actions[0].NodeID = 77

	require.Equal(t, int64(1), projection.SegmentPlacements(segmentKey)[0].NodeID)
	require.Equal(t, int64(1), projection.ChannelPlacements(channelKey)[0].NodeID)
	require.Equal(t, []int64{1, 2}, projection.Snapshot().Replicas[1000].RWNodes)
	require.Equal(t, int64(11), projection.Snapshot().Token.CurrentTargetVersion[10])
	require.Equal(t, int64(1), projection.Snapshot().PendingWork.Tasks[0].Actions[0].NodeID)
	require.Equal(t, []int64{101}, projection.Snapshot().CollectionTargets[10].Current.Channels["by-dev"].GrowingSegmentIDs)
}

func TestProjectedPlacementPreservesNonNilEmptySlices(t *testing.T) {
	emptySegmentKey := SegmentObjectKey{ReplicaID: 1000, SegmentID: 200, Scope: querypb.DataScope_Historical}
	emptyChannelKey := ChannelObjectKey{ReplicaID: 1000, Channel: "empty"}
	snapshot := projectedPlacementFixture()
	replica := snapshot.Replicas[1000]
	replica.RONodes = make([]int64, 0)
	replica.ChannelRWNodes["empty"] = make([]int64, 0)
	snapshot.Replicas[1000] = replica
	snapshot.Segments[emptySegmentKey] = make([]SegmentPlacement, 0)
	snapshot.Channels[emptyChannelKey] = make([]ChannelPlacement, 0)
	target := snapshot.CollectionTargets[10]
	targetChannel := target.Current.Channels["by-dev"]
	targetChannel.GrowingSegmentIDs = make([]int64, 0)
	target.Current.Channels["by-dev"] = targetChannel
	snapshot.CollectionTargets[10] = target
	snapshot.PendingWork.Tasks = append(snapshot.PendingWork.Tasks, task.PendingBalanceTaskSnapshot{
		TaskID:  15,
		Actions: make([]task.PendingBalanceActionSnapshot, 0),
	})

	projected := NewProjectedPlacement(snapshot).Snapshot()

	require.NotNil(t, projected.Replicas[1000].RONodes)
	require.Empty(t, projected.Replicas[1000].RONodes)
	require.NotNil(t, projected.Replicas[1000].ChannelRWNodes["empty"])
	require.Empty(t, projected.Replicas[1000].ChannelRWNodes["empty"])
	require.NotNil(t, projected.Segments[emptySegmentKey])
	require.Empty(t, projected.Segments[emptySegmentKey])
	require.NotNil(t, projected.Channels[emptyChannelKey])
	require.Empty(t, projected.Channels[emptyChannelKey])
	require.NotNil(t, projected.CollectionTargets[10].Current.Channels["by-dev"].GrowingSegmentIDs)
	require.Empty(t, projected.CollectionTargets[10].Current.Channels["by-dev"].GrowingSegmentIDs)
	require.NotNil(t, projected.PendingWork.Tasks[1].Actions)
	require.Empty(t, projected.PendingWork.Tasks[1].Actions)
}

func TestProjectedPlacementApplyAndUndoSegment(t *testing.T) {
	snapshot := projectedPlacementFixture()
	key := SegmentObjectKey{ReplicaID: 1000, SegmentID: 100, Scope: querypb.DataScope_Historical}
	before := append([]SegmentPlacement(nil), snapshot.Segments[key]...)
	projection := NewProjectedPlacement(snapshot)
	plan := segmentPlan(100, 10, 1000, 1, 2)
	plan.Scope = querypb.DataScope_Historical

	require.NoError(t, projection.Apply(plan))
	require.Equal(t, []SegmentPlacement{{
		NodeID:       2,
		CollectionID: 10,
		PartitionID:  20,
		Channel:      "by-dev",
		RowCount:     1000,
		Version:      7,
		Present:      true,
	}}, projection.SegmentPlacements(key))
	require.Equal(t, before, snapshot.Segments[key])

	projection.Undo(plan)
	require.Equal(t, before, projection.SegmentPlacements(key))
	require.Panics(t, func() { projection.Undo(plan) })
}

func TestProjectedPlacementApplyAndUndoChannel(t *testing.T) {
	snapshot := projectedPlacementFixture()
	key := ChannelObjectKey{ReplicaID: 1000, Channel: "by-dev"}
	before := append([]ChannelPlacement(nil), snapshot.Channels[key]...)
	projection := NewProjectedPlacement(snapshot)
	plan := channelPlan("by-dev", 10, 1000, 1, 2)

	require.NoError(t, projection.Apply(plan))
	require.Equal(t, []ChannelPlacement{{
		NodeID:              2,
		CollectionID:        10,
		Version:             8,
		Present:             true,
		Serviceable:         true,
		LeaderID:            1,
		LeaderVersion:       9,
		LeaderTargetVersion: 10,
		NumOfGrowingRows:    11,
	}}, projection.ChannelPlacements(key))
	require.Equal(t, before, snapshot.Channels[key])

	projection.Undo(plan)
	require.Equal(t, before, projection.ChannelPlacements(key))
}

func TestProjectedPlacementApplyValidationIsAtomic(t *testing.T) {
	validPlan := segmentPlan(100, 10, 1000, 1, 2)
	validPlan.Scope = querypb.DataScope_Historical
	tests := []struct {
		name   string
		mutate func(PlacementSnapshot) PlacementSnapshot
		plan   EpochPlan
	}{
		{
			name: "source missing",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				return snapshot
			},
			plan: replaceEndpoints(validPlan, 3, 2),
		},
		{
			name: "target already present",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				key := SegmentObjectKey{ReplicaID: 1000, SegmentID: 100, Scope: querypb.DataScope_Historical}
				target := snapshot.Segments[key][0]
				target.NodeID = 2
				snapshot.Segments[key] = append(snapshot.Segments[key], target)
				return snapshot
			},
			plan: validPlan,
		},
		{
			name: "duplicate source",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				key := SegmentObjectKey{ReplicaID: 1000, SegmentID: 100, Scope: querypb.DataScope_Historical}
				snapshot.Segments[key] = append(snapshot.Segments[key], snapshot.Segments[key][0])
				return snapshot
			},
			plan: validPlan,
		},
		{
			name: "collection mismatch",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				return snapshot
			},
			plan: replaceCollection(validPlan, 11),
		},
		{
			name: "replica mismatch",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				return snapshot
			},
			plan: replaceReplica(validPlan, 2000),
		},
		{
			name: "invalid plan shape",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				return snapshot
			},
			plan: replaceEndpoints(validPlan, 1, 1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := test.mutate(projectedPlacementFixture())
			projection := NewProjectedPlacement(snapshot)
			before := projection.Snapshot()

			require.Error(t, projection.Apply(test.plan))
			require.Equal(t, before, projection.Snapshot())
		})
	}
}

func TestProjectedPlacementChannelValidationIsAtomic(t *testing.T) {
	validPlan := channelPlan("by-dev", 10, 1000, 1, 2)
	tests := []struct {
		name   string
		mutate func(PlacementSnapshot) PlacementSnapshot
		plan   EpochPlan
	}{
		{
			name: "source missing",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				return snapshot
			},
			plan: replaceEndpoints(validPlan, 3, 2),
		},
		{
			name: "target already present",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				key := ChannelObjectKey{ReplicaID: 1000, Channel: "by-dev"}
				target := snapshot.Channels[key][0]
				target.NodeID = 2
				snapshot.Channels[key] = append(snapshot.Channels[key], target)
				return snapshot
			},
			plan: validPlan,
		},
		{
			name: "duplicate source",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				key := ChannelObjectKey{ReplicaID: 1000, Channel: "by-dev"}
				snapshot.Channels[key] = append(snapshot.Channels[key], snapshot.Channels[key][0])
				return snapshot
			},
			plan: validPlan,
		},
		{
			name: "collection mismatch",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				return snapshot
			},
			plan: replaceCollection(validPlan, 11),
		},
		{
			name: "replica mismatch",
			mutate: func(snapshot PlacementSnapshot) PlacementSnapshot {
				return snapshot
			},
			plan: replaceReplica(validPlan, 2000),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := test.mutate(projectedPlacementFixture())
			projection := NewProjectedPlacement(snapshot)
			before := projection.Snapshot()

			require.Error(t, projection.Apply(test.plan))
			require.Equal(t, before, projection.Snapshot())
		})
	}
}

func TestProjectedPlacementRejectsDuplicateApplicationWithoutMutation(t *testing.T) {
	projection := NewProjectedPlacement(projectedPlacementFixture())
	plan := segmentPlan(100, 10, 1000, 1, 2)
	plan.Scope = querypb.DataScope_Historical

	require.NoError(t, projection.Apply(plan))
	afterFirst := projection.Snapshot()
	require.Error(t, projection.Apply(plan))
	require.Equal(t, afterFirst, projection.Snapshot())
	projection.Undo(plan)
}

func TestProjectedPlacementRejectsDuplicateChannelApplicationWithoutMutation(t *testing.T) {
	projection := NewProjectedPlacement(projectedPlacementFixture())
	plan := channelPlan("by-dev", 10, 1000, 1, 2)

	require.NoError(t, projection.Apply(plan))
	afterFirst := projection.Snapshot()
	require.Error(t, projection.Apply(plan))
	require.Equal(t, afterFirst, projection.Snapshot())
	projection.Undo(plan)
	require.Panics(t, func() { projection.Undo(plan) })
}

func TestProjectedPlacementUndoRequiresMatchingSuccessfulApply(t *testing.T) {
	plan := segmentPlan(100, 10, 1000, 1, 2)
	plan.Scope = querypb.DataScope_Historical

	t.Run("without apply", func(t *testing.T) {
		projection := NewProjectedPlacement(projectedPlacementFixture())
		require.Panics(t, func() { projection.Undo(plan) })
	})

	t.Run("different plan", func(t *testing.T) {
		projection := NewProjectedPlacement(projectedPlacementFixture())
		require.NoError(t, projection.Apply(plan))
		require.Panics(t, func() { projection.Undo(replaceEndpoints(plan, 1, 3)) })
		projection.Undo(plan)
	})
}

func projectedPlacementFixture() PlacementSnapshot {
	segmentKey := SegmentObjectKey{ReplicaID: 1000, SegmentID: 100, Scope: querypb.DataScope_Historical}
	channelKey := ChannelObjectKey{ReplicaID: 1000, Channel: "by-dev"}
	return PlacementSnapshot{
		Token: SnapshotToken{
			ResourceGroup:        "rg1",
			CurrentTargetVersion: map[int64]int64{10: 11},
			NextTargetVersion:    map[int64]int64{10: 12},
		},
		CapturedAt: time.Unix(100, 200),
		Nodes: map[int64]NodeSnapshot{
			1: {ID: 1, Exists: true, Eligible: true, ResourceGroup: "rg1"},
			2: {ID: 2, Exists: true, Eligible: true, ResourceGroup: "rg1"},
		},
		Replicas: map[int64]ReplicaSnapshot{
			1000: {
				ID:             1000,
				CollectionID:   10,
				ResourceGroup:  "rg1",
				RWNodes:        []int64{1, 2},
				RONodes:        []int64{3},
				RWSQNodes:      []int64{4},
				ROSQNodes:      []int64{5},
				ChannelRWNodes: map[string][]int64{"by-dev": {1, 2}},
			},
		},
		Segments: map[SegmentObjectKey][]SegmentPlacement{
			segmentKey: {{
				NodeID:       1,
				CollectionID: 10,
				PartitionID:  20,
				Channel:      "by-dev",
				RowCount:     1000,
				Version:      7,
				Present:      true,
			}},
		},
		Channels: map[ChannelObjectKey][]ChannelPlacement{
			channelKey: {{
				NodeID:              1,
				CollectionID:        10,
				Version:             8,
				Present:             true,
				Serviceable:         true,
				LeaderID:            1,
				LeaderVersion:       9,
				LeaderTargetVersion: 10,
				NumOfGrowingRows:    11,
			}},
		},
		CollectionTargets: map[int64]CollectionTargetSnapshot{
			10: {
				Current: TargetScopeSnapshot{
					Version: 11,
					Segments: map[int64]TargetSegmentSnapshot{
						100: {ID: 100, CollectionID: 10, PartitionID: 20, Channel: "by-dev", RowCount: 1000},
					},
					Channels: map[string]TargetChannelSnapshot{
						"by-dev": {CollectionID: 10, Channel: "by-dev", GrowingSegmentIDs: []int64{101}},
					},
				},
				Next: TargetScopeSnapshot{
					Version:  12,
					Segments: map[int64]TargetSegmentSnapshot{},
					Channels: map[string]TargetChannelSnapshot{},
				},
			},
		},
		PendingWork: PendingWorkSnapshot{
			Revision: 13,
			Tasks: []task.PendingBalanceTaskSnapshot{{
				TaskID:       14,
				CollectionID: 10,
				ReplicaID:    1000,
				Actions: []task.PendingBalanceActionSnapshot{{
					NodeID:    1,
					SegmentID: 100,
					Scope:     querypb.DataScope_Historical,
					Workload:  1,
				}},
			}},
			SegmentWorkloadByNode: map[int64]int{1: 2},
			ChannelWorkloadByNode: map[int64]int{1: 3},
		},
		EligibleReplicas: map[int64]struct{}{1000: {}},
	}
}

func segmentPlan(segmentID, collectionID, replicaID, from, to int64) EpochPlan {
	return EpochPlan{
		Kind:         PlanKindSegment,
		CollectionID: collectionID,
		ReplicaID:    replicaID,
		SegmentID:    segmentID,
		From:         from,
		To:           to,
	}
}

func channelPlan(channel string, collectionID, replicaID, from, to int64) EpochPlan {
	return EpochPlan{
		Kind:         PlanKindChannel,
		CollectionID: collectionID,
		ReplicaID:    replicaID,
		Channel:      channel,
		From:         from,
		To:           to,
	}
}

func replaceKind(plan EpochPlan, kind PlanKind) EpochPlan {
	plan.Kind = kind
	return plan
}

func replaceSegmentID(plan EpochPlan, segmentID int64) EpochPlan {
	plan.SegmentID = segmentID
	return plan
}

func replaceChannel(plan EpochPlan, channel string) EpochPlan {
	plan.Channel = channel
	return plan
}

func replaceEndpoints(plan EpochPlan, from, to int64) EpochPlan {
	plan.From = from
	plan.To = to
	return plan
}

func replaceCollection(plan EpochPlan, collectionID int64) EpochPlan {
	plan.CollectionID = collectionID
	return plan
}

func replaceReplica(plan EpochPlan, replicaID int64) EpochPlan {
	plan.ReplicaID = replicaID
	return plan
}
