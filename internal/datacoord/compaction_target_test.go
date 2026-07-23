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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCompactionTargetMetaSuite(t *testing.T) {
	suite.Run(t, new(CompactionTargetMetaSuite))
}

type CompactionTargetMetaSuite struct {
	suite.Suite

	ctx context.Context
}

func (s *CompactionTargetMetaSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *CompactionTargetMetaSuite) TestReloadRetainsAllRecordStates() {
	catalog, _, _, _ := newCompactionTargetTestCatalog(s.T(),
		&datapb.CompactionTarget{
			TargetID: 1,
			Intent:   datapb.TargetIntent_INTENT_REWRITE,
			State:    datapb.TargetState_TARGET_STATE_ACTIVE,
		},
		&datapb.CompactionTarget{
			TargetID: 2,
			Intent:   datapb.TargetIntent_INTENT_REWRITE,
			State:    datapb.TargetState_TARGET_STATE_INACTIVE,
		},
		&datapb.CompactionTarget{
			TargetID: 3,
			Intent:   datapb.TargetIntent_INTENT_REWRITE,
			State:    datapb.TargetState_TARGET_STATE_INACTIVE,
		},
		&datapb.CompactionTarget{
			TargetID: 4,
			Intent:   datapb.TargetIntent_INTENT_REWRITE,
			State:    datapb.TargetState_TARGET_STATE_INACTIVE,
		},
		&datapb.CompactionTarget{
			TargetID: 5,
			Intent:   datapb.TargetIntent(999),
			State:    datapb.TargetState_TARGET_STATE_ACTIVE,
		},
	)

	meta, err := newCompactionTargetMeta(s.ctx, catalog)

	s.Require().NoError(err)
	s.Len(meta.GetCompactionTargets(), 5)
	s.Equal(datapb.TargetState_TARGET_STATE_ACTIVE, meta.GetCompactionTarget(1).GetState())
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, meta.GetCompactionTarget(2).GetState())
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, meta.GetCompactionTarget(3).GetState())
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, meta.GetCompactionTarget(4).GetState())
	s.Equal(datapb.TargetIntent(999), meta.GetCompactionTarget(5).GetIntent())
}

func (s *CompactionTargetMetaSuite) TestReturnsClones() {
	catalog, _, _, _ := newCompactionTargetTestCatalog(s.T(), &datapb.CompactionTarget{
		TargetID: 10,
		Intent:   datapb.TargetIntent_INTENT_REWRITE,
		State:    datapb.TargetState_TARGET_STATE_ACTIVE,
		Properties: map[string]string{
			"maxSize": "1024",
		},
	})

	meta, err := newCompactionTargetMeta(s.ctx, catalog)
	s.Require().NoError(err)

	record := meta.GetCompactionTarget(10)
	record.State = datapb.TargetState_TARGET_STATE_INACTIVE
	record.Properties["maxSize"] = "2048"

	stored := meta.GetCompactionTarget(10)
	s.Equal(datapb.TargetState_TARGET_STATE_ACTIVE, stored.GetState())
	s.Equal("1024", stored.GetProperties()["maxSize"])
}

func (s *CompactionTargetMetaSuite) TestSaveUpdateDrop() {
	catalog, records, updates, dropped := newCompactionTargetTestCatalog(s.T())
	meta, err := newCompactionTargetMeta(s.ctx, catalog)
	s.Require().NoError(err)

	record := &datapb.CompactionTarget{
		TargetID: 10,
		Intent:   datapb.TargetIntent_INTENT_REWRITE,
		State:    datapb.TargetState_TARGET_STATE_ACTIVE,
	}

	s.Require().NoError(meta.SaveCompactionTarget(s.ctx, record))
	s.True(proto.Equal(record, meta.GetCompactionTarget(10)))
	s.True(proto.Equal(record, records[10]))

	s.Require().NoError(meta.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_INACTIVE))
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, meta.GetCompactionTarget(10).GetState())
	s.NotZero(meta.GetCompactionTarget(10).GetInactivatedAtTS())
	s.Require().Len(*updates, 1)
	s.Equal(int64(10), (*updates)[0].targetID)
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, (*updates)[0].state)
	s.NotZero((*updates)[0].inactivatedAtTS)

	s.Require().NoError(meta.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_INACTIVE))
	s.Require().Len(*updates, 1)

	s.Require().NoError(meta.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_ACTIVE))
	s.Equal(datapb.TargetState_TARGET_STATE_ACTIVE, meta.GetCompactionTarget(10).GetState())
	s.Zero(meta.GetCompactionTarget(10).GetInactivatedAtTS())
	s.Require().Len(*updates, 2)
	s.Equal(compactionTargetCatalogUpdate{
		targetID:        10,
		state:           datapb.TargetState_TARGET_STATE_ACTIVE,
		inactivatedAtTS: 0,
	}, (*updates)[1])

	s.Require().NoError(meta.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_ACTIVE))
	s.Require().Len(*updates, 2)

	s.Require().NoError(meta.DropCompactionTarget(s.ctx, record.GetTargetID()))
	s.Nil(meta.GetCompactionTarget(10))
	s.Equal([]int64{10}, *dropped)
}

func (s *CompactionTargetMetaSuite) TestMaterializesTargetsOnRecordMutation() {
	activeRecord := &datapb.CompactionTarget{
		TargetID:     10,
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   1000,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	inactiveRecord := &datapb.CompactionTarget{
		TargetID:     20,
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   1000,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_INACTIVE,
	}
	catalog, _, _, _ := newCompactionTargetTestCatalog(s.T(), activeRecord, inactiveRecord)
	meta, err := newCompactionTargetMeta(s.ctx, catalog)
	s.Require().NoError(err)

	activeTargets := meta.GetActiveCompactionTargets()
	s.Require().Len(activeTargets, 1)
	s.Equal(int64(10), activeTargets[0].Clone().GetTargetID())
	oldTarget := activeTargets[0]
	probe := targetSegmentWithDataTS(1, 100, 10, "ch-1", 1500, 1500, false)
	s.False(oldTarget.Match(probe))

	updatedRecord := proto.Clone(activeRecord).(*datapb.CompactionTarget)
	updatedRecord.ExpectedTS = 2000
	s.Require().NoError(meta.SaveCompactionTarget(s.ctx, updatedRecord))
	activeTargets = meta.GetActiveCompactionTargets()
	s.Require().Len(activeTargets, 1)
	s.True(activeTargets[0].Match(probe))
	s.False(oldTarget.Match(probe))

	s.Require().NoError(meta.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_INACTIVE))
	s.Empty(meta.GetActiveCompactionTargets())

	s.Require().NoError(meta.SaveCompactionTarget(s.ctx, updatedRecord))
	s.Require().Len(meta.GetActiveCompactionTargets(), 1)
	s.Require().NoError(meta.DropCompactionTarget(s.ctx, updatedRecord.GetTargetID()))
	s.Empty(meta.GetActiveCompactionTargets())
}

func (s *CompactionTargetMetaSuite) TestInvalidRewriteTargetPropertiesStayDurableButNotRuntimeActive() {
	record := &datapb.CompactionTarget{
		TargetID:     10,
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Properties: map[string]string{
			compactionTargetPropertySegmentIDs: "not-json",
		},
		ExpectedTS: 1000,
		TailLimit:  0,
		State:      datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	catalog, _, _, _ := newCompactionTargetTestCatalog(s.T(), record)

	meta, err := newCompactionTargetMeta(s.ctx, catalog)

	s.Require().NoError(err)
	s.Equal(datapb.TargetState_TARGET_STATE_ACTIVE, meta.GetCompactionTarget(10).GetState())
	s.Empty(meta.GetActiveCompactionTargets())
}

func TestCompactionTargetFactoryKeepsUnsupportedIntentInert(t *testing.T) {
	target, err := newCompactionTarget(&datapb.CompactionTarget{
		Intent: datapb.TargetIntent_INTENT_SIZE,
		State:  datapb.TargetState_TARGET_STATE_ACTIVE,
	})

	require.ErrorIs(t, err, errUnsupportedCompactionTarget)
	require.NotNil(t, target)
	require.False(t, target.active())
	require.False(t, target.ScopeIn(targetSegment(1, 0, 10, "ch-1", 0, false)))
}

func TestCompactionTargetFactoryKeepsInvalidRewriteTargetInert(t *testing.T) {
	target, err := newCompactionTarget(&datapb.CompactionTarget{
		Intent: datapb.TargetIntent_INTENT_REWRITE,
		Properties: map[string]string{
			compactionTargetPropertySegmentIDs: "not-json",
		},
		State: datapb.TargetState_TARGET_STATE_ACTIVE,
	})

	require.Error(t, err)
	require.NotNil(t, target)
	require.False(t, target.active())
	require.False(t, target.ScopeIn(targetSegment(1, 0, 10, "ch-1", 0, false)))
}

func TestFiniteCompactionTargetMatchUsesRewriteBoundaryPredicate(t *testing.T) {
	record := &datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   1000,
		TailLimit:    0,
	}
	target := mustNewCompactionTarget(t, record)

	outOfScope := targetSegment(1, 200, 10, "ch-1", 900, false)
	require.False(t, target.ScopeIn(outOfScope))
	require.False(t, target.Match(outOfScope))
	require.True(t, target.Match(targetSegmentWithDataTS(2, 100, 10, "ch-1", 0, 999, false)))
	require.False(t, target.Match(targetSegmentWithDataTS(3, 100, 10, "ch-1", 1000, 999, false)))
	require.True(t, target.Match(targetSegmentWithDataTS(4, 100, 10, "ch-1", 999, 1000, false)))
	require.False(t, target.Match(targetSegmentWithDataTS(5, 100, 10, "ch-1", 999, 1001, false)))
	require.True(t, target.Match(targetSegmentWithDataTS(6, 100, 10, "ch-1", 999, 999, false)))
}

func TestCompactionTargetUsesCollectionOnly(t *testing.T) {
	record := &datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   1000,
		TailLimit:    0,
	}
	target := mustNewCompactionTarget(t, record)

	require.True(t, target.ScopeIn(targetSegment(1, 100, 10, "ch-1", 900, false)))
	require.True(t, target.ScopeIn(targetSegment(2, 100, 20, "ch-2", 900, false)))
	require.False(t, target.ScopeIn(targetSegment(3, 200, 10, "ch-1", 900, false)))
}

func TestFiniteCompactionTargetSatisfiedUsesAbsenceOfRewriteMatch(t *testing.T) {
	record := &datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   1000,
		TailLimit:    0,
	}
	target := mustNewCompactionTarget(t, record)

	require.False(t, targetSatisfied(target,
		targetSegmentWithDataTS(1, 100, 10, "ch-1", 0, 999, false),
	))
	require.True(t, targetSatisfied(target,
		targetSegmentWithDataTS(1, 100, 10, "ch-1", 1001, 999, false),
	))
	require.False(t, targetSatisfied(target,
		targetSegmentWithDataTS(1, 100, 10, "ch-1", 0, 1000, false),
	))
	require.True(t, targetSatisfied(target,
		targetSegmentWithDataTS(1, 100, 10, "ch-1", 0, 1001, false),
	))
}

func TestCompactionTargetSatisfiedUsesTailLimitPerLabel(t *testing.T) {
	record := &datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   1000,
		TailLimit:    1,
	}
	target := mustNewCompactionTarget(t, record)

	require.True(t, targetSatisfied(target,
		targetSegmentWithDataTS(1, 100, 10, "ch-1", 0, 999, false),
		targetSegmentWithDataTS(2, 100, 20, "ch-2", 0, 999, false),
	))
	require.False(t, targetSatisfied(target,
		targetSegmentWithDataTS(1, 100, 10, "ch-1", 0, 999, false),
		targetSegmentWithDataTS(2, 100, 10, "ch-1", 0, 998, false),
	))
}

func TestCompactionTargetSatisfiedNeverCompletesStandingTarget(t *testing.T) {
	record := &datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   1000,
		TailLimit:    -1,
	}
	target := mustNewCompactionTarget(t, record)

	require.False(t, targetSatisfied(target))
}

func TestFiniteCompactionTargetSatisfiedDoesNotUseIsCompactingAsCompletionBlocker(t *testing.T) {
	record := &datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   1000,
		TailLimit:    0,
	}
	target := mustNewCompactionTarget(t, record)

	require.False(t, targetSatisfied(target,
		targetSegment(1, 100, 10, "ch-1", 900, true),
	))
}

func TestRewriteCompactionTargetSegmentIDScopeUsesExactLiveSegmentID(t *testing.T) {
	record := &datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Properties:   compactionTargetSegmentIDProperties([]int64{1}),
		ExpectedTS:   1000,
		TailLimit:    0,
	}
	target := mustNewCompactionTarget(t, record)
	oldSegment := targetSegment(1, 100, 10, "ch-1", 900, false)
	replacement := targetSegment(10, 100, 10, "ch-1", 0, false, 1)

	require.True(t, target.ScopeIn(oldSegment))
	require.False(t, target.ScopeIn(replacement))
	require.False(t, targetSatisfied(target,
		oldSegment,
	))
	require.True(t, targetSatisfied(target,
		replacement,
	))
}

func mustNewCompactionTarget(t testing.TB, record *datapb.CompactionTarget) *compactionTarget {
	t.Helper()
	target, err := newCompactionTarget(record)
	require.NoError(t, err)
	return target
}

func targetSatisfied(target *compactionTarget, segments ...*SegmentInfo) bool {
	return target.Satisfied(groupCompactionTargetSegmentsByLabel(target.SegmentsInScope(segments)))
}

type compactionTargetCatalogUpdate struct {
	targetID        int64
	state           datapb.TargetState
	inactivatedAtTS uint64
}

func newCompactionTargetTestCatalog(
	t *testing.T,
	records ...*datapb.CompactionTarget,
) (*metastoremocks.DataCoordCatalog, map[int64]*datapb.CompactionTarget, *[]compactionTargetCatalogUpdate, *[]int64) {
	catalog := metastoremocks.NewDataCoordCatalog(t)
	stored := make(map[int64]*datapb.CompactionTarget)
	for _, record := range records {
		stored[record.GetTargetID()] = proto.Clone(record).(*datapb.CompactionTarget)
	}
	updates := make([]compactionTargetCatalogUpdate, 0)
	dropped := make([]int64, 0)

	catalog.EXPECT().ListCompactionTargets(mock.Anything).RunAndReturn(
		func(context.Context) ([]*datapb.CompactionTarget, error) {
			return cloneCompactionTargetSlice(records), nil
		}).Maybe()
	catalog.EXPECT().SaveCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionTarget) error {
			stored[record.GetTargetID()] = proto.Clone(record).(*datapb.CompactionTarget)
			return nil
		}).Maybe()
	catalog.EXPECT().UpdateCompactionTargetState(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, targetID int64, state datapb.TargetState, inactivatedAtTS uint64) error {
			updates = append(updates, compactionTargetCatalogUpdate{
				targetID:        targetID,
				state:           state,
				inactivatedAtTS: inactivatedAtTS,
			})
			record := proto.Clone(stored[targetID]).(*datapb.CompactionTarget)
			record.State = state
			record.InactivatedAtTS = inactivatedAtTS
			stored[targetID] = record
			return nil
		}).Maybe()
	catalog.EXPECT().DropCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionTarget) error {
			dropped = append(dropped, record.GetTargetID())
			delete(stored, record.GetTargetID())
			return nil
		}).Maybe()

	return catalog, stored, &updates, &dropped
}

func cloneCompactionTargetSlice(records []*datapb.CompactionTarget) []*datapb.CompactionTarget {
	cloned := make([]*datapb.CompactionTarget, 0, len(records))
	for _, record := range records {
		cloned = append(cloned, proto.Clone(record).(*datapb.CompactionTarget))
	}
	return cloned
}

func targetSegment(id, collectionID, partitionID int64, channel string, createTS uint64, compacting bool, compactionFrom ...int64) *SegmentInfo {
	return targetSegmentWithDataTS(id, collectionID, partitionID, channel, createTS, 0, compacting, compactionFrom...)
}

func targetSegmentWithDataTS(id, collectionID, partitionID int64, channel string, createTS uint64, dataTS uint64, compacting bool, compactionFrom ...int64) *SegmentInfo {
	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                  id,
			CollectionID:        collectionID,
			PartitionID:         partitionID,
			InsertChannel:       channel,
			DmlPosition:         &msgpb.MsgPosition{Timestamp: dataTS},
			State:               commonpb.SegmentState_Flushed,
			CreateTs:            createTS,
			CompactionFrom:      compactionFrom,
			NumOfRows:           100,
			IsImporting:         false,
			Level:               datapb.SegmentLevel_L1,
			StorageVersion:      2,
			IsSorted:            false,
			CreatedByCompaction: len(compactionFrom) > 0,
		},
		isCompacting: compacting,
	}
}
