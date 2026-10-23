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

package targets

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
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
	catalog, _, _, _ := NewCompactionTargetTestCatalog(s.T(),
		&datapb.CompactionTarget{
			TargetID:     1,
			CollectionID: 100,
			Intent:       datapb.TargetIntent_INTENT_REWRITE,
			State:        datapb.TargetState_TARGET_STATE_ACTIVE,
		},
		&datapb.CompactionTarget{
			TargetID:     2,
			CollectionID: 100,
			Intent:       datapb.TargetIntent_INTENT_REWRITE,
			State:        datapb.TargetState_TARGET_STATE_INACTIVE,
		},
		&datapb.CompactionTarget{
			TargetID:     3,
			CollectionID: 100,
			Intent:       datapb.TargetIntent_INTENT_REWRITE,
			State:        datapb.TargetState_TARGET_STATE_INACTIVE,
		},
		&datapb.CompactionTarget{
			TargetID:     4,
			CollectionID: 100,
			Intent:       datapb.TargetIntent_INTENT_REWRITE,
			State:        datapb.TargetState_TARGET_STATE_INACTIVE,
		},
		&datapb.CompactionTarget{
			TargetID: 5,
			Intent:   datapb.TargetIntent(999),
			State:    datapb.TargetState_TARGET_STATE_ACTIVE,
		},
	)

	mt, err := NewCompactionTargetMeta(s.ctx, catalog)

	s.Require().NoError(err)
	s.Len(mt.GetCompactionTargets(), 5)
	s.Equal(datapb.TargetState_TARGET_STATE_ACTIVE, mt.GetCompactionTarget(1).GetState())
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, mt.GetCompactionTarget(2).GetState())
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, mt.GetCompactionTarget(3).GetState())
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, mt.GetCompactionTarget(4).GetState())
	s.Equal(datapb.TargetIntent(999), mt.GetCompactionTarget(5).GetIntent())
}

func (s *CompactionTargetMetaSuite) TestReturnsClones() {
	catalog, _, _, _ := NewCompactionTargetTestCatalog(s.T(), &datapb.CompactionTarget{
		TargetID:     10,
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
		Properties: map[string]string{
			"maxSize": "1024",
		},
	})

	mt, err := NewCompactionTargetMeta(s.ctx, catalog)
	s.Require().NoError(err)

	record := mt.GetCompactionTarget(10)
	record.State = datapb.TargetState_TARGET_STATE_INACTIVE
	record.Properties["maxSize"] = "2048"

	stored := mt.GetCompactionTarget(10)
	s.Equal(datapb.TargetState_TARGET_STATE_ACTIVE, stored.GetState())
	s.Equal("1024", stored.GetProperties()["maxSize"])
}

func (s *CompactionTargetMetaSuite) TestSaveUpdateDrop() {
	catalog, records, updates, dropped := NewCompactionTargetTestCatalog(s.T())
	mt, err := NewCompactionTargetMeta(s.ctx, catalog)
	s.Require().NoError(err)

	record := &datapb.CompactionTarget{
		TargetID:     10,
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}

	s.Require().NoError(mt.SaveCompactionTarget(s.ctx, record))
	s.True(proto.Equal(record, mt.GetCompactionTarget(10)))
	s.True(proto.Equal(record, records[10]))

	s.Require().NoError(mt.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_INACTIVE))
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, mt.GetCompactionTarget(10).GetState())
	s.NotZero(mt.GetCompactionTarget(10).GetInactivatedAtTS())
	s.Require().Len(*updates, 1)
	s.Equal(int64(10), (*updates)[0].targetID)
	s.Equal(datapb.TargetState_TARGET_STATE_INACTIVE, (*updates)[0].state)
	s.NotZero((*updates)[0].inactivatedAtTS)

	s.Require().NoError(mt.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_INACTIVE))
	s.Require().Len(*updates, 1)

	s.Require().NoError(mt.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_ACTIVE))
	s.Equal(datapb.TargetState_TARGET_STATE_ACTIVE, mt.GetCompactionTarget(10).GetState())
	s.Zero(mt.GetCompactionTarget(10).GetInactivatedAtTS())
	s.Require().Len(*updates, 2)
	s.Equal(compactionTargetCatalogUpdate{
		targetID:        10,
		state:           datapb.TargetState_TARGET_STATE_ACTIVE,
		inactivatedAtTS: 0,
	}, (*updates)[1])

	s.Require().NoError(mt.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_ACTIVE))
	s.Require().Len(*updates, 2)

	s.Require().NoError(mt.DropCompactionTarget(s.ctx, record.GetTargetID()))
	s.Nil(mt.GetCompactionTarget(10))
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
	catalog, _, _, _ := NewCompactionTargetTestCatalog(s.T(), activeRecord, inactiveRecord)
	mt, err := NewCompactionTargetMeta(s.ctx, catalog)
	s.Require().NoError(err)

	activeTargets := mt.GetActiveCompactionTargets()
	s.Require().Len(activeTargets, 1)
	s.Contains(activeTargets, int64(10))
	oldTarget := activeTargets[10]
	probe := targetSegmentWithDataTS(1, 100, 10, "ch-1", 1500, 1500, false)
	s.False(targetMatchesSegment(oldTarget, probe))

	updatedRecord := proto.Clone(activeRecord).(*datapb.CompactionTarget)
	updatedRecord.ExpectedTS = 2000
	s.Require().NoError(mt.SaveCompactionTarget(s.ctx, updatedRecord))
	activeTargets = mt.GetActiveCompactionTargets()
	s.Require().Len(activeTargets, 1)
	s.True(targetMatchesSegment(activeTargets[10], probe))
	s.False(targetMatchesSegment(oldTarget, probe))

	s.Require().NoError(mt.UpdateCompactionTargetState(s.ctx, 10, datapb.TargetState_TARGET_STATE_INACTIVE))
	s.Empty(mt.GetActiveCompactionTargets())

	s.Require().NoError(mt.SaveCompactionTarget(s.ctx, updatedRecord))
	s.Require().Len(mt.GetActiveCompactionTargets(), 1)
	s.Require().NoError(mt.DropCompactionTarget(s.ctx, updatedRecord.GetTargetID()))
	s.Empty(mt.GetActiveCompactionTargets())
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
	catalog, _, _, _ := NewCompactionTargetTestCatalog(s.T(), record)

	mt, err := NewCompactionTargetMeta(s.ctx, catalog)

	s.Require().NoError(err)
	s.Equal(datapb.TargetState_TARGET_STATE_ACTIVE, mt.GetCompactionTarget(10).GetState())
	s.Empty(mt.GetActiveCompactionTargets())
}

func TestCompactionTargetFactoryKeepsUnsupportedIntentInert(t *testing.T) {
	target, err := newCompactionTarget(&datapb.CompactionTarget{
		Intent: datapb.TargetIntent_INTENT_SIZE,
		State:  datapb.TargetState_TARGET_STATE_ACTIVE,
	})

	require.ErrorIs(t, err, errUnsupportedCompactionTarget)
	require.NotNil(t, target)
	require.False(t, target.active())
	require.False(t, targetMatchesSegment(target, targetSegment(1, 0, 10, "ch-1", 0, false)))
}

func TestManualRewriteCompactionTargetRequiresCollectionScope(t *testing.T) {
	record, err := NewManualRewriteCompactionTarget(0, nil).Create(
		context.Background(),
		allocator.NewMockAllocator(t),
	)

	require.Error(t, err)
	require.Nil(t, record)
}

func TestCompactionTargetFactoryKeepsFiniteGlobalTargetInert(t *testing.T) {
	target, err := newCompactionTarget(&datapb.CompactionTarget{
		Intent:    datapb.TargetIntent_INTENT_REWRITE,
		TailLimit: 0,
		State:     datapb.TargetState_TARGET_STATE_ACTIVE,
	})

	require.Error(t, err)
	require.NotNil(t, target)
	require.False(t, target.active())
}

func TestCompactionTargetFactoryKeepsInvalidRewriteTargetInert(t *testing.T) {
	target, err := newCompactionTarget(&datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Properties: map[string]string{
			compactionTargetPropertySegmentIDs: "not-json",
		},
		State: datapb.TargetState_TARGET_STATE_ACTIVE,
	})

	require.Error(t, err)
	require.NotNil(t, target)
	require.False(t, target.active())
	require.False(t, targetMatchesSegment(target, targetSegment(1, 0, 10, "ch-1", 0, false)))
}

func TestRewriteCompactionTargetProvidesExecutionType(t *testing.T) {
	target := mustNewCompactionTarget(t, &datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
	})

	require.Equal(t, datapb.CompactionType_MixCompaction, target.CompactionType())
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
	require.False(t, targetMatchesSegment(target, outOfScope))
	require.True(t, targetMatchesSegment(target, targetSegmentWithDataTS(2, 100, 10, "ch-1", 0, 999, false)))
	require.False(t, targetMatchesSegment(target, targetSegmentWithDataTS(3, 100, 10, "ch-1", 1000, 999, false)))
	require.True(t, targetMatchesSegment(target, targetSegmentWithDataTS(4, 100, 10, "ch-1", 999, 1000, false)))
	require.False(t, targetMatchesSegment(target, targetSegmentWithDataTS(5, 100, 10, "ch-1", 999, 1001, false)))
	require.True(t, targetMatchesSegment(target, targetSegmentWithDataTS(6, 100, 10, "ch-1", 999, 999, false)))
}

func TestRewriteCompactionTargetMatchFiltersOwnManualSegmentDomain(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*meta.SegmentInfo)
		want   bool
	}{
		{
			name: "healthy flushed L1 segment",
			want: true,
		},
		{
			name: "flushing segment",
			mutate: func(segment *meta.SegmentInfo) {
				segment.State = commonpb.SegmentState_Flushing
			},
		},
		{
			name: "dropped segment",
			mutate: func(segment *meta.SegmentInfo) {
				segment.State = commonpb.SegmentState_Dropped
			},
		},
		{
			name: "importing segment",
			mutate: func(segment *meta.SegmentInfo) {
				segment.IsImporting = true
			},
		},
		{
			name: "L0 segment",
			mutate: func(segment *meta.SegmentInfo) {
				segment.Level = datapb.SegmentLevel_L0
			},
		},
		{
			name: "L2 segment",
			mutate: func(segment *meta.SegmentInfo) {
				segment.Level = datapb.SegmentLevel_L2
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := mustNewCompactionTarget(t, &datapb.CompactionTarget{
				CollectionID: 100,
				Intent:       datapb.TargetIntent_INTENT_REWRITE,
				ExpectedTS:   1000,
				TailLimit:    0,
			})
			segment := targetSegment(1, 100, 10, "ch-1", 999, false)
			if test.mutate != nil {
				test.mutate(segment)
			}

			require.Equal(t, test.want, targetMatchesSegment(target, segment))
		})
	}
}

func TestCompactionTargetUsesCollectionOnly(t *testing.T) {
	record := &datapb.CompactionTarget{
		CollectionID: 100,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   1000,
		TailLimit:    0,
	}
	target := mustNewCompactionTarget(t, record)

	require.True(t, targetMatchesSegment(target, targetSegment(1, 100, 10, "ch-1", 900, false)))
	require.True(t, targetMatchesSegment(target, targetSegment(2, 100, 20, "ch-2", 900, false)))
	require.False(t, targetMatchesSegment(target, targetSegment(3, 200, 10, "ch-1", 900, false)))
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

func TestStandingCompactionTargetMayUseGlobalScopeAndNeverCompletes(t *testing.T) {
	record := &datapb.CompactionTarget{
		Intent:     datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS: 1000,
		TailLimit:  -1,
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

	require.True(t, targetMatchesSegment(target, oldSegment))
	require.False(t, targetMatchesSegment(target, replacement))
	require.False(t, targetSatisfied(target,
		oldSegment,
	))
	require.True(t, targetSatisfied(target,
		replacement,
	))
}

func mustNewCompactionTarget(t testing.TB, record *datapb.CompactionTarget) compactionTarget {
	t.Helper()
	target, err := newCompactionTarget(record)
	require.NoError(t, err)
	return target
}

func targetSatisfied(target compactionTarget, segments ...*meta.SegmentInfo) bool {
	return target.Satisfied(filterTargetMatches(target, segments...))
}

func targetMatchesSegment(target compactionTarget, segment *meta.SegmentInfo) bool {
	return len(filterTargetMatches(target, segment)) == 1
}

func filterTargetMatches(target compactionTarget, segments ...*meta.SegmentInfo) []*meta.SegmentInfo {
	filters := target.MatchFilters()
	matches := make([]*meta.SegmentInfo, 0, len(segments))
	for _, segment := range segments {
		matched := true
		for _, filter := range filters {
			if !filter.Match(segment) {
				matched = false
				break
			}
		}
		if matched {
			matches = append(matches, segment)
		}
	}
	return matches
}

func targetSegment(id, collectionID, partitionID int64, channel string, createTS uint64, compacting bool, compactionFrom ...int64) *meta.SegmentInfo {
	return targetSegmentWithDataTS(id, collectionID, partitionID, channel, createTS, 0, compacting, compactionFrom...)
}

func targetSegmentWithDataTS(id, collectionID, partitionID int64, channel string, createTS uint64, dataTS uint64, compacting bool, compactionFrom ...int64) *meta.SegmentInfo {
	return &meta.SegmentInfo{
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
		IsCompacting: compacting,
	}
}
