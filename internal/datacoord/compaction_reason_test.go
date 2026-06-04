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
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCompactionReasonMetaSuite(t *testing.T) {
	suite.Run(t, new(CompactionReasonMetaSuite))
}

type CompactionReasonMetaSuite struct {
	suite.Suite

	ctx context.Context
}

func (s *CompactionReasonMetaSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *CompactionReasonMetaSuite) TestReloadRetainsAllRecordStates() {
	catalog, _, _, _ := newCompactionReasonTestCatalog(s.T(),
		&datapb.CompactionReasonRecord{
			ReasonID:   1,
			ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
			State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
		},
		&datapb.CompactionReasonRecord{
			ReasonID:   2,
			ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
			State:      datapb.CompactionReasonState_REASON_STATE_INACTIVE,
		},
		&datapb.CompactionReasonRecord{
			ReasonID:   3,
			ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
			State:      datapb.CompactionReasonState_REASON_STATE_DROPPED,
		},
		&datapb.CompactionReasonRecord{
			ReasonID:   4,
			ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
			State:      datapb.CompactionReasonState_REASON_STATE_DONE,
		},
		&datapb.CompactionReasonRecord{
			ReasonID:   5,
			ReasonType: datapb.CompactionReasonType(999),
			State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
		},
	)

	meta, err := newCompactionReasonMeta(s.ctx, catalog)

	s.Require().NoError(err)
	s.Len(meta.GetCompactionReasonRecords(), 5)
	s.Equal(datapb.CompactionReasonState_REASON_STATE_ACTIVE, meta.GetCompactionReasonRecord(1).GetState())
	s.Equal(datapb.CompactionReasonState_REASON_STATE_INACTIVE, meta.GetCompactionReasonRecord(2).GetState())
	s.Equal(datapb.CompactionReasonState_REASON_STATE_DROPPED, meta.GetCompactionReasonRecord(3).GetState())
	s.Equal(datapb.CompactionReasonState_REASON_STATE_DONE, meta.GetCompactionReasonRecord(4).GetState())
	s.Equal(datapb.CompactionReasonType(999), meta.GetCompactionReasonRecord(5).GetReasonType())
}

func (s *CompactionReasonMetaSuite) TestReturnsClones() {
	catalog, _, _, _ := newCompactionReasonTestCatalog(s.T(), &datapb.CompactionReasonRecord{
		ReasonID:   10,
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
		Properties: map[string]string{
			"maxSize": "1024",
		},
	})

	meta, err := newCompactionReasonMeta(s.ctx, catalog)
	s.Require().NoError(err)

	record := meta.GetCompactionReasonRecord(10)
	record.State = datapb.CompactionReasonState_REASON_STATE_DROPPED
	record.Properties["maxSize"] = "2048"

	stored := meta.GetCompactionReasonRecord(10)
	s.Equal(datapb.CompactionReasonState_REASON_STATE_ACTIVE, stored.GetState())
	s.Equal("1024", stored.GetProperties()["maxSize"])
}

func (s *CompactionReasonMetaSuite) TestSaveUpdateDrop() {
	catalog, records, updates, dropped := newCompactionReasonTestCatalog(s.T())
	meta, err := newCompactionReasonMeta(s.ctx, catalog)
	s.Require().NoError(err)

	record := &datapb.CompactionReasonRecord{
		ReasonID:   10,
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
	}

	s.Require().NoError(meta.SaveCompactionReasonRecord(s.ctx, record))
	s.True(proto.Equal(record, meta.GetCompactionReasonRecord(10)))
	s.True(proto.Equal(record, records[10]))

	s.Require().NoError(meta.UpdateCompactionReasonRecordState(s.ctx, 10, datapb.CompactionReasonState_REASON_STATE_INACTIVE, 50))
	s.Equal(datapb.CompactionReasonState_REASON_STATE_INACTIVE, meta.GetCompactionReasonRecord(10).GetState())
	s.Zero(meta.GetCompactionReasonRecord(10).GetDroppedAtTS())
	s.Require().Len(*updates, 1)
	s.Equal(compactionReasonCatalogUpdate{
		reasonID:    10,
		state:       datapb.CompactionReasonState_REASON_STATE_INACTIVE,
		droppedAtTS: 0,
	}, (*updates)[0])

	s.Require().NoError(meta.UpdateCompactionReasonRecordState(s.ctx, 10, datapb.CompactionReasonState_REASON_STATE_DONE, 50))
	s.Equal(datapb.CompactionReasonState_REASON_STATE_DONE, meta.GetCompactionReasonRecord(10).GetState())
	s.Zero(meta.GetCompactionReasonRecord(10).GetDroppedAtTS())
	s.Require().Len(*updates, 2)
	s.Equal(compactionReasonCatalogUpdate{
		reasonID:    10,
		state:       datapb.CompactionReasonState_REASON_STATE_DONE,
		droppedAtTS: 0,
	}, (*updates)[1])

	s.Require().NoError(meta.UpdateCompactionReasonRecordState(s.ctx, 10, datapb.CompactionReasonState_REASON_STATE_DROPPED, 50))
	s.Equal(datapb.CompactionReasonState_REASON_STATE_DROPPED, meta.GetCompactionReasonRecord(10).GetState())
	s.Equal(uint64(50), meta.GetCompactionReasonRecord(10).GetDroppedAtTS())
	s.Require().Len(*updates, 3)
	s.Equal(compactionReasonCatalogUpdate{
		reasonID:    10,
		state:       datapb.CompactionReasonState_REASON_STATE_DROPPED,
		droppedAtTS: 50,
	}, (*updates)[2])

	s.Require().NoError(meta.DropCompactionReasonRecord(s.ctx, record))
	s.Nil(meta.GetCompactionReasonRecord(10))
	s.Equal([]int64{10}, *dropped)
}

func (s *CompactionReasonMetaSuite) TestMaterializesReasonsOnRecordMutation() {
	activeRecord := &datapb.CompactionReasonRecord{
		ReasonID:   10,
		Scope:      compactionReasonScope(100, 0, ""),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 1000,
		TailLimit:  0,
		State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
	}
	inactiveRecord := &datapb.CompactionReasonRecord{
		ReasonID:   20,
		Scope:      compactionReasonScope(100, 0, ""),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 1000,
		TailLimit:  0,
		State:      datapb.CompactionReasonState_REASON_STATE_INACTIVE,
	}
	catalog, _, _, _ := newCompactionReasonTestCatalog(s.T(), activeRecord, inactiveRecord)
	meta, err := newCompactionReasonMeta(s.ctx, catalog)
	s.Require().NoError(err)

	activeReasons := meta.GetActiveCompactionReasons()
	s.Require().Len(activeReasons, 1)
	s.Equal(int64(10), activeReasons[0].Record().GetReasonID())
	oldReason := activeReasons[0]
	probe := reasonSegmentWithDataTS(1, 100, 10, "ch-1", 1500, 1500, false)
	s.False(oldReason.Match(probe))

	updatedRecord := proto.Clone(activeRecord).(*datapb.CompactionReasonRecord)
	updatedRecord.ExpectedTS = 2000
	s.Require().NoError(meta.SaveCompactionReasonRecord(s.ctx, updatedRecord))
	activeReasons = meta.GetActiveCompactionReasons()
	s.Require().Len(activeReasons, 1)
	s.True(activeReasons[0].Match(probe))
	s.False(oldReason.Match(probe))

	s.Require().NoError(meta.UpdateCompactionReasonRecordState(s.ctx, 10, datapb.CompactionReasonState_REASON_STATE_DONE, 0))
	s.Empty(meta.GetActiveCompactionReasons())

	s.Require().NoError(meta.SaveCompactionReasonRecord(s.ctx, updatedRecord))
	s.Require().Len(meta.GetActiveCompactionReasons(), 1)
	s.Require().NoError(meta.DropCompactionReasonRecord(s.ctx, updatedRecord))
	s.Empty(meta.GetActiveCompactionReasons())
}

func TestFiniteCompactionReasonMatchUsesRewriteBoundaryPredicate(t *testing.T) {
	record := &datapb.CompactionReasonRecord{
		Scope:      compactionReasonScope(100, 0, ""),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 1000,
		TailLimit:  0,
	}
	reason := newCompactionReason(record)

	outOfScope := reasonSegment(1, 200, 10, "ch-1", 900, false)
	require.False(t, reason.ScopeIn(outOfScope))
	require.False(t, reason.Match(outOfScope))
	require.True(t, reason.Match(reasonSegmentWithDataTS(2, 100, 10, "ch-1", 0, 999, false)))
	require.False(t, reason.Match(reasonSegmentWithDataTS(3, 100, 10, "ch-1", 1000, 999, false)))
	require.False(t, reason.Match(reasonSegmentWithDataTS(4, 100, 10, "ch-1", 999, 1000, false)))
	require.True(t, reason.Match(reasonSegmentWithDataTS(5, 100, 10, "ch-1", 999, 999, false)))
}

func TestCompactionReasonScopeIncludesAllPartitionsID(t *testing.T) {
	record := &datapb.CompactionReasonRecord{
		Scope:      compactionReasonScope(100, common.AllPartitionsID, ""),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 1000,
		TailLimit:  0,
	}
	reason := newCompactionReason(record)

	require.True(t, reason.ScopeIn(reasonSegment(1, 100, 10, "ch-1", 900, false)))
	require.True(t, reason.ScopeIn(reasonSegment(2, 100, 20, "ch-2", 900, false)))
	require.False(t, reason.ScopeIn(reasonSegment(3, 200, 10, "ch-1", 900, false)))
}

func TestFiniteCompactionReasonSatisfiedUsesAbsenceOfRewriteMatch(t *testing.T) {
	record := &datapb.CompactionReasonRecord{
		Scope:      compactionReasonScope(100, 0, ""),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 1000,
		TailLimit:  0,
	}
	reason := newCompactionReason(record)

	require.False(t, reason.ReasonSatisfied(reason.SegmentsInScope([]*SegmentInfo{
		reasonSegmentWithDataTS(1, 100, 10, "ch-1", 0, 999, false),
	})))
	require.True(t, reason.ReasonSatisfied(reason.SegmentsInScope([]*SegmentInfo{
		reasonSegmentWithDataTS(1, 100, 10, "ch-1", 1001, 999, false),
	})))
	require.True(t, reason.ReasonSatisfied(reason.SegmentsInScope([]*SegmentInfo{
		reasonSegmentWithDataTS(1, 100, 10, "ch-1", 0, 1000, false),
	})))
}

func TestFiniteCompactionReasonSatisfiedDoesNotUseIsCompactingAsCompletionBlocker(t *testing.T) {
	record := &datapb.CompactionReasonRecord{
		Scope:      compactionReasonScope(100, 0, ""),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 1000,
		TailLimit:  0,
	}
	reason := newCompactionReason(record)

	require.False(t, reason.ReasonSatisfied(reason.SegmentsInScope([]*SegmentInfo{
		reasonSegment(1, 100, 10, "ch-1", 900, true),
	})))
}

func TestRewriteCompactionReasonSegmentIDScopeUsesExactLiveSegmentID(t *testing.T) {
	record := &datapb.CompactionReasonRecord{
		Scope:      compactionReasonScope(100, 10, "ch-1", 1),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 1000,
		TailLimit:  0,
	}
	reason := newCompactionReason(record)
	oldSegment := reasonSegment(1, 100, 10, "ch-1", 900, false)
	replacement := reasonSegment(10, 100, 10, "ch-1", 0, false, 1)

	require.True(t, reason.ScopeIn(oldSegment))
	require.False(t, reason.ScopeIn(replacement))
	require.False(t, reason.ReasonSatisfied(reason.SegmentsInScope([]*SegmentInfo{
		oldSegment,
	})))
	require.True(t, reason.ReasonSatisfied(reason.SegmentsInScope([]*SegmentInfo{
		replacement,
	})))
}

type compactionReasonCatalogUpdate struct {
	reasonID    int64
	state       datapb.CompactionReasonState
	droppedAtTS uint64
}

func newCompactionReasonTestCatalog(
	t *testing.T,
	records ...*datapb.CompactionReasonRecord,
) (*metastoremocks.DataCoordCatalog, map[int64]*datapb.CompactionReasonRecord, *[]compactionReasonCatalogUpdate, *[]int64) {
	catalog := metastoremocks.NewDataCoordCatalog(t)
	stored := make(map[int64]*datapb.CompactionReasonRecord)
	for _, record := range records {
		stored[record.GetReasonID()] = proto.Clone(record).(*datapb.CompactionReasonRecord)
	}
	updates := make([]compactionReasonCatalogUpdate, 0)
	dropped := make([]int64, 0)

	catalog.EXPECT().ListCompactionReasonRecords(mock.Anything).RunAndReturn(
		func(context.Context) ([]*datapb.CompactionReasonRecord, error) {
			return cloneCompactionReasonRecordSlice(records), nil
		}).Maybe()
	catalog.EXPECT().SaveCompactionReasonRecord(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionReasonRecord) error {
			stored[record.GetReasonID()] = proto.Clone(record).(*datapb.CompactionReasonRecord)
			return nil
		}).Maybe()
	catalog.EXPECT().UpdateCompactionReasonRecordState(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, reasonID int64, state datapb.CompactionReasonState, droppedAtTS uint64) error {
			updates = append(updates, compactionReasonCatalogUpdate{
				reasonID:    reasonID,
				state:       state,
				droppedAtTS: droppedAtTS,
			})
			record := proto.Clone(stored[reasonID]).(*datapb.CompactionReasonRecord)
			record.State = state
			record.DroppedAtTS = droppedAtTS
			stored[reasonID] = record
			return nil
		}).Maybe()
	catalog.EXPECT().DropCompactionReasonRecord(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, record *datapb.CompactionReasonRecord) error {
			dropped = append(dropped, record.GetReasonID())
			delete(stored, record.GetReasonID())
			return nil
		}).Maybe()

	return catalog, stored, &updates, &dropped
}

func cloneCompactionReasonRecordSlice(records []*datapb.CompactionReasonRecord) []*datapb.CompactionReasonRecord {
	cloned := make([]*datapb.CompactionReasonRecord, 0, len(records))
	for _, record := range records {
		cloned = append(cloned, proto.Clone(record).(*datapb.CompactionReasonRecord))
	}
	return cloned
}

func compactionReasonScope(collectionID, partitionID int64, channel string, segmentIDs ...int64) *datapb.CompactionReasonScope {
	return &datapb.CompactionReasonScope{
		CollectionID: collectionID,
		PartitionID:  partitionID,
		Channel:      channel,
		SegmentIDs:   segmentIDs,
	}
}

func reasonSegment(id, collectionID, partitionID int64, channel string, createTS uint64, compacting bool, compactionFrom ...int64) *SegmentInfo {
	return reasonSegmentWithDataTS(id, collectionID, partitionID, channel, createTS, 0, compacting, compactionFrom...)
}

func reasonSegmentWithDataTS(id, collectionID, partitionID int64, channel string, createTS uint64, dataTS uint64, compacting bool, compactionFrom ...int64) *SegmentInfo {
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
