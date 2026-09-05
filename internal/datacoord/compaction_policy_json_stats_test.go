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
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestJSONStatsMigrationPolicySuite(t *testing.T) {
	suite.Run(t, new(JSONStatsMigrationPolicySuite))
}

type JSONStatsMigrationPolicySuite struct {
	suite.Suite

	mockAlloc *allocator.MockAllocator
	handler   *NMockHandler
	policy    *jsonStatsMigrationPolicy
	version   IndexEngineVersionManager
	gate      ScalarIndexMigrationVersionManager
}

func (s *JSONStatsMigrationPolicySuite) SetupTest() {
	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.handler = NewNMockHandler(s.T())
	s.version = newIndexEngineVersionManager()
	s.gate = s.version.(ScalarIndexMigrationVersionManager)
	s.policy = newJSONStatsMigrationPolicy(meta, s.mockAlloc, s.handler, s.version)
	s.version.Startup(map[string]*sessionutil.Session{
		"qn-1": newJSONPathMigrationQueryNode(1, common.MaximumScalarIndexEngineVersion, 1),
	})
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{
		"dn-1": newJSONPathMigrationNode(2, common.MaximumScalarIndexEngineVersion),
	})

	s.saveParam(&paramtable.Get().CommonCfg.EnabledJSONKeyStats, "true")
	s.saveParam(&paramtable.Get().DataCoordCfg.JSONStatsTriggerCount, "10")
	s.saveParam(&paramtable.Get().DataCoordCfg.JSONStatsFormatVersion, "4")
	s.saveParam(&paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens, "10")
	s.saveParam(&paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval, "120")
	s.saveParam(&paramtable.Get().DataCoordCfg.TargetScalarIndexVersion, "-1")
	s.saveParam(&paramtable.Get().DataCoordCfg.ForceRebuildScalarSegmentIndex, "false")
	s.saveParam(&paramtable.Get().DataCoordCfg.TargetVecIndexVersion, "-1")
	s.saveParam(&paramtable.Get().DataCoordCfg.ForceRebuildSegmentIndex, "false")
}

func (s *JSONStatsMigrationPolicySuite) saveParam(item *paramtable.ParamItem, value string) {
	paramtable.Get().Save(item.Key, value)
	s.T().Cleanup(func() {
		paramtable.Get().Reset(item.Key)
	})
}

func (s *JSONStatsMigrationPolicySuite) safeRebuildTargets() safeSegmentRebuildTargets {
	targets, ok := resolveSafeSegmentRebuildTargets(s.version)
	s.Require().True(ok)
	return targets
}

func (s *JSONStatsMigrationPolicySuite) setPolicyMeta(collID int64, coll *collectionInfo, segments map[UniqueID]*SegmentInfo) {
	s.setPolicyMetaWithIndexes(collID, coll, segments, nil, nil)
}

func (s *JSONStatsMigrationPolicySuite) setPolicyMetaWithIndexes(
	collID int64,
	coll *collectionInfo,
	segments map[UniqueID]*SegmentInfo,
	indexes []*model.Index,
	segmentIndexes []*model.SegmentIndex,
) {
	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)
	indexMeta := newSegmentIndexMeta(nil)
	indexMeta.indexes[collID] = make(map[UniqueID]*model.Index)
	for _, index := range indexes {
		indexMeta.indexes[collID][index.IndexID] = index
	}
	for _, segmentIndex := range segmentIndexes {
		byIndexID, ok := indexMeta.segmentIndexes.Get(segmentIndex.SegmentID)
		if !ok {
			byIndexID = typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
			indexMeta.segmentIndexes.Insert(segmentIndex.SegmentID, byIndexID)
		}
		byIndexID.Insert(segmentIndex.IndexID, segmentIndex)
	}
	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
		indexMeta:   indexMeta,
		statsTaskMeta: &statsTaskMeta{
			segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
		},
	}
}

func newJSONStatsMigrationSchema(fieldIDs ...int64) *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}
	for _, fieldID := range fieldIDs {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID:  fieldID,
			Name:     "json",
			DataType: schemapb.DataType_JSON,
		})
	}
	return &schemapb.CollectionSchema{Fields: fields}
}

func newJSONStatsMigrationTestSegment(collID, segmentID int64, level datapb.SegmentLevel, statsByField map[int64]int64) *SegmentInfo {
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collID,
		PartitionID:    10,
		InsertChannel:  "ch-1",
		Level:          level,
		State:          commonpb.SegmentState_Flushed,
		NumOfRows:      10000,
		StorageVersion: storage.StorageV3,
		IsSorted:       true,
	}}
	if len(statsByField) == 0 {
		return segment
	}
	segment.JsonKeyStats = make(map[int64]*datapb.JsonKeyStats, len(statsByField))
	for fieldID, statsFormat := range statsByField {
		segment.JsonKeyStats[fieldID] = &datapb.JsonKeyStats{
			FieldID:                fieldID,
			Version:                1,
			BuildID:                segmentID*100 + fieldID,
			Files:                  []string{"meta.json"},
			JsonKeyStatsDataFormat: statsFormat,
		}
	}
	return segment
}

func viewSegmentIDs(views []CompactionView) []int64 {
	ids := make([]int64, 0, len(views))
	for _, view := range views {
		mixView, ok := view.(*MixSegmentView)
		if !ok || len(mixView.GetSegmentsView()) != 1 {
			continue
		}
		ids = append(ids, mixView.GetSegmentsView()[0].ID)
	}
	return ids
}

func (s *JSONStatsMigrationPolicySuite) TestEnable() {
	s.True(s.policy.Enable())

	// A dedicated one-input rewrite must know both reader and writer ranges,
	// even when this collection currently has no user-defined indexes.
	s.gate.StartupDataNodes(nil)
	s.False(s.policy.Enable())
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{
		"dn-1": newJSONPathMigrationNode(2, common.MaximumScalarIndexEngineVersion),
	})
	s.True(s.policy.Enable())

	// V4 migration is independent of the ordinary auto-compaction and storage
	// migration policies. The server-level enableCompaction switch controls
	// whether the trigger manager itself is started.
	s.saveParam(&paramtable.Get().DataCoordCfg.EnableAutoCompaction, "false")
	s.saveParam(&paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled, "false")
	s.saveParam(&paramtable.Get().DataCoordCfg.StorageFormatCompactionEnabled, "false")
	s.True(s.policy.Enable())

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.JSONStatsFormatVersion.Key, "3")
	s.False(s.policy.Enable())
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.JSONStatsFormatVersion.Key, "4")

	paramtable.Get().Save(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key, "false")
	s.False(s.policy.Enable())
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key, "true")

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.JSONStatsTriggerCount.Key, "0")
	s.False(s.policy.Enable())
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.JSONStatsTriggerCount.Key, "10")
	s.True(s.policy.Enable())
}

func (s *JSONStatsMigrationPolicySuite) TestRegisteredWithTriggerManager() {
	inspector := NewMockCompactionInspector(s.T())
	versionManager := NewMockVersionManager(s.T())
	manager := NewCompactionTriggerManager(s.mockAlloc, s.handler, inspector, s.policy.meta, versionManager)

	registered, ok := manager.policies[JSONStatsMigrationTicker]
	s.Require().True(ok)
	s.IsType(&jsonStatsMigrationPolicy{}, registered)
}

func (s *JSONStatsMigrationPolicySuite) TestRoutesOnlyExistingOldStatsToSingleCompaction() {
	const collID = int64(100)
	fieldIDs := []int64{100, 101}
	coll := &collectionInfo{ID: collID, Schema: newJSONStatsMigrationSchema(fieldIDs...)}
	segments := map[UniqueID]*SegmentInfo{
		// Existing V3 stats require migration.
		201: newJSONStatsMigrationTestSegment(collID, 201, datapb.SegmentLevel_L1, map[int64]int64{100: common.JSONStatsDataFormatV3, 101: common.JSONStatsDataFormatV4}),
		// Existing V4 stats are already current.
		202: newJSONStatsMigrationTestSegment(collID, 202, datapb.SegmentLevel_L1, map[int64]int64{100: common.JSONStatsDataFormatV4, 101: common.JSONStatsDataFormatV4}),
		// Missing stats belong to StatsInspector, not migration compaction.
		203: newJSONStatsMigrationTestSegment(collID, 203, datapb.SegmentLevel_L1, nil),
		// Both L1 and L2 regular segments are eligible.
		204: newJSONStatsMigrationTestSegment(collID, 204, datapb.SegmentLevel_L2, map[int64]int64{100: common.JSONStatsDataFormatV3}),
		// A current field plus a missing field also belongs to StatsInspector.
		205: newJSONStatsMigrationTestSegment(collID, 205, datapb.SegmentLevel_L1, map[int64]int64{100: common.JSONStatsDataFormatV4}),
	}
	s.setPolicyMeta(collID, coll, segments)
	s.handler.EXPECT().GetCollection(mock.Anything, collID).Return(coll, nil).Once()
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil).Once()

	events, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Require().Len(events, 1)
	views, ok := events[TriggerTypeSingle]
	s.Require().True(ok)
	s.ElementsMatch([]int64{201, 204}, viewSegmentIDs(views))
	s.NotContains(events, TriggerTypeStorageVersionUpgrade)
	for _, view := range views {
		s.IsType(&MixSegmentView{}, view)
		s.Len(view.GetSegmentsView(), 1)
	}

	s.True(needsJSONStatsMigration(segments[201], fieldIDs, common.JSONStatsDataFormatV4))
	s.False(needDoJSONKeyIndex(segments[201], fieldIDs, true))
	s.False(needsJSONStatsMigration(segments[203], fieldIDs, common.JSONStatsDataFormatV4))
	s.True(needDoJSONKeyIndex(segments[203], fieldIDs, true))
	// A V3 field plus a missing field is owned entirely by migration compaction;
	// StatsInspector must not race it with an in-place V4 stats task.
	s.True(needsJSONStatsMigration(segments[204], fieldIDs, common.JSONStatsDataFormatV4))
	s.False(needDoJSONKeyIndex(segments[204], fieldIDs, true))
	s.False(needsJSONStatsMigration(segments[205], fieldIDs, common.JSONStatsDataFormatV4))
	s.True(needDoJSONKeyIndex(segments[205], fieldIDs, true))
}

func (s *JSONStatsMigrationPolicySuite) TestStatsInspectorOwnershipAcrossFormats() {
	const collID = int64(100)
	fieldIDs := []int64{100, 101}

	legacy := newJSONStatsMigrationTestSegment(collID, 201, datapb.SegmentLevel_L1, map[int64]int64{100: 0})
	s.True(needsJSONStatsMigration(legacy, fieldIDs, common.JSONStatsDataFormatV4))
	s.False(needDoJSONKeyIndex(legacy, fieldIDs, true))

	currentWithMissing := newJSONStatsMigrationTestSegment(collID, 202, datapb.SegmentLevel_L1,
		map[int64]int64{100: common.JSONStatsDataFormatV4})
	s.True(needDoJSONKeyIndex(currentWithMissing, fieldIDs, true))

	// With the default V3 target, preserve legacy V1/V2 in-place refreshes, but
	// do not downgrade stats written by a newer V4 binary.
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.JSONStatsFormatVersion.Key, "3")
	legacy.JsonKeyStats[100].JsonKeyStatsDataFormat = 2
	s.True(needDoJSONKeyIndex(legacy, fieldIDs, true))
	s.False(needDoJSONKeyIndex(currentWithMissing, fieldIDs, true))

	legacyAndFuture := newJSONStatsMigrationTestSegment(collID, 203, datapb.SegmentLevel_L1,
		map[int64]int64{100: 2, 101: common.JSONStatsDataFormatV4})
	s.False(needDoJSONKeyIndex(legacyAndFuture, fieldIDs, true), "newer stats must prevent an order-dependent downgrade")
}

func (s *JSONStatsMigrationPolicySuite) TestMigrationDoesNotDowngradeFutureStats() {
	segment := newJSONStatsMigrationTestSegment(100, 201, datapb.SegmentLevel_L1,
		map[int64]int64{
			100: common.JSONStatsDataFormatV3,
			101: common.JSONStatsDataFormatV4 + 1,
		})

	// A single compaction replaces stats for every field. The presence of any
	// future-format artifact must therefore block migration, regardless of field
	// iteration order.
	s.False(needsJSONStatsMigration(segment, []int64{100, 101}, common.JSONStatsDataFormatV4))
	s.False(needsJSONStatsMigration(segment, []int64{101, 100}, common.JSONStatsDataFormatV4))
}

func (s *JSONStatsMigrationPolicySuite) TestSegmentFilters() {
	const collID = int64(100)
	coll := &collectionInfo{ID: collID, Schema: newJSONStatsMigrationSchema(100)}
	oldStats := map[int64]int64{100: common.JSONStatsDataFormatV3}
	segments := map[UniqueID]*SegmentInfo{
		201: newJSONStatsMigrationTestSegment(collID, 201, datapb.SegmentLevel_L1, oldStats),
		202: newJSONStatsMigrationTestSegment(collID, 202, datapb.SegmentLevel_L2, oldStats),
		203: newJSONStatsMigrationTestSegment(collID, 203, datapb.SegmentLevel_L1, oldStats),
		204: newJSONStatsMigrationTestSegment(collID, 204, datapb.SegmentLevel_L1, oldStats),
		205: newJSONStatsMigrationTestSegment(collID, 205, datapb.SegmentLevel_L0, oldStats),
		206: newJSONStatsMigrationTestSegment(collID, 206, datapb.SegmentLevel_L1, oldStats),
		207: newJSONStatsMigrationTestSegment(collID, 207, datapb.SegmentLevel_L1, oldStats),
		208: newJSONStatsMigrationTestSegment(collID, 208, datapb.SegmentLevel_L1, oldStats),
		209: newJSONStatsMigrationTestSegment(collID, 209, datapb.SegmentLevel_L2, oldStats),
	}
	segments[203].isCompacting = true
	segments[204].IsImporting = true
	segments[206].State = commonpb.SegmentState_Dropped
	// Clustering results stay invisible until their stats and indexes are ready.
	// Migration must not replace a segment still owned by that lifecycle.
	segments[209].IsInvisible = true
	s.setPolicyMeta(collID, coll, segments)
	s.policy.meta.snapshotMeta = &snapshotMeta{
		compactionBlockedCollections: typeutil.NewUniqueSet(),
		snapshotPendingCollections:   typeutil.NewUniqueSet(),
		segmentProtectionUntil:       map[int64]uint64{207: uint64(time.Now().Add(time.Hour).Unix())},
	}
	s.policy.meta.statsTaskMeta.segmentID2Tasks.Insert(
		createSecondaryIndexKey(208, indexpb.StatsSubJob_JsonKeyIndexJob.String()),
		&indexpb.StatsTask{SegmentID: 208, SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob},
	)
	s.handler.EXPECT().GetCollection(mock.Anything, collID).Return(coll, nil).Once()
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil).Once()

	views, err := s.policy.triggerOneCollection(
		context.Background(), collID, 10, common.JSONStatsDataFormatV4, s.safeRebuildTargets())
	s.NoError(err)
	s.ElementsMatch([]int64{201, 202}, viewSegmentIDs(views))
}

func (s *JSONStatsMigrationPolicySuite) TestCollectionFilters() {
	const collID = int64(100)
	oldStats := map[int64]int64{100: common.JSONStatsDataFormatV3}

	s.Run("external collection", func() {
		coll := &collectionInfo{ID: collID, Schema: newJSONStatsMigrationSchema(100)}
		coll.Schema.Fields[0].ExternalField = "pk"
		s.handler.EXPECT().GetCollection(mock.Anything, collID).Return(coll, nil).Once()
		views, err := s.policy.triggerOneCollection(
			context.Background(), collID, 10, common.JSONStatsDataFormatV4, s.safeRebuildTargets())
		s.NoError(err)
		s.Empty(views)
		s.Zero(s.policy.currentCount)
	})

	s.Run("collection without JSON stats fields", func() {
		coll := &collectionInfo{ID: collID, Schema: newJSONStatsMigrationSchema()}
		s.handler.EXPECT().GetCollection(mock.Anything, collID).Return(coll, nil).Once()
		views, err := s.policy.triggerOneCollection(
			context.Background(), collID, 10, common.JSONStatsDataFormatV4, s.safeRebuildTargets())
		s.NoError(err)
		s.Empty(views)
		s.Zero(s.policy.currentCount)
	})

	s.Run("snapshot-blocked collection", func() {
		coll := &collectionInfo{ID: collID, Schema: newJSONStatsMigrationSchema(100)}
		s.setPolicyMeta(collID, coll, map[UniqueID]*SegmentInfo{
			201: newJSONStatsMigrationTestSegment(collID, 201, datapb.SegmentLevel_L1, oldStats),
		})
		s.policy.meta.snapshotMeta = &snapshotMeta{
			compactionBlockedCollections: typeutil.NewUniqueSet(collID),
			snapshotPendingCollections:   typeutil.NewUniqueSet(),
			segmentProtectionUntil:       map[int64]uint64{},
		}
		events, err := s.policy.Trigger(context.Background())
		s.NoError(err)
		s.Empty(events[TriggerTypeSingle])
		s.Zero(s.policy.currentCount)
	})
}

func (s *JSONStatsMigrationPolicySuite) TestRateLimit() {
	const collID = int64(100)
	s.saveParam(&paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens, "2")
	s.saveParam(&paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval, "60")
	coll := &collectionInfo{ID: collID, Schema: newJSONStatsMigrationSchema(100)}
	segments := make(map[UniqueID]*SegmentInfo)
	for id := int64(201); id <= 205; id++ {
		segments[id] = newJSONStatsMigrationTestSegment(collID, id, datapb.SegmentLevel_L1,
			map[int64]int64{100: common.JSONStatsDataFormatV3})
	}
	s.setPolicyMeta(collID, coll, segments)
	s.handler.EXPECT().GetCollection(mock.Anything, collID).Return(coll, nil).Twice()
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil).Once()
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()

	first, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Len(first[TriggerTypeSingle], 2)
	s.Equal(2, s.policy.currentCount)

	second, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(second[TriggerTypeSingle])

	s.policy.lastPeriod = time.Now().Add(-time.Minute - time.Second)
	third, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Len(third[TriggerTypeSingle], 2)
	s.Equal(2, s.policy.currentCount)
}

func (s *JSONStatsMigrationPolicySuite) TestZeroRateLimitDisablesMigration() {
	s.saveParam(&paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens, "0")
	const collID = int64(100)
	coll := &collectionInfo{ID: collID, Schema: newJSONStatsMigrationSchema(100)}
	s.setPolicyMeta(collID, coll, map[UniqueID]*SegmentInfo{
		201: newJSONStatsMigrationTestSegment(collID, 201, datapb.SegmentLevel_L1,
			map[int64]int64{100: common.JSONStatsDataFormatV3}),
	})

	events, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(events[TriggerTypeSingle])
}
