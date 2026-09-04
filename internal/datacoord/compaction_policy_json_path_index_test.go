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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestJSONPathIndexMigrationPolicySuite(t *testing.T) {
	suite.Run(t, new(JSONPathIndexMigrationPolicySuite))
}

type JSONPathIndexMigrationPolicySuite struct {
	suite.Suite

	mockAlloc *allocator.MockAllocator
	handler   *NMockHandler
	policy    *jsonPathIndexMigrationPolicy
	version   IndexEngineVersionManager
	gate      ScalarIndexMigrationVersionManager
}

func (s *JSONPathIndexMigrationPolicySuite) SetupTest() {
	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		indexMeta:   newSegmentIndexMeta(nil),
	}
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.handler = NewNMockHandler(s.T())
	s.version = newIndexEngineVersionManager()
	s.gate = s.version.(ScalarIndexMigrationVersionManager)
	s.policy = newJSONPathIndexMigrationPolicy(meta, s.mockAlloc, s.handler, s.version)

	s.saveParam(&paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens, "20")
	s.saveParam(&paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval, "120")
	s.saveParam(&paramtable.Get().DataCoordCfg.TargetScalarIndexVersion, "-1")
	s.saveParam(&paramtable.Get().DataCoordCfg.ForceRebuildScalarSegmentIndex, "false")
	s.saveParam(&paramtable.Get().DataCoordCfg.TargetVecIndexVersion, "-1")
	s.saveParam(&paramtable.Get().DataCoordCfg.ForceRebuildSegmentIndex, "false")
	s.saveParam(&paramtable.Get().DataCoordCfg.JSONStatsFormatVersion, "3")
}

func (s *JSONPathIndexMigrationPolicySuite) saveParam(item *paramtable.ParamItem, value string) {
	paramtable.Get().Save(item.Key, value)
	s.T().Cleanup(func() {
		paramtable.Get().Reset(item.Key)
	})
}

func newJSONPathMigrationSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 100, Name: "json", DataType: schemapb.DataType_JSON},
		{FieldID: 200, Name: "scalar", DataType: schemapb.DataType_Int64},
		{
			FieldID:  300,
			Name:     "vector",
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "8"},
			},
		},
	}}
}

func newJSONPathMigrationSegment(collectionID, segmentID int64) *SegmentInfo {
	return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   10,
		InsertChannel: "ch-1",
		Level:         datapb.SegmentLevel_L1,
		State:         commonpb.SegmentState_Flushed,
		NumOfRows:     10000,
		IsSorted:      true,
	}}
}

func newJSONPathMigrationIndex(collectionID, fieldID, indexID int64, castType string) *model.Index {
	params := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}}
	if castType != "" {
		params = append(params,
			&commonpb.KeyValuePair{Key: common.JSONPathKey, Value: "/a"},
			&commonpb.KeyValuePair{Key: common.JSONCastTypeKey, Value: castType},
		)
	}
	return &model.Index{
		CollectionID: collectionID,
		FieldID:      fieldID,
		IndexID:      indexID,
		IndexName:    "test-index",
		IndexParams:  params,
	}
}

func newJSONPathMigrationVectorIndex(collectionID, fieldID, indexID int64, indexType string) *model.Index {
	return &model.Index{
		CollectionID: collectionID,
		FieldID:      fieldID,
		IndexID:      indexID,
		IndexName:    "test-vector-index",
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: indexType},
		},
	}
}

func newJSONPathMigrationSegmentIndex(collectionID, segmentID, indexID int64, version int32) *model.SegmentIndex {
	return &model.SegmentIndex{
		CollectionID:              collectionID,
		SegmentID:                 segmentID,
		IndexID:                   indexID,
		IndexState:                commonpb.IndexState_Finished,
		IndexFileKeys:             []string{"index-file"},
		CurrentScalarIndexVersion: version,
	}
}

func newJSONPathMigrationNode(nodeID int64, scalarVersion int32) *sessionutil.Session {
	return &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{
		ServerID: nodeID,
		IndexEngineVersion: sessionutil.IndexEngineVersion{
			CurrentIndexVersion: 1,
			MaximumIndexVersion: 1,
		},
		ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{
			CurrentIndexVersion: scalarVersion,
			MaximumIndexVersion: scalarVersion,
		},
	}}
}

func newJSONPathMigrationQueryNode(nodeID int64, scalarVersion, vectorVersion int32) *sessionutil.Session {
	session := newJSONPathMigrationNode(nodeID, scalarVersion)
	session.IndexEngineVersion = sessionutil.IndexEngineVersion{
		CurrentIndexVersion: vectorVersion,
		MaximumIndexVersion: vectorVersion,
	}
	return session
}

func (s *JSONPathIndexMigrationPolicySuite) setQueryNodeVersions(scalarVersion, vectorVersion int32) {
	s.version.Startup(map[string]*sessionutil.Session{
		"qn-1": newJSONPathMigrationQueryNode(1, scalarVersion, vectorVersion),
	})
}

func (s *JSONPathIndexMigrationPolicySuite) setPolicyMeta(
	collection *collectionInfo,
	segments map[UniqueID]*SegmentInfo,
	indexes []*model.Index,
	segmentIndexes []*model.SegmentIndex,
) {
	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collection.ID: segments,
			},
		},
	}
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collection.ID, collection)
	indexMeta := newSegmentIndexMeta(nil)
	indexMeta.indexes[collection.ID] = make(map[UniqueID]*model.Index)
	for _, index := range indexes {
		indexMeta.indexes[collection.ID][index.IndexID] = index
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

func (s *JSONPathIndexMigrationPolicySuite) TestEnableAndRegistration() {
	// Missing either node class fails closed.
	s.False(s.policy.Enable())
	s.setQueryNodeVersions(common.MinScalarIndexVersionForJsonPathPresence, 1)
	s.False(s.policy.Enable())

	// An old DataNode can still produce the legacy artifact.
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{
		"dn-1": newJSONPathMigrationNode(2, common.MinScalarIndexVersionForJsonPathPresence-1),
	})
	s.False(s.policy.Enable())
	s.gate.UpdateDataNode(newJSONPathMigrationNode(2, common.MinScalarIndexVersionForJsonPathPresence))
	s.True(s.policy.Enable())
	legacyWriter := newJSONPathMigrationNode(2, common.MinScalarIndexVersionForJsonPathPresence)
	legacyWriter.IndexEngineVersion = sessionutil.IndexEngineVersion{}
	s.gate.UpdateDataNode(legacyWriter)
	s.False(s.policy.Enable(), "a DataNode that does not publish its vector writer maximum fails closed")
	s.gate.UpdateDataNode(newJSONPathMigrationNode(2, common.MinScalarIndexVersionForJsonPathPresence))
	s.True(s.policy.Enable())

	// A future QueryNode whose readable range starts at V7 cannot consume the
	// V6 artifact this migration writes.
	futureQueryNode := newJSONPathMigrationQueryNode(
		1, common.MinScalarIndexVersionForJsonPathPresence+1, 1)
	futureQueryNode.ScalarIndexEngineVersion.MinimalIndexVersion = common.MinScalarIndexVersionForJsonPathPresence + 1
	s.version.Update(futureQueryNode)
	s.False(s.policy.Enable())
	s.setQueryNodeVersions(common.MinScalarIndexVersionForJsonPathPresence, 1)
	s.True(s.policy.Enable())

	// An old QueryNode cannot consume the new presence semantics.
	s.version.AddNode(newJSONPathMigrationNode(3, common.MinScalarIndexVersionForJsonPathPresence-1))
	s.False(s.policy.Enable())
	s.version.Update(newJSONPathMigrationNode(3, common.MinScalarIndexVersionForJsonPathPresence))
	s.True(s.policy.Enable())

	// Even with capable nodes, a forced lower build target would make migration
	// produce another legacy artifact, so it must fail closed.
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.TargetScalarIndexVersion.Key, "5")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ForceRebuildScalarSegmentIndex.Key, "true")
	s.False(s.policy.Enable())
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.TargetScalarIndexVersion.Key, "-1")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ForceRebuildScalarSegmentIndex.Key, "false")
	s.True(s.policy.Enable())

	inspector := NewMockCompactionInspector(s.T())
	versionManager := NewMockVersionManager(s.T())
	manager := NewCompactionTriggerManager(s.mockAlloc, s.handler, inspector, s.policy.meta, versionManager)

	registered, ok := manager.policies[JSONPathIndexMigrationTicker]
	s.Require().True(ok)
	s.IsType(&jsonPathIndexMigrationPolicy{}, registered)
}

func (s *JSONPathIndexMigrationPolicySuite) TestSelectsOnlyLegacyTypedPathIndexes() {
	s.setQueryNodeVersions(common.MinScalarIndexVersionForJsonPathPresence, 1)
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{
		"dn-1": newJSONPathMigrationNode(2, common.MinScalarIndexVersionForJsonPathPresence),
	})

	const collectionID = int64(100)
	const (
		typedIndex1 = int64(1000)
		typedIndex2 = int64(1001)
		flatIndex   = int64(1002)
		scalarIndex = int64(1003)
		ngramIndex  = int64(1004)
		vectorIndex = int64(1005)
	)
	collection := &collectionInfo{ID: collectionID, Schema: newJSONPathMigrationSchema()}
	jsonNgram := newJSONPathMigrationIndex(collectionID, 100, ngramIndex, "VARCHAR")
	jsonNgram.IndexParams[0].Value = "nGrAm"
	indexes := []*model.Index{
		newJSONPathMigrationIndex(collectionID, 100, typedIndex1, "DOUBLE"),
		newJSONPathMigrationIndex(collectionID, 100, typedIndex2, "ARRAY_DOUBLE"),
		newJSONPathMigrationIndex(collectionID, 100, flatIndex, strconv.Itoa(int(schemapb.DataType_JSON))),
		newJSONPathMigrationIndex(collectionID, 200, scalarIndex, ""),
		jsonNgram,
		newJSONPathMigrationVectorIndex(collectionID, 300, vectorIndex, "FUTURE_VECTOR_INDEX"),
	}
	segments := map[UniqueID]*SegmentInfo{}
	for id := int64(201); id <= 222; id++ {
		segments[id] = newJSONPathMigrationSegment(collectionID, id)
	}
	segments[208].IsInvisible = true
	segments[216].TextStatsLogs = map[int64]*datapb.TextIndexStats{
		200: {CurrentScalarIndexVersion: common.MinScalarIndexVersionForJsonPathPresence + 1},
	}
	segments[217].JsonKeyStats = map[int64]*datapb.JsonKeyStats{
		100: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatV4},
	}
	segments[218].JsonKeyStats = map[int64]*datapb.JsonKeyStats{
		100: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatV4 + 1},
	}
	pendingFuture := newJSONPathMigrationSegmentIndex(
		collectionID, 209, typedIndex2, common.MinScalarIndexVersionForJsonPathPresence+1)
	pendingFuture.IndexState = commonpb.IndexState_InProgress
	pendingUnissued := newJSONPathMigrationSegmentIndex(collectionID, 211, typedIndex2, 0)
	pendingUnissued.IndexState = commonpb.IndexState_Unissued
	failedFuture := newJSONPathMigrationSegmentIndex(
		collectionID, 210, typedIndex2, common.MinScalarIndexVersionForJsonPathPresence+1)
	failedFuture.IndexState = commonpb.IndexState_Failed
	fakeFinishedLegacy := newJSONPathMigrationSegmentIndex(collectionID, 212, typedIndex1, 0)
	fakeFinishedLegacy.IndexFileKeys = nil
	fakeFinishedFuture := newJSONPathMigrationSegmentIndex(
		collectionID, 213, typedIndex2, common.MinScalarIndexVersionForJsonPathPresence+1)
	fakeFinishedFuture.IndexFileKeys = nil
	pendingRetryNoFiles := newJSONPathMigrationSegmentIndex(collectionID, 214, typedIndex2, 0)
	pendingRetryNoFiles.IndexState = commonpb.IndexState_Retry
	pendingRetryNoFiles.IndexFileKeys = nil
	pendingScalarNoFiles := newJSONPathMigrationSegmentIndex(collectionID, 221, scalarIndex, 0)
	pendingScalarNoFiles.IndexState = commonpb.IndexState_InProgress
	pendingScalarNoFiles.IndexFileKeys = nil
	pendingVectorNoFiles := newJSONPathMigrationSegmentIndex(collectionID, 222, vectorIndex, 0)
	pendingVectorNoFiles.IndexState = commonpb.IndexState_Unissued
	pendingVectorNoFiles.IndexFileKeys = nil
	segmentIndexes := []*model.SegmentIndex{
		// A pre-V6 typed path artifact is migrated.
		newJSONPathMigrationSegmentIndex(collectionID, 201, typedIndex1, 1),
		// A V6 typed path artifact is current.
		newJSONPathMigrationSegmentIndex(collectionID, 202, typedIndex1, common.MinScalarIndexVersionForJsonPathPresence),
		// A future version is never downgraded.
		newJSONPathMigrationSegmentIndex(collectionID, 203, typedIndex1, common.MinScalarIndexVersionForJsonPathPresence+1),
		// Multiple old path indexes still produce one segment rewrite.
		newJSONPathMigrationSegmentIndex(collectionID, 204, typedIndex1, 0),
		newJSONPathMigrationSegmentIndex(collectionID, 204, typedIndex2, 1),
		// An old and a future path index together skip the whole segment.
		newJSONPathMigrationSegmentIndex(collectionID, 205, typedIndex1, 1),
		newJSONPathMigrationSegmentIndex(collectionID, 205, typedIndex2, common.MinScalarIndexVersionForJsonPathPresence+1),
		// Flat JSON and ordinary scalar indexes are out of scope.
		newJSONPathMigrationSegmentIndex(collectionID, 206, flatIndex, 1),
		newJSONPathMigrationSegmentIndex(collectionID, 207, scalarIndex, 1),
		// Invisible clustering output is not current and must not be replaced.
		newJSONPathMigrationSegmentIndex(collectionID, 208, typedIndex1, 1),
		// A finished pre-V6 artifact plus a concurrent unknown-version build blocks
		// migration of the entire segment until that build reaches a terminal state.
		newJSONPathMigrationSegmentIndex(collectionID, 209, typedIndex1, 1),
		pendingFuture,
		// Failed artifacts are terminal and have no usable files, so they do not
		// block migration of another finished pre-V6 path index.
		newJSONPathMigrationSegmentIndex(collectionID, 210, typedIndex1, 1),
		failedFuture,
		// Unissued is pending for the same reason as InProgress.
		newJSONPathMigrationSegmentIndex(collectionID, 211, typedIndex1, 1),
		pendingUnissued,
		// Finished without files is a fake-finished small/no-train index, not a
		// legacy artifact, and must not cause an endless rewrite loop.
		fakeFinishedLegacy,
		// A fake-finished future version has no artifact to downgrade, so it does
		// not block migration of a real pre-V6 artifact on the same segment.
		newJSONPathMigrationSegmentIndex(collectionID, 213, typedIndex1, 1),
		fakeFinishedFuture,
		// An unfinished typed path build remains pending even before it has files.
		newJSONPathMigrationSegmentIndex(collectionID, 214, typedIndex1, 1),
		pendingRetryNoFiles,
		// JSON NGRAM has path/cast metadata but no presence bitmap or EXISTS
		// execution path, so V6 presence migration must leave it alone.
		newJSONPathMigrationSegmentIndex(collectionID, 215, ngramIndex, 1),
		// One-input compaction would also rebuild text stats. Do not downgrade a
		// future text artifact merely to migrate this pre-V6 typed path artifact.
		newJSONPathMigrationSegmentIndex(collectionID, 216, typedIndex1, 1),
		// JSON stats sidecars are also rebuilt after compaction. Protect both a
		// configured V4->V3 downgrade and a future V5 artifact.
		newJSONPathMigrationSegmentIndex(collectionID, 217, typedIndex1, 1),
		newJSONPathMigrationSegmentIndex(collectionID, 218, typedIndex1, 1),
		// Pending versioned sidecar jobs may publish against the source segment;
		// do not replace it concurrently.
		newJSONPathMigrationSegmentIndex(collectionID, 219, typedIndex1, 1),
		newJSONPathMigrationSegmentIndex(collectionID, 220, typedIndex1, 1),
		// Any active pending index can complete with a future artifact after
		// selection, so ordinary scalar and vector builds also block migration.
		newJSONPathMigrationSegmentIndex(collectionID, 221, typedIndex1, 1),
		pendingScalarNoFiles,
		newJSONPathMigrationSegmentIndex(collectionID, 222, typedIndex1, 1),
		pendingVectorNoFiles,
	}
	s.setPolicyMeta(collection, segments, indexes, segmentIndexes)
	s.policy.meta.statsTaskMeta.segmentID2Tasks.Insert(
		createSecondaryIndexKey(219, indexpb.StatsSubJob_JsonKeyIndexJob.String()),
		&indexpb.StatsTask{SegmentID: 219, SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob},
	)
	s.policy.meta.statsTaskMeta.segmentID2Tasks.Insert(
		createSecondaryIndexKey(220, indexpb.StatsSubJob_TextIndexJob.String()),
		&indexpb.StatsTask{SegmentID: 220, SubJobType: indexpb.StatsSubJob_TextIndexJob},
	)

	s.handler.EXPECT().GetCollection(mock.Anything, collectionID).Return(collection, nil).Once()
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(10000), nil).Once()
	events, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Require().Len(events, 1)
	views := events[TriggerTypeSingle]
	s.ElementsMatch([]int64{201, 204, 210, 213}, viewSegmentIDs(views))
	for _, view := range views {
		s.IsType(&MixSegmentView{}, view)
		s.Len(view.GetSegmentsView(), 1)
	}

	typedIDs := getTypedJSONPathIndexIDs(s.policy.meta.indexMeta, collection)
	s.Contains(typedIDs, typedIndex1)
	s.Contains(typedIDs, typedIndex2)
	s.NotContains(typedIDs, flatIndex)
	s.NotContains(typedIDs, scalarIndex)
	s.NotContains(typedIDs, ngramIndex)
	s.NotContains(typedIDs, vectorIndex)
	presenceVersion := common.MinScalarIndexVersionForJsonPathPresence
	s.True(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 201, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	s.False(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 202, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	s.False(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 203, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	s.False(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 205, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	pending := classifyJSONPathIndexVersions(s.policy.meta.indexMeta, 209, typedIDs, nil, nil, presenceVersion, presenceVersion, 1)
	s.True(pending.hasOlder)
	s.True(pending.hasPending)
	s.False(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 209, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	s.True(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 210, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	unissued := classifyJSONPathIndexVersions(s.policy.meta.indexMeta, 211, typedIDs, nil, nil, presenceVersion, presenceVersion, 1)
	s.True(unissued.hasOlder)
	s.True(unissued.hasPending)
	s.False(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 211, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	fakeFinished := classifyJSONPathIndexVersions(s.policy.meta.indexMeta, 212, typedIDs, nil, nil, presenceVersion, presenceVersion, 1)
	s.False(fakeFinished.hasOlder)
	s.False(fakeFinished.hasNewer)
	s.False(fakeFinished.hasPending)
	s.False(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 212, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	s.True(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 213, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	retry := classifyJSONPathIndexVersions(s.policy.meta.indexMeta, 214, typedIDs, nil, nil, presenceVersion, presenceVersion, 1)
	s.True(retry.hasOlder)
	s.True(retry.hasPending)
	s.False(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 214, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	s.False(needsJSONPathIndexMigration(s.policy.meta.indexMeta, 215, typedIDs, nil, nil, presenceVersion, presenceVersion, 1))
	textFuture := classifyJSONPathIndexVersions(
		s.policy.meta.indexMeta, 216, typedIDs, nil, segments[216].GetTextStatsLogs(), presenceVersion, presenceVersion, 1)
	s.True(textFuture.hasOlder)
	s.True(textFuture.hasNewer)
	s.False(needsJSONPathIndexMigration(
		s.policy.meta.indexMeta, 216, typedIDs, nil, segments[216].GetTextStatsLogs(), presenceVersion, presenceVersion, 1))
	jsonStatsFieldIDs := getJSONStatsFieldIDs(collection)
	s.True(classifyJSONStatsFormats(segments[217], jsonStatsFieldIDs, common.JSONStatsDataFormatV3).hasNewer)
	s.True(classifyJSONStatsFormats(segments[218], jsonStatsFieldIDs, common.JSONStatsDataFormatV4).hasNewer)
	s.True(hasVersionedStatsTask(s.policy.meta, 219))
	s.True(hasVersionedStatsTask(s.policy.meta, 220))
	pendingScalar := classifyJSONPathIndexVersions(
		s.policy.meta.indexMeta, 221, typedIDs, nil, nil, presenceVersion, presenceVersion, 1)
	s.True(pendingScalar.hasOlder)
	s.True(pendingScalar.hasPending)
	pendingVector := classifyJSONPathIndexVersions(
		s.policy.meta.indexMeta, 222, typedIDs, nil, nil, presenceVersion, presenceVersion, 1)
	s.True(pendingVector.hasOlder)
	s.True(pendingVector.hasPending)
}

func (s *JSONPathIndexMigrationPolicySuite) TestFutureIndexUsesLocalWritableTarget() {
	const (
		collectionID = int64(100)
		typedIndexID = int64(1000)
		scalarIndex  = int64(1001)
	)
	presenceVersion := common.MinScalarIndexVersionForJsonPathPresence
	s.setQueryNodeVersions(presenceVersion+1, 1)
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{
		"dn-1": newJSONPathMigrationNode(2, presenceVersion),
	})
	s.True(s.policy.Enable(), "QN v7 and DN v6 both support the v6 presence semantics")
	s.Equal(presenceVersion+1, s.version.ResolveScalarIndexVersion())
	s.Equal(presenceVersion, common.MaximumScalarIndexEngineVersion)

	collection := &collectionInfo{ID: collectionID, Schema: newJSONPathMigrationSchema()}
	indexes := []*model.Index{
		newJSONPathMigrationIndex(collectionID, 100, typedIndexID, "DOUBLE"),
		newJSONPathMigrationIndex(collectionID, 200, scalarIndex, ""),
	}
	segment := newJSONPathMigrationSegment(collectionID, 201)
	segmentIndexes := []*model.SegmentIndex{
		newJSONPathMigrationSegmentIndex(collectionID, 201, typedIndexID, presenceVersion-1),
		newJSONPathMigrationSegmentIndex(collectionID, 201, scalarIndex, presenceVersion+1),
	}
	s.setPolicyMeta(collection, map[UniqueID]*SegmentInfo{201: segment}, indexes, segmentIndexes)

	s.handler.EXPECT().GetCollection(mock.Anything, collectionID).Return(collection, nil).Once()
	events, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(events[TriggerTypeSingle], "the local v6 writer must not downgrade the existing v7 scalar artifact")

	typedIDs := getTypedJSONPathIndexIDs(s.policy.meta.indexMeta, collection)
	classification := classifyJSONPathIndexVersions(
		s.policy.meta.indexMeta,
		201,
		typedIDs,
		nil,
		nil,
		presenceVersion,
		common.MaximumScalarIndexEngineVersion,
		1,
	)
	s.True(classification.hasOlder)
	s.True(classification.hasNewer)
}

func (s *JSONPathIndexMigrationPolicySuite) TestFutureVectorIndexUsesDataNodeWriterCeilingAndSchemaType() {
	const (
		collectionID  = int64(100)
		typedIndexID  = int64(1000)
		vectorIndexID = int64(1001)
	)
	presenceVersion := common.MinScalarIndexVersionForJsonPathPresence
	writerCeiling := int32(7)
	futureVersion := writerCeiling + 1
	s.setQueryNodeVersions(presenceVersion, futureVersion)
	dataNode := newJSONPathMigrationNode(2, presenceVersion)
	dataNode.IndexEngineVersion = sessionutil.IndexEngineVersion{
		CurrentIndexVersion: writerCeiling,
		MaximumIndexVersion: writerCeiling,
	}
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{
		"dn-1": dataNode,
	})
	s.True(s.policy.Enable())
	s.Equal(futureVersion, s.version.ResolveVecIndexVersion())

	collection := &collectionInfo{ID: collectionID, Schema: newJSONPathMigrationSchema()}
	indexes := []*model.Index{
		newJSONPathMigrationIndex(collectionID, 100, typedIndexID, "DOUBLE"),
		newJSONPathMigrationVectorIndex(collectionID, 300, vectorIndexID, "FUTURE_VECTOR_INDEX"),
	}
	vectorSegmentIndex := newJSONPathMigrationSegmentIndex(collectionID, 201, vectorIndexID, 0)
	vectorSegmentIndex.CurrentIndexVersion = futureVersion
	segmentIndexes := []*model.SegmentIndex{
		newJSONPathMigrationSegmentIndex(collectionID, 201, typedIndexID, presenceVersion-1),
		vectorSegmentIndex,
	}
	s.setPolicyMeta(
		collection,
		map[UniqueID]*SegmentInfo{201: newJSONPathMigrationSegment(collectionID, 201)},
		indexes,
		segmentIndexes,
	)

	s.handler.EXPECT().GetCollection(mock.Anything, collectionID).Return(collection, nil).Once()
	events, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(events[TriggerTypeSingle], "the local writer must not downgrade a future vector artifact")

	typedIDs := getTypedJSONPathIndexIDs(s.policy.meta.indexMeta, collection)
	vectorIDs := getVectorIndexIDs(s.policy.meta.indexMeta, collection)
	s.Contains(vectorIDs, vectorIndexID, "schema field type identifies vector indexes even when the index type is unknown")
	classification := classifyJSONPathIndexVersions(
		s.policy.meta.indexMeta,
		201,
		typedIDs,
		vectorIDs,
		nil,
		presenceVersion,
		presenceVersion,
		writerCeiling,
	)
	s.True(classification.hasOlder)
	s.True(classification.hasNewer)
}

func (s *JSONPathIndexMigrationPolicySuite) TestVectorWriterMinimumFailsClosed() {
	const (
		collectionID = int64(100)
		typedIndexID = int64(1000)
	)
	presenceVersion := common.MinScalarIndexVersionForJsonPathPresence
	s.setQueryNodeVersions(presenceVersion, 5)
	dataNode := newJSONPathMigrationNode(2, presenceVersion)
	dataNode.IndexEngineVersion = sessionutil.IndexEngineVersion{
		MinimalIndexVersion: 6,
		CurrentIndexVersion: 8,
		MaximumIndexVersion: 10,
	}
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{"dn-1": dataNode})
	s.False(s.policy.Enable(), "the resolved vector target is below the DataNode writer range")

	collection := &collectionInfo{ID: collectionID, Schema: newJSONPathMigrationSchema()}
	s.setPolicyMeta(
		collection,
		map[UniqueID]*SegmentInfo{201: newJSONPathMigrationSegment(collectionID, 201)},
		[]*model.Index{newJSONPathMigrationIndex(collectionID, 100, typedIndexID, "DOUBLE")},
		[]*model.SegmentIndex{
			newJSONPathMigrationSegmentIndex(collectionID, 201, typedIndexID, presenceVersion-1),
		},
	)
	events, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(events[TriggerTypeSingle])
}

func (s *JSONPathIndexMigrationPolicySuite) TestVectorReaderAndWriterRangesMustIntersect() {
	const (
		collectionID = int64(100)
		typedIndexID = int64(1000)
	)
	presenceVersion := common.MinScalarIndexVersionForJsonPathPresence
	queryNode := newJSONPathMigrationQueryNode(1, presenceVersion, 20)
	queryNode.IndexEngineVersion.MinimalIndexVersion = 10
	s.version.Startup(map[string]*sessionutil.Session{"qn-1": queryNode})
	dataNode := newJSONPathMigrationNode(2, presenceVersion)
	dataNode.IndexEngineVersion = sessionutil.IndexEngineVersion{
		MinimalIndexVersion: 1,
		CurrentIndexVersion: 8,
		MaximumIndexVersion: 8,
	}
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{"dn-1": dataNode})
	s.False(s.policy.Enable(), "QN [10,20] and DN [1,8] have no safe vector rebuild target")

	collection := &collectionInfo{ID: collectionID, Schema: newJSONPathMigrationSchema()}
	s.setPolicyMeta(
		collection,
		map[UniqueID]*SegmentInfo{201: newJSONPathMigrationSegment(collectionID, 201)},
		[]*model.Index{newJSONPathMigrationIndex(collectionID, 100, typedIndexID, "DOUBLE")},
		[]*model.SegmentIndex{
			newJSONPathMigrationSegmentIndex(collectionID, 201, typedIndexID, presenceVersion-1),
		},
	)
	events, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(events[TriggerTypeSingle])
}

func (s *JSONPathIndexMigrationPolicySuite) TestVectorTargetAboveQueryCurrentWithinMaximumIsAllowed() {
	presenceVersion := common.MinScalarIndexVersionForJsonPathPresence
	queryNode := newJSONPathMigrationQueryNode(1, presenceVersion, 5)
	queryNode.IndexEngineVersion.MaximumIndexVersion = 10
	s.version.Startup(map[string]*sessionutil.Session{"qn-1": queryNode})
	dataNode := newJSONPathMigrationNode(2, presenceVersion)
	dataNode.IndexEngineVersion = sessionutil.IndexEngineVersion{
		CurrentIndexVersion: 8,
		MaximumIndexVersion: 8,
	}
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{"dn-1": dataNode})
	s.saveParam(&paramtable.Get().DataCoordCfg.TargetVecIndexVersion, "8")

	s.Equal(int32(8), s.version.ResolveVecIndexVersion())
	s.True(s.policy.Enable(), "QN reader maximum, not its default current version, is the safe upper bound")
}

func (s *JSONPathIndexMigrationPolicySuite) TestSegmentEligibilityAndRateLimit() {
	s.setQueryNodeVersions(common.MinScalarIndexVersionForJsonPathPresence, 1)
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{
		"dn-1": newJSONPathMigrationNode(2, common.MinScalarIndexVersionForJsonPathPresence),
	})

	const (
		collectionID = int64(100)
		typedIndexID = int64(1000)
	)
	s.saveParam(&paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens, "1")
	collection := &collectionInfo{ID: collectionID, Schema: newJSONPathMigrationSchema()}
	index := newJSONPathMigrationIndex(collectionID, 100, typedIndexID, "DOUBLE")
	segments := map[UniqueID]*SegmentInfo{}
	segmentIndexes := make([]*model.SegmentIndex, 0)
	for id := int64(201); id <= 207; id++ {
		segments[id] = newJSONPathMigrationSegment(collectionID, id)
		segmentIndexes = append(segmentIndexes, newJSONPathMigrationSegmentIndex(collectionID, id, typedIndexID, 1))
	}
	segments[202].isCompacting = true
	segments[203].IsImporting = true
	segments[204].IsInvisible = true
	segments[205].Level = datapb.SegmentLevel_L0
	segments[206].State = commonpb.SegmentState_Dropped
	s.setPolicyMeta(collection, segments, []*model.Index{index}, segmentIndexes)
	s.policy.meta.snapshotMeta = &snapshotMeta{
		compactionBlockedCollections: typeutil.NewUniqueSet(),
		snapshotPendingCollections:   typeutil.NewUniqueSet(),
		segmentProtectionUntil:       map[int64]uint64{207: uint64(time.Now().Add(time.Hour).Unix())},
	}

	s.handler.EXPECT().GetCollection(mock.Anything, collectionID).Return(collection, nil).Once()
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(10000), nil).Once()
	first, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Equal([]int64{201}, viewSegmentIDs(first[TriggerTypeSingle]))
	s.Equal(1, s.policy.currentCount)

	second, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(second[TriggerTypeSingle])
}

func (s *JSONPathIndexMigrationPolicySuite) TestBlockedCollectionAndExternalCollection() {
	s.setQueryNodeVersions(common.MinScalarIndexVersionForJsonPathPresence, 1)
	s.gate.StartupDataNodes(map[string]*sessionutil.Session{
		"dn-1": newJSONPathMigrationNode(2, common.MinScalarIndexVersionForJsonPathPresence),
	})

	const (
		collectionID = int64(100)
		typedIndexID = int64(1000)
	)
	collection := &collectionInfo{ID: collectionID, Schema: newJSONPathMigrationSchema()}
	segment := newJSONPathMigrationSegment(collectionID, 201)
	index := newJSONPathMigrationIndex(collectionID, 100, typedIndexID, "DOUBLE")
	segmentIndex := newJSONPathMigrationSegmentIndex(collectionID, 201, typedIndexID, 1)
	s.setPolicyMeta(collection, map[UniqueID]*SegmentInfo{201: segment}, []*model.Index{index}, []*model.SegmentIndex{segmentIndex})
	s.policy.meta.snapshotMeta = &snapshotMeta{
		compactionBlockedCollections: typeutil.NewUniqueSet(collectionID),
		snapshotPendingCollections:   typeutil.NewUniqueSet(),
		segmentProtectionUntil:       map[int64]uint64{},
	}

	events, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Empty(events[TriggerTypeSingle])

	collection.Schema.Fields[0].ExternalField = "pk"
	s.policy.meta.snapshotMeta.compactionBlockedCollections = typeutil.NewUniqueSet()
	s.handler.EXPECT().GetCollection(mock.Anything, collectionID).Return(collection, nil).Once()
	views, err := s.policy.triggerOneCollection(
		context.Background(),
		collectionID,
		20,
		common.MinScalarIndexVersionForJsonPathPresence,
		common.MaximumScalarIndexEngineVersion,
		1,
	)
	s.NoError(err)
	s.Empty(views)
}
