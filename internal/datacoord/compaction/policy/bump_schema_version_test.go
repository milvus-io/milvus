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

package policy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBumpSchemaVersionPolicySuite(t *testing.T) {
	suite.Run(t, new(BumpSchemaVersionPolicySuite))
}

type BumpSchemaVersionPolicySuite struct {
	suite.Suite

	mockAlloc               *allocator.MockAllocator
	handler                 *meta.NMockHandler
	testLabel               *view.CompactionGroupLabel
	bumpSchemaVersionPolicy *BumpSchemaVersionPolicy
}

func (s *BumpSchemaVersionPolicySuite) SetupTest() {
	s.testLabel = &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := GenSegmentsForMeta(s.testLabel)
	mockCatalog := mocks.NewDataCoordCatalog(s.T())
	store := metacache.NewMetaStore(nil)
	mt := &meta.Meta{
		Segments:  meta.NewSegmentsInfo(store),
		MetaStore: store,
		Catalog:   mockCatalog,
	}
	for id, segment := range segments {
		mt.Segments.SetSegment(id, segment)
	}

	s.mockAlloc = meta.NewMockAllocator(s.T())
	mockHandler := meta.NewNMockHandler(s.T())
	s.handler = mockHandler
	s.bumpSchemaVersionPolicy = NewBumpSchemaVersionPolicy(mt, s.mockAlloc, mockHandler)
}

func (s *BumpSchemaVersionPolicySuite) TestTrigger() {
	// Test basic trigger with no collections
	events, err := s.bumpSchemaVersionPolicy.Trigger(context.Background())
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBumpSchemaVersion]
	s.False(ok)
	s.Nil(gotViews)
	s.Equal(0, len(gotViews))
}

func (s *BumpSchemaVersionPolicySuite) TestBumpSchemaVersionViewBasic() {
	cv := &BumpSchemaVersionView{
		Label: &view.CompactionGroupLabel{
			CollectionID: 1,
			PartitionID:  10,
			Channel:      "ch-1",
		},
		Segments:  []*view.SegmentView{},
		TriggerID: 100,
		Schema:    &schemapb.CollectionSchema{Version: 2},
	}

	// Test GetGroupLabel
	label := cv.GetGroupLabel()
	s.NotNil(label)
	s.Equal(int64(1), label.CollectionID)

	// Test GetSegmentsView
	segments := cv.GetSegmentsView()
	s.NotNil(segments)
	s.Equal(0, len(segments))

	// Test Append
	segmentView := &view.SegmentView{
		ID:    101,
		Label: label,
		State: commonpb.SegmentState_Flushed,
		Level: datapb.SegmentLevel_L1,
	}
	cv.Append(segmentView)
	s.Equal(1, len(cv.GetSegmentsView()))

	// Test String
	str := cv.String()
	s.Contains(str, "BumpSchemaVersionView")
	s.Contains(str, "segments=1")
	s.Contains(str, "triggerID=100")

	// Test Trigger
	triggeredView, reason := cv.Trigger()
	s.NotNil(triggeredView)
	s.Equal("segment schema version behind collection schema", reason)

	// Test ForceTrigger
	forceView, reason := cv.ForceTrigger()
	s.NotNil(forceView)
	s.Equal("segment schema version behind collection schema", reason)

	// Test ForceTriggerAll
	forceViews, reason := cv.ForceTriggerAll()
	s.Equal(1, len(forceViews))
	s.Equal("segment schema version behind collection schema", reason)

	// Test GetTriggerID
	triggerID := cv.GetTriggerID()
	s.Equal(int64(100), triggerID)
}

func (s *BumpSchemaVersionPolicySuite) TestEnable() {
	s.False(s.bumpSchemaVersionPolicy.Enable())

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.Key)

	// Schema-version reconciliation is a correctness requirement, not an auto-compaction option.
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)
	s.True(s.bumpSchemaVersionPolicy.Enable())
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSchedulesReadySegmentWhenCollectionHasV2FlushedDataSegment() {
	ctx := context.Background()
	collID := int64(100)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest"))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(102, NewBumpSchemaVersionTestSegment(collID, 102, 1, storage.StorageV2, "manifest"))

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views := events[TriggerTypeBumpSchemaVersion]
	s.Require().Len(views, 1)
	s.Require().Len(views[0].GetSegmentsView(), 1)
	s.EqualValues(101, views[0].GetSegmentsView()[0].ID)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerCapturesSchemaSnapshot() {
	ctx := context.Background()
	collID := int64(100)
	collection := NewBumpSchemaVersionTestCollection(collID, 2)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(collection)
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest"))

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views := events[TriggerTypeBumpSchemaVersion]
	s.Require().Len(views, 1)
	cv := views[0].(*BumpSchemaVersionView)

	collection.Schema.Version = 3
	collection.Schema.Fields = append(collection.Schema.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "new_field", DataType: schemapb.DataType_Int64})
	s.EqualValues(2, cv.Schema.GetVersion())
	s.Len(cv.Schema.GetFields(), 2)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerClonesSchemaOnlyForCandidates() {
	for _, stale := range []bool{false, true} {
		s.Run(fmt.Sprintf("stale=%t", stale), func() {
			collection := NewBumpSchemaVersionTestCollection(100, 2)
			mockAlloc := meta.NewMockAllocator(s.T())
			store := metacache.NewMetaStore(nil)
			policy := NewBumpSchemaVersionPolicy(&meta.Meta{
				Segments:  meta.NewSegmentsInfo(store),
				MetaStore: store,
			}, mockAlloc, s.handler)
			store.PutCollection(collection)
			segmentSchemaVersion := int32(2)
			if stale {
				segmentSchemaVersion = 1
			}
			for _, segmentID := range []int64{101, 102} {
				policy.meta.Segments.SetSegment(segmentID, NewBumpSchemaVersionTestSegment(collection.ID, segmentID, segmentSchemaVersion, storage.StorageV3, "manifest"))
			}

			cloneCount := 0
			var clone func(proto.Message) proto.Message
			cloneMock := mockey.Mock(proto.Clone).To(func(message proto.Message) proto.Message {
				if message == collection.Schema {
					cloneCount++
				}
				return clone(message)
			}).Origin(&clone).Build()
			defer cloneMock.UnPatch()

			events, err := policy.Trigger(context.Background())
			s.NoError(err)
			views := events[TriggerTypeBumpSchemaVersion]
			if !stale {
				s.Empty(views)
				s.Zero(cloneCount)
				mockAlloc.AssertNotCalled(s.T(), "AllocID", mock.Anything)
				return
			}
			s.Require().Len(views, 2)
			s.Equal(1, cloneCount)
			s.Same(views[0].(*BumpSchemaVersionView).Schema, views[1].(*BumpSchemaVersionView).Schema)
			mockAlloc.AssertNumberOfCalls(s.T(), "AllocID", 1)
		})
	}
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerRetainsSchemaAcrossCacheReplacement() {
	collection := NewBumpSchemaVersionTestCollection(100, 2)
	policy := s.bumpSchemaVersionPolicy
	policy.meta.MetaStore.PutCollection(collection)
	policy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collection.ID, 101, 1, storage.StorageV3, "manifest"))
	// This segment only becomes stale at V3, after this scan's V2 snapshot.
	policy.meta.Segments.SetSegment(102, NewBumpSchemaVersionTestSegment(collection.ID, 102, 2, storage.StorageV3, "manifest"))

	var selectSegments func(*BumpSchemaVersionPolicy, int64, int32) []*meta.ChanPartSegments
	selectMock := mockey.Mock((*BumpSchemaVersionPolicy).staleFlushedSegments).To(
		func(policy *BumpSchemaVersionPolicy, collectionID int64, version int32) []*meta.ChanPartSegments {
			segments := selectSegments(policy, collectionID, version)
			policy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collectionID, 3))
			return segments
		}).Origin(&selectSegments).Build()
	defer selectMock.UnPatch()

	events, err := policy.Trigger(context.Background())
	s.NoError(err)
	views := events[TriggerTypeBumpSchemaVersion]
	s.Require().Len(views, 1)
	cv := views[0].(*BumpSchemaVersionView)
	s.EqualValues(101, cv.Segments[0].ID)
	s.EqualValues(2, cv.Schema.GetVersion())
	s.NotSame(collection.Schema, cv.Schema)
	s.EqualValues(3, policy.meta.GetCollection(collection.ID).Schema.GetVersion())
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSchedulesReadySegmentWhenCollectionHasMissingManifestFlushedDataSegment() {
	ctx := context.Background()
	collID := int64(100)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest"))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(102, NewBumpSchemaVersionTestSegment(collID, 102, 1, storage.StorageV3, ""))

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views := events[TriggerTypeBumpSchemaVersion]
	s.Require().Len(views, 1)
	s.Require().Len(views[0].GetSegmentsView(), 1)
	s.EqualValues(101, views[0].GetSegmentsView()[0].ID)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerFiltersSegmentLevelV3Readiness() {
	ctx := context.Background()
	collID := int64(100)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest"))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(102, NewBumpSchemaVersionTestSegment(collID, 102, 1, storage.StorageV2, ""))

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views := events[TriggerTypeBumpSchemaVersion]
	s.Require().Len(views, 1)
	s.Require().Len(views[0].GetSegmentsView(), 1)
	s.EqualValues(101, views[0].GetSegmentsView()[0].ID)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerReadinessIgnoresNonDataSegments() {
	ctx := context.Background()
	collID := int64(100)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest"))

	l0Segment := NewBumpSchemaVersionTestSegment(collID, 102, 1, storage.StorageV2, "")
	l0Segment.Level = datapb.SegmentLevel_L0
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(102, l0Segment)
	importingSegment := NewBumpSchemaVersionTestSegment(collID, 103, 1, storage.StorageV2, "")
	importingSegment.IsImporting = true
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(103, importingSegment)
	invisibleSegment := NewBumpSchemaVersionTestSegment(collID, 104, 1, storage.StorageV2, "")
	invisibleSegment.IsInvisible = true
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(104, invisibleSegment)

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	s.Require().Len(events[TriggerTypeBumpSchemaVersion], 1)
	cv := events[TriggerTypeBumpSchemaVersion][0]
	s.Require().Len(cv.GetSegmentsView(), 1)
	s.EqualValues(101, cv.GetSegmentsView()[0].ID)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerReusesOneTriggerIDPerCollection() {
	ctx := context.Background()
	collID := int64(100)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest-101"))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(102, NewBumpSchemaVersionTestSegment(collID, 102, 1, storage.StorageV3, "manifest-102"))

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views := events[TriggerTypeBumpSchemaVersion]
	s.Require().Len(views, 2)
	s.Equal(views[0].(*BumpSchemaVersionView).GetTriggerID(), views[1].(*BumpSchemaVersionView).GetTriggerID())
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerAllocatesTriggerIDPerCollection() {
	ctx := context.Background()
	collID1 := int64(100)
	collID2 := int64(200)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID1, 2))
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID2, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID1, 101, 1, storage.StorageV3, "manifest-101"))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(201, NewBumpSchemaVersionTestSegment(collID2, 201, 1, storage.StorageV3, "manifest-201"))

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views := events[TriggerTypeBumpSchemaVersion]
	s.Require().Len(views, 2)
	s.NotEqual(views[0].(*BumpSchemaVersionView).GetTriggerID(), views[1].(*BumpSchemaVersionView).GetTriggerID())
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerAllocIDFailureSkipsCurrentCollection() {
	ctx := context.Background()
	collID := int64(100)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest"))
	segmentInAnotherGroup := NewBumpSchemaVersionTestSegment(collID, 102, 1, storage.StorageV3, "manifest")
	segmentInAnotherGroup.InsertChannel = "ch-2"
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(102, segmentInAnotherGroup)
	mockAlloc := allocator.NewMockAllocator(s.T())
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(0), errors.New("alloc id failed")).Once()
	s.bumpSchemaVersionPolicy.allocator = mockAlloc

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	s.Empty(events[TriggerTypeBumpSchemaVersion])
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSkipsBlockedCollection() {
	ctx := context.Background()
	collID := int64(100)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest"))
	snapMeta := &snapshot.SnapshotMeta{
		CompactionBlockedCollections: typeutil.NewUniqueSet(collID),
		SnapshotPendingCollections:   typeutil.NewUniqueSet(),
	}
	s.bumpSchemaVersionPolicy.meta.CompactionProtection = snapMeta

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	s.Empty(events[TriggerTypeBumpSchemaVersion])
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSkipsSnapshotPendingCollection() {
	ctx := context.Background()
	collID := int64(100)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest"))
	snapMeta := &snapshot.SnapshotMeta{
		CompactionBlockedCollections: typeutil.NewUniqueSet(),
		SnapshotPendingCollections:   typeutil.NewUniqueSet(collID),
	}
	s.bumpSchemaVersionPolicy.meta.CompactionProtection = snapMeta

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	s.Empty(events[TriggerTypeBumpSchemaVersion])
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSkipsSnapshotProtectedSegment() {
	ctx := context.Background()
	collID := int64(100)
	segmentID := int64(101)
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(NewBumpSchemaVersionTestCollection(collID, 2))
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, NewBumpSchemaVersionTestSegment(collID, segmentID, 1, storage.StorageV3, "manifest"))
	snapMeta := &snapshot.SnapshotMeta{
		CompactionBlockedCollections: typeutil.NewUniqueSet(),
		SnapshotPendingCollections:   typeutil.NewUniqueSet(),
		SegmentProtectionUntil:       map[int64]uint64{segmentID: uint64(time.Now().Add(time.Hour).Unix())},
	}
	s.bumpSchemaVersionPolicy.meta.CompactionProtection = snapMeta

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	s.Empty(events[TriggerTypeBumpSchemaVersion])
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSkipsExternalCollection() {
	ctx := context.Background()
	collID := int64(100)
	coll := NewBumpSchemaVersionTestCollection(collID, 2)
	// IsExternalCollection is true when any field carries an external field mapping.
	coll.Schema.Fields[1].ExternalField = "src_text"
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(101, NewBumpSchemaVersionTestSegment(collID, 101, 1, storage.StorageV3, "manifest"))

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	s.Empty(events[TriggerTypeBumpSchemaVersion])
}

func (s *BumpSchemaVersionPolicySuite) TestName() {
	// Test Name method
	s.Equal("BumpSchemaVersion", s.bumpSchemaVersionPolicy.Name())
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerWithCurrentSchemaVersion() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	// Create collection with schema version 1
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:        "test_collection",
			Description: "test collection",
			Version:     1,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// Create segment with same schema version.
	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      1000,
			SchemaVersion:  1, // Same as collection schema version
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest",
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBumpSchemaVersion]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerWithCompactingSegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:        "test_collection",
			Description: "test collection",
			Version:     2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// Create segment that is compacting (should be filtered out)
	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      1000,
			SchemaVersion:  1, // Outdated schema version
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest",
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	segment.IsCompacting = true // Mark as compacting
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBumpSchemaVersion]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerWithImportingSegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:        "test_collection",
			Description: "test collection",
			Version:     2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// Create segment that is importing (should be filtered out)
	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
			SchemaVersion: 1,    // Outdated schema version
			IsImporting:   true, // Mark as importing
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBumpSchemaVersion]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerWithInvisibleSegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:        "test_collection",
			Description: "test collection",
			Version:     2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// Create segment that is invisible (should be filtered out)
	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
			SchemaVersion: 1,    // Outdated schema version
			IsInvisible:   true, // Mark as invisible
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBumpSchemaVersion]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerWithUnhealthySegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:        "test_collection",
			Description: "test collection",
			Version:     2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// Create segment that is not healthy (should be filtered out)
	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Dropped, // Not healthy
			NumOfRows:     1000,
			SchemaVersion: 1, // Outdated schema version
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	// AllocID should NOT be called: unhealthy segment is filtered before schema bump.
	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBumpSchemaVersion]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerWithNonFlushedSegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:        "test_collection",
			Description: "test collection",
			Version:     2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// Create segment that is not flushed (should be filtered out)
	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Growing, // Not flushed
			NumOfRows:     1000,
			SchemaVersion: 1, // Outdated schema version
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	// AllocID should NOT be called: non-flushed segment is filtered before schema bump.
	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBumpSchemaVersion]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerWithOutdatedSchemaVersion() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)

	// Create collection with schema version 2, with a BM25 function whose output field 102 is missing from segment
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:        "test_collection",
			Description: "test collection",
			Version:     2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "sparse_vector", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:           "bm25_func",
					OutputFieldIds: []int64{102},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// Create segment with outdated schema version 1, binlogs only have field 101 (missing 102)
	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      1000,
			SchemaVersion:  1,
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest",
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     0,
					ChildFields: []int64{101},
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBumpSchemaVersion]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews))

	// Verify the view
	cv, ok := gotViews[0].(*BumpSchemaVersionView)
	s.True(ok)
	s.NotNil(cv)
	s.Equal(int64(1), cv.GetTriggerID())
	s.Equal(1, len(cv.GetSegmentsView()))
	s.Equal(segmentID, cv.GetSegmentsView()[0].ID)
	s.Equal(int64(1), cv.GetTriggerID())
	s.Equal(coll.Schema.GetVersion(), cv.Schema.GetVersion())
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSchedulesSchemaBumpForStaleSegment() {
	// DataCoord schedules every stale flushed segment; DataNode decides no-op, partial bumpSchemaVersion, or full rewrite.
	ctx := context.Background()

	collID := int64(100)

	// Create collection with schema version 2.
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:        "test_collection",
			Description: "test collection",
			Version:     2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// Create segment with outdated schema version 1
	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      1000,
			SchemaVersion:  1,
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest",
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)
	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)

	views, ok := events[TriggerTypeBumpSchemaVersion]
	s.Require().True(ok)
	s.Require().Len(views, 1)
	bv, ok := views[0].(*BumpSchemaVersionView)
	s.Require().True(ok)
	s.Equal(int64(1), bv.GetTriggerID())
	s.Equal(coll.Schema.GetVersion(), bv.Schema.GetVersion())
	s.Require().Len(bv.GetSegmentsView(), 1)
	s.Equal(segmentID, bv.GetSegmentsView()[0].ID)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSchemaBumpWithAutoCompactionDisabled() {
	// Schema version bump is a correctness task and must not depend on auto compaction.
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:    "test_collection",
			Version: 2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "new_field", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      1000,
			SchemaVersion:  1,
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest",
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)
	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views, ok := events[TriggerTypeBumpSchemaVersion]
	s.Require().True(ok)
	s.Require().Len(views, 1)
	bv, ok := views[0].(*BumpSchemaVersionView)
	s.Require().True(ok)
	s.Equal(int64(1), bv.GetTriggerID())
	s.Equal(coll.Schema.GetVersion(), bv.Schema.GetVersion())
	s.Require().Len(bv.GetSegmentsView(), 1)
	s.Equal(segmentID, bv.GetSegmentsView()[0].ID)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerMissingFunctionOutputWithAutoCompactionDisabled() {
	// DataNode decides whether the schema bump needs function-output bumpSchemaVersion.
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:    "test_collection",
			Version: 2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:           "bm25_func",
					OutputFieldIds: []int64{102},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	segmentID := int64(101)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      1000,
			SchemaVersion:  1,
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest",
			StartPosition: &msgpb.MsgPosition{
				Timestamp: 10000,
			},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "test_path", EntriesNum: 1000},
					},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	// AllocID should be called: stale flushed segment is scheduled even with auto compaction disabled.
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBumpSchemaVersion]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews))

	cv, ok := gotViews[0].(*BumpSchemaVersionView)
	s.True(ok)
	s.Equal(int64(1), cv.GetTriggerID())
	s.Equal(1, len(cv.GetSegmentsView()))
	s.Equal(segmentID, cv.GetSegmentsView()[0].ID)
	s.Equal(coll.Schema.GetVersion(), cv.Schema.GetVersion())
}

// TestSchemaFrozenAtScanTime verifies that schema-bump views capture the collection
// schema at scan time. SubmitBumpSchemaVersionViewToScheduler uses this frozen schema for
// task.Schema, preventing premature schema-version advancement when the live
// collection races ahead between scan and submission.
func (s *BumpSchemaVersionPolicySuite) TestSchemaFrozenAtScanTime() {
	ctx := context.Background()
	collID := int64(200)
	schemaV1 := &schemapb.CollectionSchema{
		Version: 1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm25_v1", OutputFieldIds: []int64{102}},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(&meta.CollectionInfo{ID: collID, Schema: schemaV1})
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: int64(201), CollectionID: collID, PartitionID: 10,
			InsertChannel: "ch-1", Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, NumOfRows: 100,
			SchemaVersion: 0, StorageVersion: storage.StorageV3, ManifestPath: "manifest",
			StartPosition: &msgpb.MsgPosition{Timestamp: 1},
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "p", EntriesNum: 100}}},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(int64(201), segment)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views, ok := events[TriggerTypeBumpSchemaVersion]
	s.True(ok)
	s.Len(views, 1)

	bv, ok := views[0].(*BumpSchemaVersionView)
	s.True(ok)
	// The frozen schema must match the collection state at scan time (v1), not any
	// live version the collection might race to before SubmitBumpSchemaVersionViewToScheduler.
	s.NotNil(bv.Schema)
	s.Equal(int32(1), bv.Schema.GetVersion())
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSchedulesMultiFunctionSchemaBump() {
	ctx := context.Background()
	collID := int64(300)

	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:    "test_multi_func",
			Version: 2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "sparse1", DataType: schemapb.DataType_SparseFloatVector},
				{FieldID: 103, Name: "sparse2", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{Name: "func1", OutputFieldIds: []int64{102}},
				{Name: "func2", OutputFieldIds: []int64{103}},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// DataCoord only detects stale schema version; DataNode validates function-output field state.
	segmentID := int64(301)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: segmentID, CollectionID: collID, PartitionID: 10,
			InsertChannel: "ch-1", Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, NumOfRows: 100,
			SchemaVersion: 0, StorageVersion: storage.StorageV3, ManifestPath: "manifest",
			StartPosition: &msgpb.MsgPosition{Timestamp: 1},
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "p", EntriesNum: 100}}},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)
	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views, ok := events[TriggerTypeBumpSchemaVersion]
	s.Require().True(ok)
	s.Require().Len(views, 1)
	bv, ok := views[0].(*BumpSchemaVersionView)
	s.Require().True(ok)
	s.Equal(int64(1), bv.GetTriggerID())
	s.Require().Len(bv.GetSegmentsView(), 1)
	s.Equal(segmentID, bv.GetSegmentsView()[0].ID)
}

func (s *BumpSchemaVersionPolicySuite) TestTriggerSchedulesAlreadyMaterializedStaleSegment() {
	ctx := context.Background()
	collID := int64(400)
	coll := &meta.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:    "test_self_heal",
			Version: 2,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{Name: "bm25_func", OutputFieldIds: []int64{102}},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.MetaStore.PutCollection(coll)

	// Segment at version=1 (stale) but already has field 102 present in binlogs.
	segmentID := int64(401)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: segmentID, CollectionID: collID, PartitionID: 10,
			InsertChannel: "ch-1", Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, NumOfRows: 100,
			SchemaVersion: 1, StorageVersion: storage.StorageV3, ManifestPath: "manifest",
			StartPosition: &msgpb.MsgPosition{Timestamp: 1},
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     0,
					ChildFields: []int64{101, 102}, // 102 already present
					Binlogs:     []*datapb.Binlog{{LogPath: "p", EntriesNum: 100}},
				},
			},
		},
	}
	s.bumpSchemaVersionPolicy.meta.Segments.SetSegment(segmentID, segment)

	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)
	events, err := s.bumpSchemaVersionPolicy.Trigger(ctx)
	s.NoError(err)
	views, ok := events[TriggerTypeBumpSchemaVersion]
	s.Require().True(ok)
	s.Require().Len(views, 1)
	bv, ok := views[0].(*BumpSchemaVersionView)
	s.Require().True(ok)
	s.Equal(int64(1), bv.GetTriggerID())
	s.Equal(coll.Schema.GetVersion(), bv.Schema.GetVersion())
	s.Require().Len(bv.GetSegmentsView(), 1)
	s.Equal(segmentID, bv.GetSegmentsView()[0].ID)
}
