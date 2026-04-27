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
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBackfillCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(BackfillCompactionPolicySuite))
}

type BackfillCompactionPolicySuite struct {
	suite.Suite

	mockAlloc      *allocator.MockAllocator
	handler        *NMockHandler
	testLabel      *CompactionGroupLabel
	backfillPolicy *backfillCompactionPolicy
}

func (s *BackfillCompactionPolicySuite) SetupTest() {
	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := genSegmentsForMeta(s.testLabel)
	mockCatalog := mocks.NewDataCoordCatalog(s.T())
	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		catalog:     mockCatalog,
	}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}

	s.mockAlloc = newMockAllocator(s.T())
	mockHandler := NewNMockHandler(s.T())
	s.handler = mockHandler
	s.backfillPolicy = newBackfillCompactionPolicy(meta, s.mockAlloc, mockHandler)
}

func (s *BackfillCompactionPolicySuite) TestTrigger() {
	// Test basic trigger with no collections
	events, err := s.backfillPolicy.Trigger(context.Background())
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.False(ok)
	s.Nil(gotViews)
	s.Equal(0, len(gotViews))
}

func (s *BackfillCompactionPolicySuite) TestBackfillSegmentsViewBasic() {
	view := &BackfillSegmentsView{
		label: &CompactionGroupLabel{
			CollectionID: 1,
			PartitionID:  10,
			Channel:      "ch-1",
		},
		segments:  []*SegmentView{},
		triggerID: 100,
	}

	// Test GetGroupLabel
	label := view.GetGroupLabel()
	s.NotNil(label)
	s.Equal(int64(1), label.CollectionID)

	// Test GetSegmentsView
	segments := view.GetSegmentsView()
	s.NotNil(segments)
	s.Equal(0, len(segments))

	// Test Append
	segmentView := &SegmentView{
		ID:    101,
		label: label,
		State: commonpb.SegmentState_Flushed,
		Level: datapb.SegmentLevel_L1,
	}
	view.Append(segmentView)
	s.Equal(1, len(view.GetSegmentsView()))

	// Test String
	str := view.String()
	s.Contains(str, "BackfillSegmentsView")
	s.Contains(str, "segments=1")
	s.Contains(str, "triggerID=100")

	// Test Trigger
	triggeredView, reason := view.Trigger()
	s.NotNil(triggeredView)
	s.Equal("backfill schema version mismatch", reason)

	// Test ForceTrigger
	forceView, reason := view.ForceTrigger()
	s.NotNil(forceView)
	s.Equal("backfill schema version mismatch", reason)

	// Test ForceTriggerAll
	forceViews, reason := view.ForceTriggerAll()
	s.Equal(1, len(forceViews))
	s.Equal("backfill schema version mismatch", reason)

	// Test GetTriggerID
	triggerID := view.GetTriggerID()
	s.Equal(int64(100), triggerID)
}

func (s *BackfillCompactionPolicySuite) TestEnable() {
	// Backfill policy is always enabled regardless of EnableAutoCompaction toggle.
	// Metadata-only schema version updates are a correctness requirement.
	// Physical backfill is guarded by EnableAutoCompaction inside Trigger() instead.
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)
	s.True(s.backfillPolicy.Enable())

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	s.True(s.backfillPolicy.Enable())
	paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)
}

func (s *BackfillCompactionPolicySuite) TestName() {
	// Test Name method
	s.Equal("BackfillCompaction", s.backfillPolicy.Name())
}

func (s *BackfillCompactionPolicySuite) TestTriggerWithCollectionNoBackfill() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	// Create collection with schema version 1
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Description:        "test collection",
			Version:            1,
			DoPhysicalBackfill: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Create segment with same schema version (no backfill needed)
	segmentID := int64(101)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
			SchemaVersion: 1, // Same as collection schema version
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BackfillCompactionPolicySuite) TestTriggerWithCompactingSegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Description:        "test collection",
			Version:            2,
			DoPhysicalBackfill: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Create segment that is compacting (should be filtered out)
	segmentID := int64(101)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
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
	segment.isCompacting = true // Mark as compacting
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BackfillCompactionPolicySuite) TestTriggerWithImportingSegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Description:        "test collection",
			Version:            2,
			DoPhysicalBackfill: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Create segment that is importing (should be filtered out)
	segmentID := int64(101)
	segment := &SegmentInfo{
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BackfillCompactionPolicySuite) TestTriggerWithInvisibleSegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Description:        "test collection",
			Version:            2,
			DoPhysicalBackfill: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Create segment that is invisible (should be filtered out)
	segmentID := int64(101)
	segment := &SegmentInfo{
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BackfillCompactionPolicySuite) TestTriggerWithUnhealthySegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Description:        "test collection",
			Version:            2,
			DoPhysicalBackfill: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Create segment that is not healthy (should be filtered out)
	segmentID := int64(101)
	segment := &SegmentInfo{
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	// AllocID should NOT be called: unhealthy segment is filtered before physical backfill.
	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BackfillCompactionPolicySuite) TestTriggerWithNonFlushedSegment() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Description:        "test collection",
			Version:            2,
			DoPhysicalBackfill: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Create segment that is not flushed (should be filtered out)
	segmentID := int64(101)
	segment := &SegmentInfo{
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	// AllocID should NOT be called: non-flushed segment is filtered before physical backfill.
	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.False(ok)
	s.Nil(gotViews)
}

func (s *BackfillCompactionPolicySuite) TestGetMissingFunctionsWithChildFields() {
	// getMissingFunctions must use ChildFields (real field IDs) not FieldID (columnGroupID).
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk"},
			{FieldID: 101, Name: "text"},
			{FieldID: 102, Name: "sparse"},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm25_func", OutputFieldIds: []int64{102}},
		},
	}

	// Segment with ChildFields containing field 102 → no missing functions
	segPresent := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     0, // columnGroupID, not a real field ID
					ChildFields: []int64{100, 101, 102},
					Binlogs:     []*datapb.Binlog{{LogPath: "p1"}},
				},
			},
		},
	}
	s.Empty(s.backfillPolicy.getMissingFunctions(segPresent, schema))

	// Segment with ChildFields NOT containing field 102 → missing bm25_func
	segMissing := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     0,
					ChildFields: []int64{100, 101},
					Binlogs:     []*datapb.Binlog{{LogPath: "p1"}},
				},
			},
		},
	}
	missing := s.backfillPolicy.getMissingFunctions(segMissing, schema)
	s.Equal(1, len(missing))
	s.Equal("bm25_func", missing[0].GetName())

	// Segment where FieldID happens to equal 102 (columnGroupID collision) but
	// ChildFields does NOT contain 102 → must still report as missing
	segFalsePositive := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     102, // columnGroupID, NOT real field 102
					ChildFields: []int64{100, 101},
					Binlogs:     []*datapb.Binlog{{LogPath: "p1"}},
				},
			},
		},
	}
	missing = s.backfillPolicy.getMissingFunctions(segFalsePositive, schema)
	s.Equal(1, len(missing))
	s.Equal("bm25_func", missing[0].GetName())
}

func (s *BackfillCompactionPolicySuite) TestTriggerWithOutdatedSchemaVersion() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)

	// Create collection with schema version 2, with a BM25 function whose output field 102 is missing from segment
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Description:        "test collection",
			Version:            2,
			DoPhysicalBackfill: true,
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
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Create segment with outdated schema version 1, binlogs only have field 101 (missing 102)
	segmentID := int64(101)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
			SchemaVersion: 1,
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews))

	// Verify the view
	view, ok := gotViews[0].(*BackfillSegmentsView)
	s.True(ok)
	s.NotNil(view)
	s.Equal(int64(1), view.GetTriggerID())
	s.Equal(1, len(view.GetSegmentsView()))
	s.Equal(segmentID, view.GetSegmentsView()[0].ID)
	// Verify funcDiff contains the missing function
	s.Equal(1, len(view.funcDiff.Added))
	s.Equal("bm25_func", view.funcDiff.Added[0].GetName())
}

func (s *BackfillCompactionPolicySuite) TestTriggerInlineNoPhysicalBackfill() {
	// TriggerInline must emit an inline-executable view when DoPhysicalBackfill=false.
	// Trigger() skips this collection entirely; only TriggerInline handles it.
	ctx := context.Background()

	collID := int64(100)

	// Create collection with schema version 2, but DoPhysicalBackfill = false
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Description:        "test collection",
			Version:            2,
			DoPhysicalBackfill: false, // No physical backfill
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Create segment with outdated schema version 1
	segmentID := int64(101)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	// AllocID must NOT be called: DoPhysicalBackfill=false produces only an
	// inline-executable view (meta-only). TriggerInline handles it; Trigger() skips it.
	events, err := s.backfillPolicy.TriggerInline(ctx)
	s.NoError(err)

	views, ok := events[TriggerTypeBackfill]
	s.Require().True(ok)
	s.Require().Len(views, 1)
	bv, ok := views[0].(*BackfillSegmentsView)
	s.Require().True(ok)
	s.True(bv.IsInlineExecutable(), "meta-only path must be inline-executable")
	s.Equal(int32(2), bv.targetSchemaVersion)
	s.Require().Len(bv.GetSegmentsView(), 1)
	s.Equal(segmentID, bv.GetSegmentsView()[0].ID)

	// Trigger() must return nothing for this collection (DoPhysicalBackfill=false).
	triggerEvents, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	s.Empty(triggerEvents[TriggerTypeBackfill])
}

func (s *BackfillCompactionPolicySuite) TestTriggerInlineMetadataOnlyWithAutoCompactionDisabled() {
	// Metadata-only backfill (DoPhysicalBackfill=false) must proceed via TriggerInline
	// even when EnableAutoCompaction is disabled. Otherwise GetCollectionStatistics
	// deadlocks waiting for schema version consistency that can never be achieved.
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Version:            2,
			DoPhysicalBackfill: false,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "new_field", DataType: schemapb.DataType_VarChar},
			},
		},
	}
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	segmentID := int64(101)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
			SchemaVersion: 1,
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	events, err := s.backfillPolicy.TriggerInline(ctx)
	s.NoError(err)
	// TriggerInline must emit inline-executable view regardless of EnableAutoCompaction.
	views, ok := events[TriggerTypeBackfill]
	s.Require().True(ok)
	s.Require().Len(views, 1)
	bv, ok := views[0].(*BackfillSegmentsView)
	s.Require().True(ok)
	s.True(bv.IsInlineExecutable(), "meta-only path must be inline-executable")
	s.Equal(int32(2), bv.targetSchemaVersion)
	s.Require().Len(bv.GetSegmentsView(), 1)
	s.Equal(segmentID, bv.GetSegmentsView()[0].ID)
}

func (s *BackfillCompactionPolicySuite) TestTriggerPhysicalBackfillWithAutoCompactionDisabled() {
	// Physical backfill must also proceed when EnableAutoCompaction is disabled.
	// Backfill is a correctness requirement: without it, segments can never reconcile
	// missing function output fields and schema version consistency will never be reached.
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_collection",
			Version:            2,
			DoPhysicalBackfill: true,
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
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	segmentID := int64(101)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collID,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
			SchemaVersion: 1,
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	// AllocID should be called: physical backfill proceeds even with auto compaction disabled
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews))

	view, ok := gotViews[0].(*BackfillSegmentsView)
	s.True(ok)
	s.Equal(int64(1), view.GetTriggerID())
	s.Equal(1, len(view.GetSegmentsView()))
	s.Equal(segmentID, view.GetSegmentsView()[0].ID)
	s.Equal("bm25_func", view.funcDiff.Added[0].GetName())
}

// TestSchemaFrozenAtScanTime verifies that physical-backfill views capture the
// collection schema at scan time. SubmitBackfillViewToScheduler uses this frozen
// schema for task.Schema, preventing premature schema-version advancement when the
// live collection races ahead between scan and submission.
func (s *BackfillCompactionPolicySuite) TestSchemaFrozenAtScanTime() {
	ctx := context.Background()
	collID := int64(200)
	schemaV1 := &schemapb.CollectionSchema{
		Version:            1,
		DoPhysicalBackfill: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm25_v1", OutputFieldIds: []int64{102}},
		},
	}
	s.backfillPolicy.meta.collections.Insert(collID, &collectionInfo{ID: collID, Schema: schemaV1})
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: int64(201), CollectionID: collID, PartitionID: 10,
			InsertChannel: "ch-1", Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, NumOfRows: 100,
			SchemaVersion: 0,
			StartPosition: &msgpb.MsgPosition{Timestamp: 1},
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "p", EntriesNum: 100}}},
			},
		},
	}
	s.backfillPolicy.meta.segments.SetSegment(int64(201), segment)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	views, ok := events[TriggerTypeBackfill]
	s.True(ok)
	s.Len(views, 1)

	bv, ok := views[0].(*BackfillSegmentsView)
	s.True(ok)
	// The frozen schema must match the collection state at scan time (v1), not any
	// live version the collection might race to before SubmitBackfillViewToScheduler.
	s.NotNil(bv.schema)
	s.Equal(int32(1), bv.schema.GetVersion())
}

// TestMultipleMissingFunctionsSkipped verifies that a segment missing more than one
// function output field is skipped entirely. checkSchemaVersionConsistencyAtRootCoord
// guarantees schema version increments by exactly 1 per DDL and a single DDL adds at
// most one function, so len(missingFunctions)>1 is an invariant violation that must not
// be silently patched.
func (s *BackfillCompactionPolicySuite) TestMultipleMissingFunctionsSkipped() {
	ctx := context.Background()
	collID := int64(300)

	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_multi_func",
			Version:            2,
			DoPhysicalBackfill: true,
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
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Segment at version=0, missing both function output fields (102 and 103).
	segmentID := int64(301)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: segmentID, CollectionID: collID, PartitionID: 10,
			InsertChannel: "ch-1", Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, NumOfRows: 100,
			SchemaVersion: 0,
			StartPosition: &msgpb.MsgPosition{Timestamp: 1},
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "p", EntriesNum: 100}}},
			},
		},
	}
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	// Invariant violation: segment must be skipped, no view emitted.
	s.Empty(events[TriggerTypeBackfill])
}

// TestTriggerInlineSelfHeal verifies the self-heal path: DoPhysicalBackfill=true but
// segment already has all function output fields → stale SchemaVersion anomaly, fixed
// via TriggerInline without dispatching a compaction task.
func (s *BackfillCompactionPolicySuite) TestTriggerInlineSelfHeal() {
	ctx := context.Background()
	collID := int64(400)
	coll := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:               "test_self_heal",
			Version:            2,
			DoPhysicalBackfill: true,
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
	s.backfillPolicy.meta.collections.Insert(collID, coll)

	// Segment at version=1 (stale) but already has field 102 present in binlogs.
	segmentID := int64(401)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: segmentID, CollectionID: collID, PartitionID: 10,
			InsertChannel: "ch-1", Level: datapb.SegmentLevel_L1,
			State: commonpb.SegmentState_Flushed, NumOfRows: 100,
			SchemaVersion: 1, // stale
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
	s.backfillPolicy.meta.segments.SetSegment(segmentID, segment)

	// TriggerInline must emit an inline-executable self-heal view (no AllocID call).
	events, err := s.backfillPolicy.TriggerInline(ctx)
	s.NoError(err)
	views, ok := events[TriggerTypeBackfill]
	s.Require().True(ok)
	s.Require().Len(views, 1)
	bv, ok := views[0].(*BackfillSegmentsView)
	s.Require().True(ok)
	s.True(bv.IsInlineExecutable(), "self-heal path must be inline-executable")
	s.Equal(int32(2), bv.targetSchemaVersion)
	s.Require().Len(bv.GetSegmentsView(), 1)
	s.Equal(segmentID, bv.GetSegmentsView()[0].ID)

	// Trigger() must return nothing for this segment (self-heal is TriggerInline only).
	triggerEvents, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	s.Empty(triggerEvents[TriggerTypeBackfill])
}
