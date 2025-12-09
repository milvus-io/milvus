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
	"errors"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func TestBackfillCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(BackfillCompactionPolicySuite))
}

type BackfillCompactionPolicySuite struct {
	suite.Suite

	mockAlloc      *allocator.MockAllocator
	mockBroker     *broker.MockBroker
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
	s.mockBroker = broker.NewMockBroker(s.T())
	mockHandler := NewNMockHandler(s.T())
	s.handler = mockHandler
	s.backfillPolicy = newBackfillCompactionPolicy(meta, s.mockAlloc, mockHandler, s.mockBroker)
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
	// Test Enable method
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)
	s.True(s.backfillPolicy.Enable())

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	s.False(s.backfillPolicy.Enable())
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

	// Create schema for the segment's timestamp (same version as collection)
	segmentSchema := &schemapb.CollectionSchema{
		Name:        "test_collection",
		Description: "test collection at timestamp",
		Version:     1, // Same as collection schema version
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}

	// Setup mock broker to return the schema for the segment's timestamp
	s.mockBroker.EXPECT().
		DescribeCollectionInternal(mock.Anything, collID, uint64(10000)).
		Return(buildDescribeCollectionResponse(collID, segmentSchema), nil)

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

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

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

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

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

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

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

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

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

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

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.False(ok)
	s.Nil(gotViews)
}

// buildDescribeCollectionResponse creates a DescribeCollectionResponse with the given schema
func buildDescribeCollectionResponse(collectionID int64, schema *schemapb.CollectionSchema) *milvuspb.DescribeCollectionResponse {
	return &milvuspb.DescribeCollectionResponse{
		Status:         merr.Success(),
		CollectionID:   collectionID,
		CollectionName: schema.GetName(),
		Schema:         schema,
	}
}

func (s *BackfillCompactionPolicySuite) TestGetCollectionSchemaByVersion() {
	ctx := context.Background()
	collID := int64(100)
	timestamp := uint64(10000)

	// Create schema for the segment's timestamp
	segmentSchema := &schemapb.CollectionSchema{
		Name:        "test_collection",
		Description: "test collection at timestamp",
		Version:     1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}

	// Setup mock broker to return the schema for the given timestamp
	s.mockBroker.EXPECT().
		DescribeCollectionInternal(mock.Anything, collID, timestamp).
		Return(buildDescribeCollectionResponse(collID, segmentSchema), nil)

	// Call getCollectionSchemaByVersion
	schema, err := s.backfillPolicy.getCollectionSchemaByVersion(ctx, collID, timestamp)
	s.NoError(err)
	s.NotNil(schema)
	s.Equal(int32(1), schema.GetVersion())
	s.Equal("test_collection", schema.GetName())
}

func (s *BackfillCompactionPolicySuite) TestGetCollectionSchemaByVersionError() {
	ctx := context.Background()
	collID := int64(100)
	timestamp := uint64(10000)

	// Setup mock broker to return an error
	s.mockBroker.EXPECT().
		DescribeCollectionInternal(mock.Anything, collID, timestamp).
		Return(nil, errors.New("broker error"))

	// Call getCollectionSchemaByVersion
	schema, err := s.backfillPolicy.getCollectionSchemaByVersion(ctx, collID, timestamp)
	s.Error(err)
	s.Nil(schema)
	s.Contains(err.Error(), "failed to describe collection")
}

func (s *BackfillCompactionPolicySuite) TestGetCollectionSchemaByVersionNilResponse() {
	ctx := context.Background()
	collID := int64(100)
	timestamp := uint64(10000)

	// Setup mock broker to return nil response
	s.mockBroker.EXPECT().
		DescribeCollectionInternal(mock.Anything, collID, timestamp).
		Return(nil, nil)

	// Call getCollectionSchemaByVersion
	schema, err := s.backfillPolicy.getCollectionSchemaByVersion(ctx, collID, timestamp)
	s.Error(err)
	s.Nil(schema)
	s.Contains(err.Error(), "describe collection response is nil")
}

func (s *BackfillCompactionPolicySuite) TestGetCollectionSchemaByVersionNilSchema() {
	ctx := context.Background()
	collID := int64(100)
	timestamp := uint64(10000)

	// Setup mock broker to return response with nil schema
	s.mockBroker.EXPECT().
		DescribeCollectionInternal(mock.Anything, collID, timestamp).
		Return(&milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: collID,
			Schema:       nil, // Nil schema
		}, nil)

	// Call getCollectionSchemaByVersion
	schema, err := s.backfillPolicy.getCollectionSchemaByVersion(ctx, collID, timestamp)
	s.Error(err)
	s.Nil(schema)
	s.Contains(err.Error(), "schema is nil")
}

func (s *BackfillCompactionPolicySuite) TestTriggerWithOutdatedSchemaVersion() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	timestamp := uint64(10000)

	// Create collection with schema version 2
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
				{FieldID: 102, Name: "sparse_vector", DataType: schemapb.DataType_SparseFloatVector}, // New field in version 2
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
				Timestamp: timestamp,
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

	// Create schema for the segment's timestamp (version 1)
	segmentSchema := &schemapb.CollectionSchema{
		Name:        "test_collection",
		Description: "test collection at timestamp",
		Version:     1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}

	// Setup mock broker to return the schema for the segment's timestamp
	s.mockBroker.EXPECT().
		DescribeCollectionInternal(mock.Anything, collID, timestamp).
		Return(buildDescribeCollectionResponse(collID, segmentSchema), nil)

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(1, len(gotViews)) // Should have one backfill view

	// Verify the view
	view, ok := gotViews[0].(*BackfillSegmentsView)
	s.True(ok)
	s.NotNil(view)
	s.Equal(int64(1), view.GetTriggerID())
	s.Equal(1, len(view.GetSegmentsView()))
	s.Equal(segmentID, view.GetSegmentsView()[0].ID)
}

func (s *BackfillCompactionPolicySuite) TestTriggerWithOutdatedSchemaVersionNoPhysicalBackfill() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)

	collID := int64(100)
	timestamp := uint64(10000)

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
				Timestamp: timestamp,
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

	// Create schema for the segment's timestamp (version 1)
	segmentSchema := &schemapb.CollectionSchema{
		Name:        "test_collection",
		Description: "test collection at timestamp",
		Version:     1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}

	// Setup mock broker to return the schema for the segment's timestamp
	s.mockBroker.EXPECT().
		DescribeCollectionInternal(mock.Anything, collID, timestamp).
		Return(buildDescribeCollectionResponse(collID, segmentSchema), nil)

	// Setup mock catalog to allow AlterSegments call
	s.backfillPolicy.meta.catalog.(*mocks.DataCoordCatalog).EXPECT().
		AlterSegments(mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	// Setup allocator mock
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1), nil)

	// When DoPhysicalBackfill is false, the segment's schema version should be updated
	// but no backfill view should be created.
	events, err := s.backfillPolicy.Trigger(ctx)
	s.NoError(err)
	gotViews, ok := events[TriggerTypeBackfill]
	s.False(ok)
	s.Nil(gotViews)

	// Verify that segment's schema version has been updated to 2
	updatedSegment := s.backfillPolicy.meta.segments.GetSegment(segmentID)
	s.NotNil(updatedSegment)
	s.Equal(int32(2), updatedSegment.GetSchemaVersion(), "segment schema version should be updated to 2")
}
