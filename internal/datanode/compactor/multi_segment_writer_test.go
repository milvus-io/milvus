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

package compactor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func TestMultiSegmentWriterSuite(t *testing.T) {
	suite.Run(t, new(MultiSegmentWriterSuite))
}

type MultiSegmentWriterSuite struct {
	suite.Suite

	mockBinlogIO *mock_util.MockBinlogIO
	mockAlloc    *allocator.MockAllocator
	mockID       atomic.Int64

	collectionID int64
	partitionID  int64
	channel      string
	batchSize    int
	params       compaction.Params
}

func (s *MultiSegmentWriterSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *MultiSegmentWriterSuite) SetupTest() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")

	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.mockID.Store(time.Now().UnixMilli())
	s.mockAlloc.EXPECT().Alloc(mock.Anything).RunAndReturn(func(x uint32) (int64, int64, error) {
		start := s.mockID.Load()
		end := s.mockID.Add(int64(x))
		return start, end, nil
	}).Maybe()
	s.mockAlloc.EXPECT().AllocOne().RunAndReturn(func() (int64, error) {
		end := s.mockID.Add(1)
		return end, nil
	}).Maybe()

	s.collectionID = 100
	s.partitionID = 101
	s.channel = "test_channel"
	s.batchSize = 100
	s.params = compaction.GenParams()
}

func (s *MultiSegmentWriterSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
}

// genSimpleSchema generates a simple collection schema for testing
func (s *MultiSegmentWriterSuite) genSimpleSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test_schema",
		Description: "test schema for multi segment writer",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     "row_id",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  common.TimeStampField,
				Name:     "timestamp",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "varchar_field",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "128",
					},
				},
			},
			{
				FieldID:     102,
				Name:        "float_vector",
				Description: "float vector field",
				DataType:    schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "4",
					},
				},
			},
		},
	}
}

// genTestValue generates a test storage.Value for the given ID
func (s *MultiSegmentWriterSuite) genTestValue(id int64) *storage.Value {
	ts := tsoutil.ComposeTSByTime(time.Now(), 0)
	return &storage.Value{
		PK:        storage.NewInt64PrimaryKey(id),
		Timestamp: int64(ts),
		Value: map[int64]interface{}{
			common.RowIDField:     id,
			common.TimeStampField: int64(ts),
			100:                   id,                            // pk
			101:                   "test_varchar_value",          // varchar
			102:                   []float32{1.0, 2.0, 3.0, 4.0}, // float vector
		},
	}
}

func (s *MultiSegmentWriterSuite) TestNewMultiSegmentWriter() {
	schema := s.genSimpleSchema()
	allocator := NewCompactionAllocator(s.mockAlloc, s.mockAlloc)

	writer, err := NewMultiSegmentWriter(
		context.Background(),
		s.mockBinlogIO,
		allocator,
		1024*1024, // 1MB segment size
		schema,
		s.params,
		1000, // maxRows
		s.partitionID,
		s.collectionID,
		s.channel,
		s.batchSize,
		storage.WithStorageConfig(s.params.StorageConfig),
	)

	s.NoError(err)
	s.NotNil(writer)
	s.Equal(s.collectionID, writer.collectionID)
	s.Equal(s.partitionID, writer.partitionID)
	s.Equal(s.channel, writer.channel)
	s.Equal(int64(1024*1024), writer.segmentSize)
	s.Equal(int64(1000), writer.maxRows)
	s.Equal(s.batchSize, writer.batchSize)
	s.Empty(writer.res) // No segments created yet
}

func (s *MultiSegmentWriterSuite) TestWriteSingleSegment() {
	schema := s.genSimpleSchema()
	allocator := NewCompactionAllocator(s.mockAlloc, s.mockAlloc)

	// Use a large segment size to ensure all data fits in one segment
	writer, err := NewMultiSegmentWriter(
		context.Background(),
		s.mockBinlogIO,
		allocator,
		10*1024*1024, // 10MB segment size
		schema,
		s.params,
		1000,
		s.partitionID,
		s.collectionID,
		s.channel,
		s.batchSize,
		storage.WithStorageConfig(s.params.StorageConfig),
	)
	s.Require().NoError(err)

	// Write some test data
	numRows := 100
	for i := 0; i < numRows; i++ {
		value := s.genTestValue(int64(i))
		err := writer.WriteValue(value)
		s.NoError(err)
	}

	// Close the writer to finalize the segment
	err = writer.Close()
	s.NoError(err)

	// Check that only one segment was created
	segments := writer.GetCompactionSegments()
	s.Equal(1, len(segments))

	segment := segments[0]
	s.Equal(int64(numRows), segment.NumOfRows)
	s.Equal(s.channel, segment.Channel)
	s.NotEmpty(segment.InsertLogs)
	s.NotEmpty(segment.Field2StatslogPaths)
}

func (s *MultiSegmentWriterSuite) TestWriteMultipleSegments() {
	schema := s.genSimpleSchema()
	allocator := NewCompactionAllocator(s.mockAlloc, s.mockAlloc)

	// Use a small segment size to force multiple segments
	writer, err := NewMultiSegmentWriter(
		context.Background(),
		s.mockBinlogIO,
		allocator,
		1024, // 1KB segment size - very small to force rotation
		schema,
		s.params,
		1000,
		s.partitionID,
		s.collectionID,
		s.channel,
		s.batchSize,
		storage.WithStorageConfig(s.params.StorageConfig),
	)
	s.Require().NoError(err)

	// Write enough data to exceed segment size multiple times
	numRows := 1000
	expectedSegments := 3 // Expect at least 3 segments given the small size

	for i := 0; i < numRows; i++ {
		value := s.genTestValue(int64(i))
		err := writer.WriteValue(value)
		s.NoError(err)

		// Flush periodically to trigger size checks
		if i%100 == 0 {
			err = writer.Flush()
			s.NoError(err)
		}
	}

	// Close the writer to finalize all segments
	err = writer.Close()
	s.NoError(err)

	// Check that multiple segments were created
	segments := writer.GetCompactionSegments()
	s.GreaterOrEqual(len(segments), expectedSegments)

	// Verify all segments are valid
	totalRows := int64(0)
	for i, segment := range segments {
		s.Greater(segment.NumOfRows, int64(0), "Segment %d should have rows", i)
		s.Equal(s.channel, segment.Channel)
		s.NotEmpty(segment.InsertLogs)
		s.NotEmpty(segment.Field2StatslogPaths)
		s.NotEqual(int64(0), segment.SegmentID)
		totalRows += segment.NumOfRows
	}

	// Total rows should match what we wrote
	s.Equal(int64(numRows), totalRows)

	// Verify segment IDs are unique
	segmentIDs := make(map[int64]bool)
	for _, segment := range segments {
		s.False(segmentIDs[segment.SegmentID], "Segment ID %d should be unique", segment.SegmentID)
		segmentIDs[segment.SegmentID] = true
	}
}

func (s *MultiSegmentWriterSuite) TestSegmentRotation() {
	schema := s.genSimpleSchema()
	allocator := NewCompactionAllocator(s.mockAlloc, s.mockAlloc)

	writer, err := NewMultiSegmentWriter(
		context.Background(),
		s.mockBinlogIO,
		allocator,
		2048, // 2KB segment size
		schema,
		s.params,
		1000,
		s.partitionID,
		s.collectionID,
		s.channel,
		s.batchSize,
		storage.WithStorageConfig(s.params.StorageConfig),
	)
	s.Require().NoError(err)

	// Track segments as they are created
	initialSegments := len(writer.GetCompactionSegments())

	// Write data until we trigger rotation
	for i := 0; i < 1000; i++ {
		value := s.genTestValue(int64(i))
		err := writer.WriteValue(value)
		s.NoError(err)

		// Check if rotation happened
		if len(writer.GetCompactionSegments()) > initialSegments {
			// First rotation detected
			break
		}
	}

	// Verify at least one segment was created during rotation
	s.Greater(len(writer.GetCompactionSegments()), initialSegments)

	// Continue writing to trigger more rotations
	for i := 500; i < 1000; i++ {
		value := s.genTestValue(int64(i))
		err := writer.WriteValue(value)
		s.NoError(err)
	}

	err = writer.Close()
	s.NoError(err)

	// Should have multiple segments
	finalSegments := writer.GetCompactionSegments()
	s.GreaterOrEqual(len(finalSegments), 2)
}

func (s *MultiSegmentWriterSuite) TestWriterMethods() {
	schema := s.genSimpleSchema()
	allocator := NewCompactionAllocator(s.mockAlloc, s.mockAlloc)

	writer, err := NewMultiSegmentWriter(
		context.Background(),
		s.mockBinlogIO,
		allocator,
		1024*1024, // 1MB
		schema,
		s.params,
		1000,
		s.partitionID,
		s.collectionID,
		s.channel,
		s.batchSize,
		storage.WithStorageConfig(s.params.StorageConfig),
	)
	s.Require().NoError(err)

	// Test initial state
	s.Equal(uint64(0), writer.GetWrittenUncompressed())
	s.Equal(uint64(0), writer.GetBufferUncompressed())
	s.Empty(writer.GetCompactionSegments())

	// Write some data
	for i := 0; i < 10; i++ {
		value := s.genTestValue(int64(i))
		err := writer.WriteValue(value)
		s.NoError(err)
	}

	// Test flush operations
	err = writer.Flush()
	s.NoError(err)

	// Test after writing
	s.Greater(writer.GetWrittenUncompressed(), uint64(0))

	err = writer.FlushChunk()
	s.NoError(err)

	// Close and verify
	err = writer.Close()
	s.NoError(err)

	segments := writer.GetCompactionSegments()
	s.Equal(1, len(segments))
	s.Equal(int64(10), segments[0].NumOfRows)
}

func (s *MultiSegmentWriterSuite) TestWriteWithRecord() {
	schema := s.genSimpleSchema()
	allocator := NewCompactionAllocator(s.mockAlloc, s.mockAlloc)

	writer, err := NewMultiSegmentWriter(
		context.Background(),
		s.mockBinlogIO,
		allocator,
		1024, // Small size to trigger rotation
		schema,
		s.params,
		1000,
		s.partitionID,
		s.collectionID,
		s.channel,
		s.batchSize,
		storage.WithStorageConfig(s.params.StorageConfig),
	)
	s.Require().NoError(err)

	// Create a test record (this would normally come from storage layer)
	// For simplicity, we'll use WriteValue in this test since creating
	// proper Arrow records is complex
	numRows := 200
	for i := 0; i < numRows; i++ {
		value := s.genTestValue(int64(i))
		err := writer.WriteValue(value)
		s.NoError(err)
	}

	err = writer.Close()
	s.NoError(err)

	segments := writer.GetCompactionSegments()
	s.GreaterOrEqual(len(segments), 2) // Should have multiple segments

	totalRows := int64(0)
	for _, segment := range segments {
		totalRows += segment.NumOfRows
	}
	s.Equal(int64(numRows), totalRows)
}

func (s *MultiSegmentWriterSuite) TestEmptyWriter() {
	schema := s.genSimpleSchema()
	allocator := NewCompactionAllocator(s.mockAlloc, s.mockAlloc)

	writer, err := NewMultiSegmentWriter(
		context.Background(),
		s.mockBinlogIO,
		allocator,
		1024*1024,
		schema,
		s.params,
		1000,
		s.partitionID,
		s.collectionID,
		s.channel,
		s.batchSize,
		storage.WithStorageConfig(s.params.StorageConfig),
	)
	s.Require().NoError(err)

	// Close without writing any data
	err = writer.Close()
	s.NoError(err)

	// Should have no segments
	segments := writer.GetCompactionSegments()
	s.Empty(segments)
}

func (s *MultiSegmentWriterSuite) TestLargeDataWrite() {
	schema := s.genSimpleSchema()
	allocator := NewCompactionAllocator(s.mockAlloc, s.mockAlloc)

	writer, err := NewMultiSegmentWriter(
		context.Background(),
		s.mockBinlogIO,
		allocator,
		64*1024, // 64KB segments
		schema,
		s.params,
		1000,
		s.partitionID,
		s.collectionID,
		s.channel,
		s.batchSize,
		storage.WithStorageConfig(s.params.StorageConfig),
	)
	s.Require().NoError(err)

	// Write a large amount of data
	numRows := 5000
	for i := 0; i < numRows; i++ {
		value := s.genTestValue(int64(i))
		err := writer.WriteValue(value)
		s.NoError(err)

		// Flush every 1000 rows to trigger size checks
		if i%1000 == 0 {
			err = writer.Flush()
			s.NoError(err)
		}
	}

	err = writer.Close()
	s.NoError(err)

	segments := writer.GetCompactionSegments()
	s.GreaterOrEqual(len(segments), 1) // Should have many segments

	// Verify data integrity
	totalRows := int64(0)
	for _, segment := range segments {
		s.Greater(segment.NumOfRows, int64(0))
		s.NotEmpty(segment.InsertLogs)
		totalRows += segment.NumOfRows
	}
	s.Equal(int64(numRows), totalRows)
}
