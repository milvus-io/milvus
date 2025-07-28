package writebuffer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type WriteBufferSuite struct {
	suite.Suite
	collID      int64
	channelName string
	collSchema  *schemapb.CollectionSchema
	wb          *writeBufferBase
	syncMgr     *syncmgr.MockSyncManager
	metacache   *metacache.MockMetaCache
}

func (s *WriteBufferSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	s.collID = 100
	s.collSchema = &schemapb.CollectionSchema{
		Name: "wb_base_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			}},
		},
	}
	s.channelName = "by-dev-rootcoord-dml_0v0"
}

func (s *WriteBufferSuite) SetupTest() {
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
	s.metacache = metacache.NewMockMetaCache(s.T())
	s.metacache.EXPECT().GetSchema(mock.Anything).Return(s.collSchema).Maybe()
	s.metacache.EXPECT().Collection().Return(s.collID).Maybe()
	var err error
	s.wb, err = newWriteBufferBase(s.channelName, s.metacache, s.syncMgr, &writeBufferOption{
		pkStatsFactory: func(vchannel *datapb.SegmentInfo) pkoracle.PkStat {
			return pkoracle.NewBloomFilterSet()
		},
	})
	s.Require().NoError(err)
}

func (s *WriteBufferSuite) TestHasSegment() {
	segmentID := int64(1001)

	s.False(s.wb.HasSegment(segmentID))

	s.wb.getOrCreateBuffer(segmentID, 0)

	s.True(s.wb.HasSegment(segmentID))
}

func (s *WriteBufferSuite) TestFlushSegments() {
	segmentID := int64(1001)

	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything, mock.Anything).Return()
	s.metacache.EXPECT().GetSegmentByID(mock.Anything, mock.Anything, mock.Anything).Return(nil, true)
	wb, err := NewWriteBuffer(s.channelName, s.metacache, s.syncMgr, WithIDAllocator(allocator.NewMockAllocator(s.T())))
	s.NoError(err)

	err = wb.SealSegments(context.Background(), []int64{segmentID})
	s.NoError(err)
}

func (s *WriteBufferSuite) TestGetCheckpoint() {
	s.Run("use_consume_cp", func() {
		s.wb.checkpoint = &msgpb.MsgPosition{
			Timestamp: 1000,
		}

		checkpoint := s.wb.GetCheckpoint()
		s.EqualValues(1000, checkpoint.GetTimestamp())
	})

	s.Run("use_syncing_segment_cp", func() {
		s.wb.checkpoint = &msgpb.MsgPosition{
			Timestamp: 1000,
		}

		s.wb.syncCheckpoint.Add(1, &msgpb.MsgPosition{Timestamp: 500}, "syncing segments")
		defer s.wb.syncCheckpoint.Remove(1, 500)

		checkpoint := s.wb.GetCheckpoint()
		s.EqualValues(500, checkpoint.GetTimestamp())
	})

	s.Run("use_segment_buffer_min", func() {
		s.wb.checkpoint = &msgpb.MsgPosition{
			Timestamp: 1000,
		}

		s.wb.syncCheckpoint.Add(1, &msgpb.MsgPosition{Timestamp: 500}, "syncing segments")
		defer s.wb.syncCheckpoint.Remove(1, 500)

		buf1, err := newSegmentBuffer(2, s.collSchema)
		s.Require().NoError(err)
		buf1.insertBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 440,
		}
		buf1.deltaBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 400,
		}
		buf2, err := newSegmentBuffer(3, s.collSchema)
		s.Require().NoError(err)
		buf2.insertBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 550,
		}
		buf2.deltaBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 600,
		}

		s.wb.mut.Lock()
		s.wb.buffers[2] = buf1
		s.wb.buffers[3] = buf2
		s.wb.mut.Unlock()

		defer func() {
			s.wb.mut.Lock()
			defer s.wb.mut.Unlock()
			s.wb.buffers = make(map[int64]*segmentBuffer)
		}()

		checkpoint := s.wb.GetCheckpoint()
		s.EqualValues(400, checkpoint.GetTimestamp())
	})

	s.Run("sync_mgr_smaller", func() {
		s.wb.checkpoint = &msgpb.MsgPosition{
			Timestamp: 1000,
		}

		s.wb.syncCheckpoint.Add(1, &msgpb.MsgPosition{Timestamp: 300}, "syncing segments")
		defer s.wb.syncCheckpoint.Remove(1, 300)

		buf1, err := newSegmentBuffer(2, s.collSchema)
		s.Require().NoError(err)
		buf1.insertBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 440,
		}
		buf1.deltaBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 400,
		}
		buf2, err := newSegmentBuffer(3, s.collSchema)
		s.Require().NoError(err)
		buf2.insertBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 550,
		}
		buf2.deltaBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 600,
		}

		s.wb.mut.Lock()
		s.wb.buffers[2] = buf1
		s.wb.buffers[3] = buf2
		s.wb.mut.Unlock()

		defer func() {
			s.wb.mut.Lock()
			defer s.wb.mut.Unlock()
			s.wb.buffers = make(map[int64]*segmentBuffer)
		}()

		checkpoint := s.wb.GetCheckpoint()
		s.EqualValues(300, checkpoint.GetTimestamp())
	})

	s.Run("segment_buffer_smaller", func() {
		s.wb.checkpoint = &msgpb.MsgPosition{
			Timestamp: 1000,
		}

		s.wb.syncCheckpoint.Add(1, &msgpb.MsgPosition{Timestamp: 800}, "syncing segments")
		defer s.wb.syncCheckpoint.Remove(1, 800)

		buf1, err := newSegmentBuffer(2, s.collSchema)
		s.Require().NoError(err)
		buf1.insertBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 440,
		}
		buf1.deltaBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 400,
		}
		buf2, err := newSegmentBuffer(3, s.collSchema)
		s.Require().NoError(err)
		buf2.insertBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 550,
		}
		buf2.deltaBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 600,
		}

		s.wb.mut.Lock()
		s.wb.buffers[2] = buf1
		s.wb.buffers[3] = buf2
		s.wb.mut.Unlock()

		defer func() {
			s.wb.mut.Lock()
			defer s.wb.mut.Unlock()
			s.wb.buffers = make(map[int64]*segmentBuffer)
		}()

		checkpoint := s.wb.GetCheckpoint()
		s.EqualValues(400, checkpoint.GetTimestamp())
	})
}

func (s *WriteBufferSuite) TestEvictBuffer() {
	wb, err := newWriteBufferBase(s.channelName, s.metacache, s.syncMgr, &writeBufferOption{
		pkStatsFactory: func(vchannel *datapb.SegmentInfo) pkoracle.PkStat {
			return pkoracle.NewBloomFilterSet()
		},
	})
	s.Require().NoError(err)

	s.Run("no_checkpoint", func() {
		wb.mut.Lock()
		wb.buffers[100] = &segmentBuffer{}
		wb.mut.Unlock()
		defer func() {
			wb.mut.Lock()
			defer wb.mut.Unlock()
			wb.buffers = make(map[int64]*segmentBuffer)
		}()

		wb.EvictBuffer(GetOldestBufferPolicy(1))
	})

	s.Run("trigger_sync", func() {
		buf1, err := newSegmentBuffer(2, s.collSchema)
		s.Require().NoError(err)
		buf1.insertBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 440,
		}
		buf1.deltaBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 400,
		}
		buf2, err := newSegmentBuffer(3, s.collSchema)
		s.Require().NoError(err)
		buf2.insertBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 550,
		}
		buf2.deltaBuffer.startPos = &msgpb.MsgPosition{
			Timestamp: 600,
		}

		wb.mut.Lock()
		wb.buffers[2] = buf1
		wb.buffers[3] = buf2
		wb.checkpoint = &msgpb.MsgPosition{Timestamp: 100}
		wb.mut.Unlock()

		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2,
		}, nil, nil)
		s.metacache.EXPECT().GetSegmentByID(int64(2)).Return(segment, true)
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.Anything, mock.Anything).Return(conc.Go[struct{}](func() (struct{}, error) {
			return struct{}{}, nil
		}), nil)
		defer func() {
			s.wb.mut.Lock()
			defer s.wb.mut.Unlock()
			s.wb.buffers = make(map[int64]*segmentBuffer)
		}()
		wb.EvictBuffer(GetOldestBufferPolicy(1))
	})
}

func (s *WriteBufferSuite) TestDropPartitions() {
	wb, err := newWriteBufferBase(s.channelName, s.metacache, s.syncMgr, &writeBufferOption{
		pkStatsFactory: func(vchannel *datapb.SegmentInfo) pkoracle.PkStat {
			return pkoracle.NewBloomFilterSet()
		},
	})
	s.Require().NoError(err)

	segIDs := []int64{1, 2, 3}
	s.metacache.EXPECT().GetSegmentIDsBy(mock.Anything).Return(segIDs).Once()
	s.metacache.EXPECT().UpdateSegments(mock.AnythingOfType("metacache.SegmentAction"), metacache.WithSegmentIDs(segIDs...)).Return().Once()

	wb.dropPartitions([]int64{100, 101})
}

func TestWriteBufferBase(t *testing.T) {
	suite.Run(t, new(WriteBufferSuite))
}

// TestPrepareInsertWithMissingFields tests the missing field filling functionality
func TestPrepareInsertWithMissingFields(t *testing.T) {
	// Create a collection schema with multiple fields including nullable and default value fields
	collSchema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, Name: "RowID"},
			{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64, Name: "Timestamp"},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
				AutoID:       true,
			},
			{
				FieldID:  101,
				Name:     "int32_field",
				DataType: schemapb.DataType_Int32,
				Nullable: false,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 42,
					},
				},
			},
			{
				FieldID:  102,
				Name:     "nullable_float_field",
				DataType: schemapb.DataType_Float,
				Nullable: true,
			},
			{
				FieldID:  103,
				Name:     "default_string_field",
				DataType: schemapb.DataType_String,
				Nullable: false,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_StringData{
						StringData: "default_string",
					},
				},
			},
		},
	}

	pkField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true,
	}

	t.Run("test_missing_fields_with_default_values", func(t *testing.T) {
		// Create insert message with only some fields
		insertMsg := &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: 1000,
				EndTimestamp:   1002,
			},
			InsertRequest: &msgpb.InsertRequest{
				SegmentID:    1,
				PartitionID:  1,
				CollectionID: 1,
				FieldsData: []*schemapb.FieldData{
					{
						FieldId:   common.RowIDField,
						FieldName: "RowID",
						Type:      schemapb.DataType_Int64,
						Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_LongData{
									LongData: &schemapb.LongArray{
										Data: []int64{1, 2, 3},
									},
								},
							},
						},
					},
					{
						FieldId:   common.TimeStampField,
						FieldName: "Timestamp",
						Type:      schemapb.DataType_Int64,
						Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_LongData{
									LongData: &schemapb.LongArray{
										Data: []int64{1000, 1001, 1002},
									},
								},
							},
						},
					},
					{
						FieldId:   100, // Primary key field
						FieldName: "pk",
						Type:      schemapb.DataType_Int64,
						Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_LongData{
									LongData: &schemapb.LongArray{
										Data: []int64{1, 2, 3},
									},
								},
							},
						},
					},
					{
						FieldId:   101, // field1 with default value
						FieldName: "int32_field",
						Type:      schemapb.DataType_Int32,
						Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_IntData{
									IntData: &schemapb.IntArray{
										Data: []int32{1, 2, 3},
									},
								},
							},
						},
					},
				},
				// Missing nullable_float_field (nullable)
				// Missing default_string_field (with default value)
				NumRows:    3,
				Version:    msgpb.InsertDataVersion_ColumnBased,
				Timestamps: []uint64{1, 1, 1},
			},
		}

		insertMsgs := []*msgstream.InsertMsg{insertMsg}
		result, err := PrepareInsert(collSchema, pkField, insertMsgs)

		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, int64(3), result[0].rowNum)

		// Check that all fields are present in the result
		insertData := result[0].data[0]
		for i := 0; i < 3; i++ {
			assert.Nil(t, insertData.Data[102].GetRow(i))
			assert.Equal(t, "default_string", insertData.Data[103].GetRow(i))
		}
	})
}
