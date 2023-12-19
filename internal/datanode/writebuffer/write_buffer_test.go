package writebuffer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	s.metacache.EXPECT().Schema().Return(s.collSchema).Maybe()
	s.metacache.EXPECT().Collection().Return(s.collID).Maybe()
	s.wb = newWriteBufferBase(s.channelName, s.metacache, nil, s.syncMgr, &writeBufferOption{
		pkStatsFactory: func(vchannel *datapb.SegmentInfo) *metacache.BloomFilterSet {
			return metacache.NewBloomFilterSet()
		},
	})
}

func (s *WriteBufferSuite) TestDefaultOption() {
	s.Run("default BFPkOracle", func() {
		wb, err := NewWriteBuffer(s.channelName, s.metacache, nil, s.syncMgr)
		s.NoError(err)
		_, ok := wb.(*bfWriteBuffer)
		s.True(ok)
	})

	s.Run("default L0Delta policy", func() {
		paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableLevelZeroSegment.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableLevelZeroSegment.Key)
		wb, err := NewWriteBuffer(s.channelName, s.metacache, nil, s.syncMgr, WithIDAllocator(allocator.NewMockGIDAllocator()))
		s.NoError(err)
		_, ok := wb.(*l0WriteBuffer)
		s.True(ok)
	})
}

func (s *WriteBufferSuite) TestWriteBufferType() {
	wb, err := NewWriteBuffer(s.channelName, s.metacache, nil, s.syncMgr, WithDeletePolicy(DeletePolicyBFPkOracle))
	s.NoError(err)

	_, ok := wb.(*bfWriteBuffer)
	s.True(ok)

	wb, err = NewWriteBuffer(s.channelName, s.metacache, nil, s.syncMgr, WithDeletePolicy(DeletePolicyL0Delta), WithIDAllocator(allocator.NewMockGIDAllocator()))
	s.NoError(err)
	_, ok = wb.(*l0WriteBuffer)
	s.True(ok)

	_, err = NewWriteBuffer(s.channelName, s.metacache, nil, s.syncMgr, WithDeletePolicy(""))
	s.Error(err)
}

func (s *WriteBufferSuite) TestHasSegment() {
	segmentID := int64(1001)

	s.False(s.wb.HasSegment(segmentID))

	s.wb.getOrCreateBuffer(segmentID)

	s.True(s.wb.HasSegment(segmentID))
}

func (s *WriteBufferSuite) TestFlushSegments() {
	segmentID := int64(1001)

	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything, mock.Anything)

	wb, err := NewWriteBuffer(s.channelName, s.metacache, nil, s.syncMgr, WithDeletePolicy(DeletePolicyBFPkOracle))
	s.NoError(err)

	err = wb.FlushSegments(context.Background(), []int64{segmentID})
	s.NoError(err)
}

func (s *WriteBufferSuite) TestGetCheckpoint() {
	s.Run("use_consume_cp", func() {
		s.wb.checkpoint = &msgpb.MsgPosition{
			Timestamp: 1000,
		}

		s.syncMgr.EXPECT().GetEarliestPosition(s.channelName).Return(0, nil).Once()

		checkpoint := s.wb.GetCheckpoint()
		s.EqualValues(1000, checkpoint.GetTimestamp())
	})

	s.Run("use_sync_mgr_cp", func() {
		s.wb.checkpoint = &msgpb.MsgPosition{
			Timestamp: 1000,
		}

		s.syncMgr.EXPECT().GetEarliestPosition(s.channelName).Return(1, &msgpb.MsgPosition{
			Timestamp: 500,
		}).Once()

		checkpoint := s.wb.GetCheckpoint()
		s.EqualValues(500, checkpoint.GetTimestamp())
	})

	s.Run("use_segment_buffer_min", func() {
		s.wb.checkpoint = &msgpb.MsgPosition{
			Timestamp: 1000,
		}

		s.syncMgr.EXPECT().GetEarliestPosition(s.channelName).Return(0, nil).Once()

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

		s.syncMgr.EXPECT().GetEarliestPosition(s.channelName).Return(1, &msgpb.MsgPosition{
			Timestamp: 300,
		}).Once()

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

		s.syncMgr.EXPECT().GetEarliestPosition(s.channelName).Return(1, &msgpb.MsgPosition{
			Timestamp: 800,
		}).Once()

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

func TestWriteBufferBase(t *testing.T) {
	suite.Run(t, new(WriteBufferSuite))
}
