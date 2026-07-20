package writebuffer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
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

	s.wb.allowGrowingSourceFlush = false
	s.False(s.wb.HasSegment(segmentID))

	s.wb.getOrCreateBuffer(segmentID, 0)

	s.True(s.wb.HasSegment(segmentID))
}

func (s *WriteBufferSuite) TestFlushSourceModeNotifier() {
	segmentID := int64(1001)
	var notifiedSegmentID int64
	var notifiedMode metacache.FlushSourceMode
	s.wb.flushSourceModeNotifier = func(segmentID int64, mode metacache.FlushSourceMode) {
		notifiedSegmentID = segmentID
		notifiedMode = mode
	}

	s.Run("write_buffer_mode", func() {
		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: segmentID}, nil, nil)
		metacache.SetFlushSourceMode(metacache.FlushSourceWriteBuffer)(segment)
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, _ ...metacache.SegmentFilter) {
			action(segment)
		}).Return().Once()
		s.metacache.EXPECT().GetSegmentByID(segmentID).Return(segment, true).Once()

		s.wb.allowGrowingSourceFlush = true
		s.wb.getOrCreateBuffer(segmentID, 0)
		s.Equal(segmentID, notifiedSegmentID)
		s.Equal(metacache.FlushSourceWriteBuffer, notifiedMode)
	})

	s.Run("growing_mode", func() {
		segmentID := int64(1002)
		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: segmentID, StorageVersion: storage.StorageV3}, nil, nil)
		notifiedSegmentID = 0
		notifiedMode = metacache.FlushSourceUnknown
		s.metacache.EXPECT().GetSegmentByID(segmentID).Return(segment, true).Once()
		s.metacache.EXPECT().GetSegmentByID(segmentID).Return(segment, true).Once()
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, _ ...metacache.SegmentFilter) {
			action(segment)
		}).Return().Once()
		s.metacache.EXPECT().GetSegmentByID(segmentID).Return(segment, true).Once()

		err := s.wb.recordGrowingSourceProgress(&InsertData{
			segmentID:   segmentID,
			partitionID: 10,
			rowNum:      3,
		}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 1, 3)
		s.NoError(err)
		s.Equal(segmentID, notifiedSegmentID)
		s.Equal(metacache.FlushSourceGrowing, notifiedMode)
	})
}

func (s *WriteBufferSuite) TestCreateNewGrowingSegmentStorageVersion() {
	param := paramtable.Get()
	param.Save(param.CommonCfg.UseLoonFFI.Key, "false")
	defer param.Reset(param.CommonCfg.UseLoonFFI.Key)
	param.Save(param.CommonCfg.EnableGrowingSourceFlush.Key, "false")
	defer param.Reset(param.CommonCfg.EnableGrowingSourceFlush.Key)

	s.Run("non_text_uses_v2_when_ffi_disabled", func() {
		s.wb.allowGrowingSourceFlush = false
		s.False(s.wb.AllowGrowingSourceFlush())
		s.metacache.EXPECT().GetSegmentByID(int64(2001)).Return(nil, false).Once()
		s.metacache.EXPECT().AddSegment(mock.MatchedBy(func(info *datapb.SegmentInfo) bool {
			return info.GetStorageVersion() == storage.StorageV2 &&
				info.GetManifestPath() == "" &&
				info.GetSchemaVersion() == 11
		}), mock.Anything, mock.Anything, mock.Anything).Return().Once()

		err := s.wb.CreateNewGrowingSegment(CreateGrowingSegmentInfo{
			PartitionID:    10,
			SegmentID:      2001,
			SchemaVersion:  11,
			StorageVersion: storage.StorageV2,
		})
		s.NoError(err)
	})

	s.Run("growing_source_does_not_force_v3_manifest_when_ffi_disabled", func() {
		s.wb.allowGrowingSourceFlush = true
		s.metacache.EXPECT().GetSegmentByID(int64(2002)).Return(nil, false).Once()
		s.metacache.EXPECT().AddSegment(mock.MatchedBy(func(info *datapb.SegmentInfo) bool {
			return info.GetStorageVersion() == storage.StorageV2 &&
				info.GetManifestPath() == "" &&
				info.GetSchemaVersion() == 12
		}), mock.Anything, mock.Anything, mock.Anything).Return().Once()

		err := s.wb.CreateNewGrowingSegment(CreateGrowingSegmentInfo{
			PartitionID:    10,
			SegmentID:      2002,
			SchemaVersion:  12,
			StorageVersion: storage.StorageV2,
		})
		s.NoError(err)
	})

	s.Run("text_schema_uses_v3_manifest_without_enabling_growing_source_when_ffi_disabled", func() {
		textSchema := &schemapb.CollectionSchema{
			Name: "wb_text_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
				{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				}},
				{FieldID: 102, DataType: schemapb.DataType_Text, Name: "text"},
			},
		}
		mc := metacache.NewMockMetaCache(s.T())
		mc.EXPECT().GetSchema(mock.Anything).Return(textSchema).Maybe()
		mc.EXPECT().Collection().Return(s.collID).Maybe()

		wb, err := newWriteBufferBase(s.channelName, mc, s.syncMgr, &writeBufferOption{})
		s.Require().NoError(err)
		s.False(wb.AllowGrowingSourceFlush())

		mc.EXPECT().GetSegmentByID(int64(2003)).Return(nil, false).Once()
		mc.EXPECT().AddSegment(mock.MatchedBy(func(info *datapb.SegmentInfo) bool {
			return info.GetStorageVersion() == storage.StorageV3 &&
				info.GetManifestPath() != "" &&
				info.GetSchemaVersion() == 13
		}), mock.Anything, mock.Anything, mock.Anything).Return().Once()

		err = wb.CreateNewGrowingSegment(CreateGrowingSegmentInfo{
			PartitionID:   10,
			SegmentID:     2003,
			SchemaVersion: 13,
		})
		s.NoError(err)
	})

	s.Run("text_schema_uses_v3_manifest_when_ffi_enabled", func() {
		param.Save(param.CommonCfg.UseLoonFFI.Key, "true")
		defer param.Save(param.CommonCfg.UseLoonFFI.Key, "false")

		textSchema := &schemapb.CollectionSchema{
			Name: "wb_text_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
				{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				}},
				{FieldID: 102, DataType: schemapb.DataType_Text, Name: "text"},
			},
		}
		mc := metacache.NewMockMetaCache(s.T())
		mc.EXPECT().GetSchema(mock.Anything).Return(textSchema).Maybe()
		mc.EXPECT().Collection().Return(s.collID).Maybe()

		wb, err := newWriteBufferBase(s.channelName, mc, s.syncMgr, &writeBufferOption{})
		s.Require().NoError(err)
		s.True(wb.AllowGrowingSourceFlush())

		mc.EXPECT().GetSegmentByID(int64(2004)).Return(nil, false).Once()
		mc.EXPECT().AddSegment(mock.MatchedBy(func(info *datapb.SegmentInfo) bool {
			return info.GetStorageVersion() == storage.StorageV3 &&
				info.GetManifestPath() != "" &&
				info.GetSchemaVersion() == 14
		}), mock.Anything, mock.Anything, mock.Anything).Return().Once()

		err = wb.CreateNewGrowingSegment(CreateGrowingSegmentInfo{
			PartitionID:    10,
			SegmentID:      2004,
			SchemaVersion:  14,
			StorageVersion: storage.StorageV3,
		})
		s.NoError(err)
	})

	s.Run("streaming_requires_create_segment_storage_version", func() {
		s.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "1")

		s.metacache.EXPECT().GetSegmentByID(int64(2005)).Return(nil, false).Once()

		err := s.wb.CreateNewGrowingSegment(CreateGrowingSegmentInfo{
			PartitionID:   10,
			SegmentID:     2005,
			SchemaVersion: 15,
		})
		s.ErrorIs(err, merr.ErrServiceInternal)
	})
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

func (s *WriteBufferSuite) TestSealSegmentsMissingSegment() {
	segmentID := int64(1001)

	s.Run("non_text_returns_error", func() {
		s.wb.allowGrowingSourceFlush = false
		s.metacache.EXPECT().GetSegmentByID(segmentID).Return(nil, false).Once()

		err := s.wb.SealSegments(context.Background(), []int64{segmentID})
		s.ErrorIs(err, merr.ErrSegmentNotFound)
	})

	s.Run("text_skips_missing_segment", func() {
		s.wb.allowGrowingSourceFlush = true
		defer func() {
			s.wb.allowGrowingSourceFlush = false
		}()
		s.metacache.EXPECT().GetSegmentByID(segmentID).Return(nil, false).Once()

		err := s.wb.SealSegments(context.Background(), []int64{segmentID})
		s.NoError(err)
	})
}

func (s *WriteBufferSuite) TestSealAllSegments() {
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything, mock.Anything).Return()
	wb, err := NewWriteBuffer(s.channelName, s.metacache, s.syncMgr, WithIDAllocator(allocator.NewMockAllocator(s.T())))
	s.NoError(err)
	wb.SealAllSegments(context.Background())
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
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.MatchedBy(func(task syncmgr.Task) bool {
			return task != nil && task.SegmentID() == 2
		}), mock.Anything).Return(conc.Go[struct{}](func() (struct{}, error) {
			return struct{}{}, nil
		}), nil).Once()
		defer func() {
			s.wb.mut.Lock()
			defer s.wb.mut.Unlock()
			s.wb.buffers = make(map[int64]*segmentBuffer)
		}()
		wb.EvictBuffer(GetOldestBufferPolicy(1))
	})

	s.Run("sync_submit_outside_lock", func() {
		buf, err := newSegmentBuffer(4, s.collSchema)
		s.Require().NoError(err)
		buf.insertBuffer.startPos = &msgpb.MsgPosition{Timestamp: 440}
		buf.deltaBuffer.startPos = &msgpb.MsgPosition{Timestamp: 400}

		wb.mut.Lock()
		wb.buffers[4] = buf
		wb.checkpoint = &msgpb.MsgPosition{Timestamp: 1000}
		wb.mut.Unlock()

		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID: 4,
		}, nil, nil)
		s.metacache.EXPECT().GetSegmentByID(int64(4)).Return(segment, true)
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

		submitEntered := make(chan struct{})
		releaseSubmit := make(chan struct{})
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.MatchedBy(func(task syncmgr.Task) bool {
			return task != nil && task.SegmentID() == 4
		}), mock.Anything).RunAndReturn(
			func(context.Context, syncmgr.Task, ...func(error) error) (*conc.Future[struct{}], error) {
				close(submitEntered)
				<-releaseSubmit
				return conc.Go[struct{}](func() (struct{}, error) {
					return struct{}{}, nil
				}), nil
			},
		).Once()

		evictDone := make(chan struct{})
		go func() {
			defer close(evictDone)
			wb.EvictBuffer(GetOldestBufferPolicy(1))
		}()

		select {
		case <-submitEntered:
		case <-time.After(3 * time.Second):
			s.FailNow("SyncData should be called before checking the write buffer lock")
		}

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			_ = wb.MemorySize()
		}()

		select {
		case <-readDone:
		case <-time.After(3 * time.Second):
			s.FailNow("write buffer read lock should not be blocked by SyncData submit")
		}

		close(releaseSubmit)
		select {
		case <-evictDone:
		case <-time.After(3 * time.Second):
			s.FailNow("EvictBuffer should finish after SyncData submit is released")
		}
	})

	s.Run("drop_close_sync_submit_outside_lock", func() {
		syncMgr := syncmgr.NewMockSyncManager(s.T())
		metaCache := metacache.NewMockMetaCache(s.T())
		metaWriter := syncmgr.NewMockMetaWriter(s.T())
		metaCache.EXPECT().GetSchema(mock.Anything).Return(s.collSchema).Maybe()
		metaCache.EXPECT().Collection().Return(s.collID).Maybe()

		closeWB, err := newWriteBufferBase(s.channelName, metaCache, syncMgr, &writeBufferOption{
			metaWriter: metaWriter,
			pkStatsFactory: func(vchannel *datapb.SegmentInfo) pkoracle.PkStat {
				return pkoracle.NewBloomFilterSet()
			},
		})
		s.Require().NoError(err)

		buf, err := newSegmentBuffer(5, s.collSchema)
		s.Require().NoError(err)
		buf.insertBuffer.startPos = &msgpb.MsgPosition{Timestamp: 540}
		buf.deltaBuffer.startPos = &msgpb.MsgPosition{Timestamp: 500}

		closeWB.mut.Lock()
		closeWB.buffers[5] = buf
		closeWB.checkpoint = &msgpb.MsgPosition{Timestamp: 1000}
		closeWB.mut.Unlock()

		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID: 5,
		}, nil, nil)
		metaCache.EXPECT().GetSegmentByID(int64(5)).Return(segment, true)
		metaCache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

		submitEntered := make(chan struct{})
		releaseSubmit := make(chan struct{})
		syncMgr.EXPECT().SyncData(mock.Anything, mock.MatchedBy(func(task syncmgr.Task) bool {
			return task != nil && task.SegmentID() == 5 && task.IsDrop()
		}), mock.Anything).RunAndReturn(
			func(context.Context, syncmgr.Task, ...func(error) error) (*conc.Future[struct{}], error) {
				close(submitEntered)
				<-releaseSubmit
				return conc.Go[struct{}](func() (struct{}, error) {
					return struct{}{}, nil
				}), nil
			},
		).Once()
		metaWriter.EXPECT().DropChannel(mock.Anything, s.channelName).Return(nil).Once()

		closeDone := make(chan struct{})
		go func() {
			defer close(closeDone)
			closeWB.Close(context.Background(), true)
		}()

		select {
		case <-submitEntered:
		case <-time.After(3 * time.Second):
			s.FailNow("SyncData should be called before checking the write buffer lock")
		}

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			_ = closeWB.MemorySize()
		}()

		select {
		case <-readDone:
		case <-time.After(3 * time.Second):
			s.FailNow("write buffer read lock should not be blocked by Close SyncData submit")
		}

		close(releaseSubmit)
		select {
		case <-closeDone:
		case <-time.After(3 * time.Second):
			s.FailNow("Close should finish after SyncData submit is released")
		}
	})

	s.Run("await_outside_lock", func() {
		mockAllocator := allocator.NewMockAllocator(s.T())
		var nextSegmentID atomic.Int64
		nextSegmentID.Store(1000)
		secondBufferProgress := make(chan struct{})
		var allocCallCount atomic.Int64
		var progressNotified atomic.Bool
		mockAllocator.EXPECT().AllocOne().RunAndReturn(func() (int64, error) {
			if allocCallCount.Add(1) == 2 && progressNotified.CompareAndSwap(false, true) {
				close(secondBufferProgress)
			}
			return nextSegmentID.Add(1), nil
		}).Times(2)

		l0wb, err := NewL0WriteBuffer(s.channelName, s.metacache, s.syncMgr, &writeBufferOption{
			idAllocator:  mockAllocator,
			syncPolicies: []SyncPolicy{},
		})
		s.Require().NoError(err)

		s.metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		l0Segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID:          1001,
			PartitionID: 10,
			Level:       datapb.SegmentLevel_L0,
		}, nil, nil)
		s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(l0Segment, true).Maybe()
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return().Maybe()

		syncStarted := make(chan struct{})
		releaseSync := make(chan struct{})
		var releaseSyncClosed atomic.Bool
		closeReleaseSync := func() {
			if releaseSyncClosed.CompareAndSwap(false, true) {
				close(releaseSync)
			}
		}
		defer closeReleaseSync()
		var syncStartedOnce atomic.Bool
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.MatchedBy(func(task syncmgr.Task) bool {
			return task != nil && task.SegmentID() == 1001
		}), mock.Anything).RunAndReturn(
			func(context.Context, syncmgr.Task, ...func(error) error) (*conc.Future[struct{}], error) {
				if syncStartedOnce.CompareAndSwap(false, true) {
					close(syncStarted)
				}
				return conc.Go[struct{}](func() (struct{}, error) {
					<-releaseSync
					return struct{}{}, nil
				}), nil
			},
		).Once()

		firstDeleteMsgs := []*msgstream.DeleteMsg{
			{
				DeleteRequest: &msgpb.DeleteRequest{
					PartitionID: 10,
					PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}}},
					Timestamps:  []uint64{100},
				},
			},
		}
		err = l0wb.BufferData(nil, firstDeleteMsgs, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 0)
		s.Require().NoError(err)

		evictDone := make(chan struct{})
		go func() {
			defer close(evictDone)
			l0wb.EvictBuffer(GetOldestBufferPolicy(1))
		}()

		select {
		case <-syncStarted:
		case <-time.After(3 * time.Second):
			s.FailNow("SyncData should start before buffering second batch")
		}

		secondDeleteMsgs := []*msgstream.DeleteMsg{
			{
				DeleteRequest: &msgpb.DeleteRequest{
					PartitionID: 12,
					PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{12}}}},
					Timestamps:  []uint64{300},
				},
			},
		}

		bufferDataDone := make(chan error, 1)
		go func() {
			bufferDataDone <- l0wb.BufferData(nil, secondDeleteMsgs, &msgpb.MsgPosition{Timestamp: 300}, &msgpb.MsgPosition{Timestamp: 350}, 0)
		}()

		select {
		case <-secondBufferProgress:
			// BufferData made progress under lock before sync release.
		case <-time.After(3 * time.Second):
			s.FailNow("BufferData should make progress before sync future is released")
		}

		closeReleaseSync()
		select {
		case err := <-bufferDataDone:
			s.NoError(err)
		case <-time.After(3 * time.Second):
			s.FailNow("BufferData should finish after sync is released")
		}
		select {
		case <-evictDone:
		case <-time.After(3 * time.Second):
			s.FailNow("EvictBuffer should finish after sync is released")
		}
	})
}

func (s *WriteBufferSuite) TestGrowingSourceProgressSelectedByPolicy() {
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.SyncPeriod.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.SyncPeriod.Key)
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.FlushInsertBufferSize.Key, "100")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.FlushInsertBufferSize.Key)
	originalEstSize := s.wb.estSizePerRecord
	s.wb.estSizePerRecord = 10
	defer func() {
		s.wb.estSizePerRecord = originalEstSize
	}()

	now := time.Now()
	recentTs := tsoutil.ComposeTSByTime(now.Add(500 * time.Millisecond))
	staleTs := tsoutil.ComposeTSByTime(now.Add(2 * time.Second))
	startTs := tsoutil.ComposeTSByTime(now)

	s.Run("pending_flush", func() {
		selected := s.wb.growingSourceProgressSelectedByPolicy(recentTs, 1001, &growingSourceProgress{
			segmentID:    1001,
			pendingFlush: true,
		})
		s.True(selected)
	})

	s.Run("non_retryable_failure", func() {
		selected := s.wb.growingSourceProgressSelectedByPolicy(recentTs, 1007, &growingSourceProgress{
			segmentID:           1007,
			pendingFlush:        true,
			nonRetryableFailure: true,
		})
		s.False(selected)
	})

	s.Run("sealed_segment", func() {
		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID:    1002,
			State: commonpb.SegmentState_Sealed,
		}, nil, nil)
		s.metacache.EXPECT().GetSegmentByID(int64(1002)).Return(segment, true).Once()

		selected := s.wb.growingSourceProgressSelectedByPolicy(recentTs, 1002, &growingSourceProgress{
			segmentID: 1002,
		})
		s.True(selected)
	})

	s.Run("recent_progress", func() {
		s.metacache.EXPECT().GetSegmentByID(int64(1003)).Return(nil, false).Once()

		selected := s.wb.growingSourceProgressSelectedByPolicy(recentTs, 1003, &growingSourceProgress{
			segmentID: 1003,
			batches: []growingSourceProgressBatch{
				{startPosition: &msgpb.MsgPosition{Timestamp: startTs}},
			},
		})
		s.False(selected)
	})

	s.Run("below_row_threshold", func() {
		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1004,
		}, nil, nil)
		s.metacache.EXPECT().GetSegmentByID(int64(1004)).Return(segment, true).Once()

		selected := s.wb.growingSourceProgressSelectedByPolicy(recentTs, 1004, &growingSourceProgress{
			segmentID:    1004,
			targetOffset: 9,
			batches: []growingSourceProgressBatch{
				{startPosition: &msgpb.MsgPosition{Timestamp: startTs}},
			},
		})
		s.False(selected)
	})

	s.Run("row_threshold", func() {
		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1005,
		}, nil, nil)
		s.metacache.EXPECT().GetSegmentByID(int64(1005)).Return(segment, true).Once()

		selected := s.wb.growingSourceProgressSelectedByPolicy(recentTs, 1005, &growingSourceProgress{
			segmentID:    1005,
			targetOffset: 10,
		})
		s.True(selected)
	})

	s.Run("stale_progress", func() {
		s.metacache.EXPECT().GetSegmentByID(int64(1006)).Return(nil, false).Once()

		selected := s.wb.growingSourceProgressSelectedByPolicy(staleTs, 1006, &growingSourceProgress{
			segmentID: 1006,
			batches: []growingSourceProgressBatch{
				{startPosition: &msgpb.MsgPosition{Timestamp: startTs}},
			},
		})
		s.True(selected)
	})
}

func (s *WriteBufferSuite) TestGrowingSourceLayoutMismatch() {
	s.True(isGrowingSourceLayoutMismatch(errors.New("flush growing source data: Invalid: Column count mismatch at index 0: existing has 21 columns, but appended has 44 columns: segcore error[segcoreCode=2001]")))
	s.True(isGrowingSourceLayoutMismatch(errors.New("flush growing source data: Invalid: Column group size mismatch: existing has 10 groups, but appended has 1 groups: segcore error[segcoreCode=2001]")))
	s.False(isGrowingSourceLayoutMismatch(errors.New("flush growing source data: mock transient error")))
	s.False(isGrowingSourceLayoutMismatch(nil))
}

func (s *WriteBufferSuite) TestGrowingSourceProgressSyncableSkipsNonRetryableFailure() {
	syncable, retry := s.wb.growingSourceProgressSyncable(1001, &growingSourceProgress{
		segmentID:           1001,
		nonRetryableFailure: true,
		batches: []growingSourceProgressBatch{
			{endPosition: &msgpb.MsgPosition{Timestamp: 100}, endOffset: 10},
		},
	}, false, true)
	s.False(syncable)
	s.False(retry)
}

func (s *WriteBufferSuite) TestGrowingSourceProgressRetrySkipsNonRetryableFailure() {
	s.wb.growingSourceProgress[1001] = &growingSourceProgress{
		segmentID:           1001,
		nonRetryableFailure: true,
		batches: []growingSourceProgressBatch{
			{endPosition: &msgpb.MsgPosition{Timestamp: 100}, endOffset: 10},
		},
	}

	segments, retry := s.wb.getGrowingSourceSegmentsToRetry()
	s.Empty(segments)
	s.False(retry)
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

func TestPrepareInsertMaterializesLegacyBM25Output(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{
		Name: "bm25_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "1024"},
				},
			},
			{
				FieldID:          102,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			},
			{
				Name:          "rerank",
				Type:          schemapb.FunctionType_Rerank,
				InputFieldIds: []int64{101},
			},
		},
	}
	pkField := collSchema.GetFields()[2]
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 1000,
			EndTimestamp:   1002,
		},
		InsertRequest: &msgpb.InsertRequest{
			SegmentID:    1,
			PartitionID:  1,
			CollectionID: 1,
			NumRows:      3,
			Version:      msgpb.InsertDataVersion_ColumnBased,
			RowIDs:       []int64{10, 11, 12},
			Timestamps:   []uint64{1000, 1001, 1002},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId: 100,
					Type:    schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
							},
						},
					},
				},
				{
					FieldId: 101,
					Type:    schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{"hello world", "milvus bm25", "legacy message"}},
							},
						},
					},
				},
			},
		},
	}

	assert.NoError(t, function.GetManager().Alloc(1, "v1", collSchema))
	_, err := function.GetManager().Materialize(context.Background(), 1, "v1", collSchema.GetVersion(), insertMsg.InsertRequest)
	assert.NoError(t, err)
	defer function.GetManager().Release(1, "v1")

	result, err := PrepareInsert(collSchema, pkField, []*msgstream.InsertMsg{insertMsg})

	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Contains(t, result[0].bm25Stats, int64(102))
	assert.Equal(t, int64(3), result[0].bm25Stats[102].NumRow())
	assert.NotNil(t, insertMsg.GetFieldsData()[2])
	assert.Equal(t, int64(102), insertMsg.GetFieldsData()[2].GetFieldId())
}
