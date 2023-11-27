package syncmgr

import (
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type SyncTaskSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	segmentID    int64
	channelName  string

	metacache    *metacache.MockMetaCache
	allocator    *allocator.MockGIDAllocator
	schema       *schemapb.CollectionSchema
	chunkManager *mocks.ChunkManager
	broker       *broker.MockBroker
}

func (s *SyncTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())

	s.collectionID = 100
	s.partitionID = 101
	s.segmentID = 1001
	s.channelName = "by-dev-rootcoord-dml_0_100v0"

	s.schema = &schemapb.CollectionSchema{
		Name: "sync_task_test_col",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
}

func (s *SyncTaskSuite) SetupTest() {
	s.allocator = allocator.NewMockGIDAllocator()
	s.allocator.AllocF = func(count uint32) (int64, int64, error) {
		return time.Now().Unix(), int64(count), nil
	}
	s.allocator.AllocOneF = func() (allocator.UniqueID, error) {
		return time.Now().Unix(), nil
	}

	s.chunkManager = mocks.NewChunkManager(s.T())
	s.chunkManager.EXPECT().RootPath().Return("files").Maybe()
	s.chunkManager.EXPECT().MultiWrite(mock.Anything, mock.Anything).Return(nil).Maybe()

	s.broker = broker.NewMockBroker(s.T())
	s.metacache = metacache.NewMockMetaCache(s.T())
}

func (s *SyncTaskSuite) getEmptyInsertBuffer() *storage.InsertData {
	buf, err := storage.NewInsertData(s.schema)
	s.Require().NoError(err)

	return buf
}

func (s *SyncTaskSuite) getInsertBuffer() *storage.InsertData {
	buf := s.getEmptyInsertBuffer()

	// generate data
	for i := 0; i < 10; i++ {
		data := make(map[storage.FieldID]any)
		data[common.RowIDField] = int64(i + 1)
		data[common.TimeStampField] = int64(i + 1)
		data[100] = int64(i + 1)
		vector := lo.RepeatBy(128, func(_ int) float32 {
			return rand.Float32()
		})
		data[101] = vector
		err := buf.Append(data)
		s.Require().NoError(err)
	}
	return buf
}

func (s *SyncTaskSuite) getDeleteBuffer() *storage.DeleteData {
	buf := &storage.DeleteData{}
	for i := 0; i < 10; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i + 1))
		ts := tsoutil.ComposeTSByTime(time.Now(), 0)
		buf.Append(pk, ts)
	}
	return buf
}

func (s *SyncTaskSuite) getDeleteBufferZeroTs() *storage.DeleteData {
	buf := &storage.DeleteData{}
	for i := 0; i < 10; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i + 1))
		buf.Append(pk, 0)
	}
	return buf
}

func (s *SyncTaskSuite) getSuiteSyncTask() *SyncTask {
	task := NewSyncTask().WithCollectionID(s.collectionID).
		WithPartitionID(s.partitionID).
		WithSegmentID(s.segmentID).
		WithChannelName(s.channelName).
		WithSchema(s.schema).
		WithChunkManager(s.chunkManager).
		WithAllocator(s.allocator).
		WithMetaCache(s.metacache)

	return task
}

func (s *SyncTaskSuite) TestRunNormal() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)
	bfs := metacache.NewBloomFilterSet()
	fd, err := storage.NewFieldData(schemapb.DataType_Int64, &schemapb.FieldSchema{
		FieldID:      101,
		Name:         "ID",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	})
	s.Require().NoError(err)

	ids := []int64{1, 2, 3, 4, 5, 6, 7}
	for _, id := range ids {
		err = fd.AppendRow(id)
		s.Require().NoError(err)
	}

	bfs.UpdatePKRange(fd)
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	metacache.UpdateNumOfRows(1000)(seg)
	seg.GetBloomFilterSet().Roll()
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

	s.Run("without_insert_delete", func() {
		task := s.getSuiteSyncTask()
		task.WithMetaWriter(BrokerMetaWriter(s.broker))
		task.WithTimeRange(50, 100)
		task.WithCheckpoint(&msgpb.MsgPosition{
			ChannelName: s.channelName,
			MsgID:       []byte{1, 2, 3, 4},
			Timestamp:   100,
		})

		err := task.Run()
		s.NoError(err)
	})

	s.Run("with_insert_delete_cp", func() {
		task := s.getSuiteSyncTask()
		task.WithInsertData(s.getInsertBuffer()).WithDeleteData(s.getDeleteBuffer())
		task.WithTimeRange(50, 100)
		task.WithMetaWriter(BrokerMetaWriter(s.broker))
		task.WithCheckpoint(&msgpb.MsgPosition{
			ChannelName: s.channelName,
			MsgID:       []byte{1, 2, 3, 4},
			Timestamp:   100,
		})

		err := task.Run()
		s.NoError(err)
	})

	s.Run("with_insert_delete_flush", func() {
		task := s.getSuiteSyncTask()
		task.WithInsertData(s.getInsertBuffer()).WithDeleteData(s.getDeleteBuffer())
		task.WithFlush()
		task.WithDrop()
		task.WithMetaWriter(BrokerMetaWriter(s.broker))
		task.WithCheckpoint(&msgpb.MsgPosition{
			ChannelName: s.channelName,
			MsgID:       []byte{1, 2, 3, 4},
			Timestamp:   100,
		})

		err := task.Run()
		s.NoError(err)
	})

	s.Run("with_zero_numrow_insertdata", func() {
		task := s.getSuiteSyncTask()
		task.WithInsertData(s.getEmptyInsertBuffer())
		task.WithFlush()
		task.WithDrop()
		task.WithMetaWriter(BrokerMetaWriter(s.broker))
		task.WithCheckpoint(&msgpb.MsgPosition{
			ChannelName: s.channelName,
			MsgID:       []byte{1, 2, 3, 4},
			Timestamp:   100,
		})

		err := task.Run()
		s.Error(err)

		err = task.serializePkStatsLog()
		s.NoError(err)
		stats, rowNum := task.convertInsertData2PkStats(100, schemapb.DataType_Int64)
		s.Nil(stats)
		s.Zero(rowNum)
	})
}

func (s *SyncTaskSuite) TestRunL0Segment() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)
	bfs := metacache.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{Level: datapb.SegmentLevel_L0}, bfs)
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

	s.Run("pure_delete_l0_flush", func() {
		task := s.getSuiteSyncTask()
		task.WithDeleteData(s.getDeleteBuffer())
		task.WithTimeRange(50, 100)
		task.WithMetaWriter(BrokerMetaWriter(s.broker))
		task.WithCheckpoint(&msgpb.MsgPosition{
			ChannelName: s.channelName,
			MsgID:       []byte{1, 2, 3, 4},
			Timestamp:   100,
		})
		task.WithFlush()

		err := task.Run()
		s.NoError(err)
	})
}

func (s *SyncTaskSuite) TestCompactToNull() {
	bfs := metacache.NewBloomFilterSet()
	fd, err := storage.NewFieldData(schemapb.DataType_Int64, &schemapb.FieldSchema{
		FieldID:      101,
		Name:         "ID",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	})
	s.Require().NoError(err)

	ids := []int64{1, 2, 3, 4, 5, 6, 7}
	for _, id := range ids {
		err = fd.AppendRow(id)
		s.Require().NoError(err)
	}

	bfs.UpdatePKRange(fd)
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs)
	metacache.UpdateNumOfRows(1000)(seg)
	metacache.CompactTo(metacache.NullSegment)(seg)
	seg.GetBloomFilterSet().Roll()
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)

	task := s.getSuiteSyncTask()
	task.WithMetaWriter(BrokerMetaWriter(s.broker))
	task.WithTimeRange(50, 100)
	task.WithCheckpoint(&msgpb.MsgPosition{
		ChannelName: s.channelName,
		MsgID:       []byte{1, 2, 3, 4},
		Timestamp:   100,
	})

	err = task.Run()
	s.NoError(err)
}

func (s *SyncTaskSuite) TestRunError() {
	s.Run("segment_not_found", func() {
		s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(nil, false)
		flag := false
		handler := func(_ error) { flag = true }
		task := s.getSuiteSyncTask().WithFailureCallback(handler)
		task.WithInsertData(s.getEmptyInsertBuffer())

		err := task.Run()

		s.Error(err)
		s.True(flag)
	})

	s.metacache.ExpectedCalls = nil
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, metacache.NewBloomFilterSet())
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
	s.Run("serialize_insert_fail", func() {
		flag := false
		handler := func(_ error) { flag = true }
		task := s.getSuiteSyncTask().WithFailureCallback(handler)
		task.WithInsertData(s.getEmptyInsertBuffer())

		err := task.Run()

		s.Error(err)
		s.True(flag)
	})

	s.Run("serailize_delete_fail", func() {
		flag := false
		handler := func(_ error) { flag = true }
		task := s.getSuiteSyncTask().WithFailureCallback(handler)

		task.WithDeleteData(s.getDeleteBufferZeroTs())

		err := task.Run()

		s.Error(err)
		s.True(flag)
	})

	s.Run("chunk_manager_save_fail", func() {
		flag := false
		handler := func(_ error) { flag = true }
		s.chunkManager.ExpectedCalls = nil
		s.chunkManager.EXPECT().RootPath().Return("files")
		s.chunkManager.EXPECT().MultiWrite(mock.Anything, mock.Anything).Return(errors.New("mocked"))
		task := s.getSuiteSyncTask().WithFailureCallback(handler)

		task.WithInsertData(s.getInsertBuffer()).WithDeleteData(s.getDeleteBuffer())
		task.WithWriteRetryOptions(retry.Attempts(1))

		err := task.Run()

		s.Error(err)
		s.True(flag)
	})
}

func TestSyncTask(t *testing.T) {
	suite.Run(t, new(SyncTaskSuite))
}
