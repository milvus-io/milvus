package syncmgr

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type SyncManagerSuite struct {
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

func (s *SyncManagerSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))

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

func (s *SyncManagerSuite) SetupTest() {
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

func (s *SyncManagerSuite) getEmptyInsertBuffer() *storage.InsertData {
	buf, err := storage.NewInsertData(s.schema)
	s.Require().NoError(err)

	return buf
}

func (s *SyncManagerSuite) getInsertBuffer() *storage.InsertData {
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

func (s *SyncManagerSuite) getDeleteBuffer() *storage.DeleteData {
	buf := &storage.DeleteData{}
	for i := 0; i < 10; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i + 1))
		ts := tsoutil.ComposeTSByTime(time.Now(), 0)
		buf.Append(pk, ts)
	}
	return buf
}

func (s *SyncManagerSuite) getDeleteBufferZeroTs() *storage.DeleteData {
	buf := &storage.DeleteData{}
	for i := 0; i < 10; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i + 1))
		buf.Append(pk, 0)
	}
	return buf
}

func (s *SyncManagerSuite) getSuiteSyncTask() *SyncTask {
	task := NewSyncTask().WithCollectionID(s.collectionID).
		WithPartitionID(s.partitionID).
		WithSegmentID(s.segmentID).
		WithChannelName(s.channelName).
		WithSchema(s.schema).
		WithChunkManager(s.chunkManager).
		WithAllocator(s.allocator).
		WithMetaCache(s.metacache).
		WithAllocator(s.allocator)

	return task
}

func (s *SyncManagerSuite) TestSubmit() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)
	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

	manager := NewSyncManager(s.chunkManager)
	task := s.getSuiteSyncTask()
	task.WithMetaWriter(BrokerMetaWriter(s.broker, 1))
	task.WithTimeRange(50, 100)
	task.WithCheckpoint(&msgpb.MsgPosition{
		ChannelName: s.channelName,
		MsgID:       []byte{1, 2, 3, 4},
		Timestamp:   100,
	})

	f, err := manager.SyncData(context.Background(), task)
	s.NoError(err)
	s.NotNil(f)

	_, err = f.Await()
	s.NoError(err)
}

func (s *SyncManagerSuite) TestClose() {
	manager := NewSyncManager(s.chunkManager)
	err := manager.Close()
	s.NoError(err)

	f, err := manager.SyncData(context.Background(), nil)
	s.Error(err)
	s.Nil(f)
}

func (s *SyncManagerSuite) TestCompacted() {
	var segmentID atomic.Int64
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Run(func(_ context.Context, req *datapb.SaveBinlogPathsRequest) {
		segmentID.Store(req.GetSegmentID())
	}).Return(nil)
	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

	manager := NewSyncManager(s.chunkManager)
	task := s.getSuiteSyncTask()
	task.WithMetaWriter(BrokerMetaWriter(s.broker, 1))
	task.WithTimeRange(50, 100)
	task.WithCheckpoint(&msgpb.MsgPosition{
		ChannelName: s.channelName,
		MsgID:       []byte{1, 2, 3, 4},
		Timestamp:   100,
	})

	f, err := manager.SyncData(context.Background(), task)
	s.NoError(err)
	s.NotNil(f)

	_, err = f.Await()
	s.NoError(err)
	s.EqualValues(1001, segmentID.Load())
}

func (s *SyncManagerSuite) TestResizePool() {
	manager := NewSyncManager(s.chunkManager)

	syncMgr, ok := manager.(*syncManager)
	s.Require().True(ok)

	cap := syncMgr.keyLockDispatcher.workerPool.Cap()
	s.NotZero(cap)

	params := paramtable.Get()
	configKey := params.DataNodeCfg.MaxParallelSyncMgrTasks.Key

	syncMgr.resizeHandler(&config.Event{
		Key:        configKey,
		Value:      "abc",
		HasUpdated: true,
	})

	s.Equal(cap, syncMgr.keyLockDispatcher.workerPool.Cap())

	syncMgr.resizeHandler(&config.Event{
		Key:        configKey,
		Value:      "-1",
		HasUpdated: true,
	})
	s.Equal(cap, syncMgr.keyLockDispatcher.workerPool.Cap())

	syncMgr.resizeHandler(&config.Event{
		Key:        configKey,
		Value:      strconv.FormatInt(int64(cap*2), 10),
		HasUpdated: true,
	})
	s.Equal(cap*2, syncMgr.keyLockDispatcher.workerPool.Cap())
}

func (s *SyncManagerSuite) TestUnexpectedError() {
	manager := NewSyncManager(s.chunkManager)

	task := NewMockTask(s.T())
	task.EXPECT().SegmentID().Return(1000)
	task.EXPECT().Checkpoint().Return(&msgpb.MsgPosition{})
	task.EXPECT().Run(mock.Anything).Return(merr.WrapErrServiceInternal("mocked")).Once()
	task.EXPECT().HandleError(mock.Anything)

	f, _ := manager.SyncData(context.Background(), task)
	_, err := f.Await()
	s.Error(err)
}

func (s *SyncManagerSuite) TestTargetUpdateSameID() {
	manager := NewSyncManager(s.chunkManager)

	task := NewMockTask(s.T())
	task.EXPECT().SegmentID().Return(1000)
	task.EXPECT().Checkpoint().Return(&msgpb.MsgPosition{})
	task.EXPECT().Run(mock.Anything).Return(errors.New("mock err")).Once()
	task.EXPECT().HandleError(mock.Anything)

	f, _ := manager.SyncData(context.Background(), task)
	_, err := f.Await()
	s.Error(err)
}

func (s *SyncManagerSuite) TestSyncManager_TaskStatsJSON() {
	manager := NewSyncManager(s.chunkManager)
	syncMgr, ok := manager.(*syncManager)
	assert.True(s.T(), ok)

	task1 := &SyncTask{
		segmentID:    12345,
		collectionID: 1,
		partitionID:  1,
		channelName:  "channel1",
		schema:       &schemapb.CollectionSchema{},
		checkpoint:   &msgpb.MsgPosition{},
		tsFrom:       1000,
		tsTo:         2000,
	}

	task2 := &SyncTask{
		segmentID:    67890,
		collectionID: 2,
		partitionID:  2,
		channelName:  "channel2",
		schema:       &schemapb.CollectionSchema{},
		checkpoint:   &msgpb.MsgPosition{},
		tsFrom:       3000,
		tsTo:         4000,
	}

	syncMgr.taskStats.Add("12345-1000", task1)
	syncMgr.taskStats.Add("67890-3000", task2)

	expectedTasks := []SyncTask{*task1, *task2}
	expectedJSON, err := json.Marshal(expectedTasks)
	s.NoError(err)

	actualJSON := syncMgr.TaskStatsJSON()
	s.JSONEq(string(expectedJSON), actualJSON)
}

func TestSyncManager(t *testing.T) {
	suite.Run(t, new(SyncManagerSuite))
}
