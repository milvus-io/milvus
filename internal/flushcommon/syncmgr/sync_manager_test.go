package syncmgr

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func (s *SyncManagerSuite) getSuiteSyncTask() *SyncTask {
	task := NewSyncTask().
		WithSyncPack(new(SyncPack).
			WithCollectionID(s.collectionID).
			WithPartitionID(s.partitionID).
			WithSegmentID(s.segmentID).
			WithChannelName(s.channelName)).
		WithAllocator(s.allocator).
		WithChunkManager(s.chunkManager).
		WithMetaCache(s.metacache).
		WithAllocator(s.allocator)

	return task
}

func (s *SyncManagerSuite) TestSubmit() {
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil)
	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil, metacache.NewEmptySegmentStats())
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

	manager := NewSyncManager(s.chunkManager)
	task := s.getSuiteSyncTask()
	task.WithMetaWriter(BrokerMetaWriter(s.broker, 1))

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
	s.ErrorIs(err, context.Canceled)
	s.Nil(f)
}

func (s *SyncManagerSuite) TestCloseCancellationDoesNotInvokeHandleError() {
	manager := NewSyncManager(s.chunkManager)
	started := make(chan struct{})
	var handleErrors atomic.Int32
	task := &reorderTestTask{
		segmentID: s.segmentID,
		prepare: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
		commit: func(context.Context) error {
			s.T().Error("canceled task must not reach Commit")
			return nil
		},
		handleErr: func(error) {
			handleErrors.Add(1)
		},
	}

	future, err := manager.SyncData(context.Background(), task)
	s.NoError(err)
	s.NotNil(future)
	select {
	case <-started:
	case <-time.After(time.Second):
		s.T().Fatal("sync task did not start Prepare")
	}

	s.NoError(manager.Close())
	_, err = future.Await()
	s.ErrorIs(err, context.Canceled)
	s.Zero(handleErrors.Load(), "graceful shutdown cancellation must not be reported as a task failure")
	s.Zero(manager.(*syncManager).tasks.Len())
}

func (s *SyncManagerSuite) TestReservedAdmissionDoesNotSecondAcquireAndDefersRelease() {
	manager := NewSyncManager(s.chunkManager)
	syncMgr := manager.(*syncManager)
	syncMgr.admission.SetCapacity(1)
	nodeID := paramtable.GetStringNodeID()
	pendingBefore := testutil.ToFloat64(metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID))
	totalBefore := testutil.ToFloat64(metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(nodeID))

	admission, err := syncMgr.ReserveSyncTask(context.Background())
	s.Require().NoError(err)
	s.Equal(1, syncMgr.admission.Current())
	s.Equal(pendingBefore, testutil.ToFloat64(metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID)))
	s.Equal(totalBefore, testutil.ToFloat64(metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(nodeID)))

	releasePrepare := make(chan struct{})
	task := &reorderTestTask{
		segmentID: s.segmentID,
		prepare: func(ctx context.Context) error {
			select {
			case <-releasePrepare:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		commit: func(context.Context) error { return nil },
	}
	type submitResult struct {
		future *conc.Future[struct{}]
		err    error
	}
	submitted := make(chan submitResult, 1)
	go func() {
		future, err := admission.Submit(context.Background(), task)
		submitted <- submitResult{future: future, err: err}
	}()

	var result submitResult
	select {
	case result = <-submitted:
		s.NoError(result.err)
		s.NotNil(result.future)
	case <-time.After(time.Second):
		s.FailNow("reserved submission attempted to acquire a second admission slot")
	}
	s.Equal(1, syncMgr.admission.Current())
	s.Equal(pendingBefore+1, testutil.ToFloat64(metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID)))
	s.Equal(totalBefore+1, testutil.ToFloat64(metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(nodeID)))
	admission.Release()
	admission.Release()
	s.Equal(1, syncMgr.admission.Current(), "Release must wait until the in-flight attempt finishes callbacks")

	close(releasePrepare)
	_, err = result.future.Await()
	s.NoError(err)
	s.Eventually(func() bool { return syncMgr.admission.Current() == 0 }, time.Second, time.Millisecond)
	s.Equal(pendingBefore, testutil.ToFloat64(metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID)))
	s.NoError(manager.Close())
}

func (s *SyncManagerSuite) TestReservedAdmissionIsReusableAcrossAttempts() {
	manager := NewSyncManager(s.chunkManager)
	syncMgr := manager.(*syncManager)
	syncMgr.admission.SetCapacity(1)

	admission, err := syncMgr.ReserveSyncTask(context.Background())
	s.Require().NoError(err)
	releaseFirst := make(chan struct{})
	first, err := admission.Submit(context.Background(), &reorderTestTask{
		segmentID: s.segmentID,
		prepare: func(context.Context) error {
			<-releaseFirst
			return nil
		},
		commit: func(context.Context) error { return nil },
	})
	s.Require().NoError(err)

	concurrent, err := admission.Submit(context.Background(), &reorderTestTask{
		segmentID: s.segmentID + 1,
		prepare:   func(context.Context) error { return nil },
		commit:    func(context.Context) error { return nil },
	})
	s.Error(err)
	s.Nil(concurrent)
	close(releaseFirst)
	_, err = first.Await()
	s.NoError(err)
	s.Equal(1, syncMgr.admission.Current(), "completed attempt must retain the payload lease")

	second, err := admission.Submit(context.Background(), &reorderTestTask{
		segmentID: s.segmentID,
		prepare:   func(context.Context) error { return nil },
		commit:    func(context.Context) error { return nil },
	})
	s.Require().NoError(err)
	_, err = second.Await()
	s.NoError(err)
	s.Equal(1, syncMgr.admission.Current())
	admission.Release()
	s.Zero(syncMgr.admission.Current())
	s.NoError(manager.Close())
}

func (s *SyncManagerSuite) TestUnusedReservedAdmissionReleaseIsIdempotent() {
	manager := NewSyncManager(s.chunkManager)
	syncMgr := manager.(*syncManager)
	syncMgr.admission.SetCapacity(1)

	admission, err := syncMgr.ReserveSyncTask(context.Background())
	s.Require().NoError(err)
	s.Equal(1, syncMgr.admission.Current())
	admission.Release()
	s.Zero(syncMgr.admission.Current())
	s.NotPanics(admission.Release)
	s.Zero(syncMgr.admission.Current())

	future, err := admission.Submit(context.Background(), &reorderTestTask{
		segmentID: s.segmentID,
		prepare:   func(context.Context) error { return nil },
		commit:    func(context.Context) error { return nil },
	})
	s.Error(err)
	s.Nil(future)
	s.Zero(syncMgr.Pending())
	s.Zero(syncMgr.admission.Current())
	s.NoError(manager.Close())
}

func (s *SyncManagerSuite) TestReservedAdmissionRejectedAfterCloseReleasesSlot() {
	manager := NewSyncManager(s.chunkManager)
	syncMgr := manager.(*syncManager)
	syncMgr.admission.SetCapacity(1)

	admission, err := syncMgr.ReserveSyncTask(context.Background())
	s.Require().NoError(err)
	s.Equal(1, syncMgr.admission.Current())
	s.NoError(manager.Close())

	future, err := admission.Submit(context.Background(), &reorderTestTask{
		segmentID: s.segmentID,
		prepare:   func(context.Context) error { return nil },
		commit:    func(context.Context) error { return nil },
	})
	s.ErrorIs(err, context.Canceled)
	s.Nil(future)
	s.Zero(syncMgr.Pending())
	s.Equal(1, syncMgr.admission.Current(), "pre-accept rejection leaves the payload lease with its owner")
	s.NotPanics(func() {
		admission.Release()
		admission.Release()
	})
	s.Zero(syncMgr.admission.Current())
}

func (s *SyncManagerSuite) TestReserveAdmissionAfterCloseAlwaysRejectsWithoutLeak() {
	manager := NewSyncManager(s.chunkManager)
	syncMgr := manager.(*syncManager)
	s.NoError(manager.Close())

	for range 100 {
		admission, err := syncMgr.ReserveSyncTask(context.Background())
		s.ErrorIs(err, context.Canceled)
		s.Nil(admission)
		s.Zero(syncMgr.admission.Current())
	}
}

func (s *SyncManagerSuite) TestCompacted() {
	var segmentID atomic.Int64
	s.broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Run(func(_ context.Context, req *datapb.SaveBinlogPathsRequest) {
		segmentID.Store(req.GetSegmentID())
	}).Return(nil)
	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil, metacache.NewEmptySegmentStats())
	metacache.UpdateNumOfRows(1000)(seg)
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true)
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

	manager := NewSyncManager(s.chunkManager)
	task := s.getSuiteSyncTask()
	task.WithMetaWriter(BrokerMetaWriter(s.broker, 1))

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

	cap := syncMgr.preparePool.Cap()
	s.NotZero(cap)
	admissionCap := syncMgr.admission.Cap()
	s.Equal(dispatcherAdmissionCapacity(cap), admissionCap)

	params := paramtable.Get()
	configKey := params.DataNodeCfg.MaxParallelSyncMgrTasksPerCPUCore.Key
	oldValue := params.DataNodeCfg.MaxParallelSyncMgrTasksPerCPUCore.GetAsInt()

	syncMgr.resizeHandler(&config.Event{
		Key:        configKey,
		Value:      "abc",
		HasUpdated: true,
	})

	s.Equal(cap, syncMgr.preparePool.Cap())
	s.Equal(admissionCap, syncMgr.admission.Cap())

	syncMgr.resizeHandler(&config.Event{
		Key:        configKey,
		Value:      "-1",
		HasUpdated: true,
	})
	s.Equal(cap, syncMgr.preparePool.Cap())
	s.Equal(admissionCap, syncMgr.admission.Cap())

	syncMgr.resizeHandler(&config.Event{
		Key:        configKey,
		Value:      strconv.FormatInt(int64(oldValue*2), 10),
		HasUpdated: true,
	})
	s.Equal(cap*2, syncMgr.preparePool.Cap())
	s.Equal(dispatcherAdmissionCapacity(cap*2), syncMgr.admission.Cap())
}

func (s *SyncManagerSuite) TestUnexpectedError() {
	manager := NewSyncManager(s.chunkManager)

	task := NewMockTask(s.T())
	task.EXPECT().SegmentID().Return(1000)
	task.EXPECT().Checkpoint().Return(&msgpb.MsgPosition{})
	task.EXPECT().Prepare(mock.Anything).Return(merr.WrapErrServiceInternal("mocked")).Once()
	task.EXPECT().HandleError(mock.Anything)
	// SyncData injects the chunk manager through the Task interface now, rather
	// than type-switching to the two concrete task types.
	task.EXPECT().SetChunkManager(mock.Anything).Return()

	f, _ := manager.SyncData(context.Background(), task)
	_, err := f.Await()
	s.Error(err)
}

func (s *SyncManagerSuite) TestTargetUpdateSameID() {
	manager := NewSyncManager(s.chunkManager)

	task := NewMockTask(s.T())
	task.EXPECT().SegmentID().Return(1000)
	task.EXPECT().Checkpoint().Return(&msgpb.MsgPosition{})
	task.EXPECT().Prepare(mock.Anything).Return(errors.New("mock err")).Once()
	task.EXPECT().HandleError(mock.Anything)
	task.EXPECT().SetChunkManager(mock.Anything).Return()

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
		checkpoint:   &msgpb.MsgPosition{},
		tsFrom:       1000,
		tsTo:         2000,
	}

	task2 := &SyncTask{
		segmentID:    67890,
		collectionID: 2,
		partitionID:  2,
		channelName:  "channel2",
		checkpoint:   &msgpb.MsgPosition{},
		tsFrom:       3000,
		tsTo:         4000,
	}

	syncMgr.taskStats.Add("12345-1000", task1)
	syncMgr.taskStats.Add("67890-3000", task2)

	expectedTasks := []*SyncTask{task1, task2}
	expectedJSON, err := json.Marshal(expectedTasks)
	s.NoError(err)

	actualJSON := syncMgr.TaskStatsJSON()
	s.JSONEq(string(expectedJSON), actualJSON)
}

func TestSyncManager(t *testing.T) {
	suite.Run(t, new(SyncManagerSuite))
}
