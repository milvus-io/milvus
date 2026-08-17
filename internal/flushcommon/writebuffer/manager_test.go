package writebuffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type ManagerSuite struct {
	suite.Suite
	collID      int64
	channelName string
	collSchema  *schemapb.CollectionSchema
	syncMgr     *syncmgr.MockSyncManager
	metacache   *metacache.MockMetaCache
	allocator   *allocator.MockAllocator

	manager *bufferManager
}

func (s *ManagerSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	s.collID = 100
	s.collSchema = testCollectionSchema()

	s.channelName = "by-dev-rootcoord-dml_0_100_v0"
}

func (s *ManagerSuite) SetupTest() {
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
	s.metacache = newMockMetaCacheForTest(s.T(), s.collSchema, s.collID)
	s.allocator = allocator.NewMockAllocator(s.T())

	mgr := NewManager(s.syncMgr)
	var ok bool
	s.manager, ok = mgr.(*bufferManager)
	s.Require().True(ok)
}

// setMemoryCheckParams stages the three memory-check knobs for the duration of
// the test.
func (s *ManagerSuite) setMemoryCheckParams(interval, forceSyncEnable, watermark string) {
	param := paramtable.Get()
	saveParamForTest(s.T(), param.DataNodeCfg.MemoryCheckInterval.Key, interval)
	saveParamForTest(s.T(), param.DataNodeCfg.MemoryForceSyncEnable.Key, forceSyncEnable)
	saveParamForTest(s.T(), param.DataNodeCfg.MemoryForceSyncWatermark.Key, watermark)
}

func (s *ManagerSuite) TestRegister() {
	manager := s.manager

	err := manager.Register(context.Background(), s.channelName, s.metacache, WithIDAllocator(s.allocator))
	s.NoError(err)

	err = manager.Register(context.Background(), s.channelName, s.metacache, WithIDAllocator(s.allocator))
	s.Error(err)
	s.ErrorIs(err, merr.ErrChannelReduplicate)
}

// TestAllowGrowingSourceFlush pins the found/enabled distinction: a channel
// absent from the manager's map must be reported found=false, never conflated
// with "present but growing-source disabled". DropChannel/RemoveChannel detach
// the buffer BEFORE the long final Close, so during that window the buffer may
// still be alive and owed — a caller that read absence as "not enabled" would
// skip the release guards exactly when they matter.
func (s *ManagerSuite) TestAllowGrowingSourceFlush() {
	// Absent channel.
	enabled, found := s.manager.AllowGrowingSourceFlush(s.channelName)
	s.False(enabled)
	s.False(found)

	// Present but feature disabled.
	wb := NewMockWriteBuffer(s.T())
	wb.EXPECT().AllowGrowingSourceFlush().Return(false).Once()
	s.manager.buffers.Insert(s.channelName, wb)
	enabled, found = s.manager.AllowGrowingSourceFlush(s.channelName)
	s.False(enabled)
	s.True(found)

	// Present and enabled.
	wb.EXPECT().AllowGrowingSourceFlush().Return(true).Once()
	enabled, found = s.manager.AllowGrowingSourceFlush(s.channelName)
	s.True(enabled)
	s.True(found)
}

func (s *ManagerSuite) TestDropChannelPropagatesBoundedContext() {
	saveParamForTest(s.T(), paramtable.Get().DataNodeCfg.GracefulStopTimeout.Key, "2")

	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "caller")
	wb := NewMockWriteBuffer(s.T())
	wb.EXPECT().Close(mock.MatchedBy(func(dropCtx context.Context) bool {
		deadline, ok := dropCtx.Deadline()
		remaining := time.Until(deadline)
		return ok && dropCtx.Value(contextKey{}) == "caller" && remaining > 0 && remaining <= 2*time.Second
	}), true).Return().Once()
	s.manager.buffers.Insert(s.channelName, wb)

	s.manager.DropChannel(ctx, s.channelName)

	_, loaded := s.manager.buffers.Get(s.channelName)
	s.False(loaded)
}

func (s *ManagerSuite) TestFlushSegments() {
	manager := s.manager
	s.Run("channel_not_found", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := manager.SealSegments(ctx, s.channelName, []int64{1, 2, 3})
		s.Error(err, "FlushSegments shall return error when channel not found")
	})

	s.Run("normal_flush", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wb := NewMockWriteBuffer(s.T())
		s.manager.buffers.Insert(s.channelName, wb)

		wb.EXPECT().SealSegments(mock.Anything, mock.Anything).Return(nil)

		err := manager.SealSegments(ctx, s.channelName, []int64{1})
		s.NoError(err)
	})

	s.Run("seal all segments", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wb := NewMockWriteBuffer(s.T())
		s.manager.buffers.Insert(s.channelName, wb)

		wb.EXPECT().SealAllSegments(mock.Anything).Return()

		err := manager.SealAllSegments(ctx, s.channelName)
		s.NoError(err)
	})
}

func (s *ManagerSuite) TestFlushAllSegments() {
	manager := s.manager
	s.Run("channel_not_found", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := manager.SealAllSegments(ctx, s.channelName)
		s.Error(err, "SealAllSegments shall return error when channel not found")
	})

	s.Run("normal_flush_all", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wb := NewMockWriteBuffer(s.T())
		s.manager.buffers.Insert(s.channelName, wb)

		wb.EXPECT().SealAllSegments(mock.Anything).Return()

		err := manager.SealAllSegments(ctx, s.channelName)
		s.NoError(err)
	})
}

func (s *ManagerSuite) TestCreateNewGrowingSegment() {
	manager := s.manager
	err := manager.CreateNewGrowingSegment(context.Background(), s.channelName, CreateGrowingSegmentInfo{
		PartitionID:    1,
		SegmentID:      1,
		StorageVersion: storage.StorageV2,
	})
	s.Error(err)

	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(nil, false).Once()
	s.metacache.EXPECT().AddSegment(mock.MatchedBy(func(info *datapb.SegmentInfo) bool {
		return info.GetSchemaVersion() == 100
	}), mock.Anything, mock.Anything, mock.Anything).Return()

	wb, err := NewL0WriteBuffer(s.channelName, s.metacache, s.syncMgr, &writeBufferOption{
		idAllocator: s.allocator,
	})
	s.NoError(err)

	s.manager.buffers.Insert(s.channelName, wb)
	err = manager.CreateNewGrowingSegment(context.Background(), s.channelName, CreateGrowingSegmentInfo{
		PartitionID:    1,
		SegmentID:      1,
		SchemaVersion:  100,
		StorageVersion: storage.StorageV2,
	})
	s.NoError(err)
}

func (s *ManagerSuite) TestBufferData() {
	manager := s.manager
	s.Run("channel_not_found", func() {
		err := manager.BufferData(s.channelName, nil, nil, nil, nil, 0)
		s.Error(err, "BufferData shall return error when channel not found")
	})

	s.Run("normal_buffer_data", func() {
		wb := NewMockWriteBuffer(s.T())

		s.manager.buffers.Insert(s.channelName, wb)
		wb.EXPECT().BufferData(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		err := manager.BufferData(s.channelName, nil, nil, nil, nil, 100)
		s.NoError(err)
	})
}

func (s *ManagerSuite) TestGetCheckpoint() {
	manager := s.manager
	s.Run("channel_not_found", func() {
		_, _, err := manager.GetCheckpoint(s.channelName)
		s.Error(err, "FlushSegments shall return error when channel not found")
	})

	s.Run("normal_checkpoint", func() {
		wb := NewMockWriteBuffer(s.T())

		manager.buffers.Insert(s.channelName, wb)
		pos := &msgpb.MsgPosition{ChannelName: s.channelName, Timestamp: tsoutil.ComposeTSByTime(time.Now())}
		wb.EXPECT().GetCheckpoint().Return(pos)
		wb.EXPECT().GetFlushTimestamp().Return(nonFlushTS)
		result, needUpdate, err := manager.GetCheckpoint(s.channelName)
		s.NoError(err)
		s.Equal(pos, result)
		s.False(needUpdate)
	})

	s.Run("checkpoint_need_update", func() {
		wb := NewMockWriteBuffer(s.T())

		manager.buffers.Insert(s.channelName, wb)
		cpTimestamp := tsoutil.ComposeTSByTime(time.Now())

		pos := &msgpb.MsgPosition{ChannelName: s.channelName, Timestamp: cpTimestamp}
		wb.EXPECT().GetCheckpoint().Return(pos)
		wb.EXPECT().GetFlushTimestamp().Return(cpTimestamp - 1)
		result, needUpdate, err := manager.GetCheckpoint(s.channelName)
		s.NoError(err)
		s.Equal(pos, result)
		s.True(needUpdate)
	})
}

func (s *ManagerSuite) TestRemoveChannel() {
	manager := NewManager(s.syncMgr)

	s.Run("remove_not_exist", func() {
		s.NotPanics(func() {
			manager.RemoveChannel(s.channelName)
		})
	})

	s.Run("remove_channel", func() {
		err := manager.Register(context.Background(), s.channelName, s.metacache, WithIDAllocator(s.allocator))
		s.Require().NoError(err)

		s.NotPanics(func() {
			manager.RemoveChannel(s.channelName)
		})
	})
}

func (s *ManagerSuite) TestDropPartitions() {
	manager := s.manager

	s.Run("drop_not_exist", func() {
		s.NotPanics(func() {
			manager.DropPartitions("not_exist_channel", nil)
		})
	})

	s.Run("drop_partitions", func() {
		wb := NewMockWriteBuffer(s.T())
		wb.EXPECT().DropPartitions(mock.Anything).Return()

		manager.buffers.Insert(s.channelName, wb)
		manager.DropPartitions(s.channelName, []int64{1})
	})
}

// FenceGrowingSourceAdmission on a channel with no buffer is a no-op (nothing
// can be admitted anyway), and WaitGrowingFlushDrained on a missing channel
// returns nil: the buffer is gone, owes no flush and pins no checkpoint, so the
// release must proceed rather than fail.
func (s *ManagerSuite) TestGrowingFlushRoutingMissingChannel() {
	s.NotPanics(func() {
		s.manager.FenceGrowingSourceAdmission(s.channelName)
	})
	s.NoError(s.manager.WaitGrowingFlushDrained(context.Background(), s.channelName, []int64{1, 2}))
}

// With a registered buffer, both calls must route to that buffer.
func (s *ManagerSuite) TestGrowingFlushRoutingRegisteredChannel() {
	wb := NewMockWriteBuffer(s.T())
	wb.EXPECT().FenceGrowingSourceAdmission().Return().Once()
	wb.EXPECT().WaitGrowingFlushDrained(mock.Anything, []int64{1}).Return(nil).Once()
	s.manager.buffers.Insert(s.channelName, wb)

	s.manager.FenceGrowingSourceAdmission(s.channelName)
	s.NoError(s.manager.WaitGrowingFlushDrained(context.Background(), s.channelName, []int64{1}))
}

func (s *ManagerSuite) TestMemoryCheck() {
	manager := s.manager
	param := paramtable.Get()
	s.setMemoryCheckParams("50", "false", "0.7")

	wb := NewMockWriteBuffer(s.T())

	flag := atomic.NewBool(false)
	memoryLimit := hardware.GetMemoryCount()
	signal := make(chan struct{}, 1)
	wb.EXPECT().MemorySize().RunAndReturn(func() int64 {
		if flag.Load() {
			return int64(float64(memoryLimit) * 0.4)
		}
		return int64(float64(memoryLimit) * 0.6)
	})
	// EvictableMemorySize is on the WriteBuffer interface now, so memoryCheck
	// calls it on every candidate instead of falling back to MemorySize when the
	// buffer happened not to implement it. This test only exercises the
	// watermark, so report the whole buffer as evictable.
	wb.EXPECT().EvictableMemorySize().RunAndReturn(func() int64 {
		if flag.Load() {
			return int64(float64(memoryLimit) * 0.4)
		}
		return int64(float64(memoryLimit) * 0.6)
	}).Maybe()
	wb.EXPECT().EvictBuffer(mock.Anything).Run(func(polices ...SyncPolicy) {
		select {
		case signal <- struct{}{}:
		default:
		}
		flag.Store(true)
	}).Return()
	manager.buffers.Insert(s.channelName, wb)
	manager.Start()
	defer manager.Stop()

	<-time.After(time.Millisecond * 100)
	wb.AssertNotCalled(s.T(), "MemorySize")

	param.Save(param.DataNodeCfg.MemoryForceSyncEnable.Key, "true")

	<-time.After(time.Millisecond * 100)
	wb.AssertNotCalled(s.T(), "SetMemoryHighFlag")
	param.Save(param.DataNodeCfg.MemoryForceSyncWatermark.Key, "0.5")

	<-signal
	wb.AssertExpectations(s.T())
}

func (s *ManagerSuite) TestStopDuringMemoryCheck() {
	manager := s.manager
	s.setMemoryCheckParams("50", "true", "0.7")

	wb := NewMockWriteBuffer(s.T())

	// mock the memory size reach water mark
	memoryLimit := hardware.GetMemoryCount()
	wb.EXPECT().MemorySize().RunAndReturn(func() int64 {
		return int64(float64(memoryLimit) * 0.8)
	}).Maybe()
	//.Return(int64(float64(memoryLimit) * 0.6))
	wb.EXPECT().EvictableMemorySize().Return(int64(0)).Maybe()
	wb.EXPECT().EvictBuffer(mock.Anything).Maybe()
	manager.buffers.Insert(s.channelName, wb)
	manager.Start()

	// wait memory check triggered
	time.Sleep(200 * time.Millisecond)

	// expect stop operation won't stuck
	manager.Stop()
}

func (s *ManagerSuite) TestMemoryCheckDoesNotSpinOnInFlightPayload() {
	param := paramtable.Get()
	saveParamForTest(s.T(), param.DataNodeCfg.MemoryForceSyncEnable.Key, "true")
	saveParamForTest(s.T(), param.DataNodeCfg.MemoryForceSyncWatermark.Key, "0")

	segmentID := int64(1001)
	task := syncmgr.NewSyncTask().
		WithSyncPack(new(syncmgr.SyncPack).WithSegmentID(segmentID)).
		WithPayloadAccounting(100, 0, nil)
	entry := &writeBufferSyncEntry{task: task}
	wb := &l0WriteBuffer{writeBufferBase: &writeBufferBase{
		buffers:               make(map[int64]*segmentBuffer),
		writeBufferSyncQueues: map[int64]*writeBufferSyncQueue{segmentID: {entries: []*writeBufferSyncEntry{entry}}},
	}}
	s.manager.buffers.Insert(s.channelName, wb)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.manager.memoryCheck()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("memory check spun on non-evictable in-flight payload")
	}
}

// The dual-drive backstop: the manager ticker must reach every registered
// buffer's due retries — even with no flowgraph goroutine delivering timeticks
// at all — skip a closed buffer harmlessly, and stop with the manager.
func (s *ManagerSuite) TestRetryBackstopDrivesRegisteredBuffers() {
	saveParamForTest(s.T(), paramtable.Get().DataNodeCfg.FlushRetryInterval.Key, "1")

	var mu sync.Mutex
	driven := make(map[int64]bool)
	s.syncMgr.EXPECT().SyncData(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, task syncmgr.Task, _ ...func(error) error) (*conc.Future[struct{}], error) {
			mu.Lock()
			driven[task.SegmentID()] = true
			mu.Unlock()
			return nil, nil
		}).Maybe()

	makeBuffer := func(segmentID int64, closed bool) *l0WriteBuffer {
		entry := newTestSyncEntry(segmentID, 300, entryFailed)
		queue := &writeBufferSyncQueue{entries: []*writeBufferSyncEntry{entry}}
		queue.intent.want()
		queue.intent.attempted(time.Now().Add(-time.Hour))
		syncCtx, syncCancel := context.WithCancel(context.Background())
		return &l0WriteBuffer{writeBufferBase: &writeBufferBase{
			syncMgr:               s.syncMgr,
			syncCtx:               syncCtx,
			syncCancel:            syncCancel,
			flushRetryInterval:    time.Millisecond,
			buffers:               make(map[int64]*segmentBuffer),
			writeBufferSyncQueues: map[int64]*writeBufferSyncQueue{segmentID: queue},
			closed:                closed,
		}}
	}
	wasDriven := func(segmentID int64) bool {
		mu.Lock()
		defer mu.Unlock()
		return driven[segmentID]
	}

	manager := NewManager(s.syncMgr).(*bufferManager)
	manager.buffers.Insert("backstop-ch-1", makeBuffer(2001, false))
	manager.buffers.Insert("backstop-ch-2", makeBuffer(2002, false))
	manager.buffers.Insert("backstop-ch-3", makeBuffer(2003, true))
	manager.Start()

	s.Eventually(func() bool {
		return wasDriven(2001) && wasDriven(2002)
	}, 3*time.Second, 10*time.Millisecond,
		"the backstop ticker must drive due retries on every registered buffer")
	s.False(wasDriven(2003), "a closed buffer must be skipped harmlessly")

	stopped := make(chan struct{})
	go func() {
		manager.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		s.FailNow("Stop did not terminate the retry backstop goroutine")
	}
}

func TestManager(t *testing.T) {
	suite.Run(t, new(ManagerSuite))
}
