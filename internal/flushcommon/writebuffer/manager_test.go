package writebuffer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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
	s.collSchema = &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, Name: common.RowIDFieldName},
			{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64, Name: common.TimeStampFieldName},
			{
				FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
			},
			{
				FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}

	s.channelName = "by-dev-rootcoord-dml_0_100_v0"
}

func (s *ManagerSuite) SetupTest() {
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
	s.metacache = metacache.NewMockMetaCache(s.T())
	s.metacache.EXPECT().Collection().Return(s.collID).Maybe()
	s.metacache.EXPECT().Schema().Return(s.collSchema).Maybe()
	s.allocator = allocator.NewMockAllocator(s.T())

	mgr := NewManager(s.syncMgr)
	var ok bool
	s.manager, ok = mgr.(*bufferManager)
	s.Require().True(ok)
}

func (s *ManagerSuite) TestRegister() {
	manager := s.manager

	err := manager.Register(s.channelName, s.metacache, WithIDAllocator(s.allocator))
	s.NoError(err)

	err = manager.Register(s.channelName, s.metacache, WithIDAllocator(s.allocator))
	s.Error(err)
	s.ErrorIs(err, merr.ErrChannelReduplicate)
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
}

func (s *ManagerSuite) TestCreateNewGrowingSegment() {
	manager := s.manager
	err := manager.CreateNewGrowingSegment(context.Background(), s.channelName, 1, 1, storage.StorageV2)
	s.Error(err)

	s.metacache.EXPECT().GetSegmentByID(mock.Anything).Return(nil, false).Once()
	s.metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()

	wb, err := NewL0WriteBuffer(s.channelName, s.metacache, s.syncMgr, &writeBufferOption{
		idAllocator: s.allocator,
	})
	s.NoError(err)

	s.manager.buffers.Insert(s.channelName, wb)
	err = manager.CreateNewGrowingSegment(context.Background(), s.channelName, 1, 1, storage.StorageV2)
	s.NoError(err)
}

func (s *ManagerSuite) TestBufferData() {
	manager := s.manager
	s.Run("channel_not_found", func() {
		err := manager.BufferData(s.channelName, nil, nil, nil, nil)
		s.Error(err, "BufferData shall return error when channel not found")
	})

	s.Run("normal_buffer_data", func() {
		wb := NewMockWriteBuffer(s.T())

		s.manager.buffers.Insert(s.channelName, wb)
		wb.EXPECT().BufferData(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		err := manager.BufferData(s.channelName, nil, nil, nil, nil)
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
		pos := &msgpb.MsgPosition{ChannelName: s.channelName, Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0)}
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
		cpTimestamp := tsoutil.ComposeTSByTime(time.Now(), 0)

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
		err := manager.Register(s.channelName, s.metacache, WithIDAllocator(s.allocator))
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

func (s *ManagerSuite) TestMemoryCheck() {
	manager := s.manager
	param := paramtable.Get()

	param.Save(param.DataNodeCfg.MemoryCheckInterval.Key, "50")
	param.Save(param.DataNodeCfg.MemoryForceSyncEnable.Key, "false")
	param.Save(param.DataNodeCfg.MemoryForceSyncWatermark.Key, "0.7")

	defer func() {
		param.Reset(param.DataNodeCfg.MemoryCheckInterval.Key)
		param.Reset(param.DataNodeCfg.MemoryForceSyncEnable.Key)
		param.Reset(param.DataNodeCfg.MemoryForceSyncWatermark.Key)
	}()

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
	param := paramtable.Get()

	param.Save(param.DataNodeCfg.MemoryCheckInterval.Key, "50")
	param.Save(param.DataNodeCfg.MemoryForceSyncEnable.Key, "true")
	param.Save(param.DataNodeCfg.MemoryForceSyncWatermark.Key, "0.7")

	defer func() {
		param.Reset(param.DataNodeCfg.MemoryCheckInterval.Key)
		param.Reset(param.DataNodeCfg.MemoryForceSyncEnable.Key)
		param.Reset(param.DataNodeCfg.MemoryForceSyncWatermark.Key)
	}()

	wb := NewMockWriteBuffer(s.T())

	// mock the memory size reach water mark
	memoryLimit := hardware.GetMemoryCount()
	wb.EXPECT().MemorySize().RunAndReturn(func() int64 {
		return int64(float64(memoryLimit) * 0.8)
	}).Maybe()
	//.Return(int64(float64(memoryLimit) * 0.6))
	wb.EXPECT().EvictBuffer(mock.Anything).Maybe()
	manager.buffers.Insert(s.channelName, wb)
	manager.Start()

	// wait memory check triggered
	time.Sleep(200 * time.Millisecond)

	// expect stop operation won't stuck
	manager.Stop()
}

func TestManager(t *testing.T) {
	suite.Run(t, new(ManagerSuite))
}
