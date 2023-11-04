package writebuffer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ManagerSuite struct {
	suite.Suite
	channelName string
	collSchema  *schemapb.CollectionSchema
	syncMgr     *syncmgr.MockSyncManager
	metacache   *metacache.MockMetaCache

	manager *manager
}

func (s *ManagerSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	s.collSchema = &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
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

	mgr := NewManager(s.syncMgr)
	var ok bool
	s.manager, ok = mgr.(*manager)
	s.Require().True(ok)
}

func (s *ManagerSuite) TestRegister() {
	manager := s.manager

	err := manager.Register(s.channelName, s.collSchema, s.metacache)
	s.NoError(err)

	err = manager.Register(s.channelName, s.collSchema, s.metacache)
	s.Error(err)
	s.ErrorIs(err, merr.ErrChannelReduplicate)
}

func (s *ManagerSuite) TestFlushSegments() {
	manager := s.manager
	s.Run("channel_not_found", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := manager.FlushSegments(ctx, s.channelName, []int64{1, 2, 3})
		s.Error(err, "FlushSegments shall return error when channel not found")
	})

	s.Run("normal_flush", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wb := NewMockWriteBuffer(s.T())

		s.manager.mut.Lock()
		s.manager.buffers[s.channelName] = wb
		s.manager.mut.Unlock()

		wb.EXPECT().FlushSegments(mock.Anything, mock.Anything).Return(nil)

		err := manager.FlushSegments(ctx, s.channelName, []int64{1})
		s.NoError(err)
	})
}

func (s *ManagerSuite) TestBufferData() {
	manager := s.manager
	s.Run("channel_not_found", func() {
		err := manager.BufferData(s.channelName, nil, nil, nil, nil)
		s.Error(err, "FlushSegments shall return error when channel not found")
	})

	s.Run("normal_buffer_data", func() {
		wb := NewMockWriteBuffer(s.T())

		s.manager.mut.Lock()
		s.manager.buffers[s.channelName] = wb
		s.manager.mut.Unlock()

		wb.EXPECT().BufferData(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		err := manager.BufferData(s.channelName, nil, nil, nil, nil)
		s.NoError(err)
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
		err := manager.Register(s.channelName, s.collSchema, s.metacache)
		s.Require().NoError(err)

		s.NotPanics(func() {
			manager.RemoveChannel(s.channelName)
		})
	})
}

func TestManager(t *testing.T) {
	suite.Run(t, new(ManagerSuite))
}
