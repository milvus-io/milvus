// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package broker

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type BrokerSuite struct {
	suite.Suite

	mixCoord *mocks.MixCoord
	broker   *coordinatorBroker
}

func (s *BrokerSuite) SetupSuite() {
	paramtable.Init()
}

func (s *BrokerSuite) SetupTest() {
	s.mixCoord = mocks.NewMixCoord(s.T())
	s.broker = NewCoordinatorBroker(s.mixCoord)
}

func (s *BrokerSuite) TearDownTest() {
	if s.mixCoord != nil {
		s.mixCoord.AssertExpectations(s.T())
	}
	s.mixCoord = nil
}

func (s *BrokerSuite) TestNewCoordinatorBroker() {
	s.Run("create_broker", func() {
		mockMixCoord := mocks.NewMixCoord(s.T())
		broker := NewCoordinatorBroker(mockMixCoord)
		s.NotNil(broker)
		s.Equal(mockMixCoord, broker.mixCoord)
	})
}

func TestBrokerSuite(t *testing.T) {
	suite.Run(t, new(BrokerSuite))
}

// --- Tests using mockey for DropCollection, CreateCollection, CreatePartition ---

func TestDropCollection_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DropCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
			assert.Equal(t, "test_db", req.GetDbName())
			assert.Equal(t, "test_collection", req.GetCollectionName())
			assert.NotNil(t, req.GetBase())
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.DropCollection(ctx, "test_db", "test_collection")

	assert.NoError(t, err)
}

func TestDropCollection_RPCError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.DropCollection(ctx, "test_db", "test_collection")

	assert.Error(t, err)
}

func TestDropCollection_StatusError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(
		merr.Status(merr.WrapErrCollectionNotFound("test_collection")), nil)

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.DropCollection(ctx, "test_db", "test_collection")

	assert.Error(t, err)
}

func TestCreateCollection_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreateCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
			assert.Equal(t, "test_db", req.GetDbName())
			assert.Equal(t, "test_collection", req.GetCollectionName())
			assert.NotNil(t, req.GetBase())
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
	})

	assert.NoError(t, err)
}

func TestCreateCollection_RPCError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
	})

	assert.Error(t, err)
}

func TestCreateCollection_StatusError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(
		merr.Status(errors.New("collection already exists")), nil)

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
	})

	assert.Error(t, err)
}

func TestCreateCollection_WithExistingBase(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreateCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
			// Base should be preserved if already set
			assert.NotNil(t, req.GetBase())
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
		DbName:         "test_db",
		CollectionName: "test_collection",
	})

	assert.NoError(t, err)
}

// TestCreateCollection_UsesSnapshotRestoreBrokerTimeout guards against the restore-path
// CreateCollection RPC being bounded by queryCoord.brokerTimeout (5s by default, sized for
// QueryCoord metadata lookups) instead of its own dataCoord.snapshot.restoreBrokerTimeout.
// This DDL must be appended to the WAL before RootCoord replies, so a short metadata timeout
// can fire before the DDL completes even though it eventually succeeds, leaving an orphan
// collection behind (see https://github.com/milvus-io/milvus/issues/53715).
func TestCreateCollection_UsesSnapshotRestoreBrokerTimeout(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	oldQueryCoordTimeout := paramtable.Get().QueryCoordCfg.BrokerTimeout.SwapTempValue("1")
	defer paramtable.Get().QueryCoordCfg.BrokerTimeout.SwapTempValue(oldQueryCoordTimeout)
	oldRestoreTimeout := paramtable.Get().DataCoordCfg.SnapshotRestoreBrokerTimeout.SwapTempValue("30000")
	defer paramtable.Get().DataCoordCfg.SnapshotRestoreBrokerTimeout.SwapTempValue(oldRestoreTimeout)

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreateCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
			deadline, ok := ctx.Deadline()
			assert.True(t, ok)
			// With queryCoord.brokerTimeout at 1ms, a ctx bounded by it would have a
			// sub-millisecond budget left here; the restore-specific timeout (30s) must
			// be the one actually in effect.
			assert.True(t, time.Until(deadline) > time.Second)
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
	})

	assert.NoError(t, err)
}

func TestCreatePartition_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreatePartition(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
			assert.Equal(t, "test_db", req.GetDbName())
			assert.Equal(t, "test_collection", req.GetCollectionName())
			assert.Equal(t, "test_partition", req.GetPartitionName())
			assert.NotNil(t, req.GetBase())
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		PartitionName:  "test_partition",
	})

	assert.NoError(t, err)
}

func TestCreatePartition_RPCError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		PartitionName:  "test_partition",
	})

	assert.Error(t, err)
}

func TestCreatePartition_StatusError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(
		merr.Status(errors.New("partition already exists")), nil)

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		PartitionName:  "test_partition",
	})

	assert.Error(t, err)
}

// TestCreatePartition_UsesSnapshotRestoreBrokerTimeout mirrors
// TestCreateCollection_UsesSnapshotRestoreBrokerTimeout: CreatePartition is the other
// restore-path DDL that must go through the WAL and therefore needs the dedicated
// restore timeout rather than queryCoord.brokerTimeout.
func TestCreatePartition_UsesSnapshotRestoreBrokerTimeout(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	oldQueryCoordTimeout := paramtable.Get().QueryCoordCfg.BrokerTimeout.SwapTempValue("1")
	defer paramtable.Get().QueryCoordCfg.BrokerTimeout.SwapTempValue(oldQueryCoordTimeout)
	oldRestoreTimeout := paramtable.Get().DataCoordCfg.SnapshotRestoreBrokerTimeout.SwapTempValue("30000")
	defer paramtable.Get().DataCoordCfg.SnapshotRestoreBrokerTimeout.SwapTempValue(oldRestoreTimeout)

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreatePartition(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
			deadline, ok := ctx.Deadline()
			assert.True(t, ok)
			assert.True(t, time.Until(deadline) > time.Second)
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		PartitionName:  "test_partition",
	})

	assert.NoError(t, err)
}

func TestGetFileResources(t *testing.T) {
	paramtable.Init()

	t.Run("return requested resources in order", func(t *testing.T) {
		mockMixCoord := mocks.NewMixCoord(t)
		expected := []*internalpb.FileResourceInfo{
			{Id: 1, Name: "resource-1", Path: "files/1"},
			{Id: 2, Name: "resource-2", Path: "files/2"},
		}
		mockMixCoord.EXPECT().GetFileResources(mock.Anything, int64(1), int64(2)).Return(expected, nil)

		resources, err := NewCoordinatorBroker(mockMixCoord).GetFileResources(context.Background(), 1, 2)
		assert.NoError(t, err)
		assert.Equal(t, expected, resources)
	})

	t.Run("missing resource is internal error", func(t *testing.T) {
		mockMixCoord := mocks.NewMixCoord(t)
		expectedErr := merr.WrapErrServiceInternalMsg("file resource 1 not found")
		mockMixCoord.EXPECT().GetFileResources(mock.Anything, int64(1)).Return(nil, expectedErr)

		resources, err := NewCoordinatorBroker(mockMixCoord).GetFileResources(context.Background(), 1)
		assert.Nil(t, resources)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("resolver error is preserved", func(t *testing.T) {
		mockMixCoord := mocks.NewMixCoord(t)
		expectedErr := merr.WrapErrServiceUnavailable("mixcoord unavailable")
		mockMixCoord.EXPECT().GetFileResources(mock.Anything, int64(1)).Return(nil, expectedErr)

		resources, err := NewCoordinatorBroker(mockMixCoord).GetFileResources(context.Background(), 1)
		assert.Nil(t, resources)
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})

	t.Run("empty ids skip request", func(t *testing.T) {
		mockMixCoord := mocks.NewMixCoord(t)

		resources, err := NewCoordinatorBroker(mockMixCoord).GetFileResources(context.Background())
		assert.NoError(t, err)
		assert.Nil(t, resources)
	})
}
