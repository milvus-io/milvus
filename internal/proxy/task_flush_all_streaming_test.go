// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func createTestFlushAllTaskByStreamingService(t *testing.T, dbName string) (*flushAllTask, *mocks.MockMixCoordClient, *msgstream.MockMsgStream, *MockChannelsMgr, context.Context) {
	ctx := context.Background()
	mixCoord := mocks.NewMockMixCoordClient(t)
	replicateMsgStream := msgstream.NewMockMsgStream(t)
	chMgr := NewMockChannelsMgr(t)

	baseTask := &flushAllTask{
		baseTask:  baseTask{},
		Condition: NewTaskCondition(ctx),
		FlushAllRequest: &milvuspb.FlushAllRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     1,
				Timestamp: uint64(time.Now().UnixNano()),
				SourceID:  1,
			},
			DbName: dbName,
		},
		ctx:      ctx,
		mixCoord: mixCoord,
		chMgr:    chMgr,
	}
	return baseTask, mixCoord, replicateMsgStream, chMgr, ctx
}

func TestFlushAllTask_WithSpecificDB(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_WithSpecificDB", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ShowCollections
		showColResp := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection1", "collection2"},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
			Return(showColResp, nil).Once()

		// Mock GetCollectionID
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).To(func(cache *MetaCache, ctx context.Context, database, collectionName string) (UniqueID, error) {
			if collectionName == "collection1" {
				return UniqueID(100), nil
			} else if collectionName == "collection2" {
				return UniqueID(200), nil
			}
			return 0, fmt.Errorf("collection not found")
		}).Build()

		// Mock getVChannels
		mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
			if collID == UniqueID(100) {
				return []string{"vchannel1", "vchannel2"}, nil
			} else if collID == UniqueID(200) {
				return []string{"vchannel3"}, nil
			}
			return nil, fmt.Errorf("collection not found")
		}).Build()

		// Mock sendManualFlushToWAL
		mockey.Mock(sendManualFlushToWAL).Return(nil, nil).Build()

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	})
}

func TestFlushAllTask_WithoutSpecificDB(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_WithoutSpecificDB", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ListDatabases
		listDBResp := &milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"db1", "db2"},
		}
		mixCoord.EXPECT().ListDatabases(mock.Anything, mock.AnythingOfType("*milvuspb.ListDatabasesRequest")).
			Return(listDBResp, nil).Once()

		// Mock ShowCollections for each database
		showColResp1 := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection1"},
		}
		showColResp2 := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection2"},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
			return req.DbName == "db1"
		})).Return(showColResp1, nil).Once()
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
			return req.DbName == "db2"
		})).Return(showColResp2, nil).Once()

		// Mock GetCollectionID
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).To(func(cache *MetaCache, ctx context.Context, database, collectionName string) (UniqueID, error) {
			if collectionName == "collection1" {
				return UniqueID(100), nil
			} else if collectionName == "collection2" {
				return UniqueID(200), nil
			}
			return 0, fmt.Errorf("collection not found")
		}).Build()

		// Mock getVChannels
		mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
			if collID == UniqueID(100) {
				return []string{"vchannel1"}, nil
			} else if collID == UniqueID(200) {
				return []string{"vchannel2"}, nil
			}
			return nil, fmt.Errorf("collection not found")
		}).Build()

		// Mock sendManualFlushToWAL
		mockey.Mock(sendManualFlushToWAL).Return(nil, nil).Build()

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	})
}

func TestFlushAllTask_ListDatabasesError(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_ListDatabasesError", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ListDatabases with error
		expectedErr := fmt.Errorf("list databases failed")
		mixCoord.EXPECT().ListDatabases(mock.Anything, mock.AnythingOfType("*milvuspb.ListDatabasesRequest")).
			Return(nil, expectedErr).Once()

		err := task.Execute(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "list databases failed")
	})
}

func TestFlushAllTask_ShowCollectionsError(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_ShowCollectionsError", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ShowCollections with error
		expectedErr := fmt.Errorf("show collections failed")
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
			Return(nil, expectedErr).Once()

		err := task.Execute(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "show collections failed")
	})
}

func TestFlushAllTask_GetCollectionIDError(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_GetCollectionIDError", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ShowCollections
		showColResp := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection1"},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
			Return(showColResp, nil).Once()

		// Mock GetCollectionID with error
		globalMetaCache = &MetaCache{}
		expectedErr := fmt.Errorf("collection not found")
		mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(0), expectedErr).Build()

		err := task.Execute(ctx)
		assert.Error(t, err)
		// The error should be wrapped by merr.WrapErrAsInputErrorWhen
		assert.NotNil(t, err)
	})
}

func TestFlushAllTask_GetVChannelsError(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_GetVChannelsError", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ShowCollections
		showColResp := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection1"},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
			Return(showColResp, nil).Once()

		// Mock GetCollectionID
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()

		// Mock getVChannels with error
		expectedErr := fmt.Errorf("get vchannels failed")
		chMgr.EXPECT().getVChannels(UniqueID(100)).Return(nil, expectedErr).Once()

		err := task.Execute(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "get vchannels failed")
	})
}

func TestFlushAllTask_SendManualFlushError(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_SendManualFlushError", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ShowCollections
		showColResp := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection1"},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
			Return(showColResp, nil).Once()

		// Mock GetCollectionID
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()

		// Mock getVChannels
		chMgr.EXPECT().getVChannels(UniqueID(100)).Return([]string{"vchannel1"}, nil).Once()

		// Mock sendManualFlushToWAL with error
		expectedErr := fmt.Errorf("send manual flush failed")
		mockey.Mock(sendManualFlushToWAL).Return(nil, expectedErr).Build()

		err := task.Execute(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "send manual flush failed")
	})
}

func TestFlushAllTask_WithEmptyCollections(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_WithEmptyCollections", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ShowCollections with empty collections
		showColResp := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
			Return(showColResp, nil).Once()

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	})
}

func TestFlushAllTask_WithEmptyVChannels(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_WithEmptyVChannels", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ShowCollections
		showColResp := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection1"},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
			Return(showColResp, nil).Once()

		// Mock GetCollectionID
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()

		// Mock getVChannels with empty channels
		mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
			return []string{}, nil
		}).Build()

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	})
}

func TestFlushAllTask_MultipleVChannels(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_MultipleVChannels", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ShowCollections
		showColResp := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection1"},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
			Return(showColResp, nil).Once()

		// Mock GetCollectionID
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()

		// Mock getVChannels with multiple channels
		vchannels := []string{"vchannel1", "vchannel2", "vchannel3"}
		mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
			return vchannels, nil
		}).Build()

		// Mock sendManualFlushToWAL - should be called for each vchannel
		callCount := 0
		mockey.Mock(sendManualFlushToWAL).To(func(ctx context.Context, collID UniqueID, vchannel string, flushTs Timestamp) ([]int64, error) {
			callCount++
			assert.Equal(t, UniqueID(100), collID)
			assert.Contains(t, vchannels, vchannel)
			return nil, nil
		}).Build()

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, len(vchannels), callCount) // Verify sendManualFlushToWAL was called for each vchannel
		assert.NotNil(t, task.result)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	})
}

func TestFlushAllTask_VerifyFlushTs(t *testing.T) {
	mockey.PatchConvey("TestFlushAllTask_VerifyFlushTs", t, func() {
		task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
		defer mixCoord.AssertExpectations(t)
		defer replicateMsgStream.AssertExpectations(t)
		defer chMgr.AssertExpectations(t)

		// Mock ShowCollections
		showColResp := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection1"},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
			Return(showColResp, nil).Once()

		// Mock GetCollectionID
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()

		// Mock getVChannels
		mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
			return []string{"vchannel1"}, nil
		}).Build()

		// Mock sendManualFlushToWAL and verify flushTs
		expectedFlushTs := task.BeginTs()
		mockey.Mock(sendManualFlushToWAL).To(func(ctx context.Context, collID UniqueID, vchannel string, flushTs Timestamp) ([]int64, error) {
			assert.Equal(t, expectedFlushTs, flushTs)
			return nil, nil
		}).Build()

		err := task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, expectedFlushTs, task.result.FlushTs)
	})
}
