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
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
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
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1", "collection2"},
		CollectionIds:   []int64{100, 200},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(showColResp, nil).Once()

	// Mock GetCollectionID
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).To(func(cache *MetaCache, ctx context.Context, database, collectionName string) (UniqueID, error) {
		if collectionName == "collection1" {
			return UniqueID(100), nil
		} else if collectionName == "collection2" {
			return UniqueID(200), nil
		}
		return 0, fmt.Errorf("collection not found")
	}).Build()
	defer mockGetCollectionID.UnPatch()

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
		if collID == UniqueID(100) {
			return []string{"vchannel1", "vchannel2"}, nil
		} else if collID == UniqueID(200) {
			return []string{"vchannel3"}, nil
		}
		return nil, fmt.Errorf("collection not found")
	}).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).Return([]int64{1001, 1002}, nil).Build()
	defer mockSendManualFlush.UnPatch()

	// Mock MixCoord.FlushAll call (new API)
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.MatchedBy(func(req *datapb.FlushAllRequest) bool {
		return req.DbName == "test_db" && len(req.FlushTargets) == 1
	})).Return(&datapb.FlushAllResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		FlushResults: []*datapb.FlushResult{
			{
				DbName:          "test_db",
				CollectionName:  "collection1",
				CollectionID:    100,
				FlushSegmentIDs: []int64{2001, 2002},
				ChannelCps:      map[string]*msgpb.MsgPosition{},
			},
			{
				DbName:          "test_db",
				CollectionName:  "collection2",
				CollectionID:    200,
				FlushSegmentIDs: []int64{2003},
				ChannelCps:      map[string]*msgpb.MsgPosition{},
			},
		},
	}, nil).Once()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
}

func TestFlushAllTask_WithoutSpecificDB(t *testing.T) {
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
		CollectionIds:   []int64{100},
	}
	showColResp2 := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection2"},
		CollectionIds:   []int64{200},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
		return req.DbName == "db1"
	})).Return(showColResp1, nil).Once()
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
		return req.DbName == "db2"
	})).Return(showColResp2, nil).Once()

	// Mock GetCollectionID
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).To(func(cache *MetaCache, ctx context.Context, database, collectionName string) (UniqueID, error) {
		if collectionName == "collection1" {
			return UniqueID(100), nil
		} else if collectionName == "collection2" {
			return UniqueID(200), nil
		}
		return 0, fmt.Errorf("collection not found")
	}).Build()
	defer mockGetCollectionID.UnPatch()

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
		if collID == UniqueID(100) {
			return []string{"vchannel1"}, nil
		} else if collID == UniqueID(200) {
			return []string{"vchannel2"}, nil
		}
		return nil, fmt.Errorf("collection not found")
	}).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).Return([]int64{1001}, nil).Build()
	defer mockSendManualFlush.UnPatch()

	// Mock MixCoord.FlushAll call (new API)
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.MatchedBy(func(req *datapb.FlushAllRequest) bool {
		return req.DbName == "" && len(req.FlushTargets) == 2
	})).Return(&datapb.FlushAllResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		FlushResults: []*datapb.FlushResult{
			{
				DbName:          "db1",
				CollectionName:  "collection1",
				CollectionID:    100,
				FlushSegmentIDs: []int64{2001},
				ChannelCps:      map[string]*msgpb.MsgPosition{},
			},
			{
				DbName:          "db2",
				CollectionName:  "collection2",
				CollectionID:    200,
				FlushSegmentIDs: []int64{2002},
				ChannelCps:      map[string]*msgpb.MsgPosition{},
			},
		},
	}, nil).Once()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
}

func TestFlushAllTask_WithEmptyCollections(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections with empty collections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{},
		CollectionIds:   []int64{},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(showColResp, nil).Once()

	// Mock FlushAll call with empty targets
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.MatchedBy(func(req *datapb.FlushAllRequest) bool {
		return req.DbName == "test_db" && len(req.FlushTargets) == 1 && len(req.FlushTargets[0].CollectionIds) == 0
	})).Return(&datapb.FlushAllResponse{
		Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		FlushResults: []*datapb.FlushResult{},
	}, nil).Once()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
}

func TestFlushAllTask_MultipleVChannels(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1"},
		CollectionIds:   []int64{100},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(showColResp, nil).Once()

	// Mock GetCollectionID
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	// Mock getVChannels with multiple channels
	vchannels := []string{"vchannel1", "vchannel2", "vchannel3"}
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
		return vchannels, nil
	}).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL - should be called for each vchannel
	callCount := 0
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).To(func(ctx context.Context, collID UniqueID, vchannel string, flushTs Timestamp) ([]int64, error) {
		callCount++
		assert.Equal(t, UniqueID(100), collID)
		assert.Contains(t, vchannels, vchannel)
		return []int64{int64(callCount * 100)}, nil
	}).Build()
	defer mockSendManualFlush.UnPatch()

	// Mock MixCoord.FlushAll call (new API)
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.MatchedBy(func(req *datapb.FlushAllRequest) bool {
		return req.DbName == "test_db" && len(req.FlushTargets) == 1
	})).Return(&datapb.FlushAllResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		FlushResults: []*datapb.FlushResult{
			{
				DbName:          "test_db",
				CollectionName:  "collection1",
				CollectionID:    100,
				FlushSegmentIDs: []int64{2001, 2002},
				ChannelCps:      map[string]*msgpb.MsgPosition{},
			},
		},
	}, nil).Once()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, len(vchannels), callCount) // Verify sendManualFlushToWAL was called for each vchannel
	assert.NotNil(t, task.result)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
}

func TestFlushAllTask_VerifyFlushTs(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1"},
		CollectionIds:   []int64{100},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(showColResp, nil).Once()

	// Mock GetCollectionID
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
		return []string{"vchannel1"}, nil
	}).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL and verify flushTs
	expectedFlushTs := task.BeginTs()
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).To(func(ctx context.Context, collID UniqueID, vchannel string, flushTs Timestamp) ([]int64, error) {
		assert.Equal(t, expectedFlushTs, flushTs)
		return []int64{1001}, nil
	}).Build()
	defer mockSendManualFlush.UnPatch()

	// Mock MixCoord.FlushAll call (new API)
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.MatchedBy(func(req *datapb.FlushAllRequest) bool {
		return req.DbName == "test_db" && len(req.FlushTargets) == 1
	})).Return(&datapb.FlushAllResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		FlushResults: []*datapb.FlushResult{
			{
				DbName:          "test_db",
				CollectionName:  "collection1",
				CollectionID:    100,
				FlushSegmentIDs: []int64{2001},
				ChannelCps:      map[string]*msgpb.MsgPosition{},
			},
		},
	}, nil).Once()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.Equal(t, expectedFlushTs, task.result.FlushAllTs)
}

func TestFlushAllStreamingTask_WithFlushTargets(t *testing.T) {
	// Test streaming FlushAll with flush_targets filtering
	t.Run("streaming FlushAll with flush_targets filtering", func(t *testing.T) {
		ctx := context.Background()
		mixCoord := mocks.NewMockMixCoordClient(t)
		chMgr := NewMockChannelsMgr(t)

		// Create flushAllTask with flush_targets
		task := &flushAllTask{
			baseTask:  baseTask{},
			Condition: NewTaskCondition(ctx),
			FlushAllRequest: &milvuspb.FlushAllRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Flush,
					MsgID:     1,
					Timestamp: uint64(time.Now().UnixNano()),
					SourceID:  1,
				},
				FlushTargets: []*milvuspb.FlushAllTarget{
					{
						DbName:          "test_db1",
						CollectionNames: []string{"collection1", "collection2"},
					},
					{
						DbName:          "test_db2",
						CollectionNames: []string{"collection3"},
					},
				},
			},
			ctx:      ctx,
			mixCoord: mixCoord,
			chMgr:    chMgr,
			result: &milvuspb.FlushAllResponse{
				Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				FlushAllTs:   uint64(time.Now().UnixNano()),
				FlushResults: make([]*milvuspb.FlushAllResult, 0),
			},
		}

		// No ListDatabases or ShowCollections mock needed when using flush_targets -
		// collections are specified directly in the request

		// Mock GetCollectionID for filtered collections only
		globalMetaCache = &MetaCache{}
		mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
			switch collectionName {
			case "collection1":
				return UniqueID(101), nil
			case "collection2":
				return UniqueID(102), nil
			case "collection3":
				return UniqueID(103), nil
			default:
				return UniqueID(0), errors.New("collection not found")
			}
		}).Build()
		defer mockGetCollectionID.UnPatch()

		// Mock getVChannels for each collection
		mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
			switch collID {
			case 101:
				return []string{"vchannel1"}, nil
			case 102:
				return []string{"vchannel2"}, nil
			case 103:
				return []string{"vchannel3"}, nil
			default:
				return nil, errors.New("vchannel not found")
			}
		}).Build()
		defer mockGetVChannels.UnPatch()

		// Mock sendManualFlushToWAL - should only be called for targeted collections
		var flushedCollections []UniqueID
		var mutex sync.Mutex
		mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).To(func(ctx context.Context, collID UniqueID, vchannel string, flushTs Timestamp) ([]int64, error) {
			mutex.Lock()
			defer mutex.Unlock()
			flushedCollections = append(flushedCollections, collID)
			return []int64{collID * 10}, nil // Mock segment IDs
		}).Build()
		defer mockSendManualFlush.UnPatch()

		mixCoord.EXPECT().FlushAll(mock.Anything, mock.MatchedBy(func(req *datapb.FlushAllRequest) bool {
			return len(req.FlushTargets) == 2 &&
				req.FlushTargets[0].DbName == "test_db1" &&
				req.FlushTargets[1].DbName == "test_db2"
		})).Return(&datapb.FlushAllResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			FlushResults: []*datapb.FlushResult{
				{
					DbName:          "test_db1",
					CollectionName:  "collection1",
					CollectionID:    101,
					FlushSegmentIDs: []int64{1001},
					ChannelCps:      map[string]*msgpb.MsgPosition{},
				},
				{
					DbName:          "test_db1",
					CollectionName:  "collection2",
					CollectionID:    102,
					FlushSegmentIDs: []int64{1002},
					ChannelCps:      map[string]*msgpb.MsgPosition{},
				},
				{
					DbName:          "test_db2",
					CollectionName:  "collection3",
					CollectionID:    103,
					FlushSegmentIDs: []int64{1003},
					ChannelCps:      map[string]*msgpb.MsgPosition{},
				},
			},
		}, nil).Once()

		// Execute test
		err := task.Execute(ctx)

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)

		// Verify that only targeted collections were flushed
		assert.ElementsMatch(t, []UniqueID{101, 102, 103}, flushedCollections)
		assert.Len(t, flushedCollections, 3) // Should flush exactly 3 collections

		// Verify that flush results contain proper database-level aggregation
		assert.Len(t, task.result.FlushResults, 2) // Should have 2 database-level results

		// Verify database names in results
		dbNames := make(map[string]bool)
		for _, result := range task.result.FlushResults {
			dbNames[result.DbName] = true
		}
		assert.True(t, dbNames["test_db1"])
		assert.True(t, dbNames["test_db2"])

		mixCoord.AssertExpectations(t)
	})

	// Test streaming FlushAll with specific database filtering
	t.Run("streaming FlushAll with database-only filtering", func(t *testing.T) {
		ctx := context.Background()
		mixCoord := mocks.NewMockMixCoordClient(t)
		chMgr := NewMockChannelsMgr(t)

		// Create flushAllTask with database-only flush_targets
		task := &flushAllTask{
			baseTask:  baseTask{},
			Condition: NewTaskCondition(ctx),
			FlushAllRequest: &milvuspb.FlushAllRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Flush,
					MsgID:     1,
					Timestamp: uint64(time.Now().UnixNano()),
					SourceID:  1,
				},
				FlushTargets: []*milvuspb.FlushAllTarget{
					{
						DbName:          "target_db",
						CollectionNames: []string{}, // Empty collections means flush all in this DB
					},
				},
			},
			ctx:      ctx,
			mixCoord: mixCoord,
			chMgr:    chMgr,
			result: &milvuspb.FlushAllResponse{
				Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				FlushAllTs:   uint64(time.Now().UnixNano()),
				FlushResults: make([]*milvuspb.FlushAllResult, 0),
			},
		}

		// No ListDatabases mock needed when using flush_targets - target database is specified directly

		// Mock ShowCollections for target_db - empty CollectionNames means flush all collections in this DB
		showColResp := &milvuspb.ShowCollectionsResponse{
			Status:          merr.Success(),
			CollectionNames: []string{"collection1", "collection2"},
			CollectionIds:   []int64{201, 202},
		}
		mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
			return req.DbName == "target_db"
		})).Return(showColResp, nil).Once()

		// Mock GetCollectionID for all collections in target_db
		globalMetaCache = &MetaCache{}
		mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
			if dbName == "target_db" {
				if collectionName == "collection1" {
					return UniqueID(201), nil
				} else if collectionName == "collection2" {
					return UniqueID(202), nil
				}
			}
			return UniqueID(0), errors.New("collection not found")
		}).Build()
		defer mockGetCollectionID.UnPatch()

		// Mock getVChannels for each collection
		mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
			switch collID {
			case 201:
				return []string{"vchannel1"}, nil
			case 202:
				return []string{"vchannel2"}, nil
			default:
				return nil, errors.New("vchannel not found")
			}
		}).Build()
		defer mockGetVChannels.UnPatch()

		// Mock sendManualFlushToWAL - should be called for all collections in target_db
		var flushedCollections []UniqueID
		var mutex2 sync.Mutex
		mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).To(func(ctx context.Context, collID UniqueID, vchannel string, flushTs Timestamp) ([]int64, error) {
			mutex2.Lock()
			defer mutex2.Unlock()
			flushedCollections = append(flushedCollections, collID)
			return []int64{collID * 10}, nil
		}).Build()
		defer mockSendManualFlush.UnPatch()

		// Mock MixCoord.FlushAll call
		mixCoord.EXPECT().FlushAll(mock.Anything, mock.Anything).Return(&datapb.FlushAllResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			FlushResults: []*datapb.FlushResult{
				{
					DbName:          "target_db",
					CollectionName:  "collection1",
					CollectionID:    201,
					FlushSegmentIDs: []int64{2001},
					ChannelCps:      map[string]*msgpb.MsgPosition{},
				},
				{
					DbName:          "target_db",
					CollectionName:  "collection2",
					CollectionID:    202,
					FlushSegmentIDs: []int64{2002},
					ChannelCps:      map[string]*msgpb.MsgPosition{},
				},
			},
		}, nil).Once()

		// Execute test
		err := task.Execute(ctx)

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)

		// Verify that all collections in target_db were flushed
		assert.Len(t, flushedCollections, 2)
		assert.Contains(t, flushedCollections, int64(201))
		assert.Contains(t, flushedCollections, int64(202))

		// Verify that flush results contain exactly 1 database result
		assert.Len(t, task.result.FlushResults, 1)
		assert.Equal(t, "target_db", task.result.FlushResults[0].DbName)

		mixCoord.AssertExpectations(t)
	})
}

// Test expandFlushCollectionNames method
func TestFlushAllTask_ExpandFlushCollectionNames_WithFlushTargets(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Set flush targets in request
	task.FlushAllRequest.FlushTargets = []*milvuspb.FlushAllTarget{
		{
			DbName:          "db1",
			CollectionNames: []string{"collection1", "collection2"},
		},
		{
			DbName:          "db2",
			CollectionNames: []string{"collection3"},
		},
	}
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
		if collectionName == "collection1" {
			return UniqueID(100), nil
		} else if collectionName == "collection2" {
			return UniqueID(200), nil
		} else if collectionName == "collection3" {
			return UniqueID(300), nil
		}
		return UniqueID(0), errors.New("collection not found")
	}).Build()
	defer mockGetCollectionID.UnPatch()

	targets, err := task.expandFlushCollectionNames(ctx)
	assert.NoError(t, err)
	assert.Len(t, targets, 2)
	assert.Equal(t, "db1", targets[0].DbName)
	assert.Equal(t, []int64{100, 200}, targets[0].CollectionIds)
	assert.Equal(t, "db2", targets[1].DbName)
	assert.Equal(t, []int64{300}, targets[1].CollectionIds)
}

func TestFlushAllTask_ExpandFlushCollectionNames_WithDbName(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1", "collection2"},
		CollectionIds:   []int64{100, 200},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
		return req.DbName == "test_db"
	})).Return(showColResp, nil).Once()

	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
		if collectionName == "collection1" {
			return UniqueID(100), nil
		} else if collectionName == "collection2" {
			return UniqueID(200), nil
		}
		return UniqueID(0), errors.New("collection not found")
	}).Build()
	defer mockGetCollectionID.UnPatch()

	targets, err := task.expandFlushCollectionNames(ctx)
	assert.NoError(t, err)
	assert.Len(t, targets, 1)
	assert.Equal(t, "test_db", targets[0].DbName)
	assert.Equal(t, []int64{100, 200}, targets[0].CollectionIds)
}

func TestFlushAllTask_ExpandFlushCollectionNames_AllDatabases(t *testing.T) {
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
		CollectionIds:   []int64{100},
	}
	showColResp2 := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection2"},
		CollectionIds:   []int64{200},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
		return req.DbName == "db1"
	})).Return(showColResp1, nil).Once()
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
		return req.DbName == "db2"
	})).Return(showColResp2, nil).Once()

	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
		if collectionName == "collection1" {
			return UniqueID(100), nil
		} else if collectionName == "collection2" {
			return UniqueID(200), nil
		}
		return UniqueID(0), errors.New("collection not found")
	}).Build()
	defer mockGetCollectionID.UnPatch()

	targets, err := task.expandFlushCollectionNames(ctx)
	assert.NoError(t, err)
	assert.Len(t, targets, 2)
	assert.Equal(t, "db1", targets[0].DbName)
	assert.Equal(t, []int64{100}, targets[0].CollectionIds)
	assert.Equal(t, "db2", targets[1].DbName)
	assert.Equal(t, []int64{200}, targets[1].CollectionIds)
}

func TestFlushAllTask_ExpandFlushCollectionNames_ShowCollectionsError(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections with error
	expectedErr := fmt.Errorf("show collections failed")
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(nil, expectedErr).Once()

	targets, err := task.expandFlushCollectionNames(ctx)
	assert.Error(t, err)
	assert.Nil(t, targets)
	assert.Contains(t, err.Error(), "show collections failed")
}

func TestFlushAllTask_ExpandFlushCollectionNames_ListDatabasesError(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ListDatabases with error
	expectedErr := fmt.Errorf("list databases failed")
	mixCoord.EXPECT().ListDatabases(mock.Anything, mock.AnythingOfType("*milvuspb.ListDatabasesRequest")).
		Return(nil, expectedErr).Once()

	targets, err := task.expandFlushCollectionNames(ctx)
	assert.Error(t, err)
	assert.Nil(t, targets)
	assert.Contains(t, err.Error(), "list databases failed")
}

func TestFlushAllTask_ExpandFlushCollectionNames_EmptyCollectionNames(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Set flush targets with empty collection names
	task.FlushAllRequest.FlushTargets = []*milvuspb.FlushAllTarget{
		{
			DbName:          "db1",
			CollectionNames: []string{}, // Empty means flush all collections in this DB
		},
		{
			DbName:          "db2",
			CollectionNames: []string{"collection3"}, // Explicit collection
		},
	}

	// Mock ShowCollections for db1 (empty CollectionNames case)
	showColResp1 := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1", "collection2"},
		CollectionIds:   []int64{100, 200},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
		return req.DbName == "db1"
	})).Return(showColResp1, nil).Once()

	// No ShowCollections mock needed for db2 since collections are explicitly specified
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).To(func(ctx context.Context, dbName, collectionName string) (UniqueID, error) {
		if collectionName == "collection1" {
			return UniqueID(100), nil
		} else if collectionName == "collection2" {
			return UniqueID(200), nil
		} else if collectionName == "collection3" {
			return UniqueID(300), nil
		}
		return UniqueID(0), errors.New("collection not found")
	}).Build()
	defer mockGetCollectionID.UnPatch()

	targets, err := task.expandFlushCollectionNames(ctx)
	assert.NoError(t, err)
	assert.Len(t, targets, 2)

	// Verify db1 target - should have expanded collection names
	assert.Equal(t, "db1", targets[0].DbName)
	assert.Equal(t, []int64{100, 200}, targets[0].CollectionIds)

	// Verify db2 target - should have explicit collection names
	assert.Equal(t, "db2", targets[1].DbName)
	assert.Equal(t, []int64{300}, targets[1].CollectionIds)
}

func TestFlushAllTask_ExpandFlushCollectionNames_EmptyCollectionNamesError(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Set flush targets with empty collection names
	task.FlushAllRequest.FlushTargets = []*milvuspb.FlushAllTarget{
		{
			DbName:          "db1",
			CollectionNames: []string{}, // Empty means flush all collections in this DB
		},
	}

	// Mock ShowCollections with error
	expectedErr := fmt.Errorf("show collections failed for empty collection names")
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowCollectionsRequest) bool {
		return req.DbName == "db1"
	})).Return(nil, expectedErr).Once()

	targets, err := task.expandFlushCollectionNames(ctx)
	assert.Error(t, err)
	assert.Nil(t, targets)
	assert.Contains(t, err.Error(), "show collections failed for empty collection names")
}

// Test sendManualFlushAllToWal method
func TestFlushAllTask_SendManualFlushAllToWal_Success(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	flushTargets := []*datapb.FlushAllTarget{
		{
			DbName:        "db1",
			CollectionIds: []int64{100, 200},
		},
	}
	flushTs := Timestamp(12345)

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
		if collID == UniqueID(100) {
			return []string{"vchannel1"}, nil
		} else if collID == UniqueID(200) {
			return []string{"vchannel2"}, nil
		}
		return nil, fmt.Errorf("collection not found")
	}).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).To(func(ctx context.Context, collID UniqueID, vchannel string, flushTs Timestamp) ([]int64, error) {
		return []int64{collID * 10}, nil
	}).Build()
	defer mockSendManualFlush.UnPatch()

	result, err := task.sendManualFlushAllToWal(ctx, flushTargets, flushTs)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result, 2)
	assert.Equal(t, []int64{1000}, result[100])
	assert.Equal(t, []int64{2000}, result[200])
}

func TestFlushAllTask_SendManualFlushAllToWal_GetVChannelsError(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	flushTargets := []*datapb.FlushAllTarget{
		{
			DbName:        "db1",
			CollectionIds: []int64{100},
		},
	}
	flushTs := Timestamp(12345)

	// Mock getVChannels with error
	expectedErr := fmt.Errorf("get vchannels failed")
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).Return(nil, expectedErr).Build()
	defer mockGetVChannels.UnPatch()

	result, err := task.sendManualFlushAllToWal(ctx, flushTargets, flushTs)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "get vchannels failed")
}

func TestFlushAllTask_SendManualFlushAllToWal_SendFlushError(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	flushTargets := []*datapb.FlushAllTarget{
		{
			DbName:        "db1",
			CollectionIds: []int64{100},
		},
	}
	flushTs := Timestamp(12345)

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).Return([]string{"vchannel1"}, nil).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL with error
	expectedErr := fmt.Errorf("send flush failed")
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).Return(nil, expectedErr).Build()
	defer mockSendManualFlush.UnPatch()

	result, err := task.sendManualFlushAllToWal(ctx, flushTargets, flushTs)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "send flush failed")
}

// Test new Execute method flow with FlushAll API
func TestFlushAllTask_Execute_FlushAllAPIError(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1"},
		CollectionIds:   []int64{100},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(showColResp, nil).Once()

	// Mock GetCollectionID
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).Return([]string{"vchannel1"}, nil).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).Return([]int64{1001}, nil).Build()
	defer mockSendManualFlush.UnPatch()

	// Mock FlushAll with error
	expectedErr := fmt.Errorf("flush all failed")
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(nil, expectedErr).Once()

	err := task.Execute(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to call flush all to data coordinator")
}

// Edge case tests
func TestFlushAllTask_Execute_SegmentFiltering(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1"},
		CollectionIds:   []int64{100},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(showColResp, nil).Once()

	// Mock GetCollectionID
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).Return([]string{"vchannel1"}, nil).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL - return segments that overlap with flushed segments
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).Return([]int64{1001, 1002, 1003}, nil).Build()
	defer mockSendManualFlush.UnPatch()

	// Mock FlushAll with overlapping flushed segments
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(&datapb.FlushAllResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			FlushResults: []*datapb.FlushResult{
				{
					DbName:          "test_db",
					CollectionName:  "collection1",
					CollectionID:    100,
					FlushSegmentIDs: []int64{1002, 1004}, // 1002 overlaps, 1004 is new
					ChannelCps:      map[string]*msgpb.MsgPosition{},
				},
			},
		}, nil).Once()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)

	// Verify segment filtering - should only contain non-flushed segments
	assert.Len(t, task.result.FlushResults, 1)
	collResult := task.result.FlushResults[0].CollectionResults[0]
	assert.Equal(t, []int64{1001, 1003}, collResult.SegmentIds.Data) // 1002 should be filtered out
	assert.Equal(t, []int64{1002, 1004}, collResult.FlushSegmentIds.Data)
}

func TestFlushAllTask_Execute_EmptyFlushTargets(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Set empty flush targets
	task.FlushAllRequest.FlushTargets = []*milvuspb.FlushAllTarget{}

	// Mock ListDatabases
	listDBResp := &milvuspb.ListDatabasesResponse{
		Status:  merr.Success(),
		DbNames: []string{},
	}
	mixCoord.EXPECT().ListDatabases(mock.Anything, mock.AnythingOfType("*milvuspb.ListDatabasesRequest")).
		Return(listDBResp, nil).Once()

	// Mock FlushAll with empty targets
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.MatchedBy(func(req *datapb.FlushAllRequest) bool {
		return len(req.FlushTargets) == 0
	})).Return(&datapb.FlushAllResponse{
		Status:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		FlushResults: []*datapb.FlushResult{},
	}, nil).Once()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	assert.Len(t, task.result.FlushResults, 0)
}

func TestFlushAllTask_SendManualFlushAllToWal_EmptyTargets(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	flushTargets := []*datapb.FlushAllTarget{}
	flushTs := Timestamp(12345)

	result, err := task.sendManualFlushAllToWal(ctx, flushTargets, flushTs)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result, 0)
}

func TestFlushAllTask_Execute_MultipleCollectionsSameDB(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections with multiple collections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1", "collection2", "collection3"},
		CollectionIds:   []int64{100, 200, 300},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(showColResp, nil).Once()

	// Mock GetCollectionID
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).To(func(cache *MetaCache, ctx context.Context, database, collectionName string) (UniqueID, error) {
		switch collectionName {
		case "collection1":
			return UniqueID(100), nil
		case "collection2":
			return UniqueID(200), nil
		case "collection3":
			return UniqueID(300), nil
		}
		return 0, fmt.Errorf("collection not found")
	}).Build()
	defer mockGetCollectionID.UnPatch()

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).To(func(collID UniqueID) ([]string, error) {
		switch collID {
		case 100:
			return []string{"vchannel1"}, nil
		case 200:
			return []string{"vchannel2"}, nil
		case 300:
			return []string{"vchannel3"}, nil
		}
		return nil, fmt.Errorf("collection not found")
	}).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).To(func(ctx context.Context, collID UniqueID, vchannel string, flushTs Timestamp) ([]int64, error) {
		return []int64{collID * 10}, nil
	}).Build()
	defer mockSendManualFlush.UnPatch()

	// Mock FlushAll with multiple collections in same DB
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.MatchedBy(func(req *datapb.FlushAllRequest) bool {
		return req.DbName == "test_db" && len(req.FlushTargets) == 1 && len(req.FlushTargets[0].CollectionIds) == 3
	})).Return(&datapb.FlushAllResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		FlushResults: []*datapb.FlushResult{
			{
				DbName:          "test_db",
				CollectionName:  "collection1",
				CollectionID:    100,
				FlushSegmentIDs: []int64{2001},
				ChannelCps:      map[string]*msgpb.MsgPosition{},
			},
			{
				DbName:          "test_db",
				CollectionName:  "collection2",
				CollectionID:    200,
				FlushSegmentIDs: []int64{2002},
				ChannelCps:      map[string]*msgpb.MsgPosition{},
			},
			{
				DbName:          "test_db",
				CollectionName:  "collection3",
				CollectionID:    300,
				FlushSegmentIDs: []int64{2003},
				ChannelCps:      map[string]*msgpb.MsgPosition{},
			},
		},
	}, nil).Once()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)

	// Verify all collections are grouped under the same database
	assert.Len(t, task.result.FlushResults, 1)
	assert.Equal(t, "test_db", task.result.FlushResults[0].DbName)
	assert.Len(t, task.result.FlushResults[0].CollectionResults, 3)
}

func TestFlushAllTask_Execute_NilFlushSegmentIDs(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1"},
		CollectionIds:   []int64{100},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(showColResp, nil).Once()

	// Mock GetCollectionID
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).Return([]string{"vchannel1"}, nil).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL - return nil segments
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).Return(nil, nil).Build()
	defer mockSendManualFlush.UnPatch()

	// Mock FlushAll with nil flush segment IDs
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(&datapb.FlushAllResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			FlushResults: []*datapb.FlushResult{
				{
					DbName:          "test_db",
					CollectionName:  "collection1",
					CollectionID:    100,
					FlushSegmentIDs: nil, // Nil flush segment IDs
					ChannelCps:      map[string]*msgpb.MsgPosition{},
				},
			},
		}, nil).Once()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)

	// Verify handling of nil segment IDs
	assert.Len(t, task.result.FlushResults, 1)
	collResult := task.result.FlushResults[0].CollectionResults[0]
	assert.Empty(t, collResult.SegmentIds.Data)
	assert.Empty(t, collResult.FlushSegmentIds.Data)
}

func TestFlushAllTask_Execute_FlushAllAPIResponseError(t *testing.T) {
	task, mixCoord, replicateMsgStream, chMgr, ctx := createTestFlushAllTaskByStreamingService(t, "test_db")
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)
	defer chMgr.AssertExpectations(t)

	// Mock ShowCollections
	showColResp := &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"collection1"},
		CollectionIds:   []int64{100},
	}
	mixCoord.EXPECT().ShowCollections(mock.Anything, mock.AnythingOfType("*milvuspb.ShowCollectionsRequest")).
		Return(showColResp, nil).Once()

	// Mock GetCollectionID
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	// Mock getVChannels
	mockGetVChannels := mockey.Mock(mockey.GetMethod(chMgr, "getVChannels")).Return([]string{"vchannel1"}, nil).Build()
	defer mockGetVChannels.UnPatch()

	// Mock sendManualFlushToWAL
	mockSendManualFlush := mockey.Mock(sendManualFlushToWAL).Return([]int64{1001}, nil).Build()
	defer mockSendManualFlush.UnPatch()

	// Mock FlushAll with error response
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(&datapb.FlushAllResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "internal error"},
		}, nil).Once()

	err := task.Execute(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to call flush all to data coordinator")
}
