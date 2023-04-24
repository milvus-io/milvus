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

package indexcoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/indexnode"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestMockEtcd(t *testing.T) {
	Params.InitOnce()
	Params.EtcdCfg.MetaRootPath = "indexcoord-mock"

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd,
		Params.EtcdCfg.EtcdUseSSL,
		Params.EtcdCfg.Endpoints,
		Params.EtcdCfg.EtcdTLSCert,
		Params.EtcdCfg.EtcdTLSKey,
		Params.EtcdCfg.EtcdTLSCACert,
		Params.EtcdCfg.EtcdTLSMinVersion)
	assert.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	mockEtcd := NewMockEtcdKVWithReal(etcdKV)
	key := "foo"
	value := "foo-val"
	err = mockEtcd.Save(key, value)
	assert.NoError(t, err)

	fmt.Println(mockEtcd == nil)
	loadVal, err := mockEtcd.Load(key)
	assert.NoError(t, err)
	assert.Equal(t, value, loadVal)

	_, _, err = mockEtcd.LoadWithPrefix(key)
	assert.NoError(t, err)

	_, _, _, err = mockEtcd.LoadWithPrefix2(key)
	assert.NoError(t, err)

	_, _, _, err = mockEtcd.LoadWithRevision(key)
	assert.NoError(t, err)

	_, _, _, _, err = mockEtcd.LoadWithRevisionAndVersions(key)
	assert.NoError(t, err)

	err = mockEtcd.MultiSave(map[string]string{
		"TestMockEtcd-1": "mock-val",
		"TestMockEtcd-2": "mock-val",
	})
	assert.NoError(t, err)

	err = mockEtcd.RemoveWithPrefix("TestMockEtcd-")
	assert.NoError(t, err)

	err = mockEtcd.Remove(key)
	assert.NoError(t, err)

}

func getFlushedSegmentsMock(segments []int64) func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	return func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
		return &datapb.GetFlushedSegmentsResponse{
			Segments: segments,
		}, nil
	}
}

func testIndexCoord(t *testing.T) {
	ctx := context.Background()
	Params.EtcdCfg.MetaRootPath = "indexcoord-ut" + funcutil.RandomString(8)

	// first start an IndexNode
	inm0 := indexnode.NewIndexNodeMock()
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd,
		Params.EtcdCfg.EtcdUseSSL,
		Params.EtcdCfg.Endpoints,
		Params.EtcdCfg.EtcdTLSCert,
		Params.EtcdCfg.EtcdTLSKey,
		Params.EtcdCfg.EtcdTLSCACert,
		Params.EtcdCfg.EtcdTLSMinVersion)
	assert.NoError(t, err)

	// start IndexCoord
	factory := dependency.NewDefaultFactory(true)
	ic, err := NewIndexCoord(ctx, factory)
	assert.NoError(t, err)

	rcm := NewRootCoordMock()
	err = ic.SetRootCoord(rcm)
	assert.NoError(t, err)

	dcm := &DataCoordMock{
		CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
			segmentInfos := make([]*datapb.SegmentInfo, 0)
			for _, segID := range req.SegmentIDs {
				segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
					ID:           segID,
					CollectionID: collID,
					PartitionID:  partID,
					NumOfRows:    10240,
					State:        commonpb.SegmentState_Flushed,
					StartPosition: &internalpb.MsgPosition{
						Timestamp: createTs,
					},
					Binlogs: []*datapb.FieldBinlog{
						{
							FieldID: fieldID,
							Binlogs: []*datapb.Binlog{
								{
									LogPath: "file1",
								},
								{
									LogPath: "file2",
								},
							},
						},
					},
				})
			}
			return &datapb.GetSegmentInfoResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				Infos: segmentInfos,
			}, nil
		},
		CallGetFlushedSegment: func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
			return &datapb.GetFlushedSegmentsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				Segments: []int64{segID},
			}, nil
		},
		CallAcquireSegmentLock: func(ctx context.Context, req *datapb.AcquireSegmentLockRequest) (*commonpb.Status, error) {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, nil
		},
		CallReleaseSegmentLock: func(ctx context.Context, req *datapb.ReleaseSegmentLockRequest) (*commonpb.Status, error) {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, nil
		},
		CallListSegmentsInfo: func(ctx context.Context, req *datapb.ListSegmentsInfoRequest) (*datapb.ListSegmentsInfoResponse, error) {
			segmentInfos := make([]*datapb.SegmentInfo, 0)
			for _, segID := range req.SegmentIDs {
				segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
					ID:           segID,
					CollectionID: collID,
					PartitionID:  partID,
					NumOfRows:    10240,
					State:        commonpb.SegmentState_Flushed,
					StartPosition: &internalpb.MsgPosition{
						Timestamp: createTs,
					},
					Binlogs: []*datapb.FieldBinlog{
						{
							FieldID: fieldID,
							Binlogs: []*datapb.Binlog{
								{
									LogPath: "file1",
								},
								{
									LogPath: "file2",
								},
							},
						},
					},
				})
			}
			return &datapb.ListSegmentsInfoResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				Infos: segmentInfos,
			}, nil
		},
	}
	err = ic.SetDataCoord(dcm)
	assert.Nil(t, err)

	ic.SetEtcdClient(etcdCli)

	err = ic.Init()
	assert.NoError(t, err)
	if Params.IndexCoordCfg.EnableActiveStandby {
		assert.Equal(t, commonpb.StateCode_StandBy, ic.stateCode.Load().(commonpb.StateCode))
	} else {
		assert.Equal(t, commonpb.StateCode_Initializing, ic.stateCode.Load().(commonpb.StateCode))
	}

	err = ic.Register()
	assert.NoError(t, err)

	err = ic.Start()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, ic.stateCode.Load().(commonpb.StateCode))

	mockKv := NewMockEtcdKVWithReal(ic.etcdKV)
	ic.metaTable.catalog = &indexcoord.Catalog{
		Txn: mockKv,
	}
	assert.NoError(t, err)

	ic.nodeManager.setClient(1, inm0)

	// Test IndexCoord function
	t.Run("GetComponentStates", func(t *testing.T) {
		states, err := ic.GetComponentStates(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.StateCode_Healthy, states.State.StateCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := ic.GetStatisticsChannel(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexName:    indexName,
			Timestamp:    createTs,
		}
		resp, err := ic.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		req := &indexpb.GetIndexStateRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.GetIndexState(ctx, req)
		assert.NoError(t, err)
		for resp.State != commonpb.IndexState_Finished {
			resp, err = ic.GetIndexState(ctx, req)
			assert.NoError(t, err)
			time.Sleep(time.Second)
		}
	})

	t.Run("GetSegmentIndexState", func(t *testing.T) {
		req := &indexpb.GetSegmentIndexStateRequest{
			CollectionID: collID,
			IndexName:    indexName,
			SegmentIDs:   []UniqueID{segID},
		}
		resp, err := ic.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, len(req.SegmentIDs), len(resp.States))
	})

	t.Run("GetIndexInfos", func(t *testing.T) {
		req := &indexpb.GetIndexInfoRequest{
			CollectionID: collID,
			SegmentIDs:   []UniqueID{segID},
			IndexName:    indexName,
		}
		resp, err := ic.GetIndexInfos(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, len(req.SegmentIDs), len(resp.SegmentInfo))
	})

	getReq := func() *indexpb.DescribeIndexRequest {
		return &indexpb.DescribeIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
	}

	t.Run("DescribeIndex NotExist", func(t *testing.T) {
		indexs := ic.metaTable.collectionIndexes
		ic.metaTable.collectionIndexes = make(map[UniqueID]map[UniqueID]*model.Index)
		defer func() {
			ic.metaTable.collectionIndexes = indexs
		}()

		resp, err := ic.DescribeIndex(ctx, getReq())
		assert.NoError(t, err)
		assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_IndexNotExist)
	})

	t.Run("DescribeIndex State", func(t *testing.T) {
		req := getReq()
		res := ic.metaTable.GetIndexIDByName(collID, indexName)
		var indexIDTest int64
		for k := range res {
			indexIDTest = k
			break
		}

		updateSegmentIndexes := func(i map[UniqueID]map[UniqueID]*model.SegmentIndex) {
			ic.metaTable.segmentIndexLock.Lock()
			defer ic.metaTable.segmentIndexLock.Unlock()
			ic.metaTable.segmentIndexes = i
		}

		getSegmentIndexes := func() map[UniqueID]map[UniqueID]*model.SegmentIndex {
			ic.metaTable.segmentIndexLock.Lock()
			defer ic.metaTable.segmentIndexLock.Unlock()
			return ic.metaTable.segmentIndexes
		}

		indexs := getSegmentIndexes()
		mockIndexs := make(map[UniqueID]map[UniqueID]*model.SegmentIndex)
		updateMockIndexs := func(f func()) {
			ic.metaTable.segmentIndexLock.Lock()
			defer ic.metaTable.segmentIndexLock.Unlock()
			f()
		}

		progressIndex := &model.SegmentIndex{
			SegmentID:  222,
			IndexState: commonpb.IndexState_InProgress,
		}
		failedIndex := &model.SegmentIndex{
			IndexState: commonpb.IndexState_Failed,
			SegmentID:  333,
			FailReason: "mock fail",
		}
		finishedIndex := &model.SegmentIndex{
			SegmentID:  111,
			IndexState: commonpb.IndexState_Finished,
			NumRows:    2048,
		}

		updateSegmentIndexes(mockIndexs)
		defer func() {
			updateSegmentIndexes(indexs)
		}()
		dcm.CallGetFlushedSegment = getFlushedSegmentsMock([]int64{111})

		updateMockIndexs(func() {
			mockIndexs[111] = make(map[UniqueID]*model.SegmentIndex)
			mockIndexs[111][indexIDTest] = finishedIndex
		})

		resp, err := ic.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, commonpb.IndexState_Finished, resp.IndexInfos[0].State)

		originFunc1 := dcm.CallGetFlushedSegment
		originFunc2 := dcm.CallGetSegmentInfo
		defer func() {
			dcm.CallGetFlushedSegment = originFunc1
			dcm.SetFunc(func() {
				dcm.CallGetSegmentInfo = originFunc2
			})
		}()
		dcm.CallGetFlushedSegment = func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
			return nil, errors.New("mock error")
		}

		updateMockIndexs(func() {
			mockIndexs[222] = make(map[UniqueID]*model.SegmentIndex)
			mockIndexs[222][indexIDTest] = progressIndex
		})
		resp, err = ic.DescribeIndex(ctx, req)
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		dcm.CallGetFlushedSegment = func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
			return &datapb.GetFlushedSegmentsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "mock fail",
				},
			}, nil
		}
		resp, err = ic.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		dcm.CallGetFlushedSegment = getFlushedSegmentsMock([]int64{111, 222, 333})
		dcm.SetFunc(func() {
			dcm.CallListSegmentsInfo = func(ctx context.Context, req *datapb.ListSegmentsInfoRequest) (*datapb.ListSegmentsInfoResponse, error) {
				return nil, errors.New("mock error")
			}
		})
		resp, err = ic.DescribeIndex(ctx, req)
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		dcm.SetFunc(func() {
			dcm.CallListSegmentsInfo = func(ctx context.Context, req *datapb.ListSegmentsInfoRequest) (*datapb.ListSegmentsInfoResponse, error) {
				return &datapb.ListSegmentsInfoResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    "mock fail",
					},
				}, nil
			}
		})
		resp, err = ic.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		dcm.SetFunc(func() {
			dcm.CallListSegmentsInfo = func(ctx context.Context, req *datapb.ListSegmentsInfoRequest) (*datapb.ListSegmentsInfoResponse, error) {
				return &datapb.ListSegmentsInfoResponse{
					Infos: []*datapb.SegmentInfo{
						{ID: 222, State: commonpb.SegmentState_Flushed, NumOfRows: 2048},
						{ID: 333, State: commonpb.SegmentState_Flushed, NumOfRows: 2048},
						{ID: 444, State: commonpb.SegmentState_Flushed, NumOfRows: 2048},
					},
				}, nil
			}
		})
		resp, err = ic.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, commonpb.IndexState_InProgress, resp.IndexInfos[0].State)
		assert.Equal(t, int64(2048), resp.IndexInfos[0].IndexedRows)
		assert.Equal(t, int64(2048*3), resp.IndexInfos[0].TotalRows)

		updateMockIndexs(func() {
			mockIndexs[333] = make(map[UniqueID]*model.SegmentIndex)
			mockIndexs[333][indexIDTest] = failedIndex
		})
		resp, err = ic.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, commonpb.IndexState_Failed, resp.IndexInfos[0].State)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		req := &indexpb.DescribeIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resp.IndexInfos))
	})

	t.Run("FlushedSegmentWatcher", func(t *testing.T) {
		segmentID := segID + 1

		segment := &datapb.SegmentInfo{ID: segID}
		segBytes, err := proto.Marshal(segment)
		if err != nil {
			panic(err)
		}

		err = ic.etcdKV.Save(path.Join(util.FlushedSegmentPrefix, strconv.FormatInt(collID, 10), strconv.FormatInt(partID, 10), strconv.FormatInt(segmentID, 10)), string(segBytes))
		assert.NoError(t, err)

		req := &indexpb.GetSegmentIndexStateRequest{
			CollectionID: collID,
			IndexName:    indexName,
			SegmentIDs:   []UniqueID{segmentID},
		}
		resp, err := ic.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		for len(resp.States) != 1 || resp.States[0].State != commonpb.IndexState_Finished {
			resp, err = ic.GetSegmentIndexState(ctx, req)
			assert.NoError(t, err)
			time.Sleep(time.Second)
		}
	})

	t.Run("FlushedSegmentWatcher backward compatibility testing", func(t *testing.T) {
		segmentID := segID + 2
		err = ic.etcdKV.Save(path.Join(util.FlushedSegmentPrefix, strconv.FormatInt(collID, 10), strconv.FormatInt(partID, 10), strconv.FormatInt(segmentID, 10)), strconv.FormatInt(segmentID, 10))
		assert.NoError(t, err)

		req := &indexpb.GetSegmentIndexStateRequest{
			CollectionID: collID,
			IndexName:    indexName,
			SegmentIDs:   []UniqueID{segmentID},
		}
		resp, err := ic.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		for len(resp.States) != 1 || resp.States[0].State != commonpb.IndexState_Finished {
			resp, err = ic.GetSegmentIndexState(ctx, req)
			assert.NoError(t, err)
			time.Sleep(time.Second)
		}
	})

	t.Run("Showconfigurations, port", func(t *testing.T) {
		pattern := "Port"
		req := &internalpb.ShowConfigurationsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			Pattern: pattern,
		}

		resp, err := ic.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.Configuations))
		assert.Equal(t, "indexcoord.port", resp.Configuations[0].Key)
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		req := &indexpb.GetIndexBuildProgressRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("DropIndex", func(t *testing.T) {
		req := &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: nil,
			IndexName:    indexName,
			DropAll:      false,
		}
		resp, err := ic.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := ic.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("WatchNodeState", func(t *testing.T) {
		allClients := ic.nodeManager.GetAllClients()
		nodeSession := sessionutil.NewSession(context.Background(), Params.EtcdCfg.MetaRootPath, etcdCli)
		originNodeID := Params.IndexCoordCfg.GetNodeID()
		defer func() {
			Params.IndexCoordCfg.SetNodeID(originNodeID)
		}()
		Params.IndexCoordCfg.SetNodeID(100)
		nodeSession.Init(typeutil.IndexNodeRole, "127.0.0.1:11111", false, true)
		nodeSession.Register()

		addNodeChan := make(chan struct{})
		go func() {
			for {
				time.Sleep(200 * time.Millisecond)
				if len(ic.nodeManager.GetAllClients()) > len(allClients) {
					close(addNodeChan)
					break
				}
			}
		}()
		select {
		case <-addNodeChan:
		case <-time.After(10 * time.Second):
			assert.Fail(t, "fail to add node")
		}
		var newNodeID UniqueID = -1
		for id := range ic.nodeManager.GetAllClients() {
			if _, ok := allClients[id]; !ok {
				newNodeID = id
				break
			}
		}

		nodeSession.GoingStop()
		stoppingNodeChan := make(chan struct{})
		go func() {
			for {
				time.Sleep(200 * time.Millisecond)
				ic.nodeManager.lock.RLock()
				_, ok := ic.nodeManager.stoppingNodes[newNodeID]
				ic.nodeManager.lock.RUnlock()
				if ok {
					close(stoppingNodeChan)
					break
				}
			}
		}()
		select {
		case <-stoppingNodeChan:
		case <-time.After(10 * time.Second):
			assert.Fail(t, "fail to stop node")
		}

		nodeSession.Revoke(time.Second)
		deleteNodeChan := make(chan struct{})
		go func() {
			for {
				time.Sleep(200 * time.Millisecond)
				ic.nodeManager.lock.RLock()
				_, ok := ic.nodeManager.stoppingNodes[newNodeID]
				ic.nodeManager.lock.RUnlock()
				if !ok {
					close(deleteNodeChan)
					break
				}
			}
		}()
		select {
		case <-deleteNodeChan:
		case <-time.After(10 * time.Second):
			assert.Fail(t, "fail to stop node")
		}
	})

	// Stop IndexCoord
	err = ic.Stop()
	assert.NoError(t, err)

	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	err = etcdKV.RemoveWithPrefix("")
	assert.NoError(t, err)
}

func TestIndexCoord_DisableActiveStandby(t *testing.T) {
	Params.InitOnce()
	Params.IndexCoordCfg.EnableActiveStandby = false
	testIndexCoord(t)
}

// make sure the main functions work well when EnableActiveStandby=true
func TestIndexCoord_EnableActiveStandby(t *testing.T) {
	Params.InitOnce()
	Params.IndexCoordCfg.EnableActiveStandby = true
	testIndexCoord(t)
}

func TestIndexCoord_GetComponentStates(t *testing.T) {
	ic := &IndexCoord{}
	ic.stateCode.Store(commonpb.StateCode_Healthy)
	resp, err := ic.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, common.NotRegisteredID, resp.State.NodeID)

	ic.session = &sessionutil.Session{}
	ic.session.UpdateRegistered(true)
	resp, err = ic.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestIndexCoord_UnHealthy(t *testing.T) {
	ctx := context.Background()
	ic := &IndexCoord{
		serverID: 1,
	}
	ic.stateCode.Store(commonpb.StateCode_Abnormal)

	// Test IndexCoord function
	t.Run("CreateIndex", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexName:    indexName,
		}
		resp, err := ic.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.ErrorCode)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		req := &indexpb.GetIndexStateRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.Status.ErrorCode)
	})

	t.Run("GetSegmentIndexState", func(t *testing.T) {
		req := &indexpb.GetSegmentIndexStateRequest{
			CollectionID: collID,
			IndexName:    indexName,
			SegmentIDs:   []UniqueID{segID},
		}
		resp, err := ic.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.Status.ErrorCode)
	})

	t.Run("GetIndexInfos", func(t *testing.T) {
		req := &indexpb.GetIndexInfoRequest{
			CollectionID: collID,
			SegmentIDs:   []UniqueID{segID},
			IndexName:    indexName,
		}
		resp, err := ic.GetIndexInfos(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.Status.ErrorCode)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		req := &indexpb.DescribeIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.Status.ErrorCode)
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		req := &indexpb.GetIndexBuildProgressRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.Status.ErrorCode)
	})

	t.Run("DropIndex", func(t *testing.T) {
		req := &indexpb.DropIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.ErrorCode)
	})

	t.Run("ShowConfigurations when indexcoord is not healthy", func(t *testing.T) {
		pattern := ""
		req := &internalpb.ShowConfigurationsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_WatchQueryChannels,
				MsgID:   rand.Int63(),
			},
			Pattern: pattern,
		}

		resp, err := ic.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := ic.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.Status.ErrorCode)
	})

}

func TestIndexCoord_DropIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ic := &IndexCoord{
			metaTable: constructMetaTable(&indexcoord.Catalog{
				Txn: &mockETCDKV{
					multiSave: func(m map[string]string) error {
						return nil
					},
				},
			}),
		}
		ic.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := ic.DropIndex(context.Background(), &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: []int64{partID},
			IndexName:    indexName,
			DropAll:      false,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())

		resp, err = ic.DropIndex(context.Background(), &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: []int64{partID},
			IndexName:    indexName,
			DropAll:      false,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())

		resp, err = ic.DropIndex(context.Background(), &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: nil,
			IndexName:    indexName,
			DropAll:      false,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())

		resp, err = ic.DropIndex(context.Background(), &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: nil,
			IndexName:    indexName,
			DropAll:      false,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("fail", func(t *testing.T) {
		ic := &IndexCoord{
			metaTable: constructMetaTable(&indexcoord.Catalog{
				Txn: &mockETCDKV{
					multiSave: func(m map[string]string) error {
						return errors.New("error")
					},
				},
			}),
		}
		ic.UpdateStateCode(commonpb.StateCode_Healthy)

		resp, err := ic.DropIndex(context.Background(), &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: []int64{partID},
			IndexName:    indexName,
			DropAll:      false,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())

		resp, err = ic.DropIndex(context.Background(), &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: nil,
			IndexName:    indexName,
			DropAll:      false,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("multiple index but no index name", func(t *testing.T) {
		ic := &IndexCoord{
			metaTable: &metaTable{
				catalog:          &indexcoord.Catalog{Txn: NewMockEtcdKV()},
				indexLock:        sync.RWMutex{},
				segmentIndexLock: sync.RWMutex{},
				collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{
					collID: {
						indexID: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID,
							IndexID:         indexID,
							IndexName:       indexName,
							IsDeleted:       false,
							CreateTime:      10,
							TypeParams:      nil,
							IndexParams:     nil,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						indexID + 1: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 1,
							IndexID:         indexID + 1,
							IndexName:       indexName + "1",
							IsDeleted:       false,
							CreateTime:      20,
							TypeParams:      nil,
							IndexParams:     nil,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
					},
				},
				segmentIndexes:       nil,
				buildID2SegmentIndex: nil,
			},
		}
		ic.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := ic.DropIndex(context.Background(), &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: []int64{partID},
			IndexName:    "",
			DropAll:      false,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})
}

func TestIndexCoord_pullSegmentInfo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ic := &IndexCoord{
			dataCoordClient: NewDataCoordMock(),
		}
		info, err := ic.pullSegmentInfo(context.Background(), segID)
		assert.NoError(t, err)
		assert.NotNil(t, info)
	})

	t.Run("fail", func(t *testing.T) {
		ic := &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
					return nil, errors.New("error")
				},
			},
		}
		info, err := ic.pullSegmentInfo(context.Background(), segID)
		assert.Error(t, err)
		assert.Nil(t, info)
	})

	t.Run("not success", func(t *testing.T) {
		ic := &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
					return &datapb.GetSegmentInfoResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_UnexpectedError,
							Reason:    "fail reason",
						},
					}, nil
				},
			},
		}
		info, err := ic.pullSegmentInfo(context.Background(), segID)
		assert.Error(t, err)
		assert.Nil(t, info)
	})

	t.Run("failed to get segment", func(t *testing.T) {
		ic := &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
					return &datapb.GetSegmentInfoResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_UnexpectedError,
							Reason:    msgSegmentNotFound(segID),
						},
					}, nil
				},
			},
		}
		info, err := ic.pullSegmentInfo(context.Background(), segID)
		assert.Error(t, err)
		assert.Nil(t, info)
	})

	t.Run("seg not exist", func(t *testing.T) {
		ic := &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
					return &datapb.GetSegmentInfoResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						Infos: []*datapb.SegmentInfo{},
					}, nil
				},
			},
		}
		info, err := ic.pullSegmentInfo(context.Background(), segID)
		assert.ErrorIs(t, err, ErrSegmentNotFound)
		assert.Nil(t, info)
	})
}

func TestIndexCoord_CheckHealth(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		ic := &IndexCoord{session: &sessionutil.Session{ServerID: 1}}
		ic.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := ic.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})

	t.Run("index node health check is ok", func(t *testing.T) {
		ctx := context.Background()
		indexNode := indexnode.NewIndexNodeMock()
		nm := NewNodeManager(ctx)
		nm.nodeClients[1] = indexNode
		ic := &IndexCoord{
			session:     &sessionutil.Session{ServerID: 1},
			nodeManager: nm,
		}
		ic.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := ic.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Empty(t, resp.Reasons)
	})

	t.Run("index node health check is fail", func(t *testing.T) {
		ctx := context.Background()
		indexNode := indexnode.NewIndexNodeMock()
		indexNode.CallGetComponentStates = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
			return &milvuspb.ComponentStates{
				State: &milvuspb.ComponentInfo{
					NodeID:    1,
					StateCode: commonpb.StateCode_Abnormal,
				},
				SubcomponentStates: nil,
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
			}, nil
		}
		nm := NewNodeManager(ctx)
		nm.nodeClients[1] = indexNode
		ic := &IndexCoord{
			nodeManager: nm,
			session:     &sessionutil.Session{ServerID: 1},
		}
		ic.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := ic.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})
}

func TestIndexCoord_CreateIndex(t *testing.T) {
	ic := &IndexCoord{
		metaTable: &metaTable{
			catalog:          nil,
			indexLock:        sync.RWMutex{},
			segmentIndexLock: sync.RWMutex{},
			collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {
					indexID: {
						TenantID:     "",
						CollectionID: collID,
						FieldID:      fieldID,
						IndexID:      indexID,
						IndexName:    indexName,
						IsDeleted:    false,
						CreateTime:   10,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
						IndexParams: []*commonpb.KeyValuePair{
							{
								Key:   "index_type",
								Value: "IVF_FLAT",
							},
							{
								Key:   "metrics_type",
								Value: "HAMMING",
							},
							{
								Key:   "nlist",
								Value: "128",
							},
						},
						IsAutoIndex:     false,
						UserIndexParams: nil,
					},
				},
			},
			segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
				segID: {
					indexID: {
						SegmentID:     segID,
						CollectionID:  collID,
						PartitionID:   partID,
						NumRows:       1025,
						IndexID:       indexID,
						BuildID:       buildID,
						NodeID:        nodeID,
						IndexVersion:  1,
						IndexState:    commonpb.IndexState_Unissued,
						FailReason:    "",
						IsDeleted:     false,
						CreateTime:    10,
						IndexFileKeys: nil,
						IndexSize:     0,
						WriteHandoff:  false,
					},
				},
			},
			buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
				buildID: {
					SegmentID:     segID,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1025,
					IndexID:       indexID,
					BuildID:       buildID,
					NodeID:        nodeID,
					IndexVersion:  1,
					IndexState:    commonpb.IndexState_Unissued,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    10,
					IndexFileKeys: nil,
					IndexSize:     0,
					WriteHandoff:  false,
				},
			},
		},
	}
	ic.UpdateStateCode(commonpb.StateCode_Healthy)

	t.Run("index already exist, but params are inconsistent", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexName:    indexName,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "index_type",
					Value: "IVF_FLAT",
				},
				{
					Key:   "metrics_type",
					Value: "HAMMING",
				},
				{
					Key:   "params",
					Value: "{nlist: 128}",
				},
			},
			Timestamp:       0,
			IsAutoIndex:     false,
			UserIndexParams: nil,
		}

		resp, err := ic.CreateIndex(context.Background(), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})
}

func TestIndexCoord_GetIndexState(t *testing.T) {
	ic := &IndexCoord{
		metaTable: &metaTable{
			catalog:           &indexcoord.Catalog{Txn: NewMockEtcdKV()},
			collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{},
			segmentIndexes:    map[UniqueID]map[UniqueID]*model.SegmentIndex{},
		},
	}
	ic.stateCode.Store(commonpb.StateCode_Healthy)

	req := &indexpb.GetIndexStateRequest{
		CollectionID: collID,
		IndexName:    "",
	}
	ctx := context.Background()
	t.Run("no index", func(t *testing.T) {
		resp, err := ic.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	err := ic.metaTable.CreateIndex(&model.Index{
		TenantID:        "",
		CollectionID:    collID,
		FieldID:         fieldID,
		IndexID:         indexID,
		IndexName:       "_default_idx_101",
		IsDeleted:       false,
		CreateTime:      0,
		TypeParams:      nil,
		IndexParams:     nil,
		IsAutoIndex:     false,
		UserIndexParams: nil,
	})
	assert.NoError(t, err)
	err = ic.metaTable.CreateIndex(&model.Index{
		TenantID:        "",
		CollectionID:    collID,
		FieldID:         fieldID + 1,
		IndexID:         indexID + 1,
		IndexName:       "_default_idx_102",
		IsDeleted:       false,
		CreateTime:      0,
		TypeParams:      nil,
		IndexParams:     nil,
		IsAutoIndex:     false,
		UserIndexParams: nil,
	})
	assert.NoError(t, err)
	t.Run("multiple indexes", func(t *testing.T) {
		resp, err := ic.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}

func TestIndexCoord_GetIndexBuildProgress(t *testing.T) {
	ic := &IndexCoord{
		metaTable: &metaTable{
			catalog:           &indexcoord.Catalog{Txn: NewMockEtcdKV()},
			collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{},
			segmentIndexes:    map[UniqueID]map[UniqueID]*model.SegmentIndex{},
		},
	}
	ic.stateCode.Store(commonpb.StateCode_Healthy)

	req := &indexpb.GetIndexBuildProgressRequest{
		CollectionID: collID,
		IndexName:    "",
	}
	ctx := context.Background()

	t.Run("no index", func(t *testing.T) {
		resp, err := ic.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	err := ic.metaTable.CreateIndex(&model.Index{
		TenantID:        "",
		CollectionID:    collID,
		FieldID:         fieldID,
		IndexID:         indexID,
		IndexName:       "_default_idx_101",
		IsDeleted:       false,
		CreateTime:      0,
		TypeParams:      nil,
		IndexParams:     nil,
		IsAutoIndex:     false,
		UserIndexParams: nil,
	})
	assert.NoError(t, err)
	err = ic.metaTable.CreateIndex(&model.Index{
		TenantID:        "",
		CollectionID:    collID,
		FieldID:         fieldID + 1,
		IndexID:         indexID + 1,
		IndexName:       "_default_idx_102",
		IsDeleted:       false,
		CreateTime:      0,
		TypeParams:      nil,
		IndexParams:     nil,
		IsAutoIndex:     false,
		UserIndexParams: nil,
	})
	assert.NoError(t, err)

	t.Run("multiple indexes", func(t *testing.T) {
		resp, err := ic.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}
