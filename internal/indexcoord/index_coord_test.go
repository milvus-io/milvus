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
	"math/rand"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/indexnode"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

func TestIndexCoord(t *testing.T) {
	ctx := context.Background()
	Params.InitOnce()
	Params.EtcdCfg.MetaRootPath = "indexcoord-ut"

	// first start an IndexNode
	inm0 := indexnode.NewIndexNodeMock()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
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
	}
	err = ic.SetDataCoord(dcm)
	assert.Nil(t, err)

	ic.SetEtcdClient(etcdCli)

	err = ic.Init()
	assert.NoError(t, err)

	err = ic.Register()
	assert.NoError(t, err)

	err = ic.Start()
	assert.NoError(t, err)

	ic.UpdateStateCode(internalpb.StateCode_Healthy)

	ic.nodeManager.setClient(1, inm0)

	// Test IndexCoord function
	t.Run("GetComponentStates", func(t *testing.T) {
		states, err := ic.GetComponentStates(ctx)
		assert.NoError(t, err)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
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
		err = ic.etcdKV.Save(path.Join(util.FlushedSegmentPrefix, strconv.FormatInt(collID, 10), strconv.FormatInt(partID, 10), strconv.FormatInt(segmentID, 10)), string(strconv.FormatInt(segmentID, 10)))
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
			IndexName:    indexName,
			FieldID:      fieldID,
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

	// Stop IndexCoord
	err = ic.Stop()
	assert.NoError(t, err)

	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	err = etcdKV.RemoveWithPrefix("")
	assert.NoError(t, err)
}

func TestIndexCoord_GetComponentStates(t *testing.T) {
	ic := &IndexCoord{}
	ic.stateCode.Store(internalpb.StateCode_Healthy)
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
	ic.stateCode.Store(internalpb.StateCode_Abnormal)

	// Test IndexCoord function
	t.Run("CreateIndex", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexName:    indexName,
		}
		resp, err := ic.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		req := &indexpb.GetIndexStateRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("GetSegmentIndexState", func(t *testing.T) {
		req := &indexpb.GetSegmentIndexStateRequest{
			CollectionID: collID,
			IndexName:    indexName,
			SegmentIDs:   []UniqueID{segID},
		}
		resp, err := ic.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("GetIndexInfos", func(t *testing.T) {
		req := &indexpb.GetIndexInfoRequest{
			CollectionID: collID,
			SegmentIDs:   []UniqueID{segID},
			IndexName:    indexName,
		}
		resp, err := ic.GetIndexInfos(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		req := &indexpb.DescribeIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		req := &indexpb.GetIndexBuildProgressRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
		resp, err := ic.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("DropIndex", func(t *testing.T) {
		req := &indexpb.DropIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
			FieldID:      fieldID,
		}
		resp, err := ic.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
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
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := ic.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

}

// TODO @xiaocai2333: add ut for error occurred.

//func TestIndexCoord_watchNodeLoop(t *testing.T) {
//	ech := make(chan *sessionutil.SessionEvent)
//	in := &IndexCoord{
//		loopWg:    sync.WaitGroup{},
//		loopCtx:   context.Background(),
//		eventChan: ech,
//		session: &sessionutil.Session{
//			TriggerKill: true,
//			ServerID:    0,
//		},
//	}
//	in.loopWg.Add(1)
//
//	flag := false
//	closed := false
//	sigDone := make(chan struct{}, 1)
//	sigQuit := make(chan struct{}, 1)
//	sc := make(chan os.Signal, 1)
//	signal.Notify(sc, syscall.SIGINT)
//	defer signal.Reset(syscall.SIGINT)
//
//	go func() {
//		in.watchNodeLoop()
//		flag = true
//		sigDone <- struct{}{}
//	}()
//	go func() {
//		<-sc
//		closed = true
//		sigQuit <- struct{}{}
//	}()
//
//	close(ech)
//	<-sigDone
//	<-sigQuit
//	assert.True(t, flag)
//	assert.True(t, closed)
//}
//
//func TestIndexCoord_watchMetaLoop(t *testing.T) {
//	ctx, cancel := context.WithCancel(context.Background())
//	ic := &IndexCoord{
//		loopCtx: ctx,
//		loopWg:  sync.WaitGroup{},
//	}
//
//	watchChan := make(chan clientv3.WatchResponse, 1024)
//
//	client := &mockETCDKV{
//		watchWithRevision: func(s string, i int64) clientv3.WatchChan {
//			return watchChan
//		},
//	}
//	mt := &metaTable{
//		client:            client,
//		indexBuildID2Meta: map[UniqueID]*Meta{},
//		etcdRevision:      0,
//		lock:              sync.RWMutex{},
//	}
//	ic.metaTable = mt
//
//	t.Run("watch chan panic", func(t *testing.T) {
//		ic.loopWg.Add(1)
//		watchChan <- clientv3.WatchResponse{Canceled: true}
//
//		assert.Panics(t, func() {
//			ic.watchMetaLoop()
//		})
//		ic.loopWg.Wait()
//	})
//
//	t.Run("watch chan new meta table panic", func(t *testing.T) {
//		client = &mockETCDKV{
//			watchWithRevision: func(s string, i int64) clientv3.WatchChan {
//				return watchChan
//			},
//			loadWithRevisionAndVersions: func(s string) ([]string, []string, []int64, int64, error) {
//				return []string{}, []string{}, []int64{}, 0, fmt.Errorf("error occurred")
//			},
//		}
//		mt = &metaTable{
//			client:            client,
//			indexBuildID2Meta: map[UniqueID]*Meta{},
//			etcdRevision:      0,
//			lock:              sync.RWMutex{},
//		}
//		ic.metaTable = mt
//		ic.loopWg.Add(1)
//		watchChan <- clientv3.WatchResponse{CompactRevision: 10}
//		assert.Panics(t, func() {
//			ic.watchMetaLoop()
//		})
//		ic.loopWg.Wait()
//	})
//
//	t.Run("watch chan new meta success", func(t *testing.T) {
//		ic.loopWg = sync.WaitGroup{}
//		client = &mockETCDKV{
//			watchWithRevision: func(s string, i int64) clientv3.WatchChan {
//				return watchChan
//			},
//			loadWithRevisionAndVersions: func(s string) ([]string, []string, []int64, int64, error) {
//				return []string{}, []string{}, []int64{}, 0, nil
//			},
//		}
//		mt = &metaTable{
//			client:            client,
//			indexBuildID2Meta: map[UniqueID]*Meta{},
//			etcdRevision:      0,
//			lock:              sync.RWMutex{},
//		}
//		ic.metaTable = mt
//		ic.loopWg.Add(1)
//		watchChan <- clientv3.WatchResponse{CompactRevision: 10}
//		go ic.watchMetaLoop()
//		cancel()
//		ic.loopWg.Wait()
//	})
//}
//
//func TestIndexCoord_GetComponentStates(t *testing.T) {
//	n := &IndexCoord{}
//	n.stateCode.Store(internalpb.StateCode_Healthy)
//	resp, err := n.GetComponentStates(context.Background())
//	assert.NoError(t, err)
//	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//	assert.Equal(t, common.NotRegisteredID, resp.State.NodeID)
//	n.session = &sessionutil.Session{}
//	n.session.UpdateRegistered(true)
//	resp, err = n.GetComponentStates(context.Background())
//	assert.NoError(t, err)
//	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//}
//
//func TestIndexCoord_NotHealthy(t *testing.T) {
//	ic := &IndexCoord{}
//	ic.stateCode.Store(internalpb.StateCode_Abnormal)
//	req := &indexpb.BuildIndexRequest{}
//	resp, err := ic.BuildIndex(context.Background(), req)
//	assert.Error(t, err)
//	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
//
//	req2 := &indexpb.DropIndexRequest{}
//	status, err := ic.DropIndex(context.Background(), req2)
//	assert.Nil(t, err)
//	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
//
//	req3 := &indexpb.GetIndexStatesRequest{}
//	resp2, err := ic.GetIndexStates(context.Background(), req3)
//	assert.Nil(t, err)
//	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp2.Status.ErrorCode)
//
//	req4 := &indexpb.GetIndexFilePathsRequest{
//		IndexBuildIDs: []UniqueID{1, 2},
//	}
//	resp4, err := ic.GetIndexFilePaths(context.Background(), req4)
//	assert.Nil(t, err)
//	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp4.Status.ErrorCode)
//
//	req5 := &indexpb.RemoveIndexRequest{}
//	resp5, err := ic.RemoveIndex(context.Background(), req5)
//	assert.Nil(t, err)
//	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp5.GetErrorCode())
//}
//
//func TestIndexCoord_GetIndexFilePaths(t *testing.T) {
//	ic := &IndexCoord{
//		metaTable: &metaTable{
//			indexBuildID2Meta: map[UniqueID]*Meta{
//				1: {
//					indexMeta: &indexpb.IndexMeta{
//						IndexBuildID:   1,
//						State:          commonpb.IndexState_Finished,
//						IndexFilePaths: []string{"indexFiles-1", "indexFiles-2"},
//					},
//				},
//				2: {
//					indexMeta: &indexpb.IndexMeta{
//						IndexBuildID: 2,
//						State:        commonpb.IndexState_Failed,
//					},
//				},
//			},
//		},
//	}
//
//	ic.stateCode.Store(internalpb.StateCode_Healthy)
//
//	t.Run("GetIndexFilePaths success", func(t *testing.T) {
//		resp, err := ic.GetIndexFilePaths(context.Background(), &indexpb.GetIndexFilePathsRequest{IndexBuildIDs: []UniqueID{1}})
//		assert.NoError(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//		assert.Equal(t, 1, len(resp.FilePaths))
//		assert.ElementsMatch(t, resp.FilePaths[0].IndexFilePaths, []string{"indexFiles-1", "indexFiles-2"})
//	})
//
//	t.Run("GetIndexFilePaths failed", func(t *testing.T) {
//		resp, err := ic.GetIndexFilePaths(context.Background(), &indexpb.GetIndexFilePathsRequest{IndexBuildIDs: []UniqueID{2}})
//		assert.NoError(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//		assert.Equal(t, 0, len(resp.FilePaths[0].IndexFilePaths))
//	})
//
//	t.Run("set DataCoord with nil", func(t *testing.T) {
//		err := ic.SetDataCoord(nil)
//		assert.Error(t, err)
//	})
//}
//
//func Test_tryAcquireSegmentReferLock(t *testing.T) {
//	ic := &IndexCoord{
//		session: &sessionutil.Session{
//			ServerID: 1,
//		},
//	}
//	dcm := &DataCoordMock{
//		Err:  false,
//		Fail: false,
//	}
//	cmm := &ChunkManagerMock{
//		Err:  false,
//		Fail: false,
//	}
//
//	ic.dataCoordClient = dcm
//	ic.chunkManager = cmm
//
//	t.Run("success", func(t *testing.T) {
//		err := ic.tryAcquireSegmentReferLock(context.Background(), 1, 1, []UniqueID{1})
//		assert.Nil(t, err)
//	})
//
//	t.Run("error", func(t *testing.T) {
//		dcmE := &DataCoordMock{
//			Err:  true,
//			Fail: false,
//		}
//		ic.dataCoordClient = dcmE
//		err := ic.tryAcquireSegmentReferLock(context.Background(), 1, 1, []UniqueID{1})
//		assert.Error(t, err)
//	})
//
//	t.Run("Fail", func(t *testing.T) {
//		dcmF := &DataCoordMock{
//			Err:  false,
//			Fail: true,
//		}
//		ic.dataCoordClient = dcmF
//		err := ic.tryAcquireSegmentReferLock(context.Background(), 1, 1, []UniqueID{1})
//		assert.Error(t, err)
//	})
//}
//
//func Test_tryReleaseSegmentReferLock(t *testing.T) {
//	ic := &IndexCoord{
//		session: &sessionutil.Session{
//			ServerID: 1,
//		},
//	}
//	dcm := &DataCoordMock{
//		Err:  false,
//		Fail: false,
//	}
//
//	ic.dataCoordClient = dcm
//
//	t.Run("success", func(t *testing.T) {
//		err := ic.tryReleaseSegmentReferLock(context.Background(), 1, 1)
//		assert.NoError(t, err)
//	})
//}
//
//func TestIndexCoord_RemoveIndex(t *testing.T) {
//	ic := &IndexCoord{
//		metaTable: &metaTable{},
//		indexBuilder: &indexBuilder{
//			notify: make(chan struct{}, 10),
//		},
//	}
//	ic.stateCode.Store(internalpb.StateCode_Healthy)
//	status, err := ic.RemoveIndex(context.Background(), &indexpb.RemoveIndexRequest{BuildIDs: []UniqueID{0}})
//	assert.Nil(t, err)
//	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
//}
