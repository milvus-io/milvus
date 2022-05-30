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
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

func testIndexCoord(t *testing.T) {
	ctx := context.Background()
	inm0 := &indexnode.Mock{}
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	inm0.SetEtcdClient(etcdCli)
	err = inm0.Init()
	assert.Nil(t, err)
	err = inm0.Register()
	assert.Nil(t, err)
	err = inm0.Start()
	assert.Nil(t, err)
	factory := dependency.NewDefaultFactory(true)
	ic, err := NewIndexCoord(ctx, factory)
	assert.Nil(t, err)
	ic.reqTimeoutInterval = time.Second * 10
	ic.durationInterval = time.Second
	ic.assignTaskInterval = 200 * time.Millisecond
	ic.taskLimit = 20

	ic.SetEtcdClient(etcdCli)
	err = ic.Init()
	assert.Nil(t, err)
	err = ic.Start()
	assert.Nil(t, err)
	err = ic.Register()
	assert.Nil(t, err)

	err = inm0.Stop()
	assert.Nil(t, err)

	t.Run("create index without indexnodes", func(t *testing.T) {
		indexID := int64(rand.Int())
		req := &indexpb.BuildIndexRequest{
			IndexID:   indexID,
			DataPaths: []string{"NoIndexNode-1", "NoIndexNode-2"},
			NumRows:   10,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
			FieldSchema: &schemapb.FieldSchema{
				DataType: schemapb.DataType_FloatVector,
			},
		}
		resp, err := ic.BuildIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		time.Sleep(time.Second)
		status, err := ic.DropIndex(ctx, &indexpb.DropIndexRequest{
			IndexID: indexID,
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
	})

	in, err := grpcindexnode.NewServer(ctx, factory)
	assert.Nil(t, err)
	assert.NotNil(t, in)
	inm := &indexnode.Mock{
		Build:   true,
		Failure: false,
	}

	inm.SetEtcdClient(etcdCli)
	err = in.SetClient(inm)
	assert.Nil(t, err)

	err = in.Run()
	assert.Nil(t, err)

	state, err := ic.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, internalpb.StateCode_Healthy, state.State.StateCode)

	indexID := int64(rand.Int())

	var indexBuildID UniqueID

	t.Run("Create Index", func(t *testing.T) {
		req := &indexpb.BuildIndexRequest{
			IndexID:   indexID,
			DataPaths: []string{"DataPath-1", "DataPath-2"},
			NumRows:   0,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
			FieldSchema: &schemapb.FieldSchema{
				DataType: schemapb.DataType_FloatVector,
			},
		}
		resp, err := ic.BuildIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		indexBuildID = resp.IndexBuildID
		resp2, err := ic.BuildIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, indexBuildID, resp2.IndexBuildID)
		assert.Equal(t, "already have same index", resp2.Status.Reason)

		req2 := &indexpb.BuildIndexRequest{
			IndexID:   indexID,
			DataPaths: []string{"DataPath-3", "DataPath-4"},
			NumRows:   1000,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
			FieldSchema: &schemapb.FieldSchema{
				DataType: schemapb.DataType_FloatVector,
			},
		}
		resp3, err := ic.BuildIndex(ctx, req2)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp3.Status.ErrorCode)
	})

	t.Run("Get Index State", func(t *testing.T) {
		req := &indexpb.GetIndexStatesRequest{
			IndexBuildIDs: []UniqueID{indexBuildID},
		}
		for {
			resp, err := ic.GetIndexStates(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
			if resp.States[0].State == commonpb.IndexState_Finished ||
				resp.States[0].State == commonpb.IndexState_Failed {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("Get IndexFile Paths", func(t *testing.T) {
		req := &indexpb.GetIndexFilePathsRequest{
			IndexBuildIDs: []UniqueID{indexBuildID},
		}
		resp, err := ic.GetIndexFilePaths(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.FilePaths))
		assert.Equal(t, 2, len(resp.FilePaths[0].IndexFilePaths))
		assert.Equal(t, "IndexFilePath-1", resp.FilePaths[0].IndexFilePaths[0])
		assert.Equal(t, "IndexFilePath-2", resp.FilePaths[0].IndexFilePaths[1])
	})

	t.Run("Drop Index", func(t *testing.T) {
		req := &indexpb.DropIndexRequest{
			IndexID: indexID,
		}
		resp, err := ic.DropIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetMetrics, system info", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.Nil(t, err)
		resp, err := ic.GetMetrics(ctx, req)
		assert.Nil(t, err)
		log.Info("GetMetrics, system info",
			zap.String("name", resp.ComponentName),
			zap.String("resp", resp.Response))
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		resp, err := ic.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := ic.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics when indexcoord is not healthy", func(t *testing.T) {
		ic.UpdateStateCode(internalpb.StateCode_Abnormal)
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.Nil(t, err)
		resp, err := ic.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
		ic.UpdateStateCode(internalpb.StateCode_Healthy)
	})

	t.Run("GetMetrics when request is illegal", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType("GetIndexNodeMetrics")
		assert.Nil(t, err)
		resp, err := ic.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	t.Run("Recycle IndexMeta", func(t *testing.T) {
		indexMeta := ic.metaTable.GetIndexMetaByIndexBuildID(indexBuildID)
		for indexMeta != nil {
			log.Info("RecycleIndexMeta", zap.Any("meta", indexMeta))
			indexMeta = ic.metaTable.GetIndexMetaByIndexBuildID(indexBuildID)
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("GetMetrics request without metricType", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "GetIndexCoordMetrics",
		}
		resp, err := ic.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	err = in.Stop()
	assert.Nil(t, err)
	err = ic.Stop()
	assert.Nil(t, err)
}

func TestIndexCoord_DisableActiveStandby(t *testing.T) {
	Params.Init()
	Params.IndexCoordCfg.EnableActiveStandby = false
	testIndexCoord(t)
}

// make sure the main functions work well when EnableActiveStandby=true
func TestIndexCoord_EnableActiveStandby(t *testing.T) {
	Params.Init()
	Params.IndexCoordCfg.EnableActiveStandby = true
	testIndexCoord(t)
}

func TestIndexCoord_watchNodeLoop(t *testing.T) {
	ech := make(chan *sessionutil.SessionEvent)
	in := &IndexCoord{
		loopWg:    sync.WaitGroup{},
		loopCtx:   context.Background(),
		eventChan: ech,
		session: &sessionutil.Session{
			TriggerKill: true,
			ServerID:    0,
		},
	}
	in.loopWg.Add(1)

	flag := false
	closed := false
	sigDone := make(chan struct{}, 1)
	sigQuit := make(chan struct{}, 1)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT)
	defer signal.Reset(syscall.SIGINT)

	go func() {
		in.watchNodeLoop()
		flag = true
		sigDone <- struct{}{}
	}()
	go func() {
		<-sc
		closed = true
		sigQuit <- struct{}{}
	}()

	close(ech)
	<-sigDone
	<-sigQuit
	assert.True(t, flag)
	assert.True(t, closed)
}

func TestIndexCoord_GetComponentStates(t *testing.T) {
	n := &IndexCoord{}
	n.stateCode.Store(internalpb.StateCode_Healthy)
	resp, err := n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, common.NotRegisteredID, resp.State.NodeID)
	n.session = &sessionutil.Session{}
	n.session.UpdateRegistered(true)
	resp, err = n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestIndexCoord_NotHealthy(t *testing.T) {
	ic := &IndexCoord{}
	ic.stateCode.Store(internalpb.StateCode_Abnormal)
	req := &indexpb.BuildIndexRequest{}
	resp, err := ic.BuildIndex(context.Background(), req)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)

	req2 := &indexpb.DropIndexRequest{}
	status, err := ic.DropIndex(context.Background(), req2)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

	req3 := &indexpb.GetIndexStatesRequest{}
	resp2, err := ic.GetIndexStates(context.Background(), req3)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp2.Status.ErrorCode)

	req4 := &indexpb.GetIndexFilePathsRequest{
		IndexBuildIDs: []UniqueID{1, 2},
	}
	resp4, err := ic.GetIndexFilePaths(context.Background(), req4)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp4.Status.ErrorCode)
}

func TestIndexCoord_GetIndexFilePaths(t *testing.T) {
	ic := &IndexCoord{
		metaTable: &metaTable{
			indexBuildID2Meta: map[UniqueID]Meta{
				1: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID:   1,
						State:          commonpb.IndexState_Finished,
						IndexFilePaths: []string{"indexFiles-1", "indexFiles-2"},
					},
				},
				2: {
					indexMeta: &indexpb.IndexMeta{
						IndexBuildID: 2,
						State:        commonpb.IndexState_Failed,
					},
				},
			},
		},
	}

	ic.stateCode.Store(internalpb.StateCode_Healthy)

	t.Run("GetIndexFilePaths success", func(t *testing.T) {
		resp, err := ic.GetIndexFilePaths(context.Background(), &indexpb.GetIndexFilePathsRequest{IndexBuildIDs: []UniqueID{1}})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.FilePaths))
		assert.ElementsMatch(t, resp.FilePaths[0].IndexFilePaths, []string{"indexFiles-1", "indexFiles-2"})
	})

	t.Run("GetIndexFilePaths failed", func(t *testing.T) {
		resp, err := ic.GetIndexFilePaths(context.Background(), &indexpb.GetIndexFilePathsRequest{IndexBuildIDs: []UniqueID{2}})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
		assert.NotEqual(t, "", resp.Status.Reason)
	})
}
