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

//import (
//	"context"
//	"testing"
//
//	"github.com/milvus-io/milvus/internal/proto/datapb"
//
//	"github.com/milvus-io/milvus/internal/proto/commonpb"
//	"github.com/milvus-io/milvus/internal/proto/indexpb"
//	"github.com/milvus-io/milvus/internal/proto/internalpb"
//	"github.com/milvus-io/milvus/internal/proto/milvuspb"
//	"github.com/milvus-io/milvus/internal/util/etcd"
//	"github.com/stretchr/testify/assert"
//)
//
//func TestIndexCoordMock(t *testing.T) {
//	Params.Init()
//	icm := Mock{}
//
//	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
//	assert.Nil(t, err)
//	icm.SetEtcdClient(etcdCli)
//	defer etcdCli.Close()
//	err = icm.Register()
//	assert.Nil(t, err)
//	err = icm.Init()
//	assert.Nil(t, err)
//	err = icm.Start()
//	assert.Nil(t, err)
//	ctx := context.Background()
//
//	t.Run("Register", func(t *testing.T) {
//
//	})
//	t.Run("GetComponentStates", func(t *testing.T) {
//		states, err := icm.GetComponentStates(ctx)
//		assert.Nil(t, err)
//		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
//	})
//
//	t.Run("GetTimeTickChannel", func(t *testing.T) {
//		resp, err := icm.GetTimeTickChannel(ctx)
//		assert.Nil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//	})
//
//	t.Run("GetStatisticsChannel", func(t *testing.T) {
//		resp, err := icm.GetStatisticsChannel(ctx)
//		assert.Nil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//	})
//
//	t.Run("BuildIndex", func(t *testing.T) {
//		req := &indexpb.BuildIndexRequest{
//			IndexBuildID: 0,
//			IndexID:      0,
//			DataPaths:    []string{},
//		}
//		resp, err := icm.BuildIndex(ctx, req)
//		assert.Nil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//	})
//
//	t.Run("GetIndexStates", func(t *testing.T) {
//		req := &indexpb.GetIndexStatesRequest{
//			IndexBuildIDs: []UniqueID{0},
//		}
//		resp, err := icm.GetIndexStates(ctx, req)
//		assert.Nil(t, err)
//		assert.Equal(t, len(req.IndexBuildIDs), len(resp.States))
//		assert.Equal(t, commonpb.IndexState_Finished, resp.States[0].State)
//	})
//
//	t.Run("DropIndex", func(t *testing.T) {
//		req := &indexpb.DropIndexRequest{
//			IndexID: 0,
//		}
//		resp, err := icm.DropIndex(ctx, req)
//		assert.Nil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
//	})
//
//	t.Run("GetIndexFilePaths", func(t *testing.T) {
//		req := &indexpb.GetIndexFilePathsRequest{
//			IndexBuildIDs: []UniqueID{0, 1},
//		}
//		resp, err := icm.GetIndexFilePaths(ctx, req)
//		assert.Nil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//		assert.Equal(t, len(req.IndexBuildIDs), len(resp.FilePaths))
//	})
//
//	t.Run("ShowConfigurations", func(t *testing.T) {
//		req := &internalpb.ShowConfigurationsRequest{
//			Pattern: "",
//		}
//		resp, err := icm.ShowConfigurations(ctx, req)
//		assert.Nil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//	})
//
//	t.Run("GetMetrics", func(t *testing.T) {
//		req := &milvuspb.GetMetricsRequest{
//			Request: "",
//		}
//		resp, err := icm.GetMetrics(ctx, req)
//		assert.Nil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
//		assert.Equal(t, "IndexCoord", resp.ComponentName)
//	})
//
//	t.Run("RemoveIndex", func(t *testing.T) {
//		req := &indexpb.RemoveIndexRequest{
//			BuildIDs: []UniqueID{},
//		}
//		status, err := icm.RemoveIndex(ctx, req)
//		assert.Nil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
//	})
//
//	err = icm.Stop()
//	assert.Nil(t, err)
//}
//
//func TestIndexCoordMockError(t *testing.T) {
//	icm := Mock{
//		Failure: true,
//	}
//	err := icm.Init()
//	assert.NotNil(t, err)
//	err = icm.Start()
//	assert.NotNil(t, err)
//	ctx := context.Background()
//
//	t.Run("Register", func(t *testing.T) {
//		err = icm.Register()
//		assert.NotNil(t, err)
//	})
//	t.Run("GetComponentStates", func(t *testing.T) {
//		states, err := icm.GetComponentStates(ctx)
//		assert.NotNil(t, err)
//		assert.Equal(t, internalpb.StateCode_Abnormal, states.State.StateCode)
//	})
//
//	t.Run("GetTimeTickChannel", func(t *testing.T) {
//		resp, err := icm.GetTimeTickChannel(ctx)
//		assert.NotNil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
//	})
//
//	t.Run("GetStatisticsChannel", func(t *testing.T) {
//		resp, err := icm.GetStatisticsChannel(ctx)
//		assert.NotNil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
//	})
//
//	t.Run("BuildIndex", func(t *testing.T) {
//		req := &indexpb.BuildIndexRequest{
//			IndexBuildID: 0,
//			IndexID:      0,
//			DataPaths:    []string{},
//		}
//		resp, err := icm.BuildIndex(ctx, req)
//		assert.NotNil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
//	})
//
//	t.Run("GetIndexStates", func(t *testing.T) {
//		req := &indexpb.GetIndexStatesRequest{
//			IndexBuildIDs: []UniqueID{0},
//		}
//		resp, err := icm.GetIndexStates(ctx, req)
//		assert.NotNil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
//	})
//
//	t.Run("DropIndex", func(t *testing.T) {
//		req := &indexpb.DropIndexRequest{
//			IndexID: 0,
//		}
//		resp, err := icm.DropIndex(ctx, req)
//		assert.NotNil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
//	})
//
//	t.Run("GetIndexFilePaths", func(t *testing.T) {
//		req := &indexpb.GetIndexFilePathsRequest{
//			IndexBuildIDs: []UniqueID{0, 1},
//		}
//		resp, err := icm.GetIndexFilePaths(ctx, req)
//		assert.NotNil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
//	})
//
//	t.Run("GetMetrics", func(t *testing.T) {
//		req := &milvuspb.GetMetricsRequest{
//			Request: "",
//		}
//		resp, err := icm.GetMetrics(ctx, req)
//		assert.NotNil(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
//	})
//
//	t.Run("RemoveIndex", func(t *testing.T) {
//		req := &indexpb.RemoveIndexRequest{
//			BuildIDs: []UniqueID{},
//		}
//		status, err := icm.RemoveIndex(ctx, req)
//		assert.Error(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
//	})
//
//	err = icm.Stop()
//	assert.NotNil(t, err)
//}
//
//func TestDataCoordMock_Error(t *testing.T) {
//	t.Run("Init", func(t *testing.T) {
//		dcm := &DataCoordMock{
//			Fail: true,
//			Err:  true,
//		}
//		err := dcm.Init()
//		assert.Error(t, err)
//	})
//
//	t.Run("Start", func(t *testing.T) {
//		dcm := &DataCoordMock{
//			Fail: true,
//			Err:  true,
//		}
//		err := dcm.Start()
//		assert.Error(t, err)
//	})
//
//	t.Run("GetComponentStates", func(t *testing.T) {
//		dcm := &DataCoordMock{
//			Fail: true,
//			Err:  true,
//		}
//		resp, err := dcm.GetComponentStates(context.TODO())
//		assert.Error(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.GetErrorCode())
//
//		dcm.Err = false
//		resp, err = dcm.GetComponentStates(context.TODO())
//		assert.NoError(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.GetErrorCode())
//	})
//
//	t.Run("AcquireSegmentLock", func(t *testing.T) {
//		dcm := &DataCoordMock{
//			Fail: true,
//			Err:  true,
//		}
//		resp, err := dcm.AcquireSegmentLock(context.TODO(), &datapb.AcquireSegmentLockRequest{})
//		assert.Error(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
//
//		dcm.Err = false
//		resp, err = dcm.AcquireSegmentLock(context.TODO(), &datapb.AcquireSegmentLockRequest{})
//		assert.NoError(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
//	})
//
//	t.Run("ReleaseSegmentLock", func(t *testing.T) {
//		dcm := &DataCoordMock{
//			Fail: true,
//			Err:  true,
//		}
//
//		resp, err := dcm.ReleaseSegmentLock(context.TODO(), &datapb.ReleaseSegmentLockRequest{})
//		assert.Error(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
//
//		dcm.Err = false
//		resp, err = dcm.ReleaseSegmentLock(context.TODO(), &datapb.ReleaseSegmentLockRequest{})
//		assert.NoError(t, err)
//		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
//	})
//}
//
//func TestChunkManageMock_Error(t *testing.T) {
//	t.Run("Exist", func(t *testing.T) {
//		cmm := &ChunkManagerMock{
//			Fail: true,
//			Err:  true,
//		}
//		ok, err := cmm.Exist("")
//		assert.Error(t, err)
//		assert.False(t, ok)
//
//		cmm.Err = false
//		ok, err = cmm.Exist("")
//		assert.NoError(t, err)
//		assert.False(t, ok)
//
//		cmm.Fail = false
//		ok, err = cmm.Exist("")
//		assert.NoError(t, err)
//		assert.True(t, ok)
//	})
//	t.Run("RemoveWithPrefix", func(t *testing.T) {
//		cmm := &ChunkManagerMock{
//			Fail: true,
//			Err:  true,
//		}
//
//		err := cmm.RemoveWithPrefix("")
//		assert.Error(t, err)
//
//		cmm.Err = false
//		err = cmm.RemoveWithPrefix("")
//		assert.NoError(t, err)
//
//		cmm.Fail = false
//		err = cmm.RemoveWithPrefix("")
//		assert.NoError(t, err)
//	})
//}
