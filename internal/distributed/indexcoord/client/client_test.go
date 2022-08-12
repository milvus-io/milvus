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

package grpcindexcoordclient

import (
	"context"
	"testing"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"

	"github.com/milvus-io/milvus/internal/util/sessionutil"

	"github.com/stretchr/testify/assert"

	grpcindexcoord "github.com/milvus-io/milvus/internal/distributed/indexcoord"
	"github.com/milvus-io/milvus/internal/indexcoord"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestIndexCoordClient(t *testing.T) {
	ClientParams.InitOnce(typeutil.IndexCoordRole)
	ctx := context.Background()
	factory := dependency.NewDefaultFactory(true)
	server, err := grpcindexcoord.NewServer(ctx, factory)
	assert.NoError(t, err)
	icm := indexcoord.NewIndexCoordMock()
	etcdCli, err := etcd.GetEtcdClient(&ClientParams.EtcdCfg)
	assert.NoError(t, err)
	icm.CallRegister = func() error {
		session := sessionutil.NewSession(context.Background(), indexcoord.Params.EtcdCfg.MetaRootPath, etcdCli)
		session.Init(typeutil.IndexCoordRole, indexcoord.Params.IndexCoordCfg.Address, true, false)
		session.Register()
		return err
	}
	icm.CallStop = func() error {
		etcdKV := etcdkv.NewEtcdKV(etcdCli, indexcoord.Params.EtcdCfg.MetaRootPath)
		err = etcdKV.RemoveWithPrefix("session/" + typeutil.IndexCoordRole)
		return err
	}
	err = server.SetClient(icm)
	assert.NoError(t, err)
	dcm := &indexcoord.DataCoordMock{}
	err = server.SetDataCoord(dcm)
	assert.NoError(t, err)

	err = server.Run()
	assert.NoError(t, err)
	//
	//etcdCli, err := etcd.GetEtcdClient(&indexcoord.Params.EtcdCfg)
	//assert.NoError(t, err)
	icc, err := NewClient(ctx, indexcoord.Params.EtcdCfg.MetaRootPath, etcdCli)
	assert.NoError(t, err)
	assert.NotNil(t, icc)

	err = icc.Init()
	assert.NoError(t, err)

	err = icc.Register()
	assert.NoError(t, err)

	err = icc.Start()
	assert.NoError(t, err)

	t.Run("GetComponentStates", func(t *testing.T) {
		states, err := icc.GetComponentStates(ctx)
		assert.NoError(t, err)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
		assert.Equal(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := icc.GetStatisticsChannel(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			CollectionID: 0,
			FieldID:      0,
			IndexName:    "default",
		}
		resp, err := icc.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		req := &indexpb.GetIndexStateRequest{
			CollectionID: 0,
			IndexName:    "index",
		}
		resp, err := icc.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.IndexState_Finished, resp.State)
	})

	t.Run("GetSegmentIndexState", func(t *testing.T) {
		req := &indexpb.GetSegmentIndexStateRequest{
			CollectionID: 1,
			IndexName:    "index",
			SegmentIDs:   []int64{1, 2},
		}
		resp, err := icc.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, len(req.SegmentIDs), len(resp.States))
	})

	t.Run("GetIndexInfos", func(t *testing.T) {
		req := &indexpb.GetIndexInfoRequest{
			CollectionID: 0,
			SegmentIDs:   []int64{0},
			IndexName:    "index",
		}
		resp, err := icc.GetIndexInfos(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, len(req.SegmentIDs), len(resp.FilePaths))
		assert.True(t, resp.EnableIndex)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		req := &indexpb.DescribeIndexRequest{
			CollectionID: 1,
			IndexName:    "",
		}
		resp, err := icc.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resp.IndexInfos))
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		req := &indexpb.GetIndexBuildProgressRequest{
			CollectionID: 1,
			IndexName:    "default",
		}
		resp, err := icc.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, resp.TotalRows, resp.IndexedRows)
	})

	t.Run("DropIndex", func(t *testing.T) {
		req := &indexpb.DropIndexRequest{
			CollectionID: 0,
			IndexName:    "default",
			FieldID:      0,
		}
		resp, err := icc.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		req := &internalpb.ShowConfigurationsRequest{
			Pattern: "",
		}
		resp, err := icc.ShowConfigurations(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "",
		}
		resp, err := icc.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, typeutil.IndexCoordRole, resp.ComponentName)
	})

	err = server.Stop()
	assert.NoError(t, err)

	err = icc.Stop()
	assert.NoError(t, err)
}
