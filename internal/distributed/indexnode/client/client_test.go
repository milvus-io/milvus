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

package grpcindexnodeclient

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/mock"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_NewClient(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	client, err := NewClient(ctx, "", false)
	assert.Nil(t, client)
	assert.Error(t, err)

	client, err = NewClient(ctx, "test", false)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	err = client.Init()
	assert.NoError(t, err)

	err = client.Start()
	assert.NoError(t, err)

	err = client.Register()
	assert.NoError(t, err)

	checkFunc := func(retNotNil bool) {
		retCheck := func(notNil bool, ret interface{}, err error) {
			if notNil {
				assert.NotNil(t, ret)
				assert.NoError(t, err)
			} else {
				assert.Nil(t, ret)
				assert.Error(t, err)
			}
		}

		r1, err := client.GetComponentStates(ctx)
		retCheck(retNotNil, r1, err)

		r3, err := client.GetStatisticsChannel(ctx)
		retCheck(retNotNil, r3, err)

		r4, err := client.CreateJob(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.QueryJobs(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.DropJobs(ctx, nil)
		retCheck(retNotNil, r7, err)
	}

	client.grpcClient = &mock.GRPCClientBase[indexpb.IndexNodeClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) indexpb.IndexNodeClient {
		return &mock.GrpcIndexNodeClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.grpcClient = &mock.GRPCClientBase[indexpb.IndexNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) indexpb.IndexNodeClient {
		return &mock.GrpcIndexNodeClient{Err: errors.New("dummy")}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc2)
	checkFunc(false)

	client.grpcClient = &mock.GRPCClientBase[indexpb.IndexNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) indexpb.IndexNodeClient {
		return &mock.GrpcIndexNodeClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc3)
	checkFunc(true)

	err = client.Stop()
	assert.NoError(t, err)
}

func TestIndexNodeClient(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	factory := dependency.NewDefaultFactory(true)
	ins, err := grpcindexnode.NewServer(ctx, factory)
	assert.NoError(t, err)
	assert.NotNil(t, ins)

	inm := indexnode.NewIndexNodeMock()
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	inm.SetEtcdClient(etcdCli)
	err = ins.SetClient(inm)
	assert.NoError(t, err)

	err = ins.Run()
	assert.NoError(t, err)

	inc, err := NewClient(ctx, "localhost:21121", false)
	assert.NoError(t, err)
	assert.NotNil(t, inc)

	err = inc.Init()
	assert.NoError(t, err)

	err = inc.Start()
	assert.NoError(t, err)

	t.Run("GetComponentStates", func(t *testing.T) {
		states, err := inc.GetComponentStates(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.StateCode_Healthy, states.State.StateCode)
		assert.Equal(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := inc.GetStatisticsChannel(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreatJob", func(t *testing.T) {
		req := &indexpb.CreateJobRequest{
			ClusterID: "0",
			BuildID:   0,
		}
		resp, err := inc.CreateJob(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("QueryJob", func(t *testing.T) {
		req := &indexpb.QueryJobsRequest{}
		resp, err := inc.QueryJobs(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("DropJob", func(t *testing.T) {
		req := &indexpb.DropJobsRequest{}
		resp, err := inc.DropJobs(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		req := &internalpb.ShowConfigurationsRequest{
			Pattern: "",
		}
		resp, err := inc.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := inc.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetJobStats", func(t *testing.T) {
		req := &indexpb.GetJobStatsRequest{}
		resp, err := inc.GetJobStats(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = ins.Stop()
	assert.NoError(t, err)

	err = inc.Stop()
	assert.NoError(t, err)
}
