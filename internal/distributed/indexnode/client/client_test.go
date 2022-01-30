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
	"errors"
	"testing"

	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

var ParamsGlobal paramtable.GlobalParamTable

func Test_NewClient(t *testing.T) {
	ClientParams.InitOnce(typeutil.IndexNodeRole)
	ctx := context.Background()
	client, err := NewClient(ctx, "")
	assert.Nil(t, client)
	assert.NotNil(t, err)

	client, err = NewClient(ctx, "test")
	assert.Nil(t, err)
	assert.NotNil(t, client)

	err = client.Init()
	assert.Nil(t, err)

	err = client.Start()
	assert.Nil(t, err)

	err = client.Register()
	assert.Nil(t, err)

	checkFunc := func(retNotNil bool) {
		retCheck := func(notNil bool, ret interface{}, err error) {
			if notNil {
				assert.NotNil(t, ret)
				assert.Nil(t, err)
			} else {
				assert.Nil(t, ret)
				assert.NotNil(t, err)
			}
		}

		r1, err := client.GetComponentStates(ctx)
		retCheck(retNotNil, r1, err)

		r2, err := client.GetTimeTickChannel(ctx)
		retCheck(retNotNil, r2, err)

		r3, err := client.GetStatisticsChannel(ctx)
		retCheck(retNotNil, r3, err)

		r4, err := client.CreateIndex(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r5, err)
	}

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) interface{} {
		return &mock.IndexNodeClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) interface{} {
		return &mock.IndexNodeClient{Err: errors.New("dummy")}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc2)
	checkFunc(false)

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) interface{} {
		return &mock.IndexNodeClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc3)
	checkFunc(true)

	err = client.Stop()
	assert.Nil(t, err)
}

func TestIndexNodeClient(t *testing.T) {
	ctx := context.Background()

	ins, err := grpcindexnode.NewServer(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, ins)

	inm := &indexnode.Mock{}
	ParamsGlobal.InitOnce()
	etcdCli, err := etcd.GetEtcdClient(&ParamsGlobal.EtcdCfg)
	assert.NoError(t, err)
	inm.SetEtcdClient(etcdCli)
	err = ins.SetClient(inm)
	assert.Nil(t, err)

	err = ins.Run()
	assert.Nil(t, err)

	inc, err := NewClient(ctx, "localhost:21121")
	assert.Nil(t, err)
	assert.NotNil(t, inc)

	err = inc.Init()
	assert.Nil(t, err)

	err = inc.Start()
	assert.Nil(t, err)

	t.Run("GetComponentStates", func(t *testing.T) {
		states, err := inc.GetComponentStates(ctx)
		assert.Nil(t, err)
		assert.Equal(t, internalpb.StateCode_Healthy, states.State.StateCode)
		assert.Equal(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
	})

	t.Run("GetTimeTickChannel", func(t *testing.T) {
		resp, err := inc.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		resp, err := inc.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			IndexBuildID: 0,
			IndexID:      0,
		}
		resp, err := inc.CreateIndex(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.Nil(t, err)
		resp, err := inc.GetMetrics(ctx, req)
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	err = ins.Stop()
	assert.Nil(t, err)

	err = inc.Stop()
	assert.Nil(t, err)
}
