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
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_NewClient(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	client, err := NewClient(ctx, "", 1, false)
	assert.Nil(t, client)
	assert.Error(t, err)

	client, err = NewClient(ctx, "test", 2, false)
	assert.NoError(t, err)
	assert.NotNil(t, client)

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

		r1, err := client.GetComponentStates(ctx, nil)
		retCheck(retNotNil, r1, err)

		r3, err := client.GetStatisticsChannel(ctx, nil)
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

	client.(*Client).grpcClient = &mock.GRPCClientBase[indexpb.IndexNodeClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) indexpb.IndexNodeClient {
		return &mock.GrpcIndexNodeClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[indexpb.IndexNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) indexpb.IndexNodeClient {
		return &mock.GrpcIndexNodeClient{Err: errors.New("dummy")}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc2)
	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[indexpb.IndexNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) indexpb.IndexNodeClient {
		return &mock.GrpcIndexNodeClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc3)
	checkFunc(true)

	err = client.Close()
	assert.NoError(t, err)
}

func TestIndexNodeClient(t *testing.T) {
	inc := &mock.GrpcIndexNodeClient{Err: nil}
	assert.NotNil(t, inc)

	ctx := context.TODO()
	t.Run("GetComponentStates", func(t *testing.T) {
		_, err := inc.GetComponentStates(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		_, err := inc.GetStatisticsChannel(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreatJob", func(t *testing.T) {
		req := &indexpb.CreateJobRequest{
			ClusterID: "0",
			BuildID:   0,
		}
		_, err := inc.CreateJob(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("QueryJob", func(t *testing.T) {
		req := &indexpb.QueryJobsRequest{}
		_, err := inc.QueryJobs(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("DropJob", func(t *testing.T) {
		req := &indexpb.DropJobsRequest{}
		_, err := inc.DropJobs(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		req := &internalpb.ShowConfigurationsRequest{
			Pattern: "",
		}
		_, err := inc.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		_, err = inc.GetMetrics(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("GetJobStats", func(t *testing.T) {
		req := &indexpb.GetJobStatsRequest{}
		_, err := inc.GetJobStats(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("CreateJobV2", func(t *testing.T) {
		req := &indexpb.CreateJobV2Request{}
		_, err := inc.CreateJobV2(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("QueryJobsV2", func(t *testing.T) {
		req := &indexpb.QueryJobsV2Request{}
		_, err := inc.QueryJobsV2(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("DropJobsV2", func(t *testing.T) {
		req := &indexpb.DropJobsV2Request{}
		_, err := inc.DropJobsV2(ctx, req)
		assert.NoError(t, err)
	})

	err := inc.Close()
	assert.NoError(t, err)
}
