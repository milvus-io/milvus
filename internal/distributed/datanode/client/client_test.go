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

package grpcdatanodeclient

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_NewClient(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	client, err := NewClient(ctx, "", 1)
	assert.Nil(t, client)
	assert.Error(t, err)

	client, err = NewClient(ctx, "test", 2)
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

		r2, err := client.GetStatisticsChannel(ctx, nil)
		retCheck(retNotNil, r2, err)

		r3, err := client.WatchDmChannels(ctx, nil)
		retCheck(retNotNil, r3, err)

		r4, err := client.FlushSegments(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.CompactionV2(ctx, nil)
		retCheck(retNotNil, r6, err)

		r8, err := client.ResendSegmentStats(ctx, nil)
		retCheck(retNotNil, r8, err)

		r10, err := client.ShowConfigurations(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.GetCompactionState(ctx, nil)
		retCheck(retNotNil, r11, err)

		r12, err := client.NotifyChannelOperation(ctx, nil)
		retCheck(retNotNil, r12, err)

		r13, err := client.CheckChannelOperationProgress(ctx, nil)
		retCheck(retNotNil, r13, err)

		r14, err := client.DropCompactionPlan(ctx, nil)
		retCheck(retNotNil, r14, err)
	}

	client.(*Client).grpcClient = &mock.GRPCClientBase[datapb.DataNodeClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) datapb.DataNodeClient {
		return &mock.GrpcDataNodeClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[datapb.DataNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) datapb.DataNodeClient {
		return &mock.GrpcDataNodeClient{Err: errors.New("dummy")}
	}

	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[datapb.DataNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) datapb.DataNodeClient {
		return &mock.GrpcDataNodeClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc3)

	checkFunc(true)

	err = client.Close()
	assert.NoError(t, err)
}
