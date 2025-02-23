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

package grpcquerynodeclient

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

	ctx, cancel := context.WithCancel(ctx)

	checkFunc := func(retNotNil bool) {
		retCheck := func(notNil bool, ret any, err error) {
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

		r2, err := client.GetTimeTickChannel(ctx, nil)
		retCheck(retNotNil, r2, err)

		r3, err := client.GetStatisticsChannel(ctx, nil)
		retCheck(retNotNil, r3, err)

		r6, err := client.WatchDmChannels(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.LoadSegments(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.ReleaseCollection(ctx, nil)
		retCheck(retNotNil, r8, err)

		r8, err = client.LoadPartitions(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.ReleasePartitions(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.ReleaseSegments(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.GetSegmentInfo(ctx, nil)
		retCheck(retNotNil, r11, err)

		r12, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r12, err)

		r14, err := client.Search(ctx, nil)
		retCheck(retNotNil, r14, err)

		r15, err := client.Query(ctx, nil)
		retCheck(retNotNil, r15, err)

		r16, err := client.SyncReplicaSegments(ctx, nil)
		retCheck(retNotNil, r16, err)

		r17, err := client.GetStatistics(ctx, nil)
		retCheck(retNotNil, r17, err)

		r18, err := client.ShowConfigurations(ctx, nil)
		retCheck(retNotNil, r18, err)

		r19, err := client.QuerySegments(ctx, nil)
		retCheck(retNotNil, r19, err)

		r20, err := client.SearchSegments(ctx, nil)
		retCheck(retNotNil, r20, err)

		r21, err := client.DeleteBatch(ctx, nil)
		retCheck(retNotNil, r21, err)

		// stream rpc
		client, err := client.QueryStream(ctx, nil)
		retCheck(retNotNil, client, err)
	}

	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryNodeClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) querypb.QueryNodeClient {
		return &mock.GrpcQueryNodeClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) querypb.QueryNodeClient {
		return &mock.GrpcQueryNodeClient{Err: errors.New("dummy")}
	}

	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryNodeClient]{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) querypb.QueryNodeClient {
		return &mock.GrpcQueryNodeClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc3)

	checkFunc(true)

	// ctx canceled
	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryNodeClient]{
		GetGrpcClientErr: nil,
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)
	cancel() // make context canceled
	checkFunc(false)

	err = client.Close()
	assert.NoError(t, err)
}
