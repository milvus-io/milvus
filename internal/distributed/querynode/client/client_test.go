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
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func Test_NewClient(t *testing.T) {
	proxy.Params.InitOnce()

	ctx := context.Background()
	client, err := NewClient(ctx, "")
	assert.Nil(t, client)
	assert.NotNil(t, err)

	client, err = NewClient(ctx, "test")
	assert.Nil(t, err)
	assert.NotNil(t, client)

	ClientParams.InitOnce(typeutil.QueryNodeRole)

	err = client.Start()
	assert.Nil(t, err)

	err = client.Register()
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(ctx)

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

		r4, err := client.AddQueryChannel(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.RemoveQueryChannel(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.WatchDmChannels(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.LoadSegments(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.ReleaseCollection(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.ReleasePartitions(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.ReleaseSegments(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.GetSegmentInfo(ctx, nil)
		retCheck(retNotNil, r11, err)

		r12, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r12, err)

		r13, err := client.WatchDeltaChannels(ctx, nil)
		retCheck(retNotNil, r13, err)

		r14, err := client.Search(ctx, nil)
		retCheck(retNotNil, r14, err)

		r15, err := client.Query(ctx, nil)
		retCheck(retNotNil, r15, err)
	}

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) interface{} {
		return &mock.QueryNodeClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) interface{} {
		return &mock.QueryNodeClient{Err: errors.New("dummy")}
	}

	client.grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) interface{} {
		return &mock.QueryNodeClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc3)

	checkFunc(true)

	// ctx canceled
	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: nil,
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc1)
	cancel() // make context canceled
	checkFunc(false)

	err = client.Stop()
	assert.Nil(t, err)
}
