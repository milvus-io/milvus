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

package grpcproxyclient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/stretchr/testify/assert"
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

		r2, err := client.GetStatisticsChannel(ctx)
		retCheck(retNotNil, r2, err)

		r3, err := client.InvalidateCollectionMetaCache(ctx, nil)
		retCheck(retNotNil, r3, err)

		r4, err := client.ReleaseDQLMessageStream(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.SendSearchResult(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.SendRetrieveResult(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.InvalidateCredentialCache(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.UpdateCredentialCache(ctx, nil)
		retCheck(retNotNil, r8, err)
	}

	client.grpcClient = &mock.GRPCClientBase{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) interface{} {
		return &mock.GrpcProxyClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.grpcClient = &mock.GRPCClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) interface{} {
		return &mock.GrpcProxyClient{Err: errors.New("dummy")}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc2)
	checkFunc(false)

	client.grpcClient = &mock.GRPCClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) interface{} {
		return &mock.GrpcProxyClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc3)

	checkFunc(true)

	// timeout
	timeout := time.Nanosecond
	shortCtx, shortCancel := context.WithTimeout(ctx, timeout)
	defer shortCancel()
	time.Sleep(timeout)

	retCheck := func(ret interface{}, err error) {
		assert.Nil(t, ret)
		assert.NotNil(t, err)
	}

	r1Timeout, err := client.GetComponentStates(shortCtx)
	retCheck(r1Timeout, err)

	r2Timeout, err := client.GetStatisticsChannel(shortCtx)
	retCheck(r2Timeout, err)

	r3Timeout, err := client.InvalidateCollectionMetaCache(shortCtx, nil)
	retCheck(r3Timeout, err)

	r4Timeout, err := client.ReleaseDQLMessageStream(shortCtx, nil)
	retCheck(r4Timeout, err)

	r5Timeout, err := client.SendSearchResult(shortCtx, nil)
	retCheck(r5Timeout, err)

	r6Timeout, err := client.SendRetrieveResult(shortCtx, nil)
	retCheck(r6Timeout, err)

	r7Timeout, err := client.InvalidateCredentialCache(shortCtx, nil)
	retCheck(r7Timeout, err)

	r8Timeout, err := client.UpdateCredentialCache(shortCtx, nil)
	retCheck(r8Timeout, err)

	// cleanup
	err = client.Stop()
	assert.Nil(t, err)
}
