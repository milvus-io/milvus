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
	"errors"
	"testing"

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

		r3, err := client.WatchDmChannels(ctx, nil)
		retCheck(retNotNil, r3, err)

		r4, err := client.FlushSegments(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.Compaction(ctx, nil)
		retCheck(retNotNil, r6, err)
	}

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) interface{} {
		return &mock.DataNodeClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) interface{} {
		return &mock.DataNodeClient{Err: errors.New("dummy")}
	}

	client.grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) interface{} {
		return &mock.DataNodeClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc3)

	checkFunc(true)
}
