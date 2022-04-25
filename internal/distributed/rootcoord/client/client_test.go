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

package grpcrootcoordclient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/assert"
)

func Test_NewClient(t *testing.T) {
	proxy.Params.InitOnce()

	ctx := context.Background()
	etcdCli, err := etcd.GetEtcdClient(&proxy.Params.EtcdCfg)
	assert.NoError(t, err)
	client, err := NewClient(ctx, proxy.Params.EtcdCfg.MetaRootPath, etcdCli)
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

		r4, err := client.CreateCollection(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.DropCollection(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.HasCollection(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.DescribeCollection(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.ShowCollections(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.CreatePartition(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.DropPartition(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.HasPartition(ctx, nil)
		retCheck(retNotNil, r11, err)

		r12, err := client.ShowPartitions(ctx, nil)
		retCheck(retNotNil, r12, err)

		r13, err := client.CreateIndex(ctx, nil)
		retCheck(retNotNil, r13, err)

		r14, err := client.DropIndex(ctx, nil)
		retCheck(retNotNil, r14, err)

		r15, err := client.DescribeIndex(ctx, nil)
		retCheck(retNotNil, r15, err)

		r16, err := client.AllocTimestamp(ctx, nil)
		retCheck(retNotNil, r16, err)

		r17, err := client.AllocID(ctx, nil)
		retCheck(retNotNil, r17, err)

		r18, err := client.UpdateChannelTimeTick(ctx, nil)
		retCheck(retNotNil, r18, err)

		r19, err := client.DescribeSegment(ctx, nil)
		retCheck(retNotNil, r19, err)

		r20, err := client.ShowSegments(ctx, nil)
		retCheck(retNotNil, r20, err)

		r21, err := client.ReleaseDQLMessageStream(ctx, nil)
		retCheck(retNotNil, r21, err)

		r22, err := client.SegmentFlushCompleted(ctx, nil)
		retCheck(retNotNil, r22, err)

		r23, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r23, err)

		r24, err := client.CreateAlias(ctx, nil)
		retCheck(retNotNil, r24, err)

		r25, err := client.DropAlias(ctx, nil)
		retCheck(retNotNil, r25, err)

		r26, err := client.AlterAlias(ctx, nil)
		retCheck(retNotNil, r26, err)

		r27, err := client.Import(ctx, nil)
		retCheck(retNotNil, r27, err)

		r28, err := client.GetImportState(ctx, nil)
		retCheck(retNotNil, r28, err)

		r29, err := client.ReportImport(ctx, nil)
		retCheck(retNotNil, r29, err)

		r30, err := client.CreateCredential(ctx, nil)
		retCheck(retNotNil, r30, err)

		r31, err := client.GetCredential(ctx, nil)
		retCheck(retNotNil, r31, err)

		r32, err := client.UpdateCredential(ctx, nil)
		retCheck(retNotNil, r32, err)

		r33, err := client.DeleteCredential(ctx, nil)
		retCheck(retNotNil, r33, err)

		r34, err := client.ListCredUsers(ctx, nil)
		retCheck(retNotNil, r34, err)
	}

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) interface{} {
		return &mock.RootCoordClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) interface{} {
		return &mock.RootCoordClient{Err: errors.New("dummy")}
	}

	client.grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.grpcClient = &mock.ClientBase{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) interface{} {
		return &mock.RootCoordClient{Err: nil}
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

	r2Timeout, err := client.GetTimeTickChannel(shortCtx)
	retCheck(r2Timeout, err)

	r3Timeout, err := client.GetStatisticsChannel(shortCtx)
	retCheck(r3Timeout, err)

	r4Timeout, err := client.CreateCollection(shortCtx, nil)
	retCheck(r4Timeout, err)

	r5Timeout, err := client.DropCollection(shortCtx, nil)
	retCheck(r5Timeout, err)

	r6Timeout, err := client.HasCollection(shortCtx, nil)
	retCheck(r6Timeout, err)

	r7Timeout, err := client.DescribeCollection(shortCtx, nil)
	retCheck(r7Timeout, err)

	r8Timeout, err := client.ShowCollections(shortCtx, nil)
	retCheck(r8Timeout, err)

	r9Timeout, err := client.CreatePartition(shortCtx, nil)
	retCheck(r9Timeout, err)

	r10Timeout, err := client.DropPartition(shortCtx, nil)
	retCheck(r10Timeout, err)

	r11Timeout, err := client.HasPartition(shortCtx, nil)
	retCheck(r11Timeout, err)

	r12Timeout, err := client.ShowPartitions(shortCtx, nil)
	retCheck(r12Timeout, err)

	r13Timeout, err := client.CreateIndex(shortCtx, nil)
	retCheck(r13Timeout, err)

	r14Timeout, err := client.DropIndex(shortCtx, nil)
	retCheck(r14Timeout, err)

	r15Timeout, err := client.DescribeIndex(shortCtx, nil)
	retCheck(r15Timeout, err)

	r16Timeout, err := client.AllocTimestamp(shortCtx, nil)
	retCheck(r16Timeout, err)

	r17Timeout, err := client.AllocID(shortCtx, nil)
	retCheck(r17Timeout, err)

	r18Timeout, err := client.UpdateChannelTimeTick(shortCtx, nil)
	retCheck(r18Timeout, err)

	r19Timeout, err := client.DescribeSegment(shortCtx, nil)
	retCheck(r19Timeout, err)

	r20Timeout, err := client.ShowSegments(shortCtx, nil)
	retCheck(r20Timeout, err)

	r21Timeout, err := client.ReleaseDQLMessageStream(shortCtx, nil)
	retCheck(r21Timeout, err)

	r22Timeout, err := client.SegmentFlushCompleted(shortCtx, nil)
	retCheck(r22Timeout, err)

	r23Timeout, err := client.GetMetrics(shortCtx, nil)
	retCheck(r23Timeout, err)

	r24Timeout, err := client.CreateAlias(shortCtx, nil)
	retCheck(r24Timeout, err)

	r25Timeout, err := client.DropAlias(shortCtx, nil)
	retCheck(r25Timeout, err)

	r26Timeout, err := client.AlterAlias(shortCtx, nil)
	retCheck(r26Timeout, err)

	r27Timeout, err := client.Import(shortCtx, nil)
	retCheck(r27Timeout, err)

	r28Timeout, err := client.GetImportState(shortCtx, nil)
	retCheck(r28Timeout, err)

	r29Timeout, err := client.ReportImport(shortCtx, nil)
	retCheck(r29Timeout, err)

	r30Timeout, err := client.CreateCredential(shortCtx, nil)
	retCheck(r30Timeout, err)

	r31Timeout, err := client.GetCredential(shortCtx, nil)
	retCheck(r31Timeout, err)

	r32Timeout, err := client.UpdateCredential(shortCtx, nil)
	retCheck(r32Timeout, err)

	r33Timeout, err := client.DeleteCredential(shortCtx, nil)
	retCheck(r33Timeout, err)

	r34Timeout, err := client.ListCredUsers(shortCtx, nil)
	retCheck(r34Timeout, err)

	r35Timeout, err := client.ListImportTasks(shortCtx, nil)
	retCheck(r35Timeout, err)

	// clean up
	err = client.Stop()
	assert.Nil(t, err)
}
