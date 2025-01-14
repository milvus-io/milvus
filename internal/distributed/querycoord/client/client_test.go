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

package grpcquerycoordclient

import (
	"context"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestMain(m *testing.M) {
	// init embed etcd
	embedetcdServer, tempDir, err := etcd.StartTestEmbedEtcdServer()
	if err != nil {
		log.Fatal("failed to start embed etcd server", zap.Error(err))
	}
	defer os.RemoveAll(tempDir)
	defer embedetcdServer.Close()

	addrs := etcd.GetEmbedEtcdEndpoints(embedetcdServer)

	paramtable.Init()
	paramtable.Get().Save(Params.EtcdCfg.Endpoints.Key, strings.Join(addrs, ","))

	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}

func Test_NewClient(t *testing.T) {
	ctx := context.Background()

	client, err := NewClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)

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

		r4, err := client.ShowCollections(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.ShowPartitions(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.LoadPartitions(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.ReleasePartitions(ctx, nil)
		retCheck(retNotNil, r7, err)

		r7, err = client.SyncNewCreatedPartition(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.ShowCollections(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.LoadCollection(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.ReleaseCollection(ctx, nil)
		retCheck(retNotNil, r10, err)

		r12, err := client.ShowPartitions(ctx, nil)
		retCheck(retNotNil, r12, err)

		r13, err := client.GetPartitionStates(ctx, nil)
		retCheck(retNotNil, r13, err)

		r14, err := client.GetSegmentInfo(ctx, nil)
		retCheck(retNotNil, r14, err)

		r15, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r15, err)

		r16, err := client.LoadBalance(ctx, nil)
		retCheck(retNotNil, r16, err)

		r17, err := client.GetReplicas(ctx, nil)
		retCheck(retNotNil, r17, err)

		r18, err := client.GetShardLeaders(ctx, nil)
		retCheck(retNotNil, r18, err)

		r19, err := client.ShowConfigurations(ctx, nil)
		retCheck(retNotNil, r19, err)

		r20, err := client.CheckHealth(ctx, nil)
		retCheck(retNotNil, r20, err)

		r21, err := client.CreateResourceGroup(ctx, nil)
		retCheck(retNotNil, r21, err)

		r22, err := client.DropResourceGroup(ctx, nil)
		retCheck(retNotNil, r22, err)

		r23, err := client.TransferNode(ctx, nil)
		retCheck(retNotNil, r23, err)

		r24, err := client.TransferReplica(ctx, nil)
		retCheck(retNotNil, r24, err)

		r26, err := client.ListResourceGroups(ctx, nil)
		retCheck(retNotNil, r26, err)

		r27, err := client.DescribeResourceGroup(ctx, nil)
		retCheck(retNotNil, r27, err)

		r28, err := client.ListCheckers(ctx, nil)
		retCheck(retNotNil, r28, err)

		r29, err := client.ActivateChecker(ctx, nil)
		retCheck(retNotNil, r29, err)

		r30, err := client.DeactivateChecker(ctx, nil)
		retCheck(retNotNil, r30, err)

		r31, err := client.ListQueryNode(ctx, nil)
		retCheck(retNotNil, r31, err)

		r32, err := client.GetQueryNodeDistribution(ctx, nil)
		retCheck(retNotNil, r32, err)

		r33, err := client.SuspendBalance(ctx, nil)
		retCheck(retNotNil, r33, err)

		r34, err := client.ResumeBalance(ctx, nil)
		retCheck(retNotNil, r34, err)

		r35, err := client.SuspendNode(ctx, nil)
		retCheck(retNotNil, r35, err)

		r36, err := client.ResumeNode(ctx, nil)
		retCheck(retNotNil, r36, err)

		r37, err := client.TransferSegment(ctx, nil)
		retCheck(retNotNil, r37, err)

		r38, err := client.TransferChannel(ctx, nil)
		retCheck(retNotNil, r38, err)

		r39, err := client.CheckQueryNodeDistribution(ctx, nil)
		retCheck(retNotNil, r39, err)
	}

	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryCoordClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) querypb.QueryCoordClient {
		return &mock.GrpcQueryCoordClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryCoordClient]{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) querypb.QueryCoordClient {
		return &mock.GrpcQueryCoordClient{Err: errors.New("dummy")}
	}

	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[querypb.QueryCoordClient]{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) querypb.QueryCoordClient {
		return &mock.GrpcQueryCoordClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc3)

	checkFunc(true)

	err = client.Close()
	assert.NoError(t, err)
}
