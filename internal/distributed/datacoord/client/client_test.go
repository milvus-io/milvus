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

package grpcdatacoordclient

import (
	"context"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	// init embed etcd
	embedetcdServer, tempDir, err := etcd.StartTestEmbedEtcdServer()
	if err != nil {
		log.Fatal(err.Error())
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
	proxy.Params.Init()

	ctx := context.Background()
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.Nil(t, err)
	client, err := NewClient(ctx, proxy.Params.EtcdCfg.MetaRootPath.GetValue(), etcdCli)
	assert.Nil(t, err)
	assert.NotNil(t, client)

	err = client.Init()
	assert.Nil(t, err)

	err = client.Start()
	assert.Nil(t, err)

	err = client.Register()
	assert.Nil(t, err)

	checkFunc := func(retNotNil bool) {
		retCheck := func(notNil bool, ret any, err error) {
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

		r4, err := client.Flush(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.AssignSegmentID(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.GetSegmentInfo(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := client.GetSegmentStates(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := client.GetInsertBinlogPaths(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := client.GetCollectionStatistics(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := client.GetPartitionStatistics(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.GetSegmentInfoChannel(ctx)
		retCheck(retNotNil, r11, err)

		// r12, err := client.SaveBinlogPaths(ctx, nil)
		// retCheck(retNotNil, r12, err)

		r13, err := client.GetRecoveryInfo(ctx, nil)
		retCheck(retNotNil, r13, err)

		r14, err := client.GetFlushedSegments(ctx, nil)
		retCheck(retNotNil, r14, err)

		r15, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r15, err)

		r17, err := client.GetCompactionState(ctx, nil)
		retCheck(retNotNil, r17, err)

		r18, err := client.ManualCompaction(ctx, nil)
		retCheck(retNotNil, r18, err)

		r19, err := client.GetCompactionStateWithPlans(ctx, nil)
		retCheck(retNotNil, r19, err)

		r20, err := client.WatchChannels(ctx, nil)
		retCheck(retNotNil, r20, err)

		r21, err := client.DropVirtualChannel(ctx, nil)
		retCheck(retNotNil, r21, err)

		r22, err := client.SetSegmentState(ctx, nil)
		retCheck(retNotNil, r22, err)

		r23, err := client.Import(ctx, nil)
		retCheck(retNotNil, r23, err)

		r24, err := client.UpdateSegmentStatistics(ctx, nil)
		retCheck(retNotNil, r24, err)

		r27, err := client.SaveImportSegment(ctx, nil)
		retCheck(retNotNil, r27, err)

		r29, err := client.UnsetIsImportingState(ctx, nil)
		retCheck(retNotNil, r29, err)

		r30, err := client.MarkSegmentsDropped(ctx, nil)
		retCheck(retNotNil, r30, err)

		r31, err := client.ShowConfigurations(ctx, nil)
		retCheck(retNotNil, r31, err)

		r32, err := client.CreateIndex(ctx, nil)
		retCheck(retNotNil, r32, err)

		r33, err := client.DescribeIndex(ctx, nil)
		retCheck(retNotNil, r33, err)

		r34, err := client.DropIndex(ctx, nil)
		retCheck(retNotNil, r34, err)

		r35, err := client.GetIndexState(ctx, nil)
		retCheck(retNotNil, r35, err)

		r36, err := client.GetIndexBuildProgress(ctx, nil)
		retCheck(retNotNil, r36, err)

		r37, err := client.GetIndexInfos(ctx, nil)
		retCheck(retNotNil, r37, err)

		r38, err := client.GetSegmentIndexState(ctx, nil)
		retCheck(retNotNil, r38, err)

		r39, err := client.UpdateChannelCheckpoint(ctx, nil)
		retCheck(retNotNil, r39, err)

		{
			ret, err := client.BroadcastAlteredCollection(ctx, nil)
			retCheck(retNotNil, ret, err)
		}

		{
			ret, err := client.CheckHealth(ctx, nil)
			retCheck(retNotNil, ret, err)
		}
	}

	client.grpcClient = &mock.GRPCClientBase[datapb.DataCoordClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) datapb.DataCoordClient {
		return &mock.GrpcDataCoordClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	// special case since this method didn't use recall()
	ret, err := client.SaveBinlogPaths(ctx, nil)
	assert.Nil(t, ret)
	assert.NotNil(t, err)

	client.grpcClient = &mock.GRPCClientBase[datapb.DataCoordClient]{
		GetGrpcClientErr: nil,
	}
	newFunc2 := func(cc *grpc.ClientConn) datapb.DataCoordClient {
		return &mock.GrpcDataCoordClient{Err: errors.New("dummy")}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc2)
	checkFunc(false)

	// special case since this method didn't use recall()
	ret, err = client.SaveBinlogPaths(ctx, nil)
	assert.Nil(t, ret)
	assert.NotNil(t, err)

	client.grpcClient = &mock.GRPCClientBase[datapb.DataCoordClient]{
		GetGrpcClientErr: nil,
	}
	newFunc3 := func(cc *grpc.ClientConn) datapb.DataCoordClient {
		return &mock.GrpcDataCoordClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc3)
	checkFunc(true)

	// special case since this method didn't use recall()
	ret, err = client.SaveBinlogPaths(ctx, nil)
	assert.NotNil(t, ret)
	assert.Nil(t, err)

	err = client.Stop()
	assert.Nil(t, err)
}
