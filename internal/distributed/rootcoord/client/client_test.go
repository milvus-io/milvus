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
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/util/mock"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, err)
	client, err := NewClient(ctx, proxy.Params.EtcdCfg.MetaRootPath.GetValue(), etcdCli)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	err = client.Init()
	assert.NoError(t, err)

	err = client.Start()
	assert.NoError(t, err)

	err = client.Register()
	assert.NoError(t, err)

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

		{
			r, err := client.GetComponentStates(ctx)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetTimeTickChannel(ctx)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetStatisticsChannel(ctx)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.HasCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DescribeCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ShowCollections(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreatePartition(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropPartition(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.HasPartition(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ShowPartitions(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.AllocTimestamp(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.AllocID(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.UpdateChannelTimeTick(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ShowSegments(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetMetrics(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateAlias(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropAlias(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.AlterAlias(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.Import(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetImportState(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ReportImport(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateCredential(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetCredential(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.UpdateCredential(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DeleteCredential(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListCredUsers(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.InvalidateCollectionMetaCache(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateRole(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropRole(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.OperateUserRole(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.SelectRole(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.SelectUser(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.OperatePrivilege(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.SelectGrant(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListPolicy(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ShowConfigurations(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CheckHealth(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreateDatabase(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropDatabase(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListDatabases(ctx, nil)
			retCheck(retNotNil, r, err)
		}
	}

	client.grpcClient = &mock.GRPCClientBase[rootcoordpb.RootCoordClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
		return &mock.GrpcRootCoordClient{Err: nil}
	}
	client.grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.grpcClient = &mock.GRPCClientBase[rootcoordpb.RootCoordClient]{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
		return &mock.GrpcRootCoordClient{Err: errors.New("dummy")}
	}

	client.grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.grpcClient = &mock.GRPCClientBase[rootcoordpb.RootCoordClient]{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
		return &mock.GrpcRootCoordClient{Err: nil}
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
		assert.Error(t, err)
	}
	{
		rTimeout, err := client.GetComponentStates(shortCtx)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetTimeTickChannel(shortCtx)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetStatisticsChannel(shortCtx)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateCollection(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropCollection(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.HasCollection(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DescribeCollection(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ShowCollections(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreatePartition(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropPartition(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.HasPartition(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ShowPartitions(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.AllocTimestamp(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.AllocID(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.UpdateChannelTimeTick(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ShowSegments(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetMetrics(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateAlias(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropAlias(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.AlterAlias(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.Import(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetImportState(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ReportImport(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateCredential(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetCredential(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.UpdateCredential(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DeleteCredential(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ListCredUsers(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ListImportTasks(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.InvalidateCollectionMetaCache(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateRole(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropRole(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.OperateUserRole(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.SelectRole(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.SelectUser(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.OperatePrivilege(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.SelectGrant(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ListPolicy(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CheckHealth(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.CreateDatabase(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.DropDatabase(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ListDatabases(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	// clean up
	err = client.Stop()
	assert.NoError(t, err)
}
