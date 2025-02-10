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
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
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
			r, err := client.GetComponentStates(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetTimeTickChannel(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.GetStatisticsChannel(ctx, nil)
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
			r, err := client.ShowCollectionIDs(ctx, nil)
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
			r, err := client.DescribeAlias(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListAliases(ctx, nil)
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
		{
			r, err := client.AlterCollection(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.AlterDatabase(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.BackupRBAC(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.RestoreRBAC(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.CreatePrivilegeGroup(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.DropPrivilegeGroup(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.ListPrivilegeGroups(ctx, nil)
			retCheck(retNotNil, r, err)
		}
		{
			r, err := client.OperatePrivilegeGroup(ctx, nil)
			retCheck(retNotNil, r, err)
		}
	}

	client.(*Client).grpcClient = &mock.GRPCClientBase[rootcoordpb.RootCoordClient]{
		GetGrpcClientErr: errors.New("dummy"),
	}

	newFunc1 := func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
		return &mock.GrpcRootCoordClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[rootcoordpb.RootCoordClient]{
		GetGrpcClientErr: nil,
	}

	newFunc2 := func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
		return &mock.GrpcRootCoordClient{Err: errors.New("dummy")}
	}

	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.(*Client).grpcClient = &mock.GRPCClientBase[rootcoordpb.RootCoordClient]{
		GetGrpcClientErr: nil,
	}

	newFunc3 := func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
		return &mock.GrpcRootCoordClient{Err: nil}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc3)

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
		rTimeout, err := client.GetComponentStates(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetTimeTickChannel(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.GetStatisticsChannel(shortCtx, nil)
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
		rTimeout, err := client.ShowCollectionIDs(shortCtx, nil)
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
		rTimeout, err := client.DescribeAlias(shortCtx, nil)
		retCheck(rTimeout, err)
	}
	{
		rTimeout, err := client.ListAliases(shortCtx, nil)
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
	err = client.Close()
	assert.NoError(t, err)
}
