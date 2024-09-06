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

package distributed

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestMain(t *testing.M) {
	// init embed etcd
	embedetcdServer, tempDir, err := etcd.StartTestEmbedEtcdServer()
	if err != nil {
		log.Fatal("failed to start embed etcd server for unittest", zap.Error(err))
	}

	defer os.RemoveAll(tempDir)
	defer embedetcdServer.Server.Stop()

	addrs := etcd.GetEmbedEtcdEndpoints(embedetcdServer)

	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().EtcdCfg.Endpoints.Key, strings.Join(addrs, ","))

	os.Exit(t.Run())
}

func TestConnectionManager(t *testing.T) {
	ctx := context.Background()

	testPath := fmt.Sprintf("TestConnectionManager-%d", time.Now().Unix())
	paramtable.Get().Save(paramtable.Get().EtcdCfg.RootPath.Key, testPath)

	session := initSession(ctx)
	cm := NewConnectionManager(session)
	cm.AddDependency(typeutil.RootCoordRole)
	cm.AddDependency(typeutil.QueryCoordRole)
	cm.AddDependency(typeutil.DataCoordRole)
	cm.AddDependency(typeutil.QueryNodeRole)
	cm.AddDependency(typeutil.DataNodeRole)
	cm.AddDependency(typeutil.IndexNodeRole)
	cm.Start()

	t.Run("rootCoord", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:")
		assert.NoError(t, err)
		defer lis.Close()
		rootCoord := &testRootCoord{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		rootcoordpb.RegisterRootCoordServer(grpcServer, rootCoord)
		go grpcServer.Serve(lis)
		session.Init(typeutil.RootCoordRole, lis.Addr().String(), true, false)
		session.Register()
		assert.Eventually(t, func() bool {
			rootCoord, ok := cm.GetRootCoordClient()
			return rootCoord != nil && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("queryCoord", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:")
		assert.NoError(t, err)
		defer lis.Close()
		queryCoord := &testQueryCoord{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		querypb.RegisterQueryCoordServer(grpcServer, queryCoord)
		go grpcServer.Serve(lis)
		session.Init(typeutil.QueryCoordRole, lis.Addr().String(), true, false)
		session.Register()
		assert.Eventually(t, func() bool {
			queryCoord, ok := cm.GetQueryCoordClient()
			return queryCoord != nil && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("dataCoord", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:")
		assert.NoError(t, err)
		defer lis.Close()
		dataCoord := &testDataCoord{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		datapb.RegisterDataCoordServer(grpcServer, dataCoord)
		go grpcServer.Serve(lis)
		session.Init(typeutil.DataCoordRole, lis.Addr().String(), true, false)
		session.Register()
		assert.Eventually(t, func() bool {
			dataCoord, ok := cm.GetDataCoordClient()
			return dataCoord != nil && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("queryNode", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:")
		assert.NoError(t, err)
		defer lis.Close()
		queryNode := &testQueryNode{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		querypb.RegisterQueryNodeServer(grpcServer, queryNode)
		go grpcServer.Serve(lis)
		session.Init(typeutil.QueryNodeRole, lis.Addr().String(), true, false)
		session.Register()
		assert.Eventually(t, func() bool {
			queryNodes, ok := cm.GetQueryNodeClients()
			return len(queryNodes) == 1 && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("dataNode", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:")
		assert.NoError(t, err)
		defer lis.Close()
		dataNode := &testDataNode{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		datapb.RegisterDataNodeServer(grpcServer, dataNode)
		go grpcServer.Serve(lis)
		session.Init(typeutil.DataNodeRole, lis.Addr().String(), true, false)
		session.Register()
		assert.Eventually(t, func() bool {
			dataNodes, ok := cm.GetDataNodeClients()
			return len(dataNodes) == 1 && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("indexNode", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:")
		assert.NoError(t, err)
		defer lis.Close()
		indexNode := &testIndexNode{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		workerpb.RegisterIndexNodeServer(grpcServer, indexNode)
		go grpcServer.Serve(lis)
		session.Init(typeutil.IndexNodeRole, lis.Addr().String(), true, false)
		session.Register()
		assert.Eventually(t, func() bool {
			indexNodes, ok := cm.GetIndexNodeClients()
			return len(indexNodes) == 1 && ok
		}, 10*time.Second, 100*time.Millisecond)
	})
}

func TestConnectionManager_processEvent(t *testing.T) {
	t.Run("close closeCh", func(t *testing.T) {
		cm := &ConnectionManager{
			closeCh: make(chan struct{}),
		}

		ech := make(chan *sessionutil.SessionEvent)
		flag := false
		signal := make(chan struct{}, 1)
		go func() {
			assert.Panics(t, func() {
				cm.processEvent(ech)
			})

			flag = true
			signal <- struct{}{}
		}()

		close(ech)
		<-signal
		assert.True(t, flag)

		ech = make(chan *sessionutil.SessionEvent)
		flag = false
		go func() {
			cm.processEvent(ech)
			flag = true
			signal <- struct{}{}
		}()
		close(cm.closeCh)
		<-signal
		assert.True(t, flag)
	})

	t.Run("close watch chan", func(t *testing.T) {
		sc := make(chan os.Signal, 1)
		signal.Notify(sc, syscall.SIGINT)
		defer signal.Reset(syscall.SIGINT)
		sigQuit := make(chan struct{}, 1)

		cm := &ConnectionManager{
			closeCh: make(chan struct{}),
			session: &sessionutil.Session{
				SessionRaw: sessionutil.SessionRaw{
					ServerID:    1,
					TriggerKill: true,
				},
			},
		}

		ech := make(chan *sessionutil.SessionEvent)

		go func() {
			<-sc
			sigQuit <- struct{}{}
		}()

		go func() {
			cm.processEvent(ech)
		}()

		close(ech)

		<-sigQuit
	})
}

type testRootCoord struct {
	rootcoordpb.RootCoordServer
}

type testQueryCoord struct {
	querypb.QueryCoordServer
}
type testDataCoord struct {
	datapb.DataCoordServer
}

type testQueryNode struct {
	querypb.QueryNodeServer
}

type testDataNode struct {
	datapb.DataNodeServer
}

type testIndexNode struct {
	workerpb.IndexNodeServer
}

func initSession(ctx context.Context) *sessionutil.Session {
	baseTable := paramtable.GetBaseTable()
	rootPath, err := baseTable.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := baseTable.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	metaRootPath := rootPath + "/" + subPath

	endpoints := baseTable.GetWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
	etcdEndpoints := strings.Split(endpoints, ",")

	log.Debug("metaRootPath", zap.Any("metaRootPath", metaRootPath))
	log.Debug("etcdPoints", zap.Any("etcdPoints", etcdEndpoints))

	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	if err != nil {
		panic(err)
	}
	session := sessionutil.NewSessionWithEtcd(ctx, metaRootPath, etcdCli)
	return session
}
