package distributed

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var Params paramtable.BaseTable

func TestConnectionManager(t *testing.T) {
	Params.Init()
	ctx := context.Background()

	session := initSession(ctx)
	cm := NewConnectionManager(session)
	cm.AddDependency(typeutil.RootCoordRole)
	cm.AddDependency(typeutil.QueryCoordRole)
	cm.AddDependency(typeutil.DataCoordRole)
	cm.AddDependency(typeutil.IndexCoordRole)
	cm.AddDependency(typeutil.QueryNodeRole)
	cm.AddDependency(typeutil.DataNodeRole)
	cm.AddDependency(typeutil.IndexNodeRole)
	cm.Start()

	t.Run("rootCoord", func(t *testing.T) {
		lis, err := net.Listen("tcp", ":9999")
		assert.Nil(t, err)
		defer lis.Close()
		rootCoord := &testRootCoord{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		rootcoordpb.RegisterRootCoordServer(grpcServer, rootCoord)
		go grpcServer.Serve(lis)
		session.Init(typeutil.RootCoordRole, "127.0.0.1:9999", true)
		assert.Eventually(t, func() bool {
			rootCoord, ok := cm.GetRootCoordClient()
			return rootCoord != nil && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("queryCoord", func(t *testing.T) {
		lis, err := net.Listen("tcp", ":9999")
		assert.Nil(t, err)
		defer lis.Close()
		queryCoord := &testQueryCoord{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		querypb.RegisterQueryCoordServer(grpcServer, queryCoord)
		go grpcServer.Serve(lis)
		session.Init(typeutil.QueryCoordRole, "127.0.0.1:9999", true)
		assert.Eventually(t, func() bool {
			queryCoord, ok := cm.GetQueryCoordClient()
			return queryCoord != nil && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("dataCoord", func(t *testing.T) {
		lis, err := net.Listen("tcp", ":9999")
		assert.Nil(t, err)
		defer lis.Close()
		dataCoord := &testDataCoord{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		datapb.RegisterDataCoordServer(grpcServer, dataCoord)
		go grpcServer.Serve(lis)
		session.Init(typeutil.DataCoordRole, "127.0.0.1:9999", true)
		assert.Eventually(t, func() bool {
			dataCoord, ok := cm.GetDataCoordClient()
			return dataCoord != nil && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("indexCoord", func(t *testing.T) {
		lis, err := net.Listen("tcp", ":9999")
		assert.Nil(t, err)
		defer lis.Close()
		indexCoord := &testIndexCoord{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		indexpb.RegisterIndexCoordServer(grpcServer, indexCoord)
		go grpcServer.Serve(lis)
		session.Init(typeutil.IndexCoordRole, "127.0.0.1:9999", true)
		assert.Eventually(t, func() bool {
			indexCoord, ok := cm.GetIndexCoordClient()
			return indexCoord != nil && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("queryNode", func(t *testing.T) {
		lis, err := net.Listen("tcp", ":9999")
		assert.Nil(t, err)
		defer lis.Close()
		queryNode := &testQueryNode{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		querypb.RegisterQueryNodeServer(grpcServer, queryNode)
		go grpcServer.Serve(lis)
		session.Init(typeutil.QueryNodeRole, "127.0.0.1:9999", true)
		assert.Eventually(t, func() bool {
			queryNodes, ok := cm.GetQueryNodeClients()
			return len(queryNodes) == 1 && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("dataNode", func(t *testing.T) {
		lis, err := net.Listen("tcp", ":9999")
		assert.Nil(t, err)
		defer lis.Close()
		dataNode := &testDataNode{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		datapb.RegisterDataNodeServer(grpcServer, dataNode)
		go grpcServer.Serve(lis)
		session.Init(typeutil.DataNodeRole, "127.0.0.1:9999", true)
		assert.Eventually(t, func() bool {
			dataNodes, ok := cm.GetDataNodeClients()
			return len(dataNodes) == 1 && ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("indexNode", func(t *testing.T) {
		lis, err := net.Listen("tcp", ":9999")
		assert.Nil(t, err)
		defer lis.Close()
		indexNode := &testIndexNode{}
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		indexpb.RegisterIndexNodeServer(grpcServer, indexNode)
		go grpcServer.Serve(lis)
		session.Init(typeutil.IndexNodeRole, "127.0.0.1:9999", true)
		assert.Eventually(t, func() bool {
			indexNodes, ok := cm.GetIndexNodeClients()
			return len(indexNodes) == 1 && ok
		}, 10*time.Second, 100*time.Millisecond)
	})
}

func TestConnectionManager_processEvent(t *testing.T) {
	cm := &ConnectionManager{
		closeCh: make(chan struct{}),
	}

	ech := make(chan *sessionutil.SessionEvent)
	flag := false
	signal := make(chan struct{}, 1)
	go func() {
		cm.processEvent(ech)
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

type testIndexCoord struct {
	indexpb.IndexCoordServer
}

type testQueryNode struct {
	querypb.QueryNodeServer
}

type testDataNode struct {
	datapb.DataNodeServer
}

type testIndexNode struct {
	indexpb.IndexNodeServer
}

func initSession(ctx context.Context) *sessionutil.Session {
	rootPath, err := Params.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := Params.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	metaRootPath := rootPath + "/" + subPath

	endpoints, err := Params.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	etcdEndpoints := strings.Split(endpoints, ",")

	log.Debug("metaRootPath", zap.Any("metaRootPath", metaRootPath))
	log.Debug("etcdPoints", zap.Any("etcdPoints", etcdEndpoints))

	session := sessionutil.NewSession(ctx, metaRootPath, etcdEndpoints)
	return session
}
