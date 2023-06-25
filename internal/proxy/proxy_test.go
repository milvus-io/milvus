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

package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/tracer"

	"github.com/golang/protobuf/proto"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	grpcdatacoordclient "github.com/milvus-io/milvus/internal/distributed/datacoord"
	grpcdatacoordclient2 "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	grpcdatanode "github.com/milvus-io/milvus/internal/distributed/datanode"
	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"
	grpcquerycoord "github.com/milvus-io/milvus/internal/distributed/querycoord"
	grpcquerycoordclient "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
	grpcquerynode "github.com/milvus-io/milvus/internal/distributed/querynode"
	grpcrootcoord "github.com/milvus-io/milvus/internal/distributed/rootcoord"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	attempts      = 1000000
	sleepDuration = time.Millisecond * 200
)

var Registry *prometheus.Registry

func init() {
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	Registry.MustRegister(prometheus.NewGoCollector())
}

func runRootCoord(ctx context.Context, localMsg bool) *grpcrootcoord.Server {
	var rc *grpcrootcoord.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := dependency.NewDefaultFactory(localMsg)
		var err error
		rc, err = grpcrootcoord.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = rc.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterRootCoord(Registry)
	return rc
}

func runQueryCoord(ctx context.Context, localMsg bool) *grpcquerycoord.Server {
	var qs *grpcquerycoord.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := dependency.NewDefaultFactory(localMsg)
		var err error
		qs, err = grpcquerycoord.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = qs.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterQueryCoord(Registry)
	return qs
}

func runQueryNode(ctx context.Context, localMsg bool, alias string) *grpcquerynode.Server {
	var qn *grpcquerynode.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := dependency.MockDefaultFactory(localMsg, Params)
		var err error
		qn, err = grpcquerynode.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = qn.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterQueryNode(Registry)
	return qn
}

func runDataCoord(ctx context.Context, localMsg bool) *grpcdatacoordclient.Server {
	var ds *grpcdatacoordclient.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := dependency.NewDefaultFactory(localMsg)
		ds = grpcdatacoordclient.NewServer(ctx, factory)
		wg.Done()
		err := ds.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterDataCoord(Registry)
	return ds
}

func runDataNode(ctx context.Context, localMsg bool, alias string) *grpcdatanode.Server {
	var dn *grpcdatanode.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := dependency.MockDefaultFactory(localMsg, Params)
		var err error
		dn, err = grpcdatanode.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = dn.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterDataNode(Registry)
	return dn
}

func runIndexNode(ctx context.Context, localMsg bool, alias string) *grpcindexnode.Server {
	var in *grpcindexnode.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := dependency.MockDefaultFactory(localMsg, Params)
		var err error
		in, err = grpcindexnode.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		etcd, err := etcd.GetEtcdClient(
			Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
			Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
			Params.EtcdCfg.Endpoints.GetAsStrings(),
			Params.EtcdCfg.EtcdTLSCert.GetValue(),
			Params.EtcdCfg.EtcdTLSKey.GetValue(),
			Params.EtcdCfg.EtcdTLSCACert.GetValue(),
			Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
		if err != nil {
			panic(err)
		}
		in.SetEtcdClient(etcd)
		err = in.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterIndexNode(Registry)
	return in
}

type proxyTestServer struct {
	*Proxy
	grpcServer *grpc.Server
	ch         chan error
}

func newProxyTestServer(node *Proxy) *proxyTestServer {
	return &proxyTestServer{
		Proxy:      node,
		grpcServer: nil,
		ch:         make(chan error, 1),
	}
}

func (s *proxyTestServer) GetVersion(ctx context.Context, request *milvuspb.GetVersionRequest) (*milvuspb.GetVersionResponse, error) {
	return &milvuspb.GetVersionResponse{
		Version: "vx.x.x",
	}, nil
}

func (s *proxyTestServer) RenameCollection(ctx context.Context, request *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return s.Proxy.RenameCollection(ctx, request)
}

func (s *proxyTestServer) GetComponentStates(ctx context.Context, request *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return s.Proxy.GetComponentStates(ctx)
}

func (s *proxyTestServer) GetStatisticsChannel(ctx context.Context, request *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return s.Proxy.GetStatisticsChannel(ctx)
}

func (s *proxyTestServer) startGrpc(ctx context.Context, wg *sync.WaitGroup, p *paramtable.GrpcServerConfig) {
	defer wg.Done()

	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		Time:    60 * time.Second, // Ping the client if it is idle for 60 seconds to ensure the connection is still active
		Timeout: 10 * time.Second, // Wait 10 second for the ping ack before assuming the connection is dead
	}

	log.Debug("Proxy server listen on tcp", zap.Int("port", p.Port.GetAsInt()))
	lis, err := net.Listen("tcp", ":"+p.Port.GetValue())
	if err != nil {
		log.Warn("Proxy server failed to listen on", zap.Error(err), zap.Int("port", p.Port.GetAsInt()))
		s.ch <- err
		return
	}
	log.Debug("Proxy server already listen on tcp", zap.Int("port", p.Port.GetAsInt()))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	multiLimiter := NewMultiRateLimiter()
	s.multiRateLimiter = multiLimiter

	opts := tracer.GetInterceptorOpts()
	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.MaxRecvMsgSize(p.ServerMaxRecvSize.GetAsInt()),
		grpc.MaxSendMsgSize(p.ServerMaxSendSize.GetAsInt()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			otelgrpc.UnaryServerInterceptor(opts...),
			RateLimitInterceptor(multiLimiter),
		)),
		grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor(opts...)))
	proxypb.RegisterProxyServer(s.grpcServer, s)
	milvuspb.RegisterMilvusServiceServer(s.grpcServer, s)

	log.Debug("create Proxy grpc server",
		zap.Any("enforcement policy", kaep),
		zap.Any("server parameters", kasp))

	log.Debug("waiting for Proxy grpc server to be ready")
	go funcutil.CheckGrpcReady(ctx, s.ch)

	log.Debug("Proxy grpc server has been ready, serve grpc requests on listen")
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Warn("failed to serve on Proxy's listener", zap.Error(err))
		s.ch <- err
	}
}

func (s *proxyTestServer) waitForGrpcReady() error {
	return <-s.ch
}

func (s *proxyTestServer) gracefulStop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

func TestProxy(t *testing.T) {
	var err error
	var wg sync.WaitGroup
	paramtable.Init()

	path := "/tmp/milvus/rocksmq" + funcutil.GenRandomStr()
	t.Setenv("ROCKSMQ_PATH", path)
	defer os.RemoveAll(path)

	ctx, cancel := context.WithCancel(context.Background())
	ctx = GetContext(ctx, "root:123456")
	localMsg := true
	factory := dependency.MockDefaultFactory(localMsg, Params)
	alias := "TestProxy"

	log.Info("Initialize parameter table of Proxy")

	rc := runRootCoord(ctx, localMsg)
	log.Info("running RootCoord ...")

	if rc != nil {
		defer func() {
			err := rc.Stop()
			assert.NoError(t, err)
			log.Info("stop RootCoord")
		}()
	}

	dc := runDataCoord(ctx, localMsg)
	log.Info("running DataCoord ...")

	if dc != nil {
		defer func() {
			err := dc.Stop()
			assert.NoError(t, err)
			log.Info("stop DataCoord")
		}()
	}

	dn := runDataNode(ctx, localMsg, alias)
	log.Info("running DataNode ...")

	if dn != nil {
		defer func() {
			err := dn.Stop()
			assert.NoError(t, err)
			log.Info("stop DataNode")
		}()
	}

	qc := runQueryCoord(ctx, localMsg)
	log.Info("running QueryCoord ...")

	if qc != nil {
		defer func() {
			err := qc.Stop()
			assert.NoError(t, err)
			log.Info("stop QueryCoord")
		}()
	}

	qn := runQueryNode(ctx, localMsg, alias)
	log.Info("running QueryNode ...")

	if qn != nil {
		defer func() {
			err := qn.Stop()
			assert.NoError(t, err)
			log.Info("stop query node")
		}()
	}

	in := runIndexNode(ctx, localMsg, alias)
	log.Info("running IndexNode ...")

	if in != nil {
		defer func() {
			err := in.Stop()
			assert.NoError(t, err)
			log.Info("stop IndexNode")
		}()
	}

	time.Sleep(10 * time.Millisecond)

	proxy, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	assert.NotNil(t, proxy)

	etcdcli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	defer etcdcli.Close()
	assert.NoError(t, err)
	proxy.SetEtcdClient(etcdcli)

	testServer := newProxyTestServer(proxy)
	wg.Add(1)

	base := &paramtable.ComponentParam{}
	base.Init()
	var p paramtable.GrpcServerConfig
	p.Init(typeutil.ProxyRole, &base.BaseTable)
	testServer.Proxy.SetAddress(p.GetAddress())
	assert.Equal(t, p.GetAddress(), testServer.Proxy.GetAddress())

	go testServer.startGrpc(ctx, &wg, &p)
	assert.NoError(t, testServer.waitForGrpcReady())

	rootCoordClient, err := rcc.NewClient(ctx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdcli)
	assert.NoError(t, err)
	err = rootCoordClient.Init()
	assert.NoError(t, err)
	err = componentutil.WaitForComponentHealthy(ctx, rootCoordClient, typeutil.RootCoordRole, attempts, sleepDuration)
	assert.NoError(t, err)
	proxy.SetRootCoordClient(rootCoordClient)
	log.Info("Proxy set root coordinator client")

	dataCoordClient, err := grpcdatacoordclient2.NewClient(ctx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdcli)
	assert.NoError(t, err)
	err = dataCoordClient.Init()
	assert.NoError(t, err)
	err = componentutil.WaitForComponentHealthy(ctx, dataCoordClient, typeutil.DataCoordRole, attempts, sleepDuration)
	assert.NoError(t, err)
	proxy.SetDataCoordClient(dataCoordClient)
	log.Info("Proxy set data coordinator client")

	queryCoordClient, err := grpcquerycoordclient.NewClient(ctx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdcli)
	assert.NoError(t, err)
	err = queryCoordClient.Init()
	assert.NoError(t, err)
	err = componentutil.WaitForComponentHealthy(ctx, queryCoordClient, typeutil.QueryCoordRole, attempts, sleepDuration)
	assert.NoError(t, err)
	proxy.SetQueryCoordClient(queryCoordClient)
	proxy.SetQueryNodeCreator(defaultQueryNodeClientCreator)
	log.Info("Proxy set query coordinator client")

	proxy.UpdateStateCode(commonpb.StateCode_Initializing)
	err = proxy.Init()
	assert.NoError(t, err)

	err = proxy.Start()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, proxy.stateCode.Load().(commonpb.StateCode))

	// register proxy
	err = proxy.Register()
	assert.NoError(t, err)
	log.Info("Register proxy done")
	defer func() {
		err := proxy.Stop()
		assert.NoError(t, err)
	}()

	t.Run("get component states", func(t *testing.T) {
		states, err := proxy.GetComponentStates(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
		assert.Equal(t, paramtable.GetNodeID(), states.State.NodeID)
		assert.Equal(t, typeutil.ProxyRole, states.State.Role)
		assert.Equal(t, proxy.stateCode.Load().(commonpb.StateCode), states.State.StateCode)
	})

	t.Run("get statistics channel", func(t *testing.T) {
		resp, err := proxy.GetStatisticsChannel(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, "", resp.Value)
	})

	prefix := "test_proxy_"
	partitionPrefix := "test_proxy_partition_"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	otherCollectionName := collectionName + "_other_" + funcutil.GenRandomStr()
	partitionName := partitionPrefix + funcutil.GenRandomStr()
	otherPartitionName := partitionPrefix + "_other_" + funcutil.GenRandomStr()
	shardsNum := common.DefaultShardsNum
	int64Field := "int64"
	floatVecField := "fVec"
	dim := 128
	rowNum := 3000
	indexName := "_default"
	nlist := 10
	// nprobe := 10
	// topk := 10
	// add a test parameter
	// roundDecimal := 6
	nq := 10
	// expr := fmt.Sprintf("%s > 0", int64Field)
	var segmentIDs []int64

	// an int64 field (pk) & a float vector field
	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()

	constructCreateCollectionRequest := func() *milvuspb.CreateCollectionRequest {
		bs, err := proto.Marshal(schema)
		assert.NoError(t, err)
		return &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         bs,
			ShardsNum:      shardsNum,
		}
	}
	createCollectionReq := constructCreateCollectionRequest()

	constructCollectionInsertRequest := func() *milvuspb.InsertRequest {
		fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
		hashKeys := generateHashKeys(rowNum)
		return &milvuspb.InsertRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  "",
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		}
	}

	constructPartitionInsertRequest := func() *milvuspb.InsertRequest {
		fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
		hashKeys := generateHashKeys(rowNum)
		return &milvuspb.InsertRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		}
	}

	constructCollectionUpsertRequest := func() *milvuspb.UpsertRequest {
		fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
		hashKeys := generateHashKeys(rowNum)
		return &milvuspb.UpsertRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		}
	}

	constructCreateIndexRequest := func() *milvuspb.CreateIndexRequest {
		return &milvuspb.CreateIndexRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			IndexName:      indexName,
			ExtraParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
				{
					Key:   common.MetricTypeKey,
					Value: distance.L2,
				},
				{
					Key:   common.IndexTypeKey,
					Value: "IVF_FLAT",
				},
				{
					Key:   "nlist",
					Value: strconv.Itoa(nlist),
				},
			},
		}
	}

	wg.Add(1)
	t.Run("create collection", func(t *testing.T) {
		defer wg.Done()
		req := createCollectionReq
		resp, err := proxy.CreateCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		reqInvalidField := constructCreateCollectionRequest()
		schema := constructCollectionSchema()
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			Name:     "StringField",
			DataType: schemapb.DataType_String,
		})
		bs, err := proto.Marshal(schema)
		assert.NoError(t, err)
		reqInvalidField.CollectionName = "invalid_field_coll"
		reqInvalidField.Schema = bs

		resp, err = proxy.CreateCollection(ctx, reqInvalidField)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

	})

	wg.Add(1)
	t.Run("create alias", func(t *testing.T) {
		defer wg.Done()
		// create alias
		aliasReq := &milvuspb.CreateAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias",
		}
		resp, err := proxy.CreateAlias(ctx, aliasReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		_, _ = proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		})
	})

	wg.Add(1)
	t.Run("alter alias", func(t *testing.T) {
		defer wg.Done()
		// alter alias
		alterReq := &milvuspb.AlterAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias",
		}
		resp, err := proxy.AlterAlias(ctx, alterReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		_, _ = proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		})

		nonExistingCollName := "coll_name_random_zarathustra"
		faultyAlterReq := &milvuspb.AlterAliasRequest{
			Base:           nil,
			CollectionName: nonExistingCollName,
			Alias:          "alias",
		}
		resp, err = proxy.AlterAlias(ctx, faultyAlterReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("drop alias", func(t *testing.T) {
		defer wg.Done()
		// drop alias
		resp, err := proxy.DropAlias(ctx, &milvuspb.DropAliasRequest{
			Base:  nil,
			Alias: "alias",
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		_, _ = proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		})

	})

	wg.Add(1)
	t.Run("has collection", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.True(t, resp.Value)

		// has other collection: false
		resp, err = proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.False(t, resp.Value)
	})

	wg.Add(1)
	t.Run("describe collection", func(t *testing.T) {
		defer wg.Done()
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, collectionID, resp.CollectionID)
		// TODO(dragondriver): shards num
		assert.Equal(t, len(schema.Fields), len(resp.Schema.Fields))
		// TODO(dragondriver): compare fields schema, not sure the order of fields

		// describe other collection -> fail
		resp, err = proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			CollectionID:   collectionID,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("get collection statistics", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// TODO(dragondriver): check num rows

		// get statistics of other collection -> fail
		resp, err = proxy.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("show collections", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_All,
			CollectionNames: nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.CollectionNames), resp.CollectionNames)
	})

	wg.Add(1)
	t.Run("alter collection", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: "cn",
			Properties: []*commonpb.KeyValuePair{
				{
					Key:   common.CollectionTTLConfigKey,
					Value: "3600",
				},
			},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("create partition", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// create partition with non-exist collection -> fail
		resp, err = proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("has partition", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.True(t, resp.Value)

		resp, err = proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  otherPartitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.False(t, resp.Value)

		// non-exist collection -> fail
		resp, err = proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("get partition statistics", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// non-exist partition -> fail
		resp, err = proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  otherPartitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// non-exist collection -> fail
		resp, err = proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("show partitions", func(t *testing.T) {
		defer wg.Done()
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_All,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// default partition
		assert.Equal(t, 2, len(resp.PartitionNames))

		{
			stateResp, err := proxy.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{
				CollectionName: collectionName,
				PartitionNames: resp.PartitionNames,
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, stateResp.Status.ErrorCode)
			assert.Equal(t, commonpb.LoadState_LoadStateNotLoad, stateResp.State)
		}

		// non-exist collection -> fail
		resp, err = proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			CollectionID:   collectionID + 1,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_All,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("insert", func(t *testing.T) {
		defer wg.Done()
		req := constructCollectionInsertRequest()

		resp, err := proxy.Insert(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, rowNum, len(resp.SuccIndex))
		assert.Equal(t, 0, len(resp.ErrIndex))
		assert.Equal(t, int64(rowNum), resp.InsertCnt)
	})

	//TODO(dragondriver): proxy.Delete()

	flushed := true
	wg.Add(1)
	t.Run("flush", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Flush(ctx, &milvuspb.FlushRequest{
			Base:            nil,
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		segmentIDs = resp.CollSegIDs[collectionName].Data
		log.Info("flush collection", zap.Int64s("segments to be flushed", segmentIDs))

		f := func() bool {
			resp, err := proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
				SegmentIDs: segmentIDs,
			})
			if err != nil {
				return false
			}
			return resp.GetFlushed()
		}

		// waiting for flush operation to be done
		counter := 0
		for !f() {
			if counter > 100 {
				flushed = false
				break
			}
			// avoid too frequent rpc call
			time.Sleep(100 * time.Millisecond)
			counter++
		}
	})
	if !flushed {
		log.Warn("flush operation was not sure to be done")
	}

	wg.Add(1)
	t.Run("get statistics after flush", func(t *testing.T) {
		defer wg.Done()
		if !flushed {
			t.Skip("flush operation was not done")
		}
		resp, err := proxy.GetStatistics(ctx, &milvuspb.GetStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		rowNumStr := funcutil.KeyValuePair2Map(resp.Stats)["row_count"]
		assert.Equal(t, strconv.Itoa(rowNum), rowNumStr)

		// get statistics of other collection -> fail
		resp, err = proxy.GetStatistics(ctx, &milvuspb.GetStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("create index", func(t *testing.T) {
		defer wg.Done()
		req := constructCreateIndexRequest()

		resp, err := proxy.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("describe index", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			IndexName:      "",
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		indexName = resp.IndexDescriptions[0].IndexName
	})

	wg.Add(1)
	t.Run("get index statistics", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			IndexName:      "",
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		indexName = resp.IndexDescriptions[0].IndexName
	})

	wg.Add(1)
	t.Run("get index build progress", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexBuildProgress(ctx, &milvuspb.GetIndexBuildProgressRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			IndexName:      indexName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("get index state", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexState(ctx, &milvuspb.GetIndexStateRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			IndexName:      indexName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	loaded := true
	wg.Add(1)
	t.Run("load collection", func(t *testing.T) {
		defer wg.Done()
		{
			stateResp, err := proxy.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{
				CollectionName: collectionName,
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, stateResp.Status.ErrorCode)
			assert.Equal(t, commonpb.LoadState_LoadStateNotLoad, stateResp.State)
		}

		resp, err := proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// load other collection -> fail
		resp, err = proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		f := func() bool {
			resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
				Base:            nil,
				DbName:          dbName,
				TimeStamp:       0,
				Type:            milvuspb.ShowType_InMemory,
				CollectionNames: []string{collectionName},
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

			for idx, name := range resp.CollectionNames {
				if name == collectionName && resp.InMemoryPercentages[idx] == 100 {
					return true
				}
			}

			return false
		}

		// waiting for collection to be loaded
		counter := 0
		for !f() {
			if counter > 100 {
				loaded = false
				break
			}
			// avoid too frequent rpc call
			time.Sleep(100 * time.Millisecond)
			counter++
		}
	})
	assert.True(t, loaded)

	wg.Add(1)
	t.Run("show in-memory collections", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_InMemory,
			CollectionNames: nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.CollectionNames))

		// get in-memory percentage
		resp, err = proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_InMemory,
			CollectionNames: []string{collectionName},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.CollectionNames))
		assert.Equal(t, 1, len(resp.InMemoryPercentages))

		// get in-memory percentage of not loaded collection -> fail
		resp, err = proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_InMemory,
			CollectionNames: []string{otherCollectionName},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		{
			progressResp, err := proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
				CollectionName: collectionName,
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, progressResp.Status.ErrorCode)
			assert.NotEqual(t, int64(0), progressResp.Progress)
		}

		{
			progressResp, err := proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
				CollectionName: otherCollectionName,
			})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, progressResp.Status.ErrorCode)
			assert.Equal(t, int64(0), progressResp.Progress)
		}

		{
			stateResp, err := proxy.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{
				CollectionName: otherCollectionName,
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, stateResp.Status.ErrorCode)
			assert.Equal(t, commonpb.LoadState_LoadStateNotExist, stateResp.State)
		}
	})

	wg.Add(1)
	t.Run("get replicas", func(t *testing.T) {
		defer wg.Done()

		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionID: collectionID,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resp.Replicas))
	})

	wg.Add(1)
	t.Run("get collection statistics from shard", func(t *testing.T) {
		defer wg.Done()
		if !loaded {
			t.Skip("collection not loaded")
			return
		}
		resp, err := proxy.GetStatistics(ctx, &milvuspb.GetStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		rowNumStr := funcutil.KeyValuePair2Map(resp.Stats)["row_count"]
		assert.Equal(t, strconv.Itoa(rowNum), rowNumStr)

		// get statistics of other collection -> fail
		resp, err = proxy.GetStatistics(ctx, &milvuspb.GetStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	// nprobe := 10
	// topk := 10
	// roundDecimal := 6
	// expr := fmt.Sprintf("%s > 0", int64Field)
	// constructPlaceholderGroup := func() *milvuspb.PlaceholderGroup {
	//     values := make([][]byte, 0, nq)
	//     for i := 0; i < nq; i++ {
	//         bs := make([]byte, 0, dim*4)
	//         for j := 0; j < dim; j++ {
	//             var buffer bytes.Buffer
	//             f := rand.Float32()
	//             err := binary.Write(&buffer, common.Endian, f)
	//             assert.NoError(t, err)
	//             bs = append(bs, buffer.Bytes()...)
	//         }
	//         values = append(values, bs)
	//     }
	//
	//     return &milvuspb.PlaceholderGroup{
	//         Placeholders: []*milvuspb.PlaceholderValue{
	//             {
	//                 Tag:    "$0",
	//                 Type:   milvuspb.PlaceholderType_FloatVector,
	//                 Values: values,
	//             },
	//         },
	//     }
	// }
	//
	// constructSearchRequest := func() *milvuspb.SearchRequest {
	//     params := make(map[string]string)
	//     params["nprobe"] = strconv.Itoa(nprobe)
	//     b, err := json.Marshal(params)
	//     assert.NoError(t, err)
	//     plg := constructPlaceholderGroup()
	//     plgBs, err := proto.Marshal(plg)
	//     assert.NoError(t, err)
	//
	//     return &milvuspb.SearchRequest{
	//         Base:             nil,
	//         DbName:           dbName,
	//         CollectionName:   collectionName,
	//         PartitionNames:   nil,
	//         Dsl:              expr,
	//         PlaceholderGroup: plgBs,
	//         DslType:          commonpb.DslType_BoolExprV1,
	//         OutputFields:     nil,
	//         SearchParams: []*commonpb.KeyValuePair{
	//             {
	//                 Key:   MetricTypeKey,
	//                 Value: distance.L2,
	//             },
	//             {
	//                 Key:   SearchParamsKey,
	//                 Value: string(b),
	//             },
	//             {
	//                 Key:   AnnsFieldKey,
	//                 Value: floatVecField,
	//             },
	//             {
	//                 Key:   TopKKey,
	//                 Value: strconv.Itoa(topk),
	//             },
	//             {
	//                 Key:   RoundDecimalKey,
	//                 Value: strconv.Itoa(roundDecimal),
	//             },
	//         },
	//         TravelTimestamp:    0,
	//         GuaranteeTimestamp: 0,
	//     }
	// }

	// TODO(Goose): reopen after joint-tests
	// if loaded {
	//     wg.Add(1)
	//     t.Run("search", func(t *testing.T) {
	//         defer wg.Done()
	//         req := constructSearchRequest()
	//
	//         resp, err := proxy.Search(ctx, req)
	//         assert.NoError(t, err)
	//         assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	//     })
	//
	//     wg.Add(1)
	//     t.Run("search_travel", func(t *testing.T) {
	//         defer wg.Done()
	//         past := time.Now().Add(time.Duration(-1*Params.CommonCfg.RetentionDuration-100) * time.Second)
	//         travelTs := tsoutil.ComposeTSByTime(past, 0)
	//         req := constructSearchRequest()
	//         req.TravelTimestamp = travelTs
	//         //resp, err := proxy.Search(ctx, req)
	//         res, err := proxy.Search(ctx, req)
	//         assert.NoError(t, err)
	//         assert.NotEqual(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
	//     })
	//
	//     wg.Add(1)
	//     t.Run("search_travel_succ", func(t *testing.T) {
	//         defer wg.Done()
	//         past := time.Now().Add(time.Duration(-1*Params.CommonCfg.RetentionDuration+100) * time.Second)
	//         travelTs := tsoutil.ComposeTSByTime(past, 0)
	//         req := constructSearchRequest()
	//         req.TravelTimestamp = travelTs
	//         //resp, err := proxy.Search(ctx, req)
	//         res, err := proxy.Search(ctx, req)
	//         assert.NoError(t, err)
	//         assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
	//     })
	//
	//     wg.Add(1)
	//     t.Run("query", func(t *testing.T) {
	//         defer wg.Done()
	//         //resp, err := proxy.Query(ctx, &milvuspb.QueryRequest{
	//         _, err := proxy.Query(ctx, &milvuspb.QueryRequest{
	//             Base:               nil,
	//             DbName:             dbName,
	//             CollectionName:     collectionName,
	//             Expr:               expr,
	//             OutputFields:       nil,
	//             PartitionNames:     nil,
	//             TravelTimestamp:    0,
	//             GuaranteeTimestamp: 0,
	//         })
	//         assert.NoError(t, err)
	//         // FIXME(dragondriver)
	//         // assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	//         // TODO(dragondriver): compare query result
	//     })
	//
	//     wg.Add(1)
	//     t.Run("query_travel", func(t *testing.T) {
	//         defer wg.Done()
	//         past := time.Now().Add(time.Duration(-1*Params.CommonCfg.RetentionDuration-100) * time.Second)
	//         travelTs := tsoutil.ComposeTSByTime(past, 0)
	//         queryReq := &milvuspb.QueryRequest{
	//             Base:               nil,
	//             DbName:             dbName,
	//             CollectionName:     collectionName,
	//             Expr:               expr,
	//             OutputFields:       nil,
	//             PartitionNames:     nil,
	//             TravelTimestamp:    travelTs,
	//             GuaranteeTimestamp: 0,
	//         }
	//         res, err := proxy.Query(ctx, queryReq)
	//         assert.NoError(t, err)
	//         assert.NotEqual(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
	//     })
	//
	//     wg.Add(1)
	//     t.Run("query_travel_succ", func(t *testing.T) {
	//         defer wg.Done()
	//         past := time.Now().Add(time.Duration(-1*Params.CommonCfg.RetentionDuration+100) * time.Second)
	//         travelTs := tsoutil.ComposeTSByTime(past, 0)
	//         queryReq := &milvuspb.QueryRequest{
	//             Base:               nil,
	//             DbName:             dbName,
	//             CollectionName:     collectionName,
	//             Expr:               expr,
	//             OutputFields:       nil,
	//             PartitionNames:     nil,
	//             TravelTimestamp:    travelTs,
	//             GuaranteeTimestamp: 0,
	//         }
	//         res, err := proxy.Query(ctx, queryReq)
	//         assert.NoError(t, err)
	//         assert.Equal(t, commonpb.ErrorCode_EmptyCollection, res.Status.ErrorCode)
	//     })
	// }

	wg.Add(1)
	t.Run("calculate distance", func(t *testing.T) {
		defer wg.Done()
		opLeft := &milvuspb.VectorsArray{
			Array: &milvuspb.VectorsArray_DataArray{
				DataArray: &schemapb.VectorField{
					Dim: int64(dim),
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: generateFloatVectors(nq, dim),
						},
					},
				},
			},
		}

		opRight := &milvuspb.VectorsArray{
			Array: &milvuspb.VectorsArray_DataArray{
				DataArray: &schemapb.VectorField{
					Dim: int64(dim),
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: generateFloatVectors(nq, dim),
						},
					},
				},
			},
		}

		//resp, err := proxy.CalcDistance(ctx, &milvuspb.CalcDistanceRequest{
		_, err := proxy.CalcDistance(ctx, &milvuspb.CalcDistanceRequest{
			Base:    nil,
			OpLeft:  opLeft,
			OpRight: opRight,
			Params: []*commonpb.KeyValuePair{
				{
					Key:   common.MetricTypeKey,
					Value: distance.L2,
				},
			},
		})
		assert.NoError(t, err)
		// assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// TODO(dragondriver): compare distance

		// TODO(dragondriver): use primary key to calculate distance
	})

	t.Run("get dd channel", func(t *testing.T) {
		resp, _ := proxy.GetDdChannel(ctx, &internalpb.GetDdChannelRequest{})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("get persistent segment info", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("get query segment info", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("loadBalance", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.LoadBalance(ctx, &milvuspb.LoadBalanceRequest{
			Base: nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
	})

	// TODO(dragondriver): dummy

	wg.Add(1)
	t.Run("register link", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.RegisterLink(ctx, &milvuspb.RegisterLinkRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("get metrics", func(t *testing.T) {
		defer wg.Done()
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := proxy.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// get from cache
		resp, err = proxy.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// failed to parse metric type
		resp, err = proxy.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Base:    &commonpb.MsgBase{},
			Request: "not in json format",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// not implemented metric
		notImplemented, err := metricsinfo.ConstructRequestByMetricType("not implemented")
		assert.NoError(t, err)
		resp, err = proxy.GetMetrics(ctx, notImplemented)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("get proxy metrics", func(t *testing.T) {
		defer wg.Done()
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := proxy.GetProxyMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// failed to parse metric type
		resp, err = proxy.GetProxyMetrics(ctx, &milvuspb.GetMetricsRequest{
			Base:    &commonpb.MsgBase{},
			Request: "not in json format",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// not implemented metric
		notImplemented, err := metricsinfo.ConstructRequestByMetricType("not implemented")
		assert.NoError(t, err)
		resp, err = proxy.GetProxyMetrics(ctx, notImplemented)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// unhealthy
		proxy.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err = proxy.GetProxyMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)

		// getProxyMetric failed
		rateCol.Deregister(internalpb.RateType_DMLInsert.String())
		resp, err = proxy.GetProxyMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		rateCol.Register(internalpb.RateType_DMLInsert.String())
	})

	wg.Add(1)
	t.Run("test import", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.ImportRequest{
			CollectionName: collectionName,
			Files:          []string{"f1.json"},
		}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := proxy.Import(context.TODO(), req)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.NoError(t, err)
		// Wait a bit for complete import to start.
		time.Sleep(2 * time.Second)
	})

	wg.Add(1)
	t.Run("test import collection ID not found", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.ImportRequest{
			CollectionName: "bad_collection_name",
			Files:          []string{"f1.json"},
		}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := proxy.Import(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("test import get vChannel fail", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.ImportRequest{
			CollectionName: "bad_collection_name",
			Files:          []string{"f1.json"},
		}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := proxy.Import(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("release collection", func(t *testing.T) {
		defer wg.Done()
		_, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		assert.Equal(t, "", resp.Reason)

		// release collection cache
		resp, err = proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base:           nil,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		assert.Equal(t, "", resp.Reason)
	})

	wg.Add(1)
	t.Run("show in-memory collections after release", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_InMemory,
			CollectionNames: nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.CollectionNames))
	})

	pLoaded := true
	wg.Add(1)
	t.Run("load partitions", func(t *testing.T) {
		defer wg.Done()
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{partitionName},
			ReplicaNumber:  1,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// non-exist partition -> fail
		resp, err = proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{otherPartitionName},
			ReplicaNumber:  1,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// non-exist collection-> fail
		resp, err = proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionNames: []string{partitionName},
			ReplicaNumber:  1,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		f := func() bool {
			resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName,
				CollectionID:   collectionID,
				PartitionNames: []string{partitionName},
				Type:           milvuspb.ShowType_InMemory,
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

			for idx, name := range resp.PartitionNames {
				if name == partitionName && resp.InMemoryPercentages[idx] == 100 {
					return true
				}
			}

			return false
		}

		// waiting for collection to be loaded
		counter := 0
		for !f() {
			if counter > 100 {
				pLoaded = false
				break
			}
			// avoid too frequent rpc call
			time.Sleep(100 * time.Millisecond)
			counter++
		}
	})
	assert.True(t, pLoaded)

	wg.Add(1)
	t.Run("show in-memory partitions", func(t *testing.T) {
		defer wg.Done()
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// default partition?
		assert.Equal(t, 1, len(resp.PartitionNames))

		// show partition not in-memory -> fail
		resp, err = proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: []string{otherPartitionName},
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// non-exist collection -> fail
		resp, err = proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			CollectionID:   collectionID,
			PartitionNames: []string{partitionName},
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		{
			resp, err := proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
				CollectionName: collectionName,
				PartitionNames: []string{partitionName},
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
			assert.NotEqual(t, int64(0), resp.Progress)
		}

		{
			resp, err := proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
				CollectionName: collectionName,
				PartitionNames: []string{otherPartitionName},
			})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
			assert.Equal(t, int64(0), resp.Progress)
		}
	})

	wg.Add(1)
	t.Run("insert partition", func(t *testing.T) {
		defer wg.Done()
		req := constructPartitionInsertRequest()

		resp, err := proxy.Insert(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, rowNum, len(resp.SuccIndex))
		assert.Equal(t, 0, len(resp.ErrIndex))
		assert.Equal(t, int64(rowNum), resp.InsertCnt)
	})

	wg.Add(1)
	t.Run("get partition statistics from shard", func(t *testing.T) {
		defer wg.Done()
		if !pLoaded {
			t.Skip("partition not loaded")
		}
		resp, err := proxy.GetStatistics(ctx, &milvuspb.GetStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{partitionName},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		rowNumStr := funcutil.KeyValuePair2Map(resp.Stats)["row_count"]
		assert.Equal(t, strconv.Itoa(rowNum), rowNumStr)

		// non-exist partition -> fail
		resp, err = proxy.GetStatistics(ctx, &milvuspb.GetStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{otherPartitionName},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// non-exist collection -> fail
		resp, err = proxy.GetStatistics(ctx, &milvuspb.GetStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionNames: []string{partitionName},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("get collection statistics from hybrid", func(t *testing.T) {
		defer wg.Done()
		if !flushed {
			t.Skip("flush operation was not done")
		}
		if !pLoaded {
			t.Skip("partition not loaded")
		}
		resp, err := proxy.GetStatistics(ctx, &milvuspb.GetStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		rowNumStr := funcutil.KeyValuePair2Map(resp.Stats)["row_count"]
		assert.Equal(t, strconv.Itoa(rowNum*2), rowNumStr)

		// get statistics of other collection -> fail
		resp, err = proxy.GetStatistics(ctx, &milvuspb.GetStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("release partition", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ReleasePartitions(ctx, &milvuspb.ReleasePartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{partitionName},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("show in-memory partitions after release partition", func(t *testing.T) {
		defer wg.Done()
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// default partition
		assert.Equal(t, 0, len(resp.PartitionNames))

		resp, err = proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: []string{partitionName}, // released
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("drop partition", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// invalidate meta cache
		resp, err = proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// drop non-exist partition -> fail

		resp, err = proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("has partition after drop partition", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.False(t, resp.Value)
	})

	wg.Add(1)
	t.Run("show partitions after drop partition", func(t *testing.T) {
		defer wg.Done()
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_All,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// default partition
		assert.Equal(t, 1, len(resp.PartitionNames))
	})

	wg.Add(1)
	t.Run("drop index", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropIndex(ctx, &milvuspb.DropIndexRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			IndexName:      indexName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("Delete", func(t *testing.T) {
		defer wg.Done()
		_, err := proxy.Delete(ctx, &milvuspb.DeleteRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
			Expr:           "",
		})
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("upsert when autoID == true", func(t *testing.T) {
		defer wg.Done()
		req := constructCollectionUpsertRequest()

		resp, err := proxy.Upsert(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UpsertAutoIDTrue, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.SuccIndex))
		assert.Equal(t, rowNum, len(resp.ErrIndex))
		assert.Equal(t, int64(0), resp.UpsertCnt)
	})

	wg.Add(1)
	t.Run("drop collection", func(t *testing.T) {
		defer wg.Done()
		_, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		assert.Equal(t, "", resp.Reason)

		// invalidate meta cache
		resp, err = proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// release collection load cache
		resp, err = proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base:           nil,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("has collection after drop collection", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.False(t, resp.Value)
	})

	wg.Add(1)
	t.Run("show all collections after drop collection", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_All,
			CollectionNames: nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.CollectionNames))
	})

	username := "test_username_" + funcutil.RandomString(15)
	password := "password"

	wg.Add(1)
	t.Run("credential CREATE api", func(t *testing.T) {
		defer wg.Done()

		// 1. create credential
		constructCreateCredentialRequest := func() *milvuspb.CreateCredentialRequest {
			return &milvuspb.CreateCredentialRequest{
				Base:     nil,
				Username: username,
				Password: crypto.Base64Encode(password),
			}
		}
		createCredentialReq := constructCreateCredentialRequest()
		// success
		resp, err := proxy.CreateCredential(ctx, createCredentialReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// recreate -> fail (user already exists)
		resp, err = proxy.CreateCredential(ctx, createCredentialReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// invalid username
		createCredentialReq.Username = "11_invalid_username"
		resp, err = proxy.CreateCredential(ctx, createCredentialReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// invalid password (not decode)
		createCredentialReq.Password = "not_decoded_password"
		resp, err = proxy.CreateCredential(ctx, createCredentialReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// invalid password (length gt 256)
		createCredentialReq.Password = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmnnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrsssssssssstttttttttttuuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwwxxxxxxxxxxyyyyyyyyyzzzzzzzzzzz"
		resp, err = proxy.CreateCredential(ctx, createCredentialReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("credential UPDATE api", func(t *testing.T) {
		defer wg.Done()
		rootCtx := ctx
		fooCtx := GetContext(context.Background(), "foo:123456")
		ctx = fooCtx
		defer func() {
			ctx = rootCtx
		}()

		// 2. update credential
		newPassword := "new_password"
		constructUpdateCredentialRequest := func() *milvuspb.UpdateCredentialRequest {
			return &milvuspb.UpdateCredentialRequest{
				Base:        nil,
				Username:    username,
				OldPassword: crypto.Base64Encode(password),
				NewPassword: crypto.Base64Encode(newPassword),
			}
		}
		// cannot update non-existing user's password
		updateCredentialReq := constructUpdateCredentialRequest()
		updateCredentialReq.Username = "test_username_" + funcutil.RandomString(15)
		updateResp, err := proxy.UpdateCredential(ctx, updateCredentialReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, updateResp.ErrorCode)

		// success
		updateCredentialReq.Username = username
		updateCredentialReq.NewPassword = crypto.Base64Encode(newPassword)
		updateResp, err = proxy.UpdateCredential(ctx, updateCredentialReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, updateResp.ErrorCode)

		// invalid old password (not decode)
		updateCredentialReq.OldPassword = password
		updateCredentialReq.NewPassword = crypto.Base64Encode(newPassword)
		updateResp, err = proxy.UpdateCredential(ctx, updateCredentialReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, updateResp.ErrorCode)

		// invalid new password (not decode)
		updateCredentialReq.OldPassword = crypto.Base64Encode(password)
		updateCredentialReq.NewPassword = newPassword
		updateResp, err = proxy.UpdateCredential(ctx, updateCredentialReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, updateResp.ErrorCode)

		// invalid password (length gt 256)
		updateCredentialReq.NewPassword = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmnnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrsssssssssstttttttttttuuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwwxxxxxxxxxxyyyyyyyyyzzzzzzzzzzz"
		updateResp, err = proxy.UpdateCredential(ctx, updateCredentialReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, updateResp.ErrorCode)

		// wrong password
		updateCredentialReq.OldPassword = crypto.Base64Encode("wrong_password")
		updateCredentialReq.NewPassword = crypto.Base64Encode(newPassword)
		updateResp, err = proxy.UpdateCredential(ctx, updateCredentialReq)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, updateResp.ErrorCode)

		// super user
		paramtable.Get().Save(Params.CommonCfg.SuperUsers.Key, "root")
		defer paramtable.Get().Reset(Params.CommonCfg.SuperUsers.Key)
		updateCredentialReq.OldPassword = crypto.Base64Encode("wrong_password")
		updateCredentialReq.NewPassword = crypto.Base64Encode(newPassword)
		updateResp, err = proxy.UpdateCredential(rootCtx, updateCredentialReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, updateResp.ErrorCode)
	})

	wg.Add(1)
	t.Run("credential GET api", func(t *testing.T) {
		defer wg.Done()

		// 3. get credential
		newPassword := "new_password"
		constructGetCredentialRequest := func() *rootcoordpb.GetCredentialRequest {
			return &rootcoordpb.GetCredentialRequest{
				Base:     nil,
				Username: username,
			}
		}
		getCredentialReq := constructGetCredentialRequest()
		getResp, err := rootCoordClient.GetCredential(ctx, getCredentialReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, getResp.Status.ErrorCode)
		assert.True(t, passwordVerify(ctx, username, newPassword, globalMetaCache))

		getCredentialReq.Username = "("
		getResp, err = rootCoordClient.GetCredential(ctx, getCredentialReq)
		assert.Error(t, err)
	})

	wg.Add(1)
	t.Run("credential LIST api", func(t *testing.T) {
		defer wg.Done()

		// 4. list credential usernames
		constructListCredUsersRequest := func() *milvuspb.ListCredUsersRequest {
			return &milvuspb.ListCredUsersRequest{
				Base: nil,
			}
		}
		listCredUsersReq := constructListCredUsersRequest()
		listUsersResp, err := proxy.ListCredUsers(ctx, listCredUsersReq)
		assert.NoError(t, err)
		assert.True(t, len(listUsersResp.Usernames) > 0)
	})

	wg.Add(1)
	t.Run("credential DELETE api", func(t *testing.T) {
		defer wg.Done()

		// 5. delete credential
		constructDelCredRequest := func() *milvuspb.DeleteCredentialRequest {
			return &milvuspb.DeleteCredentialRequest{
				Base:     nil,
				Username: username,
			}
		}
		delCredReq := constructDelCredRequest()

		deleteResp, err := proxy.DeleteCredential(ctx, delCredReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, deleteResp.ErrorCode)
	})

	testProxyRole(ctx, t, proxy)
	testProxyPrivilege(ctx, t, proxy)
	testProxyRefreshPolicyInfoCache(ctx, t, proxy)

	// proxy unhealthy
	//
	//notStateCode := "not state code"
	//proxy.stateCode.Store(notStateCode)
	//
	//t.Run("GetComponentStates fail", func(t *testing.T) {
	//	_, err := proxy.GetComponentStates(ctx)
	//	assert.Error(t, err)
	//})

	proxy.UpdateStateCode(commonpb.StateCode_Abnormal)

	wg.Add(1)
	t.Run("CreateCollection fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropCollection fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("HasCollection fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("LoadCollection fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("ReleaseCollection fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DescribeCollection fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetCollectionStatistics fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("ShowCollections fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("alter collection fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: "cn",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreatePartition fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropPartition fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("HasPartition fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("LoadPartitions fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("ReleasePartitions fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ReleasePartitions(ctx, &milvuspb.ReleasePartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetPartitionStatistics fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("ShowPartitions fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetLoadingProgress fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetLoadState fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreateIndex fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DescribeIndex fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetIndexStatistics fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropIndex fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropIndex(ctx, &milvuspb.DropIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetIndexBuildProgress fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexBuildProgress(ctx, &milvuspb.GetIndexBuildProgressRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetIndexState fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexState(ctx, &milvuspb.GetIndexStateRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Insert fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Insert(ctx, &milvuspb.InsertRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Delete fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Delete(ctx, &milvuspb.DeleteRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Upsert fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Upsert(ctx, &milvuspb.UpsertRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Search fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Search(ctx, &milvuspb.SearchRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Flush fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Flush(ctx, &milvuspb.FlushRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Query fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Query(ctx, &milvuspb.QueryRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreateAlias fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropAlias fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("AlterAlias fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetPersistentSegmentInfo fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetQuerySegmentInfo fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("LoadBalance fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.LoadBalance(ctx, &milvuspb.LoadBalanceRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("RegisterLink fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.RegisterLink(ctx, &milvuspb.RegisterLinkRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetMetrics fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("InvalidateCredCache fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{Username: "xxx"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("UpdateCredentialCache fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.UpdateCredentialCache(ctx, &proxypb.UpdateCredCacheRequest{Username: "xxx", Password: "xxx"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreateCredential fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateCredential(ctx, &milvuspb.CreateCredentialRequest{Username: "xxx"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("UpdateCredential fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.UpdateCredential(ctx, &milvuspb.UpdateCredentialRequest{Username: "xxx"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DeleteCredential fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DeleteCredential(ctx, &milvuspb.DeleteCredentialRequest{Username: "xxx"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("ListCredUsers fail, unhealthy", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ListCredUsers(ctx, &milvuspb.ListCredUsersRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("InvalidateCollectionMetaCache failed", func(t *testing.T) {
		resp, err := proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	testProxyRoleUnhealthy(ctx, t, proxy)
	testProxyPrivilegeUnhealthy(ctx, t, proxy)
	testProxyRefreshPolicyInfoCacheUnhealthy(ctx, t, proxy)

	proxy.UpdateStateCode(commonpb.StateCode_Healthy)

	// queue full

	ddParallel := proxy.sched.ddQueue.getMaxTaskNum()
	proxy.sched.ddQueue.setMaxTaskNum(0)

	wg.Add(1)
	t.Run("CreateCollection fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropCollection fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("HasCollection fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("LoadCollection fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("ReleaseCollection fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DescribeCollection fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetCollectionStatistics fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("ShowCollections fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("alter collection fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: "cn",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreatePartition fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropPartition fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("HasPartition fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("LoadPartitions fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("ReleasePartitions fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ReleasePartitions(ctx, &milvuspb.ReleasePartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetPartitionStatistics fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("ShowPartitions fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreateIndex fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DescribeIndex fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetIndexStatistics fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropIndex fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropIndex(ctx, &milvuspb.DropIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetIndexBuildProgress fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexBuildProgress(ctx, &milvuspb.GetIndexBuildProgressRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetIndexState fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexState(ctx, &milvuspb.GetIndexStateRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Flush fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Flush(ctx, &milvuspb.FlushRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreateAlias fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropAlias fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("AlterAlias fail, dd queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	proxy.sched.ddQueue.setMaxTaskNum(ddParallel)

	dmParallelism := proxy.sched.dmQueue.getMaxTaskNum()
	proxy.sched.dmQueue.setMaxTaskNum(0)

	wg.Add(1)
	t.Run("Insert fail, dm queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Insert(ctx, &milvuspb.InsertRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Delete fail, dm queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Delete(ctx, &milvuspb.DeleteRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Upsert fail, dm queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Upsert(ctx, &milvuspb.UpsertRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	proxy.sched.dmQueue.setMaxTaskNum(dmParallelism)

	dqParallelism := proxy.sched.dqQueue.getMaxTaskNum()
	proxy.sched.dqQueue.setMaxTaskNum(0)

	wg.Add(1)
	t.Run("Search fail, dq queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Search(ctx, &milvuspb.SearchRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Query fail, dq queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Query(ctx, &milvuspb.QueryRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	proxy.sched.dqQueue.setMaxTaskNum(dqParallelism)

	// timeout

	timeout := time.Nanosecond
	shortCtx, shortCancel := context.WithTimeout(ctx, timeout)
	defer shortCancel()
	time.Sleep(timeout)

	wg.Add(1)
	t.Run("CreateCollection, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateCollection(shortCtx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropCollection fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropCollection(shortCtx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("HasCollection fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasCollection(shortCtx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("LoadCollection fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.LoadCollection(shortCtx, &milvuspb.LoadCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("ReleaseCollection fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ReleaseCollection(shortCtx, &milvuspb.ReleaseCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DescribeCollection fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DescribeCollection(shortCtx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetCollectionStatistics fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetCollectionStatistics(shortCtx, &milvuspb.GetCollectionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("ShowCollections fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowCollections(shortCtx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("alter collection fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.AlterCollection(shortCtx, &milvuspb.AlterCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: "cn",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreatePartition fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreatePartition(shortCtx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropPartition fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropPartition(shortCtx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("HasPartition fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.HasPartition(shortCtx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("LoadPartitions fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.LoadPartitions(shortCtx, &milvuspb.LoadPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("ReleasePartitions fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ReleasePartitions(shortCtx, &milvuspb.ReleasePartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetPartitionStatistics fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetPartitionStatistics(shortCtx, &milvuspb.GetPartitionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("ShowPartitions fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.ShowPartitions(shortCtx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetLoadingProgress fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetLoadingProgress(shortCtx, &milvuspb.GetLoadingProgressRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreateIndex fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateIndex(shortCtx, &milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DescribeIndex fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DescribeIndex(shortCtx, &milvuspb.DescribeIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetIndexStatistics fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexStatistics(shortCtx, &milvuspb.GetIndexStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropIndex fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropIndex(shortCtx, &milvuspb.DropIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetIndexBuildProgress fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexBuildProgress(shortCtx, &milvuspb.GetIndexBuildProgressRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("GetIndexState fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.GetIndexState(shortCtx, &milvuspb.GetIndexStateRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Flush fail, timeout", func(t *testing.T) {
		defer wg.Done()
		_, err := proxy.Flush(shortCtx, &milvuspb.FlushRequest{})
		assert.NoError(t, err)
		// FIXME(dragondriver)
		// assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Insert fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Insert(shortCtx, &milvuspb.InsertRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Delete fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Delete(shortCtx, &milvuspb.DeleteRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Update fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Upsert(shortCtx, &milvuspb.UpsertRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Search fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Search(shortCtx, &milvuspb.SearchRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("Query fail, dq queue full", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.Query(shortCtx, &milvuspb.QueryRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreateAlias fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateAlias(shortCtx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DropAlias fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DropAlias(shortCtx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("AlterAlias fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.AlterAlias(shortCtx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("CreateCredential fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreateCredential(shortCtx, &milvuspb.CreateCredentialRequest{Username: "xxx"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("UpdateCredential fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.UpdateCredential(shortCtx, &milvuspb.UpdateCredentialRequest{Username: "xxx"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DeleteCredential fail, user root cannot be deleted", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DeleteCredential(shortCtx, &milvuspb.DeleteCredentialRequest{Username: "root"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("DeleteCredential fail, timeout", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.DeleteCredential(shortCtx, &milvuspb.DeleteCredentialRequest{Username: "xxx"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	testProxyRoleTimeout(shortCtx, t, proxy)
	testProxyPrivilegeTimeout(shortCtx, t, proxy)

	constructCollectionSchema = func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       false,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema = constructCollectionSchema()

	constructCreateCollectionRequest = func() *milvuspb.CreateCollectionRequest {
		bs, err := proto.Marshal(schema)
		assert.NoError(t, err)
		return &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         bs,
			ShardsNum:      shardsNum,
		}
	}
	createCollectionReq = constructCreateCollectionRequest()

	constructPartitionReqUpsertRequestValid := func() *milvuspb.UpsertRequest {
		pkFieldData := newScalarFieldData(schema.Fields[0], int64Field, rowNum)
		fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
		hashKeys := generateHashKeys(rowNum)
		return &milvuspb.UpsertRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
			FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		}
	}

	constructPartitionReqUpsertRequestInvalid := func() *milvuspb.UpsertRequest {
		pkFieldData := newScalarFieldData(schema.Fields[0], int64Field, rowNum)
		fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
		hashKeys := generateHashKeys(rowNum)
		return &milvuspb.UpsertRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  "%$@",
			FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		}
	}

	constructCollectionUpsertRequestValid := func() *milvuspb.UpsertRequest {
		pkFieldData := newScalarFieldData(schema.Fields[0], int64Field, rowNum)
		fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
		hashKeys := generateHashKeys(rowNum)
		return &milvuspb.UpsertRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
			FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		}
	}

	wg.Add(1)
	t.Run("create collection upsert valid", func(t *testing.T) {
		defer wg.Done()
		req := createCollectionReq
		resp, err := proxy.CreateCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		reqInvalidField := constructCreateCollectionRequest()
		schema := constructCollectionSchema()
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			Name:     "StringField",
			DataType: schemapb.DataType_String,
		})
		bs, err := proto.Marshal(schema)
		assert.NoError(t, err)
		reqInvalidField.CollectionName = "invalid_field_coll_upsert_valid"
		reqInvalidField.Schema = bs

		resp, err = proxy.CreateCollection(ctx, reqInvalidField)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

	})

	wg.Add(1)
	t.Run("create partition", func(t *testing.T) {
		defer wg.Done()
		resp, err := proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// create partition with non-exist collection -> fail
		resp, err = proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("upsert partition", func(t *testing.T) {
		defer wg.Done()
		req := constructPartitionReqUpsertRequestValid()

		resp, err := proxy.Upsert(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, rowNum, len(resp.SuccIndex))
		assert.Equal(t, 0, len(resp.ErrIndex))
		assert.Equal(t, int64(rowNum), resp.UpsertCnt)
	})

	wg.Add(1)
	t.Run("upsert when occurs unexpected error like illegal partition name", func(t *testing.T) {
		defer wg.Done()
		req := constructPartitionReqUpsertRequestInvalid()

		resp, err := proxy.Upsert(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.SuccIndex))
		assert.Equal(t, rowNum, len(resp.ErrIndex))
		assert.Equal(t, int64(0), resp.UpsertCnt)
	})

	wg.Add(1)
	t.Run("upsert when autoID == false", func(t *testing.T) {
		defer wg.Done()
		req := constructCollectionUpsertRequestValid()

		resp, err := proxy.Upsert(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, rowNum, len(resp.SuccIndex))
		assert.Equal(t, 0, len(resp.ErrIndex))
		assert.Equal(t, int64(rowNum), resp.UpsertCnt)
	})

	testServer.gracefulStop()

	wg.Wait()
	cancel()
}

func testProxyRole(ctx context.Context, t *testing.T, proxy *Proxy) {
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("Create Role", func(t *testing.T) {
		defer wg.Done()

		entity := &milvuspb.RoleEntity{Name: " "}
		resp, _ := proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{Entity: entity})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		entity.Name = "unit_test"
		resp, _ = proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{Entity: entity})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{Entity: entity})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: entity.Name})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("Drop Role", func(t *testing.T) {
		defer wg.Done()

		resp, _ := proxy.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: " "})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: "public"})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		username := "root"
		roleName := "unit_test"

		roleEntity := &milvuspb.RoleEntity{Name: roleName}
		roleResp, _ := proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{Entity: roleEntity})
		assert.Equal(t, commonpb.ErrorCode_Success, roleResp.ErrorCode)

		privilegeRequest := &milvuspb.OperatePrivilegeRequest{
			Type: milvuspb.OperatePrivilegeType_Grant,
			Entity: &milvuspb.GrantEntity{
				ObjectName: "col1",
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				Role:       &milvuspb.RoleEntity{Name: roleName},
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeLoad.String())}},
			},
		}
		privilegeResp, _ := proxy.OperatePrivilege(ctx, privilegeRequest)
		assert.Equal(t, commonpb.ErrorCode_Success, privilegeResp.ErrorCode)

		userResp, _ := proxy.SelectUser(ctx, &milvuspb.SelectUserRequest{User: &milvuspb.UserEntity{Name: username}, IncludeRoleInfo: true})
		assert.Equal(t, commonpb.ErrorCode_Success, userResp.Status.ErrorCode)
		roleNumOfUser := len(userResp.Results[0].Roles)

		roleResp, _ = proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{
			Username: username,
			RoleName: roleName,
			Type:     milvuspb.OperateUserRoleType_AddUserToRole,
		})
		assert.Equal(t, commonpb.ErrorCode_Success, roleResp.ErrorCode)

		resp, _ = proxy.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: roleName})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		privilegeRequest.Type = milvuspb.OperatePrivilegeType_Revoke
		privilegeResp, _ = proxy.OperatePrivilege(ctx, privilegeRequest)
		assert.Equal(t, commonpb.ErrorCode_Success, privilegeResp.ErrorCode)

		roleResp, _ = proxy.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: roleName})
		assert.Equal(t, commonpb.ErrorCode_Success, roleResp.ErrorCode)

		userResp, _ = proxy.SelectUser(ctx, &milvuspb.SelectUserRequest{User: &milvuspb.UserEntity{Name: username}, IncludeRoleInfo: true})
		assert.Equal(t, commonpb.ErrorCode_Success, userResp.Status.ErrorCode)
		assert.Equal(t, roleNumOfUser, len(userResp.Results[0].Roles))
	})

	wg.Add(1)
	t.Run("Operate User Role", func(t *testing.T) {
		defer wg.Done()

		username := "root"
		roleName := "public"
		// AddUserToRole
		resp, _ := proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: "  "})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: username, RoleName: "  "})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: "not_existed", RoleName: roleName})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: username, RoleName: "not_existed"})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: username, RoleName: roleName})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: username, RoleName: roleName, Type: milvuspb.OperateUserRoleType_RemoveUserFromRole})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: "NotExisted", RoleName: roleName, Type: milvuspb.OperateUserRoleType_RemoveUserFromRole})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run("Select Role", func(t *testing.T) {
		defer wg.Done()

		resp, _ := proxy.SelectRole(ctx, &milvuspb.SelectRoleRequest{Role: &milvuspb.RoleEntity{Name: "  "}})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		resp, _ = proxy.SelectRole(ctx, &milvuspb.SelectRoleRequest{})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		roleNum := len(resp.Results)

		resp, _ = proxy.SelectRole(ctx, &milvuspb.SelectRoleRequest{Role: &milvuspb.RoleEntity{Name: "not_existed"}})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.Results))

		roleName := "unit_test"
		roleResp, _ := proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: roleName}})
		assert.Equal(t, commonpb.ErrorCode_Success, roleResp.ErrorCode)

		resp, _ = proxy.SelectRole(ctx, &milvuspb.SelectRoleRequest{})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, roleNum+1, len(resp.Results))

		roleResp, _ = proxy.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: roleName})
		assert.Equal(t, commonpb.ErrorCode_Success, roleResp.ErrorCode)

		opResp, _ := proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: "root", RoleName: "admin"})
		assert.Equal(t, commonpb.ErrorCode_Success, opResp.ErrorCode)

		resp, _ = proxy.SelectRole(ctx, &milvuspb.SelectRoleRequest{Role: &milvuspb.RoleEntity{Name: "admin"}, IncludeUserInfo: true})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.NotEqual(t, 0, len(resp.Results))
		assert.NotEqual(t, 0, len(resp.Results[0].Users))

		opResp, _ = proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: "root", RoleName: "admin", Type: milvuspb.OperateUserRoleType_RemoveUserFromRole})
		assert.Equal(t, commonpb.ErrorCode_Success, opResp.ErrorCode)
	})

	wg.Add(1)
	t.Run("Select User", func(t *testing.T) {
		defer wg.Done()

		entity := &milvuspb.UserEntity{Name: "  "}
		resp, _ := proxy.SelectUser(ctx, &milvuspb.SelectUserRequest{User: entity})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		entity.Name = "not_existed"
		resp, _ = proxy.SelectUser(ctx, &milvuspb.SelectUserRequest{User: entity})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.Results))

		entity.Name = "root"
		resp, _ = proxy.SelectUser(ctx, &milvuspb.SelectUserRequest{User: entity})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.NotEqual(t, 0, len(resp.Results))

		opResp, _ := proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: "root", RoleName: "admin"})
		assert.Equal(t, commonpb.ErrorCode_Success, opResp.ErrorCode)

		resp, _ = proxy.SelectUser(ctx, &milvuspb.SelectUserRequest{User: entity, IncludeRoleInfo: true})
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.NotEqual(t, 0, len(resp.Results))
		assert.NotEqual(t, 0, len(resp.Results[0].Roles))

		opResp, _ = proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: "root", RoleName: "admin", Type: milvuspb.OperateUserRoleType_RemoveUserFromRole})
		assert.Equal(t, commonpb.ErrorCode_Success, opResp.ErrorCode)
	})

	wg.Wait()
}

func testProxyRoleUnhealthy(ctx context.Context, t *testing.T, proxy *Proxy) {
	testProxyRoleFail(ctx, t, proxy, "unhealthy")
}

func testProxyRoleTimeout(ctx context.Context, t *testing.T, proxy *Proxy) {
	testProxyRoleFail(ctx, t, proxy, "timeout")
}

func testProxyRoleFail(ctx context.Context, t *testing.T, proxy *Proxy, reason string) {
	var wg sync.WaitGroup
	roleName := "xxx"

	wg.Add(1)
	t.Run(fmt.Sprintf("CreateRole fail, %s", reason), func(t *testing.T) {
		defer wg.Done()
		resp, _ := proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: roleName}})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run(fmt.Sprintf("DropRole fail, %s", reason), func(t *testing.T) {
		defer wg.Done()
		resp, _ := proxy.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: roleName})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run(fmt.Sprintf("OperateUserRole fail, %s", reason), func(t *testing.T) {
		defer wg.Done()
		resp, _ := proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{Username: "root", RoleName: "public"})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run(fmt.Sprintf("SelectRole fail, %s", reason), func(t *testing.T) {
		defer wg.Done()
		resp, _ := proxy.SelectRole(ctx, &milvuspb.SelectRoleRequest{})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Add(1)
	t.Run(fmt.Sprintf("SelectUser fail, %s", reason), func(t *testing.T) {
		defer wg.Done()
		resp, _ := proxy.SelectUser(ctx, &milvuspb.SelectUserRequest{})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	wg.Wait()
}

func testProxyPrivilege(ctx context.Context, t *testing.T, proxy *Proxy) {
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("Operate Privilege, Select Grant", func(t *testing.T) {
		defer wg.Done()

		// GrantPrivilege
		req := &milvuspb.OperatePrivilegeRequest{}
		resp, _ := proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity = &milvuspb.GrantEntity{}
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity.Grantor = &milvuspb.GrantorEntity{}
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity.Grantor.Privilege = &milvuspb.PrivilegeEntity{}
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity.Grantor.Privilege.Name = util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeAll.String())

		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity.Object = &milvuspb.ObjectEntity{}
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity.Object.Name = commonpb.ObjectType_Global.String()
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity.ObjectName = "col1"
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity.Role = &milvuspb.RoleEntity{}
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity.Role = &milvuspb.RoleEntity{Name: "admin"}
		resp, _ = proxy.OperatePrivilege(context.Background(), req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		req.Entity.Grantor.Privilege.Name = "not existed"
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		req.Entity.Grantor.Privilege.Name = util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeAll.String())
		req.Entity.Object.Name = "not existed"
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		req.Entity.Object.Name = commonpb.ObjectType_Global.String()

		req.Entity.Role.Name = "not existed"
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		req.Entity.Role = &milvuspb.RoleEntity{Name: "admin"}

		roleReq := &milvuspb.OperatePrivilegeRequest{
			Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: "public"},
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				ObjectName: "col1",
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeLoad.String())}},
			},
			Type: milvuspb.OperatePrivilegeType_Grant,
		}
		resp, _ = proxy.OperatePrivilege(ctx, roleReq)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// select grant
		selectReq := &milvuspb.SelectGrantRequest{}
		results, _ := proxy.SelectGrant(ctx, selectReq)
		assert.NotEqual(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)

		selectReq.Entity = &milvuspb.GrantEntity{}
		results, _ = proxy.SelectGrant(ctx, selectReq)
		assert.NotEqual(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)

		selectReq.Entity.Object = &milvuspb.ObjectEntity{}
		results, _ = proxy.SelectGrant(ctx, selectReq)
		assert.NotEqual(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)

		selectReq.Entity.Object.Name = commonpb.ObjectType_Collection.String()
		results, _ = proxy.SelectGrant(ctx, selectReq)
		assert.NotEqual(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)

		selectReq.Entity.ObjectName = "col1"
		results, _ = proxy.SelectGrant(ctx, selectReq)
		assert.NotEqual(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)

		selectReq.Entity.Role = &milvuspb.RoleEntity{}
		results, _ = proxy.SelectGrant(ctx, selectReq)
		assert.NotEqual(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)

		selectReq.Entity.Role = &milvuspb.RoleEntity{Name: "public"}

		results, _ = proxy.SelectGrant(ctx, selectReq)
		assert.Equal(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)
		assert.NotEqual(t, 0, len(results.Entities))

		selectReq.Entity.Object.Name = "not existed"
		results, _ = proxy.SelectGrant(ctx, selectReq)
		assert.NotEqual(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)
		selectReq.Entity.Object.Name = commonpb.ObjectType_Collection.String()

		selectReq.Entity.Role = &milvuspb.RoleEntity{Name: "not existed"}
		results, _ = proxy.SelectGrant(ctx, selectReq)
		assert.NotEqual(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)

		results, _ = proxy.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
			Entity: &milvuspb.GrantEntity{
				Role: &milvuspb.RoleEntity{Name: "public"},
			},
		})
		assert.Equal(t, commonpb.ErrorCode_Success, results.Status.ErrorCode)
		assert.NotEqual(t, 0, len(results.Entities))

		req.Type = milvuspb.OperatePrivilegeType_Revoke
		resp, _ = proxy.OperatePrivilege(ctx, req)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		roleReq.Type = milvuspb.OperatePrivilegeType_Revoke
		resp, _ = proxy.OperatePrivilege(ctx, roleReq)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		roleReq = &milvuspb.OperatePrivilegeRequest{
			Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: "public"},
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				ObjectName: "col1",
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: util.AnyWord}},
			},
			Type: milvuspb.OperatePrivilegeType_Grant,
		}
		resp, _ = proxy.OperatePrivilege(ctx, roleReq)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		roleReq.Type = milvuspb.OperatePrivilegeType_Revoke
		resp, _ = proxy.OperatePrivilege(ctx, roleReq)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		roleReq = &milvuspb.OperatePrivilegeRequest{
			Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: "public"},
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
				ObjectName: "col1",
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeManageOwnership.String())}},
			},
			Type: milvuspb.OperatePrivilegeType_Grant,
		}
		resp, _ = proxy.OperatePrivilege(ctx, roleReq)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		roleReq.Type = milvuspb.OperatePrivilegeType_Revoke
		resp, _ = proxy.OperatePrivilege(ctx, roleReq)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Wait()
}

func testProxyPrivilegeUnhealthy(ctx context.Context, t *testing.T, proxy *Proxy) {
	testProxyPrivilegeFail(ctx, t, proxy, "unhealthy")
}

func testProxyPrivilegeTimeout(ctx context.Context, t *testing.T, proxy *Proxy) {
	testProxyPrivilegeFail(ctx, t, proxy, "timeout")
}

func testProxyPrivilegeFail(ctx context.Context, t *testing.T, proxy *Proxy, reason string) {
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run(fmt.Sprintf("Operate Grant fail, %s", reason), func(t *testing.T) {
		defer wg.Done()
		resp, _ := proxy.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{
			Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: "admin"},
				ObjectName: "col1",
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: util.MetaStore2API(commonpb.ObjectPrivilege_PrivilegeAll.String())}},
			},
		})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	wg.Add(1)
	t.Run(fmt.Sprintf("SelectGrant fail, %s", reason), func(t *testing.T) {
		defer wg.Done()

		resp, _ := proxy.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
			Entity: &milvuspb.GrantEntity{
				Role: &milvuspb.RoleEntity{Name: "admin"},
			},
		})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})
	wg.Wait()
}

func testProxyRefreshPolicyInfoCache(ctx context.Context, t *testing.T, proxy *Proxy) {
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("RefreshPolicyInfoCache", func(t *testing.T) {
		defer wg.Done()

		resp, err := proxy.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: int32(typeutil.CacheAddUserToRole),
			OpKey:  funcutil.EncodeUserRoleCache("foo", "public"),
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		_, err = proxy.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
		assert.Error(t, err)

		_, err = proxy.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: 100,
			OpKey:  funcutil.EncodeUserRoleCache("foo", "public"),
		})
		assert.Error(t, err)
	})
	wg.Wait()
}

func testProxyRefreshPolicyInfoCacheUnhealthy(ctx context.Context, t *testing.T, proxy *Proxy) {
	testProxyRefreshPolicyInfoCacheFail(ctx, t, proxy, "unhealthy")
}

func testProxyRefreshPolicyInfoCacheFail(ctx context.Context, t *testing.T, proxy *Proxy, reason string) {
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run(fmt.Sprintf("RefreshPolicyInfoCache fail, %s", reason), func(t *testing.T) {
		defer wg.Done()
		resp, _ := proxy.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: int32(typeutil.CacheAddUserToRole),
			OpKey:  funcutil.EncodeUserRoleCache("foo", "public"),
		})
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})
	wg.Wait()
}

func Test_GetCompactionState(t *testing.T) {
	t.Run("get compaction state", func(t *testing.T) {
		datacoord := &DataCoordMock{}
		proxy := &Proxy{dataCoord: datacoord}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := proxy.GetCompactionState(context.TODO(), nil)
		assert.EqualValues(t, &milvuspb.GetCompactionStateResponse{}, resp)
		assert.NoError(t, err)
	})

	t.Run("get compaction state with unhealthy proxy", func(t *testing.T) {
		datacoord := &DataCoordMock{}
		proxy := &Proxy{dataCoord: datacoord}
		proxy.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := proxy.GetCompactionState(context.TODO(), nil)
		assert.EqualValues(t, unhealthyStatus(), resp.Status)
		assert.NoError(t, err)
	})
}

func Test_ManualCompaction(t *testing.T) {
	t.Run("test manual compaction", func(t *testing.T) {
		datacoord := &DataCoordMock{}
		proxy := &Proxy{dataCoord: datacoord}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := proxy.ManualCompaction(context.TODO(), nil)
		assert.EqualValues(t, &milvuspb.ManualCompactionResponse{}, resp)
		assert.NoError(t, err)
	})
	t.Run("test manual compaction with unhealthy", func(t *testing.T) {
		datacoord := &DataCoordMock{}
		proxy := &Proxy{dataCoord: datacoord}
		proxy.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := proxy.ManualCompaction(context.TODO(), nil)
		assert.EqualValues(t, unhealthyStatus(), resp.Status)
		assert.NoError(t, err)
	})
}

func Test_GetCompactionStateWithPlans(t *testing.T) {
	t.Run("test get compaction state with plans", func(t *testing.T) {
		datacoord := &DataCoordMock{}
		proxy := &Proxy{dataCoord: datacoord}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := proxy.GetCompactionStateWithPlans(context.TODO(), nil)
		assert.EqualValues(t, &milvuspb.GetCompactionPlansResponse{}, resp)
		assert.NoError(t, err)
	})
	t.Run("test get compaction state with plans with unhealthy proxy", func(t *testing.T) {
		datacoord := &DataCoordMock{}
		proxy := &Proxy{dataCoord: datacoord}
		proxy.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := proxy.GetCompactionStateWithPlans(context.TODO(), nil)
		assert.EqualValues(t, unhealthyStatus(), resp.Status)
		assert.NoError(t, err)
	})
}

func Test_GetFlushState(t *testing.T) {
	t.Run("normal test", func(t *testing.T) {
		datacoord := &DataCoordMock{}
		proxy := &Proxy{dataCoord: datacoord}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := proxy.GetFlushState(context.TODO(), nil)
		assert.EqualValues(t, &milvuspb.GetFlushStateResponse{}, resp)
		assert.NoError(t, err)
	})

	t.Run("test get flush state with unhealthy proxy", func(t *testing.T) {
		datacoord := &DataCoordMock{}
		proxy := &Proxy{dataCoord: datacoord}
		proxy.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := proxy.GetFlushState(context.TODO(), nil)
		assert.EqualValues(t, unhealthyStatus(), resp.Status)
		assert.NoError(t, err)
	})
}

func TestProxy_GetComponentStates(t *testing.T) {
	n := &Proxy{}
	n.stateCode.Store(commonpb.StateCode_Healthy)
	resp, err := n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, common.NotRegisteredID, resp.State.NodeID)
	n.session = &sessionutil.Session{}
	n.session.UpdateRegistered(true)
	resp, err = n.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
}

func TestProxy_GetComponentStates_state_code(t *testing.T) {
	p := &Proxy{}
	p.stateCode.Store("not commonpb.StateCode")
	states, err := p.GetComponentStates(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
}

func TestProxy_Import(t *testing.T) {
	var wg sync.WaitGroup

	wg.Add(1)
	t.Run("test import with unhealthy", func(t *testing.T) {
		defer wg.Done()
		req := &milvuspb.ImportRequest{
			CollectionName: "dummy",
		}
		proxy := &Proxy{}
		proxy.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err := proxy.Import(context.TODO(), req)
		assert.NoError(t, err)
		assert.EqualValues(t, unhealthyStatus(), resp.GetStatus())
	})

	wg.Add(1)
	t.Run("rootcoord fail", func(t *testing.T) {
		defer wg.Done()
		proxy := &Proxy{}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)
		cache := newMockCache()
		globalMetaCache = cache
		chMgr := newMockChannelsMgr()
		proxy.chMgr = chMgr
		rc := newMockRootCoord()
		rc.ImportFunc = func(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
			return nil, errors.New("mock")
		}
		proxy.rootCoord = rc
		req := &milvuspb.ImportRequest{
			CollectionName: "dummy",
		}
		resp, err := proxy.Import(context.TODO(), req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	wg.Add(1)
	t.Run("normal case", func(t *testing.T) {
		defer wg.Done()
		proxy := &Proxy{}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)
		cache := newMockCache()
		globalMetaCache = cache
		chMgr := newMockChannelsMgr()
		proxy.chMgr = chMgr
		rc := newMockRootCoord()
		rc.ImportFunc = func(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
			return &milvuspb.ImportResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil
		}
		proxy.rootCoord = rc
		req := &milvuspb.ImportRequest{
			CollectionName: "dummy",
		}
		resp, err := proxy.Import(context.TODO(), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	wg.Add(1)
	t.Run("illegal import options", func(t *testing.T) {
		defer wg.Done()
		proxy := &Proxy{}
		proxy.UpdateStateCode(commonpb.StateCode_Healthy)
		cache := newMockCache()
		globalMetaCache = cache
		chMgr := newMockChannelsMgr()
		proxy.chMgr = chMgr
		rc := newMockRootCoord()
		rc.ImportFunc = func(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
			return &milvuspb.ImportResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil
		}
		proxy.rootCoord = rc
		req := &milvuspb.ImportRequest{
			CollectionName: "dummy",
			Options: []*commonpb.KeyValuePair{
				{
					Key:   importutil.StartTs,
					Value: "0",
				},
				{
					Key:   importutil.EndTs,
					Value: "not a number",
				},
			},
		}
		resp, err := proxy.Import(context.TODO(), req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
	wg.Wait()
}

func TestProxy_GetImportState(t *testing.T) {
	req := &milvuspb.GetImportStateRequest{
		Task: 1,
	}
	rootCoord := &RootCoordMock{}
	rootCoord.state.Store(commonpb.StateCode_Healthy)
	t.Run("test get import state", func(t *testing.T) {
		proxy := &Proxy{rootCoord: rootCoord}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := proxy.GetImportState(context.TODO(), req)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.NoError(t, err)
	})
	t.Run("test get import state with unhealthy", func(t *testing.T) {
		proxy := &Proxy{rootCoord: rootCoord}
		proxy.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := proxy.GetImportState(context.TODO(), req)
		assert.EqualValues(t, unhealthyStatus(), resp.Status)
		assert.NoError(t, err)
	})
}

func TestProxy_ListImportTasks(t *testing.T) {
	req := &milvuspb.ListImportTasksRequest{}
	rootCoord := &RootCoordMock{}
	rootCoord.state.Store(commonpb.StateCode_Healthy)
	t.Run("test list import tasks", func(t *testing.T) {
		proxy := &Proxy{rootCoord: rootCoord}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)

		resp, err := proxy.ListImportTasks(context.TODO(), req)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.NoError(t, err)
	})
	t.Run("test list import tasks with unhealthy", func(t *testing.T) {
		proxy := &Proxy{rootCoord: rootCoord}
		proxy.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := proxy.ListImportTasks(context.TODO(), req)
		assert.EqualValues(t, unhealthyStatus(), resp.Status)
		assert.NoError(t, err)
	})
}

func TestProxy_GetStatistics(t *testing.T) {

}

func TestProxy_GetLoadState(t *testing.T) {
	originCache := globalMetaCache
	m := newMockCache()
	m.setGetIDFunc(func(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
		return 1, nil
	})
	m.setGetPartitionIDFunc(func(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error) {
		return 2, nil
	})
	globalMetaCache = m
	defer func() {
		globalMetaCache = originCache
	}()

	{
		qc := getQueryCoord()
		qc.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				NodeID:    0,
				Role:      typeutil.QueryCoordRole,
				StateCode: commonpb.StateCode_Abnormal,
				ExtraInfo: nil,
			},
			SubcomponentStates: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       nil,
			InMemoryPercentages: []int64{},
		}, nil)
		proxy := &Proxy{queryCoord: qc}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)
		stateResp, err := proxy.GetLoadState(context.Background(), &milvuspb.GetLoadStateRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, stateResp.Status.ErrorCode)

		progressResp, err := proxy.GetLoadingProgress(context.Background(), &milvuspb.GetLoadingProgressRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, progressResp.Status.ErrorCode)
	}

	{
		qc := getQueryCoord()
		qc.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				NodeID:    0,
				Role:      typeutil.QueryCoordRole,
				StateCode: commonpb.StateCode_Healthy,
				ExtraInfo: nil,
			},
			SubcomponentStates: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, errors.New("test"))
		qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(nil, errors.New("test"))
		proxy := &Proxy{queryCoord: qc}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)

		stateResp, err := proxy.GetLoadState(context.Background(), &milvuspb.GetLoadStateRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stateResp.Status.ErrorCode)
		assert.Equal(t, commonpb.LoadState_LoadStateNotLoad, stateResp.State)

		stateResp, err = proxy.GetLoadState(context.Background(), &milvuspb.GetLoadStateRequest{CollectionName: "foo", PartitionNames: []string{"p1"}})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stateResp.Status.ErrorCode)
		assert.Equal(t, commonpb.LoadState_LoadStateNotLoad, stateResp.State)

		progressResp, err := proxy.GetLoadingProgress(context.Background(), &milvuspb.GetLoadingProgressRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, progressResp.Status.ErrorCode)
		assert.Equal(t, int64(0), progressResp.Progress)

		progressResp, err = proxy.GetLoadingProgress(context.Background(), &milvuspb.GetLoadingProgressRequest{CollectionName: "foo", PartitionNames: []string{"p1"}})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, progressResp.Status.ErrorCode)
		assert.Equal(t, int64(0), progressResp.Progress)
	}

	{
		qc := getQueryCoord()
		qc.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				NodeID:    0,
				Role:      typeutil.QueryCoordRole,
				StateCode: commonpb.StateCode_Healthy,
				ExtraInfo: nil,
			},
			SubcomponentStates: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       nil,
			InMemoryPercentages: []int64{},
		}, nil)
		qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(nil, errors.New("test"))
		proxy := &Proxy{queryCoord: qc}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)

		stateResp, err := proxy.GetLoadState(context.Background(), &milvuspb.GetLoadStateRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stateResp.Status.ErrorCode)
		assert.Equal(t, commonpb.LoadState_LoadStateNotLoad, stateResp.State)

		progressResp, err := proxy.GetLoadingProgress(context.Background(), &milvuspb.GetLoadingProgressRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, progressResp.Status.ErrorCode)
	}

	{
		qc := getQueryCoord()
		qc.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				NodeID:    0,
				Role:      typeutil.QueryCoordRole,
				StateCode: commonpb.StateCode_Healthy,
				ExtraInfo: nil,
			},
			SubcomponentStates: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       nil,
			InMemoryPercentages: []int64{100},
		}, nil)
		proxy := &Proxy{queryCoord: qc}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)

		stateResp, err := proxy.GetLoadState(context.Background(), &milvuspb.GetLoadStateRequest{CollectionName: "foo", Base: &commonpb.MsgBase{}})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stateResp.Status.ErrorCode)
		assert.Equal(t, commonpb.LoadState_LoadStateLoaded, stateResp.State)

		stateResp, err = proxy.GetLoadState(context.Background(), &milvuspb.GetLoadStateRequest{CollectionName: ""})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, stateResp.Status.ErrorCode)

		progressResp, err := proxy.GetLoadingProgress(context.Background(), &milvuspb.GetLoadingProgressRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, progressResp.Status.ErrorCode)
		assert.Equal(t, int64(100), progressResp.Progress)
	}

	{
		qc := getQueryCoord()
		qc.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				NodeID:    0,
				Role:      typeutil.QueryCoordRole,
				StateCode: commonpb.StateCode_Healthy,
				ExtraInfo: nil,
			},
			SubcomponentStates: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       nil,
			InMemoryPercentages: []int64{50},
		}, nil)
		proxy := &Proxy{queryCoord: qc}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)

		stateResp, err := proxy.GetLoadState(context.Background(), &milvuspb.GetLoadStateRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stateResp.Status.ErrorCode)
		assert.Equal(t, commonpb.LoadState_LoadStateLoading, stateResp.State)

		progressResp, err := proxy.GetLoadingProgress(context.Background(), &milvuspb.GetLoadingProgressRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, progressResp.Status.ErrorCode)
		assert.Equal(t, int64(50), progressResp.Progress)
	}

	t.Run("test insufficient memory", func(t *testing.T) {
		qc := getQueryCoord()
		qc.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				NodeID:    0,
				Role:      typeutil.QueryCoordRole,
				StateCode: commonpb.StateCode_Healthy,
				ExtraInfo: nil,
			},
			SubcomponentStates: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_InsufficientMemoryToLoad},
		}, nil)
		qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_InsufficientMemoryToLoad},
		}, nil)
		proxy := &Proxy{queryCoord: qc}
		proxy.stateCode.Store(commonpb.StateCode_Healthy)

		stateResp, err := proxy.GetLoadState(context.Background(), &milvuspb.GetLoadStateRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_InsufficientMemoryToLoad, stateResp.Status.ErrorCode)

		progressResp, err := proxy.GetLoadingProgress(context.Background(), &milvuspb.GetLoadingProgressRequest{CollectionName: "foo"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_InsufficientMemoryToLoad, progressResp.Status.ErrorCode)

		progressResp, err = proxy.GetLoadingProgress(context.Background(), &milvuspb.GetLoadingProgressRequest{CollectionName: "foo", PartitionNames: []string{"p1"}})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_InsufficientMemoryToLoad, progressResp.Status.ErrorCode)
	})
}

func TestUnhealthProxy_GetIndexStatistics(t *testing.T) {
	// check GetIndexStatistics when proxy is unhealthy
	factory := dependency.NewDefaultFactory(true)
	ctx := context.Background()
	proxy, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	assert.NotNil(t, proxy)
	t.Run("TestProxy_GetIndexStatistics", func(t *testing.T) {
		resp, err := proxy.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: "hello_milvus",
			IndexName:      "",
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.Status.ErrorCode)
	})
}
