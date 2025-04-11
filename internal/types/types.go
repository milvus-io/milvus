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

package types

import (
	"context"
	"io"

	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
)

// Limiter defines the interface to perform request rate limiting.
// If Limit function return true, the request will be rejected.
// Otherwise, the request will pass. Limit also returns limit of limiter.
type Limiter interface {
	Check(dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error
	Alloc(ctx context.Context, dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error
}

// Component is the interface all services implement
type Component interface {
	Init() error
	Start() error
	Stop() error
	Register() error
}

// DataNodeClient is the client interface for datanode server
type DataNodeClient interface {
	io.Closer
	datapb.DataNodeClient
	workerpb.IndexNodeClient
}

// DataNode is the interface `datanode` package implements
type DataNode interface {
	Component
	datapb.DataNodeServer
	workerpb.IndexNodeServer
}

// DataNodeComponent is used by grpc server of DataNode
//
//go:generate mockery --name=DataNodeComponent --structname=MockDataNode --output=../mocks  --filename=mock_datanode.go --with-expecter
type DataNodeComponent interface {
	DataNode

	// UpdateStateCode updates state code for DataNode
	//  `stateCode` is current statement of this data node, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

	// GetStateCode return state code of this data node
	GetStateCode() commonpb.StateCode

	SetAddress(address string)
	GetAddress() string
	GetNodeID() int64

	// SetEtcdClient set etcd client for DataNode
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetMixCoordClient set SetMixCoordClient for DataNode
	// `mixCoord` is a client of root coordinator.
	//
	// Return a generic error in status:
	//     If the mixCoord is nil or the mixCoord has been set before.
	// Return nil in status:
	//     The mixCoord is not nil.
	SetMixCoordClient(mixCoord MixCoordClient) error
}

// DataCoordClient is the client interface for datacoord server
type DataCoordClient interface {
	io.Closer
	datapb.DataCoordClient
	indexpb.IndexCoordClient
}

// DataCoord is the interface `datacoord` package implements
type DataCoord interface {
	Component
	datapb.DataCoordServer
}

// DataCoordComponent defines the interface of DataCoord component.
//
//go:generate mockery --name=DataCoordComponent --structname=MockDataCoord --output=../mocks  --filename=mock_datacoord.go --with-expecter
type DataCoordComponent interface {
	DataCoord

	SetAddress(address string)
	// SetEtcdClient set EtcdClient for DataCoord
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetTiKVClient set TiKV client for QueryNode
	SetTiKVClient(client *txnkv.Client)

	SetMixCoord(mixCoord MixCoord)

	// SetDataNodeCreator set DataNode client creator func for DataCoord
	SetDataNodeCreator(func(context.Context, string, int64) (DataNodeClient, error))
}

// RootCoordClient is the client interface for rootcoord server
type RootCoordClient interface {
	io.Closer
	rootcoordpb.RootCoordClient
}

// RootCoord is the interface `rootcoord` package implements
//
//go:generate mockery --name=RootCoord  --output=../mocks --filename=mock_rootcoord.go --with-expecter
type RootCoord interface {
	Component
	rootcoordpb.RootCoordServer
}

// RootCoordComponent is used by grpc server of RootCoord
type RootCoordComponent interface {
	RootCoord

	SetAddress(address string)
	// SetEtcdClient set EtcdClient for RootCoord
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetTiKVClient set TiKV client for RootCoord
	SetTiKVClient(client *txnkv.Client)

	// UpdateStateCode updates state code for RootCoord
	// State includes: Initializing, Healthy and Abnormal
	UpdateStateCode(commonpb.StateCode)

	// SetMixCoord set SetMixCoord for RootCoord
	// `dataCoord` is a client of data coordinator.
	//
	// Always return nil.
	SetMixCoord(mixCoord MixCoord) error

	// SetProxyCreator set Proxy client creator func for RootCoord
	SetProxyCreator(func(ctx context.Context, addr string, nodeID int64) (ProxyClient, error))

	// GetMetrics notifies RootCoordComponent to collect metrics for specified component
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// ProxyClient is the client interface for proxy server
type ProxyClient interface {
	io.Closer
	proxypb.ProxyClient
}

// Proxy is the interface `proxy` package implements
type Proxy interface {
	Component
	proxypb.ProxyServer
	milvuspb.MilvusServiceServer

	ImportV2(context.Context, *internalpb.ImportRequest) (*internalpb.ImportResponse, error)
	GetImportProgress(context.Context, *internalpb.GetImportProgressRequest) (*internalpb.GetImportProgressResponse, error)
	ListImports(context.Context, *internalpb.ListImportsRequest) (*internalpb.ListImportsResponse, error)
}

// ProxyComponent defines the interface of proxy component.
//
//go:generate mockery --name=ProxyComponent --structname=MockProxy --output=../mocks  --filename=mock_proxy.go --with-expecter
type ProxyComponent interface {
	Proxy

	SetAddress(address string)
	GetAddress() string
	// SetEtcdClient set EtcdClient for Proxy
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetMixCoordClient set MixCoord for Proxy
	// `mixCoord` is a client of mix coordinator.
	SetMixCoordClient(rootCoord MixCoordClient)

	// SetQueryNodeCreator set QueryNode client creator func for Proxy
	SetQueryNodeCreator(func(ctx context.Context, addr string, nodeID int64) (QueryNodeClient, error))

	// GetRateLimiter returns the rateLimiter in Proxy
	GetRateLimiter() (Limiter, error)

	// UpdateStateCode updates state code for Proxy
	//  `stateCode` is current statement of this proxy node, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)
}

type QueryNodeClient interface {
	io.Closer
	querypb.QueryNodeClient
}

// QueryNode is the interface `querynode` package implements
type QueryNode interface {
	Component
	querypb.QueryNodeServer
}

// QueryNodeComponent is used by grpc server of QueryNode
//
//go:generate mockery --name=QueryNodeComponent --structname=MockQueryNode --output=../mocks  --filename=mock_querynode.go --with-expecter
type QueryNodeComponent interface {
	QueryNode

	// UpdateStateCode updates state code for QueryNode
	//  `stateCode` is current statement of this query node, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

	SetAddress(address string)
	GetAddress() string
	GetNodeID() int64

	// SetEtcdClient set etcd client for QueryNode
	SetEtcdClient(etcdClient *clientv3.Client)
}

// QueryCoordClient is the client interface for querycoord server
type QueryCoordClient interface {
	io.Closer
	querypb.QueryCoordClient
}

// QueryCoord is the interface `querycoord` package implements
type QueryCoord interface {
	Component
	querypb.QueryCoordServer
}

// QueryCoordComponent is used by grpc server of QueryCoord
//
//go:generate mockery --name=QueryCoordComponent --structname=MockQueryCoord --output=../mocks  --filename=mock_querycoord.go --with-expecter
type QueryCoordComponent interface {
	QueryCoord

	SetAddress(address string)

	// SetEtcdClient set etcd client for QueryCoord
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetTiKVClient set TiKV client for QueryCoord
	SetTiKVClient(client *txnkv.Client)

	// UpdateStateCode updates state code for QueryCoord
	//  `stateCode` is current statement of this QueryCoord, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

	// SetQueryNodeCreator set QueryNode client creator func for QueryCoord
	SetQueryNodeCreator(func(ctx context.Context, addr string, nodeID int64) (QueryNodeClient, error))

	SetMixCoord(mixCoord MixCoord)
}

// MixCoordClient is the client interface for mixcoord server
type MixCoordClient interface {
	io.Closer
	rootcoordpb.RootCoordClient
	querypb.QueryCoordClient
	datapb.DataCoordClient
	indexpb.IndexCoordClient
}

// MixCoord is the interface `MixCoord` package implements
//
//go:generate mockery --name=MixCoord  --output=../mocks --filename=mock_mixcoord.go --with-expecter
type MixCoord interface {
	Component
	rootcoordpb.RootCoordServer
	querypb.QueryCoordServer
	datapb.DataCoordServer

	// GetMetrics notifies MixCoordComponent to collect metrics for specified component
	GetDcMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)

	// GetMetrics notifies MixCoordComponent to collect metrics for specified component
	GetQcMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// MixCoordComponent is used by grpc server of MixCoord
type MixCoordComponent interface {
	MixCoord

	SetAddress(address string)
	// SetEtcdClient set EtcdClient for MixCoord
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetTiKVClient set TiKV client for MixCoord
	SetTiKVClient(client *txnkv.Client)

	// UpdateStateCode updates state code for MixCoord
	// State includes: Initializing, Healthy and Abnormal
	UpdateStateCode(commonpb.StateCode)

	// GetMetrics notifies MixCoordComponent to collect metrics for specified component
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)

	RegisterStreamingCoordGRPCService(server *grpc.Server)

	GracefulStop()

	SetMixCoordClient(client MixCoordClient)
}
