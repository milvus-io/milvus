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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
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
}

// DataNode is the interface `datanode` package implements
type DataNode interface {
	Component
	datapb.DataNodeServer
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

	// SetRootCoordClient set SetRootCoordClient for DataNode
	// `rootCoord` is a client of root coordinator.
	//
	// Return a generic error in status:
	//     If the rootCoord is nil or the rootCoord has been set before.
	// Return nil in status:
	//     The rootCoord is not nil.
	SetRootCoordClient(rootCoord RootCoordClient) error

	// SetDataCoordClient set DataCoord for DataNode
	// `dataCoord` is a client of data coordinator.
	//
	// Return a generic error in status:
	//     If the dataCoord is nil or the dataCoord has been set before.
	// Return nil in status:
	//     The dataCoord is not nil.
	SetDataCoordClient(dataCoord DataCoordClient) error
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

	RegisterStreamingCoordGRPCService(s *grpc.Server)

	SetAddress(address string)
	// SetEtcdClient set EtcdClient for DataCoord
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetTiKVClient set TiKV client for QueryNode
	SetTiKVClient(client *txnkv.Client)

	SetRootCoordClient(rootCoord RootCoordClient)

	// SetDataNodeCreator set DataNode client creator func for DataCoord
	SetDataNodeCreator(func(context.Context, string, int64) (DataNodeClient, error))

	// SetIndexNodeCreator set Index client creator func for DataCoord
	SetIndexNodeCreator(func(context.Context, string, int64) (IndexNodeClient, error))
}

// IndexNodeClient is the client interface for indexnode server
type IndexNodeClient interface {
	io.Closer
	workerpb.IndexNodeClient
}

// IndexNode is the interface `indexnode` package implements
type IndexNode interface {
	Component
	workerpb.IndexNodeServer
}

// IndexNodeComponent is used by grpc server of IndexNode
type IndexNodeComponent interface {
	IndexNode

	SetAddress(address string)
	GetAddress() string
	// SetEtcdClient set etcd client for IndexNodeComponent
	SetEtcdClient(etcdClient *clientv3.Client)

	// UpdateStateCode updates state code for IndexNodeComponent
	//  `stateCode` is current statement of this QueryCoord, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)
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

	// SetDataCoordClient set SetDataCoordClient for RootCoord
	// `dataCoord` is a client of data coordinator.
	//
	// Always return nil.
	SetDataCoordClient(dataCoord DataCoordClient) error

	// SetQueryCoord set QueryCoord for RootCoord
	//  `queryCoord` is a client of query coordinator.
	//
	// Always return nil.
	SetQueryCoordClient(queryCoord QueryCoordClient) error

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

	// SetRootCoordClient set RootCoord for Proxy
	// `rootCoord` is a client of root coordinator.
	SetRootCoordClient(rootCoord RootCoordClient)

	// SetDataCoordClient set DataCoord for Proxy
	// `dataCoord` is a client of data coordinator.
	SetDataCoordClient(dataCoord DataCoordClient)

	// SetIndexCoordClient set IndexCoord for Proxy
	//  `indexCoord` is a client of index coordinator.
	// SetIndexCoordClient(indexCoord IndexCoord)

	// SetQueryCoordClient set QueryCoord for Proxy
	//  `queryCoord` is a client of query coordinator.
	SetQueryCoordClient(queryCoord QueryCoordClient)

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

	// SetDataCoordClient set SetDataCoordClient for QueryCoord
	// `dataCoord` is a client of data coordinator.
	//
	// Return a generic error in status:
	//     If the dataCoord is nil.
	// Return nil in status:
	//     The dataCoord is not nil.
	SetDataCoordClient(dataCoord DataCoordClient) error

	// SetRootCoordClient set SetRootCoordClient for QueryCoord
	// `rootCoord` is a client of root coordinator.
	//
	// Return a generic error in status:
	//     If the rootCoord is nil.
	// Return nil in status:
	//     The rootCoord is not nil.
	SetRootCoordClient(rootCoord RootCoordClient) error

	// SetQueryNodeCreator set QueryNode client creator func for QueryCoord
	SetQueryNodeCreator(func(ctx context.Context, addr string, nodeID int64) (QueryNodeClient, error))
}
