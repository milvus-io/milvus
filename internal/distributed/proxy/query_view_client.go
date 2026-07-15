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

package grpcproxy

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	querynodehandler "github.com/milvus-io/milvus/internal/querynodev2/client/handler"
	streamingcoordclient "github.com/milvus-io/milvus/internal/streamingcoord/client"
	streamingnodehandler "github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
)

type viewQueryClientSetter interface {
	SetViewQueryClient(queryclient.Client)
}

var newProxyViewQueryClient = newDefaultProxyViewQueryClient

func (s *Server) initViewQueryClient(setter viewQueryClientSetter) error {
	client, closeFunc, err := newProxyViewQueryClient(s.etcdCli)
	if err != nil {
		return err
	}
	setter.SetViewQueryClient(client)
	s.viewQueryClientClose = closeFunc
	return nil
}

func (s *Server) closeViewQueryClient() {
	if s.viewQueryClientClose == nil {
		return
	}
	s.viewQueryClientClose()
	s.viewQueryClientClose = nil
}

func newDefaultProxyViewQueryClient(etcdCli *clientv3.Client) (queryclient.Client, func(), error) {
	streamingCoordClient := streamingcoordclient.NewClient(etcdCli)
	assignment := streamingCoordClient.Assignment()
	shardResolver := resolver.NewShardResolverImpl(assignment)
	streamingNodeClient := streamingnodehandler.NewHandlerClient(assignment)
	queryNodeClient := querynodehandler.NewClient(etcdCli)

	queryPlanClient := streamingNodeClient.QueryViewClient()
	queryServiceClient := queryclient.NewCompositeViewQueryServiceClient(
		streamingNodeClient.QueryViewClient(),
		queryNodeClient.QueryViewClient(),
	)
	client := queryclient.NewLegacyViewQueryClient(
		queryclient.ViewQueryClientConfig{},
		queryPlanClient,
		queryServiceClient,
		shardResolver,
		queryclient.NewRandomReplicaPicker(),
	)
	closeFunc := func() {
		shardResolver.Close()
		streamingNodeClient.Close()
		queryNodeClient.Close()
		streamingCoordClient.Close()
	}
	return client, closeFunc, nil
}
