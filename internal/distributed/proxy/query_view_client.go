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
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	querynodehandler "github.com/milvus-io/milvus/internal/querynodev2/client/handler"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type viewQueryClientSetter interface {
	SetViewQueryClient(queryclient.Client)
}

var newProxyViewQueryClient = newDefaultProxyViewQueryClient

func (s *Server) initViewQueryClient(setter viewQueryClientSetter) error {
	// The vchannel provider is the proxy's GetCollection flow (metacache);
	// the setter is the proxy component itself.
	provider, ok := s.proxy.(resolver.CollectionVChannelProvider)
	if !ok {
		return merr.WrapErrServiceInternalMsg("proxy does not implement collection vchannel provider")
	}
	client, closeFunc, err := newProxyViewQueryClient(s.etcdCli, provider, s.mixCoordClient)
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

func newDefaultProxyViewQueryClient(etcdCli *clientv3.Client, vchannelProvider resolver.CollectionVChannelProvider, loadInfoClient queryclient.LoadInfoClient) (queryclient.Client, func(), error) {
	hook, err := optimizers.LoadQueryHook()
	if err != nil && paramtable.Get().AutoIndexConfig.Enable.GetAsBool() {
		return nil, nil, err
	}
	closeHook := func() {}
	if hook != nil {
		if err := optimizers.InitQueryHook(hook); err != nil {
			return nil, nil, err
		}
		closeHook = optimizers.WatchQueryHook(context.TODO(), hook, "queryViewClientHook")
	}
	shardResolver := resolver.NewShardResolverImpl(vchannelProvider)
	queryNodeClient := querynodehandler.NewClient(etcdCli)

	queryPlanClient := streaming.WAL().QueryViewClient()
	queryServiceClient := queryclient.NewCompositeViewQueryServiceClient(
		queryPlanClient,
		queryNodeClient.QueryViewClient(),
	)
	client := queryclient.NewLegacyViewQueryClient(
		queryclient.ViewQueryClientConfig{LoadInfoClient: loadInfoClient, QueryHook: hook},
		queryPlanClient,
		queryServiceClient,
		shardResolver,
	)
	closeFunc := func() {
		client.Close()
		closeHook()
		shardResolver.Close()
		queryNodeClient.Close()
	}
	return client, closeFunc, nil
}
