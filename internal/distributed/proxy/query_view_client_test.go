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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/util/searchutil/mock_optimizers"
	querynodehandler "github.com/milvus-io/milvus/internal/querynodev2/client/handler"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type fakeViewQueryClient struct {
	legacy queryclient.LegacyClient
}

func (c *fakeViewQueryClient) Close() {}

func (c *fakeViewQueryClient) Legacy() queryclient.LegacyClient {
	return c.legacy
}

type fakeViewQueryClientSetter struct {
	client queryclient.Client
}

func (s *fakeViewQueryClientSetter) SetViewQueryClient(client queryclient.Client) {
	s.client = client
}

// fakeViewQueryClientProxy implements types.ProxyComponent via embedding and
// the collection vchannel provider interface for the view query client.
type fakeViewQueryClientProxy struct {
	types.ProxyComponent
}

func (fakeViewQueryClientProxy) GetCollectionVChannels(context.Context, int64) ([]string, error) {
	return nil, merr.WrapErrCollectionNotLoaded(0)
}

func TestInitViewQueryClientInjectsClient(t *testing.T) {
	original := newProxyViewQueryClient
	t.Cleanup(func() {
		newProxyViewQueryClient = original
	})

	expected := &fakeViewQueryClient{}
	closeCalled := false
	buildCalled := false
	newProxyViewQueryClient = func(_ *clientv3.Client, _ resolver.CollectionVChannelProvider, _ queryclient.LoadInfoClient) (queryclient.Client, func(), error) {
		buildCalled = true
		return expected, func() {
			closeCalled = true
		}, nil
	}

	proxy := &fakeViewQueryClientSetter{}
	server := &Server{proxy: fakeViewQueryClientProxy{}}

	require.NoError(t, server.initViewQueryClient(proxy))
	require.True(t, buildCalled)
	require.Same(t, expected, proxy.client)

	server.closeViewQueryClient()
	require.True(t, closeCalled)
}

func TestInitViewQueryClientFailsWithoutProvider(t *testing.T) {
	proxy := &fakeViewQueryClientSetter{}
	server := &Server{}
	require.Error(t, server.initViewQueryClient(proxy))
}

type startupWAL struct{ streaming.WALAccesser }

func (*startupWAL) QueryViewClient() streaming.QueryViewClient { return nil }

type startupQueryNodeClient struct{}

func (*startupQueryNodeClient) QueryViewClient() querynodehandler.QueryViewClient { return nil }
func (*startupQueryNodeClient) Close()                                            {}

func TestDefaultProxyViewQueryClientHookInitFailure(t *testing.T) {
	params := paramtable.Get()
	oldEnable := params.AutoIndexConfig.Enable.GetValue()
	t.Cleanup(func() {
		params.Save(params.AutoIndexConfig.Enable.Key, oldEnable)
	})
	mockey.PatchConvey("hook init failure is fatal only when AutoIndex is enabled", t, func() {
		hook := mock_optimizers.NewMockQueryHook(t)
		hook.EXPECT().Init(params.AutoIndexConfig.AutoIndexSearchConfig.GetValue()).Return(merr.ErrServiceInternal).Twice()
		mockey.Mock(optimizers.LoadQueryHook).Return(hook, nil).Build()
		watch := mockey.Mock(optimizers.WatchQueryHook).Return(func() {}).Build()
		mockey.Mock(streaming.WAL).Return(&startupWAL{}).Build()
		mockey.Mock(querynodehandler.NewClient).Return(&startupQueryNodeClient{}).Build()
		mockey.Mock(queryclient.NewLegacyViewQueryClient).To(func(cfg queryclient.ViewQueryClientConfig, _ queryclient.QueryPlanClient, _ queryclient.ViewQueryServiceClient, _ resolver.ShardResolver) queryclient.Client {
			require.Nil(t, cfg.QueryHook)
			return &fakeViewQueryClient{}
		}).Build()

		require.NoError(t, params.Save(params.AutoIndexConfig.Enable.Key, "false"))
		client, closeClient, err := newDefaultProxyViewQueryClient(nil, fakeViewQueryClientProxy{}, nil)
		require.NoError(t, err)
		require.NotNil(t, client)
		closeClient()

		require.NoError(t, params.Save(params.AutoIndexConfig.Enable.Key, "true"))
		_, _, err = newDefaultProxyViewQueryClient(nil, fakeViewQueryClientProxy{}, nil)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Zero(t, watch.Times())
	})
}
