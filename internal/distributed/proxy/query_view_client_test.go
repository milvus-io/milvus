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

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeViewQueryClient struct {
	legacy queryclient.LegacyClient
}

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
	newProxyViewQueryClient = func(_ *clientv3.Client, _ resolver.CollectionVChannelProvider) (queryclient.Client, func(), error) {
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
