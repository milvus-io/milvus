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
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	grpcproxyclient "github.com/milvus-io/milvus/internal/distributed/proxy/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestProxyRpcLimit(t *testing.T) {
	var err error
	var wg sync.WaitGroup

	path := "/tmp/milvus/rocksmq" + funcutil.GenRandomStr()
	t.Setenv("ROCKSMQ_PATH", path)
	defer os.RemoveAll(path)

	ctx := GetContext(context.Background(), "root:123456")
	localMsg := true
	factory := dependency.NewDefaultFactory(localMsg)

	var p paramtable.GrpcServerConfig
	assert.NoError(t, err)
	p.InitOnce(typeutil.ProxyRole)
	p.Save("proxy.grpc.serverMaxRecvSize", "1")

	p.InitServerMaxRecvSize()
	assert.Equal(t, p.ServerMaxRecvSize, 1)
	log.Info("Initialize parameter table of Proxy")

	proxy, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	assert.NotNil(t, proxy)

	testServer := newProxyTestServer(proxy)
	wg.Add(1)
	go testServer.startGrpc(ctx, &wg, p)
	assert.NoError(t, testServer.waitForGrpcReady())
	defer testServer.gracefulStop()
	client, err := grpcproxyclient.NewClient(ctx, "localhost:"+fmt.Sprint(p.Port))
	assert.NoError(t, err)
	proxy.stateCode.Store(commonpb.StateCode_Healthy)

	rates := make([]*internalpb.Rate, 0)

	req := &proxypb.SetRatesRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgID(int64(0)),
			commonpbutil.WithTimeStamp(0),
		),
		Rates: rates,
	}
	_, err = client.SetRates(ctx, req)
	// should be limited because of the rpc limit
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "ResourceExhausted"))

	p.Remove("proxy.grpc.serverMaxRecvSize")
	p.Init(typeutil.ProxyRole)
}
