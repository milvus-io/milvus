package proxy

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	grpcproxyclient "github.com/milvus-io/milvus/internal/distributed/proxy/client"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

	bt := paramtable.NewBaseTable(paramtable.SkipRemote(true))
	base := &paramtable.ComponentParam{}
	base.Init(bt)
	var p paramtable.GrpcServerConfig
	assert.NoError(t, err)
	p.Init(typeutil.ProxyRole, bt)
	base.Save("proxy.grpc.serverMaxRecvSize", "1")

	assert.Equal(t, p.ServerMaxRecvSize.GetAsInt(), 1)
	log.Info("Initialize parameter table of Proxy")

	proxy, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	assert.NotNil(t, proxy)

	testServer := newProxyTestServer(proxy)
	testServer.Proxy.SetAddress(p.GetAddress())
	wg.Add(1)
	go testServer.startGrpc(ctx, &wg, &p)
	assert.NoError(t, testServer.waitForGrpcReady())
	defer testServer.grpcServer.Stop()
	client, err := grpcproxyclient.NewClient(ctx, "localhost:"+p.Port.GetValue(), 1)
	assert.NoError(t, err)
	proxy.UpdateStateCode(commonpb.StateCode_Healthy)

	rates := make([]*internalpb.Rate, 0)

	req := &proxypb.SetRatesRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgID(int64(0)),
			commonpbutil.WithTimeStamp(0),
		),
		Rates: []*proxypb.CollectionRate{
			{
				Collection: 1,
				Rates:      rates,
			},
		},
	}
	_, err = client.SetRates(ctx, req)
	// should be limited because of the rpc limit
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "ResourceExhausted"))
}
