package grpcproxy

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Keep the real Proxy.MultiSearch coordinator; substitute only leaf execution.
type multiSearchEntryTestProxy struct {
	*proxy.Proxy
	calls atomic.Int64
}

func (p *multiSearchEntryTestProxy) Search(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	p.calls.Add(1)
	return &milvuspb.SearchResults{Status: merr.Success(), CollectionName: req.DbName}, nil
}

type multiSearchEntryTestLimiter struct {
	types.Limiter
	calls  atomic.Int64
	tokens atomic.Int64
}

func (l *multiSearchEntryTestLimiter) Check(dbID int64, _ map[int64][]int64, rt internalpb.RateType, n int) error {
	l.calls.Add(1)
	l.tokens.Add(int64(n))
	if dbID == 2 {
		return merr.ErrServiceRateLimit
	}
	return nil
}

// A real gRPC request must reach Proxy.MultiSearch and the shared Search entry.
// A quota rejection stays item-local and must not invoke leaf Search. Each item
// is charged exactly once, while the envelope consumes no Search quota.
func TestMultiSearchServerProxyEntry(t *testing.T) {
	accesslog.InitAccessLogger(proxy.Params)
	require.NoError(t, proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "false"))
	t.Cleanup(func() { proxy.Params.Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key) })
	node, err := proxy.NewProxy(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Stop()) })
	component := &multiSearchEntryTestProxy{Proxy: node}
	cache := proxy.NewMockCache(t)
	cache.EXPECT().GetDatabaseInfo(mock.Anything, "db_a").Return(&metacache.DatabaseInfo{DBID: 1}, nil)
	cache.EXPECT().GetDatabaseInfo(mock.Anything, "db_b").Return(&metacache.DatabaseInfo{DBID: 2}, nil)
	cache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, "same").Return(int64(10), nil)
	getCache := func() proxy.Cache { return cache }
	limiter := &multiSearchEntryTestLimiter{}
	configureMultiSearch(component, limiter, getCache)
	server := &Server{proxy: component}
	lis := bufconn.Listen(1024 * 1024)
	rpc := grpc.NewServer(grpc.UnaryInterceptor(proxyEntryInterceptor(limiter, getCache)))
	milvuspb.RegisterMilvusServiceServer(rpc, server)
	t.Cleanup(rpc.Stop)
	go func() { _ = rpc.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///multisearch",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := milvuspb.NewMilvusServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &milvuspb.MultiSearchRequest{Items: []*milvuspb.MultiSearchItem{
		{Id: "a", Search: &milvuspb.SearchRequest{DbName: "db_a", CollectionName: "same", Nq: 3}},
		{Id: "b", Search: &milvuspb.SearchRequest{DbName: "db_b", CollectionName: "same", Nq: 7}},
	}}
	resp, err := client.MultiSearch(ctx, req)
	require.NoError(t, err)
	require.True(t, merr.Ok(resp.Status))
	require.Len(t, resp.Items, 2)
	require.Equal(t, "db_a", resp.Items[0].Result.CollectionName)
	require.Equal(t, merr.Code(merr.ErrServiceRateLimit), resp.Items[1].Result.Status.Code)
	require.True(t, resp.Items[1].Result.Status.Retriable)
	require.Equal(t, int64(1), component.calls.Load())
	require.Equal(t, int64(2), limiter.calls.Load())
	require.Equal(t, int64(10), limiter.tokens.Load())

	ordinary, err := client.Search(ctx, &milvuspb.SearchRequest{DbName: "db_a", CollectionName: "same", Nq: 5})
	require.NoError(t, err)
	require.True(t, merr.Ok(ordinary.Status))
	require.Equal(t, int64(2), component.calls.Load())
	require.Equal(t, int64(3), limiter.calls.Load())
	require.Equal(t, int64(15), limiter.tokens.Load())

	require.NoError(t, proxy.Params.Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true"))
	_, err = client.MultiSearch(ctx, req)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Equal(t, int64(2), component.calls.Load())
	require.Equal(t, int64(3), limiter.calls.Load())
}
