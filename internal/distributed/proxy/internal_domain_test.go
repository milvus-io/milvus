package grpcproxy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type portsProvider struct{ grpcPort, restPort int }

func (portsProvider) Name() string                       { return "test" }
func (portsProvider) Requires() []extension.CapabilityID { return nil }
func (p portsProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{InternalSurfaces: p}
}
func (p portsProvider) InternalDomainPorts() (int, int) { return p.grpcPort, p.restPort }

// With no capability installed no listener is opened: a stock binary serves
// exactly the surfaces it always did.
func TestInternalDomainPortsAreZeroWithoutAProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	grpcPort, restPort := internalDomainPorts()
	assert.Zero(t, grpcPort)
	assert.Zero(t, restPort)
}

// A form's declaration is passed through verbatim; zero disables a listener
// individually.
func TestInternalDomainPortsFollowTheDeclaration(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	assert.NoError(t, extension.SetProvider(portsProvider{grpcPort: 26330, restPort: 0}))

	grpcPort, restPort := internalDomainPorts()
	assert.Equal(t, 26330, grpcPort)
	assert.Zero(t, restPort)
}

// Every request the internal-domain gRPC listener accepts carries the
// provenance mark: it is how the shared handlers' admin seam tells the
// control plane's calls from tenants'.
func TestInternalDomainGrpcInterceptorMarksTheContext(t *testing.T) {
	var sawMark bool
	_, err := internalDomainMarkInterceptor(context.Background(), nil, nil,
		func(ctx context.Context, _ any) (any, error) {
			sawMark = extension.FromInternalDomain(ctx)
			return nil, nil
		})
	assert.NoError(t, err)
	assert.True(t, sawMark, "the handler must see the internal-domain mark")
}

// The stream-side twin of the unary mark: without it a stream method on this
// listener would arrive unmarked and the admin seam would refuse the control
// plane's own CreateReplicateStream.
func TestInternalDomainStreamInterceptorMarksTheContext(t *testing.T) {
	var sawMark bool
	err := internalDomainMarkStreamInterceptor(nil, plainServerStream{ctx: context.Background()}, nil,
		func(_ any, ss grpc.ServerStream) error {
			sawMark = extension.FromInternalDomain(ss.Context())
			return nil
		})
	assert.NoError(t, err)
	assert.True(t, sawMark)
}

type plainServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s plainServerStream) Context() context.Context { return s.ctx }

// The listeners really serve, with the provenance mark attached: the gRPC
// port answers an admin RPC as the control plane (passing the kite-style
// refusal a tenant would get), and the REST port serves /metrics. This is the
// smallest end-to-end proof that startInternalDomain* opens working sockets.
func TestInternalDomainListenersServe(t *testing.T) {
	paramtable.Init()
	// The listener's interceptor chain includes the access logger, which is a
	// process global the full server start initializes; do the same here.
	accesslog.InitAccessLogger(paramtable.Get())
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	mockProxy := mocks.NewMockProxy(t)
	mockProxy.EXPECT().ListCredUsers(mock.Anything, mock.Anything).
		Return(&milvuspb.ListCredUsersResponse{Status: merr.Success()}, nil).Maybe()
	s := &Server{ctx: context.Background(), proxy: mockProxy}
	grpcPort := freeTestPort(t)
	require.NoError(t, s.startInternalDomainGrpc(grpcPort))
	t.Cleanup(func() { s.internalDomainGrpcServer.Stop() })

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := milvuspb.NewMilvusServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.ListCredUsers(ctx, &milvuspb.ListCredUsersRequest{})
	require.NoError(t, err, "the internal gRPC listener must answer the full MilvusService")

	restPort := freeTestPort(t)
	require.NoError(t, s.startInternalDomainRest(restPort))
	t.Cleanup(func() { _ = s.internalDomainHTTPServer.Close() })
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", restPort))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "the internal REST listener must serve /metrics")
}

func freeTestPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}
