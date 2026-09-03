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

type portsProvider struct {
	grpcPort, restPort int
	bindAddress        string
}

func (portsProvider) Name() string                       { return "test" }
func (portsProvider) Requires() []extension.CapabilityID { return nil }
func (p portsProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{InternalSurfaces: p}
}
func (p portsProvider) InternalDomainListeners() extension.InternalListeners {
	return extension.InternalListeners{GRPCPort: p.grpcPort, RESTPort: p.restPort, BindAddress: p.bindAddress}
}

// With no capability installed no listener is opened: a stock binary serves
// exactly the surfaces it always did.
func TestInternalDomainPortsAreZeroWithoutAProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	listeners := internalDomainListeners()
	assert.Zero(t, listeners.GRPCPort)
	assert.Zero(t, listeners.RESTPort)
	assert.Empty(t, listeners.BindAddress)
}

// A form's declaration is passed through verbatim; zero disables a listener
// individually.
func TestInternalDomainPortsFollowTheDeclaration(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	assert.NoError(t, extension.SetProvider(portsProvider{grpcPort: 26330, restPort: 0}))

	listeners := internalDomainListeners()
	assert.Equal(t, 26330, listeners.GRPCPort)
	assert.Zero(t, listeners.RESTPort)
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
	require.NoError(t, s.startInternalDomainGrpc("", grpcPort))
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
	require.NoError(t, s.startInternalDomainRest("", restPort))
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

// A listener that cannot be opened must abort start-up through the merr
// framework, not with a raw fmt.Errorf: callers up the stack read the code, and
// a bare error reaches them as an unclassified failure.
func TestInternalDomainStartFailureIsAMerrError(t *testing.T) {
	paramtable.Init()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	// Hold the port the declaration asks for, so net.Listen inside the
	// starter fails for a real reason rather than a synthetic one.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })
	port := occupied.Addr().(*net.TCPAddr).Port
	require.NoError(t, extension.SetProvider(portsProvider{grpcPort: port, restPort: 0}))

	s := &Server{ctx: context.Background()}
	err = s.startInternalDomainServers()
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal,
		"a failed internal-domain listener must be classified, not raw")
}

// The same for the TLS branch, which is a separate raw-error site: a form
// that declares the surfaces but points at an unreadable certificate is a
// broken deployment and must say so in the framework's vocabulary.
func TestInternalDomainGrpcTLSFailureIsAMerrError(t *testing.T) {
	paramtable.Init()
	params := &paramtable.Get().ProxyGrpcServerCfg
	paramtable.Get().Save(params.TLSMode.Key, "1")
	paramtable.Get().Save(params.ServerPemPath.Key, "/nonexistent/server.pem")
	paramtable.Get().Save(params.ServerKeyPath.Key, "/nonexistent/server.key")
	t.Cleanup(func() {
		paramtable.Get().Reset(params.TLSMode.Key)
		paramtable.Get().Reset(params.ServerPemPath.Key)
		paramtable.Get().Reset(params.ServerKeyPath.Key)
	})

	s := &Server{ctx: context.Background()}
	err := s.startInternalDomainGrpc("", freeTestPort(t))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

// The internal-domain REST listener is the same class of server as the
// external one and must carry the same timeout policy. Without
// ReadHeaderTimeout in particular a client can hold a connection open by
// dribbling headers, which is the Slowloris shape gosec flags; the remaining
// four are the sibling server's settings, so the two listeners cannot drift.
func TestInternalDomainRestServerCarriesTheHTTPTimeouts(t *testing.T) {
	paramtable.Init()
	accesslog.InitAccessLogger(paramtable.Get())
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	mockProxy := mocks.NewMockProxy(t)
	s := &Server{ctx: context.Background(), proxy: mockProxy}
	require.NoError(t, s.startInternalDomainRest("", freeTestPort(t)))
	t.Cleanup(func() { _ = s.internalDomainHTTPServer.Close() })

	assert.Equal(t, 5*time.Second, s.internalDomainHTTPServer.ReadHeaderTimeout,
		"the header-read timeout is what closes the Slowloris window")
	assert.Equal(t, 300*time.Second, s.internalDomainHTTPServer.IdleTimeout)
	assert.Zero(t, s.internalDomainHTTPServer.MaxHeaderBytes,
		"unset, so net/http applies its 1MiB default; the external listener's 16MiB "+
			"is an HTTP/2 shared-port concession that does not apply to this socket")
}
