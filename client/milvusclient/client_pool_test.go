// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

type poolTestServer struct {
	milvuspb.UnimplementedMilvusServiceServer
	milvuspb.UnimplementedClientTelemetryServiceServer

	nextID      atomic.Int64
	failConnect atomic.Bool
	failAfter   atomic.Int64
	failDial    atomic.Bool
	liveConns   atomic.Int64
	mut         sync.Mutex
	requests    []metadata.MD

	blockIdentifier  string
	blockStarted     chan struct{}
	blockRelease     chan struct{}
	blockHeartbeat   atomic.Bool
	streamHeaders    chan metadata.MD
	heartbeatHeaders chan metadata.MD
}

func (s *poolTestServer) Connect(context.Context, *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	if s.failConnect.Load() || (s.failAfter.Load() > 0 && s.nextID.Load() >= s.failAfter.Load()) {
		return nil, status.Error(codes.Unavailable, "connect unavailable")
	}
	return &milvuspb.ConnectResponse{
		Status:     &commonpb.Status{},
		Identifier: s.nextID.Add(1),
		ServerInfo: &commonpb.ServerInfo{BuildTags: "v3.0.0-test"},
	}, nil
}

func (s *poolTestServer) ListDatabases(ctx context.Context, _ *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.mut.Lock()
	s.requests = append(s.requests, md.Copy())
	block := len(md.Get(identifierHeader)) > 0 && md.Get(identifierHeader)[0] == s.blockIdentifier
	started, release := s.blockStarted, s.blockRelease
	s.mut.Unlock()
	if block {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return &milvuspb.ListDatabasesResponse{Status: &commonpb.Status{}}, nil
}

func (s *poolTestServer) CreateReplicateStream(stream milvuspb.MilvusService_CreateReplicateStreamServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	s.streamHeaders <- md.Copy()
	_, _ = stream.Recv()
	return nil
}

func (s *poolTestServer) ClientHeartbeat(ctx context.Context, _ *milvuspb.ClientHeartbeatRequest) (*milvuspb.ClientHeartbeatResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	select {
	case s.heartbeatHeaders <- md.Copy():
	default:
	}
	if s.blockHeartbeat.Load() {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &milvuspb.ClientHeartbeatResponse{Status: &commonpb.Status{}}, nil
}

func (s *poolTestServer) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (s *poolTestServer) HandleRPC(context.Context, stats.RPCStats) {}
func (s *poolTestServer) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (s *poolTestServer) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		s.liveConns.Add(1)
	case *stats.ConnEnd:
		s.liveConns.Add(-1)
	}
}

func (s *poolTestServer) gotRequests() []metadata.MD {
	s.mut.Lock()
	defer s.mut.Unlock()
	return append([]metadata.MD(nil), s.requests...)
}

func newPoolTestBackend(t *testing.T, config *ClientConfig) *poolTestServer {
	t.Helper()
	listener := bufconn.Listen(bufSize)
	backend := &poolTestServer{
		streamHeaders:    make(chan metadata.MD, 10),
		heartbeatHeaders: make(chan metadata.MD, 100),
	}
	server := grpc.NewServer(grpc.StatsHandler(backend))
	milvuspb.RegisterMilvusServiceServer(server, backend)
	milvuspb.RegisterClientTelemetryServiceServer(server, backend)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-serveDone
	})
	config.Address = "bufnet"
	config.DialOptions = append(config.DialOptions, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		if backend.failDial.Load() {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return listener.DialContext(ctx)
	}))
	if config.TelemetryConfig == nil {
		config.TelemetryConfig = &TelemetryConfig{Enabled: false}
	}
	return backend
}

func newPoolTestClient(t *testing.T, config *ClientConfig) (*Client, *poolTestServer) {
	t.Helper()
	backend := newPoolTestBackend(t, config)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.Background())) })
	return client, backend
}

func poolConnections(client *Client) []*clientConn {
	client.connectionsMut.RLock()
	defer client.connectionsMut.RUnlock()
	return append([]*clientConn(nil), client.connections...)
}

func awaitPoolResult[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for result")
		var zero T
		return zero
	}
}

func blockPoolRequest(t *testing.T, client *Client, backend *poolTestServer) <-chan error {
	t.Helper()
	old := poolConnections(client)[0]
	backend.mut.Lock()
	backend.blockIdentifier = old.getIdentifier()
	backend.blockStarted = make(chan struct{}, 1)
	backend.blockRelease = make(chan struct{})
	started := backend.blockStarted
	backend.mut.Unlock()
	done := make(chan error, 1)
	go func() {
		_, err := client.ListDatabase(context.Background(), NewListDatabaseOption())
		done <- err
	}()
	awaitPoolResult(t, started)
	return done
}

func TestClientConnectionPool(t *testing.T) {
	client, backend := newPoolTestClient(t, &ClientConfig{ConnectionPoolSize: 3})
	connections := poolConnections(client)
	require.Len(t, connections, 3)
	for i := 0; i < 6; i++ {
		_, err := client.ListDatabase(context.Background(), NewListDatabaseOption())
		require.NoError(t, err)
	}
	var identifiers []string
	for _, md := range backend.gotRequests() {
		identifiers = append(identifiers, md.Get(identifierHeader)[0])
	}
	require.Equal(t, []string{"1", "2", "3", "1", "2", "3"}, identifiers)
	require.NoError(t, client.Close(context.Background()))
	for _, connection := range connections {
		require.Equal(t, connectivity.Shutdown, connection.conn.GetState())
	}
	require.NoError(t, client.Close(context.Background()))
	require.Nil(t, client.GetService())
	_, err := client.ListDatabase(context.Background(), NewListDatabaseOption())
	require.Error(t, err)
	_, err = client.CreateReplicateStream(context.Background())
	require.Error(t, err)
}

func TestClientConnectionRotation(t *testing.T) {
	client, backend := newPoolTestClient(t, &ClientConfig{
		ConnectionPoolSize: 2, ConnectionMaxAge: 40 * time.Millisecond,
		ConnectionDrainTimeout: time.Second, ConnectionRotationTimeout: time.Second,
	})
	initial := poolConnections(client)
	stable := client.GetService()
	require.Eventually(t, func() bool {
		current := poolConnections(client)
		return current[0] != initial[0] && current[1] != initial[1]
	}, 3*time.Second, 10*time.Millisecond)
	require.Same(t, stable, client.GetService())
	_, err := client.ListDatabase(context.Background(), NewListDatabaseOption())
	require.NoError(t, err)
	require.NotContains(t, []string{"1", "2", "3"}, backend.gotRequests()[0].Get(identifierHeader)[0])
	require.Equal(t, "v3.0.0-test", client.config.GetServerVersion())
}

func TestClientConnectionRotationDrainsUnary(t *testing.T) {
	client, backend := newPoolTestClient(t, &ClientConfig{ConnectionMaxAge: time.Hour})
	old := poolConnections(client)[0]
	requestDone := blockPoolRequest(t, client, backend)
	rotateDone := make(chan error, 1)
	go func() { rotateDone <- client.rotateConnection(0) }()
	require.Eventually(t, func() bool { return poolConnections(client)[0] != old }, time.Second, time.Millisecond)
	require.NotEqual(t, connectivity.Shutdown, old.conn.GetState())
	_, err := client.ListDatabase(context.Background(), NewListDatabaseOption())
	require.NoError(t, err)
	close(backend.blockRelease)
	require.NoError(t, awaitPoolResult(t, requestDone))
	require.NoError(t, awaitPoolResult(t, rotateDone))
	require.Equal(t, connectivity.Shutdown, old.conn.GetState())
}

func TestClientConnectionDrainTimeout(t *testing.T) {
	client, backend := newPoolTestClient(t, &ClientConfig{
		ConnectionMaxAge: time.Hour, ConnectionDrainTimeout: 30 * time.Millisecond,
	})
	old := poolConnections(client)[0]
	requestDone := blockPoolRequest(t, client, backend)
	require.NoError(t, client.rotateConnection(0))
	require.Error(t, awaitPoolResult(t, requestDone))
	require.Equal(t, connectivity.Shutdown, old.conn.GetState())
	_, err := client.ListDatabase(context.Background(), NewListDatabaseOption())
	require.NoError(t, err)
}

func TestClientConnectionRotationFailureKeepsOldConnection(t *testing.T) {
	for _, failure := range []string{"dial", "handshake"} {
		t.Run(failure, func(t *testing.T) {
			client, backend := newPoolTestClient(t, &ClientConfig{
				ConnectionMaxAge: time.Hour, ConnectionRotationTimeout: 50 * time.Millisecond,
			})
			old := poolConnections(client)[0]
			if failure == "dial" {
				backend.failDial.Store(true)
			} else {
				backend.failConnect.Store(true)
			}
			require.Error(t, client.rotateConnection(0))
			require.Same(t, old, poolConnections(client)[0])
			require.NotEqual(t, connectivity.Shutdown, old.conn.GetState())
			backend.failDial.Store(false)
			backend.failConnect.Store(false)
			require.Eventually(t, func() bool { return backend.liveConns.Load() == 2 }, time.Second, time.Millisecond)
			_, err := client.ListDatabase(context.Background(), NewListDatabaseOption())
			require.NoError(t, err)
			require.NoError(t, client.rotateConnection(0))
			require.NotSame(t, old, poolConnections(client)[0])
		})
	}
}

func TestClientPoolInitializationFailureClosesConnections(t *testing.T) {
	for _, stableFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "pool", true: "stable"}[stableFailure], func(t *testing.T) {
			config := &ClientConfig{ConnectionPoolSize: 2}
			successes := int64(1)
			if stableFailure {
				config.ConnectionMaxAge = time.Hour
				successes = 2
			}
			backend := newPoolTestBackend(t, config)
			backend.failAfter.Store(successes)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			client, err := New(ctx, config)
			require.Error(t, err)
			require.Nil(t, client)
			require.Equal(t, successes, backend.nextID.Load())
			require.Eventually(t, func() bool { return backend.liveConns.Load() == 0 }, time.Second, time.Millisecond)
		})
	}
}

func TestStreamingUsesNonRotatingConnection(t *testing.T) {
	client, backend := newPoolTestClient(t, &ClientConfig{ConnectionPoolSize: 2, ConnectionMaxAge: time.Hour})
	requestID := "4bf92f3577b34da6a3ce929d0e0e4736"
	ctx, cancel := context.WithCancel(WithClientRequestID(context.Background(), requestID))
	defer cancel()
	stream, err := client.CreateReplicateStream(ctx)
	require.NoError(t, err)
	md := awaitPoolResult(t, backend.streamHeaders)
	require.Equal(t, []string{"3"}, md.Get(identifierHeader))
	require.Equal(t, []string{requestID}, md.Get(ClientRequestIDKey))
	require.NoError(t, client.rotateConnection(0))
	require.NoError(t, client.rotateConnection(1))
	// An existing stream can still send after both unary transports are replaced.
	request := &milvuspb.ReplicateRequest{}
	require.NoError(t, stream.Send(request))
	require.NoError(t, stream.CloseSend())
	stream, err = client.CreateReplicateStream(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	md = awaitPoolResult(t, backend.streamHeaders)
	require.Equal(t, []string{"3"}, md.Get(identifierHeader))
}

func TestClientCloseUnblocksDatabaseSwitchAndRotation(t *testing.T) {
	client, backend := newPoolTestClient(t, &ClientConfig{ConnectionMaxAge: time.Hour})
	requestDone := blockPoolRequest(t, client, backend)
	dbDone := make(chan error, 1)
	go func() { dbDone <- client.UseDatabase(context.Background(), NewUseDatabaseOption("db2")) }()
	// Wait until UseDatabase is queued for the write lock. TryRLock fails once
	// a writer is pending, while the active RPC still owns a read lock.
	require.Eventually(t, func() bool {
		if client.lifecycleMut.TryRLock() {
			client.lifecycleMut.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	rotationStarted := make(chan struct{})
	client.rotationWG.Add(1)
	go func() {
		defer client.rotationWG.Done()
		close(rotationStarted)
		_ = client.rotateConnection(0)
	}()
	awaitPoolResult(t, rotationStarted)
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	require.NoError(t, awaitPoolResult(t, closeDone))
	require.Error(t, awaitPoolResult(t, requestDone))
	require.Error(t, awaitPoolResult(t, dbDone))
}

func TestClientCloseCancelsUnboundedDrain(t *testing.T) {
	client, backend := newPoolTestClient(t, &ClientConfig{ConnectionMaxAge: time.Hour})
	old := poolConnections(client)[0]
	requestDone := blockPoolRequest(t, client, backend)
	client.rotationWG.Add(1)
	go func() {
		defer client.rotationWG.Done()
		_ = client.rotateConnection(0)
	}()
	require.Eventually(t, func() bool { return poolConnections(client)[0] != old }, time.Second, time.Millisecond)
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	require.NoError(t, awaitPoolResult(t, closeDone))
	require.Error(t, awaitPoolResult(t, requestDone))
	require.Equal(t, connectivity.Shutdown, old.conn.GetState())
}

func TestClientCloseWhileCreatingStream(t *testing.T) {
	entered := make(chan struct{})
	client, _ := newPoolTestClient(t, &ClientConfig{
		ConnectionMaxAge: time.Hour,
		DialOptions: []grpc.DialOption{grpc.WithChainStreamInterceptor(func(ctx context.Context, _ *grpc.StreamDesc, cc *grpc.ClientConn, _ string, _ grpc.Streamer, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			close(entered)
			for state := cc.GetState(); state != connectivity.Shutdown; state = cc.GetState() {
				if !cc.WaitForStateChange(ctx, state) {
					return nil, ctx.Err()
				}
			}
			return nil, status.Error(codes.Canceled, "transport closed")
		})},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streamDone := make(chan error, 1)
	go func() {
		_, err := client.CreateReplicateStream(ctx)
		streamDone <- err
	}()
	awaitPoolResult(t, entered)
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	require.NoError(t, awaitPoolResult(t, closeDone))
	require.Error(t, awaitPoolResult(t, streamDone))
}

func TestPoolDatabaseSwitchAndRequestMetadata(t *testing.T) {
	client, backend := newPoolTestClient(t, &ClientConfig{
		DBName: "db1", APIKey: "token", ConnectionPoolSize: 2, ConnectionMaxAge: time.Hour,
	})
	// A failed validation must restore the database used by every connection.
	backend.failConnect.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, client.UseDatabase(ctx, NewUseDatabaseOption("missing")))
	require.Equal(t, "db1", client.getCurrentDB())
	backend.failConnect.Store(false)
	require.NoError(t, client.UseDatabase(context.Background(), NewUseDatabaseOption("db2")))
	require.NoError(t, client.rotateConnection(0))
	requestID := "4bf92f3577b34da6a3ce929d0e0e4736"
	ctx = WithClientRequestID(context.Background(), requestID)
	for i := 0; i < 4; i++ {
		_, err := client.ListDatabase(ctx, NewListDatabaseOption())
		require.NoError(t, err)
	}
	current := poolConnections(client)
	for i, md := range backend.gotRequests() {
		require.Equal(t, []string{current[i%2].getIdentifier()}, md.Get(identifierHeader))
		require.Equal(t, []string{"db2"}, md.Get(databaseHeader))
		require.Equal(t, []string{requestID}, md.Get(ClientRequestIDKey))
		require.NotEmpty(t, md.Get(authorizationHeader))
		require.Len(t, md.Get(ClientRequestMsecKey), 1)
	}
}

func TestPoolTelemetrySurvivesRotation(t *testing.T) {
	config := DefaultTelemetryConfig()
	config.HeartbeatInterval = 10 * time.Millisecond
	client, backend := newPoolTestClient(t, &ClientConfig{
		ConnectionPoolSize: 2, ConnectionMaxAge: 20 * time.Millisecond, TelemetryConfig: config,
	})
	initial := awaitPoolResult(t, backend.heartbeatHeaders)
	require.Equal(t, []string{"3"}, initial.Get(identifierHeader))
	require.Eventually(t, func() bool { return backend.nextID.Load() > 5 }, 3*time.Second, time.Millisecond)
	for i := 0; i < 10; i++ {
		md := awaitPoolResult(t, backend.heartbeatHeaders)
		require.Equal(t, initial.Get(identifierHeader), md.Get(identifierHeader))
		reply := client.telemetry.handleGetConfig(&ClientCommand{CommandId: "config"})
		require.True(t, reply.Success)
		var config GetConfigResponse
		require.NoError(t, json.Unmarshal(reply.Payload, &config))
		require.Equal(t, "v3.0.0-test", config.UserConfig["server_version"])
	}
	// Close must interrupt the heartbeat transport before waiting for its loop.
	backend.blockHeartbeat.Store(true)
	for len(backend.heartbeatHeaders) > 0 {
		<-backend.heartbeatHeaders
	}
	awaitPoolResult(t, backend.heartbeatHeaders)
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	require.NoError(t, awaitPoolResult(t, closeDone))
}
