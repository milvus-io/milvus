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

package milvusclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/common"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/crypto"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

type Client struct {
	connectionsMut sync.RWMutex
	connections    []*clientConn
	streamConn     *clientConn
	nextConnection atomic.Uint64
	closed         bool

	closeMut     sync.Mutex
	lifecycleMut sync.RWMutex
	rotationCtx  context.Context
	rotationStop context.CancelFunc
	rotationWG   sync.WaitGroup

	// Bound to the non-rotating connection before telemetry starts.
	telemetryService milvuspb.ClientTelemetryServiceClient
	config           *ClientConfig

	// mutable status
	stateMut  sync.RWMutex
	currentDB string

	metadataHeaders map[string]string

	collCache *CollectionCache

	// Telemetry manager for metrics collection and heartbeat
	telemetry *ClientTelemetryManager
}

type clientConn struct {
	conn    *grpc.ClientConn
	service milvuspb.MilvusServiceClient

	stateMut   sync.RWMutex
	identifier string

	inflight  atomic.Int64
	retiring  atomic.Bool
	drained   chan struct{}
	drainOnce sync.Once
}

func New(ctx context.Context, config *ClientConfig) (*Client, error) {
	if err := config.parse(); err != nil {
		return nil, err
	}

	rotationCtx, rotationStop := context.WithCancel(context.Background())
	c := &Client{
		config:       config,
		currentDB:    config.DBName,
		rotationCtx:  rotationCtx,
		rotationStop: rotationStop,
	}

	// Parse remote address.
	addr := c.config.getParsedAddress()

	// parse authentication parameters
	c.parseAuthentication()

	// Independent ClientConns give an L4 load balancer multiple TCP connections
	// to distribute across Proxy instances.
	for i := 0; i < config.getConnectionPoolSize(); i++ {
		connection, err := c.newConnection(ctx, addr)
		if err != nil {
			_ = c.Close(context.Background())
			return nil, err
		}
		c.connections = append(c.connections, connection)
	}

	if config.ConnectionMaxAge > 0 {
		connection, err := c.newConnection(ctx, addr)
		if err != nil {
			_ = c.Close(context.Background())
			return nil, err
		}
		c.streamConn = connection
	}
	telemetryConn := c.connections[0]
	if c.streamConn != nil {
		telemetryConn = c.streamConn
	}
	c.telemetryService = milvuspb.NewClientTelemetryServiceClient(telemetryConn.conn)

	c.collCache = NewCollectionCache(func(ctx context.Context, collName string) (*entity.Collection, error) {
		return c.DescribeCollection(ctx, NewDescribeCollectionOption(collName))
	})

	// Initialize and start telemetry manager
	c.telemetry = NewClientTelemetryManager(c, config.TelemetryConfig)
	c.telemetry.Start()
	c.startConnectionRotation()

	return c, nil
}

func (c *Client) dialOptions() []grpc.DialOption {
	return c.dialOptionsWithInterceptors(c.MetadataUnaryInterceptor(), c.MetadataStreamInterceptor())
}

func (c *Client) dialOptionsForConnection(connection *clientConn) []grpc.DialOption {
	return c.dialOptionsWithInterceptors(
		c.metadataUnaryInterceptor(connection.getIdentifier),
		c.metadataStreamInterceptor(connection.getIdentifier),
	)
}

func (c *Client) dialOptionsWithInterceptors(unaryInterceptor grpc.UnaryClientInterceptor, streamInterceptor grpc.StreamClientInterceptor) []grpc.DialOption {
	var options []grpc.DialOption
	// Construct dial option.
	if c.config.EnableTLSAuth {
		if c.config.tlsConfig != nil {
			options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(c.config.tlsConfig)))
		} else {
			options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
		}
	} else {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Always apply default connection options first, then let caller override/extend.
	options = append(options, DefaultGrpcOpts...)
	options = append(options, c.config.DialOptions...)

	options = append(options,
		grpc.WithChainUnaryInterceptor(grpc_retry.UnaryClientInterceptor(
			grpc_retry.WithMax(6),
			grpc_retry.WithBackoff(func(attempt uint) time.Duration {
				return 60 * time.Millisecond * time.Duration(math.Pow(3, float64(attempt)))
			}),
			grpc_retry.WithCodes(codes.Unavailable, codes.ResourceExhausted)),

		// c.getRetryOnRateLimitInterceptor(),
		))

	options = append(options, grpc.WithChainUnaryInterceptor(
		unaryInterceptor,
	))

	options = append(options, grpc.WithChainStreamInterceptor(
		streamInterceptor,
	))

	return options
}

// parseAuthentication prepares authentication headers for grpc inteceptors based on the provided username, password or API key.
func (c *Client) parseAuthentication() {
	cfg := c.config
	c.metadataHeaders = make(map[string]string)
	if cfg.Username != "" || cfg.Password != "" {
		value := crypto.Base64Encode(fmt.Sprintf("%s:%s", cfg.Username, cfg.Password))
		c.metadataHeaders[authorizationHeader] = value
	}
	// API overwrites username & passwd
	if cfg.APIKey != "" {
		value := crypto.Base64Encode(cfg.APIKey)
		c.metadataHeaders[authorizationHeader] = value
	}
}

func (c *Client) Close(_ context.Context) error {
	c.closeMut.Lock()
	defer c.closeMut.Unlock()

	c.connectionsMut.Lock()
	if c.closed {
		c.connectionsMut.Unlock()
		return nil
	}
	c.closed = true
	connections := c.connections
	streamConn := c.streamConn
	c.connections = nil
	c.streamConn = nil
	c.connectionsMut.Unlock()

	if c.rotationStop != nil {
		c.rotationStop()
	}
	// Close transports before waiting: active RPCs may hold lifecycleMut while
	// a database switch and a rotation are waiting for that lock.
	var firstErr error
	for _, connection := range connections {
		if err := connection.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if streamConn != nil {
		if err := streamConn.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.rotationWG.Wait()
	if c.telemetry != nil {
		c.telemetry.Stop()
	}
	return firstErr
}

func (c *Client) usingDatabase(dbName string) {
	c.stateMut.Lock()
	defer c.stateMut.Unlock()
	c.currentDB = dbName
}

func (c *Client) getCurrentDB() string {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()
	return c.currentDB
}

func (c *clientConn) setIdentifier(identifier string) {
	c.stateMut.Lock()
	defer c.stateMut.Unlock()
	c.identifier = identifier
}

func (c *clientConn) getIdentifier() string {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()
	return c.identifier
}

func (c *Client) newConnection(ctx context.Context, addr string) (*clientConn, error) {
	if addr == "" {
		return nil, merr.WrapErrParameterInvalidMsg("address is empty")
	}
	connection := &clientConn{drained: make(chan struct{})}
	options := c.dialOptionsForConnection(connection)
	conn, err := grpc.DialContext(ctx, addr, options...)
	if err != nil {
		return nil, err
	}

	connection.conn = conn
	connection.service = milvuspb.NewMilvusServiceClient(conn)

	if !c.config.DisableConn {
		if err := c.connectConnection(ctx, connection); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}

	return connection, nil
}

func (c *Client) connectConnection(ctx context.Context, connection *clientConn) error {
	hostName, err := os.Hostname()
	if err != nil {
		return err
	}

	req := &milvuspb.ConnectRequest{
		ClientInfo: &commonpb.ClientInfo{
			SdkType:    "GoMilvusClient",
			SdkVersion: common.SDKVersion,
			LocalTime:  time.Now().String(),
			User:       c.config.Username,
			Host:       hostName,
		},
	}

	resp, err := connection.service.Connect(ctx, req)
	if err != nil {
		status, ok := status.FromError(err)
		if ok {
			if status.Code() == codes.Unimplemented {
				// disable unsupported feature
				c.config.addFlags(
					disableDatabase |
						disableJSON |
						disableParitionKey |
						disableDynamicSchema)
				return nil
			}
		}
		return err
	}

	if !merr.Ok(resp.GetStatus()) {
		return merr.Error(resp.GetStatus())
	}

	c.config.setServerInfo(resp.GetServerInfo().GetBuildTags())
	connection.setIdentifier(strconv.FormatInt(resp.GetIdentifier(), 10))

	return nil
}

func (c *Client) connectInternal(ctx context.Context) error {
	connection := c.acquireConnection()
	if connection == nil {
		return merr.WrapErrServiceNotReady("SDK", 0, "not connected")
	}
	defer connection.release()
	return c.connectConnection(ctx, connection)
}

// selectConnection returns the next connection. The caller must hold connectionsMut.
func (c *Client) selectConnection() *clientConn {
	if len(c.connections) == 0 {
		return nil
	}
	idx := c.nextConnection.Add(1) - 1
	return c.connections[idx%uint64(len(c.connections))]
}

func (c *Client) callService(fn func(milvusService milvuspb.MilvusServiceClient) error) error {
	c.lifecycleMut.RLock()
	defer c.lifecycleMut.RUnlock()

	connection := c.acquireConnection()
	if connection == nil {
		return merr.WrapErrServiceNotReady("SDK", 0, "not connected")
	}
	defer connection.release()

	return fn(connection.service)
}

func (c *Client) callStreamService(fn func(milvusService milvuspb.MilvusServiceClient) error) error {
	c.lifecycleMut.RLock()
	defer c.lifecycleMut.RUnlock()

	// GetService releases connectionsMut before invoking any RPC. In particular,
	// a stream waiting for a transport must not block Close from closing it.
	service := c.GetService()
	if service == nil {
		return merr.WrapErrServiceNotReady("SDK", 0, "not connected")
	}
	return fn(service)
}

// GetService returns a service bound to a non-rotating connection. Calls made
// through it bypass pool selection and graceful unary draining.
func (c *Client) GetService() milvuspb.MilvusServiceClient {
	c.connectionsMut.RLock()
	defer c.connectionsMut.RUnlock()

	connection := c.streamConn
	if connection == nil && len(c.connections) > 0 {
		connection = c.connections[0]
	}
	if connection == nil {
		return nil
	}
	return connection.service
}

// GetTelemetry returns the telemetry manager for this client
func (c *Client) GetTelemetry() *ClientTelemetryManager {
	return c.telemetry
}

// recordOperation records an operation for telemetry metrics (internal use only)
func (c *Client) recordOperation(operation, collection string, startTime time.Time, err error) {
	if c.telemetry != nil {
		c.telemetry.RecordOperation(operation, collection, startTime, err)
	}
}
