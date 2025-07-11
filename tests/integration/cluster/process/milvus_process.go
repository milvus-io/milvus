package process

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding ownership. The ASF licenses this file
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

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	MilvusClusterComponent = "cluster-manager"
)

// nodeClient is the client for the every Milvus process.
type nodeClient interface {
	// GetComponentStates gets the component states of the Milvus process.
	// use to check if the milvus process is ready.
	GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)

	// Close closes the client.
	io.Closer
}

// MilvusProcess represents a running Milvus process
type MilvusProcess struct {
	log.Binder

	notifier *syncutil.AsyncTaskNotifier[error]
	graceful lifetime.SafeChan
	client   *syncutil.Future[io.Closer]

	stopCallback func(*MilvusProcess)
	etcdCli      *clientv3.Client
	cmd          *exec.Cmd
	env          map[string]string
	role         string
	nodeID       int64
	workDir      string
	rootPath     string
	wg           sync.WaitGroup
}

// Option is a function that configures a MilvusProcess
type Option func(*MilvusProcess)

// WithWorkDir sets the working directory for the process
func WithWorkDir(workDir string) Option {
	return func(mp *MilvusProcess) {
		mp.workDir = workDir
	}
}

// WithRole sets the role for the process
func WithRole(role string) Option {
	return func(mp *MilvusProcess) {
		for _, r := range []string{"mixcoord", "proxy", "datanode", "querynode", "streamingnode"} {
			if role == r {
				mp.role = role
				return
			}
		}
		panic(fmt.Errorf("invalid role: %s", role))
	}
}

// WithServerID sets the server ID for the process
func WithServerID(serverID int64) Option {
	return func(mp *MilvusProcess) {
		mp.nodeID = serverID
	}
}

// WithEnvironment sets the environment variables for the process
func WithEnvironment(env map[string]string) Option {
	return func(mp *MilvusProcess) {
		mp.env = env
	}
}

func WithRootPath(rootPath string) Option {
	return func(mp *MilvusProcess) {
		mp.rootPath = rootPath
	}
}

// WithETCDClient sets the etcd client for the process
func WithETCDClient(etcdCli *clientv3.Client) Option {
	return func(mp *MilvusProcess) {
		mp.etcdCli = etcdCli
	}
}

// WithStopCallback sets the stop callback for the process
func WithStopCallback(stopCallback func(*MilvusProcess)) Option {
	return func(mp *MilvusProcess) {
		mp.stopCallback = stopCallback
	}
}

// NewMilvusProcess creates a new MilvusProcess instance
func NewMilvusProcess(opts ...Option) *MilvusProcess {
	mp := &MilvusProcess{
		env:      make(map[string]string),
		notifier: syncutil.NewAsyncTaskNotifier[error](),
		graceful: lifetime.NewSafeChan(),
		client:   syncutil.NewFuture[io.Closer](),
	}

	for _, opt := range opts {
		opt(mp)
	}
	mp.initCmd()
	go mp.background()
	return mp
}

// initCmd initializes the command to start the Milvus process
func (mp *MilvusProcess) initCmd() {
	// Find the milvus binary
	milvusBin := filepath.Join(mp.workDir, "bin", "milvus")
	if _, err := os.Stat(milvusBin); os.IsNotExist(err) {
		// Try current directory
		defaultMilvusBin := "./bin/milvus"
		if _, err := os.Stat(defaultMilvusBin); os.IsNotExist(err) {
			panic(fmt.Errorf("milvus binary not found at %s or ./bin/milvus", milvusBin))
		}
	}

	// Prepare command arguments
	cmdArgs := append([]string{"run"}, mp.role)

	mp.cmd = exec.Command(milvusBin, cmdArgs...) // #nosec G204
	mp.cmd.Dir = mp.workDir

	// Set up environment variables
	mp.cmd.Env = append(mp.cmd.Env, os.Environ()...)
	mp.env["LOG_LEVEL"] = "info"
	mp.env[sessionutil.MilvusNodeIDForTesting] = strconv.FormatInt(mp.nodeID, 10)
	mp.env["MQ_TYPE"] = "pulsar"
	mp.env["ETCD_ROOTPATH"] = mp.rootPath
	mp.env["MSGCHANNEL_CHANNAMEPREFIX_CLUSTER"] = mp.rootPath
	mp.env["MINIO_ROOTPATH"] = mp.rootPath
	mp.env["LOCALSTORAGE_PATH"] = path.Join(os.TempDir(), "milvus", "integration", mp.rootPath, strconv.FormatInt(mp.nodeID, 10))
	for k, v := range mp.env {
		mp.cmd.Env = append(mp.cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	// Set up stdout and stderr
	mp.cmd.Stdout = os.Stdout
	mp.cmd.Stderr = os.Stderr
	mp.SetLogger(mp.Logger().With(
		zap.String("role", mp.role),
		zap.String("rootPath", mp.rootPath),
		zap.Int64("nodeID", mp.nodeID),
		log.FieldComponent(MilvusClusterComponent),
	))
}

// background starts the Milvus process in the background
func (mp *MilvusProcess) background() (err error) {
	defer func() {
		mp.notifier.Cancel()
		mp.wg.Wait()
		mp.notifier.Finish(err)
		if mp.stopCallback != nil {
			mp.stopCallback(mp)
		}
		if err != nil {
			mp.Logger().Warn("subprocess fail to exit", zap.Error(err))
		}
	}()

	// Start the process
	mp.Logger().Info("Milvus process start...")
	if err := mp.cmd.Start(); err != nil {
		mp.Logger().Warn("failed to start milvus process", zap.Error(err))
		return errors.Wrap(err, "when start process")
	}

	mp.SetLogger(mp.Logger().With(zap.Int("pid", mp.cmd.Process.Pid)))
	mp.Logger().Info("Milvus process started")

	// Start monitoring goroutine
	waitFuture := syncutil.NewFuture[error]()
	mp.wg.Add(3)
	go mp.asyncWait(waitFuture)
	go mp.asyncGetClient(mp.notifier.Context())
	go mp.notifyBySignal()

	forceStop := mp.notifier.Context().Done()
	gracefulStop := mp.graceful.CloseCh()
	for {
		select {
		case <-gracefulStop:
			// graceful stop
			mp.Logger().Info("Graceful stop Milvus process", zap.Int("pid", mp.cmd.Process.Pid))
			gracefulStop = nil
			mp.cmd.Process.Signal(syscall.SIGTERM)
		case <-forceStop:
			// force stop
			mp.Logger().Info("Force stop Milvus process", zap.Int("pid", mp.cmd.Process.Pid))
			mp.cmd.Process.Signal(syscall.SIGKILL)
			forceStop = nil
		case <-waitFuture.Done():
			mp.Logger().Info("Milvus process stopped")
			return waitFuture.Get()
		}
	}
}

// notifyBySignal notifies the Sub milvus process to stop by current process signal.
func (mp *MilvusProcess) notifyBySignal() {
	defer mp.wg.Done()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-mp.notifier.Context().Done():
			return
		case <-sigChan:
			// Send SIGKILL to stop the sub milvus process.
			mp.cmd.Process.Signal(syscall.SIGKILL)
		}
	}
}

// monitor monitors the process and handles cleanup
func (mp *MilvusProcess) asyncWait(f *syncutil.Future[error]) {
	defer mp.wg.Done()

	err := mp.cmd.Wait()
	f.Set(err)
}

// GetNodeID returns the node ID of the Milvus process
func (mp *MilvusProcess) GetNodeID() int64 {
	return mp.nodeID
}

// IsWorking returns true if the Milvus process is working
func (mp *MilvusProcess) IsWorking() bool {
	return !mp.graceful.IsClosed()
}

// Stop stops the Milvus process with a graceful timeout
// If the process does not stop within the timeout, it will forcefully stop.
func (mp *MilvusProcess) Stop(gracefulTimeout ...time.Duration) error {
	mp.graceful.Close()
	timeout := 10 * time.Second
	if len(gracefulTimeout) > 0 {
		timeout = gracefulTimeout[0]
	}

	now := time.Now()
	mp.Logger().Info("stop milvus process...")
	defer func() {
		mp.Logger().Info("stop milvus process done", zap.Duration("cost", time.Since(now)))
	}()

	select {
	case <-mp.notifier.FinishChan():
		return mp.notifier.BlockAndGetResult()
	case <-time.After(timeout):
		// after timeout, cancel the notifier to notify a force stop
		return mp.ForceStop()
	}
}

// ForceStop forcefully stops the Milvus process
func (mp *MilvusProcess) ForceStop() error {
	mp.Logger().Info("force stop milvus process...")
	now := time.Now()
	defer func() {
		mp.Logger().Info("force stop milvus process done", zap.Duration("cost", time.Since(now)))
	}()

	mp.graceful.Close()
	mp.notifier.Cancel()
	return mp.notifier.BlockAndGetResult()
}

// waitForReady waits for the Milvus process to be ready.
func (mp *MilvusProcess) waitForReady(ctx context.Context) error {
	mp.GetClient(ctx)
	return nil
}

// GetClient gets the client for the Milvus process.
func (mp *MilvusProcess) GetClient(ctx context.Context) (io.Closer, error) {
	ctx2, cancel := contextutil.MergeContext(ctx, mp.notifier.Context())
	defer cancel()

	return mp.client.GetWithContext(ctx2)
}

// asyncGetClient gets the client for the Milvus process in the background.
func (mp *MilvusProcess) asyncGetClient(ctx context.Context) {
	defer mp.wg.Done()
	logger := mp.Logger().With(zap.String("operation", "Init"))
	now := time.Now()

	c, err := mp.getClient(ctx)
	if err != nil {
		logger.Warn("failed to get client", zap.Error(err))
		return
	}

	logger.Info("client get", zap.Duration("cost", time.Since(now)))
	now = time.Now()

	ticker := time.NewTicker(20 * time.Millisecond)
	for {
		resp, err := c.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
		if err == nil && resp.State.GetStateCode() == commonpb.StateCode_Healthy {
			break
		}

		select {
		case <-ctx.Done():
			c.Close()
			return
		case <-ticker.C:
		}
	}

	mp.client.Set(c)
	logger.Info("client set", zap.Duration("cost", time.Since(now)))
}

// asyncGetClient gets the client for the Milvus process in the background.
func (mp *MilvusProcess) getClient(ctx context.Context) (nodeClient, error) {
	logger := mp.Logger().With(zap.String("operation", "Init"))
	now := time.Now()

	addr, err := mp.GetAddress(ctx)
	if err != nil {
		return nil, err
	}
	logger.Info("get address from etcd", zap.Duration("cost", time.Since(now)))
	now = time.Now()

	conn, err := mp.getGrpcClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	logger.Info("create grpc client", zap.Duration("cost", time.Since(now)))
	switch mp.role {
	case "mixcoord":
		return newMixCoordClient(conn), nil
	case "proxy":
		return newProxyClient(conn), nil
	case "datanode":
		return newDataNodeClient(conn), nil
	case "querynode":
		return newQueryNodeClient(conn), nil
	case "streamingnode":
		return newQueryNodeClient(conn), nil
	default:
		return nil, fmt.Errorf("unknown role: %s", mp.role)
	}
}

// getAddress gets the address of the Milvus process
func (mp *MilvusProcess) GetAddress(ctx context.Context) (string, error) {
	var key string
	switch mp.role {
	case typeutil.MixCoordRole:
		key = path.Join(mp.rootPath, "meta/session", mp.role)
	case typeutil.StreamingNodeRole:
		key = path.Join(mp.rootPath, "meta/session", typeutil.QueryNodeRole) + "-" + strconv.FormatInt(mp.nodeID, 10)
	default:
		key = path.Join(mp.rootPath, "meta/session", mp.role) + "-" + strconv.FormatInt(mp.nodeID, 10)
	}

	// Wait for etcd key to be available with timeout
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-mp.notifier.Context().Done():
			return "", mp.notifier.BlockAndGetResult()
		case <-ticker.C:
			val, err := mp.etcdCli.Get(ctx, key)
			if err != nil {
				return "", fmt.Errorf("failed to get address from etcd: %w", err)
			}
			if len(val.Kvs) == 0 {
				continue
			}
			var session struct {
				Address string `json:"address"`
			}
			if err := json.Unmarshal(val.Kvs[0].Value, &session); err != nil {
				return "", fmt.Errorf("failed to unmarshal session info: %w", err)
			}
			return session.Address, nil
		}
	}
}

func (mp *MilvusProcess) getGrpcClient(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if mp.getConfigValueFromEnv(paramtable.Get().InternalTLSCfg.InternalTLSEnabled.Key) == "true" {
		caPemPath := mp.getConfigValueFromEnv(paramtable.Get().InternalTLSCfg.InternalTLSCaPemPath.Key)
		sni := mp.getConfigValueFromEnv(paramtable.Get().InternalTLSCfg.InternalTLSSNI.Key)
		creds, err := credentials.NewClientTLSFromFile(caPemPath, sni)
		if err != nil {
			panic(fmt.Errorf("failed to create client tls from file: %w", err))
		}
		mp.Logger().Info("create grpc client with tls")
		return DailGRPClient(ctx, addr, mp.rootPath, mp.nodeID, grpc.WithTransportCredentials(creds))
	}
	return DailGRPClient(ctx, addr, mp.rootPath, mp.nodeID)
}

// getConfigValueFromEnv gets the value of the environment variable from the process.
func (mp *MilvusProcess) getConfigValueFromEnv(key string) string {
	envKey := strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	return mp.env[envKey]
}

// DailGRPClient dials a grpc client with the given address, root path and node id.
func DailGRPClient(ctx context.Context, addr string, rootPath string, nodeID int64, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			clusterInjectionUnaryClientInterceptor(rootPath),
			interceptor.ServerIDInjectionUnaryClientInterceptor(nodeID),
		)),
		grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
			clusterInjectionStreamClientInterceptor(rootPath),
			interceptor.ServerIDInjectionStreamClientInterceptor(nodeID),
		)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                200 * time.Millisecond,
			Timeout:             2 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithPerRPCCredentials(&grpcclient.Token{Value: crypto.Base64Encode(util.MemberCredID)}),
		grpc.FailOnNonTempDialError(true),
		grpc.WithReturnConnectionError(),
		grpc.WithDisableRetry(),
	}
	opts = append(opts, extraOpts...)
	return grpc.DialContext(
		ctx,
		addr,
		opts...,
	)
}

// clusterInjectionUnaryClientInterceptor returns a new unary client interceptor that injects `cluster` into outgoing context.
func clusterInjectionUnaryClientInterceptor(clusterKey string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, interceptor.ClusterKey, clusterKey)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// clusterInjectionStreamClientInterceptor returns a new streaming client interceptor that injects `cluster` into outgoing context.
func clusterInjectionStreamClientInterceptor(clusterKey string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, interceptor.ClusterKey, clusterKey)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
