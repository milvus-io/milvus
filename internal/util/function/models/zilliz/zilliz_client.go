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

package zilliz

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/proto/modelservicepb"
)

type clientConfig struct {
	endpoint           string
	enableTLS          bool
	caPemPath          string
	serverNameOverride string
	MaxRecvMsgSize     int
	MaxSendMsgSize     int
	Timeout            time.Duration
	KeepAliveTime      time.Duration
}

func defaultClientConfig(endpoint string, enableTLS bool, caPemPath string, serverNameOverride string) *clientConfig {
	return &clientConfig{
		endpoint:           endpoint,
		enableTLS:          enableTLS,
		caPemPath:          caPemPath,
		serverNameOverride: serverNameOverride,
		MaxRecvMsgSize:     1024 * 1024 * 100, // 100MB
		MaxSendMsgSize:     1024 * 1024 * 100, // 100MB
		Timeout:            10 * time.Second,
		KeepAliveTime:      30 * time.Second,
	}
}

type clientManager struct {
	mu     sync.Mutex
	conn   *grpc.ClientConn
	config *clientConfig
}

func loadConfig(config map[string]string) (*clientConfig, error) {
	endpoint := config["endpoint"]
	if endpoint == "" {
		return nil, fmt.Errorf("Zilliz client config error, lost endpoint config")
	}
	enableTLSStr := config["enableTLS"]
	enableTLS := false
	if enableTLSStr != "" {
		var err error
		if enableTLS, err = strconv.ParseBool(enableTLSStr); err != nil {
			return nil, fmt.Errorf("Zilliz client config err: enableTLS:%s is not bool, err:%w", enableTLSStr, err)
		}
	}
	if enableTLS {
		caPemPath := config["certFile"]
		serverNameOverride := config["serverNameOverride"]
		return defaultClientConfig(endpoint, enableTLS, caPemPath, serverNameOverride), nil
	}
	return defaultClientConfig(endpoint, enableTLS, "", ""), nil
}

func (m *clientManager) GetConn(clientConf *clientConfig) (*grpc.ClientConn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn != nil {
		if clientConf.endpoint != m.config.endpoint {
			_ = m.conn.Close()
			m.conn = nil
		} else {
			state := m.conn.GetState()
			if state == connectivity.Ready || state == connectivity.Idle || state == connectivity.Connecting {
				return m.conn, nil
			}
			_ = m.conn.Close()
			m.conn = nil
		}
	}
	var creds credentials.TransportCredentials
	if !clientConf.enableTLS {
		creds = insecure.NewCredentials()
	} else {
		var err error
		if creds, err = credentials.NewClientTLSFromFile(clientConf.caPemPath, clientConf.serverNameOverride); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), clientConf.Timeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		clientConf.endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(clientConf.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(clientConf.MaxSendMsgSize),
			grpc.UseCompressor("gzip"),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                clientConf.KeepAliveTime,
			Timeout:             clientConf.Timeout,
			PermitWithoutStream: true,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 10 * time.Millisecond,
		}),
	)
	if err != nil {
		return nil, err
	}
	m.config = clientConf
	m.conn = conn
	return conn, nil
}

func (m *clientManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		err := m.conn.Close()
		m.conn = nil
		return err
	}
	return nil
}

var (
	globalClientManager *clientManager
	globalClientOnce    sync.Once
)

func getClientManager() *clientManager {
	globalClientOnce.Do(func() {
		globalClientManager = &clientManager{}
	})
	return globalClientManager
}

type ZillizClient struct {
	modelDeploymentID string
	clusterID         string
	dbName            string
	conn              *grpc.ClientConn
}

func NewZilliClient(modelDeploymentID string, clusterID string, dbName string, info map[string]string) (*ZillizClient, error) {
	mgr := getClientManager()
	clientConf, err := loadConfig(info)
	if err != nil {
		return nil, err
	}
	conn, err := mgr.GetConn(clientConf)
	if err != nil {
		return nil, fmt.Errorf("Connect model serving failed, err:%w", err)
	}
	return &ZillizClient{
		modelDeploymentID: modelDeploymentID,
		clusterID:         clusterID,
		dbName:            dbName,
		conn:              conn,
	}, nil
}

func (c *ZillizClient) setMeta(ctx context.Context) context.Context {
	md := metadata.Pairs(
		"instance-id", c.clusterID,
		"model-deployment-id", c.modelDeploymentID,
		"db-name", c.dbName,
	)
	ctx = metadata.NewOutgoingContext(ctx, md)
	return ctx
}

func (c *ZillizClient) Embedding(ctx context.Context, texts []string, params map[string]string) ([][]float32, error) {
	stub := modelservicepb.NewTextEmbeddingServiceClient(c.conn)
	req := &modelservicepb.TextEmbeddingRequest{
		Texts:  texts,
		Params: params,
	}
	ctx = c.setMeta(ctx)
	res, err := stub.Embedding(ctx, req)
	if err != nil {
		return nil, err
	}
	embds := make([][]float32, 0, len(res.GetResults()))
	for _, ret := range res.GetResults() {
		reader := bytes.NewReader(ret.Dense.Data)
		n := len(ret.Dense.Data) / 4
		embd := make([]float32, n)
		err := binary.Read(reader, binary.LittleEndian, &embd)
		if err != nil {
			return nil, err
		}
		embds = append(embds, embd)
	}

	return embds, nil
}

func (c *ZillizClient) Rerank(ctx context.Context, query string, texts []string, params map[string]string) ([]float32, error) {
	stub := modelservicepb.NewRerankServiceClient(c.conn)
	req := &modelservicepb.TextRerankRequest{
		Query:     query,
		Documents: texts,
		Params:    params,
	}
	ctx = c.setMeta(ctx)
	res, err := stub.Rerank(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Scores, nil
}
