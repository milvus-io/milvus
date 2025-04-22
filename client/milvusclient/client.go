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
	"time"

	"github.com/cockroachdb/errors"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/common"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type Client struct {
	conn    *grpc.ClientConn
	service milvuspb.MilvusServiceClient
	config  *ClientConfig

	// mutable status
	stateMut   sync.RWMutex
	currentDB  string
	identifier string // Identifier for this connection

	metadataHeaders map[string]string

	collCache *CollectionCache
}

func New(ctx context.Context, config *ClientConfig) (*Client, error) {
	if err := config.parse(); err != nil {
		return nil, err
	}

	c := &Client{
		config:    config,
		currentDB: config.DBName,
	}

	// Parse remote address.
	addr := c.config.getParsedAddress()

	// parse authentication parameters
	c.parseAuthentication()
	// Parse grpc options
	options := c.dialOptions()

	// Connect the grpc server.
	if err := c.connect(ctx, addr, options...); err != nil {
		return nil, err
	}

	c.collCache = NewCollectionCache(func(ctx context.Context, collName string) (*entity.Collection, error) {
		return c.DescribeCollection(ctx, NewDescribeCollectionOption(collName))
	})

	return c, nil
}

func (c *Client) dialOptions() []grpc.DialOption {
	var options []grpc.DialOption
	// Construct dial option.
	if c.config.EnableTLSAuth {
		options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if c.config.DialOptions == nil {
		// Add default connection options.
		options = append(options, DefaultGrpcOpts...)
	} else {
		options = append(options, c.config.DialOptions...)
	}

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
		c.MetadataUnaryInterceptor(),
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

func (c *Client) Close(ctx context.Context) error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	if err != nil {
		return err
	}
	c.conn = nil
	c.service = nil
	return nil
}

func (c *Client) usingDatabase(dbName string) {
	c.stateMut.Lock()
	defer c.stateMut.Unlock()
	c.currentDB = dbName
}

func (c *Client) setIdentifier(identifier string) {
	c.stateMut.Lock()
	defer c.stateMut.Unlock()
	c.identifier = identifier
}

func (c *Client) connect(ctx context.Context, addr string, options ...grpc.DialOption) error {
	if addr == "" {
		return errors.New("address is empty")
	}
	conn, err := grpc.DialContext(ctx, addr, options...)
	if err != nil {
		return err
	}

	c.conn = conn
	c.service = milvuspb.NewMilvusServiceClient(c.conn)

	if !c.config.DisableConn {
		err = c.connectInternal(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) connectInternal(ctx context.Context) error {
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

	resp, err := c.service.Connect(ctx, req)
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
	c.setIdentifier(strconv.FormatInt(resp.GetIdentifier(), 10))
	if c.collCache != nil {
		c.collCache.Reset()
	}

	return nil
}

func (c *Client) callService(fn func(milvusService milvuspb.MilvusServiceClient) error) error {
	service := c.service
	if service == nil {
		return merr.WrapErrServiceNotReady("SDK", 0, "not connected")
	}

	return fn(c.service)
}
