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

package client

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/common"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type Client struct {
	conn    *grpc.ClientConn
	service milvuspb.MilvusServiceClient
	config  *ClientConfig

	collCache *CollectionCache
}

func New(ctx context.Context, config *ClientConfig) (*Client, error) {
	if err := config.parse(); err != nil {
		return nil, err
	}

	c := &Client{
		config: config,
	}

	// Parse remote address.
	addr := c.config.getParsedAddress()

	// Parse grpc options
	options := c.config.getDialOption()

	// Connect the grpc server.
	if err := c.connect(ctx, addr, options...); err != nil {
		return nil, err
	}

	c.collCache = NewCollectionCache(func(ctx context.Context, collName string) (*entity.Collection, error) {
		return c.DescribeCollection(ctx, NewDescribeCollectionOption(collName))
	})

	return c, nil
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

func (c *Client) connect(ctx context.Context, addr string, options ...grpc.DialOption) error {
	if addr == "" {
		return fmt.Errorf("address is empty")
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
			SdkType:    "Golang",
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
			}
			return nil
		}
		return err
	}

	if !merr.Ok(resp.GetStatus()) {
		return merr.Error(resp.GetStatus())
	}

	c.config.setServerInfo(resp.GetServerInfo().GetBuildTags())
	c.config.setIdentifier(strconv.FormatInt(resp.GetIdentifier(), 10))

	return nil
}

func (c *Client) callService(fn func(milvusService milvuspb.MilvusServiceClient) error) error {
	service := c.service
	if service == nil {
		return merr.WrapErrServiceNotReady("SDK", 0, "not connected")
	}

	return fn(c.service)
}
