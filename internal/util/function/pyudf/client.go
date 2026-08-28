// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/pyudfpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ExecuteRequest borrows Inputs and Params for the duration of Execute. Each
// input column has one chunk per query, including queries with zero rows.
type ExecuteRequest struct {
	ResourceName string
	UDFPath      string
	Stage        string
	Params       *schemapb.FunctionParamObject
	Inputs       []*arrow.Chunked
}

// Client owns the process's fixed worker connections. Concurrent Execute calls
// share these connections without a queue, admission limit or business retries.
type Client struct {
	address         string
	rpcTimeout      time.Duration
	maxMessageBytes int
	conns           []*grpc.ClientConn
	next            atomic.Uint64
	closed          atomic.Bool
}

var sharedClient struct {
	sync.Mutex
	client *Client
}

// NewClient returns the shared client for the single restart-only configuration.
// Construction does not wait for worker readiness or start Python processes.
func NewClient(config Config) (*Client, error) { return newClient(config, grpc.NewClient) }

func newClient(config Config, dial func(string, ...grpc.DialOption) (*grpc.ClientConn, error)) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	sharedClient.Lock()
	defer sharedClient.Unlock()
	if c := sharedClient.client; c != nil {
		if c.address != config.Address || len(c.conns) != config.ConnectionPoolSize || c.maxMessageBytes != config.MaxMessageBytes || c.rpcTimeout != config.RPCTimeout {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: client configuration changed; restart is required")
		}
		return c, nil
	}
	c := &Client{address: config.Address, rpcTimeout: config.RPCTimeout, maxMessageBytes: config.MaxMessageBytes}
	for i := 0; i < config.ConnectionPoolSize; i++ {
		conn, err := dial("passthrough:///"+config.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(false), grpc.MaxCallSendMsgSize(config.MaxMessageBytes), grpc.MaxCallRecvMsgSize(config.MaxMessageBytes)))
		if err != nil {
			for _, opened := range c.conns {
				_ = opened.Close()
			}
			return nil, merr.WrapErrServiceUnavailableErr(err, "py_udf: create client connection")
		}
		c.conns = append(c.conns, conn)
	}
	sharedClient.client = c
	return c, nil
}

// CloseClients closes the shared connections after new submissions have stopped.
// Outstanding RPCs fail through gRPC; this does not wait for Python execution.
func CloseClients() error {
	sharedClient.Lock()
	defer sharedClient.Unlock()
	c := sharedClient.client
	if c == nil {
		return nil
	}
	c.closed.Store(true)
	sharedClient.client = nil
	var err error
	for _, conn := range c.conns {
		err = merr.Combine(err, conn.Close())
	}
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "py_udf: close client connections")
	}
	return nil
}

// Execute returns caller-owned output columns. On failure no partial output is
// returned. Encoding/decoding run synchronously, checking the deadline between
// batches and before publishing results; no background operation borrows inputs.
func (c *Client) Execute(ctx context.Context, request ExecuteRequest) (outputs []*arrow.Chunked, err error) {
	if ctx == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: nil Execute context")
	}
	ctx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()
	defer func() {
		if ctx.Err() != nil {
			err = merr.Wrap(ctx.Err(), "py_udf: Execute")
		}
		if err != nil {
			releaseColumns(outputs)
			outputs = nil
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, merr.Wrap(err, "py_udf: Execute")
	}
	if c.closed.Load() {
		return nil, merr.WrapErrServiceUnavailableMsg("py_udf: client is closed")
	}
	wire, err := encodeExecuteRequest(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, merr.Wrap(err, "py_udf: encode inputs")
	}
	index := c.next.Inc() % uint64(len(c.conns))
	response, err := pyudfpb.NewPyUDFWorkerClient(c.conns[index]).Execute(ctx, wire)
	if err != nil {
		return nil, transportError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, merr.Wrap(err, "py_udf: Execute")
	}
	if response == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: missing response")
	}
	switch result := response.Result.(type) {
	case *pyudfpb.ExecuteResponse_Error:
		return nil, executionError(result.Error)
	case *pyudfpb.ExecuteResponse_Outputs:
		return decodeOutputs(ctx, result.Outputs)
	default:
		return nil, merr.WrapErrServiceInternalMsg("py_udf: missing response result")
	}
}
