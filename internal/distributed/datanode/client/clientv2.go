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

package grpcdatanodeclient

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"google.golang.org/grpc"
)

// TODO
const datanodePrefix = "foo/bar/"

// ClientV2 simplify the Client and make grpc connection logic clearer
type ClientV2 struct {
	nodeID      int64
	cli         datapb.DataNodeClient
	conn        *grpc.ClientConn
	etcdAddress string
	connCh      chan struct{}
}

func NewClientV2(nodeID int64, etcdAddress string) *ClientV2 {
	return &ClientV2{
		nodeID:      nodeID,
		etcdAddress: etcdAddress,
		connCh:      make(chan struct{}, 1),
	}
}

func (cli *ClientV2) initGrpcClient(ctx context.Context) error {
	var err error
	if cli.cli != nil {
		return nil
	}
	select {
	case cli.connCh <- struct{}{}:
		// only one goroutine can try to connect with server at the same time
		defer func() {
			<-cli.connCh
		}()
		if cli.cli != nil {
			return nil
		}
		cli.conn, err = grpcclient.Connect(ctx, cli.etcdAddress, fmt.Sprintf("etcd:///%s/%d", datanodePrefix, cli.nodeID))
		if err != nil {
			return err
		}
		cli.cli = datapb.NewDataNodeClient(cli.conn)
	case <-ctx.Done():
		return fmt.Errorf("init grpc client timeout for node %d", cli.nodeID)
	}
	return nil
}

func (cli *ClientV2) Init() error {
	return nil
}

func (cli *ClientV2) Start() error {
	return nil
}

func (cli *ClientV2) Stop() error {
	if cli.conn != nil {
		return cli.conn.Close()
	}
	return nil
}

func (cli *ClientV2) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	if err := cli.initGrpcClient(ctx); err != nil {
		return nil, err
	}
	return cli.cli.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
}

func (cli *ClientV2) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	if err := cli.initGrpcClient(ctx); err != nil {
		return nil, err
	}
	return cli.cli.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
}

func (cli *ClientV2) Register() error {
	return nil
}

// Deprecated
func (cli *ClientV2) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	if err := cli.initGrpcClient(ctx); err != nil {
		return nil, err
	}
	return cli.cli.WatchDmChannels(ctx, req)
}

// FlushSegments notifies DataNode to flush the segments req provids. The flush tasks are async to this
//  rpc, DataNode will flush the segments in the background.
//
// Return UnexpectedError code in status:
//     If DataNode isn't in HEALTHY: states not HEALTHY or dynamic checks not HEALTHY
//     If DataNode doesn't find the correspounding segmentID in its memeory replica
// Return Success code in status and trigers background flush:
//     Log an info log if a segment is under flushing
func (cli *ClientV2) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	if err := cli.initGrpcClient(ctx); err != nil {
		return nil, err
	}
	return cli.cli.FlushSegments(ctx, req)
}

// GetMetrics gets the metrics about DataNode.
func (cli *ClientV2) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if err := cli.initGrpcClient(ctx); err != nil {
		return nil, err
	}
	return cli.cli.GetMetrics(ctx, req)
}

// Compaction will add a compaction task according to the request plan
func (cli *ClientV2) Compaction(ctx context.Context, req *datapb.CompactionPlan) (*commonpb.Status, error) {
	if err := cli.initGrpcClient(ctx); err != nil {
		return nil, err
	}
	return cli.cli.Compaction(ctx, req)
}
