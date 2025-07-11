package process

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
)

type mixCoordClient struct {
	conn *grpc.ClientConn
	rootcoordpb.RootCoordClient
	querypb.QueryCoordClient
	datapb.DataCoordClient
}

func newMixCoordClient(conn *grpc.ClientConn) types.MixCoordClient {
	c := &mixCoordClient{
		conn:             conn,
		RootCoordClient:  rootcoordpb.NewRootCoordClient(conn),
		QueryCoordClient: querypb.NewQueryCoordClient(conn),
		DataCoordClient:  datapb.NewDataCoordClient(conn),
	}
	return c
}

func (c *mixCoordClient) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return c.RootCoordClient.GetComponentStates(ctx, in, opts...)
}

func (c *mixCoordClient) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return c.RootCoordClient.ShowConfigurations(ctx, in, opts...)
}

func (c *mixCoordClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return c.RootCoordClient.GetTimeTickChannel(ctx, in, opts...)
}

func (c *mixCoordClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return c.RootCoordClient.GetMetrics(ctx, in, opts...)
}

func (c *mixCoordClient) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return c.RootCoordClient.CheckHealth(ctx, in, opts...)
}

func (c *mixCoordClient) Close() error {
	return c.conn.Close()
}

type proxyClient struct {
	conn *grpc.ClientConn
	proxypb.ProxyClient
}

func newProxyClient(conn *grpc.ClientConn) types.ProxyClient {
	return &proxyClient{
		conn:        conn,
		ProxyClient: proxypb.NewProxyClient(conn),
	}
}

func (c *proxyClient) Close() error {
	return c.conn.Close()
}

type dataNodeClient struct {
	conn *grpc.ClientConn
	datapb.DataNodeClient
	workerpb.IndexNodeClient
}

func newDataNodeClient(conn *grpc.ClientConn) types.DataNodeClient {
	return &dataNodeClient{
		conn:            conn,
		DataNodeClient:  datapb.NewDataNodeClient(conn),
		IndexNodeClient: workerpb.NewIndexNodeClient(conn),
	}
}

func (c *dataNodeClient) CreateJob(ctx context.Context, in *workerpb.CreateJobRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return c.IndexNodeClient.CreateJob(ctx, in, opts...)
}

func (c *dataNodeClient) CreateJobV2(ctx context.Context, in *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return c.IndexNodeClient.CreateJobV2(ctx, in, opts...)
}

func (c *dataNodeClient) Close() error {
	return c.conn.Close()
}

type queryNodeClient struct {
	conn *grpc.ClientConn
	querypb.QueryNodeClient
}

func newQueryNodeClient(conn *grpc.ClientConn) types.QueryNodeClient {
	return &queryNodeClient{
		conn:            conn,
		QueryNodeClient: querypb.NewQueryNodeClient(conn),
	}
}

func (c *queryNodeClient) Close() error {
	return c.conn.Close()
}
