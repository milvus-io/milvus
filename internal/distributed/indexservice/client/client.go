package grpcindexserviceclient

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"google.golang.org/grpc"
)

type Client struct {
	grpcClient indexpb.IndexServiceClient
}

func (g Client) RegisterNode(req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {

	ctx := context.TODO()
	return g.grpcClient.RegisterNode(ctx, req)
}

func (g Client) BuildIndex(req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {

	ctx := context.TODO()

	return g.grpcClient.BuildIndex(ctx, req)
}

func (g Client) GetIndexStates(req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {

	ctx := context.TODO()

	return g.grpcClient.GetIndexStates(ctx, req)
}

func (g Client) GetIndexFilePaths(req *indexpb.IndexFilePathRequest) (*indexpb.IndexFilePathsResponse, error) {

	ctx := context.TODO()

	return g.grpcClient.GetIndexFilePaths(ctx, req)
}

func (g Client) NotifyBuildIndex(nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {
	ctx := context.TODO()

	return g.grpcClient.NotifyBuildIndex(ctx, nty)
}

func NewClient(address string) *Client {

	ctx1, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx1, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Connect to IndexService failed, error= %v", err)
	}
	log.Printf("Connected to IndexService, IndexService=%s", address)

	return &Client{
		grpcClient: indexpb.NewIndexServiceClient(conn),
	}
}
