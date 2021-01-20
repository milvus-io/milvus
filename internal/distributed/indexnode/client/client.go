package grpcindexnodeclient

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"google.golang.org/grpc"
)

type Client struct {
	grpcClient indexpb.IndexNodeClient
}

func (c Client) BuildIndex(req *indexpb.BuildIndexCmd) (*commonpb.Status, error) {

	ctx := context.TODO()

	return c.grpcClient.BuildIndex(ctx, req)
}

func NewClient(nodeAddress string) *Client {
	ctx1, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx1, nodeAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Connect to IndexNode failed, error= %v", err)
	}
	log.Printf("Connected to IndexService, IndexService=%s", nodeAddress)
	return &Client{
		grpcClient: indexpb.NewIndexNodeClient(conn),
	}
}
