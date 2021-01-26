package grpcindexserviceclient

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"google.golang.org/grpc"
)

type UniqueID = typeutil.UniqueID

type Client struct {
	grpcClient indexpb.IndexServiceClient
	address    string
}

func (g *Client) Init() error {
	return nil
}

func (g *Client) Start() error {
	return nil
}

func (g *Client) Stop() error {
	return nil
}

func (g *Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return nil, nil
}

func (g *Client) GetTimeTickChannel() (string, error) {
	return "", nil
}

func (g *Client) GetStatisticsChannel() (string, error) {
	return "", nil
}

func (g *Client) tryConnect() error {
	if g.grpcClient != nil {
		return nil
	}
	ctx1, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx1, g.address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Connect to IndexService failed, error= %v", err)
		return err
	}
	g.grpcClient = indexpb.NewIndexServiceClient(conn)
	return nil
}

func (g *Client) RegisterNode(req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {
	err := g.tryConnect()
	if err != nil {
		return nil, err
	}

	ctx := context.TODO()
	return g.grpcClient.RegisterNode(ctx, req)
}

func (g *Client) BuildIndex(req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	err := g.tryConnect()
	if err != nil {
		return nil, err
	}

	ctx := context.TODO()
	return g.grpcClient.BuildIndex(ctx, req)
}

func (g *Client) GetIndexStates(req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {
	err := g.tryConnect()
	if err != nil {
		return nil, err
	}

	ctx := context.TODO()
	return g.grpcClient.GetIndexStates(ctx, req)
}
func (g *Client) GetIndexFilePaths(req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error) {
	err := g.tryConnect()
	if err != nil {
		return nil, err
	}

	ctx := context.TODO()
	return g.grpcClient.GetIndexFilePaths(ctx, req)
}

func (g *Client) NotifyBuildIndex(nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {
	err := g.tryConnect()
	if err != nil {
		return nil, err
	}

	ctx := context.TODO()
	return g.grpcClient.NotifyBuildIndex(ctx, nty)
}

func NewClient(address string) *Client {

	log.Println("new indexservice, address = ", address)
	return &Client{
		address: address,
	}
}
