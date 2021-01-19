package grpcindexnode

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"google.golang.org/grpc"
)

type Client struct {
	grpcClient indexpb.IndexNodeClient
}

func (c Client) Init() {
	//TODO:???
	panic("implement me")
}

func (c Client) Start() {
	//TODO:???
	panic("implement me")
}

func (c Client) Stop() {
	panic("implement me")
}

func (c Client) GetServiceStates() (internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (c Client) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (c Client) GetStatisticsChannel() (string, error) {
	panic("implement me")
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
		log.Printf("IndexNode connect to IndexService failed, error= %v", err)
	}
	log.Printf("IndexNode connected to IndexService, IndexService=%s", Params.Address)
	return &Client{
		grpcClient: indexpb.NewIndexNodeClient(conn),
	}
}
