package indexbuilder

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type Client struct {
	client indexbuilderpb.IndexBuildServiceClient
}

type IndexStatus int32

const (
	NOTEXIST   IndexStatus = 0
	UNISSUED   IndexStatus = 1
	INPROGRESS IndexStatus = 2
	FINISHED   IndexStatus = 3
)

type IndexDescription struct {
	ID                UniqueID
	Status            IndexStatus
	EnqueueTime       time.Time
	ScheduleTime      time.Time
	BuildCompleteTime time.Time
}

func NewBuildIndexClient(conn *grpc.ClientConn) *Client {
	return &Client{
		client: indexbuilderpb.NewIndexBuildServiceClient(conn),
	}
}

func (c *Client) BuildIndexWithoutID(columnDataPaths []string, typeParams map[string]string, indexParams map[string]string) (UniqueID, error) {
	//first new a build service client

	var typeParamsKV []*commonpb.KeyValuePair
	for typeParam := range typeParams {
		typeParamsKV = append(typeParamsKV, &commonpb.KeyValuePair{
			Key:   typeParam,
			Value: typeParams[typeParam],
		})
	}

	var indexParamsKV []*commonpb.KeyValuePair
	for indexParam := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   indexParam,
			Value: indexParams[indexParam],
		})
	}

	ctx := context.TODO()
	requset := &indexbuilderpb.BuildIndexRequest{
		DataPaths:   columnDataPaths,
		TypeParams:  typeParamsKV,
		IndexParams: indexParamsKV,
	}
	response, err := c.client.BuildIndex(ctx, requset)
	if err != nil {
		return 0, err
	}

	indexID := response.IndexID
	return indexID, err
}

func (c *Client) DescribeIndex(indexID UniqueID) (IndexDescription, error) {
	//ctx := context.TODO()
	//request := &indexbuilderpb.DescribleIndexRequest{
	//	IndexID: indexID,
	//}
	//response, err := c.client.DescribeIndex(ctx, request)
	//if err != nil {
	//	return IndexDescription{}, err
	//}
	//
	//indexDescription := &IndexDescription{
	//	ID:          indexID,
	//	Status:      response.IndexStatus,
	//	EnqueueTime: time.Unix(),
	//}
	return IndexDescription{}, nil
}

func (c *Client) GetIndexFilePaths(IndexID UniqueID) ([]string, error) {

	return nil, nil
}
