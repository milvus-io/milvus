package indexbuilderclient

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

type IndexDescription struct {
	ID                UniqueID
	Status            indexbuilderpb.IndexStatus
	EnqueueTime       time.Time
	ScheduleTime      time.Time
	BuildCompleteTime time.Time
}

func NewBuildIndexClient(ctx context.Context, address string) (*Client, error) {
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return &Client{
		client: indexbuilderpb.NewIndexBuildServiceClient(conn),
	}, nil
}

func parseTS(t int64) time.Time {
	return time.Unix(0, t)
}

func (c *Client) BuildIndexWithoutID(columnDataPaths []string, typeParams map[string]string, indexParams map[string]string) (UniqueID, error) {
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

func (c *Client) DescribeIndex(indexID UniqueID) (*IndexDescription, error) {
	ctx := context.TODO()
	request := &indexbuilderpb.DescribleIndexRequest{
		IndexID: indexID,
	}
	response, err := c.client.DescribeIndex(ctx, request)
	if err != nil {
		return &IndexDescription{}, err
	}

	indexDescription := IndexDescription{
		ID:                indexID,
		Status:            response.IndexStatus,
		EnqueueTime:       parseTS(response.EnqueTime),
		ScheduleTime:      parseTS(response.ScheduleTime),
		BuildCompleteTime: parseTS(response.BuildCompleteTime),
	}
	return &indexDescription, nil
}

func (c *Client) GetIndexFilePaths(indexID UniqueID) ([]string, error) {
	ctx := context.TODO()
	request := &indexbuilderpb.GetIndexFilePathsRequest{
		IndexID: indexID,
	}

	response, err := c.client.GetIndexFilePaths(ctx, request)
	if err != nil {
		return nil, err
	}

	return response.IndexFilePaths, nil
}
