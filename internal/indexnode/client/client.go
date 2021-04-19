package indexnodeclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type Client struct {
	client  indexbuilderpb.IndexBuildServiceClient
	address string
	ctx     context.Context
}

type IndexDescription struct {
	ID                UniqueID
	Status            indexbuilderpb.IndexStatus
	EnqueueTime       time.Time
	ScheduleTime      time.Time
	BuildCompleteTime time.Time
}

func NewBuildIndexClient(ctx context.Context, address string) (*Client, error) {
	return &Client{
		address: address,
		ctx:     ctx,
	}, nil
}

func parseTS(t int64) time.Time {
	return time.Unix(0, t)
}

func (c *Client) tryConnect() error {
	if c.client != nil {
		return nil
	}
	conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	c.client = indexbuilderpb.NewIndexBuildServiceClient(conn)
	return nil
}

func (c *Client) BuildIndexWithoutID(columnDataPaths []string, typeParams map[string]string, indexParams map[string]string) (UniqueID, error) {
	if c.tryConnect() != nil {
		panic("BuildIndexWithoutID: failed to connect index builder")
	}
	parseMap := func(mStr string) (map[string]string, error) {
		buffer := make(map[string]interface{})
		err := json.Unmarshal([]byte(mStr), &buffer)
		if err != nil {
			return nil, errors.New("Unmarshal params failed")
		}
		ret := make(map[string]string)
		for key, value := range buffer {
			valueStr := fmt.Sprintf("%v", value)
			ret[key] = valueStr
		}
		return ret, nil
	}
	var typeParamsKV []*commonpb.KeyValuePair
	for key := range typeParams {
		if key == "params" {
			mapParams, err := parseMap(typeParams[key])
			if err != nil {
				log.Println("parse params error: ", err)
			}
			for pk, pv := range mapParams {
				typeParamsKV = append(typeParamsKV, &commonpb.KeyValuePair{
					Key:   pk,
					Value: pv,
				})
			}
		} else {
			typeParamsKV = append(typeParamsKV, &commonpb.KeyValuePair{
				Key:   key,
				Value: typeParams[key],
			})
		}
	}

	var indexParamsKV []*commonpb.KeyValuePair
	for key := range indexParams {
		if key == "params" {
			mapParams, err := parseMap(indexParams[key])
			if err != nil {
				log.Println("parse params error: ", err)
			}
			for pk, pv := range mapParams {
				indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
					Key:   pk,
					Value: pv,
				})
			}
		} else {
			indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
				Key:   key,
				Value: indexParams[key],
			})
		}
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
	if c.tryConnect() != nil {
		panic("DescribeIndex: failed to connect index builder")
	}
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
	if c.tryConnect() != nil {
		panic("GetIndexFilePaths: failed to connect index builder")
	}
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
