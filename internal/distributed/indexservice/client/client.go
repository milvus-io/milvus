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

//func (g *Client) BuildIndex2(columnDataPaths []string, typeParams map[string]string, indexParams map[string]string) (UniqueID, error) {
//
//	parseMap := func(mStr string) (map[string]string, error) {
//		buffer := make(map[string]interface{})
//		err := json.Unmarshal([]byte(mStr), &buffer)
//		if err != nil {
//			return nil, errors.New("Unmarshal params failed")
//		}
//		ret := make(map[string]string)
//		for key, value := range buffer {
//			valueStr := fmt.Sprintf("%v", value)
//			ret[key] = valueStr
//		}
//		return ret, nil
//	}
//	var typeParamsKV []*commonpb.KeyValuePair
//	for key := range typeParams {
//		if key == "params" {
//			mapParams, err := parseMap(typeParams[key])
//			if err != nil {
//				log.Println("parse params error: ", err)
//			}
//			for pk, pv := range mapParams {
//				typeParamsKV = append(typeParamsKV, &commonpb.KeyValuePair{
//					Key:   pk,
//					Value: pv,
//				})
//			}
//		} else {
//			typeParamsKV = append(typeParamsKV, &commonpb.KeyValuePair{
//				Key:   key,
//				Value: typeParams[key],
//			})
//		}
//	}
//
//	var indexParamsKV []*commonpb.KeyValuePair
//	for key := range indexParams {
//		if key == "params" {
//			mapParams, err := parseMap(indexParams[key])
//			if err != nil {
//				log.Println("parse params error: ", err)
//			}
//			for pk, pv := range mapParams {
//				indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
//					Key:   pk,
//					Value: pv,
//				})
//			}
//		} else {
//			indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
//				Key:   key,
//				Value: indexParams[key],
//			})
//		}
//	}
//
//	requset := &indexpb.BuildIndexRequest{
//		DataPaths:   columnDataPaths,
//		TypeParams:  typeParamsKV,
//		IndexParams: indexParamsKV,
//	}
//	response, err := g.BuildIndex(requset)
//	if err != nil {
//		return 0, err
//	}
//
//	indexID := response.IndexID
//
//	return indexID, nil
//}
//
//func (g *Client) GetIndexStates2(indexIDs []UniqueID) (*indexpb.IndexStatesResponse, error) {
//
//	request := &indexpb.IndexStatesRequest{
//		IndexIDs: indexIDs,
//	}
//
//	response, err := g.GetIndexStates(request)
//	return response, err
//}
//
//func (g *Client) GetIndexFilePaths2(indexIDs []UniqueID) ([][]string, error) {
//
//	request := &indexpb.IndexFilePathsRequest{
//		IndexIDs: indexIDs,
//	}
//
//	response, err := g.GetIndexFilePaths(request)
//	if err != nil {
//		return nil, err
//	}
//
//	var filePaths [][]string
//	for _, indexID := range indexIDs {
//		for _, filePathInfo := range response.FilePaths {
//			if indexID == filePathInfo.IndexID {
//				filePaths = append(filePaths, filePathInfo.IndexFilePaths)
//				break
//			}
//		}
//	}
//
//	return filePaths, nil
//}

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
	log.Println("indexservice address = ", g.address)
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
