package indexservice

import (
	"context"
	"errors"
	"strconv"

	grpcindexnodeclient "github.com/zilliztech/milvus-distributed/internal/distributed/indexnode/client"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func (i *IndexService) removeNode(nodeID UniqueID) {
	i.nodeLock.Lock()
	defer i.nodeLock.Unlock()
	i.nodeClients.Remove(nodeID)
}

func (i *IndexService) addNode(nodeID UniqueID, req *indexpb.RegisterNodeRequest) error {
	i.nodeLock.Lock()
	defer i.nodeLock.Unlock()

	if i.nodeClients.CheckAddressExist(req.Address) {
		errMsg := "Register IndexNode fatal, address conflict with nodeID:%d 's address" + strconv.FormatInt(nodeID, 10)
		return errors.New(errMsg)
	}

	nodeAddress := req.Address.Ip + ":" + strconv.FormatInt(req.Address.Port, 10)
	nodeClient, err := grpcindexnodeclient.NewClient(nodeAddress)
	if err != nil {
		return err
	}
	err = nodeClient.Init()
	if err != nil {
		return err
	}
	item := &PQItem{
		value:    nodeClient,
		key:      nodeID,
		addr:     req.Address,
		priority: 0,
	}
	i.nodeClients.Push(item)
	return nil
}

func (i *IndexService) prepareNodeInitParams() []*commonpb.KeyValuePair {
	var params []*commonpb.KeyValuePair
	params = append(params, &commonpb.KeyValuePair{Key: "minio.address", Value: Params.MinIOAddress})
	params = append(params, &commonpb.KeyValuePair{Key: "minio.accessKeyID", Value: Params.MinIOAccessKeyID})
	params = append(params, &commonpb.KeyValuePair{Key: "minio.secretAccessKey", Value: Params.MinIOSecretAccessKey})
	params = append(params, &commonpb.KeyValuePair{Key: "minio.useSSL", Value: strconv.FormatBool(Params.MinIOUseSSL)})
	params = append(params, &commonpb.KeyValuePair{Key: "minio.bucketName", Value: Params.MinioBucketName})
	return params
}

func (i *IndexService) RegisterNode(ctx context.Context, req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {
	ret := &indexpb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	nodeID, err := i.idAllocator.AllocOne()
	if err != nil {
		ret.Status.Reason = "IndexService:RegisterNode Failed to acquire NodeID"
		return ret, nil
	}

	err = i.addNode(nodeID, req)
	if err != nil {
		ret.Status.Reason = err.Error()
		return ret, nil
	}

	ret.Status.ErrorCode = commonpb.ErrorCode_Success
	params := i.prepareNodeInitParams()
	ret.InitParams = &internalpb.InitParams{
		NodeID:      nodeID,
		StartParams: params,
	}
	return ret, nil
}
