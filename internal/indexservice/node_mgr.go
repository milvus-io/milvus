// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexservice

import (
	"context"
	"errors"
	"strconv"

	"go.uber.org/zap"

	grpcindexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
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
	nodeClient, err := grpcindexnodeclient.NewClient(nodeAddress, 10)
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
	log.Debug("indexservice", zap.Any("register index node, node address = ", req.Address))
	ret := &indexpb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	err := i.addNode(req.NodeID, req)
	if err != nil {
		ret.Status.Reason = err.Error()
		return ret, nil
	}

	ret.Status.ErrorCode = commonpb.ErrorCode_Success
	params := i.prepareNodeInitParams()
	ret.InitParams = &internalpb.InitParams{
		NodeID:      req.NodeID,
		StartParams: params,
	}
	return ret, nil
}
