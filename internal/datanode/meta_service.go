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

package datanode

import (
	"context"
	"fmt"
	"reflect"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/types"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

type metaService struct {
	ctx          context.Context
	replica      Replica
	masterClient types.MasterService
}

func newMetaService(ctx context.Context, replica Replica, m types.MasterService) *metaService {
	return &metaService{
		ctx:          ctx,
		replica:      replica,
		masterClient: m,
	}
}

func (mService *metaService) init() {
	log.Debug("Initing meta ...")
	ctx := context.Background()
	err := mService.loadCollections(ctx)
	if err != nil {
		log.Error("metaService init failed", zap.Error(err))
	}
}

func (mService *metaService) loadCollections(ctx context.Context) error {
	names, err := mService.getCollectionNames(ctx)
	if err != nil {
		return err
	}

	for _, name := range names {
		err := mService.createCollection(ctx, name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mService *metaService) getCollectionNames(ctx context.Context) ([]string, error) {
	req := &milvuspb.ShowCollectionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowCollections,
			MsgID:     0, //GOOSE TODO
			Timestamp: 0, // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		DbName: "default", // GOOSE TODO
	}

	response, err := mService.masterClient.ShowCollections(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("Get collection names from master service wrong: %v", err)
	}
	return response.GetCollectionNames(), nil
}

func (mService *metaService) createCollection(ctx context.Context, name string) error {
	log.Debug("Describing collections")
	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeCollection,
			MsgID:     0, //GOOSE TODO
			Timestamp: 0, // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		DbName:         "default", // GOOSE TODO
		CollectionName: name,
	}

	response, err := mService.masterClient.DescribeCollection(ctx, req)
	if err != nil {
		return fmt.Errorf("Describe collection %v from master service wrong: %v", name, err)
	}

	err = mService.replica.addCollection(response.GetCollectionID(), response.GetSchema())
	if err != nil {
		return fmt.Errorf("Add collection %v into collReplica wrong: %v", name, err)
	}

	return nil
}

func printCollectionStruct(obj *etcdpb.CollectionMeta) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		if typeOfS.Field(i).Name == "GrpcMarshalString" {
			continue
		}
		fmt.Printf("Field: %s\tValue: %v\n", typeOfS.Field(i).Name, v.Field(i).Interface())
	}
}
