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

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

// metaService initialize replica collections in data node from root coord.
// Initializing replica collections happens on data node starting. It depends on
// a healthy root coord and a valid root coord grpc client.
type metaService struct {
	replica      Replica
	collectionID UniqueID
	rootCoord    types.RootCoord
}

// newMetaService creates a new metaService with provided RootCoord and collectionID
func newMetaService(rc types.RootCoord, collectionID UniqueID) *metaService {
	return &metaService{
		rootCoord:    rc,
		collectionID: collectionID,
	}
}

// getCollectionSchema get collection schema with provided collection id at specified timestamp
func (mService *metaService) getCollectionSchema(ctx context.Context, collID UniqueID, timestamp Timestamp) (*schemapb.CollectionSchema, error) {
	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeCollection,
			MsgID:     0, //GOOSE TODO
			Timestamp: 0, // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		DbName:       "default", // GOOSE TODO
		CollectionID: collID,
		TimeStamp:    timestamp,
	}

	response, err := mService.rootCoord.DescribeCollection(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("Grpc error when describe collection %v from rootcoord: %s", collID, err.Error())
	}

	if response.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, fmt.Errorf("Describe collection %v from rootcoord wrong: %s", collID, response.GetStatus().GetReason())
	}

	return response.GetSchema(), nil
}

// printCollectionStruct util function to print schema data
// used in tests only
func printCollectionStruct(obj *etcdpb.CollectionMeta) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	typeOfS := v.Type()

	for i := 0; i < v.NumField()-3; i++ {
		if typeOfS.Field(i).Name == "GrpcMarshalString" {
			continue
		}
		fmt.Printf("Field: %s\tValue: %v\n", typeOfS.Field(i).Name, v.Field(i).Interface())
	}
}
