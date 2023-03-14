// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datanode

import (
	"context"
	"fmt"
	"reflect"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
)

// metaService initialize channel collection in data node from root coord.
// Initializing channel collection happens on data node starting. It depends on
// a healthy root coord and a valid root coord grpc client.
type metaService struct {
	channel      Channel
	collectionID UniqueID
	rootCoord    types.RootCoord
}

// newMetaService creates a new metaService with provided RootCoord and collectionID.
func newMetaService(rc types.RootCoord, collectionID UniqueID) *metaService {
	return &metaService{
		rootCoord:    rc,
		collectionID: collectionID,
	}
}

// getCollectionSchema get collection schema with provided collection id at specified timestamp.
func (mService *metaService) getCollectionSchema(ctx context.Context, collID UniqueID, timestamp Timestamp) (*schemapb.CollectionSchema, error) {
	response, err := mService.getCollectionInfo(ctx, collID, timestamp)
	if response != nil {
		return response.GetSchema(), err
	}
	return nil, err
}

// getCollectionInfo get collection info with provided collection id at specified timestamp.
func (mService *metaService) getCollectionInfo(ctx context.Context, collID UniqueID, timestamp Timestamp) (*milvuspb.DescribeCollectionResponse, error) {
	req := &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
			commonpbutil.WithMsgID(0), //GOOSE TODO
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		DbName:       "default", // GOOSE TODO
		CollectionID: collID,
		TimeStamp:    timestamp,
	}

	response, err := mService.rootCoord.DescribeCollectionInternal(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpc error when describe collection %v from rootcoord: %s", collID, err.Error())
	}

	if response.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, fmt.Errorf("describe collection %v from rootcoord wrong: %s", collID, response.GetStatus().GetReason())
	}

	return response, nil
}

// printCollectionStruct util function to print schema data, used in tests only.
func printCollectionStruct(obj *etcdpb.CollectionMeta) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	typeOfS := v.Type()

	for i := 0; i < v.NumField()-3; i++ {
		if typeOfS.Field(i).Name == "GrpcMarshalString" {
			continue
		}
		log.Info("Collection field", zap.String("field", typeOfS.Field(i).Name), zap.Any("value", v.Field(i).Interface()))
	}
}
