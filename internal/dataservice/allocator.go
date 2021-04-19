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
package dataservice

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/types"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
)

type allocatorInterface interface {
	allocTimestamp() (Timestamp, error)
	allocID() (UniqueID, error)
}

type allocator struct {
	masterClient types.MasterService
}

func newAllocator(masterClient types.MasterService) *allocator {
	return &allocator{
		masterClient: masterClient,
	}
}

func (allocator *allocator) allocTimestamp() (Timestamp, error) {
	ctx := context.TODO()
	resp, err := allocator.masterClient.AllocTimestamp(ctx, &masterpb.AllocTimestampRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowCollections,
			MsgID:     -1, // todo add msg id
			Timestamp: 0,  // todo
			SourceID:  Params.NodeID,
		},
		Count: 1,
	})
	if err = VerifyResponse(resp, err); err != nil {
		return 0, err
	}
	return resp.Timestamp, nil
}

func (allocator *allocator) allocID() (UniqueID, error) {
	ctx := context.TODO()
	resp, err := allocator.masterClient.AllocID(ctx, &masterpb.AllocIDRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowCollections,
			MsgID:     -1, // todo add msg id
			Timestamp: 0,  // todo
			SourceID:  Params.NodeID,
		},
		Count: 1,
	})
	if err = VerifyResponse(resp, err); err != nil {
		return 0, err
	}

	return resp.ID, nil
}
