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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
)

// allocator is the interface that allocating `UniqueID` or `Timestamp`
type allocator interface {
	allocTimestamp(context.Context) (Timestamp, error)
	allocID(context.Context) (UniqueID, error)
}

// make sure rootCoordAllocator implements allocator interface
var _ allocator = (*rootCoordAllocator)(nil)

// rootCoordAllocator use RootCoord as allocator
type rootCoordAllocator struct {
	types.RootCoord
}

// newRootCoordAllocator gets an allocator from RootCoord
func newRootCoordAllocator(rootCoordClient types.RootCoord) allocator {
	return &rootCoordAllocator{
		RootCoord: rootCoordClient,
	}
}

// allocTimestamp allocates a Timestamp
// invoking RootCoord `AllocTimestamp`
func (alloc *rootCoordAllocator) allocTimestamp(ctx context.Context) (Timestamp, error) {
	resp, err := alloc.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestTSO,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.DataCoordCfg.NodeID,
		},
		Count: 1,
	})
	if err = VerifyResponse(resp, err); err != nil {
		return 0, err
	}
	return resp.Timestamp, nil
}

// allocID allocates an `UniqueID` from RootCoord, invoking AllocID grpc
func (alloc *rootCoordAllocator) allocID(ctx context.Context) (UniqueID, error) {
	resp, err := alloc.AllocID(ctx, &rootcoordpb.AllocIDRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestID,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.DataCoordCfg.NodeID,
		},
		Count: 1,
	})
	if err = VerifyResponse(resp, err); err != nil {
		return 0, err
	}

	return resp.ID, nil
}
