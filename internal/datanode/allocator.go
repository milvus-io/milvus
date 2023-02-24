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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/metautil"
	"github.com/milvus-io/milvus/internal/util/paramtable"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
)

type allocatorInterface interface {
	allocID() (UniqueID, error)
	allocIDBatch(count uint32) (UniqueID, uint32, error)
	genKey(ids ...UniqueID) (key string, err error)
}

type allocator struct {
	rootCoord types.RootCoord
}

// check if allocator implements allocatorInterface
var _ allocatorInterface = &allocator{}

func newAllocator(s types.RootCoord) *allocator {
	return &allocator{
		rootCoord: s,
	}
}

// allocID allocates one ID from rootCoord
func (alloc *allocator) allocID() (UniqueID, error) {
	ctx := context.TODO()
	resp, err := alloc.rootCoord.AllocID(ctx, &rootcoordpb.AllocIDRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestID),
			commonpbutil.WithMsgID(1), // GOOSE TODO
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		Count: 1,
	})
	if err != nil {
		return 0, err
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return 0, errors.New(resp.GetStatus().GetReason())
	}

	return resp.ID, nil
}

// allocIDBatch allocates IDs in batch from rootCoord
func (alloc *allocator) allocIDBatch(count uint32) (UniqueID, uint32, error) {
	ctx := context.Background()
	resp, err := alloc.rootCoord.AllocID(ctx, &rootcoordpb.AllocIDRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestID),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		Count: count,
	})

	if err != nil {
		return 0, 0, err
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return 0, 0, errors.New(resp.GetStatus().GetReason())
	}

	return resp.GetID(), resp.GetCount(), nil
}

// genKey gives a valid key string for lists of UniqueIDs:
func (alloc *allocator) genKey(ids ...UniqueID) (string, error) {
	idx, err := alloc.allocID()
	if err != nil {
		return "", err
	}
	ids = append(ids, idx)
	return metautil.JoinIDPath(ids...), nil
}
