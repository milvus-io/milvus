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
	"errors"
	"path"
	"strconv"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

type allocatorInterface interface {
	allocID() (UniqueID, error)
	allocIDBatch(count uint32) (UniqueID, uint32, error)
	genKey(alloc bool, ids ...UniqueID) (key string, err error)
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
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_RequestID,
			MsgID:     1, // GOOSE TODO
			Timestamp: 0, // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		Count: 1,
	})

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return 0, errors.New(resp.Status.GetReason())
	}

	if err != nil {
		return 0, err
	}

	return resp.ID, nil
}

// allocIDBatch allocates IDs in batch from rootCoord
func (alloc *allocator) allocIDBatch(count uint32) (UniqueID, uint32, error) {
	ctx := context.Background()
	resp, err := alloc.rootCoord.AllocID(ctx, &rootcoordpb.AllocIDRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_RequestID,
			SourceID: Params.NodeID,
		},
		Count: count,
	})

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return 0, 0, errors.New(resp.Status.GetReason())
	}

	if err != nil {
		return 0, 0, err
	}
	return resp.GetID(), resp.GetCount(), nil
}

// genKey gives a valid key string for lists of UniqueIDs:
//  if alloc is true, the returned keys will have a generated-unique ID at the end.
//  if alloc is false, the returned keys will only consist of provided ids.
func (alloc *allocator) genKey(isalloc bool, ids ...UniqueID) (string, error) {
	if isalloc {
		idx, err := alloc.allocID()
		if err != nil {
			return "", err
		}
		ids = append(ids, idx)
	}
	return JoinIDPath(ids...), nil
}

// JoinIDPath joins ids to path format.
func JoinIDPath(ids ...UniqueID) string {
	idStr := make([]string, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	return path.Join(idStr...)
}
