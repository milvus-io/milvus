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

package querycoord

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Update applies a composite set of UpdateActions as a single write. Each
// action is dispatched on its (Entry, Type) into a kv encoding, accumulated
// into a txn.Builder, and committed via txn.Commit - atomically when the op
// count fits the store's txn op limit, else via the caller-ordered chunked
// fallback.
//
// A ReplicaEntry+ActionUpdate is a replica upsert, encoded exactly like
// SaveReplica. A ReplicaKeyEntry+ActionDelete removes a replica's kv record,
// encoded exactly like ReleaseReplica. Entries this catalog does not own, or
// Type/Entry combinations it does not implement, are a caller programming
// error and are rejected with a ServiceInternal error and no write.
func (s Catalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	b := txn.New()
	for _, action := range actions {
		switch e := action.Entry.(type) {
		case metastore.ReplicaEntry:
			if action.Type != metastore.ActionUpdate {
				return unsupportedAction(action)
			}
			if e.Replica == nil {
				return merr.WrapErrServiceInternalMsg("querycoord catalog: nil replica in UpdateAction")
			}
			key := encodeReplicaKey(e.Replica.GetCollectionID(), e.Replica.GetID())
			value, err := proto.Marshal(e.Replica)
			if err != nil {
				return err
			}
			b.Save(key, string(value))
		case metastore.ReplicaKeyEntry:
			if action.Type != metastore.ActionDelete {
				return unsupportedAction(action)
			}
			b.Remove(encodeReplicaKey(e.CollectionID, e.ReplicaID))
		default:
			return merr.WrapErrServiceInternalMsg("querycoord catalog cannot apply entry %T", action.Entry)
		}
	}
	return txn.Commit(ctx, s.cli, b)
}

func unsupportedAction(action metastore.UpdateAction) error {
	return merr.WrapErrServiceInternalMsg("querycoord catalog cannot apply action type %v to entry %T", action.Type, action.Entry)
}
