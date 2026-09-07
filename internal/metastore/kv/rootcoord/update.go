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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Update applies a composite set of UpdateActions as a single write: each
// action's Entry/Type pair is type-switched into the same kv encoding its
// dedicated Catalog method uses, accumulated into a txn.Builder, which is
// then committed via txn.Commit - atomically if the op count fits within
// the store's txn op limit, else via the caller-ordered chunked fallback.
//
// ts is unused: it is kept for interface parity with the legacy
// CreateCollection/DropCollection methods, which also ignore ts since
// kc.Txn is a plain TxnKV.
func (kc *Catalog) Update(ctx context.Context, ts typeutil.Timestamp, actions ...metastore.UpdateAction) error {
	b := txn.New()
	for _, action := range actions {
		ce, ok := action.Entry.(metastore.CollectionEntry)
		if !ok {
			return merr.WrapErrServiceInternalMsg("rootcoord catalog cannot apply entry %T", action.Entry)
		}
		coll := ce.Collection
		switch action.Type {
		case metastore.ActionAdd:
			// CreateCollection appends the child kvs and the collection
			// key/value (as the commit marker), using the same encoding as
			// the legacy Catalog.CreateCollection. It keeps the legacy
			// overwrite/idempotent-on-retry semantics (a duplicate create
			// silently overwrites rather than failing). CommitSave marks
			// the collection key as the visibility point, so children are
			// persisted before the collection key on the ordered fallback
			// path.
			if coll.State != pb.CollectionState_CollectionCreated {
				return merr.WrapErrServiceInternalMsg("collection state should be created, collection name: %s, collection id: %d, state: %s", coll.Name, coll.CollectionID, coll.State)
			}

			k1, v1, err := buildCollectionKV(coll)
			if err != nil {
				return err
			}
			kvs, err := buildCreateCollectionChildKvs(coll)
			if err != nil {
				return err
			}

			for k, v := range kvs {
				b.Save(k, v)
			}
			b.CommitSave(k1, v1)
		case metastore.ActionDelete:
			// DropCollection appends the child metadata removals and the
			// collection key removal (as the commit marker), using the same
			// keys as the legacy Catalog.DropCollection.
			collectionKey, delMetakeysSnap := buildDropCollectionKeys(coll)
			for _, k := range delMetakeysSnap {
				b.Remove(k)
			}
			b.CommitRemove(collectionKey)
		default:
			return merr.WrapErrServiceInternalMsg("rootcoord catalog cannot apply action type %v to CollectionEntry", action.Type)
		}
	}

	return txn.Commit(ctx, kc.Txn, b)
}
