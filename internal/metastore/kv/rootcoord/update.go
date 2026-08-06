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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
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
		switch entry := action.Entry.(type) {
		case metastore.CollectionEntry:
			coll := entry.Collection
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
		case metastore.CollectionRenameEntry:
			// RenameCollection rewrites the collection record with the exact
			// same encoding as the legacy Catalog.AlterCollection(MODIFY) /
			// AlterCollectionDB pair, routed on whether the database changed.
			// The record is the visibility marker of the whole rename: it
			// lands last on the chunked fallback path, so a crash mid-rename
			// leaves the collection under its old name (and the rename
			// retryable) instead of publishing a renamed collection whose
			// grants are still keyed by the old name.
			if action.Type != metastore.ActionUpdate {
				return merr.WrapErrServiceInternalMsg("rootcoord catalog cannot apply action type %v to CollectionRenameEntry", action.Type)
			}
			if entry.Old.DBID != entry.New.DBID {
				saves, removals, err := alterCollectionDBKvs(entry.Old, entry.New)
				if err != nil {
					return err
				}
				// the record move is a single save/remove pair; both sides
				// flip together in the final guarded txn.
				for k, v := range saves {
					b.CommitSave(k, v)
				}
				for _, k := range removals {
					b.CommitRemove(k)
				}
			} else {
				saves, removals, err := alterModifyCollectionKvs(entry.Old, entry.New, entry.FieldModify)
				if err != nil {
					return err
				}
				collKey := BuildCollectionKey(entry.New.DBID, entry.New.CollectionID)
				for _, k := range removals {
					b.Remove(k)
				}
				for k, v := range saves {
					if k != collKey {
						b.Save(k, v)
					}
				}
				b.CommitSave(collKey, saves[collKey])
			}
		case metastore.GrantMigrateEntry:
			// MigrateCollectionGrants rewrites every grantee (and grantee-id)
			// key referencing the old (db, collection) name pair, using the
			// exact same kv set as the legacy MigrateGrantCollectionName.
			// This is a read-current-state-then-commit rewrite with no
			// predicate checks; see the MigrateCollectionGrants constructor
			// for the serialization the caller must provide.
			if action.Type != metastore.ActionUpdate {
				return merr.WrapErrServiceInternalMsg("rootcoord catalog cannot apply action type %v to GrantMigrateEntry", action.Type)
			}
			saves, granteeRemovals, idRemovals, err := kc.migrateGrantCollectionNameKvs(ctx, entry.Tenant, entry.OldDBName, entry.OldName, entry.NewDBName, entry.NewName)
			if err != nil {
				return err
			}
			for k, v := range saves {
				b.Save(k, v)
			}
			// the old grantee-privileges keys are the only index from which a
			// retry can recompute this rewrite set (their values reference the
			// grantee-id subtrees), so on the chunked fallback path they must
			// land after the grantee-id removals - otherwise a crash between
			// removal chunks permanently orphans the remaining old keys.
			for _, k := range idRemovals {
				b.Remove(k)
			}
			for _, k := range granteeRemovals {
				b.Remove(k)
			}
		case metastore.CollectionGrantsEntry:
			// DropCollectionGrants appends the exact same keys as the legacy
			// Catalog.DeleteGrantByCollectionName: the unshared grantee-id
			// subtrees as prefix removals, then the matching grantee-privileges
			// leaves as exact removals. The leaves are the only index from
			// which a retry can recompute this removal set (their values
			// reference the grantee-id subtrees), so on the chunked fallback
			// path they must land after the grantee-id subtrees - otherwise a
			// crash in between permanently orphans the remaining subtrees. The
			// collection record - the commit marker of the whole drop - is
			// composed by the caller after this entry and lands last. This is
			// a read-current-state-then-commit removal with no predicate
			// checks; see the DropCollectionGrants constructor for the
			// serialization the caller must provide.
			if action.Type != metastore.ActionDelete {
				return merr.WrapErrServiceInternalMsg("rootcoord catalog cannot apply action type %v to CollectionGrantsEntry", action.Type)
			}
			exactRemovals, prefixRemovals, err := kc.deleteGrantByCollectionNameKvs(ctx, entry.Tenant, entry.DBName, entry.CollectionName)
			if err != nil {
				return err
			}
			for _, p := range prefixRemovals {
				b.RemovePrefix(p)
			}
			for _, k := range exactRemovals {
				b.Remove(k)
			}
		case metastore.RoleEntry:
			// DropRole appends the exact same keys as the legacy
			// Catalog.DropRole: user-role mapping removals first, then the
			// role record removal as the commit marker, so on the chunked
			// fallback path the role stays visible (and the drop retryable)
			// until everything else has landed.
			if action.Type != metastore.ActionDelete {
				return merr.WrapErrServiceInternalMsg("rootcoord catalog cannot apply action type %v to RoleEntry", action.Type)
			}
			deleteKeys, err := kc.dropRoleRemovals(ctx, entry.Tenant, entry.Name)
			if err != nil {
				return err
			}
			for _, k := range deleteKeys[1:] {
				b.Remove(k)
			}
			b.CommitRemove(deleteKeys[0])
		case metastore.RoleGrantsEntry:
			// DropRoleGrants appends the exact same prefixes as the legacy
			// Catalog.DeleteGrant: the grantee-privileges subtree plus the
			// unshared grantee-id subtrees. The grantee-privileges subtree is
			// the only index from which a retry can rediscover the grantee-id
			// subtrees, so on the chunked fallback path it must land last —
			// otherwise a crash between prefix chunks permanently orphans the
			// remaining grantee-id subtrees, and a same-name role re-grant
			// would silently resurrect them (GranteeID is deterministic).
			if action.Type != metastore.ActionDelete {
				return merr.WrapErrServiceInternalMsg("rootcoord catalog cannot apply action type %v to RoleGrantsEntry", action.Type)
			}
			prefixes, err := kc.deleteGrantPrefixes(ctx, entry.Tenant, &milvuspb.RoleEntity{Name: entry.RoleName})
			if err != nil {
				return err
			}
			for _, p := range prefixes[1:] {
				b.RemovePrefix(p)
			}
			b.RemovePrefix(prefixes[0])
		default:
			return merr.WrapErrServiceInternalMsg("rootcoord catalog cannot apply entry %T", action.Entry)
		}
	}

	return txn.Commit(ctx, kc.Txn, b)
}
