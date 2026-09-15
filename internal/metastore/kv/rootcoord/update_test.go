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
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCatalog_Update_CreateCollection(t *testing.T) {
	txnkv := mocks.NewTxnKV(t)
	txnkv.EXPECT().MaxTxnOps().Return(128).Maybe()
	txnkv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	c := NewCatalog(txnkv)
	coll := &model.Collection{CollectionID: 1, State: pb.CollectionState_CollectionCreated}
	err := c.Update(context.TODO(), 0, metastore.CreateCollection(coll))
	assert.NoError(t, err)
}

func TestCatalog_Update_DropCollection(t *testing.T) {
	txnkv := mocks.NewTxnKV(t)
	txnkv.EXPECT().MaxTxnOps().Return(128).Maybe()
	var gotSaves map[string]string
	var gotRemovals []string
	txnkv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			gotSaves = saves
			gotRemovals = removals
			return nil
		}).Once()
	c := NewCatalog(txnkv)
	coll := &model.Collection{
		CollectionID: 1,
		Partitions:   []*model.Partition{{PartitionID: 10}},
	}
	err := c.Update(context.TODO(), 0, metastore.DropCollection(coll))
	assert.NoError(t, err)
	assert.Empty(t, gotSaves)
	if assert.Len(t, gotRemovals, 2) {
		assert.Contains(t, gotRemovals, BuildCollectionKey(coll.DBID, coll.CollectionID))
		assert.Contains(t, gotRemovals, BuildPartitionKey(coll.CollectionID, 10))
	}
}

// TestCatalog_Update_RejectsForeignEntry proves the rootcoord catalog's
// Update rejects an entry it does not own (SegmentEntry belongs to the
// datacoord catalog) with a merr ServiceInternal error, and issues no KV
// call.
func TestCatalog_Update_RejectsForeignEntry(t *testing.T) {
	txnkv := mocks.NewTxnKV(t)
	txnkv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(txnkv)
	err := c.Update(context.TODO(), 0, metastore.UpdateAction{
		Type:  metastore.ActionUpdate,
		Entry: metastore.SegmentEntry{},
	})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// Catalog.CreateCollection now delegates to Update, so both must commit the
// exact same kv set: every child kv (partitions/fields/...) plus the
// collection commit key, in a single MultiSaveAndRemove with no removals.
func TestCatalog_Update_CreateCollectionEncodingMatchesLegacy(t *testing.T) {
	coll := &model.Collection{
		CollectionID: 1,
		State:        pb.CollectionState_CollectionCreated,
		Partitions:   []*model.Partition{{PartitionID: 10}},
		Fields:       []*model.Field{{FieldID: 100}},
	}

	// Expected kv set built from the shared encoding helpers.
	k1, v1, err := buildCollectionKV(coll)
	assert.NoError(t, err)
	wantSaves, err := buildCreateCollectionChildKvs(coll)
	assert.NoError(t, err)
	wantSaves[k1] = v1

	// CreateCollection path.
	var directSaves map[string]string
	txnkv := mocks.NewTxnKV(t)
	txnkv.EXPECT().MaxTxnOps().Return(128).Maybe()
	txnkv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			directSaves = saves
			assert.Empty(t, removals)
			return nil
		}).Once()
	c := NewCatalog(txnkv)
	assert.NoError(t, c.CreateCollection(context.TODO(), coll, 0))

	// Update path.
	var compositeSaves map[string]string
	txnkv2 := mocks.NewTxnKV(t)
	txnkv2.EXPECT().MaxTxnOps().Return(128).Maybe()
	txnkv2.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			compositeSaves = saves
			assert.Empty(t, removals)
			return nil
		}).Once()
	c2 := NewCatalog(txnkv2)
	assert.NoError(t, c2.Update(context.TODO(), 0, metastore.CreateCollection(coll)))

	assert.Equal(t, wantSaves, directSaves)
	assert.Equal(t, wantSaves, compositeSaves)
}

// seedDropRoleRBAC populates a catalog with the fixture used by the
// drop-role-with-grants tests: role1 (the drop target) with two user
// mappings and two grants, plus role10 as the exact-vs-prefix widening
// canary (a prefix delete of "roles/role1" or "grantee-privileges/role1"
// without the trailing slash would wipe role10's keys too).
func seedDropRoleRBAC(t *testing.T, c metastore.RootCoordCatalog) {
	ctx := context.TODO()
	tenant := util.DefaultTenant
	for _, role := range []string{"role1", "role10"} {
		assert.NoError(t, c.CreateRole(ctx, tenant, &milvuspb.RoleEntity{Name: role}))
	}
	assert.NoError(t, c.AlterUserRole(ctx, tenant, &milvuspb.UserEntity{Name: "user1"}, &milvuspb.RoleEntity{Name: "role1"}, milvuspb.OperateUserRoleType_AddUserToRole))
	assert.NoError(t, c.AlterUserRole(ctx, tenant, &milvuspb.UserEntity{Name: "user2"}, &milvuspb.RoleEntity{Name: "role1"}, milvuspb.OperateUserRoleType_AddUserToRole))
	assert.NoError(t, c.AlterUserRole(ctx, tenant, &milvuspb.UserEntity{Name: "user1"}, &milvuspb.RoleEntity{Name: "role10"}, milvuspb.OperateUserRoleType_AddUserToRole))
	grant := func(role, objType, objName, priv string) {
		assert.NoError(t, c.AlterGrant(ctx, tenant, &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: role},
			Object:     &milvuspb.ObjectEntity{Name: objType},
			ObjectName: objName,
			DbName:     "db1",
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "root"},
				Privilege: &milvuspb.PrivilegeEntity{Name: priv},
			},
		}, milvuspb.OperatePrivilegeType_Grant))
	}
	grant("role1", "Collection", "coll1", "Insert")
	grant("role1", "Global", "*", "CreateCollection")
	grant("role10", "Collection", "coll1", "Insert")
}

func dumpKV(t *testing.T, k interface {
	LoadWithPrefix(ctx context.Context, key string) ([]string, []string, error)
},
) map[string]string {
	keys, vals, err := k.LoadWithPrefix(context.TODO(), "")
	assert.NoError(t, err)
	got := make(map[string]string, len(keys))
	for i, key := range keys {
		got[key] = vals[i]
	}
	return got
}

// TestCatalog_Update_DropRoleWithGrants_MatchesLegacyBytes proves the new
// single-txn drop-role path leaves byte-for-byte the same store state as
// today's two-call path (Catalog.DropRole + Catalog.DeleteGrant): role
// record, user-role mappings, grantee-privileges subtree and grantee-id
// subtrees all gone, canary keys of another role untouched.
func TestCatalog_Update_DropRoleWithGrants_MatchesLegacyBytes(t *testing.T) {
	ctx := context.TODO()
	tenant := util.DefaultTenant
	legacyKV := memkv.NewMemoryKV()
	compositeKV := memkv.NewMemoryKV()
	legacy := NewCatalog(legacyKV)
	composite := NewCatalog(compositeKV)
	seedDropRoleRBAC(t, legacy)
	seedDropRoleRBAC(t, composite)
	assert.Equal(t, dumpKV(t, legacyKV), dumpKV(t, compositeKV))

	// capture role1's grantee ids before the drop, to assert their subtrees die.
	granteeKeys, granteeIDs, err := compositeKV.LoadWithPrefix(ctx, funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, "role1"))
	assert.NoError(t, err)
	assert.Len(t, granteeKeys, 2)

	// today's two-call path.
	assert.NoError(t, legacy.DropRole(ctx, tenant, "role1"))
	assert.NoError(t, legacy.DeleteGrant(ctx, tenant, &milvuspb.RoleEntity{Name: "role1"}))

	// new single-txn path.
	assert.NoError(t, composite.Update(ctx, 0,
		metastore.DropRoleGrants(tenant, "role1"),
		metastore.DropRole(tenant, "role1")))

	got := dumpKV(t, compositeKV)
	assert.Equal(t, dumpKV(t, legacyKV), got)

	// all four key classes of role1 are gone.
	assert.NotContains(t, got, RolePrefix+"/role1")
	assert.NotContains(t, got, RoleMappingPrefix+"/user1/role1")
	assert.NotContains(t, got, RoleMappingPrefix+"/user2/role1")
	for k := range got {
		assert.False(t, strings.HasPrefix(k, funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, "role1")), k)
		for _, id := range granteeIDs {
			assert.False(t, strings.HasPrefix(k, funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, id)), k)
		}
	}
	// canaries of role10 survive.
	assert.Contains(t, got, RolePrefix+"/role10")
	assert.Contains(t, got, RoleMappingPrefix+"/user1/role10")
	has, err := compositeKV.HasPrefix(ctx, funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, "role10"))
	assert.NoError(t, err)
	assert.True(t, has)
}

// writeRecordingKV records every write-path TxnKV call so a test can assert
// the composite drop-role issues exactly one transaction.
type writeRecordingKV struct {
	*memkv.MemoryKV
	calls          []string
	removals       []string
	prefixRemovals []string
}

func (w *writeRecordingKV) Save(ctx context.Context, key, value string) error {
	w.calls = append(w.calls, "Save")
	return w.MemoryKV.Save(ctx, key, value)
}

func (w *writeRecordingKV) Remove(ctx context.Context, key string) error {
	w.calls = append(w.calls, "Remove")
	return w.MemoryKV.Remove(ctx, key)
}

func (w *writeRecordingKV) RemoveWithPrefix(ctx context.Context, key string) error {
	w.calls = append(w.calls, "RemoveWithPrefix")
	return w.MemoryKV.RemoveWithPrefix(ctx, key)
}

func (w *writeRecordingKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	w.calls = append(w.calls, "MultiSave")
	return w.MemoryKV.MultiSave(ctx, kvs)
}

func (w *writeRecordingKV) MultiRemove(ctx context.Context, keys []string) error {
	w.calls = append(w.calls, "MultiRemove")
	return w.MemoryKV.MultiRemove(ctx, keys)
}

func (w *writeRecordingKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	w.calls = append(w.calls, "MultiSaveAndRemove")
	return w.MemoryKV.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

func (w *writeRecordingKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	w.calls = append(w.calls, "MultiSaveAndRemoveWithPrefix")
	return w.MemoryKV.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, preds...)
}

func (w *writeRecordingKV) MultiSaveAndRemoveMixed(ctx context.Context, saves map[string]string, removals []string, prefixRemovals []string, preds ...predicates.Predicate) error {
	w.calls = append(w.calls, "MultiSaveAndRemoveMixed")
	w.removals = removals
	w.prefixRemovals = prefixRemovals
	return w.MemoryKV.MultiSaveAndRemoveMixed(ctx, saves, removals, prefixRemovals, preds...)
}

// TestCatalog_Update_DropRoleWithGrants_SingleTxn proves the whole removal is
// one MultiSaveAndRemoveMixed transaction carrying the role record and the
// mappings as EXACT removals and the grant subtrees as PREFIX removals.
func TestCatalog_Update_DropRoleWithGrants_SingleTxn(t *testing.T) {
	ctx := context.TODO()
	tenant := util.DefaultTenant
	mk := memkv.NewMemoryKV()
	seedDropRoleRBAC(t, NewCatalog(mk))

	_, granteeIDs, err := mk.LoadWithPrefix(ctx, funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, "role1"))
	assert.NoError(t, err)
	assert.Len(t, granteeIDs, 2)

	rec := &writeRecordingKV{MemoryKV: mk}
	c := NewCatalog(rec)
	assert.NoError(t, c.Update(ctx, 0,
		metastore.DropRoleGrants(tenant, "role1"),
		metastore.DropRole(tenant, "role1")))

	assert.Equal(t, []string{"MultiSaveAndRemoveMixed"}, rec.calls)
	assert.ElementsMatch(t, []string{
		RolePrefix + "/role1",
		RoleMappingPrefix + "/user1/role1",
		RoleMappingPrefix + "/user2/role1",
	}, rec.removals)
	wantPrefixes := []string{funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, "role1")}
	for _, id := range granteeIDs {
		wantPrefixes = append(wantPrefixes, funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, id))
	}
	assert.ElementsMatch(t, wantPrefixes, rec.prefixRemovals)
}

// flakyMixedKV fails the first MultiSaveAndRemoveMixed to simulate a crash of
// the atomic commit.
type flakyMixedKV struct {
	*memkv.MemoryKV
	failures int
}

func (f *flakyMixedKV) MultiSaveAndRemoveMixed(ctx context.Context, saves map[string]string, removals []string, prefixRemovals []string, preds ...predicates.Predicate) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("injected crash")
	}
	return f.MemoryKV.MultiSaveAndRemoveMixed(ctx, saves, removals, prefixRemovals, preds...)
}

// TestCatalog_Update_DropRoleWithGrants_AtomicCrashRetry: a failed atomic
// commit must leave the store untouched (no partial removal, no orphaned
// grants), and retrying the same composite drop must converge to exactly the
// legacy two-call end state.
func TestCatalog_Update_DropRoleWithGrants_AtomicCrashRetry(t *testing.T) {
	ctx := context.TODO()
	tenant := util.DefaultTenant

	legacyKV := memkv.NewMemoryKV()
	legacy := NewCatalog(legacyKV)
	seedDropRoleRBAC(t, legacy)
	assert.NoError(t, legacy.DropRole(ctx, tenant, "role1"))
	assert.NoError(t, legacy.DeleteGrant(ctx, tenant, &milvuspb.RoleEntity{Name: "role1"}))

	fk := &flakyMixedKV{MemoryKV: memkv.NewMemoryKV(), failures: 1}
	seedDropRoleRBAC(t, NewCatalog(fk.MemoryKV))
	before := dumpKV(t, fk.MemoryKV)

	c := NewCatalog(fk)
	drop := func() error {
		return c.Update(ctx, 0,
			metastore.DropRoleGrants(tenant, "role1"),
			metastore.DropRole(tenant, "role1"))
	}
	assert.Error(t, drop())
	// atomic: the failed commit applied nothing.
	assert.Equal(t, before, dumpKV(t, fk.MemoryKV))

	assert.NoError(t, drop())
	assert.Equal(t, dumpKV(t, legacyKV), dumpKV(t, fk.MemoryKV))
}

// smallTxnFlakyPrefixKV shrinks MaxTxnOps so the composite takes the chunked
// fallback and fails the first prefix-removal batch, simulating a crash
// mid-fallback before the commit marker.
type smallTxnFlakyPrefixKV struct {
	*memkv.MemoryKV
	failures int
}

func (f *smallTxnFlakyPrefixKV) MaxTxnOps() int { return 2 }

func (f *smallTxnFlakyPrefixKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("injected crash")
	}
	return f.MemoryKV.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, preds...)
}

// secondChunkCrashKV shrinks MaxTxnOps so the prefix removals split into two
// chunks and fails only the second one, simulating a crash between chunks.
type secondChunkCrashKV struct {
	*memkv.MemoryKV
	prefixCalls int
	failAtCall  int
}

func (f *secondChunkCrashKV) MaxTxnOps() int { return 2 }

func (f *secondChunkCrashKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	f.prefixCalls++
	if f.prefixCalls == f.failAtCall {
		return errors.New("injected crash")
	}
	return f.MemoryKV.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, preds...)
}

// TestCatalog_Update_DropRoleWithGrants_SecondPrefixChunkCrashRetry: on the
// chunked fallback path the grantee-privileges subtree is the only index from
// which a retry can rediscover the grantee-id subtrees. A crash between prefix
// chunks must therefore leave that subtree intact so the retry converges to
// the legacy end state instead of permanently orphaning grantee-id subtrees
// (which a same-name role re-grant would silently resurrect).
func TestCatalog_Update_DropRoleWithGrants_SecondPrefixChunkCrashRetry(t *testing.T) {
	ctx := context.TODO()
	tenant := util.DefaultTenant

	legacyKV := memkv.NewMemoryKV()
	legacy := NewCatalog(legacyKV)
	seedDropRoleRBAC(t, legacy)
	assert.NoError(t, legacy.DropRole(ctx, tenant, "role1"))
	assert.NoError(t, legacy.DeleteGrant(ctx, tenant, &milvuspb.RoleEntity{Name: "role1"}))

	fk := &secondChunkCrashKV{MemoryKV: memkv.NewMemoryKV(), failAtCall: 2}
	seedDropRoleRBAC(t, NewCatalog(fk.MemoryKV))

	c := NewCatalog(fk)
	drop := func() error {
		return c.Update(ctx, 0,
			metastore.DropRoleGrants(tenant, "role1"),
			metastore.DropRole(tenant, "role1"))
	}
	assert.Error(t, drop())
	// the rediscovery index must have survived the crash.
	keys, _, err := fk.MemoryKV.LoadWithPrefix(ctx, funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, "role1"))
	assert.NoError(t, err)
	assert.NotEmpty(t, keys)

	assert.NoError(t, drop())
	assert.Equal(t, dumpKV(t, legacyKV), dumpKV(t, fk.MemoryKV))
}

// TestCatalog_Update_DropRoleWithGrants_FallbackCrashRetry: on the chunked
// fallback path a crash before the commit marker must leave the role record
// visible (so the drop is observably incomplete and retryable, never a
// role-gone-grants-orphaned state), and the retry must converge to the legacy
// end state.
func TestCatalog_Update_DropRoleWithGrants_FallbackCrashRetry(t *testing.T) {
	ctx := context.TODO()
	tenant := util.DefaultTenant

	legacyKV := memkv.NewMemoryKV()
	legacy := NewCatalog(legacyKV)
	seedDropRoleRBAC(t, legacy)
	assert.NoError(t, legacy.DropRole(ctx, tenant, "role1"))
	assert.NoError(t, legacy.DeleteGrant(ctx, tenant, &milvuspb.RoleEntity{Name: "role1"}))

	fk := &smallTxnFlakyPrefixKV{MemoryKV: memkv.NewMemoryKV(), failures: 1}
	seedDropRoleRBAC(t, NewCatalog(fk.MemoryKV))

	c := NewCatalog(fk)
	drop := func() error {
		return c.Update(ctx, 0,
			metastore.DropRoleGrants(tenant, "role1"),
			metastore.DropRole(tenant, "role1"))
	}
	assert.Error(t, drop())
	// crash-safety: the role record (commit marker) must still be visible.
	has, err := fk.Has(ctx, RolePrefix+"/role1")
	assert.NoError(t, err)
	assert.True(t, has)

	assert.NoError(t, drop())
	assert.Equal(t, dumpKV(t, legacyKV), dumpKV(t, fk.MemoryKV))
}
