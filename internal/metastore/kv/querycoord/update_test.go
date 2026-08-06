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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCatalog_Update_Empty(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv)
	err := c.Update(context.TODO())
	assert.NoError(t, err)
}

// TestCatalog_Update_SaveReplicaEncodingMatchesLegacy proves SaveReplica
// writes the same kv as the legacy Catalog.SaveReplica.
func TestCatalog_Update_SaveReplicaEncodingMatchesLegacy(t *testing.T) {
	replica := &querypb.Replica{ID: 100, CollectionID: 1, ResourceGroup: "rg1"}

	var legacySaves map[string]string
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, kvs map[string]string) error {
		legacySaves = kvs
		return nil
	}).Once()
	c := NewCatalog(metakv)
	assert.NoError(t, c.SaveReplica(context.TODO(), replica))

	var compositeSaves map[string]string
	metakv2 := mocks.NewMetaKv(t)
	metakv2.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv2.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			compositeSaves = saves
			assert.Empty(t, removals)
			return nil
		}).Once()
	c2 := NewCatalog(metakv2)
	assert.NoError(t, c2.Update(context.TODO(), metastore.SaveReplica(replica)))

	assert.Equal(t, legacySaves, compositeSaves)
	assert.Len(t, compositeSaves, 1)
	key := encodeReplicaKey(replica.GetCollectionID(), replica.GetID())
	persisted := &querypb.Replica{}
	assert.NoError(t, proto.Unmarshal([]byte(compositeSaves[key]), persisted))
	assert.Equal(t, replica.GetID(), persisted.GetID())
}

// TestCatalog_Update_ReleaseReplicaKeyMatchesLegacy proves ReleaseReplica
// removes the same key as the legacy Catalog.ReleaseReplica.
func TestCatalog_Update_ReleaseReplicaKeyMatchesLegacy(t *testing.T) {
	collectionID, replicaID := int64(1), int64(100)

	metakv := mocks.NewMetaKv(t)

	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	var removals []string
	metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, dels []string, _ ...predicates.Predicate) error {
			assert.Empty(t, saves)
			removals = dels
			return nil
		}).Once()
	c := NewCatalog(metakv)

	err := c.Update(context.TODO(), metastore.ReleaseReplica(collectionID, replicaID))
	assert.NoError(t, err)

	assert.Equal(t, []string{encodeReplicaKey(collectionID, replicaID)}, removals)
}

// TestCatalog_Update_MixedSaveAndRelease proves a composite write that mixes
// a replica save and a replica release lands as a single atomic
// MultiSaveAndRemove call.
func TestCatalog_Update_MixedSaveAndRelease(t *testing.T) {
	collectionID := int64(1)
	newReplica := &querypb.Replica{ID: 100, CollectionID: collectionID, ResourceGroup: "rg1"}
	redundantID := int64(7)

	metakv := mocks.NewMetaKv(t)

	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	var saves map[string]string
	var removals []string
	metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, s map[string]string, dels []string, _ ...predicates.Predicate) error {
			saves = s
			removals = dels
			return nil
		}).Once()
	c := NewCatalog(metakv)

	err := c.Update(context.TODO(),
		metastore.SaveReplica(newReplica),
		metastore.ReleaseReplica(collectionID, redundantID))
	assert.NoError(t, err)

	assert.Contains(t, saves, encodeReplicaKey(collectionID, newReplica.GetID()))
	assert.Equal(t, []string{encodeReplicaKey(collectionID, redundantID)}, removals)
}

// TestCatalog_Update_RejectsUnsupportedType proves a ReplicaEntry/
// ReplicaKeyEntry paired with an action type it does not implement is
// rejected with a merr ServiceInternal error, and issues no KV call.
func TestCatalog_Update_RejectsUnsupportedType(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv)

	err := c.Update(context.TODO(), metastore.UpdateAction{
		Type:  metastore.ActionDelete,
		Entry: metastore.ReplicaEntry{Replica: &querypb.Replica{ID: 1, CollectionID: 1}},
	})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	err = c.Update(context.TODO(), metastore.UpdateAction{
		Type:  metastore.ActionUpdate,
		Entry: metastore.ReplicaKeyEntry{CollectionID: 1, ReplicaID: 1},
	})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// TestCatalog_Update_RejectsForeignEntry proves the querycoord catalog's
// Update rejects an entry it does not own (SegmentEntry belongs to the
// datacoord catalog) with a merr ServiceInternal error.
func TestCatalog_Update_RejectsForeignEntry(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv)

	err := c.Update(context.TODO(), metastore.UpdateAction{
		Type:  metastore.ActionAdd,
		Entry: metastore.CollectionEntry{},
	})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// TestCatalog_Update_RejectsNilReplica proves a ReplicaEntry with a nil
// Replica is rejected with no KV call.
func TestCatalog_Update_RejectsNilReplica(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv)

	err := c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: metastore.ReplicaEntry{}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}

// metaMemKV adapts the in-memory TxnKV to the kv.MetaKv interface NewCatalog
// requires, for tests that need a real (stateful) store.
type metaMemKV struct {
	*memkv.MemoryKV
}

func (m *metaMemKV) GetPath(key string) string { return key }

func (m *metaMemKV) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	return false, errors.New("not implemented")
}

func (m *metaMemKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	keys, vals, err := m.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	for i := range keys {
		if err := fn([]byte(keys[i]), []byte(vals[i])); err != nil {
			return err
		}
	}
	return nil
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

// legacySaveCollection replays today's two-step SaveCollection persistence
// (collection record via Save, partitions via SavePartition) so tests can
// compare the composite path's end state byte-for-byte against it.
func legacySaveCollection(t *testing.T, store kv.MetaKv, coll *querypb.CollectionLoadInfo, parts ...*querypb.PartitionLoadInfo) {
	v, err := proto.Marshal(coll)
	assert.NoError(t, err)
	assert.NoError(t, store.Save(context.TODO(), EncodeCollectionLoadInfoKey(coll.GetCollectionID()), string(v)))
	assert.NoError(t, NewCatalog(store).SavePartition(context.TODO(), parts...))
}

// loadInfoFixture returns a collection load info and two partition load infos.
// The FieldIndexID map carries a single entry so proto.Marshal stays
// deterministic and byte-for-byte comparisons cannot flake on map order.
func loadInfoFixture() (*querypb.CollectionLoadInfo, []*querypb.PartitionLoadInfo) {
	coll := &querypb.CollectionLoadInfo{
		CollectionID:  1,
		ReplicaNumber: 2,
		Status:        querypb.LoadStatus_Loaded,
		LoadType:      querypb.LoadType_LoadCollection,
		FieldIndexID:  map[int64]int64{100: 1000},
	}
	parts := []*querypb.PartitionLoadInfo{
		{CollectionID: 1, PartitionID: 11, ReplicaNumber: 2, Status: querypb.LoadStatus_Loaded},
		{CollectionID: 1, PartitionID: 12, ReplicaNumber: 2, Status: querypb.LoadStatus_Loaded},
	}
	return coll, parts
}

// saveRecordingKV records every write-path call so a test can assert the
// composite save issues exactly one transaction.
type saveRecordingKV struct {
	kv.MetaKv
	calls []string
}

func (r *saveRecordingKV) Save(ctx context.Context, key, value string) error {
	r.calls = append(r.calls, "Save")
	return r.MetaKv.Save(ctx, key, value)
}

func (r *saveRecordingKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	r.calls = append(r.calls, "MultiSave")
	return r.MetaKv.MultiSave(ctx, kvs)
}

func (r *saveRecordingKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	r.calls = append(r.calls, "MultiSaveAndRemove")
	return r.MetaKv.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

func (r *saveRecordingKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	r.calls = append(r.calls, "MultiSaveAndRemoveWithPrefix")
	return r.MetaKv.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, preds...)
}

func (r *saveRecordingKV) MultiSaveAndRemoveMixed(ctx context.Context, saves map[string]string, removals []string, prefixRemovals []string, preds ...predicates.Predicate) error {
	r.calls = append(r.calls, "MultiSaveAndRemoveMixed")
	return r.MetaKv.MultiSaveAndRemoveMixed(ctx, saves, removals, prefixRemovals, preds...)
}

// TestCatalog_SaveCollection_SingleTxnMatchesLegacyBytes proves SaveCollection
// lands collection and partition load infos in ONE transaction, and that the
// end state is byte-for-byte the same as today's two-step path (collection
// record via Save, partitions via MultiSave).
func TestCatalog_SaveCollection_SingleTxnMatchesLegacyBytes(t *testing.T) {
	ctx := context.TODO()
	coll, parts := loadInfoFixture()

	legacyKV := &metaMemKV{MemoryKV: memkv.NewMemoryKV()}
	legacySaveCollection(t, legacyKV, coll, parts...)

	rec := &saveRecordingKV{MetaKv: &metaMemKV{MemoryKV: memkv.NewMemoryKV()}}
	c := NewCatalog(rec)
	assert.NoError(t, c.SaveCollection(ctx, coll, parts...))

	assert.Equal(t, []string{"MultiSaveAndRemove"}, rec.calls)
	assert.Equal(t, dumpKV(t, legacyKV), dumpKV(t, rec.MetaKv.(*metaMemKV)))
}

// TestCatalog_Update_SaveCollectionLoadMatchesLegacyBytes proves the
// action-level composite (partition saves composed before the collection
// save) writes byte-for-byte the same store state as the legacy two-step
// path.
func TestCatalog_Update_SaveCollectionLoadMatchesLegacyBytes(t *testing.T) {
	ctx := context.TODO()
	coll, parts := loadInfoFixture()

	legacyKV := &metaMemKV{MemoryKV: memkv.NewMemoryKV()}
	legacySaveCollection(t, legacyKV, coll, parts...)

	compositeKV := &metaMemKV{MemoryKV: memkv.NewMemoryKV()}
	c := NewCatalog(compositeKV)
	assert.NoError(t, c.Update(ctx,
		metastore.SavePartitionLoadInfo(parts[0]),
		metastore.SavePartitionLoadInfo(parts[1]),
		metastore.SaveCollectionLoadInfo(coll)))

	assert.Equal(t, dumpKV(t, legacyKV), dumpKV(t, compositeKV))
}

// flakyWriteKV fails partition-carrying writes (MultiSave on the legacy path,
// MultiSaveAndRemove on the atomic composite path) while failures remain,
// simulating a crash mid-save.
type flakyWriteKV struct {
	kv.MetaKv
	failures int
}

func (f *flakyWriteKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("injected crash")
	}
	return f.MetaKv.MultiSave(ctx, kvs)
}

func (f *flakyWriteKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("injected crash")
	}
	return f.MetaKv.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

// TestCatalog_SaveCollection_AtomicCrashRetry: a crash while persisting the
// partition load infos must never leave a loaded collection visible with its
// partitions missing - collection and partition keys either all exist or none
// do. A retry of the same save must converge to the legacy end state.
func TestCatalog_SaveCollection_AtomicCrashRetry(t *testing.T) {
	ctx := context.TODO()
	coll, parts := loadInfoFixture()

	legacyKV := &metaMemKV{MemoryKV: memkv.NewMemoryKV()}
	legacySaveCollection(t, legacyKV, coll, parts...)

	fk := &flakyWriteKV{MetaKv: &metaMemKV{MemoryKV: memkv.NewMemoryKV()}, failures: 1}
	c := NewCatalog(fk)
	assert.Error(t, c.SaveCollection(ctx, coll, parts...))

	// all-or-nothing: a collection record without its partitions is the
	// crash window this change closes.
	store := fk.MetaKv.(*metaMemKV)
	hasColl, err := store.Has(ctx, EncodeCollectionLoadInfoKey(coll.GetCollectionID()))
	assert.NoError(t, err)
	partKeys, _, err := store.LoadWithPrefix(ctx, EncodePartitionLoadInfoPrefix(coll.GetCollectionID()))
	assert.NoError(t, err)
	if hasColl {
		assert.Len(t, partKeys, len(parts), "collection load info visible with missing partitions")
	} else {
		assert.Empty(t, partKeys)
	}

	assert.NoError(t, c.SaveCollection(ctx, coll, parts...))
	assert.Equal(t, dumpKV(t, legacyKV), dumpKV(t, store))
}

// chunkCrashKV shrinks MaxTxnOps so a composite save takes the chunked
// fallback, and fails the n-th MultiSave chunk to simulate a crash between
// partition batches.
type chunkCrashKV struct {
	kv.MetaKv
	multiSaveCalls int
	failAtCall     int
}

func (f *chunkCrashKV) MaxTxnOps() int { return 2 }

func (f *chunkCrashKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	f.multiSaveCalls++
	if f.multiSaveCalls == f.failAtCall {
		return errors.New("injected crash")
	}
	return f.MetaKv.MultiSave(ctx, kvs)
}

// TestCatalog_SaveCollection_FallbackCrashKeepsCollectionInvisible: when the
// partition set exceeds the store's txn limit the save must flush partitions
// first and land the collection record - recovery's visibility marker - last,
// so a crash between partition chunks leaves the load invisible (recovery
// reads partitions only for collections whose record exists) instead of
// publishing a loaded collection with missing partitions. The retry must
// converge to the legacy end state.
func TestCatalog_SaveCollection_FallbackCrashKeepsCollectionInvisible(t *testing.T) {
	ctx := context.TODO()
	coll := &querypb.CollectionLoadInfo{
		CollectionID:  1,
		ReplicaNumber: 1,
		Status:        querypb.LoadStatus_Loaded,
		LoadType:      querypb.LoadType_LoadCollection,
	}
	parts := make([]*querypb.PartitionLoadInfo, 0, 5)
	for i := int64(0); i < 5; i++ {
		parts = append(parts, &querypb.PartitionLoadInfo{
			CollectionID: 1,
			PartitionID:  11 + i,
			Status:       querypb.LoadStatus_Loaded,
		})
	}

	legacyKV := &metaMemKV{MemoryKV: memkv.NewMemoryKV()}
	legacySaveCollection(t, legacyKV, coll, parts...)

	fk := &chunkCrashKV{MetaKv: &metaMemKV{MemoryKV: memkv.NewMemoryKV()}, failAtCall: 2}
	c := NewCatalog(fk)
	assert.Error(t, c.SaveCollection(ctx, coll, parts...))

	// crash-safety: the collection record (commit marker) must not be visible.
	store := fk.MetaKv.(*metaMemKV)
	hasColl, err := store.Has(ctx, EncodeCollectionLoadInfoKey(coll.GetCollectionID()))
	assert.NoError(t, err)
	assert.False(t, hasColl, "collection load info landed before its partitions")

	assert.NoError(t, c.SaveCollection(ctx, coll, parts...))
	assert.Equal(t, dumpKV(t, legacyKV), dumpKV(t, store))
}

// TestCatalog_Update_RejectsBadLoadInfoActions proves load-info entries paired
// with an action type the catalog does not implement, or carrying nil
// payloads, are rejected with a merr ServiceInternal error and no KV call.
func TestCatalog_Update_RejectsBadLoadInfoActions(t *testing.T) {
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	c := NewCatalog(metakv)

	err := c.Update(context.TODO(), metastore.UpdateAction{
		Type:  metastore.ActionDelete,
		Entry: metastore.CollectionLoadEntry{Collection: &querypb.CollectionLoadInfo{CollectionID: 1}},
	})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	err = c.Update(context.TODO(), metastore.UpdateAction{
		Type:  metastore.ActionDelete,
		Entry: metastore.PartitionLoadEntry{Partition: &querypb.PartitionLoadInfo{CollectionID: 1, PartitionID: 11}},
	})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	err = c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: metastore.CollectionLoadEntry{}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))

	err = c.Update(context.TODO(), metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: metastore.PartitionLoadEntry{}})
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
}
