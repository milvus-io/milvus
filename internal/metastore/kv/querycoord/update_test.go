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

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
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
