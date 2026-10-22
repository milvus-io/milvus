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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
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
