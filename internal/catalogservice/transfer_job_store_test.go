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

package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
)

func TestKVTransferJobStoreRejectsUnsafeTransferID(t *testing.T) {
	ctx := context.Background()
	kv := mocks.NewMetaKv(t)
	store := NewKVTransferJobStore(kv, "catalog/transfer")

	for _, transferID := range []string{"", "../transfer-1", "tenant/transfer-1", "transfer 1"} {
		_, err := store.Get(ctx, transferID)
		require.ErrorIs(t, err, ErrInvalidCatalogPathSegment)

		err = store.Save(ctx, &TransferJob{TransferID: transferID, State: TransferStatePending})
		require.ErrorIs(t, err, ErrInvalidCatalogPathSegment)

		err = store.CompareAndSave(ctx, nil, &TransferJob{TransferID: transferID, State: TransferStatePending})
		require.ErrorIs(t, err, ErrInvalidCatalogPathSegment)
	}
}

func TestKVTransferJobStorePersistsJob(t *testing.T) {
	ctx := context.Background()
	kv := mocks.NewMetaKv(t)
	store := NewKVTransferJobStore(kv, "catalog/transfer")

	var savedKey string
	var savedValue string
	kv.EXPECT().Save(mock.Anything, "catalog/transfer/transfer-1", mock.Anything).RunAndReturn(func(_ context.Context, key string, value string) error {
		savedKey = key
		savedValue = value
		return nil
	})
	require.NoError(t, store.Save(ctx, &TransferJob{
		TransferID:    "transfer-1",
		TransferEpoch: 10,
		CollectionID:  100,
		Collection:    testTransferCollection(),
		State:         TransferStateCatalogMoved,
	}))
	require.Equal(t, "catalog/transfer/transfer-1", savedKey)

	kv.EXPECT().Load(mock.Anything, "catalog/transfer/transfer-1").Return(savedValue, nil)
	job, err := store.Get(ctx, "transfer-1")
	require.NoError(t, err)
	require.Equal(t, TransferStateCatalogMoved, job.State)
	require.Equal(t, int64(100), job.CollectionID)
	require.NotNil(t, job.Collection)
	require.Equal(t, "coll", job.Collection.Name)
	require.Equal(t, int64(101), job.Collection.Partitions[0].PartitionID)
}

func TestKVTransferJobStoreCompareAndSaveCreatesJobIfAbsent(t *testing.T) {
	ctx := context.Background()
	kv := mocks.NewMetaKv(t)
	store := NewKVTransferJobStore(kv, "catalog/transfer")
	job := &TransferJob{TransferID: "transfer-1", State: TransferStatePending}

	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.MatchedBy(func(saves map[string]string) bool {
		return len(saves) == 1 && saves["catalog/transfer/transfer-1"] != ""
	}), mock.Anything, mock.MatchedBy(func(pred predicates.Predicate) bool {
		return pred.Key() == "catalog/transfer/transfer-1" && pred.TargetValue() == false
	})).RunAndReturn(func(_ context.Context, saves map[string]string, _ []string, _ ...predicates.Predicate) error {
		require.Contains(t, saves["catalog/transfer/transfer-1"], `"Version":1`)
		return nil
	})

	require.NoError(t, store.CompareAndSave(ctx, nil, job))
	require.Equal(t, int64(1), job.Version)
}

func TestKVTransferJobStoreCompareAndSaveUsesExpectedValuePredicate(t *testing.T) {
	ctx := context.Background()
	kv := mocks.NewMetaKv(t)
	store := NewKVTransferJobStore(kv, "catalog/transfer")
	expected := &TransferJob{TransferID: "transfer-1", Version: 3, State: TransferStatePrepared}

	kv.EXPECT().Load(mock.Anything, "catalog/transfer/transfer-1").Return(`{"TransferID":"transfer-1","Version":3,"State":"PREPARED"}`, nil)
	loaded, err := store.Get(ctx, "transfer-1")
	require.NoError(t, err)
	require.Equal(t, expected.Version, loaded.Version)

	next := loaded.clone()
	next.State = TransferStateSourceDropped
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.MatchedBy(func(saves map[string]string) bool {
		return len(saves) == 1 && saves["catalog/transfer/transfer-1"] != ""
	}), mock.Anything, mock.MatchedBy(func(pred predicates.Predicate) bool {
		return pred.Key() == "catalog/transfer/transfer-1" &&
			pred.TargetValue() == loaded.storeValue
	})).Return(nil)

	require.NoError(t, store.CompareAndSave(ctx, loaded, next))
	require.Equal(t, int64(4), next.Version)
}
