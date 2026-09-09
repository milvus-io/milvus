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

package datacoord

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
)

func newTestCatalogGroup() *model.SegmentChangeGroup {
	return &model.SegmentChangeGroup{
		GroupID:              1,
		Source:               model.SegmentChangeSourceMixCompaction,
		CollectionID:         10,
		State:                model.SegmentChangeStateStaged,
		NewSegmentIDs:        []int64{1001},
		SupersededSegmentIDs: []int64{2001},
		CreateTS:             123,
	}
}

// memoryMetaKv backs the mock MetaKv with an in-memory map so the catalog
// persistence round-trip is exercised with real encodings.
type memoryMetaKv struct {
	mu     sync.Mutex
	values map[string]string
}

func newMemoryMetaKv() *memoryMetaKv {
	return &memoryMetaKv{values: make(map[string]string)}
}

// TestCatalog_SegmentChangeGroupLifecycle verifies the composite Update entry
// encoding and the List/Drop catalog methods used for recovery and collection
// drop.
func TestCatalog_SegmentChangeGroupLifecycle(t *testing.T) {
	ctx := context.Background()
	backing := newMemoryMetaKv()
	metakv := mocks.NewMetaKv(t)

	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			backing.mu.Lock()
			defer backing.mu.Unlock()
			for k, v := range saves {
				backing.values[k] = v
			}
			for _, k := range removals {
				delete(backing.values, k)
			}
			return nil
		})
	metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, prefix string, _ int, fn func([]byte, []byte) error) error {
			backing.mu.Lock()
			defer backing.mu.Unlock()
			for k, v := range backing.values {
				if strings.HasPrefix(k, prefix) {
					if err := fn([]byte(k), []byte(v)); err != nil {
						return err
					}
				}
			}
			return nil
		})
	metakv.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, prefix string) error {
			backing.mu.Lock()
			defer backing.mu.Unlock()
			for k := range backing.values {
				if strings.HasPrefix(k, prefix) {
					delete(backing.values, k)
				}
			}
			return nil
		})

	catalog := NewCatalog(metakv, "", "")

	group := newTestCatalogGroup()
	require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(group)))

	groups, err := catalog.ListSegmentChangeGroups(ctx)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, group, groups[0])

	// Recovery round-trip of a ready group.
	ready := group.Clone()
	ready.State = model.SegmentChangeStateReady
	require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(ready)))
	groups, err = catalog.ListSegmentChangeGroups(ctx)
	require.NoError(t, err)
	require.Equal(t, model.SegmentChangeStateReady, groups[0].State)

	// Collection-scoped delete removes every group of the collection.
	other := newTestCatalogGroup()
	other.GroupID = 2
	other.CollectionID = 11
	require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(other)))
	require.NoError(t, catalog.DropSegmentChangeGroups(ctx, 10))
	groups, err = catalog.ListSegmentChangeGroups(ctx)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, int64(11), groups[0].CollectionID)

	// Per-record delete.
	require.NoError(t, catalog.Update(ctx, metastore.DeleteSegmentChangeGroup(11, 2)))
	groups, err = catalog.ListSegmentChangeGroups(ctx)
	require.NoError(t, err)
	require.Empty(t, groups)
}

// TestCatalog_SegmentChangeGroupCorruptValueFailsClosed verifies C4: a malformed
// persisted group record aborts the List walk (propagating the decode error),
// because a skipped group would orphan its staged members — unlike DataView
// snapshots, a group record is not reconstructible without SegmentInfo's
// change_group_id field.
func TestCatalog_SegmentChangeGroupCorruptValueFailsClosed(t *testing.T) {
	ctx := context.Background()
	backing := newMemoryMetaKv()
	metakv := mocks.NewMetaKv(t)

	metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
	metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			backing.mu.Lock()
			defer backing.mu.Unlock()
			for k, v := range saves {
				backing.values[k] = v
			}
			for _, k := range removals {
				delete(backing.values, k)
			}
			return nil
		})
	metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, prefix string, _ int, fn func([]byte, []byte) error) error {
			backing.mu.Lock()
			defer backing.mu.Unlock()
			for k, v := range backing.values {
				if strings.HasPrefix(k, prefix) {
					if err := fn([]byte(k), []byte(v)); err != nil {
						return err
					}
				}
			}
			return nil
		})

	catalog := NewCatalog(metakv, "", "")
	require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(newTestCatalogGroup())))
	backing.mu.Lock()
	backing.values[buildSegmentChangeGroupKey(99, 1)] = "not-json"
	backing.mu.Unlock()

	_, err := catalog.ListSegmentChangeGroups(ctx)
	require.Error(t, err, "a malformed persisted group must fail recovery, not be skipped")
}
