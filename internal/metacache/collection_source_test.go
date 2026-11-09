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

package metacache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCollections_Hooks(t *testing.T) {
	s := NewCollections()
	var added, removed []int64
	s.OnCollectionAdded(func(id int64) { added = append(added, id) })
	s.OnCollectionRemoved(func(id int64) { removed = append(removed, id) })
	var dropping, altered []int64
	s.OnCollectionDropping(func(id int64) { dropping = append(dropping, id) })
	s.OnCollectionAltered(func(old, updated *CollectionInfo) {
		assert.Equal(t, old.ID, updated.ID)
		altered = append(altered, updated.ID)
	})

	s.Put(&CollectionInfo{ID: 1})
	s.Put(&CollectionInfo{ID: 1, Partitions: []int64{10}}) // replace, not add
	s.Put(&CollectionInfo{ID: 2})
	s.Put(&CollectionInfo{ID: 2, State: etcdpb.CollectionState_CollectionDropping})
	s.Put(&CollectionInfo{ID: 2, State: etcdpb.CollectionState_CollectionDropping}) // already dropping
	s.Remove(1)
	s.Remove(1) // already gone
	s.Remove(3) // never there

	assert.Equal(t, []int64{1, 2}, added)
	assert.Equal(t, []int64{1}, removed)
	assert.Equal(t, []int64{2}, dropping)
	assert.Equal(t, []int64{1}, altered, "only a Created collection replaced by a Created one")
	assert.Equal(t, []int64{2}, s.GetCollectionIDs())
}

func TestMetaStore_ForwardsCollectionReads(t *testing.T) {
	store := NewMetaStore(nil)
	collectionsOf(store).Put(&CollectionInfo{ID: 1})

	replacement := NewCollections()
	replacement.Put(&CollectionInfo{ID: 2})
	store.SetCollectionSource(replacement)

	_, ok := store.GetCollection(1)
	assert.False(t, ok)
	got, ok := store.GetCollection(2)
	assert.True(t, ok)
	assert.Equal(t, int64(2), got.ID)
	assert.Equal(t, []int64{2}, store.GetCollectionIDs())
	assert.Len(t, store.GetCollections(), 1)
	assert.Same(t, replacement, store.CollectionSource())
}

func TestCollectionInfo_IsCreated(t *testing.T) {
	var nilInfo *CollectionInfo
	assert.False(t, nilInfo.IsCreated())
	assert.True(t, (&CollectionInfo{}).IsCreated())
}

func TestCollectionInfo_LivePartitionID(t *testing.T) {
	info := &CollectionInfo{
		Partitions:         []int64{1, 3},
		DroppingPartitions: []int64{2, 4},
		// "a" was dropped and reused by partition 3; "b" is only Dropping.
		PartitionIDsByName: map[string]int64{"_default": 1, "a": 3, "b": 2},
	}
	for _, tc := range []struct {
		name   string
		wantID int64
		wantOK bool
	}{
		{"_default", 1, true},
		{"a", 3, true},
		{"b", 0, false},
		{"missing", 0, false},
	} {
		id, ok := info.LivePartitionID(tc.name)
		assert.Equal(t, tc.wantOK, ok, tc.name)
		assert.Equal(t, tc.wantID, id, tc.name)
	}
}

// Reads go through the MetaStore, which maps an empty dbName to the default
// database.
func TestMetaStore_GetCollectionByName(t *testing.T) {
	s := NewMetaStore(nil)
	collectionsOf(s).Put(&CollectionInfo{ID: 1, DatabaseName: "db1", Schema: &schemapb.CollectionSchema{Name: "a"}})
	collectionsOf(s).Put(&CollectionInfo{ID: 2, DatabaseName: util.DefaultDBName, Schema: &schemapb.CollectionSchema{Name: "a"}})
	collectionsOf(s).Put(&CollectionInfo{
		ID: 3, DatabaseName: "db1", Schema: &schemapb.CollectionSchema{Name: "gone"},
		State: etcdpb.CollectionState_CollectionDropping,
	})
	tests := []struct {
		name   string
		dbName string
		coll   string
		want   int64
	}{
		{"by database", "db1", "a", 1},
		{"empty db is default", "", "a", 2},
		{"miss", "db1", "b", 0},
		{"dropping", "db1", "gone", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info, ok := s.GetCollectionByName(tc.dbName, tc.coll)
			assert.Equal(t, tc.want != 0, ok)
			if ok {
				assert.Equal(t, tc.want, info.ID)
			}
		})
	}
}

func TestGetDatabaseOrNotFound(t *testing.T) {
	s := NewMetaStore(nil)
	collectionsOf(s).PutDatabase(&DatabaseInfo{ID: 1, Name: util.DefaultDBName})
	db, err := GetDatabaseOrNotFound(s, "")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), db.ID)
	_, err = GetDatabaseOrNotFound(s, "missing")
	assert.ErrorIs(t, err, merr.ErrDatabaseNotFound)
}

func TestMetaStore_RelaysCurrentSourceEvents(t *testing.T) {
	store := NewMetaStore(nil)
	old := collectionsOf(store)
	var added, removed []int64
	store.OnCollectionAdded(func(id int64) { added = append(added, id) })
	store.OnCollectionRemoved(func(id int64) { removed = append(removed, id) })

	old.Put(&CollectionInfo{ID: 1})
	replacement := NewCollections()
	store.SetCollectionSource(replacement)
	old.Put(&CollectionInfo{ID: 2}) // no longer current
	replacement.Put(&CollectionInfo{ID: 3})
	replacement.Remove(3)

	assert.Equal(t, []int64{1, 3}, added)
	assert.Equal(t, []int64{3}, removed)
}

// collectionsOf returns the store-owned collection source behind view.
func collectionsOf(view MetaView) *Collections {
	return view.(MetaStore).CollectionSource().(*Collections)
}
