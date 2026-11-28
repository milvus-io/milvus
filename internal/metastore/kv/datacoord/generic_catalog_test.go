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
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// recordingMetaKv is an in-memory MetaKv without the revision and watch
// capabilities (the non-etcd fallback path). It records every write call, in
// order, so tests can pin the exact KV traffic.
type recordingMetaKv struct {
	mu     sync.Mutex
	values map[string]string
	calls  []kvCall
}

// kvCall is one recorded MetaKv write.
type kvCall struct {
	op       string
	saves    map[string]string
	removals []string
}

func (r *recordingMetaKv) apply(op string, saves map[string]string, removals []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, k := range removals {
		delete(r.values, k)
	}
	for k, v := range saves {
		r.values[k] = v
	}
	sortedRemovals := append([]string(nil), removals...)
	sort.Strings(sortedRemovals)
	if len(sortedRemovals) == 0 {
		sortedRemovals = nil
	}
	r.calls = append(r.calls, kvCall{op: op, saves: saves, removals: sortedRemovals})
}

func newRecordingCatalog(t *testing.T, maxTxnOps int) (*Catalog, *recordingMetaKv) {
	catalog, r, _ := newRecordingCatalogWith(t, maxTxnOps)
	return catalog, r
}

func newRecordingCatalogWith(t *testing.T, maxTxnOps int) (*Catalog, *recordingMetaKv, *mocks.MetaKv) {
	r := &recordingMetaKv{values: make(map[string]string)}
	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MaxTxnOps().Return(maxTxnOps).Maybe()
	saveAndRemove := func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
		r.apply("MultiSaveAndRemove", saves, removals)
		return nil
	}
	metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(saveAndRemove).Maybe()
	metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(saveAndRemove).Maybe()
	metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, saves map[string]string) error {
			r.apply("MultiSave", saves, nil)
			return nil
		}).Maybe()
	metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, prefix string, _ int, fn func([]byte, []byte) error) error {
			r.mu.Lock()
			keys := make([]string, 0, len(r.values))
			for k := range r.values {
				if strings.HasPrefix(k, prefix) {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			values := make([]string, len(keys))
			for i, k := range keys {
				values[i] = r.values[k]
			}
			r.mu.Unlock()
			for i, k := range keys {
				if err := fn([]byte(k), []byte(values[i])); err != nil {
					return err
				}
			}
			return nil
		}).Maybe()
	metakv.EXPECT().Load(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, key string) (string, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			v, ok := r.values[key]
			if !ok {
				return "", merr.WrapErrIoKeyNotFound(key)
			}
			return v, nil
		}).Maybe()
	return NewCatalog(metakv, "", ""), r, metakv
}

func (r *recordingMetaKv) set(key, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[key] = value
}

func (r *recordingMetaKv) del(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.values, key)
}

func (r *recordingMetaKv) recorded() []kvCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]kvCall(nil), r.calls...)
}

// Golden values: the exact bytes the typed SegmentChangeGroupEntry encoding
// wrote before it moved onto the registered model type
// (model.MarshalSegmentChangeGroup, sonic ConfigStd, struct field order). They
// must not change, or upgrades and downgrades would read different records.
const (
	goldenStagedGroupKey   = "datacoord-meta/segment-change-group/10/1"
	goldenStagedGroupValue = `{"groupID":1,"source":3,"collectionID":10,"partitionID":0,"sourceJobID":0,` +
		`"state":0,"newSegmentIds":[1001],"supersededSegmentIds":[2001],"supersededL0SegmentIds":null,` +
		`"commitTS":0,"publishEpoch":0,"createTS":123,"readyTS":0,"commitTime":0,"failReason":""}`
	goldenCommittedGroupKey   = "datacoord-meta/segment-change-group/10/2"
	goldenCommittedGroupValue = `{"groupID":2,"source":1,"collectionID":10,"partitionID":0,"sourceJobID":0,` +
		`"state":2,"newSegmentIds":[1002,1003],"supersededSegmentIds":null,"supersededL0SegmentIds":null,` +
		`"commitTS":77,"publishEpoch":3,"createTS":0,"readyTS":0,"commitTime":88,"failReason":""}`
	goldenDeletedGroupKey = "datacoord-meta/segment-change-group/11/5"
)

func goldenGroupActions() []metastore.UpdateAction {
	committed := &model.SegmentChangeGroup{
		GroupID: 2, Source: model.SegmentChangeSourceImportJob, CollectionID: 10,
		State: model.SegmentChangeStateCommitted, NewSegmentIDs: []int64{1002, 1003},
		CommitTS: 77, CommitTime: 88, PublishEpoch: 3,
	}
	return []metastore.UpdateAction{
		metastore.SaveSegmentChangeGroup(newTestCatalogGroup()),
		metastore.SaveSegmentChangeGroup(committed),
		metastore.DeleteSegmentChangeGroup(11, 5),
	}
}

// TestSegmentChangeGroup_EncodingMatchesLegacy pins keys, values and the KV
// call sequence: one atomic txn when it fits, ordered chunks otherwise (no
// commit markers), the type prefix for recovery and the collection prefix
// for collection drop.
func TestSegmentChangeGroup_EncodingMatchesLegacy(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		maxTxnOps int
		actions   []metastore.UpdateAction
		want      []kvCall
	}{
		{
			name:      "atomic update",
			maxTxnOps: 128,
			actions:   goldenGroupActions(),
			want: []kvCall{{
				op:       "MultiSaveAndRemove",
				saves:    map[string]string{goldenStagedGroupKey: goldenStagedGroupValue, goldenCommittedGroupKey: goldenCommittedGroupValue},
				removals: []string{goldenDeletedGroupKey},
			}},
		},
		{
			// 3 ops over a limit of 2: chunks in the caller's order.
			name:      "chunked fallback",
			maxTxnOps: 2,
			actions:   goldenGroupActions(),
			want: []kvCall{
				{op: "MultiSave", saves: map[string]string{goldenStagedGroupKey: goldenStagedGroupValue, goldenCommittedGroupKey: goldenCommittedGroupValue}},
				{op: "MultiSaveAndRemove", removals: []string{goldenDeletedGroupKey}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog, r := newRecordingCatalog(t, tt.maxTxnOps)
			require.NoError(t, catalog.Update(ctx, tt.actions...))
			require.Equal(t, tt.want, r.recorded())
		})
	}

	t.Run("recovery prefix", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().WalkWithPrefix(mock.Anything, scgKey(""), mock.Anything, mock.Anything).
			Return(nil).Once()
		groups, err := listTestGroups(ctx, NewCatalog(metakv, "", ""))
		require.NoError(t, err)
		require.Empty(t, groups)
	})
}

// TestCatalog_UpdateRejects covers the writes Update refuses before any
// write.
func TestCatalog_UpdateRejects(t *testing.T) {
	g := newTestCatalogGroup()
	tests := []struct {
		name   string
		action metastore.UpdateAction
	}{
		{"nil value", metastore.SaveSegmentChangeGroup(nil)},
		{"unknown action type", metastore.UpdateAction{Type: metastore.ActionType(100), Entry: g}},
		{"value of an unregistered type", metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: *g}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog, r := newRecordingCatalog(t, 128)
			require.Error(t, catalog.Update(context.Background(), tt.action))
			require.Empty(t, r.recorded())
		})
	}
}

// TestCatalog_UnchangedSince covers the read-modify-write guard: every
// written key is guarded by the read revision, a value saved with
// ActionUpdate must still exist, an unmet guard matches predicates.ErrNotMet, and a guarded
// Update that cannot be one atomic txn writes nothing.
func TestCatalog_UnchangedSince(t *testing.T) {
	ctx := context.Background()
	g := newTestCatalogGroup() // collection 10, group 1

	t.Run("guards every written key", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
		metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, []string{scgKey("10/2")},
			mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, saves map[string]string, _ []string, preds ...predicates.Predicate) error {
				require.Contains(t, saves, scgKey("10/1"))
				type pred struct {
					target predicates.PredicateTarget
					key    string
				}
				got := make([]pred, 0, len(preds))
				for _, p := range preds {
					got = append(got, pred{p.Target(), p.Key()})
					if p.Target() == predicates.PredTargetUnchangedSince {
						require.Equal(t, int64(7), p.TargetValue())
					}
				}
				require.Equal(t, []pred{
					{predicates.PredTargetKeyExist, scgKey("10/1")},
					{predicates.PredTargetUnchangedSince, scgKey("10/1")},
					{predicates.PredTargetUnchangedSince, scgKey("10/2")},
				}, got)
				return nil
			}).Once()
		catalog := NewCatalog(revisionedMetaKv{metakv}, "", "")
		require.NoError(t, catalog.Update(ctx,
			metastore.UnchangedSince(7),
			metastore.SaveSegmentChangeGroup(g),
			metastore.DeleteSegmentChangeGroup(10, 2)))
	})

	t.Run("unmet guard is predicate not met", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MaxTxnOps().Return(128).Maybe()
		metakv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.Mark(merr.WrapErrIoFailedReason("failed to execute transaction"), predicates.ErrNotMet)).Once()
		catalog := NewCatalog(revisionedMetaKv{metakv}, "", "")
		err := catalog.Update(ctx, metastore.UnchangedSince(7), metastore.SaveSegmentChangeGroup(g))
		require.True(t, errors.Is(err, predicates.ErrNotMet), "%v", err)
		require.ErrorIs(t, err, merr.ErrIoFailed)
	})

	for _, tc := range []struct {
		name      string
		maxTxnOps int
		actions   []metastore.UpdateAction
	}{
		{"two guards", 128, []metastore.UpdateAction{
			metastore.UnchangedSince(7), metastore.UnchangedSince(8), metastore.SaveSegmentChangeGroup(g),
		}},
		{"zero revision", 128, []metastore.UpdateAction{
			metastore.UnchangedSince(0), metastore.SaveSegmentChangeGroup(g),
		}},
		{"over the txn limit", 1, []metastore.UpdateAction{
			metastore.UnchangedSince(7), metastore.SaveSegmentChangeGroup(g), metastore.DeleteSegmentChangeGroup(10, 2),
		}},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			_, r, metakv := newRecordingCatalogWith(t, tc.maxTxnOps)
			catalog := NewCatalog(revisionedMetaKv{metakv}, "", "")
			err := catalog.Update(ctx, tc.actions...)
			require.Error(t, err)
			require.NotErrorIs(t, err, merr.ErrServiceUnimplemented)
			require.Empty(t, r.recorded())
		})
	}

	t.Run("unimplemented without revisions", func(t *testing.T) {
		catalog, r := newRecordingCatalog(t, 128)
		err := catalog.Update(ctx, metastore.UnchangedSince(7), metastore.SaveSegmentChangeGroup(g))
		require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
		require.Empty(t, r.recorded())
	})
}

// revisionedMetaKv gives a MetaKv the revisionLoader capability, so the
// catalog accepts guarded Updates on it.
type revisionedMetaKv struct {
	kv.MetaKv
}

func (revisionedMetaKv) LoadWithRevision(context.Context, string) (string, int64, error) {
	return "", 0, merr.WrapErrServiceInternalMsg("not used")
}

// TestCatalog_Deletes covers the deletes: a value names one record, zero IDs
// included, and the chunked fallback applies deletes in order.
func TestCatalog_Deletes(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		maxTxnOps int
		actions   []metastore.UpdateAction
		want      []kvCall
	}{
		{
			name: "a zero group ID is one record", maxTxnOps: 128,
			actions: []metastore.UpdateAction{metastore.DeleteSegmentChangeGroup(10, 0)},
			want:    []kvCall{{op: "MultiSaveAndRemove", saves: map[string]string{}, removals: []string{scgKey("10/0")}}},
		},
		{
			name: "the chunked fallback applies them in order", maxTxnOps: 1,
			actions: []metastore.UpdateAction{metastore.DeleteSegmentChangeGroup(11, 5), metastore.DeleteSegmentChangeGroup(10, 1)},
			want: []kvCall{
				{op: "MultiSaveAndRemove", removals: []string{goldenDeletedGroupKey}},
				{op: "MultiSaveAndRemove", removals: []string{scgKey("10/1")}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog, r := newRecordingCatalog(t, tt.maxTxnOps)
			require.NoError(t, catalog.Update(ctx, tt.actions...))
			require.Equal(t, tt.want, r.recorded())
		})
	}
}

// TestCatalog_ListKeysOnly covers a keys-only List: it returns coordinates
// without decoding, so a corrupt value is listed, fails on a key that is not
// a record, and cannot filter.
func TestCatalog_ListKeysOnly(t *testing.T) {
	ctx := context.Background()
	catalog, r := newRecordingCatalog(t, 128)
	require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(newTestCatalogGroup())))
	r.set(scgKey("10/2"), "not-json")
	r.set(scgKey("11/3"), "not-json")

	res, err := catalog.List(ctx, metastore.SegmentGroupType, []string{"10"}, metastore.ListOptions{KeysOnly: true})
	require.NoError(t, err)
	require.Nil(t, res.Values)
	require.Equal(t, [][]string{{"10", "1"}, {"10", "2"}}, res.Coordinates)

	_, err = catalog.List(ctx, metastore.SegmentGroupType, []string{"10"}, metastore.ListOptions{})
	require.Error(t, err, "a full List still fails closed on the corrupt value")

	r.set(scgKey("12"), "x")
	_, err = catalog.List(ctx, metastore.SegmentGroupType, nil, metastore.ListOptions{KeysOnly: true})
	require.ErrorIs(t, err, merr.ErrDataIntegrity, "a key that is not a record fails")

	_, err = catalog.List(ctx, metastore.SegmentGroupType, nil,
		metastore.ListOptions{KeysOnly: true, Filter: func(any) bool { return true }})
	require.Error(t, err)
}

// TestCatalog_ListPrefixIsADirectory pins that a prefix ends with "/": the
// groups of collection 10 do not include those of 1 or 100.
func TestCatalog_ListPrefixIsADirectory(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newRecordingCatalog(t, 128)
	for _, coll := range []int64{1, 10, 100} {
		g := newTestCatalogGroup()
		g.CollectionID = coll
		require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(g)))
	}
	res, err := catalog.List(ctx, metastore.SegmentGroupType, []string{"10"}, metastore.ListOptions{})
	require.NoError(t, err)
	require.Len(t, res.Values, 1)
	require.Equal(t, int64(10), res.Values[0].(*model.SegmentChangeGroup).CollectionID)
}

// TestCatalog_GenericReadsFallback covers Get, List and Watch on a MetaKv
// without the page and watch capabilities.
func TestCatalog_GenericReadsFallback(t *testing.T) {
	ctx := context.Background()
	catalog, r := newRecordingCatalog(t, 128)
	for _, ids := range [][2]int64{{10, 1}, {10, 2}, {10, 3}, {11, 4}, {11, 5}} {
		g := newTestCatalogGroup()
		g.CollectionID, g.GroupID = ids[0], ids[1]
		g.NewSegmentIDs = []int64{1000 + ids[1]}
		g.SupersededSegmentIDs = nil
		if ids[1]%2 == 0 {
			g.State = model.SegmentChangeStateCommitted
		}
		require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(g)))
	}

	t.Run("get", func(t *testing.T) {
		v, rev, err := catalog.Get(ctx, metastore.SegmentGroupType, []string{"10", "2"})
		require.NoError(t, err)
		require.Zero(t, rev, "no revision without the capability")
		require.Equal(t, []int64{1002}, v.(*model.SegmentChangeGroup).NewSegmentIDs)
		_, _, err = catalog.Get(ctx, metastore.SegmentGroupType, []string{"10", "9"})
		require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	})

	t.Run("list", func(t *testing.T) {
		tests := []struct {
			name   string
			prefix []string
			filter func(any) bool
			want   []string
		}{
			{"whole type", nil, nil, []string{"10/1", "10/2", "10/3", "11/4", "11/5"}},
			{"one collection", []string{"11"}, nil, []string{"11/4", "11/5"}},
			{"filtered", nil, committedGroup, []string{"10/2", "11/4"}},
			{"filtered collection", []string{"10"}, committedGroup, []string{"10/2"}},
			{"empty collection", []string{"1"}, nil, []string{}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				res, err := catalog.List(ctx, metastore.SegmentGroupType, tt.prefix, metastore.ListOptions{Filter: tt.filter})
				require.NoError(t, err)
				require.Equal(t, tt.want, groupIDs(res.Values))
			})
		}
	})

	t.Run("a corrupt record fails the list even when the filter would skip it", func(t *testing.T) {
		r.set(scgKey("12/1"), "not-json")
		defer r.del(scgKey("12/1"))
		_, err := catalog.List(ctx, metastore.SegmentGroupType, nil, metastore.ListOptions{
			Filter: func(any) bool { return false },
		})
		require.ErrorContains(t, err, scgKey("12/1"))
	})

	t.Run("watch is unsupported", func(t *testing.T) {
		err := catalog.Watch(ctx, metastore.SegmentGroupType, nil, metastore.WatchOptions{}, ignoreBatch)
		require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	})

	t.Run("coordinates and prefixes are checked", func(t *testing.T) {
		_, _, err := catalog.Get(ctx, metastore.SegmentGroupType, []string{"10"})
		require.Error(t, err, "a prefix is not a record")
		_, _, err = catalog.Get(ctx, metastore.SegmentGroupType, []string{"10", ""})
		require.Error(t, err, "an empty component")
		_, err = catalog.List(ctx, metastore.SegmentGroupType, []string{"10", "1"}, metastore.ListOptions{})
		require.Error(t, err, "a record is not a prefix")
		_, err = catalog.List(ctx, &metastore.ModelType{Root: "unregistered", Depth: 1}, nil, metastore.ListOptions{})
		require.Error(t, err, "an unregistered type")
		err = catalog.Watch(ctx, metastore.SegmentGroupType, []string{"10", "1"}, metastore.WatchOptions{}, ignoreBatch)
		require.Error(t, err, "a watch names a prefix, not a record")
	})
}

// pagedMetaKv adds the pageScanner capability, as the etcd MetaKv has it,
// over the records of a recordingMetaKv. An unpinned read reports revision;
// the first compactions pinned reads fail as if the backend compacted the
// revision.
type pagedMetaKv struct {
	*mocks.MetaKv
	r           *recordingMetaKv
	revision    int64
	compactions int
	reads       int
}

func (m *pagedMetaKv) WalkPrefixPage(_ context.Context, prefix, after string, limit int, revision int64,
	fn func(key, value []byte) error,
) (int64, bool, error) {
	m.reads++
	if revision != 0 && m.compactions > 0 {
		m.compactions--
		return 0, false, errors.Mark(rpctypes.ErrCompacted, kv.ErrCompacted)
	}
	m.r.mu.Lock()
	var keys []string
	for k := range m.r.values {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	n := min(limit, len(keys))
	values := make([]string, n)
	for i, k := range keys[:n] {
		values[i] = m.r.values[k]
	}
	m.r.mu.Unlock()
	for i, k := range keys[:n] {
		if err := fn([]byte(k), []byte(values[i])); err != nil {
			return 0, false, err
		}
	}
	if revision == 0 {
		revision = m.revision
	}
	return revision, n < len(keys), nil
}

// TestCatalog_ListPages covers List over the pageScanner capability: every
// page of a walk reads one revision; a List of every match restarts after a
// compaction and walks without a snapshot after maxListAttempts; a paged List
// returns at most
// Limit matches and the cursor of the next page, reports a compaction
// instead of restarting, and rejects a cursor outside its scope.
func TestCatalog_ListPages(t *testing.T) {
	ctx := context.Background()
	newCatalog := func(t *testing.T, compactions int) (*Catalog, *pagedMetaKv) {
		base, r, metakv := newRecordingCatalogWith(t, 128)
		for id := int64(1); id <= 5; id++ {
			g := newTestCatalogGroup()
			g.GroupID = id
			g.NewSegmentIDs = []int64{1000 + id}
			g.SupersededSegmentIDs = nil
			if id%2 == 0 {
				g.State = model.SegmentChangeStateCommitted
			}
			require.NoError(t, base.Update(ctx, metastore.SaveSegmentChangeGroup(g)))
		}
		paged := &pagedMetaKv{MetaKv: metakv, r: r, revision: 42, compactions: compactions}
		catalog := NewCatalog(paged, "", "")
		catalog.paginationSize = 2 // several reads per List
		return catalog, paged
	}
	list := func(c *Catalog, opts metastore.ListOptions) (metastore.ListResult, error) {
		return c.List(ctx, metastore.SegmentGroupType, nil, opts)
	}

	t.Run("every match from one revision", func(t *testing.T) {
		catalog, paged := newCatalog(t, 0)
		res, err := list(catalog, metastore.ListOptions{})
		require.NoError(t, err)
		require.Equal(t, []string{"10/1", "10/2", "10/3", "10/4", "10/5"}, groupIDs(res.Values))
		require.Equal(t, metastore.Revision(42), res.Revision)
		require.Empty(t, res.Next)
		require.Equal(t, 3, paged.reads)
	})

	t.Run("restarted after a compaction", func(t *testing.T) {
		catalog, _ := newCatalog(t, maxListAttempts-1)
		res, err := list(catalog, metastore.ListOptions{})
		require.NoError(t, err)
		require.Equal(t, []string{"10/1", "10/2", "10/3", "10/4", "10/5"}, groupIDs(res.Values),
			"records of a failed walk are dropped")
	})

	t.Run("walks without a snapshot after maxListAttempts", func(t *testing.T) {
		catalog, _ := newCatalog(t, maxListAttempts)
		res, err := list(catalog, metastore.ListOptions{})
		require.NoError(t, err)
		require.Equal(t, []string{"10/1", "10/2", "10/3", "10/4", "10/5"}, groupIDs(res.Values))
		require.Zero(t, res.Revision, "no snapshot, no revision")
	})

	t.Run("pages", func(t *testing.T) {
		catalog, _ := newCatalog(t, 0)
		opts := metastore.ListOptions{Limit: 2}
		var pages [][]string
		for {
			res, err := list(catalog, opts)
			require.NoError(t, err)
			require.Equal(t, metastore.Revision(42), res.Revision)
			pages = append(pages, groupIDs(res.Values))
			if res.Next == "" {
				break
			}
			opts.After, opts.Revision = res.Next, res.Revision
		}
		require.Equal(t, [][]string{{"10/1", "10/2"}, {"10/3", "10/4"}, {"10/5"}}, pages)
	})

	tests := []struct {
		name     string
		opts     metastore.ListOptions
		want     []string
		wantNext string
	}{
		{
			"a page that ends the list has no cursor",
			metastore.ListOptions{Limit: 5},
			[]string{"10/1", "10/2", "10/3", "10/4", "10/5"},
			"",
		},
		{
			"a filtered page",
			metastore.ListOptions{Limit: 1, Filter: committedGroup},
			[]string{"10/2"},
			scgKey("10/2"),
		},
		{
			"a filtered page after a cursor",
			metastore.ListOptions{Limit: 1, Filter: committedGroup, After: scgKey("10/2")},
			[]string{"10/4"},
			scgKey("10/4"),
		},
		{
			"a cursor may be set when no match remains",
			metastore.ListOptions{Limit: 1, Filter: committedGroup, After: scgKey("10/4")},
			[]string{},
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog, _ := newCatalog(t, 0)
			res, err := list(catalog, tt.opts)
			require.NoError(t, err)
			require.Equal(t, tt.want, groupIDs(res.Values))
			require.Equal(t, tt.wantNext, res.Next)
		})
	}

	t.Run("a paged list reports a compaction", func(t *testing.T) {
		catalog, paged := newCatalog(t, 1)
		_, err := list(catalog, metastore.ListOptions{Limit: 2, After: scgKey("10/2"), Revision: 42})
		require.True(t, errors.Is(err, kv.ErrCompacted), "%v", err)
		require.Equal(t, 1, paged.reads, "only the caller can restart a paged list")
	})

	t.Run("misuse", func(t *testing.T) {
		catalog, _ := newCatalog(t, 0)
		_, err := catalog.List(ctx, metastore.SegmentGroupType, []string{"10"}, metastore.ListOptions{After: "elsewhere/1"})
		require.Error(t, err, "a cursor outside the scope")
		_, err = list(catalog, metastore.ListOptions{Limit: -1})
		require.Error(t, err, "a negative limit")
	})

	t.Run("without the capability", func(t *testing.T) {
		catalog, _ := newRecordingCatalog(t, 128)
		for id := int64(1); id <= 3; id++ {
			g := newTestCatalogGroup()
			g.GroupID = id
			g.SupersededSegmentIDs = nil
			require.NoError(t, catalog.Update(ctx, metastore.SaveSegmentChangeGroup(g)))
		}
		res, err := list(catalog, metastore.ListOptions{Limit: 2})
		require.NoError(t, err)
		require.Equal(t, []string{"10/1", "10/2"}, groupIDs(res.Values))
		require.Equal(t, scgKey("10/2"), res.Next)
		require.Zero(t, res.Revision, "no snapshot without the capability")
		res, err = list(catalog, metastore.ListOptions{Limit: 2, After: res.Next})
		require.NoError(t, err)
		require.Equal(t, []string{"10/3"}, groupIDs(res.Values))
		require.Empty(t, res.Next)
	})
}

// TestGroupByRevision_TypedEvents covers how etcd watch events become typed
// batches: one batch per revision, puts decoded, deletes identified by
// coordinate, and a corrupt record failing the batch.
func TestGroupByRevision_TypedEvents(t *testing.T) {
	batches, err := groupByRevision([]*clientv3.Event{
		testWatchPut(t, "10/1", 5, 10, 1),
		testWatchPut(t, "10/2", 5, 10, 2),
		testWatchDelete("10/1", 6),
	}, testWatchedRecords)
	require.NoError(t, err)
	require.Len(t, batches, 2)
	require.Equal(t, metastore.Revision(5), batches[0].Revision)
	require.Len(t, batches[0].Events, 2)
	first := batches[0].Events[0]
	require.Equal(t, metastore.EventPut, first.Type)
	require.Equal(t, []string{"10", "1"}, first.Coordinate)
	require.Equal(t, int64(1), first.Value.(*model.SegmentChangeGroup).GroupID)

	deleted := batches[1].Events[0]
	require.Equal(t, metastore.EventDelete, deleted.Type)
	require.Equal(t, []string{"10", "1"}, deleted.Coordinate)
	require.Nil(t, deleted.Value)

	tests := []struct {
		name  string
		event *clientv3.Event
	}{
		{"corrupt put", testWatchEvent(mvccpb.PUT, "10/3", 8, []byte("not-json"))},
		{"delete of a prefix key", testWatchEvent(mvccpb.DELETE, "10", 8, nil)},
		{"put under another group's key", testWatchPut(t, "10/3", 8, 10, 4)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := groupByRevision([]*clientv3.Event{tt.event}, testWatchedRecords)
			require.Error(t, err)
		})
	}
}

// TestConsumeWatch covers how a watch ends: by ctx (nil), by fn's error, by
// compaction (kv.ErrCompacted), by a closed channel, by a watch error, and by a
// record that fails to decode.
func TestConsumeWatch(t *testing.T) {
	errStop := errors.New("stop")
	tests := []struct {
		name     string
		resps    []clientv3.WatchResponse
		close    bool
		cancel   bool
		fnErr    error
		wantNil  bool
		wantErr  error // nil: any error
		wantRevs []metastore.Revision
	}{
		{
			name:     "ctx done",
			resps:    []clientv3.WatchResponse{{Events: []*clientv3.Event{testWatchPut(t, "10/1", 5, 10, 1)}}},
			cancel:   true,
			wantNil:  true,
			wantRevs: []metastore.Revision{5},
		},
		{
			name:     "fn error",
			resps:    []clientv3.WatchResponse{{Events: []*clientv3.Event{testWatchPut(t, "10/1", 5, 10, 1)}}},
			fnErr:    errStop,
			wantErr:  errStop,
			wantRevs: []metastore.Revision{5},
		},
		{
			name:    "compacted",
			resps:   []clientv3.WatchResponse{{CompactRevision: 9}},
			wantErr: kv.ErrCompacted,
		},
		{
			name:    "channel closed",
			close:   true,
			wantErr: merr.ErrServiceUnavailable,
		},
		{
			name:  "watch error",
			resps: []clientv3.WatchResponse{{Canceled: true}},
		},
		{
			name: "corrupt record",
			resps: []clientv3.WatchResponse{{Events: []*clientv3.Event{
				testWatchEvent(mvccpb.PUT, "10/1", 5, []byte("not-json")),
			}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wch := make(chan clientv3.WatchResponse, len(tt.resps))
			for _, r := range tt.resps {
				wch <- r
			}
			if tt.close {
				close(wch)
			}
			var revs []metastore.Revision
			err := consumeWatch(ctx, wch, testWatchedRecords, func(b metastore.EventBatch) error {
				revs = append(revs, b.Revision)
				if tt.cancel {
					cancel()
				}
				return tt.fnErr
			})
			require.Equal(t, tt.wantRevs, revs)
			switch {
			case tt.wantNil:
				require.NoError(t, err)
			case tt.wantErr != nil:
				require.True(t, errors.Is(err, tt.wantErr), "%v", err)
			default:
				require.Error(t, err)
			}
		})
	}
}

// TestDecodeRecord_ChecksCoordinate verifies that a record whose decoded
// coordinate differs from the key it is stored under fails closed, naming
// the absolute key.
func TestDecodeRecord_ChecksCoordinate(t *testing.T) {
	g := newTestCatalogGroup()
	value, err := model.MarshalSegmentChangeGroup(g)
	require.NoError(t, err)
	_, err = decodeRecord("by-dev/meta", metastore.SegmentGroupType, scgKey("10/1"), value)
	require.NoError(t, err)
	_, err = decodeRecord("by-dev/meta", metastore.SegmentGroupType, scgKey("10/2"), value)
	require.ErrorIs(t, err, merr.ErrDataIntegrity, "a group stored under another group's key")
	require.ErrorContains(t, err, "by-dev/meta/"+scgKey("10/2"))
	_, err = decodeRecord("by-dev/meta", metastore.SegmentGroupType, scgKey("10/1"), []byte("not a group"))
	require.ErrorContains(t, err, "by-dev/meta/"+scgKey("10/1"))
}

// TestRootPrefix covers mapping walk and watch keys to record keys, whether
// the key is absolute or relative and however the root is spelled.
func TestRootPrefix(t *testing.T) {
	key := scgKey("10/1")
	tests := []struct {
		name, root, key string
	}{
		{"relative key", "by-dev/meta", key},
		{"absolute key", "by-dev/meta", "by-dev/meta/" + key},
		{"root with a trailing slash", "by-dev/meta/", "by-dev/meta/" + key},
		{"no root", "", key},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, key, strings.TrimPrefix(tt.key, rootPrefix(tt.root)))
		})
	}
}

func ignoreBatch(metastore.EventBatch) error { return nil }

// testWatchedRecords maps watch keys under the "root" root path to
// segment change group records.
var testWatchedRecords = watchedRecords{root: "root", prefix: "root/", t: metastore.SegmentGroupType}

func testWatchEvent(typ mvccpb.Event_EventType, id string, rev int64, value []byte) *clientv3.Event {
	return &clientv3.Event{Type: typ, Kv: &mvccpb.KeyValue{
		Key: []byte("root/" + scgKey(id)), ModRevision: rev, Value: value,
	}}
}

// testWatchPut is a put event at id of the group (collectionID, groupID).
func testWatchPut(t *testing.T, id string, rev, collectionID, groupID int64) *clientv3.Event {
	g := newTestCatalogGroup()
	g.CollectionID, g.GroupID = collectionID, groupID
	v, err := model.MarshalSegmentChangeGroup(g)
	require.NoError(t, err)
	return testWatchEvent(mvccpb.PUT, id, rev, v)
}

func testWatchDelete(id string, rev int64) *clientv3.Event {
	return testWatchEvent(mvccpb.DELETE, id, rev, nil)
}

// listTestGroups lists every segment change group the way DataCoord recovery
// does: a typed List, failing on a corrupt record.
func listTestGroups(ctx context.Context, store metastore.GenericCatalog) ([]*model.SegmentChangeGroup, error) {
	listed, err := store.List(ctx, metastore.SegmentGroupType, nil, metastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	groups := make([]*model.SegmentChangeGroup, 0, len(listed.Values))
	for _, v := range listed.Values {
		groups = append(groups, v.(*model.SegmentChangeGroup))
	}
	return groups, nil
}

// groupIDs returns "{collectionID}/{groupID}" of each group.
func groupIDs(values []any) []string {
	ids := make([]string, 0, len(values))
	for _, v := range values {
		g := v.(*model.SegmentChangeGroup)
		ids = append(ids, fmt.Sprintf("%d/%d", g.CollectionID, g.GroupID))
	}
	return ids
}

func committedGroup(v any) bool {
	return v.(*model.SegmentChangeGroup).State == model.SegmentChangeStateCommitted
}

// scgKey is a key below the segment change group root.
func scgKey(rest string) string {
	return "datacoord-meta/segment-change-group/" + rest
}
