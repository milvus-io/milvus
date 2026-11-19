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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type adoptionKV struct {
	*dataViewMetaKV
	failSave  func(string) error
	uncertain bool
	commits   int
	maxValue  int
}

func (m *adoptionKV) MaxTxnOps() int { return 2 }
func (m *adoptionKV) Load(_ context.Context, key string) (string, error) {
	v, ok := m.values[key]
	if !ok {
		return "", merr.WrapErrIoKeyNotFound(key)
	}
	return v, nil
}

func (m *adoptionKV) Save(ctx context.Context, key, value string) error {
	if m.failSave != nil {
		if err := m.failSave(key); err != nil {
			return err
		}
	}
	m.maxValue = max(m.maxValue, len(value))
	return m.dataViewMetaKV.Save(ctx, key, value)
}

func (m *adoptionKV) MultiSave(ctx context.Context, values map[string]string) error {
	requireSize := len(values)
	if requireSize > 2 {
		return merr.WrapErrServiceInternalMsg("too many commit ops")
	}
	if len(values) == 2 {
		m.commits++
	}
	for k, v := range values {
		m.maxValue = max(m.maxValue, len(v))
		m.values[k] = v
	}
	if m.uncertain {
		return merr.WrapErrServiceUnavailableMsg("commit response lost")
	}
	return nil
}

func (m *adoptionKV) MultiSaveAndRemove(ctx context.Context, values map[string]string, remove []string, preds ...predicates.Predicate) error {
	for _, pred := range preds {
		value, exists := m.values[pred.Key()]
		if pred.Target() == predicates.PredTargetKeyExistence && !exists {
			continue
		}
		if !exists || !pred.IsTrue(value) {
			return merr.WrapErrIoFailedReason("transaction predicate failed")
		}
	}
	if err := m.MultiSave(ctx, values); err != nil {
		return err
	}
	for _, k := range remove {
		delete(m.values, k)
	}
	return nil
}

func adoptionFixture(n int) *model.SplitAdoption {
	record := &model.SplitAdoption{TaskID: 7, CollectionID: 100, Source: "s"}
	for i := 1; i <= n; i++ {
		record.Moves = append(record.Moves, model.SplitAdoptionMove{SegmentID: int64(i), PartitionID: 3, Target: "t", TransformStart: 100, ManifestPath: strings.Repeat("m", 1024)})
	}
	return record
}

func TestStagedAdoptionPagesInvisibleAndAtomicUncertainCommit(t *testing.T) {
	ctx := context.Background()
	store := &adoptionKV{dataViewMetaKV: newDataViewMetaKV()}
	catalog := NewCatalog(store, "", "")
	failed := false
	store.failSave = func(key string) error {
		if strings.Contains(key, "/pages/") && !failed {
			failed = true
			return merr.WrapErrServiceUnavailableMsg("page write interrupted")
		}
		return nil
	}
	_, err := catalog.PrepareSplitAdoption(ctx, adoptionFixture(2048))
	require.Error(t, err)
	root, err := catalog.LoadSplitAdoption(ctx, 100, 7)
	require.NoError(t, err)
	require.Equal(t, "Preparing", root.State)
	rows := []*datapb.SegmentInfo{{ID: 1, CollectionID: 100, PartitionID: 3, InsertChannel: "s", SplitTarget: "t"}}
	require.NoError(t, catalog.applyCommittedAdoptions(ctx, 100, rows))
	require.Equal(t, "s", rows[0].GetInsertChannel())
	store.failSave = nil
	root, err = catalog.PrepareSplitAdoption(ctx, adoptionFixture(2048))
	require.NoError(t, err)
	require.Equal(t, "Prepared", root.State)
	require.NoError(t, catalog.applyCommittedAdoptions(ctx, 100, rows))
	require.Equal(t, "s", rows[0].GetInsertChannel())
	// Both the move set and DataView exceed the previous all-txn payload cap.
	view := &viewpb.DataViewOfCollection{CollectionId: 100, ShardSetEpoch: 3, DataVersion: &viewpb.DataVersion{CompactVersion: 2}, Shards: []*viewpb.DataViewOfShard{{Vchannel: strings.Repeat("t", 2<<20)}}}
	store.uncertain = true
	require.NoError(t, catalog.CommitSplitAdoption(ctx, root, view))
	require.Equal(t, 1, store.commits)
	require.LessOrEqual(t, store.maxValue, metadataRootBytes)
	require.NoError(t, catalog.applyCommittedAdoptions(ctx, 100, rows))
	require.Equal(t, "t", rows[0].GetInsertChannel())
	require.EqualValues(t, 100, rows[0].GetTransformStartAfterTimetick())
	views, err := catalog.ListDataViews(ctx, 100)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.True(t, proto.Equal(view, views[0]))
	// A newer canonical target cursor is authoritative; recovery cannot reset it.
	rows[0].TransformStartAfterTimetick = 400
	rows[0].ManifestPath = "newer"
	require.NoError(t, catalog.applyCommittedAdoptions(ctx, 100, rows))
	require.EqualValues(t, 400, rows[0].GetTransformStartAfterTimetick())
	require.Equal(t, "newer", rows[0].GetManifestPath())
	require.NoError(t, catalog.applyCommittedAdoptions(ctx, 100, nil), "a later drop must not resurrect a row")
	store.uncertain = false
	require.NoError(t, catalog.SaveSplitAdoptionState(ctx, root, "Cleaning"))
	require.NoError(t, catalog.applyCommittedAdoptions(ctx, 100, rows))
	require.NoError(t, catalog.CanonicalizeSplitAdoptionSegment(ctx, rows[0]))
	require.NoError(t, catalog.SaveSplitAdoptionState(ctx, root, "Done"))
	loaded, err := catalog.LoadSplitAdoption(ctx, 100, 7)
	require.NoError(t, err)
	require.Equal(t, "Done", loaded.State)
	require.Len(t, loaded.Moves, 2048)
	require.NoError(t, catalog.DropDataViews(ctx, 100))
	for key := range store.values {
		require.False(t, strings.Contains(key, "/pages/") || strings.Contains(key, "/split-adoption/") || strings.HasPrefix(key, DataViewPrefix+"/"))
	}
}

func TestPagedMetadataFailsClosedOnMissingAndCorruptPages(t *testing.T) {
	ctx := context.Background()
	store := &adoptionKV{dataViewMetaKV: newDataViewMetaKV()}
	catalog := NewCatalog(store, "", "")
	root, err := catalog.preparePagedValue(ctx, 100, []byte(strings.Repeat("a", metadataPageBytes+1)))
	require.NoError(t, err)
	for key := range store.values {
		store.values[key] = "corrupt"
		break
	}
	_, err = catalog.readPagedValue(ctx, root)
	require.Error(t, err)
	_, err = catalog.readPagedValue(ctx, pagedValueMagic+`{"Hash":"0000000000000000000000000000000000000000000000000000000000000000","Bytes":9223372036854775807,"Pages":1,"CollectionID":100}`)
	require.Error(t, err)
}

func TestLargeNeverSplitDataViewKeepsLegacyEncoding(t *testing.T) {
	ctx := context.Background()
	store := &adoptionKV{dataViewMetaKV: newDataViewMetaKV()}
	catalog := NewCatalog(store, "", "")
	view := &viewpb.DataViewOfCollection{CollectionId: 100, ShardSetEpoch: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, Shards: []*viewpb.DataViewOfShard{{Vchannel: strings.Repeat("ordinary", 1<<16)}}}
	require.NoError(t, catalog.SaveDataView(ctx, view))
	require.Len(t, store.values, 1)
	value := store.values[buildDataViewVersionKey(100, 1, 0)]
	require.False(t, strings.HasPrefix(value, pagedValueMagic))
	decoded := &viewpb.DataViewOfCollection{}
	require.NoError(t, proto.Unmarshal([]byte(value), decoded))
	require.True(t, proto.Equal(view, decoded))
	view.ShardSetEpoch = 2
	view.DataVersion.StreamingVersion = 2
	require.NoError(t, catalog.SaveDataView(ctx, view))
	require.True(t, strings.HasPrefix(store.values[buildDataViewVersionKey(100, 2, 0)], pagedValueMagic))
}

// etcd/TiKV Load accepts a logical key while WalkWithPrefix callbacks contain
// the configured backend root. This fake deliberately preserves that split.
type prefixedAdoptionKV struct{ *adoptionKV }

func (m *prefixedAdoptionKV) WalkWithPrefix(ctx context.Context, prefix string, n int, fn func([]byte, []byte) error) error {
	return m.adoptionKV.WalkWithPrefix(ctx, prefix, n, func(key, value []byte) error { return fn([]byte("/by-dev/meta/"+string(key)), value) })
}

func TestSplitAdoptionUsesLogicalLoadAndPrefixedRecoveryWalk(t *testing.T) {
	ctx := context.Background()
	store := &prefixedAdoptionKV{adoptionKV: &adoptionKV{dataViewMetaKV: newDataViewMetaKV()}}
	catalog := NewCatalog(store, "", "/by-dev/meta")
	record, err := catalog.PrepareSplitAdoption(ctx, adoptionFixture(1))
	require.NoError(t, err)
	loaded, err := catalog.LoadSplitAdoption(ctx, 100, 7)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, "Prepared", loaded.State)
	view := &viewpb.DataViewOfCollection{CollectionId: 100, ShardSetEpoch: 3, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}}
	require.NoError(t, catalog.CommitSplitAdoption(ctx, record, view))
	rows := []*datapb.SegmentInfo{{ID: 1, CollectionID: 100, PartitionID: 3, InsertChannel: "s", SplitTarget: "t"}}
	require.NoError(t, catalog.applyCommittedAdoptions(ctx, 100, rows))
	require.Equal(t, "t", rows[0].GetInsertChannel())
	require.False(t, adoptionWalkKeyMatches("/by-dev/meta/foreign-datacoord-meta/split-adoption/100/7", adoptionKey(100, 7)))
}

// Models an accepted transaction whose execution occurs after its RPC failed.
// A successful read alone cannot exclude this delayed execution.
type delayedAdoptionKV struct {
	*adoptionKV
	pending           func() error
	originalWins      bool
	loseFenceResponse bool
}

func (m *delayedAdoptionKV) MultiSaveAndRemove(ctx context.Context, values map[string]string, removals []string, preds ...predicates.Predicate) error {
	if len(values) == 2 && m.pending == nil {
		m.pending = func() error { return m.adoptionKV.MultiSaveAndRemove(context.Background(), values, removals, preds...) }
		return merr.WrapErrServiceUnavailableMsg("transaction accepted; execution delayed")
	}
	if len(values) == 1 && m.pending != nil && m.originalWins {
		m.originalWins = false
		if err := m.pending(); err != nil {
			return err
		}
	}
	err := m.adoptionKV.MultiSaveAndRemove(ctx, values, removals, preds...)
	if err == nil && len(values) == 1 && m.pending != nil && m.loseFenceResponse {
		m.loseFenceResponse = false
		return merr.WrapErrServiceUnavailableMsg("fence committed; response lost")
	}
	return err
}

func TestAdoptionUncertainPreparedReadFencesDelayedTransactionAndABA(t *testing.T) {
	for _, originalWins := range []bool{false, true} {
		name := "fence-wins-with-lost-response"
		if originalWins {
			name = "original-wins-fence-race"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := &delayedAdoptionKV{adoptionKV: &adoptionKV{dataViewMetaKV: newDataViewMetaKV()}, originalWins: originalWins, loseFenceResponse: true}
			catalog := NewCatalog(store, "", "")
			record, err := catalog.PrepareSplitAdoption(ctx, adoptionFixture(1))
			require.NoError(t, err)
			firstAttempt := record.AttemptID
			view := &viewpb.DataViewOfCollection{CollectionId: 100, ShardSetEpoch: 3, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}}
			err = catalog.CommitSplitAdoption(ctx, record, view)
			if originalWins {
				require.NoError(t, err)
				require.Equal(t, "Committed", record.State)
				return
			}
			require.Error(t, err)
			durable, err := catalog.LoadSplitAdoption(ctx, 100, 7)
			require.NoError(t, err)
			require.Equal(t, "Preparing", durable.State)
			require.NotContains(t, store.values, buildDataViewVersionKey(100, 1, 1))
			// Recreating the same move set must never recreate an old commit's guard.
			record, err = catalog.PrepareSplitAdoption(ctx, adoptionFixture(1))
			require.NoError(t, err)
			require.NotEqual(t, firstAttempt, record.AttemptID)
			require.Error(t, store.pending(), "late original transaction must fail its old attempt guard")
			require.NotContains(t, store.values, buildDataViewVersionKey(100, 1, 1))
			require.NoError(t, catalog.CommitSplitAdoption(ctx, record, view))
		})
	}
}

// A restarted coordinator can read Prepared while the former coordinator's
// accepted commit is still pending. Reprepare must compare the root it read.
type adoptionPrepareRaceKV struct {
	*adoptionKV
	before func() error
}

func (m *adoptionPrepareRaceKV) MultiSaveAndRemove(ctx context.Context, values map[string]string, removals []string, preds ...predicates.Predicate) error {
	if m.before != nil {
		before := m.before
		m.before = nil
		if err := before(); err != nil {
			return err
		}
	}
	return m.adoptionKV.MultiSaveAndRemove(ctx, values, removals, preds...)
}

func TestAdoptionReprepareCannotOverwriteLateCommittedAttempt(t *testing.T) {
	ctx := context.Background()
	store := &adoptionPrepareRaceKV{adoptionKV: &adoptionKV{dataViewMetaKV: newDataViewMetaKV()}}
	catalog := NewCatalog(store, "", "")
	record, err := catalog.PrepareSplitAdoption(ctx, adoptionFixture(1))
	require.NoError(t, err)
	prepared, err := encodeAdoption(record)
	require.NoError(t, err)
	committed := *record
	committed.State = "Committed"
	root, err := encodeAdoption(&committed)
	require.NoError(t, err)
	key := adoptionKey(100, 7)
	// Execute the prior process's conditional commit between the new process's
	// read and its first conditional Preparing write.
	store.before = func() error {
		return store.adoptionKV.MultiSaveAndRemove(ctx, map[string]string{key: root, buildDataViewVersionKey(100, 1, 1): "committed-view"}, nil, predicates.ValueEqual(key, prepared))
	}
	_, err = catalog.PrepareSplitAdoption(ctx, adoptionFixture(1))
	require.Error(t, err)
	durable, err := catalog.LoadSplitAdoption(ctx, 100, 7)
	require.NoError(t, err)
	require.Equal(t, "Committed", durable.State)
	require.Equal(t, record.AttemptID, durable.AttemptID)
	resumed, err := catalog.PrepareSplitAdoption(ctx, adoptionFixture(1))
	require.NoError(t, err)
	require.Equal(t, durable.AttemptID, resumed.AttemptID)
	require.Equal(t, "Committed", resumed.State)
}
