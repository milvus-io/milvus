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

package segments

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

func addIndexAction(fieldID int64, params []*commonpb.KeyValuePair) *querypb.UpdateIndexRequest_Action {
	return &querypb.UpdateIndexRequest_Action{
		Op: &querypb.UpdateIndexRequest_Action_AddIndexRequest{
			AddIndexRequest: &querypb.UpdateIndexRequest_AddIndex{
				IndexInfo: &indexpb.IndexInfo{FieldID: fieldID, IndexParams: params},
			},
		},
	}
}

func TestMergeIndexAction(t *testing.T) {
	t.Run("nil action returns nil", func(t *testing.T) {
		assert.Nil(t, mergeIndexAction(nil, nil))
	})

	t.Run("add with nil index info returns nil", func(t *testing.T) {
		action := &querypb.UpdateIndexRequest_Action{
			Op: &querypb.UpdateIndexRequest_Action_AddIndexRequest{
				AddIndexRequest: &querypb.UpdateIndexRequest_AddIndex{},
			},
		}
		assert.Nil(t, mergeIndexAction(nil, action))
	})

	t.Run("drop index is a no-op (deferred to V2)", func(t *testing.T) {
		action := &querypb.UpdateIndexRequest_Action{
			Op: &querypb.UpdateIndexRequest_Action_DropIndexRequest{
				DropIndexRequest: &querypb.UpdateIndexRequest_DropIndex{IndexId: 1},
			},
		}
		assert.Nil(t, mergeIndexAction(nil, action))
	})

	t.Run("add on nil base seeds one field", func(t *testing.T) {
		m := mergeIndexAction(nil, addIndexAction(100, nil))
		require.NotNil(t, m)
		require.Len(t, m.GetIndexMetas(), 1)
		assert.EqualValues(t, 100, m.GetIndexMetas()[0].GetFieldID())
	})

	t.Run("add new field appends and does not mutate base", func(t *testing.T) {
		base := &segcorepb.CollectionIndexMeta{
			IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 100}},
		}
		m := mergeIndexAction(base, addIndexAction(200, nil))
		require.Len(t, m.GetIndexMetas(), 2)
		// base is cloned, not mutated
		assert.Len(t, base.GetIndexMetas(), 1)
	})

	t.Run("add existing field upserts (replaces) its params", func(t *testing.T) {
		base := &segcorepb.CollectionIndexMeta{
			IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 100, IndexName: "old"}},
		}
		m := mergeIndexAction(base, addIndexAction(100, []*commonpb.KeyValuePair{{Key: "k1", Value: "1.2"}}))
		require.Len(t, m.GetIndexMetas(), 1)
		assert.EqualValues(t, 100, m.GetIndexMetas()[0].GetFieldID())
		require.Len(t, m.GetIndexMetas()[0].GetIndexParams(), 1)
		assert.Equal(t, "1.2", m.GetIndexMetas()[0].GetIndexParams()[0].GetValue())
	})
}

func TestMergeIndexActions(t *testing.T) {
	dropAction := func(indexID int64) *querypb.UpdateIndexRequest_Action {
		return &querypb.UpdateIndexRequest_Action{
			Op: &querypb.UpdateIndexRequest_Action_DropIndexRequest{
				DropIndexRequest: &querypb.UpdateIndexRequest_DropIndex{IndexId: indexID},
			},
		}
	}

	t.Run("no actions returns nil", func(t *testing.T) {
		assert.Nil(t, mergeIndexActions(nil, nil))
	})

	t.Run("all no-op actions return nil", func(t *testing.T) {
		base := &segcorepb.CollectionIndexMeta{IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 100}}}
		assert.Nil(t, mergeIndexActions(base, []*querypb.UpdateIndexRequest_Action{dropAction(1), dropAction(2)}))
	})

	// The core fix: every field bound to ONE DDL folds into a single meta in one pass.
	t.Run("folds every field of the DDL into one meta", func(t *testing.T) {
		m := mergeIndexActions(nil, []*querypb.UpdateIndexRequest_Action{
			addIndexAction(101, nil),
			addIndexAction(102, nil),
			addIndexAction(103, nil),
		})
		require.NotNil(t, m)
		require.Len(t, m.GetIndexMetas(), 3)
		assert.True(t, hasFieldIndexMeta(m, 101))
		assert.True(t, hasFieldIndexMeta(m, 102))
		assert.True(t, hasFieldIndexMeta(m, 103))
	})

	t.Run("keeps added fields alongside base and does not mutate base", func(t *testing.T) {
		base := &segcorepb.CollectionIndexMeta{IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 100}}}
		m := mergeIndexActions(base, []*querypb.UpdateIndexRequest_Action{
			addIndexAction(101, nil),
			dropAction(1), // no-op (deferred to V2), must not discard the surrounding adds
			addIndexAction(102, nil),
		})
		require.NotNil(t, m)
		require.Len(t, m.GetIndexMetas(), 3)
		assert.True(t, hasFieldIndexMeta(m, 100))
		assert.True(t, hasFieldIndexMeta(m, 101))
		assert.True(t, hasFieldIndexMeta(m, 102))
		assert.Len(t, base.GetIndexMetas(), 1)
	})
}

func hasFieldIndexMeta(meta *segcorepb.CollectionIndexMeta, fieldID int64) bool {
	for _, m := range meta.GetIndexMetas() {
		if m.GetFieldID() == fieldID {
			return true
		}
	}
	return false
}

func TestUnionIndexMeta(t *testing.T) {
	t.Run("nil base returns add", func(t *testing.T) {
		add := &segcorepb.CollectionIndexMeta{IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 100}}}
		assert.Same(t, add, unionIndexMeta(nil, add))
	})

	t.Run("nil add returns base", func(t *testing.T) {
		base := &segcorepb.CollectionIndexMeta{IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 100}}}
		assert.Same(t, base, unionIndexMeta(base, nil))
	})

	t.Run("keeps base-only fields and applies add's field (never drop)", func(t *testing.T) {
		base := &segcorepb.CollectionIndexMeta{IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 100}, {FieldID: 200}}}
		add := &segcorepb.CollectionIndexMeta{IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 300}}} // missing 100, 200
		out := unionIndexMeta(base, add)
		assert.True(t, hasFieldIndexMeta(out, 100), "base-only field 100 must survive")
		assert.True(t, hasFieldIndexMeta(out, 200), "base-only field 200 must survive")
		assert.True(t, hasFieldIndexMeta(out, 300), "add's field 300 must be present")
		assert.Len(t, base.GetIndexMetas(), 2) // base not mutated
	})

	t.Run("upsert replaces on FieldID conflict", func(t *testing.T) {
		base := &segcorepb.CollectionIndexMeta{IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 100, IndexName: "old"}}}
		add := &segcorepb.CollectionIndexMeta{IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 100, IndexName: "new"}}}
		out := unionIndexMeta(base, add)
		require.Len(t, out.GetIndexMetas(), 1)
		assert.Equal(t, "new", out.GetIndexMetas()[0].GetIndexName())
	})

	t.Run("maxIndexRowCount taken from add when set", func(t *testing.T) {
		base := &segcorepb.CollectionIndexMeta{MaxIndexRowCount: 100}
		add := &segcorepb.CollectionIndexMeta{MaxIndexRowCount: 1024}
		assert.EqualValues(t, 1024, unionIndexMeta(base, add).GetMaxIndexRowCount())
	})
}

// TestUpdateIndex exercises collectionManager.UpdateIndex end-to-end against a real
// cgo-backed Collection (built by the suite's SetupTest PutOrRef): the monotonic
// version guard, the merge+advance path (which pushes into ccollection), the
// idempotent current-meta return for stale/no-op requests, and the not-found error.
func (s *CollectionManagerSuite) TestUpdateIndex() {
	schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
	// A real, cgo-valid IndexInfo for the collection's vector field so the merged
	// meta round-trips through ccollection.UpdateIndexMeta without a parse error.
	infos := mock_segcore.GenTestIndexInfoList(1, schema)
	s.Require().NotEmpty(infos)
	vecInfo := infos[0]

	addReq := func(barrierTs uint64, info *indexpb.IndexInfo) *querypb.UpdateIndexRequest {
		return &querypb.UpdateIndexRequest{
			CollectionID: 1,
			Actions: []*querypb.UpdateIndexRequest_Action{
				{
					Op: &querypb.UpdateIndexRequest_Action_AddIndexRequest{
						AddIndexRequest: &querypb.UpdateIndexRequest_AddIndex{IndexInfo: info},
					},
				},
			},
			IndexBarrierTs: barrierTs,
		}
	}

	s.Run("newer_version_merges_and_advances", func() {
		meta, version, err := s.cm.UpdateIndex(1, addReq(100, vecInfo))
		s.NoError(err)
		s.Require().NotNil(meta)
		s.Equal(uint64(100), version)
		s.Equal(uint64(100), s.cm.Get(1).indexMetaVersion.Load())
		found := false
		for _, m := range meta.GetIndexMetas() {
			if m.GetFieldID() == vecInfo.GetFieldID() {
				found = true
			}
		}
		s.True(found, "merged meta must contain the added field's index")
	})

	s.Run("stale_version_returns_current_meta_without_advancing", func() {
		cur := s.cm.Get(1).indexMetaVersion.Load()
		s.Require().NotZero(cur)
		meta, version, err := s.cm.UpdateIndex(1, addReq(cur-1, vecInfo))
		s.NoError(err)
		// Stale returns the CURRENT meta+version so the caller can idempotently re-fan.
		s.NotNil(meta)
		s.Equal(cur, version)
		s.Equal(cur, s.cm.Get(1).indexMetaVersion.Load())
	})

	s.Run("newer_version_but_noop_action_returns_current_without_advancing", func() {
		cur := s.cm.Get(1).indexMetaVersion.Load()
		// DropIndex merges to nil (deferred to V2), so even a newer barrier must not
		// advance the version; the current meta is returned for idempotent re-fan.
		req := &querypb.UpdateIndexRequest{
			CollectionID: 1,
			Actions: []*querypb.UpdateIndexRequest_Action{
				{
					Op: &querypb.UpdateIndexRequest_Action_DropIndexRequest{
						DropIndexRequest: &querypb.UpdateIndexRequest_DropIndex{IndexId: 1},
					},
				},
			},
			IndexBarrierTs: cur + 10,
		}
		meta, version, err := s.cm.UpdateIndex(1, req)
		s.NoError(err)
		s.NotNil(meta)
		s.Equal(cur, version)
		s.Equal(cur, s.cm.Get(1).indexMetaVersion.Load())
	})

	s.Run("collection_not_found", func() {
		meta, version, err := s.cm.UpdateIndex(999, addReq(100, vecInfo))
		s.Error(err)
		s.Nil(meta)
		s.Zero(version)
	})
}

// TestPutOrRefUnionKeepsDDLSyncedField: PutOrRef (LoadSegments timeline) unions instead of
// replacing, so an out-of-order stale load cannot roll back an index already synced by the
// WAL-consumed add-field DDL (UpdateIndex timeline).
func (s *CollectionManagerSuite) TestPutOrRefUnionKeepsDDLSyncedField() {
	schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
	const ddlFieldID = int64(9999)

	// DDL syncs ddlFieldID's index into the merge base.
	_, _, err := s.cm.UpdateIndex(1, &querypb.UpdateIndexRequest{
		CollectionID: 1,
		Actions: []*querypb.UpdateIndexRequest_Action{
			{
				Op: &querypb.UpdateIndexRequest_Action_AddIndexRequest{
					AddIndexRequest: &querypb.UpdateIndexRequest_AddIndex{
						IndexInfo: &indexpb.IndexInfo{CollectionID: 1, FieldID: ddlFieldID, IndexID: 42, IndexName: "ddl_idx"},
					},
				},
			},
		},
		IndexBarrierTs: 100,
	})
	s.Require().NoError(err)
	s.Require().True(hasFieldIndexMeta(s.cm.Get(1).indexMeta.Load(), ddlFieldID))

	// A stale load whose index list predates the DDL (missing ddlFieldID).
	staleMeta := mock_segcore.GenTestIndexMeta(1, schema)
	s.Require().False(hasFieldIndexMeta(staleMeta, ddlFieldID))
	err = s.cm.PutOrRef(1, schema, staleMeta, &querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection})
	s.Require().NoError(err)

	// Union must not roll back the DDL-synced field.
	s.True(hasFieldIndexMeta(s.cm.Get(1).indexMeta.Load(), ddlFieldID),
		"stale load must not roll back the DDL-synced field")
}
