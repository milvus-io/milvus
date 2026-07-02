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
