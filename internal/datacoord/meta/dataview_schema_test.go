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

package meta

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/dataview"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCompactionSchemaPublication(t *testing.T) {
	m := NewEmptyTestMeta()
	target := &schemapb.CollectionSchema{Version: 2}
	require.NoError(t, m.checkCompactionSchemaPublication(t.Context(), 1, target))
	m.DataViewManager = dataview.NewManager(datacoordkv.NewCatalog(NewMetaMemoryKV(), "", ""), nil)
	require.ErrorIs(t, m.checkCompactionSchemaPublication(t.Context(), 1, target), merr.ErrServiceNotReady)
	m.MetaStore.PutCollection(&CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{Version: 1}})
	_, err := m.DataViewManager.OnCreateCollection(t.Context(), dataview.CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	require.ErrorIs(t, m.checkCompactionSchemaPublication(t.Context(), 1, target), merr.ErrServiceNotReady)
	derived := &schemapb.CollectionSchema{Version: 2, Functions: []*schemapb.FunctionSchema{{OutputFieldIds: []int64{102}}}}
	require.NoError(t, m.checkCompactionSchemaPublication(t.Context(), 1, derived), "materialization must make progress before DDL publication")
	_, err = m.DataViewManager.AdvanceSchema(t.Context(), 1, 2, nil)
	require.NoError(t, err)
	require.NoError(t, m.checkCompactionSchemaPublication(t.Context(), 1, target))
	require.NoError(t, m.checkCompactionSchemaPublication(t.Context(), 1, &schemapb.CollectionSchema{Version: 1}), "old jobs retain their captured schema")
}
