// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loadmgr

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadConfigObserverOnlyReportsCommittedChanges(t *testing.T) {
	store, catalog := newTestStore(t)
	var events []bool
	store.RegisterObserver(func(id int64, released bool) {
		require.EqualValues(t, 100, id)
		events = append(events, released)
	})
	cfg := sampleConfig()
	failedSave := mockey.Mock(catalogkv.Catalog.SaveCollection).Return(merr.WrapErrServiceUnavailableMsg("catalog unavailable")).Build()
	t.Cleanup(func() { failedSave.UnPatch() })
	require.Error(t, store.Put(context.Background(), cfg))
	require.Empty(t, events)
	require.Equal(t, 1, failedSave.Times())
	failedSave.UnPatch()
	expectFullSave(t, catalog, 2)
	require.NoError(t, store.Put(context.Background(), cfg))
	require.NoError(t, store.Put(context.Background(), cfg))
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseReplicas).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 100, collection)
		return nil
	}), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseCollection).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 100, collection)
		return nil
	}), 1)
	require.NoError(t, store.Remove(context.Background(), 100))
	require.NoError(t, store.Remove(context.Background(), 100))
	require.Equal(t, []bool{false, false, true}, events)
}
