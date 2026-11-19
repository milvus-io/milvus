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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSplitHistoryBoundaryPersistence(t *testing.T) {
	ctx := t.Context()
	kv := &adoptionKV{dataViewMetaKV: newDataViewMetaKV()}
	stored := kv.values
	task := &datapb.SplitShardTask{TaskId: 7, CollectionId: 100, SwitchTimeTick: 100}
	target := &datapb.SplitShardTaskTarget{Vchannel: "p_100v1", GenesisTimeTick: 110}
	catalog := NewCatalog(kv, "", "")
	f, err := catalog.LoadSplitHistoryBoundary(ctx, task, target)
	require.NoError(t, err)
	require.Zero(t, f)
	require.NoError(t, catalog.SaveSplitHistoryBoundary(ctx, task, target, 201))
	// Recovery creates a new catalog and reuses exactly the persisted F.
	recovered := NewCatalog(kv, "", "")
	f, err = recovered.LoadSplitHistoryBoundary(ctx, task, target)
	require.NoError(t, err)
	require.EqualValues(t, 201, f)
	require.NoError(t, recovered.SaveSplitHistoryBoundary(ctx, task, target, 201))
	require.ErrorIs(t, recovered.SaveSplitHistoryBoundary(ctx, task, target, 202), merr.ErrDataIntegrity)
	stored[splitHistoryKey(task, target)] = "corrupt"
	_, err = recovered.LoadSplitHistoryBoundary(ctx, task, target)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	stored[splitHistoryKey(task, target)] = "100"
	_, err = recovered.LoadSplitHistoryBoundary(ctx, task, target)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, recovered.DropSplitShardTask(ctx, task))
	f, err = recovered.LoadSplitHistoryBoundary(ctx, task, target)
	require.NoError(t, err)
	require.Zero(t, f)
	require.Empty(t, stored, "task drop removes companions as well")
}

type delayedBoundaryKV struct {
	*adoptionKV
	pending func() error
}

func (m *delayedBoundaryKV) MultiSaveAndRemove(ctx context.Context, values map[string]string, removals []string, preds ...predicates.Predicate) error {
	if m.pending == nil {
		m.pending = func() error { return m.adoptionKV.MultiSaveAndRemove(context.Background(), values, removals, preds...) }
		return merr.WrapErrServiceUnavailableMsg("boundary write accepted; execution delayed")
	}
	return m.adoptionKV.MultiSaveAndRemove(ctx, values, removals, preds...)
}

func TestSplitHistoryBoundaryDelayedCreationCannotReplaceEstablishedF(t *testing.T) {
	ctx := context.Background()
	store := &delayedBoundaryKV{adoptionKV: &adoptionKV{dataViewMetaKV: newDataViewMetaKV()}}
	catalog := NewCatalog(store, "", "")
	task := &datapb.SplitShardTask{TaskId: 7, CollectionId: 100, SwitchTimeTick: 100}
	target := &datapb.SplitShardTaskTarget{Vchannel: "p_100v1", GenesisTimeTick: 110}
	require.Error(t, catalog.SaveSplitHistoryBoundary(ctx, task, target, 201))
	require.NoError(t, catalog.SaveSplitHistoryBoundary(ctx, task, target, 202))
	require.Error(t, store.pending())
	f, err := catalog.LoadSplitHistoryBoundary(ctx, task, target)
	require.NoError(t, err)
	require.EqualValues(t, 202, f)
}
