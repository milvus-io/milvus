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

//go:build test

package index

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type FailBackfillCatalogKV struct {
	*meta.MetaMemoryKV
	FailAtomicUpdate bool

	updateSizesMu     sync.Mutex
	AtomicUpdateSizes []int
}

func (kv *FailBackfillCatalogKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	kv.updateSizesMu.Lock()
	kv.AtomicUpdateSizes = append(kv.AtomicUpdateSizes, len(saves)+len(removals))
	kv.updateSizesMu.Unlock()
	if kv.FailAtomicUpdate {
		return merr.WrapErrIoFailedReason("injected backfill catalog failure")
	}
	return kv.MetaMemoryKV.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

func IndexManifestPublished(t *testing.T, im *IndexMeta, buildID int64) bool {
	t.Helper()
	record, ok := im.GetIndexJob(buildID)
	require.True(t, ok)
	return record.ManifestPublished
}

func WithManifestIndexBackfillEnabled(t *testing.T, enabled bool) {
	t.Helper()
	key := Params.DataCoordCfg.ManifestIndexBackfillEnabled.Key
	previous := Params.DataCoordCfg.ManifestIndexBackfillEnabled.GetValue()
	Params.Save(key, fmt.Sprintf("%t", enabled))
	t.Cleanup(func() { Params.Save(key, previous) })
}

func SeedBackfillIndexDefinition(t *testing.T, m *meta.Meta) {
	t.Helper()
	if m.IndexCatalog.(*IndexMeta).IsIndexExist(meta.RestartCollID, meta.RestartIndexID) {
		return
	}
	require.NoError(t, m.IndexCatalog.(*IndexMeta).CreateIndex(context.TODO(), &model.Index{
		CollectionID: meta.RestartCollID,
		FieldID:      meta.RestartFieldID,
		IndexID:      meta.RestartIndexID,
		IndexName:    "vec_idx",
		TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
	}))
}

func SeedLegacyBackfillRecord(t *testing.T, m *meta.Meta, segmentID, buildID int64) {
	t.Helper()
	ctx := context.TODO()
	SeedBackfillIndexDefinition(t, m)
	require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   meta.RestartCollID,
		PartitionID:    RestartPartID,
		State:          commonpb.SegmentState_Flushed,
		NumOfRows:      1000,
		StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath(
			fmt.Sprintf("/tmp/test-backfill/insert_log/%d/%d/%d", meta.RestartCollID, RestartPartID, segmentID), 1),
	})))
	require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndex(ctx, &model.SegmentIndex{
		CollectionID:          meta.RestartCollID,
		PartitionID:           RestartPartID,
		SegmentID:             segmentID,
		NumRows:               1000,
		IndexID:               meta.RestartIndexID,
		BuildID:               buildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))
	require.NoError(t, m.IndexCatalog.(*IndexMeta).FinishTask(&workerpb.IndexTaskInfo{
		BuildID:               buildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        4096,
		MemSize:               8192,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))
}

func SeedLegacyBackfillIndexes(t *testing.T, m *meta.Meta, count int) []*model.SegmentIndex {
	t.Helper()
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
	first, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	records := []*model.SegmentIndex{first}
	for offset := 1; offset < count; offset++ {
		definition := model.CloneIndex(m.IndexCatalog.(*IndexMeta).GetIndexesForCollection(meta.RestartCollID, "vec_idx")[0])
		definition.IndexID += int64(offset)
		definition.FieldID += int64(offset)
		definition.IndexName = fmt.Sprintf("vec_idx_%d", offset)
		require.NoError(t, m.IndexCatalog.(*IndexMeta).CreateIndex(context.TODO(), definition))
		record := model.CloneSegmentIndex(first)
		record.IndexID = definition.IndexID
		record.BuildID += int64(offset)
		require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndex(context.TODO(), record))
		records = append(records, record)
	}
	return records
}
