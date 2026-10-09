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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
)

func TestIndexGCManifestReadinessDeletedRecordWinsOnReload(t *testing.T) {
	m := setupManifestReloadMeta(t)
	mockReloadManifestEntry(t, 5100)
	require.NoError(t, m.indexMeta.AddSegmentIndex(context.Background(), &model.SegmentIndex{
		CollectionID: 100, PartitionID: 10, SegmentID: 5001, IndexID: 500,
		BuildID: 5100, IndexState: commonpb.IndexState_Finished,
		IndexFileKeys: []string{"from-etcd"},
	}))
	require.NoError(t, m.indexMeta.DeleteTask(5100))
	var err error
	m.indexMeta, err = newIndexMeta(context.Background(), m.catalog, []int64{100})
	require.NoError(t, err)
	require.NoError(t, m.reloadSegmentIndexesFromManifests(context.Background()))
	records := m.indexMeta.GetAllSegmentIndexes(5001)
	require.Len(t, records, 1)
	require.True(t, records[0].IsDeleted, "persisted GC tombstone wins over the stale Finished manifest entry")
	require.Equal(t, []string{"from-etcd"}, records[0].IndexFileKeys)
	require.Empty(t, m.indexMeta.GetSegmentIndexes(100, 5001))
	require.Nil(t, m.indexMeta.getSegmentsIndexStates(100, []int64{5001})[5001][500])
}
