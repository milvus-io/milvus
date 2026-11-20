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

package copysegment

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestSummaryCopyPublishesInventoryWithoutBuildRecords(t *testing.T) {
	t.Run("reported memory size", func(t *testing.T) {
		testSummaryCopyPublishesInventory(t, proto.Uint64(256), 256)
	})
	t.Run("omitted memory size", func(t *testing.T) {
		testSummaryCopyPublishesInventory(t, nil, 64)
	})
}

func testSummaryCopyPublishesInventory(t *testing.T, reportedMemSize *uint64, expectedMemSize uint64) {
	t.Helper()
	fixture := &CopySegmentTaskSuite{}
	fixture.SetT(t)
	task, copies, mt, im, resp := fixture.newCopiedManifestReadBackFixture()
	require.NoError(t, im.UseSegmentSummaries(mt.MetaStore))
	result := resp.GetSegmentResults()[0]
	result.ManifestIndexRewritten = proto.Bool(true)
	result.ManifestIndexBuildIds = []int64{9001}
	result.IndexInfos[9001].IndexId = 300
	result.IndexInfos[9001].IndexType = "HNSW"
	result.IndexInfos[9001].Version = 4
	result.IndexInfos[9001].IndexSize = 64
	result.IndexInfos[9001].MemSize = reportedMemSize
	result.IndexInfos[9001].CurrentIndexVersion = 7
	result.IndexInfos[9001].CurrentScalarIndexVersion = 2
	result.IndexInfos[9001].IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	require.NoError(t, im.CreateIndex(t.Context(), &model.Index{CollectionID: 100, IndexID: 200, FieldID: 102, IndexName: "scalar_idx"}))
	result.IndexInfos[9002] = &datapb.VectorScalarIndexInfo{IndexId: 200, BuildId: 9002, FieldId: 102, IndexName: "scalar_idx"}
	entry := packed.ManifestIndexInfo{
		IndexID: 300, FieldID: 101, BuildID: 9001, IndexVersion: 4, IndexName: "vec_idx", IndexType: "HNSW",
		Path: "files/insert_log/100/10/2001/_index/9001/4", IndexFileKeys: []string{"index.bin"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}
	meta.MockManifestIndexSubmissions(t)
	read := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{entry}, nil).Build()
	defer read.UnPatch()
	require.NoError(t, SyncCopySegmentTask(t.Context(), task, resp, copies, mt, im))
	segment := mt.GetSegment(context.Background(), 2001)
	require.Equal(t, commonpb.SegmentState_Flushed, segment.GetState())
	require.Len(t, segment.GetIndexSummaries(), 2)
	require.EqualValues(t, 200, segment.GetIndexSummaries()[0].GetIndexId())
	require.False(t, segment.GetIndexSummaries()[0].GetHasArtifact())
	require.EqualValues(t, 300, segment.GetIndexSummaries()[1].GetIndexId())
	require.EqualValues(t, 9001, segment.GetIndexSummaries()[1].GetBuildId())
	require.True(t, segment.GetIndexSummaries()[1].GetHasArtifact())
	require.EqualValues(t, 64, segment.GetIndexSummaries()[1].GetSerializedSize())
	require.Equal(t, expectedMemSize, segment.GetIndexSummaries()[1].GetMemSize())
	require.EqualValues(t, 101, segment.GetIndexSummaries()[1].GetFieldId())
	require.EqualValues(t, 7, segment.GetIndexSummaries()[1].GetVectorEngineVersion())
	require.EqualValues(t, 2, segment.GetIndexSummaries()[1].GetScalarEngineVersion())
	persisted, err := mt.Catalog.ListSegments(t.Context(), 100)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Equal(t, expectedMemSize, persisted[0].GetIndexSummaries()[1].GetMemSize())
	require.False(t, im.HasBuildInfo(9001))
	require.Zero(t, im.SegmentIndexes.Len())
	require.NoError(t, SyncCopySegmentTask(t.Context(), task, resp, copies, mt, im))
	require.Len(t, mt.GetSegment(t.Context(), 2001).GetIndexSummaries(), 2)
}

func TestCopyWithoutIndexesAcceptsNilIndexMeta(t *testing.T) {
	fixture := &CopySegmentTaskSuite{}
	fixture.SetT(t)
	task, copies, mt, _, resp := fixture.newCopiedManifestReadBackFixture()
	result := resp.GetSegmentResults()[0]
	result.ManifestIndexRewritten = proto.Bool(true)
	result.IndexInfos = nil
	require.NoError(t, SyncCopySegmentTask(t.Context(), task, resp, copies, mt, nil))
	segment := mt.GetSegment(t.Context(), result.GetSegmentId())
	require.Equal(t, commonpb.SegmentState_Flushed, segment.GetState())
	require.Empty(t, segment.GetIndexSummaries())
}
