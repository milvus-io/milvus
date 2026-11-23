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

package datacoord

import (
	"context"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// GetIndexInfos serves the load path from in-memory SegmentIndex metadata and
// must never read a manifest: it is driven by QueryCoord's index checker every
// checkIndexInterval, so a per-segment object read would be paid on every round
// by exactly the segments that are legitimately unindexed.
func TestServerGetIndexInfosReadsNoManifest(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(2)
		segmentID    = int64(3)
		fieldID      = int64(100)
		indexID      = int64(101)
		buildID      = int64(102)
		indexVersion = int64(4)
		fieldID2     = int64(200)
		indexID2     = int64(201)
		buildID2     = int64(202)
	)

	segmentIndex := &model.SegmentIndex{
		SegmentID:             segmentID,
		CollectionID:          collectionID,
		PartitionID:           partitionID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          indexVersion,
		IndexState:            commonpb.IndexState_Finished,
		IndexType:             "HNSW",
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}
	segmentIndex2 := &model.SegmentIndex{
		SegmentID:             segmentID,
		CollectionID:          collectionID,
		PartitionID:           partitionID,
		IndexID:               indexID2,
		BuildID:               buildID2,
		IndexVersion:          indexVersion,
		IndexState:            commonpb.IndexState_Finished,
		IndexType:             "HNSW",
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	segmentIndexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segmentIndexes.Insert(indexID, segmentIndex)
	segmentIndexes.Insert(indexID2, segmentIndex2)
	ms := metacache.NewMetaStore(nil)
	segments := meta.NewSegmentsInfo(ms)
	segments.SetSegment(segmentID, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		StorageVersion: storage.StorageV3,
		ManifestPath:   `{"base_path":"segments/1/2/3","ver":2}`,
	}})
	im := &index.IndexMeta{
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
			collectionID: {
				indexID: {
					CollectionID: collectionID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    "vector_hnsw",
					IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}, {Key: "metric_type", Value: "L2"}},
					TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}},
				},
				indexID2: {
					CollectionID: collectionID,
					FieldID:      fieldID2,
					IndexID:      indexID2,
					IndexName:    "vector_hnsw_2",
				},
			},
		},
		SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	server := &Server{meta: &meta.Meta{
		Segments: segments,
	}, domainMetas: domainMetas{indexMeta: im}}
	server.indexMeta.SegmentIndexes.Insert(segmentID, segmentIndexes)
	server.meta.IndexCatalog = server.indexMeta
	server.stateCode.Store(commonpb.StateCode_Healthy)
	req := &indexpb.GetIndexInfoRequest{CollectionID: collectionID, SegmentIDs: []int64{segmentID}}
	manifestIndex := packed.ManifestIndexInfo{
		IndexName:             "vector_hnsw",
		IndexType:             "HNSW",
		Path:                  "index_v1/1/2/3/102/4",
		FieldID:               fieldID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          indexVersion,
		NumRows:               1000,
		SerializedSize:        2000,
		MemSize:               3000,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:         []string{"0", "1"},
	}
	manifestIndex2 := manifestIndex
	manifestIndex2.IndexName = "vector_hnsw_2"
	manifestIndex2.Path = "index_v1/1/2/3/202/4"
	manifestIndex2.FieldID = fieldID2
	manifestIndex2.IndexID = indexID2
	manifestIndex2.BuildID = buildID2

	// A segment with no SegmentIndex record has no index artifact: a manifest
	// entry is never published without its record being installed in memory by
	// the same commit, so there is nothing to recover by reading the manifest.
	t.Run("absent segment index records yield nothing and read no manifest", func(t *testing.T) {
		server.indexMeta.SegmentIndexes.Remove(segmentID)
		manifestReadCount := 0
		patch := mockey.Mock(packed.GetManifestIndexInfosAsync).To(
			func(_ context.Context, _ *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				manifestReadCount++
				return []packed.ManifestIndexInfo{manifestIndex, manifestIndex2}, nil
			}).Build()
		defer patch.UnPatch()

		resp, err := server.GetIndexInfos(context.Background(), req)
		require.NoError(t, err)
		require.Zero(t, manifestReadCount)
		info := resp.GetSegmentInfo()[segmentID]
		require.False(t, info.GetEnableIndex())
		require.Empty(t, info.GetIndexInfos())
		server.indexMeta.SegmentIndexes.Insert(segmentID, segmentIndexes)
	})

	// A finished record with no index file keys is a fake-finished build (a
	// segment too small to train), which publishes no manifest entry either -
	// so there is nothing to look up, and the empty file list is the answer.
	t.Run("finished task missing index file keys reads no manifest", func(t *testing.T) {
		server.meta.ChunkManager = storage.NewLocalChunkManager()
		manifestReadCount := 0
		patch := mockey.Mock(packed.GetManifestIndexInfosAsync).To(
			func(_ context.Context, _ *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				manifestReadCount++
				return []packed.ManifestIndexInfo{manifestIndex, manifestIndex2}, nil
			}).Build()
		defer patch.UnPatch()

		resp, err := server.GetIndexInfos(context.Background(), req)
		require.NoError(t, err)
		require.Zero(t, manifestReadCount)
		infos := resp.GetSegmentInfo()[segmentID].GetIndexInfos()
		require.Len(t, infos, 2)
		for _, info := range infos {
			assert.Empty(t, info.GetIndexFilePaths())
		}
	})

	t.Run("uses recorded index file keys without reading manifest", func(t *testing.T) {
		segmentIndex.IndexFileKeys = []string{"index.bin"}
		segmentIndex.IndexSerializedSize = 2000
		segmentIndex.IndexMemSize = 3000
		segmentIndex2.IndexFileKeys = []string{"index.bin"}
		segmentIndex2.IndexSerializedSize = 2000
		segmentIndex2.IndexMemSize = 3000
		server.meta.ChunkManager = storage.NewLocalChunkManager()
		manifestReadCount := 0
		patch := mockey.Mock(packed.GetManifestIndexInfosAsync).To(
			func(_ context.Context, _ *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				manifestReadCount++
				return nil, nil
			}).Build()
		defer patch.UnPatch()

		resp, err := server.GetIndexInfos(context.Background(), req)
		require.NoError(t, err)
		require.Zero(t, manifestReadCount)
		require.True(t, resp.GetSegmentInfo()[segmentID].GetEnableIndex())
		require.Len(t, resp.GetSegmentInfo()[segmentID].GetIndexInfos(), 2)
		for _, info := range resp.GetSegmentInfo()[segmentID].GetIndexInfos() {
			if info.GetBuildID() == buildID {
				require.Equal(t, []string{"segments/1/2/3/_index/102/4/index.bin"}, info.GetIndexFilePaths())
				require.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED, info.GetIndexStorePathVersion())
				require.Equal(t, map[string]string{common.IndexTypeKey: "HNSW", "metric_type": "L2", common.DimKey: "8"}, common.KeyValuePairs(info.GetIndexParams()).ToMap())
			}
		}
	})
}

func TestSummaryModeIndexInfoKeepsRetainedTasksAndEmptySlots(t *testing.T) {
	ctx := t.Context()
	mt, dm, err := index.NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	im := dm.IndexMeta
	require.NoError(t, im.CreateIndex(ctx, &model.Index{IndexID: 10, CollectionID: 1, FieldID: 100, IndexName: "idx"}))
	base := t.TempDir()
	mt.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(base))
	for _, id := range []int64{3, 4} {
		require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Dropped,
			StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath(base, 1), TransformStartAfterTimetick: 100,
		})))
	}
	require.NoError(t, im.UseSegmentSummaries(mt.MetaStore))
	version := indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	for _, id := range []int64{3, 4} {
		require.NoError(t, im.AddSegmentIndex(ctx, &model.SegmentIndex{
			IndexID: 10, BuildID: id + 20, IndexVersion: 1, SegmentID: id, CollectionID: 1, PartitionID: 2,
			IndexState: commonpb.IndexState_InProgress, IndexStorePathVersion: version,
		}))
	}
	require.NoError(t, mt.FinishTask(ctx, &workerpb.IndexTaskInfo{
		BuildID: 23, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"idx.bin"}, IndexStorePathVersion: version,
	}))
	srv := &Server{meta: mt, domainMetas: domainMetas{indexMeta: im}}
	srv.stateCode.Store(commonpb.StateCode_Healthy)
	require.Equal(t, []int64{3}, im.GetIndexedSegments(1, []int64{3, 4}, []int64{100}))

	resp, err := srv.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{CollectionID: 1, SegmentIDs: []int64{3, 4, 5, 3}})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Len(t, resp.SegmentInfo, 3)
	require.Len(t, resp.SegmentInfo[3].GetIndexInfos(), 1)
	expected := []string{path.Join(base, "_index/23/1/idx.bin")}
	require.Equal(t, expected, resp.SegmentInfo[3].GetIndexInfos()[0].GetIndexFilePaths())
	require.True(t, resp.SegmentInfo[4].GetEnableIndex())
	require.Empty(t, resp.SegmentInfo[4].GetIndexInfos())
	require.False(t, resp.SegmentInfo[5].GetEnableIndex())
	require.Empty(t, resp.SegmentInfo[5].GetIndexInfos())

	results, _, err := srv.GetQueryViewSegmentLoadInfos(ctx, 1, []int64{3}, false)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NoError(t, merr.Error(results[0].GetStatus()))
	require.Len(t, results[0].GetLoadInfo().GetIndexInfos(), 1)
	require.Equal(t, expected, results[0].GetLoadInfo().GetIndexInfos()[0].GetIndexFilePaths())
}

func TestSummaryModeIndexInfoReadsManifestsConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	limit := &paramtable.Get().DataCoordCfg.SegmentIndexManifestLoadConcurrency
	previous := limit.SwapTempValue("2")
	t.Cleanup(func() { limit.SwapTempValue(previous) })

	mt, dm, err := index.NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, dm.IndexMeta.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
	root := t.TempDir()
	mt.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
	version := indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
	manifestEntries := make(map[string]packed.ManifestIndexInfo)
	for _, id := range []int64{3, 4} {
		base := path.Join(root, "insert_log/1/2", strconv.FormatInt(id, 10))
		manifest := packed.MarshalManifestPath(base, 1)
		manifestEntries[manifest] = packed.ManifestIndexInfo{
			IndexID: 10, BuildID: id, FieldID: 100, IndexVersion: 1, IndexName: "idx", IndexType: "HNSW",
			Path:          path.Join(base, "_index", strconv.FormatInt(id, 10), "1"),
			IndexFileKeys: []string{"index.bin"}, IndexStorePathVersion: version,
		}
		require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
			ManifestPath: manifest, ManifestHasIndex: true,
			IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 10, FieldId: 100, BuildId: id, HasArtifact: true, IndexVersion: 1}},
		})))
	}
	require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
	srv := &Server{meta: mt, domainMetas: domainMetas{indexMeta: dm.IndexMeta}}
	srv.stateCode.Store(commonpb.StateCode_Healthy)
	started := make(chan string, len(manifestEntries))
	release := make(chan struct{})
	read := mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(readCtx context.Context, _ *packed.ManifestIOContext, manifest string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			started <- manifest
			select {
			case <-release:
				return []packed.ManifestIndexInfo{manifestEntries[manifest]}, nil
			case <-readCtx.Done():
				return nil, readCtx.Err()
			}
		}).Build()
	defer read.UnPatch()
	done := make(chan *indexpb.GetIndexInfoResponse, 1)
	go func() {
		resp, _ := srv.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{CollectionID: 1, SegmentIDs: []int64{3, 4}})
		done <- resp
	}()
	var opened []string
	for i := 0; i < len(manifestEntries); i++ {
		select {
		case manifest := <-started:
			opened = append(opened, manifest)
		case <-ctx.Done():
		}
	}
	close(release)
	resp := <-done
	require.Len(t, opened, 2)
	require.NoError(t, merr.Error(resp.GetStatus()))
	for _, manifest := range opened {
		entry := manifestEntries[manifest]
		require.Len(t, resp.SegmentInfo[entry.BuildID].IndexInfos, 1)
		require.Equal(t, []string{path.Join(entry.Path, "index.bin")}, resp.SegmentInfo[entry.BuildID].IndexInfos[0].IndexFilePaths)
	}
}
