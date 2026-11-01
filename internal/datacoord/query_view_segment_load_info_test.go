package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewSegmentLoadInfoPreservesV3ResourceStatistics(t *testing.T) {
	segment := &datapb.SegmentInfo{
		ID: 10, CollectionID: 1, PartitionID: 2, NumOfRows: 100,
		StorageVersion: storage.StorageV3, ManifestPath: "segment/_manifest/1.manifest",
		Stats: &datapb.Statistics{
			InsertBinlogSize: 1600, DeltaBinlogSize: 64,
			LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
				{GroupId: 100, FieldIds: []int64{100, 101}, MemorySize: 1600},
			}},
		},
	}
	info, err := (&Server{}).packQueryViewSegmentLoadInfo(segment, nil, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(segment.GetStats(), info.GetStats()))
	require.Empty(t, info.GetBinlogPaths())
	require.Empty(t, info.GetDeltalogs())
	binlogs, deltalogs := loadresource.ResolveSegmentEstimateLogs(&schemapb.CollectionSchema{}, info)
	require.Equal(t, []*datapb.FieldBinlog{{
		FieldID: 100, ChildFields: []int64{100, 101},
		Binlogs: []*datapb.Binlog{{EntriesNum: 100, MemorySize: 1600}},
	}}, binlogs)
	require.Equal(t, []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 64}}}}, deltalogs)
}

func TestQueryViewSegmentLoadInfoRejectsUnknownReplayCursor(t *testing.T) {
	m := &meta{indexMeta: &indexMeta{}}
	patchIndexes := mockey.Mock((*indexMeta).getQueryViewIndexSnapshot).Return([]*model.Index(nil), map[int64]map[UniqueID]*model.SegmentIndex(nil)).Build()
	defer patchIndexes.UnPatch()
	patchSegment := mockey.Mock((*meta).GetSegment).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 10, CollectionID: 1}}).Build()
	defer patchSegment.UnPatch()
	s := &Server{meta: m}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	infos, indexes, err := s.GetQueryViewSegmentLoadInfos(context.Background(), 1, []int64{10})
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.ErrorIs(t, merr.Error(infos[0].GetStatus()), merr.ErrServiceNotReady)
	require.Nil(t, infos[0].LoadInfo)
	require.Empty(t, indexes)
}

func TestQueryViewSegmentLoadInfoBatchIsolation(t *testing.T) {
	patchIndexes := mockey.Mock((*indexMeta).getQueryViewIndexSnapshot).Return([]*model.Index{{CollectionID: 1, IndexID: 5}}, map[int64]map[UniqueID]*model.SegmentIndex(nil)).Build()
	defer patchIndexes.UnPatch()
	patchSegment := mockey.Mock((*meta).GetSegment).To(func(_ *meta, _ context.Context, id int64) *SegmentInfo {
		switch id {
		case 1:
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: 1, ManifestPath: "segment/_metadata/manifest-1.avro", TransformStartAfterTimetick: 100, Stats: &datapb.Statistics{InsertBinlogSize: 512}}}
		case 2:
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: 1}}
		case 3:
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: 2, TransformStartAfterTimetick: 100}}
		default:
			return nil
		}
	}).Build()
	defer patchSegment.UnPatch()
	s := &Server{meta: &meta{indexMeta: &indexMeta{}}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	results, indexes, err := s.GetQueryViewSegmentLoadInfos(t.Context(), 1, []int64{2, 3, 4, 1, 1})
	require.NoError(t, err)
	require.Len(t, results, 4)
	require.ErrorIs(t, merr.Error(results[0].Status), merr.ErrServiceNotReady)
	require.ErrorIs(t, merr.Error(results[1].Status), merr.ErrSegmentNotFound)
	require.ErrorIs(t, merr.Error(results[2].Status), merr.ErrSegmentNotFound)
	require.NoError(t, merr.Error(results[3].Status))
	require.EqualValues(t, 512, results[3].LoadInfo.Stats.InsertBinlogSize)
	require.EqualValues(t, 100, results[3].LoadInfo.TransformStartAfterTimetick)
	require.Len(t, indexes, 1)
	require.Equal(t, 1, patchIndexes.Times())
	require.Equal(t, 4, patchSegment.Times())
	results, indexes, err = s.GetQueryViewSegmentLoadInfos(t.Context(), 1, nil)
	require.NoError(t, err)
	require.Empty(t, results)
	require.Empty(t, indexes)
}

func TestQueryViewSegmentLoadInfoIndexPaths(t *testing.T) {
	for _, tc := range []struct {
		name     string
		version  indexpb.IndexStorePathVersion
		manifest string
		prefix   string
	}{
		{"build_rooted", indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, "", "files/index_files/30/4/2/10"},
		{"collection_rooted", indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, "", "files/index_v1/1/2/10/30/4"},
		{"segment_rooted", indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED, packed.MarshalManifestPath("files/insert_log/1/2/10", 7), "files/insert_log/1/2/10/_index/30/4"},
		{"relocated_segment", indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED, packed.MarshalManifestPath("legacy/other/segment", 7), "legacy/other/segment/_index/30/4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{meta: &meta{chunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files"))}}
			segment := &datapb.SegmentInfo{ID: 10, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3, ManifestPath: tc.manifest}
			indexes := []*indexpb.IndexInfo{{CollectionID: 1, FieldID: 100, IndexID: 5}}
			segmentIndexes := map[int64]*model.SegmentIndex{5: {
				CollectionID: 1, PartitionID: 2, SegmentID: 10, IndexID: 5, BuildID: 30, IndexVersion: 4,
				IndexState: commonpb.IndexState_Finished, IndexStorePathVersion: tc.version,
				IndexFileKeys: []string{"index.bin", "index.meta"},
			}}
			info, err := s.packQueryViewSegmentLoadInfo(segment, indexes, segmentIndexes)
			require.NoError(t, err)
			require.Len(t, info.IndexInfos, 1)
			require.Equal(t, []string{tc.prefix + "/index.bin", tc.prefix + "/index.meta"}, info.IndexInfos[0].IndexFilePaths)
			require.Equal(t, tc.manifest, info.ManifestPath)
			require.Equal(t, tc.version, info.IndexInfos[0].IndexStorePathVersion)

			// Unfinished and definition-less indexes must still be ignored,
			// even when their owning segment has no usable manifest.
			segment.ManifestPath = "invalid"
			segmentIndexes[5].IndexState = commonpb.IndexState_InProgress
			info, err = s.packQueryViewSegmentLoadInfo(segment, indexes, segmentIndexes)
			require.NoError(t, err)
			require.Empty(t, info.IndexInfos)
			segmentIndexes[5].IndexState = commonpb.IndexState_Finished
			info, err = s.packQueryViewSegmentLoadInfo(segment, nil, segmentIndexes)
			require.NoError(t, err)
			require.Empty(t, info.IndexInfos)

			// A finished index with no file keys needs no path resolution.
			segmentIndexes[5].IndexFileKeys = nil
			info, err = s.packQueryViewSegmentLoadInfo(segment, indexes, segmentIndexes)
			require.NoError(t, err)
			require.Len(t, info.IndexInfos, 1)
			require.Empty(t, info.IndexInfos[0].IndexFilePaths)
		})
	}
}

func TestQueryViewSegmentLoadInfoManifestErrorIsolation(t *testing.T) {
	manifests := []string{"invalid", "", packed.MarshalManifestPath("", 7), packed.MarshalManifestPath("legacy/other/segment", 7)}
	segments := make(map[int64]*SegmentInfo, len(manifests))
	segmentIndexes := make(map[int64]map[UniqueID]*model.SegmentIndex, len(manifests))
	for i, manifest := range manifests {
		id := int64(i + 1)
		segments[id] = &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: id, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3,
			ManifestPath: manifest, TransformStartAfterTimetick: 100,
		}}
		segmentIndexes[id] = map[UniqueID]*model.SegmentIndex{5: {
			CollectionID: 1, PartitionID: 2, SegmentID: id, IndexID: 5, BuildID: 30, IndexVersion: 4,
			IndexState:            commonpb.IndexState_Finished,
			IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
			IndexFileKeys:         []string{"index.bin"},
		}}
	}
	patchIndexes := mockey.Mock((*indexMeta).getQueryViewIndexSnapshot).Return([]*model.Index{{CollectionID: 1, FieldID: 100, IndexID: 5}}, segmentIndexes).Build()
	defer patchIndexes.UnPatch()
	patchSegment := mockey.Mock((*meta).GetSegment).To(func(_ *meta, _ context.Context, id int64) *SegmentInfo {
		return segments[id]
	}).Build()
	defer patchSegment.UnPatch()
	s := &Server{meta: &meta{indexMeta: &indexMeta{}, chunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files"))}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	results, indexes, err := s.GetQueryViewSegmentLoadInfos(t.Context(), 1, []int64{1, 2, 3, 4})
	require.NoError(t, err)
	require.Len(t, results, 4)
	require.Len(t, indexes, 1)
	for i, result := range results[:3] {
		require.EqualValues(t, i+1, result.SegmentID)
		require.ErrorIs(t, merr.Error(result.Status), merr.ErrDataIntegrity)
		require.Equal(t, merr.SystemError, merr.GetErrorType(merr.Error(result.Status)))
		require.Nil(t, result.LoadInfo)
	}
	require.NoError(t, merr.Error(results[3].Status))
	require.EqualValues(t, 4, results[3].SegmentID)
	require.Len(t, results[3].LoadInfo.IndexInfos, 1)
	require.Equal(t, []string{"legacy/other/segment/_index/30/4/index.bin"}, results[3].LoadInfo.IndexInfos[0].IndexFilePaths)
	require.Equal(t, 1, patchIndexes.Times())
	for i, manifest := range manifests {
		require.Equal(t, manifest, segments[int64(i+1)].GetManifestPath())
	}
}
