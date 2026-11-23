package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
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
	im := &index.IndexMeta{}
	m := &meta.Meta{IndexCatalog: im}
	patchIndexes := mockey.Mock((*index.IndexMeta).GetQueryViewIndexSnapshot).Return([]*model.Index(nil), map[int64]map[UniqueID]*model.SegmentIndex(nil)).Build()
	defer patchIndexes.UnPatch()
	patchSegment := mockey.Mock((*meta.Meta).GetSegment).Return(&meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 10, CollectionID: 1}}).Build()
	defer patchSegment.UnPatch()
	s := &Server{meta: m, domainMetas: domainMetas{indexMeta: im}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	infos, indexes, err := s.GetQueryViewSegmentLoadInfos(context.Background(), 1, []int64{10}, false)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.ErrorIs(t, merr.Error(infos[0].GetStatus()), merr.ErrServiceNotReady)
	require.Nil(t, infos[0].LoadInfo)
	require.Empty(t, indexes)
}

func TestQueryViewSegmentLoadInfoBatchIsolation(t *testing.T) {
	patchIndexes := mockey.Mock((*index.IndexMeta).GetQueryViewIndexSnapshot).Return([]*model.Index{{CollectionID: 1, IndexID: 5}}, map[int64]map[UniqueID]*model.SegmentIndex(nil)).Build()
	defer patchIndexes.UnPatch()
	patchSegment := mockey.Mock((*meta.Meta).GetSegment).To(func(_ *meta.Meta, _ context.Context, id int64) *meta.SegmentInfo {
		switch id {
		case 1:
			return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: 1, ManifestPath: "segment/_metadata/manifest-1.avro", TransformStartAfterTimetick: 100, Stats: &datapb.Statistics{InsertBinlogSize: 512}}}
		case 2:
			return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: 1}}
		case 3:
			return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: 2, TransformStartAfterTimetick: 100}}
		default:
			return nil
		}
	}).Build()
	defer patchSegment.UnPatch()
	im := &index.IndexMeta{}
	s := &Server{meta: &meta.Meta{IndexCatalog: im}, domainMetas: domainMetas{indexMeta: im}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	results, indexes, err := s.GetQueryViewSegmentLoadInfos(t.Context(), 1, []int64{2, 3, 4, 1, 1}, false)
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
	results, indexes, err = s.GetQueryViewSegmentLoadInfos(t.Context(), 1, nil, false)
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
			s := &Server{meta: &meta.Meta{ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files"))}}
			segment := &datapb.SegmentInfo{ID: 10, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3, ManifestPath: tc.manifest}
			indexes := map[int64]*indexpb.IndexInfo{5: {CollectionID: 1, FieldID: 100, IndexID: 5}}
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
	segments := make(map[int64]*meta.SegmentInfo, len(manifests))
	segmentIndexes := make(map[int64]map[UniqueID]*model.SegmentIndex, len(manifests))
	for i, manifest := range manifests {
		id := int64(i + 1)
		segments[id] = &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
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
	patchIndexes := mockey.Mock((*index.IndexMeta).GetQueryViewIndexSnapshot).Return([]*model.Index{{CollectionID: 1, FieldID: 100, IndexID: 5}}, segmentIndexes).Build()
	defer patchIndexes.UnPatch()
	patchSegment := mockey.Mock((*meta.Meta).GetSegment).To(func(_ *meta.Meta, _ context.Context, id int64) *meta.SegmentInfo {
		return segments[id]
	}).Build()
	defer patchSegment.UnPatch()
	im := &index.IndexMeta{}
	s := &Server{meta: &meta.Meta{IndexCatalog: im, ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files"))}, domainMetas: domainMetas{indexMeta: im}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	results, indexes, err := s.GetQueryViewSegmentLoadInfos(t.Context(), 1, []int64{1, 2, 3, 4}, false)
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

func TestGetSegmentInfoProjectsIndexResourcesWithoutManifestIO(t *testing.T) {
	im := &index.IndexMeta{}
	server := &Server{meta: &meta.Meta{}, domainMetas: domainMetas{indexMeta: im}}
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	rows := int64(2500)
	segment := &datapb.SegmentInfo{
		ID: 10, CollectionID: 1, NumOfRows: 3000, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath("unavailable", 8), ManifestHasIndex: true,
		IndexSummaries: []*datapb.SegmentIndexSummary{
			{IndexId: 1, FieldId: 100, BuildId: 1000, HasArtifact: true, MemSize: 40, IndexType: "DISKANN", NumRows: &rows},
			{IndexId: 2, FieldId: 101, BuildId: 1001},
			{IndexId: 3, FieldId: 102, BuildId: 1002, HasArtifact: true, MemSize: 50},
			{IndexId: 5, FieldId: 104, BuildId: 1004, HasArtifact: true},
		},
	}
	definitions := []*model.Index{
		{
			IndexID: 1, FieldID: 100, IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "HNSW"}},
			TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}},
		},
		{IndexID: 2, FieldID: 101},
		{IndexID: 3, FieldID: 102},
		{IndexID: 4, FieldID: 103},
	}
	tasks := map[int64]map[int64]*model.SegmentIndex{10: {
		3: {IndexID: 3, BuildID: 1002, IndexState: commonpb.IndexState_Finished, IndexMemSize: 90, NumRows: 3000, IndexFileKeys: []string{"retained"}},
		4: {IndexID: 4, BuildID: 1003, IndexState: commonpb.IndexState_Finished, IndexMemSize: 80, NumRows: 3100, IndexFileKeys: []string{"retained"}},
	}}
	defer mockey.Mock((*index.IndexMeta).UsesSegmentSummaries).Return(true).Build().UnPatch()
	defer mockey.Mock((*index.IndexMeta).GetQueryViewIndexSnapshot).Return(definitions, tasks).Build().UnPatch()
	defer mockey.Mock((*meta.Meta).GetSegment).To(func(_ *meta.Meta, _ context.Context, id int64) *meta.SegmentInfo {
		if id == 10 {
			return &meta.SegmentInfo{SegmentInfo: segment}
		}
		return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: 1}}
	}).Build().UnPatch()
	defer mockey.Mock((*meta.Meta).GetCompactionTo).Return(nil, true).Build().UnPatch()
	open := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.ErrServiceUnavailable).Build()
	defer open.UnPatch()

	resp, err := server.GetSegmentInfo(t.Context(), &datapb.GetSegmentInfoRequest{SegmentIDs: []int64{10, 11}, IncludeUnHealthy: true, IndexResourceCollectionId: 1})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Zero(t, open.Times())
	require.Contains(t, resp.GetIndexResourceInfo(), int64(11))
	require.Empty(t, resp.GetIndexResourceInfo()[11].GetIndexInfos())

	indexes := make(map[int64]*indexpb.IndexFilePathInfo)
	for _, info := range resp.GetIndexResourceInfo()[10].GetIndexInfos() {
		require.NotNil(t, info.HasArtifact)
		require.Empty(t, info.GetIndexFilePaths())
		indexes[info.GetIndexID()] = info
	}
	require.Len(t, indexes, 4)
	require.Equal(t, rows, indexes[1].GetNumRows())
	require.EqualValues(t, 40, indexes[1].GetMemSize())
	require.Equal(t, "DISKANN", indexes[1].GetIndexType())
	require.Equal(t, []*commonpb.KeyValuePair{{Key: "index_type", Value: "DISKANN"}, {Key: "dim", Value: "128"}}, indexes[1].GetIndexParams())
	require.True(t, indexes[1].GetHasArtifact())
	require.False(t, indexes[2].GetHasArtifact())
	require.Equal(t, segment.GetNumOfRows(), indexes[2].GetNumRows())
	require.True(t, indexes[3].GetHasArtifact())
	require.EqualValues(t, 90, indexes[3].GetMemSize())
	require.True(t, indexes[4].GetHasArtifact())
	require.EqualValues(t, 3100, indexes[4].GetNumRows())
	require.Equal(t, "HNSW", definitions[0].IndexParams[0].GetValue())

	legacy, err := server.GetSegmentInfo(t.Context(), &datapb.GetSegmentInfoRequest{SegmentIDs: []int64{10}, IncludeUnHealthy: true})
	require.NoError(t, merr.CheckRPCCall(legacy, err))
	require.Empty(t, legacy.GetIndexResourceInfo())
}

func TestQueryViewNegotiatesLocalManifestIndexesWithoutCoordinatorRead(t *testing.T) {
	const segmentID = 10
	im := &index.IndexMeta{}
	s := &Server{
		meta:        &meta.Meta{ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files"))},
		domainMetas: domainMetas{indexMeta: im},
	}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	const base = "files/insert_log/1/2/10"
	pointer := packed.MarshalManifestPath(base, 7)
	segment := &datapb.SegmentInfo{
		ID: segmentID, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3,
		ManifestPath: pointer, TransformStartAfterTimetick: 100, ManifestHasIndex: true,
		Stats: &datapb.Statistics{InsertBinlogSize: 512},
		IndexSummaries: []*datapb.SegmentIndexSummary{
			{IndexId: 5, FieldId: 100, BuildId: 30, IndexVersion: 4, HasArtifact: true},
			{IndexId: 6, FieldId: 101, BuildId: 31, IndexVersion: 4, HasArtifact: true},
		},
	}
	definitions := []*model.Index{
		{CollectionID: 1, IndexID: 5, FieldID: 100},
		{CollectionID: 1, IndexID: 6, FieldID: 101},
	}
	collection := &meta.CollectionInfo{Schema: &schemapb.CollectionSchema{}}
	defer mockey.Mock((*index.IndexMeta).UsesSegmentSummaries).Return(true).Build().UnPatch()
	defer mockey.Mock((*meta.Meta).GetCollection).Return(collection).Build().UnPatch()
	defer mockey.Mock((*index.IndexMeta).GetQueryViewIndexSnapshot).To(func(*index.IndexMeta, int64, []int64) ([]*model.Index, map[int64]map[int64]*model.SegmentIndex) {
		return definitions, map[int64]map[int64]*model.SegmentIndex{segmentID: {
			5: {
				CollectionID: 1, PartitionID: 2, SegmentID: segmentID, IndexID: 5, BuildID: 29,
				IndexState: commonpb.IndexState_Finished, IndexFileKeys: []string{"old-index"},
			},
			6: {
				CollectionID: 1, PartitionID: 2, SegmentID: segmentID, IndexID: 6, BuildID: 31, IndexVersion: 4,
				IndexState: commonpb.IndexState_Finished, IndexFileKeys: []string{"retained-index"},
			},
		}}
	}).Build().UnPatch()
	defer mockey.Mock((*meta.Meta).GetSegment).To(func(_ *meta.Meta, _ context.Context, id int64) *meta.SegmentInfo {
		switch id {
		case segmentID:
			return &meta.SegmentInfo{SegmentInfo: segment}
		case 11:
			return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: id, CollectionID: 1, StorageVersion: storage.StorageV2, TransformStartAfterTimetick: 110,
			}}
		default:
			return nil
		}
	}).Build().UnPatch()
	open := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{{
		IndexID: 5, FieldID: 100, BuildID: 30, IndexVersion: 4, IndexName: "vector", IndexType: "HNSW",
		Path: base + "/_index/30/4", IndexFileKeys: []string{"index.bin"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}}, nil).Build()
	defer open.UnPatch()

	local, _, err := s.GetQueryViewSegmentLoadInfos(t.Context(), 1, []int64{segmentID, 11, 404}, true)
	require.NoError(t, err)
	require.Zero(t, open.Times())
	require.NoError(t, merr.Error(local[0].GetStatus()))
	require.Equal(t, pointer, local[0].GetLoadInfo().GetManifestPath())
	require.EqualValues(t, 100, local[0].GetLoadInfo().GetTransformStartAfterTimetick())
	require.EqualValues(t, 512, local[0].GetLoadInfo().GetStats().GetInsertBinlogSize())
	require.Empty(t, local[0].GetLoadInfo().GetIndexInfos())
	require.Len(t, local[0].GetManifestIndexes().GetSummaries(), 1)
	require.EqualValues(t, 30, local[0].GetManifestIndexes().GetSummaries()[0].GetBuildId())
	require.Len(t, local[0].GetManifestIndexes().GetFinishedTasks(), 1)
	require.EqualValues(t, 31, local[0].GetManifestIndexes().GetFinishedTasks()[0].GetBuildID())
	require.NoError(t, merr.Error(local[1].GetStatus()))
	require.Nil(t, local[1].GetManifestIndexes())
	require.ErrorIs(t, merr.Error(local[2].GetStatus()), merr.ErrSegmentNotFound)

	full, _, err := s.GetQueryViewSegmentLoadInfos(t.Context(), 1, []int64{segmentID}, false)
	require.NoError(t, err)
	require.NoError(t, merr.Error(full[0].GetStatus()))
	require.Equal(t, 1, open.Times())
	require.Nil(t, full[0].GetManifestIndexes())
	require.Len(t, full[0].GetLoadInfo().GetIndexInfos(), 2)
	files := make(map[int64][]string)
	for _, info := range full[0].GetLoadInfo().GetIndexInfos() {
		files[info.GetBuildID()] = info.GetIndexFilePaths()
	}
	require.Equal(t, []string{base + "/_index/30/4/index.bin"}, files[30])
	require.Equal(t, local[0].GetManifestIndexes().GetFinishedTasks()[0].GetIndexFilePaths(), files[31])

	collection.Schema = &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{ExternalField: "source_column"}}}
	external, _, err := s.GetQueryViewSegmentLoadInfos(t.Context(), 1, []int64{segmentID}, true)
	require.NoError(t, err)
	require.NoError(t, merr.Error(external[0].GetStatus()))
	require.Nil(t, external[0].GetManifestIndexes())
	require.Equal(t, 2, open.Times())
}
