package datacoord

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/idf"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type splitIDFClient struct {
	datapb.DataCoordClient
	server *Server
}

func (c splitIDFClient) GetStreamingNodeQueryViewResources(ctx context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
	return c.server.GetStreamingNodeQueryViewResources(ctx, req)
}

func checkColdSplitIDF(t *testing.T, s *Server, chunks storage.ChunkManager) {
	t.Helper()
	ctx := t.Context()
	module, err := idf.NewProvider(splitIDFClient{server: s}, idf.WithChunkManager(chunks)).NewRuntime()
	require.NoError(t, err)
	runtime := module.(*idf.Runtime)
	defer runtime.Close()
	wal := walview.VChannelWALView{
		CollectionID: 1, VChannel: "v1", PartitionIDs: []int64{2},
		Schema: &schemapb.CollectionSchema{
			Fields:    []*schemapb.FieldSchema{{FieldID: 7, Name: "text", DataType: schemapb.DataType_VarChar}, {FieldID: 8, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector}},
			Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{7}, OutputFieldIds: []int64{8}}},
		},
		RequiredFields:  []*viewpb.LoadFieldConfig{{FieldId: 8}},
		SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 1}},
	}
	wal.CaptureQueryView = func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool { capture(wal); return true }
	require.NoError(t, runtime.Prepare(ctx, wal))
	binding := runtime.BindView(&viewpb.QueryViewMeta{CollectionId: 1, Version: &viewpb.QueryViewVersion{DataVersion: wal.SegmentSnapshot.DataVersion.IntoProto()}})
	defer binding.Release()
	require.NoError(t, binding.Prepare(ctx, nil))
	oracle := binding.(interface {
		BuildIDFBatch(context.Context, []queryresource.IDFRequest) ([]queryresource.IDFResult, error)
	})
	tfs := &schemapb.SparseFloatArray{Contents: [][]byte{typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})}}
	result, err := oracle.BuildIDFBatch(ctx, []queryresource.IDFRequest{{FieldID: 8, TFs: tfs}})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, float64(4), result[0].Avgdl)
}

func TestStreamingNodeResourcesRecoverRetainedSplitSource(t *testing.T) {
	chunks := storage.NewLocalChunkManager()
	statsPath := filepath.Join(t.TempDir(), "bm25")
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 4})
	serialized, err := stats.Serialize()
	require.NoError(t, err)
	require.NoError(t, chunks.Write(t.Context(), statsPath, serialized))
	for _, tc := range []struct {
		name      string
		state     datapb.SplitShardTaskState
		channel   string
		published bool
		foreign   bool
		wantError error
	}{
		{"adopted", datapb.SplitShardTaskState_SplitShardTaskAdopted, "v2", true, false, nil},
		{"task save lags atomic adoption", datapb.SplitShardTaskState_SplitShardTaskRewriteDone, "v2", true, false, nil},
		{"not adopted", datapb.SplitShardTaskState_SplitShardTaskWindowOpen, "v2", true, false, merr.ErrServiceInternal},
		{"unpublished adoption", datapb.SplitShardTaskState_SplitShardTaskRewriteDone, "v2", false, false, merr.ErrServiceNotReady},
		{"foreign target", datapb.SplitShardTaskState_SplitShardTaskAdopted, "foreign", true, false, merr.ErrServiceInternal},
		{"retired source", datapb.SplitShardTaskState_SplitShardTaskRetired, "v2", true, false, merr.ErrServiceInternal},
		{"mismatched source record", datapb.SplitShardTaskState_SplitShardTaskAdopted, "v2", true, false, merr.ErrDataIntegrity},
		{"foreign collection", datapb.SplitShardTaskState_SplitShardTaskAdopted, "v2", true, true, merr.ErrDataIntegrity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			oldVersion := &viewpb.DataVersion{StreamingVersion: 1}
			views := []*viewpb.DataViewOfCollection{{CollectionId: 1, DataVersion: oldVersion, Shards: []*viewpb.DataViewOfShard{
				{Vchannel: "v1", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 2, SegmentIds: []int64{3}, SegmentManifestVersions: []int64{0}}}},
				{Vchannel: "v2"},
				{Vchannel: "v3"},
			}}}
			kv := catalogkv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			require.NoError(t, kv.SaveDataView(ctx, views[0]))
			source := "v1"
			if tc.name == "mismatched source record" {
				source = "another-source"
			}
			record, err := kv.PrepareSplitAdoption(ctx, &model.SplitAdoption{
				TaskID: 7, CollectionID: 1, Source: source, Moves: []model.SplitAdoptionMove{{SegmentID: 3, PartitionID: 2, Target: "v2", TransformStart: 100}},
			})
			require.NoError(t, err)
			if tc.published {
				adopted := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, Shards: []*viewpb.DataViewOfShard{
					{Vchannel: "v2", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 2, SegmentIds: []int64{3}, SegmentManifestVersions: []int64{0}}}},
					{Vchannel: "v3"},
				}}
				require.NoError(t, kv.CommitSplitAdoption(ctx, record, adopted))
				require.NoError(t, kv.SaveSplitAdoptionState(ctx, record, "Done"))
				// A later target compaction replaces segment 3 with segment 4.
				adopted.DataVersion.CompactVersion++
				adopted.Shards[0].Partitions[0].SegmentIds = []int64{4}
				require.NoError(t, kv.SaveDataView(ctx, adopted))
			}
			manager, err := dataview.RecoverManager(ctx, kv, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
			require.NoError(t, err)
			catalog := &splitTestCatalog{tasks: map[int64]*datapb.SplitShardTask{7: {
				TaskId: 7, CollectionId: 1, SourceVchannel: "v1", State: tc.state,
				Targets: []*datapb.SplitShardTaskTarget{{Vchannel: "v2"}, {Vchannel: "v3"}},
			}}}
			splitStore, err := split.NewStore(ctx, catalog)
			require.NoError(t, err)
			store := metacache.NewMetaStore(nil)
			s := &Server{dataViewManager: manager, splitStore: splitStore, meta: &meta.Meta{Catalog: kv, Segments: meta.NewSegmentsInfo(store), MetaStore: store}}
			s.stateCode.Store(commonpb.StateCode_Healthy)
			info := &datapb.SegmentInfo{ID: 3, CollectionID: 1, PartitionID: 2, InsertChannel: tc.channel, State: commonpb.SegmentState_Dropped, Bm25Statslogs: []*datapb.FieldBinlog{{FieldID: 8, Binlogs: []*datapb.Binlog{{LogPath: statsPath}}}}}
			if tc.foreign {
				info.CollectionID = 99
			}
			s.meta.Segments.SetSegment(3, meta.NewSegmentInfo(info))
			resp, err := s.GetStreamingNodeQueryViewResources(ctx, &datapb.GetStreamingNodeQueryViewResourcesRequest{CollectionId: 1, Vchannel: "v1", DataVersion: oldVersion})
			require.NoError(t, err)
			if tc.wantError != nil {
				require.ErrorIs(t, merr.Error(resp.GetStatus()), tc.wantError)
				return
			}
			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Len(t, resp.GetBm25Resources(), 1)
			require.Equal(t, int64(3), resp.Bm25Resources[0].GetSegmentId())
			require.Equal(t, statsPath, resp.Bm25Resources[0].GetBm25Binlogs()[0].GetBinlogs()[0].GetLogPath())
			checkColdSplitIDF(t, s, chunks)
			require.Equal(t, tc.channel, s.meta.GetSegment(ctx, 3).GetInsertChannel(), "historical read must not rewrite current ownership")
		})
	}
}
