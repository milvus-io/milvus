package qnview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type distributionTestSegment struct{ segments.Segment }

func (*distributionTestSegment) LastDeltaTimestamp() uint64 { panic("unpatched") }

func TestSegmentDistributionRetainsPhysicalBases(t *testing.T) {
	mockey.PatchConvey("distribution pins the selected physical base and owns response metadata", t, func() {
		entered, proceed := make(chan struct{}), make(chan struct{})
		mockey.Mock((*distributionTestSegment).LastDeltaTimestamp).To(func(*distributionTestSegment) uint64 {
			close(entered)
			<-proceed
			return 90
		}).Build()
		m := &SegmentResourceManager{resources: make(map[int64]*segmentResource)}
		r := &segmentResource{manager: m, id: 10, collectionID: 1, scheduled: true}
		base := &segmentBase{caughtUp: true, segment: &metricsTransformSegment{physical: &distributionTestSegment{}}}
		metadata := &querypb.SegmentVersionInfo{
			ID: 10, ManifestPath: "manifest-7", DataVersion: proto.Int32(3),
			IndexInfo: map[int64]*querypb.FieldIndexInfo{20: {BuildID: 30}}, JsonStatsInfo: map[int64]*querypb.JsonStatsInfo{102: {BuildID: 40}},
		}
		r.bases = []*segmentBase{base}
		r.reads = []*preparedSegmentRead{{base: base, distribution: metadata}}
		m.resources[10] = r
		m.resources[11] = &segmentResource{collectionID: 1}
		m.resources[12] = &segmentResource{closing: true, reads: r.reads}
		result := make(chan []*querypb.SegmentVersionInfo, 1)
		go func() { result <- m.SegmentDistribution() }()
		<-entered
		m.mu.Lock()
		require.Equal(t, 1, base.operationRefs)
		r.reads = nil // read cache retirement does not release an in-flight sample's base
		require.Empty(t, r.pruneBasesLocked())
		m.mu.Unlock()
		close(proceed)
		got := <-result
		require.Len(t, got, 1)
		require.EqualValues(t, 90, got[0].LastDeltaTimestamp)
		metadata.IndexInfo[20].BuildID = 99
		metadata.JsonStatsInfo[102].BuildID = 99
		metadata.ManifestPath = "later"
		require.EqualValues(t, 30, got[0].IndexInfo[20].BuildID)
		require.EqualValues(t, 40, got[0].JsonStatsInfo[102].BuildID)
		require.Equal(t, "manifest-7", got[0].ManifestPath)
		require.EqualValues(t, 3, got[0].GetDataVersion())
		m.mu.Lock()
		require.Zero(t, base.operationRefs)
		require.Len(t, r.pruneBasesLocked(), 1)
		m.mu.Unlock()
	})
}

func TestSegmentDistributionTracksPreparedReads(t *testing.T) {
	mockey.PatchConvey("same base reports only successful filtered read generations", t, func() {
		var available atomic.Bool
		f := setupResourceTest(t, func(c CollectionRuntime) error {
			if c.SchemaVersion() == 2 && !available.Load() {
				return context.DeadlineExceeded
			}
			return nil
		})
		defer f.manager.Close()
		f.getMock.To(func(_ *resourceTestMetadata, _ context.Context, c, s int64) (SegmentLoadInfoSnapshot, error) {
			snapshot := resourceSnapshot(c, s, f.manifest.Load())
			snapshot.LoadInfo.DataVersion = int32(f.manifest.Load())
			snapshot.LoadInfo.IndexInfos = []*querypb.FieldIndexInfo{{IndexID: 20, BuildID: 30}, {IndexID: 21, BuildID: 31}}
			return snapshot, nil
		})
		f.dependencyMock.To(func(_ *fakeQueryViewCollectionRuntimeManager, _ context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			meta := view.IntoProto().Meta
			var selected []*indexpb.IndexInfo
			if meta.LoadInfoVersion < 3 {
				selected = []*indexpb.IndexInfo{{IndexID: 19 + int64(meta.LoadInfoVersion)}}
			}
			return &fakeCollectionRuntimeGuard{schemaVersion: int64(meta.SchemaVersion), schema: &schemapb.CollectionSchema{Version: meta.SchemaVersion}, updatedIndexes: selected}, false, nil
		})
		first := f.acquire(1, 1, 1)
		f.await(t, 1)
		initial := f.manager.SegmentDistribution()
		require.Len(t, initial, 1)
		require.Contains(t, initial[0].IndexInfo, int64(20))
		require.NotContains(t, initial[0].IndexInfo, int64(21))
		before := f.manager.DistributionModifyTS()
		f.manifest.Store(2)
		next := f.request(2, 2, 2)
		next.View.Partitions[0].SegmentManifestVersions = []int64{2}
		f.manager.Acquire(next)
		require.Eventually(t, func() bool { return f.prepares.Load() >= 2 }, 5*time.Second, time.Millisecond)
		require.Equal(t, initial, f.manager.SegmentDistribution(), "failed reopen cannot advertise its target")
		available.Store(true)
		f.await(t, 2)
		updated := f.manager.SegmentDistribution()
		require.Len(t, updated, 1)
		require.Equal(t, packed.MarshalManifestPath("test", 2), updated[0].ManifestPath)
		require.EqualValues(t, 2, updated[0].GetDataVersion())
		require.Contains(t, updated[0].IndexInfo, int64(21))
		require.NotContains(t, updated[0].IndexInfo, int64(20))
		require.Greater(t, f.manager.DistributionModifyTS(), before)
		require.EqualValues(t, 1, f.loads.Load(), "new read shares the original physical base")
		droppedIndex := f.request(3, 2, 3)
		droppedIndex.View.Partitions[0].SegmentManifestVersions = []int64{2}
		f.manager.Acquire(droppedIndex)
		f.await(t, 2)
		require.Empty(t, f.manager.SegmentDistribution()[0].IndexInfo, "a dropped index must not survive via initial base metadata")
		f.manager.mu.Lock()
		require.Len(t, f.manager.resources[1000].reads, 3, "old views retain their reads")
		f.manager.mu.Unlock()
		f.drop(t, droppedIndex)
		f.drop(t, next)
		require.Eventually(t, func() bool {
			got := f.manager.SegmentDistribution()
			return len(got) == 1 && got[0].ManifestPath == initial[0].ManifestPath
		}, 5*time.Second, time.Millisecond)
		f.drop(t, first)
		require.Empty(t, f.manager.SegmentDistribution())
	})
}

func TestSegmentDistributionDeduplicatesRetainedBases(t *testing.T) {
	mockey.PatchConvey("new row domain has one logical entry while old physical bases remain owned", t, func() {
		f := setupResourceTest(t, nil)
		defer f.manager.Close()
		first := f.acquire(1, 1, 1)
		f.await(t, 1)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), first.Key, first.View, 0)
		require.NoError(t, err)
		defer handles[0].Release()
		f.manifest.Store(2)
		f.getMock.To(func(_ *resourceTestMetadata, _ context.Context, c, s int64) (SegmentLoadInfoSnapshot, error) {
			snapshot := resourceSnapshot(c, s, 2)
			snapshot.LoadInfo.NumOfRows++ // row-domain change prevents base reuse
			return snapshot, nil
		})
		next := f.request(2, 2, 2)
		next.View.Partitions[0].SegmentManifestVersions = []int64{2}
		f.manager.Acquire(next)
		f.await(t, 2)
		f.manager.mu.Lock()
		require.Len(t, f.manager.resources[1000].bases, 2)
		f.manager.mu.Unlock()
		got := f.manager.SegmentDistribution()
		require.Len(t, got, 1)
		require.Equal(t, packed.MarshalManifestPath("test", 2), got[0].ManifestPath)
		require.EqualValues(t, 2, f.loads.Load())
		dropping := f.beginDrop(first)
		requireDropPending(t, dropping)
		handles[0].Release()
		awaitShutdown(t, dropping)
		f.drop(t, next)
	})
}

func TestPreparedReadDistributionCopiesSelectedArtifacts(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().CommonCfg.EnabledJSONKeyStats.Key
	require.NoError(t, paramtable.Get().Save(key, "true"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	info := &querypb.SegmentLoadInfo{
		SegmentID: 10, CollectionID: 1, PartitionID: 2, InsertChannel: "channel", Level: datapb.SegmentLevel_L1, IsSorted: true, DataVersion: 3,
		IndexInfos: []*querypb.FieldIndexInfo{{IndexID: 20, BuildID: 30}}, JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
			101: {FieldID: 101, BuildID: 40, Version: 7, JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion}, 102: nil, 103: {JsonKeyStatsDataFormat: -1},
		},
	}
	got := preparedReadDistribution(info)
	require.Len(t, got.JsonStatsInfo, 1)
	require.EqualValues(t, 40, got.JsonStatsInfo[101].BuildID)
	info.IndexInfos[0].BuildID = 99
	info.JsonKeyStatsLogs[101].BuildID = 99
	require.EqualValues(t, 30, got.IndexInfo[20].BuildID)
	require.EqualValues(t, 40, got.JsonStatsInfo[101].BuildID)
	require.EqualValues(t, 0, got.Version)
	require.NoError(t, paramtable.Get().Save(key, "false"))
	require.Empty(t, preparedReadDistribution(info).JsonStatsInfo)
	t.Run("stats-resolution-fallback", func(t *testing.T) {
		mockey.PatchConvey("unavailable manifest stats retain load-info fallback", t, func() {
			require.NoError(t, paramtable.Get().Save(key, "true"))
			mockey.Mock((*packed.StatsResultWithErr).Err).Return(context.Canceled).Build()
			require.Len(t, preparedReadDistribution(info).JsonStatsInfo, 1)
		})
	})
}

func TestDistributionTimestampTracksTransforms(t *testing.T) {
	mockey.PatchConvey("deletes invalidate distribution including partial failures; polling does not", t, func() {
		m := &SegmentResourceManager{}
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for range 50 {
					m.markDistributionChanged()
				}
			})
		}
		wg.Wait()
		before := m.DistributionModifyTS()
		require.Positive(t, before)
		m.distributionModifyTs.Store(before + int64(time.Hour))
		before = m.DistributionModifyTS()
		s := &transformStartSegment{TransformSegment: &fakeTransformSegment{}, onTransform: m.markDistributionChanged}
		require.NoError(t, s.ApplyTransform(t.Context(), nil))
		require.Equal(t, before, m.DistributionModifyTS())
		entry := &streamingpb.TransformLogEntry{Entry: &streamingpb.TransformLogEntry_Delete{Delete: &streamingpb.TransformDeleteEntry{}}}
		require.NoError(t, s.ApplyTransform(t.Context(), entry))
		require.Greater(t, m.DistributionModifyTS(), before)
		mockey.Mock((*fakeTransformSegment).ApplyTransform).Return(context.Canceled).Build()
		before = m.DistributionModifyTS()
		require.ErrorIs(t, s.ApplyTransform(t.Context(), entry), context.Canceled)
		require.Greater(t, m.DistributionModifyTS(), before)
		s.onTransform = nil
		require.ErrorIs(t, s.ApplyTransform(t.Context(), entry), context.Canceled)
	})
}
