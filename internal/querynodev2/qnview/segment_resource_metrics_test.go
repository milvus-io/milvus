package qnview

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

type metricsTransformSegment struct {
	TransformSegment
	physical segments.Segment
}

func (s *metricsTransformSegment) QuerySegment() segments.Segment { return s.physical }

type metricsTestSegment struct{ segments.Segment }

func (*metricsTestSegment) ID() int64                             { panic("unpatched") }
func (*metricsTestSegment) Collection() int64                     { panic("unpatched") }
func (*metricsTestSegment) Partition() int64                      { panic("unpatched") }
func (*metricsTestSegment) Type() segments.SegmentType            { panic("unpatched") }
func (*metricsTestSegment) ResourceGroup() string                 { panic("unpatched") }
func (*metricsTestSegment) MemSize() int64                        { panic("unpatched") }
func (*metricsTestSegment) InsertCount() int64                    { panic("unpatched") }
func (*metricsTestSegment) Indexes() []*segments.IndexedFieldInfo { panic("unpatched") }
func (*metricsTestSegment) HasRawData(int64) bool                 { panic("unpatched") }

func TestSegmentResourceMetricsRetainsPhysicalBases(t *testing.T) {
	mockey.PatchConvey("metrics protects physical bases during concurrent retirement", t, func() {
		mockey.Mock((*metricsTestSegment).ID).Return(int64(10)).Build()
		mockey.Mock((*metricsTestSegment).Collection).Return(int64(1)).Build()
		mockey.Mock((*metricsTestSegment).Partition).Return(int64(2)).Build()
		mockey.Mock((*metricsTestSegment).Type).Return(segments.SegmentTypeSealed).Build()
		mockey.Mock((*metricsTestSegment).ResourceGroup).Return("rg").Build()
		mockey.Mock((*metricsTestSegment).InsertCount).Return(int64(100)).Build()
		mockey.Mock((*metricsTestSegment).Indexes).Return([]*segments.IndexedFieldInfo{{IndexInfo: &querypb.FieldIndexInfo{FieldID: 101, IndexID: 20, BuildID: 30}, IsLoaded: true}}).Build()
		mockey.Mock((*metricsTestSegment).HasRawData).Return(true).Build()
		entered, proceed := make(chan struct{}, 2), make(chan struct{})
		mockey.Mock((*metricsTestSegment).MemSize).To(func(*metricsTestSegment) int64 { entered <- struct{}{}; <-proceed; return 50 }).Build()
		// Several Views and read generations share each base. No read/view count
		// participates in metrics; separate physical bases do add their memory.
		m := &SegmentResourceManager{resources: make(map[int64]*segmentResource)}
		r := &segmentResource{manager: m, id: 10, collectionID: 1, scheduled: true}
		base := func() *segmentBase {
			return &segmentBase{segment: &transformStartSegment{TransformSegment: &metricsTransformSegment{physical: &metricsTestSegment{}}}}
		}
		r.bases = []*segmentBase{base(), base()}
		m.resources[10] = r
		m.resources[11] = &segmentResource{collectionID: 1} // No physical resources yet.
		m.resources[12] = &segmentResource{collectionID: 1, closing: true, bases: []*segmentBase{base()}}
		require.Empty(t, m.SegmentMetrics(99, 5))
		result := make(chan []*metricsinfo.Segment, 1)
		go func() { result <- m.SegmentMetrics(1, 5) }()
		<-entered
		m.mu.Lock()
		for _, base := range r.bases {
			require.Equal(t, 1, base.operationRefs)
		}
		require.Empty(t, r.pruneBasesLocked(), "no View remains, but in-flight sampling retains both bases")
		m.mu.Unlock()
		close(proceed)
		got := <-result
		require.Len(t, got, 1)
		require.EqualValues(t, 100, got[0].MemSize)
		require.EqualValues(t, 100, got[0].LoadedInsertRowCount, "do not multiply rows by View/base count")
		require.EqualValues(t, 5, got[0].NodeID)
		require.Len(t, got[0].IndexedFields, 1)
		require.True(t, got[0].IndexedFields[0].HasRawData)
		m.mu.Lock()
		for _, base := range r.bases {
			require.Zero(t, base.operationRefs)
		}
		require.Len(t, r.pruneBasesLocked(), 2, "retirement becomes possible after sampling")
		m.mu.Unlock()
	})
}

func TestSegmentResourceDroppedWaitsForMetricsAndPhysicalRelease(t *testing.T) {
	mockey.PatchConvey("metrics pins delay destruction but cannot bypass the last binding's Dropped promise", t, func() {
		f := setupResourceTest(t, nil)
		mockey.Mock((*metricsTestSegment).ID).Return(int64(1000)).Build()
		mockey.Mock((*metricsTestSegment).Collection).Return(int64(1)).Build()
		mockey.Mock((*metricsTestSegment).Partition).Return(int64(10)).Build()
		mockey.Mock((*metricsTestSegment).Type).Return(segments.SegmentTypeSealed).Build()
		mockey.Mock((*metricsTestSegment).ResourceGroup).Return("rg").Build()
		mockey.Mock((*metricsTestSegment).InsertCount).Return(int64(100)).Build()
		mockey.Mock((*metricsTestSegment).Indexes).Return([]*segments.IndexedFieldInfo(nil)).Build()
		f.loadMock.To(func(_ *resourceTestLoader, _ context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
			return &metricsTransformSegment{
				TransformSegment: &fakeTransformSegment{id: info.SegmentID, partitionID: info.PartitionID},
				physical:         &metricsTestSegment{},
			}, nil
		})
		sampling, finishSampling := make(chan struct{}), make(chan struct{})
		releasing, finishRelease := make(chan struct{}), make(chan struct{})
		unblockSampling := sync.OnceFunc(func() { close(finishSampling) })
		unblockRelease := sync.OnceFunc(func() { close(finishRelease) })
		defer unblockSampling()
		defer unblockRelease()
		mockey.Mock((*metricsTestSegment).MemSize).To(func(*metricsTestSegment) int64 {
			close(sampling)
			<-finishSampling
			return 50
		}).Build()
		f.releaseMock.To(func(*fakeTransformSegment, context.Context) error {
			close(releasing)
			<-finishRelease
			return nil
		})
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		result := make(chan []*metricsinfo.Segment, 1)
		go func() { result <- f.manager.SegmentMetrics(0, 5) }()
		awaitShutdown(t, sampling)
		dropped := f.beginDrop(req)
		f.manager.mu.Lock()
		r := f.manager.resources[1000]
		require.True(t, r.closing)
		require.Empty(t, r.bindings)
		f.manager.mu.Unlock()
		require.Empty(t, f.manager.SegmentMetrics(0, 5), "Closing refuses additional sampling pins")
		requireDropPending(t, dropped)
		requireDropPending(t, releasing)
		unblockSampling()
		require.Len(t, <-result, 1)
		awaitShutdown(t, releasing)
		requireDropPending(t, dropped)
		unblockRelease()
		awaitShutdown(t, dropped)
		awaitShutdown(t, r.done)
		f.manager.Close()
	})
}
