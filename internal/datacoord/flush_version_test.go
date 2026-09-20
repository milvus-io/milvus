package datacoord

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *ServerSuite) setupFlushVersionCatalog() (*datacoordkv.Catalog, dataview.Manager) {
	catalog := datacoordkv.NewCatalog(NewMetaMemoryKV(), "", "")
	manager := dataview.NewManager(catalog, nil)
	_, err := manager.OnCreateCollection(context.Background(), dataview.CreateCollectionDataViewEvent{
		CollectionID: 100, VChannels: []string{"ch1"},
	})
	require.NoError(s.T(), err)
	s.testServer.meta.catalog = catalog
	s.testServer.dataViewManager = manager
	return catalog, manager
}

func (s *ServerSuite) addFlushVersionSegment(id, rows int64) *datapb.SaveBinlogPathsRequest {
	require.NoError(s.T(), s.testServer.meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID: id, CollectionID: 100, PartitionID: 10, InsertChannel: "ch1",
		State: commonpb.SegmentState_Growing, Level: datapb.SegmentLevel_L1,
		NumOfRows: rows, StorageVersion: storage.StorageV2,
	})))
	req := &datapb.SaveBinlogPathsRequest{
		SegmentID: id, CollectionID: 100, PartitionID: 10, Channel: "ch1",
		Flushed: true, SegLevel: datapb.SegmentLevel_L1, WithFullBinlogs: true,
		StorageVersion: storage.StorageV2,
	}
	if rows > 0 {
		req.Field2BinlogPaths = []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: id, EntriesNum: rows}}}}
	}
	return req
}

func (s *ServerSuite) flushVersion(req *datapb.SaveBinlogPathsRequest) *viewpb.DataVersion {
	resp, err := s.testServer.SaveBinlogPaths(context.Background(), proto.Clone(req).(*datapb.SaveBinlogPathsRequest))
	require.NoError(s.T(), merr.CheckRPCCall(resp, err))
	version, err := dataview.ParseFlushResult(resp)
	require.NoError(s.T(), err)
	return version
}

func (s *ServerSuite) TestFlushVersionSurvivesRetryRecoveryAndDataViewGC() {
	ctx := context.Background()
	catalog, manager := s.setupFlushVersionCatalog()
	reqA := s.addFlushVersionSegment(10, 10)
	a := s.flushVersion(reqA)
	require.Equal(s.T(), int64(2), a.GetStreamingVersion())
	b := s.flushVersion(s.addFlushVersionSegment(20, 10))
	require.Equal(s.T(), int64(3), b.GetStreamingVersion())
	require.True(s.T(), proto.Equal(a, s.flushVersion(reqA)))

	// The first publication cannot be recovered from old DataViews after GC.
	require.NoError(s.T(), manager.GarbageCollect(ctx, 100, 1))
	old, err := manager.Get(ctx, 100, a)
	require.NoError(s.T(), err)
	require.Nil(s.T(), old)

	// Reload both sides from catalog, simulating a coordinator restart after
	// the first RPC response was lost and other segments advanced the view.
	segments, err := catalog.ListSegments(ctx, 100)
	require.NoError(s.T(), err)
	for _, segment := range segments {
		s.testServer.meta.segments.SetSegment(segment.GetID(), NewSegmentInfo(segment))
	}
	recovered, err := dataview.RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) {
		return true, nil
	}, nil, nil, nil)
	require.NoError(s.T(), err)
	s.testServer.dataViewManager = recovered
	require.True(s.T(), proto.Equal(a, s.flushVersion(reqA)))

	// Compaction may retire the source before SN retries. The original version
	// must survive, and a retry must not put the source back into membership.
	require.NoError(s.T(), s.testServer.meta.UpdateSegmentsInfo(ctx, UpdateStatusOperator(10, commonpb.SegmentState_Dropped)))
	_, err = recovered.RecomputeNow(ctx, 100, func(context.Context, int64) ([]dataview.LoadableSegment, error) {
		return []dataview.LoadableSegment{{SegmentID: 20, VChannel: "ch1", PartitionID: 10, RowNum: 10}}, nil
	})
	require.NoError(s.T(), err)
	require.True(s.T(), proto.Equal(a, s.flushVersion(reqA)))
	ref, err := recovered.Latest(ctx, 100)
	require.NoError(s.T(), err)
	defer ref.Deref()
	require.Equal(s.T(), []int64{20}, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func (s *ServerSuite) TestFlushVersionConcurrentRetry() {
	_, manager := s.setupFlushVersionCatalog()
	req := s.addFlushVersionSegment(10, 10)
	const count = 8
	statuses := make(chan *commonpb.Status, count)
	errors := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := s.testServer.SaveBinlogPaths(context.Background(), proto.Clone(req).(*datapb.SaveBinlogPathsRequest))
			statuses <- resp
			errors <- err
		}()
	}
	wg.Wait()
	for range count {
		require.NoError(s.T(), <-errors)
		status := <-statuses
		require.True(s.T(), merr.Ok(status))
		version, err := dataview.ParseFlushResult(status)
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(2), version.GetStreamingVersion())
	}
	ref, err := manager.Latest(context.Background(), 100)
	require.NoError(s.T(), err)
	defer ref.Deref()
	require.Equal(s.T(), int64(2), ref.Version().GetStreamingVersion())
	require.True(s.T(), proto.Equal(ref.Version(), s.testServer.meta.GetSegment(context.Background(), 10).GetSealedAtDataVersion()))
}

func (s *ServerSuite) TestFlushVersionEmptyAndDropped() {
	_, manager := s.setupFlushVersionCatalog()
	req := s.addFlushVersionSegment(10, 0)
	require.Nil(s.T(), s.flushVersion(req))
	require.Nil(s.T(), s.flushVersion(req))
	require.Equal(s.T(), commonpb.SegmentState_Dropped, s.testServer.meta.GetSegment(context.Background(), 10).GetState())
	ref, err := manager.Latest(context.Background(), 100)
	require.NoError(s.T(), err)
	defer ref.Deref()
	require.Equal(s.T(), int64(1), ref.Version().GetStreamingVersion())

	req = s.addFlushVersionSegment(20, 10)
	require.NoError(s.T(), s.testServer.meta.UpdateSegmentsInfo(context.Background(), UpdateStatusOperator(20, commonpb.SegmentState_Dropped)))
	require.Nil(s.T(), s.flushVersion(req))
}

func (s *ServerSuite) TestFlushVersionFailedPublicationDoesNotInstallBinding() {
	_, manager := s.setupFlushVersionCatalog()
	req := s.addFlushVersionSegment(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	patch := mockey.Mock((*datacoordkv.Catalog).Update).To(func(_ *datacoordkv.Catalog, _ context.Context, _ ...metastore.UpdateAction) error {
		cancel()
		return context.Canceled
	}).Build()
	defer patch.UnPatch()
	resp, err := s.testServer.SaveBinlogPaths(ctx, req)
	require.Error(s.T(), merr.CheckRPCCall(resp, err))
	segment := s.testServer.meta.GetSegment(context.Background(), 10)
	require.Nil(s.T(), segment.GetSealedAtDataVersion())
	require.Equal(s.T(), commonpb.SegmentState_Growing, segment.GetState())
	ref, err := manager.Latest(context.Background(), 100)
	require.NoError(s.T(), err)
	defer ref.Deref()
	require.Equal(s.T(), int64(1), ref.Version().GetStreamingVersion())
	patch.UnPatch()
	require.Equal(s.T(), int64(2), s.flushVersion(req).GetStreamingVersion())
}

// A binding is a completion proof only if its DataView was committed as well.
// Force the catalog's large-binlog fallback and fail its final transaction.
func TestFlushVersionChunkedCatalogCommit(t *testing.T) {
	ctx := context.Background()
	store := NewMetaMemoryKV()
	catalog := datacoordkv.NewCatalog(store, "", "")
	segment := &datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 10, NumOfRows: 10, State: commonpb.SegmentState_Growing}
	require.NoError(t, catalog.AddSegment(ctx, segment))
	segment.State = commonpb.SegmentState_Flushed
	segment.SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 2}
	for field := int64(100); field < 106; field++ {
		segment.Binlogs = append(segment.Binlogs, &datapb.FieldBinlog{FieldID: field, Binlogs: []*datapb.Binlog{{LogID: field, EntriesNum: 10}}})
	}
	view := &viewpb.DataViewOfCollection{CollectionId: 100, DataVersion: segment.SealedAtDataVersion}
	limit := mockey.Mock((*memkv.MemoryKV).MaxTxnOps).Return(2).Build()
	defer limit.UnPatch()
	fail := true
	finalCalls := 0
	var original func(*memkv.MemoryKV, context.Context, map[string]string, []string, ...predicates.Predicate) error
	patch := mockey.Mock((*memkv.MemoryKV).MultiSaveAndRemove).Origin(&original).To(
		func(kv *memkv.MemoryKV, ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
			finalCalls++
			require.Len(t, saves, 2, "the version binding and DataView must commit together")
			for key, value := range saves {
				if strings.HasPrefix(key, datacoordkv.SegmentPrefix+"/") {
					record := &datapb.SegmentInfo{}
					require.NoError(t, proto.Unmarshal([]byte(value), record))
					require.True(t, proto.Equal(segment.SealedAtDataVersion, record.GetSealedAtDataVersion()))
				}
			}
			if fail {
				return merr.WrapErrServiceUnavailableMsg("injected final transaction failure")
			}
			return original(kv, ctx, saves, removals, preds...)
		}).Build()
	defer patch.UnPatch()
	actions := []metastore.UpdateAction{{Type: metastore.ActionUpdate, Entry: metastore.SegmentEntry{
		Segment: segment, AlterEncoding: true, Binlogs: []metastore.BinlogsIncrement{{Segment: segment}},
	}}, metastore.SaveDataView(view)}
	require.Error(t, catalog.Update(ctx, actions...))
	stored, err := catalog.ListSegments(ctx, 100)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Nil(t, stored[0].GetSealedAtDataVersion(), "pre-commit binlog writes must not publish the version")
	require.Equal(t, commonpb.SegmentState_Growing, stored[0].GetState())
	views, err := catalog.ListAllDataViews(ctx)
	require.NoError(t, err)
	require.Empty(t, views)
	fail = false
	require.NoError(t, catalog.Update(ctx, actions...))
	require.Equal(t, 2, finalCalls)
	stored, err = catalog.ListSegments(ctx, 100)
	require.NoError(t, err)
	require.True(t, proto.Equal(segment.SealedAtDataVersion, stored[0].GetSealedAtDataVersion()))
	views, err = catalog.ListAllDataViews(ctx)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.True(t, proto.Equal(stored[0].GetSealedAtDataVersion(), views[0].GetDataVersion()))
}

// Compaction preserves first-publication bindings but publishes a different view;
// its many source records must remain eligible for the chunked write phase.
func TestFlushVersionDoesNotExpandCompactionCommit(t *testing.T) {
	ctx := context.Background()
	catalog := datacoordkv.NewCatalog(NewMetaMemoryKV(), "", "")
	limit := mockey.Mock((*memkv.MemoryKV).MaxTxnOps).Return(2).Build()
	defer limit.UnPatch()
	var actions []metastore.UpdateAction
	for id := int64(1); id <= 5; id++ {
		actions = append(actions, metastore.UpdateAction{Type: metastore.ActionUpdate, Entry: metastore.SegmentEntry{
			Segment: &datapb.SegmentInfo{
				ID: id, CollectionID: 100, PartitionID: 10,
				State: commonpb.SegmentState_Dropped, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 2},
			},
		}})
	}
	actions = append(actions, metastore.SaveDataView(&viewpb.DataViewOfCollection{
		CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1},
	}))
	require.NoError(t, catalog.Update(ctx, actions...))
	segments, err := catalog.ListSegments(ctx, 100)
	require.NoError(t, err)
	require.Len(t, segments, 5)
	views, err := catalog.ListAllDataViews(ctx)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, int64(1), views[0].GetDataVersion().GetCompactVersion())
}
