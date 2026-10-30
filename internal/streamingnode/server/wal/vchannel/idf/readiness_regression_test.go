package idf

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Recovery can register multiple retained schemas while the initial runtime is
// building. Their preparation order must not determine which views can be Ready.
func TestQueryViewReadyAfterOlderSchemaPreparation(t *testing.T) {
	paramtable.Init()
	for _, oldHasBM25 := range []bool{true, false} {
		name := "old schema with BM25"
		if !oldHasBM25 {
			name = "old schema without BM25"
		}
		t.Run(name, func(t *testing.T) {
			scheduler := nodescheduler.New(1)
			defer scheduler.Close()
			dispatcher := queryresource.NewDispatcher(1)
			defer dispatcher.Close()
			provider := NewProvider(&mocks.MockMixCoordClient{})
			patch := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
			defer patch.UnPatch()
			manager := queryresource.NewManager(queryresource.Config{
				Scheduler: scheduler, Dispatcher: dispatcher,
				Builders: []queryresource.QueryRuntimeModuleBuilder{provider},
			})
			defer manager.Close()
			version := qviews.DataVersion{StreamingVersion: 10}
			view := walview.VChannelWALView{
				CollectionID: 1, VChannel: "v1", Schema: &schemapb.CollectionSchema{Version: 1},
				SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: version},
			}
			view.CaptureQueryView = func(meta *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
				next := view
				next.LoadInfoVersion = meta.GetLoadInfoVersion()
				next.Schema = &schemapb.CollectionSchema{Version: meta.GetSchemaVersion()}
				if meta.GetSchemaVersion() == 3 || oldHasBM25 {
					next.Schema = fieldTestSchema(meta.GetSchemaVersion(), 102)
				}
				capture(next)
				return true
			}
			build := func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) { return view, true }
			// S3 is rechecked after S2 completes, including a disabled S2 oracle.
			// Load identities are hashes: a numerically lower identity is valid.
			keys := []struct {
				SchemaVersion   int32
				LoadInfoVersion uint64
			}{
				{SchemaVersion: 1},
				{SchemaVersion: 3, LoadInfoVersion: 90},
				{SchemaVersion: 2, LoadInfoVersion: 80},
				{SchemaVersion: 3, LoadInfoVersion: 90},
				{SchemaVersion: 3, LoadInfoVersion: 7},
				{SchemaVersion: 3, LoadInfoVersion: 90},
			}
			for i, identity := range keys {
				key := qviews.QueryViewKey{
					ShardID:          qviews.ShardID{ReplicaID: 1, VChannel: "v1"},
					QueryViewVersion: qviews.QueryViewVersion{DataVersion: version, QueryVersion: int64(i + 1)},
				}
				meta := &viewpb.QueryViewMeta{SchemaVersion: identity.SchemaVersion, LoadInfoVersion: identity.LoadInfoVersion, Version: key.QueryViewVersion.IntoProto()}
				ready := make(chan struct{})
				manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnReady: sync.OnceFunc(func() { close(ready) })}, build)
				select {
				case <-ready:
				case <-time.After(5 * time.Second):
					t.Fatalf("view %v with read identity %v did not become Ready", key, identity)
				}
			}
		})
	}
}

func TestReleaseAdvancesAllReadSchemasBeforeOldFilesCanDrop(t *testing.T) {
	first, resource := newTestOracle(t)
	second, _ := newTestOracle(t)
	descriptor := resource(20, 2)
	version := qviews.DataVersion{StreamingVersion: 11}
	resources := []*datapb.StreamingNodeBM25Resource{descriptor}
	mockViewResources(t, first, &version, &resources)
	require.NoError(t, first.PrepareDataVersion(t.Context(), version))
	require.NoError(t, second.PrepareDataVersion(t.Context(), version))
	module := &Runtime{minDataVersion: version, fields: map[fieldKey]*fieldResource{
		{scope: "a", fieldID: 102}: {oracle: first}, {scope: "b", fieldID: 102}: {oracle: second},
	}}
	defer module.Close()
	path := descriptor.GetBm25Binlogs()[0].GetBinlogs()[0].GetLogPath()
	data, err := first.provider.chunkManager.Read(t.Context(), path)
	require.NoError(t, err)
	require.NoError(t, first.provider.chunkManager.Remove(t.Context(), path))
	version.StreamingVersion++
	resources = nil // advancing needs to subtract the old on-disk contribution
	require.Error(t, module.BeforeRelease(t.Context(), version), "missing old stats must prevent Dropped")
	require.Equal(t, version, module.minDataVersion, "a failed release must still fence new snapshots")
	require.NoError(t, first.provider.chunkManager.Write(t.Context(), path, data))
	require.NoError(t, module.BeforeRelease(t.Context(), version))
	require.Equal(t, version, first.currentVersion)
	require.Equal(t, version, second.currentVersion)
	require.Empty(t, first.currentSealed)
	require.Empty(t, second.currentSealed)
	require.NoError(t, first.provider.chunkManager.Remove(t.Context(), path))
	require.NoError(t, module.BeforeRelease(t.Context(), qviews.DataVersion{StreamingVersion: 10}))
	require.Equal(t, version, module.minDataVersion)
}

func TestSchemaPreparationAndReleaseShareDataViewFloor(t *testing.T) {
	paramtable.Init()
	captured, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	captureVersions := make(chan qviews.DataVersion, 3)
	module := &Runtime{}
	defer module.Close()
	require.NoError(t, module.Prepare(t.Context(), walview.VChannelWALView{
		Schema:          &schemapb.CollectionSchema{Version: 1},
		SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}},
		CaptureQueryView: func(meta *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
			version := qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion())
			captureVersions <- version
			if meta.GetSchemaVersion() == 3 {
				close(captured)
				<-resume
			}
			capture(walview.VChannelWALView{
				Schema:          &schemapb.CollectionSchema{Version: meta.GetSchemaVersion()},
				SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: version},
			})
			return true
		},
	}))
	meta := &viewpb.QueryViewMeta{SchemaVersion: 3, Version: &viewpb.QueryViewVersion{DataVersion: (&qviews.DataVersion{StreamingVersion: 10}).IntoProto()}}
	prepared := make(chan error, 1)
	go func() {
		prepared <- module.BindView(proto.Clone(meta).(*viewpb.QueryViewMeta)).Prepare(t.Context(), nil)
	}()
	select {
	case <-captured:
	case <-time.After(5 * time.Second):
		t.Fatal("schema preparation did not start")
	}
	floor := qviews.DataVersion{StreamingVersion: 20}
	require.ErrorIs(t, module.BeforeRelease(t.Context(), floor), merr.ErrServiceNotReady)
	unblock()
	require.NoError(t, <-prepared)
	require.NoError(t, module.BeforeRelease(t.Context(), floor))
	meta.SchemaVersion = 2
	require.NoError(t, module.BindView(proto.Clone(meta).(*viewpb.QueryViewMeta)).Prepare(t.Context(), nil))
	require.EqualValues(t, 10, (<-captureVersions).StreamingVersion)
	require.Equal(t, floor, <-captureVersions, "a disabled default oracle cannot erase the release floor")
	require.EqualValues(t, 10, meta.GetVersion().GetDataVersion().GetStreamingVersion(), "preparation must not mutate View metadata")
	require.NoError(t, module.BeforeRelease(context.Background(), qviews.DataVersion{StreamingVersion: 15}))
	meta.SchemaVersion = 4
	require.NoError(t, module.BindView(proto.Clone(meta).(*viewpb.QueryViewMeta)).Prepare(t.Context(), nil))
	require.Equal(t, floor, <-captureVersions)
}

func TestReleaseInFlightBlocksNewSchemaSnapshot(t *testing.T) {
	oracle, _ := newTestOracle(t)
	advancing, resume := make(chan qviews.DataVersion, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	captures := 0
	module := &Runtime{
		fields:         map[fieldKey]*fieldResource{{fieldID: 102}: {oracle: oracle}},
		minDataVersion: qviews.DataVersion{StreamingVersion: 10},
	}
	module.cond = sync.NewCond(&module.mu)
	defer module.Close()
	floor := qviews.DataVersion{StreamingVersion: 20}
	module.view.CaptureQueryView = func(meta *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		captures++
		require.Equal(t, floor, qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion()))
		capture(walview.VChannelWALView{
			Schema:          &schemapb.CollectionSchema{Version: 2},
			SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: floor},
		})
		return true
	}
	patch := mockey.Mock((*oracleRuntime).PrepareDataVersion).To(func(_ *oracleRuntime, _ context.Context, target qviews.DataVersion) error {
		advancing <- target
		<-resume
		return nil
	}).Build()
	defer patch.UnPatch()
	released := make(chan error, 1)
	go func() { released <- module.BeforeRelease(t.Context(), floor) }()
	select {
	case target := <-advancing:
		require.Equal(t, floor, target)
	case <-time.After(5 * time.Second):
		t.Fatal("release did not start advancing the retained oracle")
	}
	meta := &viewpb.QueryViewMeta{SchemaVersion: 2, Version: &viewpb.QueryViewVersion{DataVersion: (&qviews.DataVersion{StreamingVersion: 10}).IntoProto()}}
	require.ErrorIs(t, module.BindView(proto.Clone(meta).(*viewpb.QueryViewMeta)).Prepare(t.Context(), nil), merr.ErrServiceNotReady)
	require.Zero(t, captures)
	unblock()
	require.NoError(t, <-released)
	require.NoError(t, module.BindView(proto.Clone(meta).(*viewpb.QueryViewMeta)).Prepare(t.Context(), nil))
	require.Equal(t, 1, captures)
}

func TestReleaseWaitsForCanceledLazyReaderToExit(t *testing.T) {
	oracle := newLazyOracle(t)
	module := &Runtime{fields: map[fieldKey]*fieldResource{{fieldID: 102}: {oracle: oracle}}}
	defer module.Close()
	entered, canceled, unwind := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(unwind) })
	defer unblock()
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, _ qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-unwind
		return nil, ctx.Err()
	}).Build()
	defer patch.UnPatch()
	queryCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	queryDone := make(chan error, 1)
	go func() { queryDone <- oracle.ensureMaterialized(queryCtx) }()
	<-entered
	cancel()
	require.ErrorIs(t, <-queryDone, context.Canceled)
	released := make(chan error, 1)
	go func() { released <- module.BeforeRelease(t.Context(), qviews.DataVersion{StreamingVersion: 11}) }()
	<-canceled
	require.Never(t, func() bool { return len(released) > 0 }, 20*time.Millisecond, time.Millisecond)
	unblock()
	require.NoError(t, <-released)
}
