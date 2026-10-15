package idf

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Events already represented by the snapshot can still be queued in the parent
// dispatcher. Filter their WAL prefix, and ignore sealed notifications covered
// by the target DataView. Repeated later seal notifications are idempotent.
type schemaRefresh struct {
	timeTick uint64
	version  qviews.DataVersion
	pending  []walview.VChannelResourceEvent
	err      error
}

func (s *schemaRefresh) includes(event walview.VChannelResourceEvent) bool {
	if event.Message != nil {
		return event.Message.TimeTick() > s.timeTick
	}
	return event.SegmentSealed != nil && event.SegmentSealed.SealedAtDataVersion.GT(s.version)
}

func (r *Runtime) SchemaVersion() int32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.schemaVersion
}

// PrepareSchema keeps the current oracle serving until a fresh no-gap snapshot
// and its live tail are fully prepared. It never replays GrowingRuntime or Delete
// resources, and never publishes a partially rebuilt BM25 aggregate.
func (r *Runtime) PrepareSchema(ctx context.Context, meta *viewpb.QueryViewMeta, loadInfo queryresource.LoadInfoProvider) error {
	if !r.prepareMu.TryLock() {
		return merr.WrapErrServiceNotReadyMsg("BM25 resource preparation is in progress")
	}
	defer r.prepareMu.Unlock()
	r.mu.RLock()
	base, closed := r.view, r.closed
	_, prepared := r.oracles[queryresource.ReadSchemaKey{SchemaVersion: meta.GetSchemaVersion(), LoadInfoVersion: meta.GetLoadInfoVersion()}]
	oracle := r.oracle
	r.mu.RUnlock()
	if closed {
		return context.Canceled
	}
	if prepared {
		return nil
	}
	if base.CaptureQueryView == nil {
		return merr.WrapErrServiceInternalMsg("BM25 schema refresh has no WAL snapshot provider")
	}
	// An older retained view must not roll the shared aggregate back to files
	// whose DataView has already been released after a newer preparation.
	target := proto.Clone(meta).(*viewpb.QueryViewMeta)
	if oracle != nil {
		oracle.mu.RLock()
		version := oracle.currentVersion
		oracle.mu.RUnlock()
		if version.GT(qviews.FromProtoDataVersion(target.GetVersion().GetDataVersion())) {
			target.Version.DataVersion = version.IntoProto()
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if base.RuntimeContext != nil {
		stop := context.AfterFunc(base.RuntimeContext, cancel)
		defer stop()
	}
	stage := &schemaRefresh{}
	defer func() {
		r.mu.Lock()
		r.refresh = nil
		r.cond.Broadcast()
		r.mu.Unlock()
	}()
	// Cancellation must wake a full event queue before QueryRuntime.Close waits
	// for its application lock. It must not wait for an object-storage read.
	stop := context.AfterFunc(ctx, func() {
		r.mu.Lock()
		stage.err = ctx.Err()
		r.cond.Broadcast()
		r.mu.Unlock()
	})
	defer stop()
	var view walview.VChannelWALView
	if !base.CaptureQueryView(target, func(captured walview.VChannelWALView) {
		view = captured
		r.mu.Lock()
		stage.timeTick = view.BaseGrowingTimeTick
		stage.version = view.SegmentSnapshot.DataVersion
		r.refresh = stage
		r.mu.Unlock()
	}) {
		return merr.WrapErrServiceNotReadyMsg("BM25 schema snapshot is not ready")
	}
	if view.Schema.GetVersion() != meta.GetSchemaVersion() {
		return merr.WrapErrServiceNotReadyMsg("BM25 schema snapshot version mismatch")
	}
	view.ResourceEventBarrier = base.ResourceEventBarrier
	view.RuntimeContext = base.RuntimeContext
	if view.LoadInfoVersion != 0 && loadInfo != nil {
		info, err := loadInfo.QueryViewLoadInfo(ctx, view.CollectionID, view.LoadInfoVersion)
		if err != nil {
			return err
		}
		view.PartitionIDs = append([]int64{}, info.PartitionIDs...)
		view.LoadFields = info.LoadFields
		view.RequiredFields = qviews.ResolveRequiredFields(view.Schema, info.RequiredFields)
	}
	var next *oracleRuntime
	if hasLoadedBM25Function(view.Schema, loadFieldIDs(view.RequiredFields)) {
		provider, err := r.resolveProvider(ctx)
		if err != nil {
			return err
		}
		next, err = provider.buildOracle(ctx, view, paramtable.Get().QueryViewCfg.IDFLazyLoadSealedStats.GetAsBool())
		if err != nil {
			return err
		}
	}
	published := false
	defer func() {
		if !published && next != nil {
			next.Close()
		}
	}()
	for {
		r.mu.Lock()
		if r.closed || ctx.Err() != nil {
			r.mu.Unlock()
			return context.Canceled
		}
		batch := stage.pending
		stage.pending = nil
		r.cond.Broadcast()
		if len(batch) == 0 {
			r.oracle, r.disabled = next, next == nil
			r.oracles[queryresource.ReadSchemaKey{SchemaVersion: view.Schema.GetVersion(), LoadInfoVersion: view.LoadInfoVersion}] = next
			r.schemaVersion = view.Schema.GetVersion()
			r.loadInfoVersion = view.LoadInfoVersion
			r.refresh = nil
			published = true
			r.mu.Unlock()
			return nil
		}
		r.mu.Unlock()
		if next != nil {
			for _, event := range batch {
				next.ApplyLiveEvent(ctx, event)
			}
		}
	}
}
