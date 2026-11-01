package idf

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

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

// VChannel is implicit in the owning Runtime. Schema/load versions identify
// bindings, while field semantics and the actual partition set identify data.
type fieldKey struct {
	scope     string
	fieldID   int64
	signature string
}
type fieldResource struct {
	key      fieldKey
	refs     int
	oracle   *oracleRuntime
	coverage schemaRefresh
}
type viewBinding struct {
	runtime  *Runtime
	meta     *viewpb.QueryViewMeta
	fields   []*fieldResource
	resolved bool
}

func (r *Runtime) BindView(meta *viewpb.QueryViewMeta) queryresource.QueryViewResource {
	return &viewBinding{runtime: r, meta: meta}
}

func partitionScopeKey(ids []int64) string {
	if ids == nil {
		return "all"
	}
	ids = slices.Clone(ids)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	return fmt.Sprint(ids)
}

func (b *viewBinding) Prepare(ctx context.Context, loadInfo queryresource.LoadInfoProvider) error {
	r := b.runtime
	if !r.prepareMu.TryLock() {
		return merr.WrapErrServiceNotReadyMsg("BM25 field preparation is in progress")
	}
	defer r.prepareMu.Unlock()
	r.mu.RLock()
	base, closed, floor := r.view, r.closed, r.minDataVersion
	r.mu.RUnlock()
	if closed {
		return context.Canceled
	}
	if b.resolved {
		return b.advance(ctx, qviews.FromProtoDataVersion(b.meta.GetVersion().GetDataVersion()))
	}
	if base.CaptureQueryView == nil {
		return merr.WrapErrServiceInternalMsg("BM25 preparation has no WAL snapshot provider")
	}
	target := proto.Clone(b.meta).(*viewpb.QueryViewMeta)
	if floor.GT(qviews.FromProtoDataVersion(target.GetVersion().GetDataVersion())) {
		target.Version.DataVersion = floor.IntoProto()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if base.RuntimeContext != nil {
		stop := context.AfterFunc(base.RuntimeContext, cancel)
		defer stop()
	}
	stage := &schemaRefresh{}
	defer func() { r.mu.Lock(); r.refresh = nil; r.cond.Broadcast(); r.mu.Unlock() }()
	stop := context.AfterFunc(ctx, func() { r.mu.Lock(); stage.err = ctx.Err(); r.cond.Broadcast(); r.mu.Unlock() })
	defer stop()
	var view walview.VChannelWALView
	if !base.CaptureQueryView(target, func(v walview.VChannelWALView) {
		view = v
		r.mu.Lock()
		stage.timeTick = v.BaseGrowingTimeTick
		stage.version = v.SegmentSnapshot.DataVersion
		r.refresh = stage
		r.mu.Unlock()
	}) {
		return merr.WrapErrServiceNotReadyMsg("BM25 schema snapshot is not ready")
	}
	if view.Schema.GetVersion() != b.meta.GetSchemaVersion() {
		return merr.WrapErrServiceNotReadyMsg("BM25 schema snapshot version mismatch")
	}
	view.ResourceEventBarrier = base.ResourceEventBarrier
	view.RuntimeContext = base.RuntimeContext
	if loadInfo != nil {
		info, err := loadInfo.QueryViewLoadInfo(ctx, view.CollectionID, b.meta.GetLoadInfoVersion())
		if err != nil {
			return err
		}
		view.PartitionIDs = append([]int64{}, info.PartitionIDs...)
		view.RequiredFields = qviews.ResolveRequiredFields(view.Schema, info.RequiredFields)
	}
	// Resource enumeration is scoped by DataView/partitions; DataCoord does not
	// require LoadInfoVersion. A shared field must not retain its first View's ID.
	view.LoadInfoVersion = 0
	if b.fields == nil {
		allowed := make(map[int64]bool)
		for _, field := range qviews.ResolveRequiredFields(view.Schema, view.RequiredFields) {
			allowed[field.GetFieldId()] = true
		}
		keys := make([]fieldKey, 0)
		for _, fn := range view.Schema.GetFunctions() {
			if fn.GetType() != schemapb.FunctionType_BM25 || len(fn.GetOutputFieldIds()) != 1 {
				continue
			}
			id := fn.GetOutputFieldIds()[0]
			if !allowed[id] {
				continue
			}
			signature, err := function.FunctionSignature(view.Schema, fn)
			if err != nil {
				return err
			}
			keys = append(keys, fieldKey{partitionScopeKey(view.PartitionIDs), id, signature})
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return context.Canceled
		}
		for _, key := range keys {
			field := r.fields[key]
			if field == nil {
				field = &fieldResource{key: key}
				r.fields[key] = field
			}
			field.refs++
			b.fields = append(b.fields, field)
		}
		r.mu.Unlock()
	}
	next := make(map[*fieldResource]*oracleRuntime)
	published := false
	defer func() {
		if !published {
			for _, oracle := range next {
				oracle.Close()
			}
		}
	}()
	for _, field := range b.fields {
		if field.oracle != nil {
			continue
		}
		provider, err := r.resolveProvider(ctx)
		if err != nil {
			return err
		}
		fieldView := view
		fieldView.RequiredFields = []*viewpb.LoadFieldConfig{{FieldId: field.key.fieldID}}
		oracle, err := provider.buildOracle(ctx, fieldView, paramtable.Get().QueryViewCfg.IDFLazyLoadSealedStats.GetAsBool())
		if err != nil {
			return err
		}
		next[field] = oracle
	}
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
			for field, oracle := range next {
				field.oracle = oracle
				field.coverage = schemaRefresh{timeTick: stage.timeTick, version: stage.version}
			}
			r.refresh = nil
			published = true
			r.mu.Unlock()
			break
		}
		r.mu.Unlock()
		for _, oracle := range next {
			for _, event := range batch {
				oracle.ApplyLiveEvent(ctx, event)
			}
		}
	}
	// Shared fields may still be receiving events already covered by the new
	// field's snapshot. Drain that prefix before publishing this binding as ready.
	if base.ResourceEventBarrier != nil {
		if err := base.ResourceEventBarrier(ctx); err != nil {
			return err
		}
	}
	if err := b.advance(ctx, stage.version); err != nil {
		return err
	}
	b.resolved = true
	return nil
}

func (b *viewBinding) advance(ctx context.Context, target qviews.DataVersion) error {
	for _, field := range b.fields {
		field.oracle.mu.RLock()
		version := field.oracle.currentVersion
		field.oracle.mu.RUnlock()
		if version.GT(target) {
			target = version
		}
	}
	for _, field := range b.fields {
		if err := field.oracle.PrepareDataVersion(ctx, target); err != nil {
			return err
		}
	}
	return nil
}

func (b *viewBinding) Release() {
	r := b.runtime
	// TODO: Make resource release retryable so waiting for another View's
	// preparation does not occupy a shared scheduler worker. A failed TryLock
	// must retain this binding until the scheduler retries its release.
	r.prepareMu.Lock()
	defer r.prepareMu.Unlock()
	r.mu.Lock()
	var retired []*oracleRuntime
	for _, field := range b.fields {
		field.refs--
		if field.refs == 0 {
			delete(r.fields, field.key)
			if field.oracle != nil {
				retired = append(retired, field.oracle)
			}
		}
	}
	r.mu.Unlock()
	for _, oracle := range retired {
		oracle.Close()
	}
}

func (b *viewBinding) BuildIDFBatch(ctx context.Context, requests []queryresource.IDFRequest) ([]queryresource.IDFResult, error) {
	// Shared fields may have advanced for a newer View. Bring the other fields
	// forward before reading a hybrid batch; never regress a shared aggregate.
	var baseline qviews.DataVersion
	aligned := true
	for i, field := range b.fields {
		field.oracle.mu.RLock()
		version := field.oracle.currentVersion
		field.oracle.mu.RUnlock()
		if i == 0 {
			baseline = version
		} else if !version.EQ(baseline) {
			aligned = false
		}
	}
	if !aligned {
		if err := b.advance(ctx, qviews.DataVersion{}); err != nil {
			if errors.Is(err, nodescheduler.ErrDelay) {
				return nil, merr.WrapErrServiceNotReadyMsg("BM25 fields are advancing to a common DataVersion")
			}
			return nil, err
		}
	}

	for _, field := range b.fields {
		if err := field.oracle.ensureMaterialized(ctx); err != nil {
			return nil, err
		}
	}
	r := b.runtime
	r.mu.RLock()
	defer r.mu.RUnlock() // one WAL event updates every shared field once
	for _, field := range b.fields {
		field.oracle.mu.RLock()
		defer field.oracle.mu.RUnlock()
	}
	var version qviews.DataVersion
	byID := make(map[int64]*oracleRuntime, len(b.fields))
	for i, field := range b.fields {
		oracle := field.oracle
		if oracle.closed {
			return nil, context.Canceled
		}
		if i == 0 {
			version = oracle.currentVersion
		} else if !version.EQ(oracle.currentVersion) {
			return nil, merr.WrapErrServiceNotReadyMsg("BM25 fields are advancing to a common DataVersion")
		}
		if oracle.currentStats == nil {
			return nil, merr.WrapErrServiceNotReadyMsg("BM25 field materialization changed")
		}
		byID[field.key.fieldID] = oracle
	}
	results := make([]queryresource.IDFResult, len(requests))
	for i, req := range requests {
		oracle := byID[req.FieldID]
		if oracle == nil {
			return nil, merr.WrapErrServiceInternalMsg("BM25 field %d is not bound to the QueryView", req.FieldID)
		}
		stats := oracle.currentStats[req.FieldID]
		results[i].Avgdl = stats.GetAvgdl()
		if results[i].Avgdl <= 0 {
			results[i].Avgdl = 1
		}
		for _, tf := range req.TFs.GetContents() {
			results[i].Vectors = append(results[i].Vectors, stats.BuildIDF(tf))
		}
	}
	return results, nil
}
