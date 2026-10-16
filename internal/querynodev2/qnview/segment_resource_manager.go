package qnview

import (
	"context"
	"slices"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SegmentResourceManager owns view bindings and every physical segment they use.
// All decisions are serialized by mu; I/O and callbacks never run under it.
type SegmentResourceManager struct {
	ctx         context.Context
	scheduler   nodescheduler.Scheduler
	loader      PhysicalSegmentLoader
	estimator   SegmentResourceEstimator
	stream      SegmentLoadInfoStream
	buffer      TransformLogBuffer
	collections QueryViewCollectionRuntimeManager
	mu          sync.Mutex
	views       map[qviews.QueryViewKey]*viewResources
	resources   map[int64]*segmentResource
}

type viewResources struct {
	req              AcquireSegments
	ctx              context.Context
	cancel           context.CancelFunc
	dependenciesDone chan struct{}
	collection       CollectionRuntimeGuard
	buffer           TransformLogGuard
	bindings         map[int64]*segmentBinding
	failed           bool
	released         bool
	operationRefs    int // queries and synchronous resource preparation borrowing the View collection
	cleanupOnce      sync.Once
}

type segmentBinding struct {
	view        *viewResources
	resource    *segmentResource
	partitionID int64
	manifest    int64
	prepared    *preparedSegmentRead
	reported    bool
	retryAfter  time.Time
}

type preparedSegmentRead struct {
	schema   int32
	load     uint64
	manifest int64
	revision SegmentLoadInfoRevision
	base     *segmentBase
	read     segments.ReadState
}

type segmentBase struct {
	ctx            context.Context
	cancel         context.CancelFunc
	anchor         *preparedSegmentRead
	queryRefs      int
	segment        TransformSegment
	snapshot       SegmentLoadInfoSnapshot
	registration   TransformRegistration
	caughtUp       bool
	catchupRunning bool
	catchupErr     error
}

// A resource remains in the registry through Closing. Bindings arriving during
// closure wait for its done channel before installing a replacement owner.
type segmentResource struct {
	manager        *SegmentResourceManager
	id             int64
	collectionID   int64
	ctx            context.Context
	cancel         context.CancelFunc
	bindings       []*segmentBinding
	snapshot       *SegmentLoadInfoSnapshot
	subscription   SegmentLoadInfoSubscription
	metadataClosed bool
	bases          []*segmentBase
	reads          []*preparedSegmentRead
	queryRefs      int
	// scheduled covers only running or immediately queued work, never backoff.
	scheduled  bool
	retryTimer *time.Timer
	dirty      bool
	closing    bool
	done       chan struct{}
	cursor     int
	operations sync.WaitGroup
}

func NewSegmentResourceManager(ctx context.Context, scheduler nodescheduler.Scheduler, loader PhysicalSegmentLoader, estimator SegmentResourceEstimator, stream SegmentLoadInfoStream, buffer TransformLogBuffer, collections QueryViewCollectionRuntimeManager) *SegmentResourceManager {
	return &SegmentResourceManager{ctx: ctx, scheduler: scheduler, loader: loader, estimator: estimator, stream: stream, buffer: buffer, collections: collections, views: make(map[qviews.QueryViewKey]*viewResources), resources: make(map[int64]*segmentResource)}
}

func (m *SegmentResourceManager) Acquire(req AcquireSegments) {
	req = cloneAcquireSegments(req)
	ctx, cancel := context.WithCancel(m.ctx)
	v := &viewResources{req: req, ctx: ctx, cancel: cancel, dependenciesDone: make(chan struct{}), bindings: make(map[int64]*segmentBinding)}
	// Buffer range ownership is registered synchronously with the View.
	var bindErr error
	v.buffer, bindErr = m.buffer.Bind(qviews.NewQueryViewAtQueryNode(req.Meta, req.View).(*qviews.QueryViewAtQueryNode))
	m.mu.Lock()
	if m.views[req.Key] != nil {
		m.mu.Unlock()
		cancel()
		if v.buffer != nil {
			go v.buffer.Release()
		}
		return
	}
	m.views[req.Key] = v
	for _, partition := range req.View.GetPartitions() {
		for i, id := range partition.GetSegmentIds() {
			r := m.resources[id]
			if r == nil {
				r = m.newResourceLocked(id, req.Meta.GetCollectionId())
			}
			b := &segmentBinding{view: v, resource: r, partitionID: partition.GetPartitionId()}
			if i < len(partition.GetSegmentManifestVersions()) {
				b.manifest = partition.GetSegmentManifestVersions()[i]
			}
			v.bindings[id] = b
			r.bindings = append(r.bindings, b)
			r.wakeLocked()
		}
	}
	m.mu.Unlock()
	// Dependency work never occupies the handler's shard lock. The complete
	// bindings above already protect shared resources while dependencies resolve.
	go func() {
		if bindErr != nil {
			m.failView(v, bindErr)
			close(v.dependenciesDone)
			return
		}
		m.prepareView(v)
	}()
}

func (m *SegmentResourceManager) newResourceLocked(id, collection int64) *segmentResource {
	ctx, cancel := context.WithCancel(m.ctx) // #nosec G118 -- Ownership transfers to the resource; Release or reconciliation cancels it.
	r := &segmentResource{manager: m, id: id, collectionID: collection, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	m.resources[id] = r
	return r
}

func (m *SegmentResourceManager) prepareView(v *viewResources) {
	defer close(v.dependenciesDone)
	view := qviews.NewQueryViewAtQueryNode(v.req.Meta, v.req.View).(*qviews.QueryViewAtQueryNode)
	for v.ctx.Err() == nil {
		collection, retryable, err := m.collections.Acquire(v.ctx, view)
		if err != nil {
			if !retryable {
				m.failView(v, err)
				return
			}
			if !waitResourceRetry(v.ctx) {
				return
			}
			continue
		}
		retryable, err = v.buffer.Prepare(v.ctx)
		if err != nil {
			collection.Release()
			if v.ctx.Err() != nil {
				return
			}
			if !retryable {
				m.failView(v, err)
				return
			}
			if !waitResourceRetry(v.ctx) {
				return
			}
			continue
		}
		m.mu.Lock()
		if v.released {
			m.mu.Unlock()
			collection.Release()
			return
		}
		v.collection = collection
		for _, b := range v.bindings {
			b.resource.wakeLocked()
		}
		empty := len(v.bindings) == 0
		m.mu.Unlock()
		if empty && v.req.OnReady != nil {
			v.req.OnReady(map[int64][]int64{})
		}
		return
	}
}

func waitResourceRetry(ctx context.Context) bool {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (m *SegmentResourceManager) failView(v *viewResources, err error) {
	m.mu.Lock()
	notify := !v.failed && !v.released
	v.failed = true
	m.mu.Unlock()
	if notify {
		mlog.Warn(v.ctx, "query view resource preparation failed", mlog.FieldCollectionID(v.req.Meta.GetCollectionId()), mlog.Err(err))
		if v.req.OnUnrecoverable != nil {
			v.req.OnUnrecoverable()
		}
	}
}

func (r *segmentResource) wakeLocked() {
	r.dirty = true
	if r.scheduled || r.closing {
		return
	}
	r.submitLocked()
}

func (r *segmentResource) submitLocked() {
	r.scheduled = true
	r.manager.scheduler.Submit(schedulerTaskFunc(func(ctx context.Context) error { return r.reconcile(ctx) }))
}

func (r *segmentResource) stopRetryLocked() {
	if r.retryTimer != nil {
		r.retryTimer.Stop()
		r.retryTimer = nil
	}
}

func (r *segmentResource) retryAtLocked(at time.Time) {
	if at.IsZero() || r.closing {
		return
	}
	var timer *time.Timer
	timer = time.AfterFunc(time.Until(at), func() {
		r.manager.mu.Lock()
		defer r.manager.mu.Unlock()
		// Stop may race with an already fired callback. Only the current timer
		// may wake this concrete owner, never its replacement with the same ID.
		if r.retryTimer != timer || r.closing {
			return
		}
		r.retryTimer = nil
		r.wakeLocked()
	})
	r.retryTimer = timer
}

func (r *segmentResource) reconcile(schedulerCtx context.Context) error {
	m := r.manager
	m.mu.Lock()
	// Every return holds mu. Events arriving during I/O either get consumed
	// by the next iteration or transfer to a successor under this same lock.
	defer func() {
		r.scheduled = false
		if r.dirty {
			// Release can set closing during I/O. Its successor must still run
			// to close the resource after the physical operation finishes.
			r.submitLocked()
		}
		m.mu.Unlock()
	}()
	r.stopRetryLocked()
	prepared := false
	for {
		r.dirty = false
		if r.closing || (len(r.bindings) == 0 && r.queryRefs == 0) {
			r.closing = true
			r.cancel()
			go r.close()
			return nil
		}
		if r.metadataClosed {
			subscription := r.subscription
			affected := append([]*segmentBinding(nil), r.bindings...)
			m.mu.Unlock()
			// Error uses the transport's callback mutex: never read it under mu.
			err := subscription.Error()
			if err == nil {
				err = merr.WrapErrServiceNotReadyMsg("segment %d metadata subscription closed", r.id)
			}
			for _, binding := range affected {
				m.failView(binding.view, err)
			}
			m.mu.Lock()
			return nil
		}
		if r.subscription == nil {
			m.mu.Unlock()
			sub := m.stream.Subscribe(SegmentLoadInfoSubscriptionOption{CollectionID: r.collectionID, SegmentID: r.id, Handler: resourceMetadataHandler{r}})
			m.mu.Lock()
			r.subscription = sub
			// Subscribe may deliver its first snapshot synchronously.
			continue
		}
		cursor := r.cursor
		b, retryAt := r.nextBindingLocked()
		if b != nil && prepared {
			// Publish ready reads immediately, but yield further physical work
			// to the queue tail so one resource cannot monopolize a worker.
			r.cursor = cursor
			r.dirty = true
			b = nil
		}
		obsolete := r.pruneReadsLocked()
		anchors := append([]*preparedSegmentRead(nil), r.reads...)
		retired := r.pruneBasesLocked()
		if b == nil {
			r.retryAtLocked(retryAt)
			m.mu.Unlock()
			r.retainBaseReads(anchors)
			for _, read := range obsolete {
				read.ReleaseReadState()
			}
			r.releaseBases(retired)
			m.mu.Lock()
			return nil
		}
		snapshot := *r.snapshot
		collection := b.view.collection
		b.view.operationRefs++
		m.mu.Unlock()
		r.retainBaseReads(anchors)
		for _, read := range obsolete {
			read.ReleaseReadState()
		}
		r.releaseBases(retired)
		ctx, cancel := mergeTaskContext(schedulerCtx, r.ctx)
		result, err := r.prepare(ctx, b, collection, snapshot)
		cancel()
		if err != nil {
			mlog.RatedWarn(r.ctx, rate.Limit(0.2), "segment resource preparation will retry", mlog.FieldSegmentID(r.id), mlog.Int64("schemaVersion", collection.SchemaVersion()), mlog.Err(err))
			if merr.GetErrorType(err) == merr.InputError {
				m.failView(b.view, err)
			}
		}
		m.mu.Lock()
		b.view.operationRefs--
		if err != nil {
			b.retryAfter = time.Now().Add(200 * time.Millisecond)
		} else {
			r.reads = append(r.reads, result)
		}
		prepared = true
		m.mu.Unlock()
		m.cleanupView(b.view)
		m.mu.Lock()
		// Recheck readiness now; unfinished catch-up has its own wakeup.
	}
}

// Complete compatible reads are published before selecting another operation.
// The rotating cursor gives every executable requirement an opportunity even
// while another schema is waiting for materialization.
func (r *segmentResource) nextBindingLocked() (*segmentBinding, time.Time) {
	m := r.manager
	var retryAt time.Time
	for _, b := range r.bindings {
		if b.view.collection == nil || b.view.buffer == nil || b.view.failed {
			continue
		}
		if b.prepared != nil && b.prepared.base.catchupErr != nil {
			go m.failView(b.view, b.prepared.base.catchupErr)
			continue
		}
		for _, read := range r.reads {
			if read.schema == b.view.req.Meta.GetSchemaVersion() && read.load == b.view.req.Meta.GetLoadInfoVersion() && read.manifest >= b.manifest && (!typeutil.IsExternalCollection(b.view.collection.Schema()) || b.manifest == 0 || read.manifest == b.manifest) && read.base.caughtUp {
				if b.prepared == nil || read.revision == r.snapshot.Revision {
					b.prepared = read
				}
			}
		}
		if b.prepared != nil && !b.reported {
			b.reported = true
			cb, partition, id := b.view.req.OnReady, b.partitionID, r.id
			if cb != nil {
				go cb(map[int64][]int64{partition: {id}})
			}
		}
	}
	if r.snapshot == nil {
		return nil, retryAt
	}
	for n := 0; n < len(r.bindings); n++ {
		i := (r.cursor + n) % len(r.bindings)
		b := r.bindings[i]
		if b.view.collection == nil || b.view.buffer == nil || b.view.failed {
			continue
		}
		// External bases can change their row domain under the same ID. An
		// already bound historical read remains valid but must not adopt a new
		// external snapshot merely because its manifest number is greater.
		if typeutil.IsExternalCollection(b.view.collection.Schema()) && b.manifest != 0 && snapshotManifestVersion(*r.snapshot) != b.manifest {
			if b.prepared == nil {
				go m.failView(b.view, merr.WrapErrServiceNotReadyMsg("required external segment %d manifest %d is unavailable in current metadata", r.id, b.manifest))
			}
			continue
		}
		if b.prepared != nil && b.prepared.revision == r.snapshot.Revision {
			continue
		}
		// Do not build a duplicate while its transform attachment is catching up.
		waiting := false
		for _, read := range r.reads {
			if read.schema == b.view.req.Meta.GetSchemaVersion() && read.load == b.view.req.Meta.GetLoadInfoVersion() && read.revision == r.snapshot.Revision && read.manifest >= b.manifest {
				waiting = true
				if read.base.catchupErr != nil {
					go m.failView(b.view, read.base.catchupErr)
				}
			}
		}
		if waiting {
			continue
		}
		if snapshotManifestVersion(*r.snapshot) < b.manifest {
			continue
		}
		if time.Now().Before(b.retryAfter) {
			if retryAt.IsZero() || b.retryAfter.Before(retryAt) {
				retryAt = b.retryAfter
			}
			continue
		}
		r.cursor = (i + 1) % len(r.bindings)
		return b, retryAt
	}
	return nil, retryAt
}

func (r *segmentResource) prepare(ctx context.Context, b *segmentBinding, collection CollectionRuntime, snapshot SegmentLoadInfoSnapshot) (*preparedSegmentRead, error) {
	m := r.manager
	info := snapshot.LoadInfo
	if err := updateCollectionIndexMeta(ctx, collection, snapshot.IndexInfos); err != nil {
		return nil, err
	}
	if m.estimator != nil {
		reservation, err := m.estimator.Reserve(ctx, info, collection)
		if err != nil {
			return nil, err
		}
		if reservation != nil {
			defer reservation.Release()
		}
	}
	var base *segmentBase
	var reuse segments.ReadState
	// Normal sealed same-ID additive manifests preserve the row domain. External
	// snapshots may replace it, so they require an identical manifest to share.
	m.mu.Lock()
	for _, candidate := range r.bases {
		sameManifest := candidate.snapshot.LoadInfo.GetManifestPath() == info.GetManifestPath()
		if candidate.segment.TransformStartAfterTimeTick() <= info.GetTransformStartAfterTimetick() && (sameManifest || (!typeutil.IsExternalCollection(collection.Schema()) && candidate.snapshot.LoadInfo.GetNumOfRows() == info.GetNumOfRows())) {
			base = candidate
			break
		}
	}
	if base != nil {
		for _, read := range r.reads {
			if read.base == base {
				reuse = read.read
			}
		}
	}
	m.mu.Unlock()
	if base == nil {
		segment, err := m.loader.Load(ctx, info, collection)
		if err != nil {
			return nil, err
		}
		segment = &transformStartSegment{TransformSegment: segment, startAfter: info.GetTransformStartAfterTimetick()}
		baseCtx, cancel := context.WithCancel(r.ctx) //nolint:gosec // Ownership transfers to the base; resource closure or base retirement cancels it.
		base = &segmentBase{segment: segment, snapshot: snapshot, ctx: baseCtx, cancel: cancel}
		m.mu.Lock()
		r.bases = append(r.bases, base)
		r.operations.Add(1)
		base.catchupRunning = true
		m.mu.Unlock()
		go r.catchup(base)
	}
	read, err := m.loader.PrepareReadState(ctx, base.segment, reuse, collection, snapshot)
	if err != nil {
		return nil, err
	}
	return &preparedSegmentRead{schema: b.view.req.Meta.GetSchemaVersion(), load: b.view.req.Meta.GetLoadInfoVersion(), manifest: snapshotManifestVersion(snapshot), revision: snapshot.Revision, base: base, read: read}, nil
}

func (r *segmentResource) catchup(base *segmentBase) {
	defer r.operations.Done()
	reg, err := r.manager.buffer.RegisterSegment(base.ctx, base.segment)
	if err == nil {
		err = reg.WaitCatchup(base.ctx)
	}
	m := r.manager
	m.mu.Lock()
	base.registration, base.catchupErr = reg, err
	base.caughtUp, base.catchupRunning = err == nil, false
	r.wakeLocked()
	m.mu.Unlock()
	if err == nil {
		err = reg.WaitFailure(base.ctx)
		m.mu.Lock()
		if base.ctx.Err() == nil {
			base.catchupErr = err
			r.wakeLocked()
		}
		m.mu.Unlock()
	}
}

func (r *segmentResource) close() {
	m := r.manager
	r.operations.Wait()
	if r.subscription != nil {
		r.subscription.Close()
	}
	for _, base := range r.bases {
		if base.registration != nil {
			base.registration.Unregister()
		}
	}
	for _, read := range r.reads {
		read.read.ReleaseReadState()
	}
	for _, base := range r.bases {
		_ = base.segment.Release(context.WithoutCancel(r.ctx))
	}
	m.mu.Lock()
	delete(m.resources, r.id)
	if len(r.bindings) > 0 {
		next := m.newResourceLocked(r.id, r.collectionID)
		next.bindings = r.bindings
		for _, b := range next.bindings {
			b.resource = next
			b.prepared = nil
		}
		next.wakeLocked()
	}
	close(r.done)
	m.mu.Unlock()
}

func (m *SegmentResourceManager) Release(req ReleaseSegments) {
	m.mu.Lock()
	v := m.views[req.Key]
	delete(m.views, req.Key)
	var closed []<-chan struct{}
	if v != nil {
		v.released = true
		v.cancel()
		for _, b := range v.bindings {
			r := b.resource
			for i, candidate := range r.bindings {
				if candidate == b {
					r.bindings = slices.Delete(r.bindings, i, i+1)
					break
				}
			}
			if len(r.bindings) == 0 && r.queryRefs == 0 && !r.closing {
				// Establish closure before another Bind can reuse this owner.
				r.wakeLocked()
				r.closing = true
				r.stopRetryLocked()
				r.cancel()
				closed = append(closed, r.done)
			} else {
				r.wakeLocked()
			}
		}
	}
	m.mu.Unlock()
	go func() {
		for _, done := range closed {
			<-done
		}
		if v != nil {
			<-v.dependenciesDone
			m.cleanupView(v)
		}
		if req.OnDropped != nil {
			req.OnDropped()
		}
	}()
}

func (m *SegmentResourceManager) cleanupView(v *viewResources) {
	m.mu.Lock()
	cleanup := v.released && v.operationRefs == 0
	m.mu.Unlock()
	if cleanup {
		v.cleanupOnce.Do(func() {
			if v.buffer != nil {
				v.buffer.Release()
			}
			if v.collection != nil {
				v.collection.Release()
			}
		})
	}
}

type resourceMetadataHandler struct{ resource *segmentResource }

func (h resourceMetadataHandler) Handle(snapshot SegmentLoadInfoSnapshot) error {
	r := h.resource
	r.manager.mu.Lock()
	if !r.closing {
		owned := snapshot
		owned.LoadInfo = proto.Clone(snapshot.LoadInfo).(*querypb.SegmentLoadInfo)
		r.snapshot = &owned
		r.wakeLocked()
	}
	r.manager.mu.Unlock()
	return nil
}

func (h resourceMetadataHandler) Close() {
	r := h.resource
	r.manager.mu.Lock()
	if !r.closing {
		r.metadataClosed = true
		r.wakeLocked()
	}
	r.manager.mu.Unlock()
}

// WaitTransformVisible covers delivery and application to all selected bases.
func (m *SegmentResourceManager) WaitTransformVisible(ctx context.Context, key qviews.QueryViewKey, timetick uint64) error {
	m.mu.Lock()
	v := m.views[key]
	if v == nil {
		m.mu.Unlock()
		return viewerror.NewViewNotFound("query view %s is not found", key.String())
	}
	guard := v.buffer
	m.mu.Unlock()
	if guard == nil {
		return viewerror.NewViewInvalidated("query view %s is not ready", key.String())
	}
	return guard.WaitTransformVisible(ctx, timetick)
}

// Retain bound results and work awaiting catch-up. Keep one reusable snapshot
// only for bases with no published binding while dependencies are pending.
// Queries independently pin their selected ReadState, so replacing a binding
// releases only the resource cache's reference.
func (r *segmentResource) pruneReadsLocked() []segments.ReadState {
	keep := make(map[*preparedSegmentRead]bool)
	for _, b := range r.bindings {
		if b.prepared != nil {
			keep[b.prepared] = true
		}
	}
	bound := make(map[*segmentBase]bool)
	for read := range keep {
		bound[read.base] = true
	}
	latest := make(map[*segmentBase]*preparedSegmentRead)
	for _, read := range r.reads {
		latest[read.base] = read
	}
	for base, read := range latest {
		if bound[base] {
			continue
		}
		for _, b := range r.bindings {
			if b.prepared == nil && !b.view.failed && (b.view.collection == nil || !typeutil.IsExternalCollection(b.view.collection.Schema()) || b.manifest == 0 || b.manifest == read.manifest) {
				keep[read] = true
				break
			}
		}
	}
	// A result awaiting catch-up is still the outcome of a live demand.
	for _, read := range r.reads {
		if read.base.caughtUp {
			continue
		}
		for _, b := range r.bindings {
			if read.schema == b.view.req.Meta.GetSchemaVersion() && read.load == b.view.req.Meta.GetLoadInfoVersion() {
				keep[read] = true
			}
		}
	}
	var obsolete []segments.ReadState
	retained := r.reads[:0]
	for _, read := range r.reads {
		if keep[read] {
			retained = append(retained, read)
		} else {
			obsolete = append(obsolete, read.read)
		}
	}
	clear(r.reads[len(retained):])
	r.reads = retained
	return obsolete
}

// The transform base shares one live read's columns. It must not retain the
// initial load scope after its last View exits. Only Reconcile changes anchors.
func (r *segmentResource) retainBaseReads(reads []*preparedSegmentRead) {
	anchors := make(map[*segmentBase]*preparedSegmentRead)
	for _, read := range reads {
		anchors[read.base] = read
	}
	for base, read := range anchors {
		if base.anchor == read {
			continue
		}
		if err := r.manager.loader.AdoptReadState(base.segment, read.read); err != nil {
			mlog.Warn(r.ctx, "failed to adopt live segment read state", mlog.FieldSegmentID(r.id), mlog.Err(err))
			continue
		}
		base.anchor = read
	}
}

func cloneAcquireSegments(req AcquireSegments) AcquireSegments {
	out := req
	if req.Meta != nil {
		out.Meta = proto.Clone(req.Meta).(*viewpb.QueryViewMeta)
	}
	if req.View != nil {
		out.View = proto.Clone(req.View).(*viewpb.QueryViewOfQueryNode)
	}
	return out
}

func snapshotManifestVersion(snapshot SegmentLoadInfoSnapshot) int64 {
	_, version, _ := packed.UnmarshalManifestPath(snapshot.LoadInfo.GetManifestPath())
	return version
}

// Only the reconciler retires physical bases, after their cached read references
// have been released. Queries hold a separate reference to the concrete base.
func (r *segmentResource) pruneBasesLocked() []*segmentBase {
	used := make(map[*segmentBase]bool)
	for _, read := range r.reads {
		used[read.base] = true
	}
	var retired []*segmentBase
	kept := r.bases[:0]
	for _, base := range r.bases {
		pending := false
		for _, b := range r.bindings {
			if b.prepared == nil && !b.view.failed {
				pending = true
				break
			}
		}
		if used[base] || base.queryRefs > 0 || base.catchupRunning || pending {
			kept = append(kept, base)
		} else {
			retired = append(retired, base)
		}
	}
	clear(r.bases[len(kept):])
	r.bases = kept
	return retired
}

func (r *segmentResource) releaseBases(bases []*segmentBase) {
	for _, base := range bases {
		base.cancel()
		if base.registration != nil {
			base.registration.Unregister()
		}
		_ = base.segment.Release(context.WithoutCancel(r.ctx))
	}
}
