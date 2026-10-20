package qnview

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
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
	metadata    SegmentLoadInfoProvider
	buffer      TransformLogBuffer
	collections QueryViewCollectionRuntimeManager
	mu          sync.Mutex
	views       map[qviews.QueryViewKey]*viewResources
	resources   map[int64]*segmentResource
	closed      bool
	closeOnce   sync.Once
	// Tracks owners through actual cleanup, including views removed from the map.
	drain sync.WaitGroup
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
	cleanupDone      chan struct{} // dependencies and this View's segment operations have finished
}

type segmentBinding struct {
	view         *viewResources
	resource     *segmentResource
	partitionID  int64
	manifest     int64
	prepared     *preparedSegmentRead
	reported     bool
	retryAfter   time.Time
	retryBackoff *backoff.ExponentialBackOff
}

type preparedSegmentRead struct {
	schema   int32
	load     uint64
	manifest int64
	base     *segmentBase
	read     segments.ReadState
}

type segmentBase struct {
	ctx            context.Context
	cancel         context.CancelFunc
	anchor         *preparedSegmentRead
	operationRefs  int // Query handles and metrics pins that prevent base pruning.
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
	manager      *SegmentResourceManager
	id           int64
	collectionID int64
	ctx          context.Context
	cancel       context.CancelFunc
	bindings     []*segmentBinding
	snapshot     *SegmentLoadInfoSnapshot
	bases        []*segmentBase
	reads        []*preparedSegmentRead
	// scheduled covers only running or immediately queued work, never backoff.
	scheduled  bool
	retryTimer *time.Timer
	dirty      bool
	closing    bool
	done       chan struct{}
	cursor     int
	// Query handles, metrics sampling and catch-up pin this incarnation until
	// completion. Only bindings determine whether it can admit new consumers.
	operations sync.WaitGroup
}

func NewSegmentResourceManager(ctx context.Context, scheduler nodescheduler.Scheduler, loader PhysicalSegmentLoader, estimator SegmentResourceEstimator, metadata SegmentLoadInfoProvider, buffer TransformLogBuffer, collections QueryViewCollectionRuntimeManager) *SegmentResourceManager {
	return &SegmentResourceManager{ctx: ctx, scheduler: scheduler, loader: loader, estimator: estimator, metadata: metadata, buffer: buffer, collections: collections, views: make(map[qviews.QueryViewKey]*viewResources), resources: make(map[int64]*segmentResource)}
}

func (m *SegmentResourceManager) Acquire(req AcquireSegments) {
	req = cloneAcquireSegments(req)
	ctx, cancel := context.WithCancel(m.ctx)
	v := &viewResources{req: req, ctx: ctx, cancel: cancel, dependenciesDone: make(chan struct{}), cleanupDone: make(chan struct{}), bindings: make(map[int64]*segmentBinding)}
	m.mu.Lock()
	if m.closed || m.views[req.Key] != nil {
		closed := m.closed
		m.mu.Unlock()
		cancel()
		if closed && req.OnUnrecoverable != nil {
			go req.OnUnrecoverable()
		}
		return
	}
	// Bind only registers in-memory range ownership; it never performs I/O.
	// Admission and ownership accounting must precede Close's drain.
	var bindErr error
	v.buffer, bindErr = m.buffer.Bind(qviews.NewQueryViewAtQueryNode(req.Meta, req.View).(*qviews.QueryViewAtQueryNode))
	m.drain.Add(1)
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
	m.drain.Add(1)
	m.resources[id] = r
	return r
}

func (m *SegmentResourceManager) prepareView(v *viewResources) {
	defer close(v.dependenciesDone)
	view := qviews.NewQueryViewAtQueryNode(v.req.Meta, v.req.View).(*qviews.QueryViewAtQueryNode)
	retryBackoff := newResourceRetryBackoff(100 * time.Millisecond)
	for v.ctx.Err() == nil {
		collection, retryable, err := m.collections.Acquire(v.ctx, view)
		if err != nil {
			if !retryable {
				m.failView(v, err)
				return
			}
			if !waitResourceRetry(v.ctx, nextResourceRetryDelay(retryBackoff)) {
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
			if !waitResourceRetry(v.ctx, nextResourceRetryDelay(retryBackoff)) {
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

func waitResourceRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
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
		if r.closing || len(r.bindings) == 0 {
			r.closing = true
			r.cancel()
			go r.close()
			return nil
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
		if b != nil && (r.snapshot == nil || snapshotManifestVersion(*r.snapshot) < b.manifest) {
			m.mu.Unlock()
			ctx, cancel := mergeTaskContext(schedulerCtx, r.ctx)
			snapshot, err := m.metadata.GetQueryViewSegmentLoadInfo(ctx, r.collectionID, r.id)
			cancel()
			if err == nil {
				err = r.validateSnapshot(snapshot)
			}
			m.mu.Lock()
			if r.closing {
				continue
			}
			if err == nil {
				snapshot.LoadInfo = proto.Clone(snapshot.LoadInfo).(*querypb.SegmentLoadInfo)
				r.snapshot = &snapshot
				if snapshotManifestVersion(snapshot) < b.manifest {
					err = merr.WrapErrServiceNotReadyMsg("segment %d manifest is behind required version %d", r.id, b.manifest)
				}
			}
			if err != nil {
				b.backoffLocked()
				m.mu.Unlock()
				if merr.GetErrorType(err) == merr.InputError || errors.Is(err, merr.ErrSegmentNotFound) || errors.Is(err, merr.ErrServiceInternal) || errors.Is(err, merr.ErrDataIntegrity) {
					m.failView(b.view, err)
				}
				mlog.RatedWarn(r.ctx, rate.Limit(0.2), "query view segment metadata acquisition failed", mlog.FieldSegmentID(r.id), mlog.Err(err))
				m.mu.Lock()
			}
			// Re-evaluate all bindings, including ones released or added during Get.
			continue
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
			b.backoffLocked()
		} else {
			b.resetRetryLocked()
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
		if b.prepared == nil {
			for _, read := range r.reads {
				if readMatchesBinding(read, b) && read.base.caughtUp && read.base.catchupErr == nil {
					b.prepared = read
					b.resetRetryLocked()
					break
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
	for n := 0; n < len(r.bindings); n++ {
		i := (r.cursor + n) % len(r.bindings)
		b := r.bindings[i]
		if b.view.collection == nil || b.view.buffer == nil || b.view.failed || b.prepared != nil {
			continue
		}
		waiting := false
		for _, read := range r.reads {
			if readMatchesBinding(read, b) {
				waiting = true
				if read.base.catchupErr != nil {
					go m.failView(b.view, read.base.catchupErr)
				}
			}
		}
		if waiting {
			continue
		}
		if time.Now().Before(b.retryAfter) {
			if retryAt.IsZero() || b.retryAfter.Before(retryAt) {
				retryAt = b.retryAfter
			}
			continue
		}
		// External manifests may change the row domain. Never replace a required
		// historical external base with the current one or retry that mismatch forever.
		if r.snapshot != nil && b.manifest != 0 && typeutil.IsExternalCollection(b.view.collection.Schema()) && snapshotManifestVersion(*r.snapshot) > b.manifest {
			go m.failView(b.view, merr.WrapErrServiceNotReadyMsg("required external segment %d manifest %d is unavailable in current metadata", r.id, b.manifest))
			continue
		}
		r.cursor = (i + 1) % len(r.bindings)
		return b, retryAt
	}
	return nil, retryAt
}

func readMatchesBinding(read *preparedSegmentRead, b *segmentBinding) bool {
	return read.schema == b.view.req.Meta.GetSchemaVersion() && read.load == b.view.req.Meta.GetLoadInfoVersion() &&
		read.manifest >= b.manifest && (!typeutil.IsExternalCollection(b.view.collection.Schema()) || b.manifest == 0 || read.manifest == b.manifest)
}

func (r *segmentResource) validateSnapshot(snapshot SegmentLoadInfoSnapshot) error {
	info := snapshot.LoadInfo
	if info == nil || snapshot.CollectionID != r.collectionID || snapshot.SegmentID != r.id || info.GetCollectionID() != r.collectionID || info.GetSegmentID() != r.id {
		return merr.WrapErrServiceInternalMsg("segment %d received mismatched loading metadata", r.id)
	}
	_, version, err := packed.UnmarshalManifestPath(info.GetManifestPath())
	if err != nil || version <= 0 || info.GetStorageVersion() != storage.StorageV3 {
		return merr.WrapErrServiceInternalMsg("segment %d requires a committed StorageV3 manifest", r.id)
	}
	if info.GetTransformStartAfterTimetick() == 0 {
		return merr.WrapErrServiceNotReadyMsg("segment %d has no certified transform replay cursor", r.id)
	}
	return nil
}

func (r *segmentResource) prepare(ctx context.Context, b *segmentBinding, collection CollectionRuntime, snapshot SegmentLoadInfoSnapshot) (*preparedSegmentRead, error) {
	m := r.manager
	info := proto.Clone(snapshot.LoadInfo).(*querypb.SegmentLoadInfo)
	selected := make(map[int64]struct{}, len(collection.IndexInfos()))
	for _, index := range collection.IndexInfos() {
		selected[index.GetIndexID()] = struct{}{}
	}
	info.IndexInfos = slices.DeleteFunc(info.IndexInfos, func(index *querypb.FieldIndexInfo) bool {
		_, ok := selected[index.GetIndexID()]
		return !ok
	})
	snapshot.LoadInfo = info
	snapshot.IndexInfos = collection.IndexInfos()
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
	return &preparedSegmentRead{schema: b.view.req.Meta.GetSchemaVersion(), load: b.view.req.Meta.GetLoadInfoVersion(), manifest: snapshotManifestVersion(snapshot), base: base, read: read}, nil
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
		reg.OnFailure(func(err error) {
			m.mu.Lock()
			if base.ctx.Err() == nil {
				base.catchupErr = err
				r.wakeLocked()
			}
			m.mu.Unlock()
		})
	}
}

func (r *segmentResource) close() {
	m := r.manager
	defer m.drain.Done()
	// Closing fences query/metrics admission under mu. Reconcile has finished
	// synchronous preparation, including admission of any catch-up goroutine,
	// before starting close, so no operation can be added after Wait begins.
	r.operations.Wait()
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

// closeIfUnusedLocked fences an incarnation when its final View binding leaves.
// Existing operations delay destruction, not Closing or its completion promise.
// The caller holds manager.mu; closing itself runs asynchronously.
func (r *segmentResource) closeIfUnusedLocked() bool {
	if r.closing {
		return true
	}
	if len(r.bindings) != 0 {
		return false
	}
	r.wakeLocked()
	r.closing = true
	r.stopRetryLocked()
	r.cancel()
	return true
}

func (m *SegmentResourceManager) Release(req ReleaseSegments) {
	m.mu.Lock()
	v := m.views[req.Key]
	delete(m.views, req.Key)
	var resources []*segmentResource
	if v != nil {
		m.drain.Add(1) // Join the release callback as well as the resource owners.
		v.released = true
		v.cancel()
		for _, b := range v.bindings {
			r := b.resource
			resources = append(resources, r)
			for i, candidate := range r.bindings {
				if candidate == b {
					r.bindings = slices.Delete(r.bindings, i, i+1)
					break
				}
			}
			if !r.closeIfUnusedLocked() {
				r.wakeLocked()
			}
		}
	}
	m.mu.Unlock()
	go func() {
		if v != nil {
			defer m.drain.Done()
			<-v.dependenciesDone
			m.cleanupView(v)
			// Dropped includes queries and preparation that borrowed this View.
			// Their final operation release completes cleanup without blocking a worker.
			<-v.cleanupDone
			m.mu.Lock()
			var closed []<-chan struct{}
			for _, r := range resources {
				// Other View bindings may still own the segment. Do not wait for
				// those bindings or a later takeover. An already-closing
				// incarnation must finish even if successor bindings now wait on it.
				if r.closeIfUnusedLocked() {
					closed = append(closed, r.done)
				}
			}
			m.mu.Unlock()
			for _, done := range closed {
				<-done
			}
		}
		if req.OnDropped != nil {
			req.OnDropped()
		}
	}()
}

// Close stops admission and joins physical release, preparation, and existing
// query handles. The shared node scheduler remains alive throughout draining.
func (m *SegmentResourceManager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		keys := make([]qviews.QueryViewKey, 0, len(m.views))
		for key := range m.views {
			keys = append(keys, key)
		}
		m.mu.Unlock()
		for _, key := range keys {
			m.Release(ReleaseSegments{Key: key})
		}
		m.drain.Wait()
		m.buffer.Close()
	})
}

func (m *SegmentResourceManager) cleanupView(v *viewResources) {
	// Preparation may still be installing or releasing dependency guards.
	// Release's waiter retries cleanup once this phase has finished.
	select {
	case <-v.dependenciesDone:
	default:
		return
	}
	m.mu.Lock()
	cleanup := v.released && v.operationRefs == 0
	m.mu.Unlock()
	if cleanup {
		v.cleanupOnce.Do(func() {
			defer m.drain.Done()
			defer close(v.cleanupDone)
			if v.buffer != nil {
				v.buffer.Release()
			}
			if v.collection != nil {
				v.collection.Release()
			}
		})
	}
}

// WaitTransformVisible waits for the View buffer frontier.
// AcquireSealedSegmentHandles additionally waits for each acquired physical base.
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
		if used[base] || base.operationRefs > 0 || base.catchupRunning || pending {
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
