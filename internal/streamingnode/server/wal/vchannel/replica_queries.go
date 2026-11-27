package vchannel

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/idf"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReplicaQueries owns live read resources for the lifetime of the physical
// replica. Only an actual View capture reads writer metadata and summary chunks.
type ReplicaQueries struct {
	writer   atomic.Bool
	ctx      context.Context
	cancel   context.CancelFunc
	channel  string
	buffer   *replica.Buffer
	dispatch *queryresource.Dispatcher
	mu       sync.Mutex
	// Cleanup only needs the resource manager. Its lookup must not take mu
	// while holding the recovery module lock used to serialize admission.
	active        *typeutil.ConcurrentMap[string, *queryChannel]
	writerContext context.Context
	recovery      *PChannelRecoveryManager // bound while this replica has writer recovery
	// events serializes snapshot installation, live delivery and readiness barriers.
	// Releasing a view must remain possible while delivery waits for queue space.
	events  sync.Mutex
	through uint64
	changed chan struct{}
}
type queryChannel struct {
	resources *queryresource.Manager
	// A non-nil request is waiting for an admissible DataVersion.
	refs map[qviews.QueryViewKey]*snview.AcquireResource
}

func NewReplicaQueries(ctx context.Context, channel string, b *replica.Buffer, through uint64) *ReplicaQueries {
	q := &ReplicaQueries{
		channel:  channel,
		buffer:   b,
		through:  through,
		dispatch: queryresource.NewConfiguredDispatcher(),
		active:   typeutil.NewConcurrentMap[string, *queryChannel](),
		changed:  make(chan struct{}),
	}
	q.ctx, q.cancel = context.WithCancel(ctx) //nolint:gosec // ReplicaQueries.Close cancels blocked observers before draining resources.
	return q
}

// RetryFailedPreparation reports whether local failures should be retried for
// the writer that supplied the current target.
func (q *ReplicaQueries) RetryFailedPreparation() bool { return !q.writer.Load() }

// BindWriter attaches the admission check before writer cleanup starts. A
// demotion or failed open detaches recovery without releasing any query views.
func (q *ReplicaQueries) BindWriter(m *PChannelRecoveryManager) func() {
	q.mu.Lock()
	defer q.mu.Unlock()
	ctx, cancel := context.WithCancel(q.ctx)
	q.writerContext = ctx
	q.recovery = m
	return func() {
		q.mu.Lock()
		cancel()
		q.recovery = nil
		q.mu.Unlock()
	}
}

func (q *ReplicaQueries) OldestDataVersion(vchannel string) (qviews.DataVersion, bool) {
	if ch, ok := q.active.Get(vchannel); ok {
		return ch.resources.OldestDataVersion()
	}
	return qviews.DataVersion{}, false
}

// SetWriter changes failure ownership without interrupting a running build.
func (q *ReplicaQueries) SetWriter(writer bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.writer.Store(writer)
	q.active.Range(func(_ string, ch *queryChannel) bool {
		q.retryAdmissionLocked(ch)
		return true
	})
}

func (q *ReplicaQueries) Acquire(req snview.AcquireResource) {
	q.mu.Lock()
	defer q.mu.Unlock()
	vc := req.Key.ShardID.VChannel
	ch, _ := q.active.Get(vc)
	if ch == nil {
		r := resource.Resource()
		cfg := queryresource.Config{
			RetryFailedPreparation: q.RetryFailedPreparation,
			CaptureView:            q.capture, Dispatcher: q.dispatch, ChunkManager: r.ChunkManager(),
			LoadInfoProvider: queryresource.NewFutureLoadInfoProvider(r.MixCoordClient()),
			Builders: []queryresource.QueryRuntimeModuleBuilder{
				queryresource.NewGrowingRuntimeModuleBuilder(nil),
				queryresource.NewReadSchemaModuleBuilder(),
				idf.NewFutureProvider(r.MixCoordClient(), idf.WithChunkManager(r.ChunkManager())),
			},
		}
		ch = &queryChannel{
			resources: queryresource.NewManager(cfg),
			refs:      make(map[qviews.QueryViewKey]*snview.AcquireResource),
		}
		q.active.Insert(vc, ch)
	}
	q.admitLocked(ch, &req)
}

func (q *ReplicaQueries) admitLocked(ch *queryChannel, req *snview.AcquireResource) {
	if !q.tryAcquireLocked(ch, req) {
		if !q.writer.Load() {
			ch.refs[req.Key] = req
			return
		}
		ch.resources.Reject(*req)
	}
	ch.refs[req.Key] = nil
}

// Both a new reference and a cleanup decision are made under module.mu. The
// writer checks the in-memory floor, including a pending catalog write.
func (q *ReplicaQueries) tryAcquireLocked(ch *queryChannel, req *snview.AcquireResource) bool {
	if q.recovery != nil {
		module := q.recovery.Module(req.Key.ShardID.VChannel)
		if module == nil {
			return false
		}
		module.mu.Lock()
		defer module.mu.Unlock()
		if !module.acceptsQueryVersionLocked(req.Key.QueryViewVersion.DataVersion) {
			return false
		}
	}
	return ch.resources.TryAcquireLocked(*req, nil)
}

func (q *ReplicaQueries) retryAdmissionLocked(ch *queryChannel) {
	var pending []*snview.AcquireResource
	for _, req := range ch.refs {
		if req != nil {
			pending = append(pending, req)
		}
	}
	// A fresh runtime must retain the oldest requested base before a newer
	// admission advances its watermark.
	sort.Slice(pending, func(i, j int) bool {
		return pending[j].Key.QueryViewVersion.GT(pending[i].Key.QueryViewVersion)
	})
	for _, req := range pending {
		q.admitLocked(ch, req)
	}
}

func (q *ReplicaQueries) Release(req snview.ReleaseResource) {
	q.mu.Lock()
	ch, _ := q.active.Get(req.Key.ShardID.VChannel)
	if ch == nil {
		q.mu.Unlock()
		if req.OnDropped != nil {
			go req.OnDropped()
		}
		return
	}
	delete(ch.refs, req.Key)
	onDropped := req.OnDropped
	req.OnDropped = func() {
		q.mu.Lock()
		if current, _ := q.active.Get(req.Key.ShardID.VChannel); current == ch && len(ch.refs) == 0 {
			q.active.Remove(req.Key.ShardID.VChannel)
		}
		q.mu.Unlock()
		if onDropped != nil {
			onDropped()
		}
	}
	// Serialize unregister/register of the same key. Release schedules cleanup
	// and the callback outside the manager lock.
	ch.resources.Release(req)
	if q.recovery != nil {
		if module := q.recovery.Module(req.Key.ShardID.VChannel); module != nil {
			module.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameSegment)
		}
	}
	q.retryAdmissionLocked(ch)
	q.mu.Unlock()
}

func (q *ReplicaQueries) Close() {
	q.cancel()
	q.mu.Lock()
	var active []*queryChannel
	q.active.Range(func(name string, ch *queryChannel) bool {
		active = append(active, ch)
		q.active.Remove(name)
		return true
	})
	q.mu.Unlock()
	for _, ch := range active {
		ch.resources.Close()
	}
	q.dispatch.Close()
}

func (q *ReplicaQueries) GetQueryReadRuntime(key qviews.QueryViewKey) (*queryresource.ReadRuntime, bool) {
	ch, _ := q.active.Get(key.ShardID.VChannel)
	if ch == nil {
		return nil, false
	}
	return ch.resources.ReadRuntime(key)
}

func (q *ReplicaQueries) QueryRuntime(key qviews.QueryViewKey) (snview.QueryRuntime, bool) {
	v, ok := q.GetQueryReadRuntime(key)
	if !ok {
		return nil, false
	}
	return v, true
}

func (q *ReplicaQueries) Observe(batch []message.ImmutableMessage, through uint64) {
	q.events.Lock()
	defer q.events.Unlock()
	if through <= q.through {
		return
	}
	for _, msg := range batch {
		if msg.TimeTick() <= q.through {
			continue
		}
		if ch, ok := q.active.Get(msg.VChannel()); ok {
			ch.resources.ObserveEvent(q.ctx, walview.VChannelResourceEvent{Message: msg})
		}
	}
	// A physical confirmation covers every vchannel on this PChannel.
	q.active.Range(func(_ string, ch *queryChannel) bool {
		ch.resources.ObserveEvent(q.ctx, walview.VChannelResourceEvent{QueryBarrier: &walview.QueryBarrierEvent{TimeTick: through}})
		return true
	})
	q.through = through
	close(q.changed)
	q.changed = make(chan struct{})
}

func (q *ReplicaQueries) capture(ctx context.Context, meta *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) error {
	if err := q.WaitForAssignments(ctx, meta.Vchannel, meta.RequiredStreamingVersion); err != nil {
		return err
	}
	cursor, floor, err := q.buffer.Pin(false)
	if err != nil {
		return err
	}
	defer cursor.Close()
	catalog := resource.Resource().StreamingNodeCatalog()
	vcs, err := catalog.ListVChannel(ctx, q.channel)
	if err != nil {
		return err
	}
	var vc *VChannelView
	frontier := floor
	for _, m := range vcs {
		if m.Vchannel == meta.Vchannel {
			vc = NewVChannelViewFromMeta(m)
			frontier = max(frontier, m.CheckpointTimeTick)
			break
		}
	}
	all, err := catalog.ListSegmentAssignment(ctx, q.channel)
	if err != nil {
		return err
	}
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	for _, s := range all {
		if s.Vchannel != meta.Vchannel {
			continue
		}
		frontier = max(frontier, s.CheckpointTimeTick)
		segments[s.SegmentId] = s
	}
	store := walsummary.NewStore(resource.Resource().ChunkManager(), q.channel, 0)
	manifest, err := walsummary.ReadSnapshot(ctx, store, math.MaxInt64)
	if err != nil {
		return err
	}
	q.events.Lock()
	defer q.events.Unlock()
	if err := q.waitForWALLocked(ctx, frontier); err != nil {
		return err
	}
	tail, err := cursor.Window(q.through)
	if err != nil {
		return err
	}
	view, err := buildReplicaView(meta, vc, segments, tail, q.through)
	if err != nil {
		return err
	}
	view.PChannel = q.channel
	view.TransformLogStream = walsummary.NewQuerySnapshot(store, manifest, tail, q.through)
	var covered atomic.Int64
	covered.Store(meta.RequiredStreamingVersion)
	view.SyncShardAssignments = func(ctx context.Context, required int64, enqueue func(walview.VChannelResourceEvent) bool) error {
		return q.syncAssignments(ctx, meta.Vchannel, &covered, required, enqueue)
	}
	view.WithResourceEventLock = func(work func()) { q.events.Lock(); defer q.events.Unlock(); work() }
	view.CaptureQueryView = func(ctx context.Context, m *viewpb.QueryViewMeta, cb func(walview.VChannelWALView)) bool {
		return q.capture(ctx, m, cb) == nil
	}
	install(view)
	return nil
}

// The cursor belongs to this runtime. Under events, WAL delivery precedes
// seal events and the Ready barrier follows the entire batch.
func (q *ReplicaQueries) syncAssignments(ctx context.Context, vc string, covered *atomic.Int64, required int64, enqueue func(walview.VChannelResourceEvent) bool) error {
	if required <= covered.Load() {
		return nil
	}
	if err := q.WaitForAssignments(ctx, vc, required); err != nil {
		return err
	}
	assignments, err := resource.Resource().StreamingNodeCatalog().ListSegmentAssignment(ctx, q.channel)
	if err != nil {
		return err
	}
	var frontier uint64
	for _, s := range assignments {
		if s.Vchannel == vc && s.SealedAtDataVersion != nil && s.SealedAtDataVersion.StreamingVersion <= required {
			frontier = max(frontier, s.CheckpointTimeTick)
		}
	}
	q.events.Lock()
	defer q.events.Unlock()
	if err := q.waitForWALLocked(ctx, frontier); err != nil {
		return err
	}
	after := covered.Load()
	if required <= after {
		return nil
	}
	for _, s := range assignments {
		version := s.GetSealedAtDataVersion().GetStreamingVersion()
		if s.Vchannel == vc && version > after && version <= required {
			if !enqueue(walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: s.SegmentId, VChannel: s.Vchannel, SealedAtDataVersion: qviews.FromProtoDataVersion(s.SealedAtDataVersion)}}) {
				return context.Canceled
			}
		}
	}
	covered.Store(required)
	return nil
}

func (q *ReplicaQueries) waitForWALLocked(ctx context.Context, frontier uint64) error {
	for q.through < frontier {
		changed := q.changed
		q.events.Unlock()
		select {
		case <-ctx.Done():
		case <-q.ctx.Done():
		case <-changed:
		}
		q.events.Lock()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if q.ctx.Err() != nil {
			return q.ctx.Err()
		}
	}
	return ctx.Err()
}

// WaitForAssignments takes a finite cut of this writer's outstanding commits.
// DataCoord allocated the target before the command arrived. Every local commit
// that could have produced it has therefore already started or been recovered.
func (q *ReplicaQueries) WaitForAssignments(ctx context.Context, vc string, required int64) error {
	if required == 0 {
		return nil
	}
	q.mu.Lock()
	recovery, owner := q.recovery, q.writerContext
	q.mu.Unlock()
	if recovery == nil {
		return nil
	} // RO receives Preparing after writer confirmation.
	m := recovery.Module(vc)
	if m == nil {
		return nil
	} // Admission handles a retired shard.
	m.mu.Lock()
	if required <= m.confirmedQueryStreamingVersion {
		m.mu.Unlock()
		return nil
	}
	var pending []<-chan struct{}
	for _, s := range m.segments {
		if persisted := s.PublicationWait(required); persisted != nil {
			pending = append(pending, persisted)
		}
	}
	m.mu.Unlock()
	for _, persisted := range pending {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-owner.Done():
			return owner.Err()
		case <-persisted:
		}
	}
	m.mu.Lock()
	m.confirmedQueryStreamingVersion = max(m.confirmedQueryStreamingVersion, required)
	m.mu.Unlock()
	return nil
}

func buildReplicaView(meta *viewpb.QueryViewMeta, vc *VChannelView, segments map[int64]*streamingpb.SegmentAssignmentMeta, tail []message.ImmutableMessage, through uint64) (walview.VChannelWALView, error) {
	var empty walview.VChannelWALView
	version := qviews.FromProtoDataVersion(meta.Version.DataVersion)
	inserts := make(map[int64][]message.ImmutableMessage)
	for _, msg := range tail {
		if msg.VChannel() != meta.Vchannel {
			continue
		}
		switch msg.MessageType() {
		case message.MessageTypeCreateCollection:
			create := message.MustAsImmutableCreateCollectionMessageV1(msg)
			if vc == nil {
				vc = NewVChannelViewFromCreateCollectionMessage(create)
			} else if next, _ := vc.ObserveCreateCollectionMessageV1(create); next != nil {
				vc = next
			}
		case message.MessageTypeAlterCollection:
			if vc != nil {
				vc.ObserveAlterCollectionMessageV2(message.MustAsImmutableAlterCollectionMessageV2(msg))
			}
		case message.MessageTypeSchemaChange:
			if vc != nil {
				vc.ObserveSchemaChangeMessageV2(message.MustAsImmutableSchemaChangeMessageV2(msg))
			}
		case message.MessageTypeDropCollection:
			if vc != nil {
				vc.ObserveDropCollectionMessageV1(message.MustAsImmutableDropCollectionMessageV1(msg))
			}
		case message.MessageTypeCreatePartition:
			if vc != nil {
				vc.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(msg))
			}
		case message.MessageTypeDropPartition:
			if vc != nil {
				vc.ObserveDropPartitionMessageV1(message.MustAsImmutableDropPartitionMessageV1(msg))
			}
		case message.MessageTypeCreateSegment:
			create := message.MustAsImmutableCreateSegmentMessageV2(msg)
			if id := create.Header().GetSegmentId(); segments[id] == nil {
				segments[id] = segment.NewSegmentAssignmentMetaFromCreateSegmentMessage(create)
			}
		case message.MessageTypeInsert, message.MessageTypeTxn:
			seen := make(map[int64]bool)
			err := walview.ForEachSegmentInsertMessage(msg, 0, func(i walview.SegmentInsertMessage) error {
				id := i.Assignment.GetSegmentAssignment().GetSegmentId()
				if s := segments[id]; s != nil && i.TimeTick > s.CheckpointTimeTick && !seen[id] {
					inserts[id] = append(inserts[id], msg)
					seen[id] = true
				}
				return nil
			})
			if err != nil {
				return empty, err
			}
		}
	}
	if vc == nil || !vc.IsActive() || !version.GTE(vc.SegmentDataVersionSummary()) {
		return empty, merr.WrapErrServiceNotReadyMsg("query replica vchannel %s is unavailable at requested version", meta.Vchannel)
	}
	schema, err := vc.GetSchemaVersion(meta.CollectionId, meta.SchemaVersion)
	if err != nil {
		return empty, err
	}
	state, ok := vc.WritePathRecoveryState()
	if !ok {
		return empty, merr.WrapErrServiceNotReadyMsg("missing replica schema for %s", meta.Vchannel)
	}
	snapshot := walview.VisibleSegmentSnapshot{VChannel: meta.Vchannel, CollectionID: meta.CollectionId, DataVersion: version, BaseGrowingTimeTick: through}
	partitions := typeutil.NewSet(state.PartitionIDs...)
	for _, s := range segments {
		if !partitions.Contain(s.PartitionId) {
			continue
		}
		if s.SealedAtDataVersion != nil && !qviews.FromProtoDataVersion(s.SealedAtDataVersion).GT(version) {
			snapshot.FlushedSegments = append(snapshot.FlushedSegments, walview.FlushedSegment{SegmentID: s.SegmentId, PartitionID: s.PartitionId, FlushTimeTick: s.CheckpointTimeTick, SealedAtDataVersion: qviews.FromProtoDataVersion(s.SealedAtDataVersion)})
			continue
		}
		if s.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED && s.SealedAtDataVersion == nil {
			continue
		}
		snapshot.Segments = append(snapshot.Segments, walview.VisibleSegment{SegmentID: s.SegmentId, PartitionID: s.PartitionId, Schema: vc.restoreSegmentSchema(s.PartitionId, s.GetStat().GetCreateSegmentTimeTick(), s.SchemaVersion), Assignment: s, SealedAtDataVersion: s.SealedAtDataVersion, Data: walview.SegmentSnapshotData{PersistedStorage: s.PersistedStorage, InsertMessages: inserts[s.SegmentId]}})
	}
	after := max(deleteReplayStartAfter(snapshot), meta.TransformStartAfterTimetick)
	if len(snapshot.Segments) == 0 {
		after = through
	}
	return walview.VChannelWALView{VChannel: meta.Vchannel, CollectionID: meta.CollectionId, BaseGrowingTimeTick: through, BaseTransformTimeTick: through, LoadInfoVersion: meta.LoadInfoVersion, Schema: schema.Schema, WriteSchema: state.Schema, SegmentSnapshot: snapshot, DeleteReplayStartAfterTimeTick: after}, nil
}
