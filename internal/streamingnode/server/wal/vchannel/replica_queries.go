package vchannel

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
	active   *typeutil.ConcurrentMap[string, *queryChannel]
	recovery *PChannelRecoveryManager // bound while this replica has writer recovery
	// events serializes snapshot installation, live delivery and readiness barriers.
	// Releasing a view must remain possible while delivery waits for queue space.
	events  sync.Mutex
	through uint64
	changed chan struct{}
}
type queryChannel struct {
	resources *queryresource.Manager
	// A non-nil request is waiting for an admissible DataVersion.
	refs           map[qviews.QueryViewKey]*snview.AcquireResource
	awaitingSealed map[int64]struct{}
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
	q.recovery = m
	return func() {
		q.mu.Lock()
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
			resources:      queryresource.NewManager(cfg),
			refs:           make(map[qviews.QueryViewKey]*snview.AcquireResource),
			awaitingSealed: make(map[int64]struct{}),
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

// PrepareUp refreshes the first sealed bindings at the last local transition
// before a prepared view may start serving. The runtime barrier also drains
// WAL effects and sealed events delivered during the catalog read.
func (q *ReplicaQueries) PrepareUp(ctx context.Context, key qviews.QueryViewKey) error {
	ch, _ := q.active.Get(key.ShardID.VChannel)
	if ch != nil {
		if runtime, ok := ch.resources.QueryRuntime(key); ok {
			return runtime.PrepareQueryView(ctx)
		}
	}
	return merr.WrapErrServiceNotReadyMsg("query replica view %s is preparing", key)
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
			if msg.MessageType() == message.MessageTypeCreateSegment {
				ch.awaitingSealed[message.MustAsImmutableCreateSegmentMessageV2(msg).Header().GetSegmentId()] = struct{}{}
			}
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
	if err := readSealedVersions(ctx, segments); err != nil {
		return err
	}
	store := walsummary.NewStore(resource.Resource().ChunkManager(), q.channel, 0)
	manifest, err := walsummary.ReadSnapshot(ctx, store, math.MaxInt64)
	if err != nil {
		return err
	}
	q.events.Lock()
	defer q.events.Unlock()
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
	view.RefreshSealedVersions = func(ctx context.Context) error { return q.refreshSealedVersions(ctx, meta.Vchannel) }
	view.WithResourceEventLock = func(work func()) { q.events.Lock(); defer q.events.Unlock(); work() }
	view.CaptureQueryView = func(m *viewpb.QueryViewMeta, cb func(walview.VChannelWALView)) bool {
		return q.capture(q.ctx, m, cb) == nil
	}
	if ch, ok := q.active.Get(meta.Vchannel); ok {
		for _, segment := range view.SegmentSnapshot.Segments {
			if segment.SealedAtDataVersion == nil {
				ch.awaitingSealed[segment.SegmentID] = struct{}{}
			}
		}
	}
	install(view)
	return nil
}

// A reused runtime also needs the first sealed binding for segments flushed
// since its initial capture. The ready barrier drains these notifications.
func (q *ReplicaQueries) refreshSealedVersions(ctx context.Context, vchannel string) error {
	all, err := resource.Resource().StreamingNodeCatalog().ListSegmentAssignment(ctx, q.channel)
	if err != nil {
		return err
	}
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	for _, s := range all {
		if s.Vchannel == vchannel {
			segments[s.SegmentId] = s
		}
	}
	q.events.Lock()
	ch, _ := q.active.Get(vchannel)
	if ch != nil {
		for id := range ch.awaitingSealed {
			if _, ok := segments[id]; !ok {
				segments[id] = &streamingpb.SegmentAssignmentMeta{SegmentId: id}
			}
		}
	}
	q.events.Unlock()
	if ch == nil {
		return nil
	}
	if err := readSealedVersions(ctx, segments); err != nil {
		return err
	}
	q.events.Lock()
	defer q.events.Unlock()
	if current, _ := q.active.Get(vchannel); current == ch {
		for id, s := range segments {
			if version := s.SealedAtDataVersion; version != nil {
				delete(ch.awaitingSealed, id)
				ch.resources.ObserveEvent(ctx, walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{
					SegmentID: id, VChannel: vchannel, SealedAtDataVersion: qviews.FromProtoDataVersion(version),
				}})
			}
		}
	}
	return nil
}

func readSealedVersions(ctx context.Context, segments map[int64]*streamingpb.SegmentAssignmentMeta) error {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return err
	}
	for id, s := range segments {
		if s.SealedAtDataVersion != nil {
			continue
		}
		resp, err := coord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{SegmentIDs: []int64{id}, IncludeUnHealthy: true})
		if err = merr.CheckRPCCall(resp, err); errors.Is(err, merr.ErrSegmentNotFound) {
			continue
		} else if err != nil {
			return err
		}
		for _, info := range resp.GetInfos() {
			if info.GetID() == id && info.SealedAtDataVersion != nil {
				s.SealedAtDataVersion = info.SealedAtDataVersion
			}
		}
	}
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
		if s.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING && s.SealedAtDataVersion == nil {
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
