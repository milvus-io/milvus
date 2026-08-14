package snview

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// snShardView manages all query view state machines for a single shard on a StreamingNode.
// All public methods are concurrent-safe via the internal mutex.
//
// All StreamingNodeResourceManager operations (Acquire, Release) are
// invoked while holding the shard mutex. The ResourceManager's liveness
// contracts require that all callbacks are asynchronous, so this does not
// cause deadlocks.
type snShardView struct {
	mu              sync.Mutex
	closed          bool
	pchannel        string
	shardID         qviews.ShardID
	collectionID    int64
	hasCollectionID bool
	views           map[qviews.QueryViewVersion]*snViewEntry
	catalog         metastore.StreamingNodeCataLog
	resMgr          StreamingNodeResourceManager
	onEmpty         func() // called (under mu) when the last view entry is removed
}

// snViewEntry pairs an ApplyView (carrying the OnReport callback) with its state machine.
type snViewEntry struct {
	handler.ApplyView
	sm             *snQueryViewStateMachine
	recovered      bool
	queryRefs      int
	releasePending bool
}

// recoverSnShardView constructs an snShardView from pre-built recovered state machines
// and starts recovery for each view via ResourceManager (under shard lock).
// Called during handler construction.
func recoverSnShardView(
	pchannel string,
	shardID qviews.ShardID,
	views map[qviews.QueryViewVersion]*snQueryViewStateMachine,
	catalog metastore.StreamingNodeCataLog,
	resMgr StreamingNodeResourceManager,
) *snShardView {
	entries := make(map[qviews.QueryViewVersion]*snViewEntry, len(views))
	for version, sm := range views {
		// Populate ApplyView.View from SM's full shard view so query planning
		// after recovery still sees QueryNode topology.
		view := qviews.NewQueryViewAtWorkNodeFromProto(&viewpb.QueryViewOfShard{
			Meta:          sm.Meta(),
			QueryNode:     sm.QueryNodes(),
			StreamingNode: sm.SNView(),
		})
		entries[version] = &snViewEntry{
			ApplyView: handler.ApplyView{View: view},
			sm:        sm,
			recovered: true,
		}
	}
	s := &snShardView{
		pchannel: pchannel,
		shardID:  shardID,
		views:    entries,
		catalog:  catalog,
		resMgr:   resMgr,
	}
	for _, sm := range views {
		s.setCollectionIDLocked(sm.Meta().GetCollectionId())
		break
	}

	// Start recovery for each view via ResourceManager under shard lock.
	versions := make([]qviews.QueryViewVersion, 0, len(views))
	for version := range views {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[j].GT(versions[i])
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, version := range versions {
		key := qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version}
		v := version // capture loop variable
		qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeRecoverAcquireResourceEvent{
			View: key,
		})
		resMgr.Acquire(AcquireResource{
			Key:  key,
			Meta: views[version].Meta(),
			OnReady: func() {
				s.notifyRecoveringDone(v)
			},
			OnUnrecoverable: func() {
				s.notifyUnrecoverable(v)
			},
		})
	}

	return s
}

// ApplyViews applies a batch of coord-pushed views atomically.
// Preparing and Up views are processed first so new serving candidates are
// installed before old views are released.
func (s *snShardView) ApplyViews(views []handler.ApplyView) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}

	for i := range views {
		state := views[i].View.State()
		if state == qviews.QueryViewStatePreparing || state == qviews.QueryViewStateUp {
			s.applyOneLocked(&views[i])
		}
	}
	for i := range views {
		state := views[i].View.State()
		if state != qviews.QueryViewStatePreparing && state != qviews.QueryViewStateUp {
			s.applyOneLocked(&views[i])
		}
	}
}

func (s *snShardView) CloseForHandoff() {
	s.mu.Lock()
	s.closed = true
	releases := make([]qviews.QueryViewKey, 0, len(s.views))
	for version, entry := range s.views {
		key := entry.View.QueryViewKey()
		if key.QueryViewVersion == (qviews.QueryViewVersion{}) {
			key = qviews.QueryViewKey{ShardID: s.shardID, QueryViewVersion: version}
		}
		releases = append(releases, key)
	}
	s.views = make(map[qviews.QueryViewVersion]*snViewEntry)
	if s.onEmpty != nil {
		s.onEmpty()
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(releases))
	for _, key := range releases {
		k := key
		s.resMgr.Release(ReleaseResource{
			Key: k,
			OnDropped: func() {
				wg.Done()
			},
		})
	}
	wg.Wait()
}

func (s *snShardView) acquireLatestUpView(ctx context.Context) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var selected *snViewEntry
	var selectedVersion qviews.QueryViewVersion
	for version, entry := range s.views {
		if entry.sm.State() != qviews.QueryViewStateUp {
			continue
		}
		if selected == nil || version.GT(selectedVersion) {
			selected = entry
			selectedVersion = version
		}
	}
	if selected == nil {
		return nil, viewerror.NewViewNotFound("latest up query view %s is not found", s.shardID.String())
	}
	selected.queryRefs++
	view := proto.Clone(selected.View.IntoProto()).(*viewpb.QueryViewOfShard)
	var once sync.Once
	return &QueryViewLease{
		Version: selectedVersion,
		Meta:    proto.Clone(view.GetMeta()).(*viewpb.QueryViewMeta),
		View:    view,
		Release: func() { once.Do(func() { s.releaseQueryViewLease(selectedVersion) }) },
	}, nil
}

func (s *snShardView) releaseQueryViewLease(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists || entry.queryRefs == 0 {
		return
	}
	entry.queryRefs--
	if entry.queryRefs == 0 && entry.releasePending {
		s.releaseQueryResourceLocked(version, entry)
	}
}

// applyOneLocked applies a single view. Caller must hold s.mu.
func (s *snShardView) applyOneLocked(av *handler.ApplyView) {
	key := av.View.QueryViewKey()
	s.setCollectionIDLocked(av.View.IntoProto().GetMeta().GetCollectionId())
	entry, exists := s.views[key.QueryViewVersion]
	pushedState := av.View.State()

	if !exists {
		switch pushedState {
		case qviews.QueryViewStatePreparing:
			// New Preparing view: create SM and acquire resources.
			snView := av.View.(*qviews.QueryViewAtStreamingNode)
			pb := snView.IntoProto()
			sm := newSNQueryViewStateMachine(
				pb.Meta,
				pb.StreamingNode,
				pb.QueryNode,
			)
			entry = &snViewEntry{ApplyView: *av, sm: sm}
			s.views[key.QueryViewVersion] = entry
			qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeAcquireResourceEvent{
				View: key,
			})
			// SN SM constructor generates a Preparing report.
			s.consumeReport(entry)

			// Tell ResourceManager to prepare resources. Callbacks will drive SM progress.
			version := key.QueryViewVersion
			s.resMgr.Acquire(AcquireResource{
				Key:  key,
				Meta: sm.Meta(),
				OnReady: func() {
					s.notifyReady(version)
				},
				OnUnrecoverable: func() {
					s.notifyUnrecoverable(version)
				},
			})
		case qviews.QueryViewStateDropped:
			// View doesn't exist (e.g., SN restarted). Report Dropped immediately
			// so Coord can finish cleanup.
			if av.OnReport != nil {
				av.OnReport(av.View)
			}
		default:
			// View unknown to this node (e.g., state lost after restart).
			// Report Unrecoverable so Coord can generate a replacement view.
			if av.OnReport != nil {
				pb := av.View.IntoProto()
				pb.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateUnrecoverable)
				av.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(pb))
			}
		}
		return
	}

	if pushedState == qviews.QueryViewStateUp {
		s.retireSupersededRecoveredViewsLocked(key.QueryViewVersion)
	}

	// Existing view: replace callback and deliver coord push.
	entry.ApplyView = *av
	entry.sm.UpdateView(av.View.IntoProto())
	before := entry.sm.State()
	entry.sm.OnCoordStateDelivered(pushedState)
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeApplyCoordViewEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: collectionIDForEntry(entry),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(key.QueryViewVersion, entry)
}

// retireSupersededRecoveredViewsLocked removes startup-only views that are no
// longer known by Coord. A higher Up view is sufficient proof that these older
// recovered views must not keep the shared query runtime at an obsolete
// DataVersion. Normal handoff views are not marked recovered and remain under
// Coord's lease-driven lifecycle.
func (s *snShardView) retireSupersededRecoveredViewsLocked(upVersion qviews.QueryViewVersion) {
	for version, entry := range s.views {
		if !entry.recovered || !upVersion.GT(version) {
			continue
		}
		state := entry.sm.State()
		if state != qviews.QueryViewStateUp && state != qviews.QueryViewStateUpRecovering {
			continue
		}
		key := entry.View.QueryViewKey()
		entry.sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
		qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeApplyCoordViewEvent{
			ViewStateTransition: qvobserve.ViewStateTransition{
				CollectionID: collectionIDForEntry(entry),
				View:         key,
				From:         state,
				To:           entry.sm.State(),
			},
		})
		s.consumeReportPersistAndCleanup(version, entry)
	}
}

func (s *snShardView) setCollectionIDLocked(collectionID int64) {
	if collectionID == 0 {
		return
	}
	if s.hasCollectionID {
		return
	}
	s.collectionID = collectionID
	s.hasCollectionID = true
}

// notifyReady is called by ResourceManager callback when resource preparation
// completes. Drives the SM from Preparing → Ready.
func (s *snShardView) notifyReady(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	key := entry.View.QueryViewKey()
	before := entry.sm.State()
	entry.sm.OnReady()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeResourceReadyEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: collectionIDForEntry(entry),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(version, entry)
}

// notifyUnrecoverable is called by ResourceManager when the requested
// resources can no longer be reconstructed at the QueryView DataVersion.
func (s *snShardView) notifyUnrecoverable(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}
	entry.sm.OnUnrecoverable()
	s.consumeReportPersistAndCleanup(version, entry)
}

// notifyRecoveringDone is called by ResourceManager callback when WAL catch-up
// completes. Drives the SM from UpRecovering → Up.
func (s *snShardView) notifyRecoveringDone(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	key := entry.View.QueryViewKey()
	before := entry.sm.State()
	entry.sm.OnRecoveringDone()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeRecoveringDoneEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: collectionIDForEntry(entry),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(version, entry)
}

// consumeReport drains pending report and invokes callback.
// Caller must hold s.mu.
func (s *snShardView) consumeReport(entry *snViewEntry) {
	report := entry.sm.ConsumeReport()
	if report != nil && entry.OnReport != nil {
		qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeReportViewEvent{
			View:  entry.View.QueryViewKey(),
			State: qviews.QueryViewState(report.GetMeta().GetState()),
		})
		entry.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(report))
	}
}

// notifyDropped is called by ResourceManager callback when resource release
// completes. Drives the SM from Dropping → Dropped.
func (s *snShardView) notifyDropped(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	key := entry.View.QueryViewKey()
	before := entry.sm.State()
	entry.sm.OnDropped()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeReleaseDoneEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: collectionIDForEntry(entry),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(version, entry)
}

// consumeReportPersistAndCleanup drains pending persist, report, and release,
// invokes callbacks, and removes the entry if it has reached Dropped state.
// Persist is done BEFORE report: if SN crashes after reporting but before
// persisting, Coord would believe the state advanced while SN lost it.
// Caller must hold s.mu.
func (s *snShardView) consumeReportPersistAndCleanup(version qviews.QueryViewVersion, entry *snViewEntry) {
	s.consumeAndPersist(entry)
	s.consumeReport(entry)
	s.consumeAndRelease(version, entry)
	s.cleanupIfDropped(version, entry)
}

// cleanupIfDropped removes the entry if it has reached Dropped state,
// and fires the onEmpty callback if the shard has no more entries.
// Caller must hold s.mu.
func (s *snShardView) cleanupIfDropped(version qviews.QueryViewVersion, entry *snViewEntry) {
	if entry.sm.State() != qviews.QueryViewStateDropped {
		return
	}
	delete(s.views, version)
	if len(s.views) == 0 && s.onEmpty != nil {
		s.onEmpty()
	}
}

// consumeAndPersist drains pending persist and writes to catalog.
// The catalog handles save vs delete based on the view's state.
// Caller must hold s.mu.
func (s *snShardView) consumeAndPersist(entry *snViewEntry) {
	persist := entry.sm.ConsumePersist()
	if persist == nil {
		return
	}
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodePersistViewEvent{
		View:  entry.View.QueryViewKey(),
		State: qviews.QueryViewState(persist.GetMeta().GetState()),
	})
	if err := s.catalog.SaveQueryViews(context.Background(), s.pchannel, []*viewpb.QueryViewOfShard{persist}); err != nil {
		panic(fmt.Sprintf("persist query view %s failed: %v", persist.GetMeta().GetVchannel(), err))
	}
}

// consumeAndRelease drains pending release and calls ResourceManager.Release.
// Caller must hold s.mu.
func (s *snShardView) consumeAndRelease(version qviews.QueryViewVersion, entry *snViewEntry) {
	if !entry.sm.ConsumeRelease() {
		return
	}
	if entry.queryRefs > 0 {
		entry.releasePending = true
		return
	}
	s.releaseQueryResourceLocked(version, entry)
}

func (s *snShardView) releaseQueryResourceLocked(version qviews.QueryViewVersion, entry *snViewEntry) {
	entry.releasePending = false
	key := entry.View.QueryViewKey()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeReleaseResourceEvent{
		View: key,
	})
	s.resMgr.Release(ReleaseResource{
		Key: key,
		OnDropped: func() {
			s.notifyDropped(version)
		},
	})
}

func collectionIDForEntry(entry *snViewEntry) int64 {
	return entry.sm.Meta().GetCollectionId()
}
