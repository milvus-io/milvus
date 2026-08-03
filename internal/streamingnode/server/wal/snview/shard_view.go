package snview

import (
	"context"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// snShardView manages all query view state machines for a single shard on a StreamingNode.
// All public methods are concurrent-safe via the internal mutex.
//
// All StreamingNodeResourceManager operations (Acquire, Release) are
// invoked while holding the shard mutex. The ResourceManager's liveness
// contracts require that all callbacks are asynchronous, so this does not
// cause deadlocks.
type snShardView struct {
	mu     sync.Mutex
	closed bool

	detached        bool
	ctx             context.Context
	pchannel        string
	shardID         qviews.ShardID
	collectionID    int64
	hasCollectionID bool
	views           map[qviews.QueryViewVersion]*snViewEntry
	catalog         metastore.StreamingNodeCataLog
	resMgr          StreamingNodeResourceManager
	onEmpty         func(*snShardView) // called (under mu) when the last view entry is removed
}

// snViewEntry pairs an ApplyView (carrying the OnReport callback) with its state machine.
type snViewEntry struct {
	handler.ApplyView
	sm             *snQueryViewStateMachine
	releaseStarted bool
	releaseDone    chan struct{}
}

// recoverSnShardView constructs an snShardView from pre-built recovered state machines.
// Recovery starts only after the handler publishes the shard.
func recoverSnShardView(
	ctx context.Context,
	pchannel string,
	shardID qviews.ShardID,
	views map[qviews.QueryViewVersion]*snQueryViewStateMachine,
	catalog metastore.StreamingNodeCataLog,
	resMgr StreamingNodeResourceManager,
) *snShardView {
	entries := make(map[qviews.QueryViewVersion]*snViewEntry, len(views))
	for version, sm := range views {
		// Populate ApplyView.View from SM's meta+snView so that
		// consumeAndRelease can safely call entry.View.QueryViewKey().
		view := qviews.NewQueryViewAtWorkNodeFromProto(&viewpb.QueryViewOfShard{
			Meta:          sm.Meta(),
			QueryNode:     sm.QueryNodes(),
			StreamingNode: sm.SNView(),
		})
		entries[version] = &snViewEntry{
			ApplyView: handler.ApplyView{View: view},
			sm:        sm,
		}
	}
	s := &snShardView{
		ctx:      ctx,
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

	return s
}

// startRecovery starts resource acquisition for recovered views after the
// handler has published the shard and installed its empty callback.
func (s *snShardView) startRecovery() {
	s.mu.Lock()
	defer s.mu.Unlock()

	versions := make([]qviews.QueryViewVersion, 0, len(s.views))
	for version := range s.views {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[j].GT(versions[i])
	})
	for _, version := range versions {
		key := qviews.QueryViewKey{ShardID: s.shardID, QueryViewVersion: version}
		entry := s.views[version]
		v := version // capture loop variable
		s.resMgr.Acquire(AcquireResource{
			Key:  key,
			Meta: entry.sm.Meta(),
			OnReady: func() {
				s.notifyRecoveringDone(v)
			},
			OnUnrecoverable: func() {
				s.notifyUnrecoverable(v)
			},
		})
	}
}

// ApplyViews applies a batch of coord-pushed views atomically.
func (s *snShardView) ApplyViews(views []handler.ApplyView) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.detached {
		return false
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
	return true
}

func (s *snShardView) CloseForHandoff() {
	s.mu.Lock()
	s.closed = true

	s.detached = true
	releases := make([]<-chan struct{}, 0, len(s.views))
	for version, entry := range s.views {
		releases = append(releases, s.startReleaseLocked(version, entry))
	}
	s.views = make(map[qviews.QueryViewVersion]*snViewEntry)
	if s.onEmpty != nil {
		s.onEmpty(s)
	}
	s.mu.Unlock()

	for _, releaseDone := range releases {
		<-releaseDone
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
			sm := newSNQueryViewStateMachine(pb.Meta, pb.StreamingNode, pb.QueryNode)
			entry = &snViewEntry{ApplyView: *av, sm: sm}
			s.views[key.QueryViewVersion] = entry
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
		case qviews.QueryViewStateDown, qviews.QueryViewStateDropped:
			// A missing teardown view means the SN has already lost its local
			// state and resources. Report Dropped so Coord can finish cleanup
			// and notify all QueryNodes.
			if av.OnReport != nil {
				pb := av.View.IntoProto()
				pb.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateDropped)
				av.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(pb))
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

	// Existing view: replace callback and deliver coord push.
	entry.ApplyView = *av
	entry.sm.UpdateView(av.View.IntoProto())
	entry.sm.OnCoordStateDelivered(pushedState)
	s.consumeReportPersistAndCleanup(key.QueryViewVersion, entry)
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

	entry.sm.OnReady()
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

	entry.sm.OnRecoveringDone()
	s.consumeReportPersistAndCleanup(version, entry)
}

// consumeReport drains pending report and invokes callback.
// Caller must hold s.mu.
func (s *snShardView) consumeReport(entry *snViewEntry) {
	report := entry.sm.ConsumeReport()
	if report != nil && entry.OnReport != nil {
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

	entry.sm.OnDropped()
	s.consumeReportPersistAndCleanup(version, entry)
}

// consumeReportPersistAndCleanup drains pending persist, report, and release,
// invokes callbacks, and removes the entry if it has reached Dropped state.
// Persist is done BEFORE report: if SN crashes after reporting but before
// persisting, Coord would believe the state advanced while SN lost it.
// Caller must hold s.mu.
func (s *snShardView) consumeReportPersistAndCleanup(version qviews.QueryViewVersion, entry *snViewEntry) {
	if !s.consumeAndPersist(entry) {
		return
	}
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
		s.detached = true
		s.onEmpty(s)
	}
}

// consumeAndPersist drains pending persist and writes to catalog.
// The catalog handles save vs delete based on the view's state.
// Caller must hold s.mu.
func (s *snShardView) consumeAndPersist(entry *snViewEntry) bool {
	persist := entry.sm.ConsumePersist()
	if persist == nil {
		return true
	}
	if err := s.catalog.SaveQueryViews(s.ctx, s.pchannel, []*viewpb.QueryViewOfShard{persist}); err != nil {
		if s.ctx.Err() != nil {
			return false
		}
		panic(merr.Wrapf(err, "persist query view %s failed", persist.GetMeta().GetVchannel()))
	}
	return true
}

// consumeAndRelease drains pending release and calls ResourceManager.Release.
// Caller must hold s.mu.
func (s *snShardView) consumeAndRelease(version qviews.QueryViewVersion, entry *snViewEntry) {
	if !entry.sm.ConsumeRelease() {
		return
	}
	s.startReleaseLocked(version, entry)
}

// startReleaseLocked starts exactly one resource release for an entry and
// returns the completion channel shared by normal dropping and handoff.
// Caller must hold s.mu.
func (s *snShardView) startReleaseLocked(version qviews.QueryViewVersion, entry *snViewEntry) <-chan struct{} {
	if entry.releaseStarted {
		return entry.releaseDone
	}
	entry.releaseStarted = true
	entry.releaseDone = make(chan struct{})
	key := entry.View.QueryViewKey()
	if key.QueryViewVersion == (qviews.QueryViewVersion{}) {
		key = qviews.QueryViewKey{ShardID: s.shardID, QueryViewVersion: version}
	}
	s.resMgr.Release(ReleaseResource{
		Key: key,
		OnDropped: func() {
			defer close(entry.releaseDone)
			s.notifyDropped(version)
		},
	})
	return entry.releaseDone
}
