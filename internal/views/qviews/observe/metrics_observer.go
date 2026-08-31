package observe

import (
	"container/heap"
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

const defaultViewStateMaxAgeTopN = 5

const (
	shardLoadStateLoading    = "loading"
	shardLoadStateRecovering = "recovering"
	shardLoadStateLoaded     = "loaded"
)

type MetricsObserver struct {
	mu              sync.Mutex
	now             func() time.Time
	topN            int
	nextID          uint64
	states          map[metricViewKey]metricViewState
	shards          map[metricShardKey]*metricShardState
	topK            map[string]*viewStateAgeHeap
	topKValidCounts map[string]int
}

type metricViewKey struct {
	component string
	view      qviews.QueryViewKey
}

type metricShardKey struct {
	component string
	shard     qviews.ShardID
}

type metricViewState struct {
	collectionID int64
	state        qviews.QueryViewState
	enteredAt    time.Time
	version      uint64
}

type metricShardState struct {
	activeCount  int
	upCount      int
	hasLoaded    bool
	currentState string
}

type viewStateAgeCandidate struct {
	key       metricViewKey
	enteredAt time.Time
	version   uint64
}

type viewStateAgeHeap []viewStateAgeCandidate

func (h viewStateAgeHeap) Len() int {
	return len(h)
}

func (h viewStateAgeHeap) Less(i, j int) bool {
	if !h[i].enteredAt.Equal(h[j].enteredAt) {
		return h[i].enteredAt.Before(h[j].enteredAt)
	}
	return lessMetricViewKey(h[i].key, h[j].key)
}

func (h viewStateAgeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *viewStateAgeHeap) Push(x any) {
	*h = append(*h, x.(viewStateAgeCandidate))
}

func (h *viewStateAgeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func NewMetricsObserver() *MetricsObserver {
	return newMetricsObserverWithNow(time.Now)
}

func newMetricsObserverWithNow(now func() time.Time) *MetricsObserver {
	observer := &MetricsObserver{
		now:             now,
		topN:            defaultViewStateMaxAgeTopN,
		states:          make(map[metricViewKey]metricViewState),
		shards:          make(map[metricShardKey]*metricShardState),
		topK:            make(map[string]*viewStateAgeHeap),
		topKValidCounts: make(map[string]int),
	}
	return observer
}

func (o *MetricsObserver) collectViewStateMaxAge() []metrics.QVViewStateMaxAgeMetric {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := o.now()
	components := make([]string, 0, len(o.topK))
	for component := range o.topK {
		components = append(components, component)
	}
	sort.Strings(components)

	result := make([]metrics.QVViewStateMaxAgeMetric, 0, len(components)*o.topN)
	for _, component := range components {
		h := o.topK[component]
		selected := make([]viewStateAgeCandidate, 0, o.topN)
		for h.Len() > 0 && len(selected) < o.topN {
			candidate := heap.Pop(h).(viewStateAgeCandidate)
			state, ok := o.states[candidate.key]
			if !ok || state.version != candidate.version || !isMaxAgeCandidateState(state.state) {
				continue
			}
			selected = append(selected, candidate)
			result = append(result, metricFromViewState(candidate.key, state, len(selected), now))
		}
		for _, candidate := range selected {
			heap.Push(h, candidate)
		}
		if h.Len() == 0 {
			delete(o.topK, component)
			delete(o.topKValidCounts, component)
		}
	}
	return result
}

func (o *MetricsObserver) Observe(_ context.Context, event Event) {
	component := event.ComponentInfo()
	if component == "" {
		return
	}
	if created, ok := event.(CoordViewCreatedEvent); ok {
		o.setState(component, created.CollectionID, created.View, created.State)
		return
	}
	if acquired, ok := event.(QueryNodeAcquireSegmentsEvent); ok {
		o.setState(component, acquired.CollectionID, acquired.View, qviews.QueryViewStatePreparing)
		return
	}
	if acquired, ok := event.(StreamingNodeAcquireResourceEvent); ok {
		o.setState(component, acquired.CollectionID, acquired.View, qviews.QueryViewStatePreparing)
		return
	}
	if persisted, ok := event.(CoordPersistViewEvent); ok && persisted.State == qviews.QueryViewStateDropped {
		o.deleteState(component, persisted.View)
	}

	transition, ok := metricTransition(event)
	if !ok {
		return
	}
	trigger := event.TriggerInfo()
	if trigger == "" {
		return
	}
	if transition.From != transition.To {
		metrics.QVViewTransitionTotal.WithLabelValues(
			component,
			viewStateLabel(transition.From),
			viewStateLabel(transition.To),
			trigger,
		).Inc()
	}
	if transition.To == qviews.QueryViewStateDropped {
		o.deleteState(component, transition.View)
		return
	}
	o.moveState(component, transition.CollectionID, transition.View, transition.To)
}

func (o *MetricsObserver) setState(component string, collectionID int64, view qviews.QueryViewKey, state qviews.QueryViewState) {
	o.mu.Lock()
	defer o.mu.Unlock()

	key := metricViewKey{component: component, view: view}
	o.upsertStateLocked(key, collectionID, state)
}

func (o *MetricsObserver) moveState(component string, collectionID int64, view qviews.QueryViewKey, to qviews.QueryViewState) {
	o.mu.Lock()
	defer o.mu.Unlock()

	key := metricViewKey{component: component, view: view}
	if old, ok := o.states[key]; ok && collectionID == 0 {
		collectionID = old.collectionID
	}
	o.upsertStateLocked(key, collectionID, to)
}

// upsertStateLocked inserts or updates a view state entry.
//
// A no-op transition (same state, e.g. a repeated report) keeps the original
// enteredAt and version so a stuck view keeps aging and is not counted as a
// fresh transition; only the collection id is refreshed.
func (o *MetricsObserver) upsertStateLocked(key metricViewKey, collectionID int64, state qviews.QueryViewState) {
	old, existed := o.states[key]
	if existed && old.state == state {
		if collectionID != 0 {
			old.collectionID = collectionID
		}
		o.states[key] = old
		return
	}
	if existed {
		metrics.QVViewStates.WithLabelValues(key.component, viewStateLabel(old.state)).Dec()
		o.removeShardState(key, old)
		o.updateTopKValidCount(key.component, old.state, state)
	} else if isMaxAgeCandidateState(state) {
		o.topKValidCounts[key.component]++
	}

	stateSnapshot := metricViewState{
		collectionID: collectionID,
		state:        state,
		enteredAt:    o.now(),
		version:      o.nextVersion(),
	}
	o.states[key] = stateSnapshot
	o.addShardState(key, stateSnapshot)
	o.cleanupShardState(key)
	o.pushTopKCandidate(key, stateSnapshot)
	o.compactTopKCandidates(key.component)
	metrics.QVViewStates.WithLabelValues(key.component, viewStateLabel(state)).Inc()
}

func (o *MetricsObserver) deleteState(component string, view qviews.QueryViewKey) {
	o.mu.Lock()
	defer o.mu.Unlock()

	key := metricViewKey{component: component, view: view}
	old, ok := o.states[key]
	if !ok {
		return
	}
	delete(o.states, key)
	o.removeShardState(key, old)
	o.cleanupShardState(key)
	if isMaxAgeCandidateState(old.state) {
		o.topKValidCounts[component]--
		if o.topKValidCounts[component] <= 0 {
			delete(o.topKValidCounts, component)
		}
	}
	o.compactTopKCandidates(component)
	metrics.QVViewStates.WithLabelValues(component, viewStateLabel(old.state)).Dec()
}

func (o *MetricsObserver) nextVersion() uint64 {
	o.nextID++
	return o.nextID
}

func (o *MetricsObserver) pushTopKCandidate(key metricViewKey, state metricViewState) {
	if !isMaxAgeCandidateState(state.state) {
		return
	}
	h, ok := o.topK[key.component]
	if !ok {
		h = &viewStateAgeHeap{}
		heap.Init(h)
		o.topK[key.component] = h
	}
	heap.Push(h, viewStateAgeCandidate{
		key:       key,
		enteredAt: state.enteredAt,
		version:   state.version,
	})
}

func (o *MetricsObserver) rebuildTopK(component string) {
	h := &viewStateAgeHeap{}
	validCount := 0
	for key, state := range o.states {
		if key.component != component || !isMaxAgeCandidateState(state.state) {
			continue
		}
		validCount++
		*h = append(*h, viewStateAgeCandidate{
			key:       key,
			enteredAt: state.enteredAt,
			version:   state.version,
		})
	}
	if validCount == 0 {
		delete(o.topK, component)
		delete(o.topKValidCounts, component)
		return
	}
	heap.Init(h)
	o.topK[component] = h
	o.topKValidCounts[component] = validCount
}

func (o *MetricsObserver) updateTopKValidCount(component string, from, to qviews.QueryViewState) {
	if isMaxAgeCandidateState(from) == isMaxAgeCandidateState(to) {
		return
	}
	if isMaxAgeCandidateState(to) {
		o.topKValidCounts[component]++
		return
	}
	o.topKValidCounts[component]--
	if o.topKValidCounts[component] <= 0 {
		delete(o.topKValidCounts, component)
	}
}

func (o *MetricsObserver) compactTopKCandidates(component string) {
	h, ok := o.topK[component]
	if !ok {
		return
	}
	validCount := o.topKValidCounts[component]
	if validCount == 0 {
		delete(o.topK, component)
		return
	}
	if h.Len() > validCount*4+o.topN {
		o.rebuildTopK(component)
	}
}

func isMaxAgeCandidateState(state qviews.QueryViewState) bool {
	return state != qviews.QueryViewStateUp && state != qviews.QueryViewStateDropped
}

func (o *MetricsObserver) addShardState(key metricViewKey, state metricViewState) {
	shardKey, ok := newMetricShardKey(key)
	if !ok {
		return
	}
	if !isShardLoadCandidateState(state.state) {
		return
	}
	shardState := o.getShardState(shardKey)
	oldState := shardState.currentState
	shardState.activeCount++
	if state.state == qviews.QueryViewStateUp {
		shardState.upCount++
		shardState.hasLoaded = true
	}
	o.updateShardLoadStateMetric(shardState, oldState)
}

func (o *MetricsObserver) removeShardState(key metricViewKey, state metricViewState) {
	shardKey, ok := newMetricShardKey(key)
	if !ok {
		return
	}
	shardState, ok := o.shards[shardKey]
	if !ok {
		return
	}
	if !isShardLoadCandidateState(state.state) {
		return
	}
	oldState := shardState.currentState
	shardState.activeCount--
	if state.state == qviews.QueryViewStateUp {
		shardState.upCount--
	}
	o.updateShardLoadStateMetric(shardState, oldState)
}

func (o *MetricsObserver) getShardState(key metricShardKey) *metricShardState {
	state, ok := o.shards[key]
	if ok {
		return state
	}
	state = &metricShardState{}
	o.shards[key] = state
	return state
}

func (o *MetricsObserver) updateShardLoadStateMetric(state *metricShardState, oldState string) {
	newState := state.loadState()
	if oldState == newState {
		return
	}
	if oldState != "" {
		metrics.QVShardLoadStates.WithLabelValues(oldState).Dec()
	}
	if newState != "" {
		metrics.QVShardLoadStates.WithLabelValues(newState).Inc()
	}
	state.currentState = newState
}

func (s *metricShardState) loadState() string {
	if s.activeCount <= 0 {
		return ""
	}
	if s.upCount > 0 {
		return shardLoadStateLoaded
	}
	if s.hasLoaded {
		return shardLoadStateRecovering
	}
	return shardLoadStateLoading
}

func newMetricShardKey(key metricViewKey) (metricShardKey, bool) {
	if key.component != componentCoord {
		return metricShardKey{}, false
	}
	return metricShardKey{
		component: key.component,
		shard:     key.view.ShardID,
	}, true
}

func (o *MetricsObserver) cleanupShardState(key metricViewKey) {
	shardKey, ok := newMetricShardKey(key)
	if !ok {
		return
	}
	shardState, ok := o.shards[shardKey]
	if !ok {
		return
	}
	if shardState.activeCount <= 0 && shardState.currentState == "" {
		delete(o.shards, shardKey)
	}
}

func isShardLoadCandidateState(state qviews.QueryViewState) bool {
	switch state {
	case qviews.QueryViewStatePreparing, qviews.QueryViewStateReady, qviews.QueryViewStateUp, qviews.QueryViewStateUnrecoverable:
		return true
	default:
		return false
	}
}

func metricFromViewState(key metricViewKey, state metricViewState, rank int, now time.Time) metrics.QVViewStateMaxAgeMetric {
	return metrics.QVViewStateMaxAgeMetric{
		Component:        key.component,
		State:            viewStateLabel(state.state),
		Rank:             strconv.Itoa(rank),
		CollectionID:     strconv.FormatInt(state.collectionID, 10),
		ReplicaID:        strconv.FormatInt(key.view.ShardID.ReplicaID, 10),
		VChannel:         key.view.ShardID.VChannel,
		QueryViewVersion: key.view.QueryViewVersion.String(),
		DataVersion:      key.view.QueryViewVersion.DataVersion.String(),
		AgeSeconds:       now.Sub(state.enteredAt).Seconds(),
	}
}

// viewStateLabel lowercases the state name so qv_* metrics keep one consistent
// label vocabulary (the shard load lifecycle labels are already lowercase).
func viewStateLabel(state qviews.QueryViewState) string {
	return strings.ToLower(state.String())
}

func lessMetricViewKey(left, right metricViewKey) bool {
	if left.component != right.component {
		return left.component < right.component
	}
	if left.view.ShardID.ReplicaID != right.view.ShardID.ReplicaID {
		return left.view.ShardID.ReplicaID < right.view.ShardID.ReplicaID
	}
	if left.view.ShardID.VChannel != right.view.ShardID.VChannel {
		return left.view.ShardID.VChannel < right.view.ShardID.VChannel
	}
	leftDV := left.view.QueryViewVersion.DataVersion
	rightDV := right.view.QueryViewVersion.DataVersion
	if leftDV.StreamingVersion != rightDV.StreamingVersion {
		return leftDV.StreamingVersion < rightDV.StreamingVersion
	}
	if leftDV.CompactVersion != rightDV.CompactVersion {
		return leftDV.CompactVersion < rightDV.CompactVersion
	}
	return left.view.QueryViewVersion.QueryVersion < right.view.QueryViewVersion.QueryVersion
}

func metricTransition(event Event) (ViewStateTransition, bool) {
	switch e := event.(type) {
	case CoordViewPreemptedEvent:
		return e.ViewStateTransition, true
	case CoordViewAdvancedFromUnrecoverableEvent:
		return e.ViewStateTransition, true
	case CoordViewReleaseRequestedEvent:
		return e.ViewStateTransition, true
	case CoordViewHandoffToNewUpEvent:
		return e.ViewStateTransition, true
	case CoordViewReportAppliedEvent:
		return e.ViewStateTransition, true
	case CoordViewQueryNodeLostAppliedEvent:
		return e.ViewStateTransition, true
	case QueryNodeApplyCoordViewEvent:
		return e.ViewStateTransition, true
	case QueryNodeSegmentsReadyEvent:
		return e.ViewStateTransition, true
	case QueryNodeSegmentUnrecoverableEvent:
		return e.ViewStateTransition, true
	case QueryNodeReleaseDoneEvent:
		return e.ViewStateTransition, true
	case StreamingNodeResourceReadyEvent:
		return e.ViewStateTransition, true
	case StreamingNodeApplyCoordViewEvent:
		return e.ViewStateTransition, true
	case StreamingNodeRecoveringDoneEvent:
		return e.ViewStateTransition, true
	case StreamingNodeReleaseDoneEvent:
		return e.ViewStateTransition, true
	default:
		return ViewStateTransition{}, false
	}
}
