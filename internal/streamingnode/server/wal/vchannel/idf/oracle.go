package idf

import (
	"context"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type bm25Stats map[int64]*storage.BM25Stats

func newBM25StatsFromSchema(schema *schemapb.CollectionSchema, loadedFields []int64) bm25Stats {
	stats := make(bm25Stats)
	if schema == nil {
		return stats
	}
	for _, function := range schema.GetFunctions() {
		if function.GetType() != schemapb.FunctionType_BM25 || len(function.GetOutputFieldIds()) == 0 {
			continue
		}
		fieldID := function.GetOutputFieldIds()[0]
		if len(loadedFields) > 0 && !slices.Contains(loadedFields, fieldID) {
			continue
		}
		stats.getOrCreate(fieldID)
	}
	return stats
}

func (s bm25Stats) getOrCreate(fieldID int64) *storage.BM25Stats {
	stats, ok := s[fieldID]
	if !ok {
		stats = storage.NewBM25Stats()
		s[fieldID] = stats
	}
	return stats
}

func (s bm25Stats) merge(src bm25Stats) {
	for fieldID, srcStats := range src {
		if srcStats == nil {
			continue
		}
		s.getOrCreate(fieldID).Merge(srcStats)
	}
}

type growingSegmentStats struct {
	createTimeTick uint64
	partitionID    int64
	stats          bm25Stats
	flushed        bool
	sealedAt       *qviews.DataVersion
}

// growingStatsStore belongs to one oracleRuntime. After initialization, its
// owner must hold oracleRuntime.mu for all access, including segment contents.
// Membership cleanup and aggregate publication share the same critical section.
type growingStatsStore struct {
	schema   *schemapb.CollectionSchema
	fieldIDs []int64
	segments map[int64]*growingSegmentStats
}

func newGrowingStatsStore(schema *schemapb.CollectionSchema, loadedFields []int64) *growingStatsStore {
	return &growingStatsStore{
		fieldIDs: loadedFields,
		schema:   schema,
		segments: make(map[int64]*growingSegmentStats),
	}
}

func (s *growingStatsStore) registerSegment(segmentID int64, partitionID int64, createTimeTick uint64) {
	if segmentID == 0 {
		return
	}
	if _, ok := s.segments[segmentID]; ok {
		return
	}
	s.segments[segmentID] = &growingSegmentStats{
		createTimeTick: createTimeTick,
		partitionID:    partitionID,
		stats:          newBM25StatsFromSchema(s.schema, s.fieldIDs),
	}
}

func (s *growingStatsStore) getOrCreateSegment(segmentID int64, partitionID int64) *growingSegmentStats {
	segment := s.segments[segmentID]
	if segment == nil {
		segment = &growingSegmentStats{
			partitionID: partitionID,
			stats:       newBM25StatsFromSchema(s.schema, s.fieldIDs),
		}
		s.segments[segmentID] = segment
	}
	return segment
}

func (s *growingStatsStore) appendStats(segmentID int64, partitionID int64, stats bm25Stats) {
	if segmentID == 0 {
		return
	}
	segment := s.getOrCreateSegment(segmentID, partitionID)
	if segment.partitionID == 0 {
		segment.partitionID = partitionID
	}
	segment.stats.merge(stats)
}

func (s *growingStatsStore) appendInsert(insert walview.SegmentInsertMessage) (int64, bm25Stats, error) {
	segmentID := insert.Assignment.GetSegmentAssignment().GetSegmentId()
	partitionID := insert.Assignment.GetPartitionId()
	stats := newBM25StatsFromSchema(s.schema, s.fieldIDs)
	if err := collectGrowingInsertStats(stats, s.schema, insert); err != nil {
		return 0, nil, err
	}
	segment := s.getOrCreateSegment(segmentID, partitionID)
	if segment.flushed {
		return 0, nil, merr.WrapErrServiceInternalMsg("BM25 growing segment %d already flushed", segmentID)
	}
	if segment.sealedAt != nil {
		return 0, nil, merr.WrapErrServiceInternalMsg("BM25 growing segment %d already sealed", segmentID)
	}
	if segment.partitionID == 0 {
		segment.partitionID = partitionID
	}
	segment.stats.merge(stats)
	return segmentID, stats, nil
}

func (s *growingStatsStore) markFlushed(segmentID int64) {
	segment := s.segments[segmentID]
	if segment == nil {
		return
	}
	for _, stats := range segment.stats {
		if stats.NumRow() > 0 {
			segment.flushed = true
			return
		}
	}
	// Empty segments retire without a sealed DataVersion or a seal notification.
	// Their zero contribution must not hold up the next aggregate publication.
	delete(s.segments, segmentID)
}

func (s *growingStatsStore) markSealed(segmentID int64, sealedAt qviews.DataVersion) {
	if segmentID == 0 {
		return
	}
	segment := s.getOrCreateSegment(segmentID, 0)
	if segment.sealedAt != nil && !segment.sealedAt.EQ(sealedAt) {
		panic("conflicting sealed data version for BM25 growing segment")
	}
	segment.sealedAt = &sealedAt
}

// selectForDataVersion returns membership and the statistics of changed segments.
// Consume the borrowed statistics under the owner's lock, or during initialization.
func (s *growingStatsStore) selectForDataVersion(
	target qviews.DataVersion,
	targetSealed map[int64]*datapb.StreamingNodeBM25Resource,
	current map[int64]struct{},
) (map[int64]struct{}, map[int64]bm25Stats) {
	next := make(map[int64]struct{})
	stats := make(map[int64]bm25Stats)
	for segmentID, segment := range s.segments {
		_, sealed := targetSealed[segmentID]
		visible := !sealed && (segment.sealedAt == nil || segment.sealedAt.GT(target))
		if visible {
			next[segmentID] = struct{}{}
		}
		_, currentlyVisible := current[segmentID]
		if visible != currentlyVisible {
			stats[segmentID] = segment.stats
		}
	}
	return next, stats
}

func (s *growingStatsStore) cleanup(currentDataVersion qviews.DataVersion, currentGrowing map[int64]struct{}) {
	for segmentID, segment := range s.segments {
		if _, ok := currentGrowing[segmentID]; ok {
			continue
		}
		if segment.sealedAt != nil && !segment.sealedAt.GT(currentDataVersion) {
			delete(s.segments, segmentID)
		}
	}
}

type idfDiff struct {
	target     qviews.DataVersion
	positive   bm25Stats
	negative   bm25Stats
	nextSealed map[int64]*datapb.StreamingNodeBM25Resource
}

type materializationCall struct {
	target qviews.DataVersion
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

type oracleRuntime struct {
	provider *Provider

	collectionID    int64
	vchannel        string
	partitionIDs    []int64
	loadInfoVersion uint64
	schema          *schemapb.CollectionSchema
	fieldIDs        []int64
	barrier         func(context.Context) error
	ctx             context.Context
	cancel          context.CancelFunc

	advanceMu       sync.Mutex
	mu              sync.RWMutex
	lazy            bool
	closed          bool
	currentVersion  qviews.DataVersion
	currentStats    bm25Stats
	currentSealed   map[int64]*datapb.StreamingNodeBM25Resource
	currentGrowing  map[int64]struct{}
	materialization *materializationCall
	growingStore    *growingStatsStore

	closeOnce sync.Once
}

func newOracleRuntime(
	ctx context.Context,
	provider *Provider,
	walView walview.VChannelWALView,
	initialResources []*datapb.StreamingNodeBM25Resource,
	lazy bool,
) (*oracleRuntime, error) {
	lifetime, cancel := context.WithCancel(context.Background())
	keep := false
	defer func() {
		if !keep {
			cancel()
		}
	}()
	r := &oracleRuntime{
		provider:        provider,
		lazy:            lazy,
		collectionID:    walView.CollectionID,
		vchannel:        walView.VChannel,
		partitionIDs:    slices.Clone(walView.PartitionIDs),
		fieldIDs:        loadFieldIDs(walView.LoadFields),
		barrier:         walView.ResourceEventBarrier,
		ctx:             lifetime,
		cancel:          cancel,
		loadInfoVersion: walView.LoadInfoVersion,
		schema:          proto.Clone(walView.Schema).(*schemapb.CollectionSchema),
		currentVersion:  walView.SegmentSnapshot.DataVersion,
		growingStore:    newGrowingStatsStore(walView.Schema, loadFieldIDs(walView.LoadFields)),
	}
	if lazy {
		if err := r.loadInitialGrowing(ctx, walView); err != nil {
			return nil, err
		}
		keep = true
		return r, nil
	}

	r.currentStats = newBM25StatsFromSchema(walView.Schema, r.fieldIDs)
	sealed, err := r.indexResources(initialResources)
	if err != nil {
		return nil, err
	}
	loaded, err := provider.loadSealedContributions(ctx, sealed, r.currentStats)
	if err != nil {
		return nil, err
	}
	for field, stats := range loaded {
		r.currentStats[field] = stats
	}
	r.currentSealed = sealed
	if err := r.loadInitialGrowing(ctx, walView); err != nil {
		return nil, err
	}
	var growingStats map[int64]bm25Stats
	r.currentGrowing, growingStats = r.growingStore.selectForDataVersion(
		walView.SegmentSnapshot.DataVersion,
		r.currentSealed,
		nil,
	)
	for _, stats := range growingStats {
		r.currentStats.merge(stats)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.growingStore.cleanup(r.currentVersion, r.currentGrowing)
	keep = true
	return r, nil
}

func (r *oracleRuntime) loadInitialGrowing(ctx context.Context, walView walview.VChannelWALView) error {
	for _, segment := range walView.SegmentSnapshot.Segments {
		if !r.includesPartition(segment.PartitionID) {
			continue
		}
		r.growingStore.registerSegment(segment.SegmentID, segment.PartitionID, segment.Assignment.GetStat().GetCreateSegmentTimeTick())
		if err := r.collectPersistedGrowingStats(ctx, segment); err != nil {
			return err
		}
		for _, raw := range segment.Data.InsertMessages {
			if err := walview.ForEachSegmentInsertMessage(raw, segment.SegmentID, func(insert walview.SegmentInsertMessage) error {
				_, _, err := r.growingStore.appendInsert(insert)
				return err
			}); err != nil {
				return err
			}
		}
		if segment.SealedAtDataVersion != nil {
			r.growingStore.markSealed(segment.SegmentID, qviews.FromProtoDataVersion(segment.SealedAtDataVersion))
		}
	}
	return nil
}

func (r *oracleRuntime) collectPersistedGrowingStats(ctx context.Context, segment walview.VisibleSegment) error {
	if r.provider.chunkManager == nil || segment.Data.PersistedStorage == nil {
		return nil
	}
	stats := newBM25StatsFromSchema(r.schema, r.fieldIDs)
	resource := &datapb.StreamingNodeBM25Resource{
		SegmentId:      segment.SegmentID,
		StorageVersion: storage.StorageV2,
		ManifestPath:   segment.Data.PersistedStorage.GetManifestPath(),
	}
	if resource.ManifestPath != "" {
		resource.StorageVersion = storage.StorageV3
	}
	for _, binlogs := range segment.Data.PersistedStorage.GetBinlogs() {
		resource.Bm25Binlogs = append(resource.Bm25Binlogs, binlogs.GetBm25Binlog()...)
	}
	// StorageV3 keeps BM25 paths in the manifest rather than explicit binlogs.
	loaded, err := loadSealedSegmentStats(ctx, r.provider.chunkManager, resource, stats)
	if err != nil {
		return err
	}
	stats.merge(loaded)
	r.growingStore.appendStats(segment.SegmentID, segment.PartitionID, stats)
	return nil
}

func (r *oracleRuntime) BuildIDF(ctx context.Context, _ qviews.DataVersion, fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	results, err := r.BuildIDFBatch(ctx, []queryresource.IDFRequest{{FieldID: fieldID, TFs: tfs}})
	if err != nil {
		return nil, 0, err
	}
	return results[0].Vectors, results[0].Avgdl, nil
}

func (r *oracleRuntime) PrepareDataVersion(ctx context.Context, target qviews.DataVersion) error {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()
	defer cancel()
	if !r.advanceMu.TryLock() {
		return nodescheduler.ErrDelay
	}
	defer r.advanceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return context.Canceled
	}
	if !target.GT(r.currentVersion) {
		r.mu.Unlock()
		return nil
	}
	if r.lazy && r.currentStats == nil {
		if err := ctx.Err(); err != nil {
			r.mu.Unlock()
			return err
		}
		call := r.materialization
		r.currentVersion = target
		r.currentGrowing = nil
		r.growingStore.cleanup(target, nil)
		r.mu.Unlock()
		if call != nil && !call.target.EQ(target) {
			call.cancel()
		}
		return nil
	}
	r.mu.Unlock()
	diff, err := r.computeDiff(ctx, target)
	if err != nil {
		return err
	}
	return r.commitDiff(ctx, diff)
}

func (r *oracleRuntime) ensureMaterialized(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return context.Canceled
		}
		if r.currentStats != nil {
			r.mu.Unlock()
			return nil
		}
		call := r.materialization
		if call == nil {
			materializationCtx, cancel := context.WithCancel(r.ctx)
			call = &materializationCall{
				target: r.currentVersion,
				ctx:    materializationCtx,
				cancel: cancel,
				done:   make(chan struct{}),
			}
			r.materialization = call
			go func() {
				defer cancel()
				_ = r.materialize(call) // The deferred completion publishes the error to waiters.
			}()
		}
		r.mu.Unlock()

		select {
		case <-call.done:
			if err := ctx.Err(); err != nil {
				return err
			}
			r.mu.RLock()
			targetChanged := !r.currentVersion.EQ(call.target)
			r.mu.RUnlock()
			if targetChanged {
				continue
			}
			return call.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *oracleRuntime) materialize(call *materializationCall) (resultErr error) {
	defer func() {
		r.mu.Lock()
		call.err = resultErr
		if r.materialization == call {
			r.materialization = nil
		}
		close(call.done)
		r.mu.Unlock()
	}()
	resources, err := r.provider.getSealedBM25Resources(
		call.ctx,
		r.collectionID,
		r.vchannel,
		call.target,
		r.partitionIDs,
		r.loadInfoVersion,
	)
	if err != nil {
		return merr.Wrapf(err, "get sealed BM25 resources for data version %s", call.target.String())
	}
	stats := newBM25StatsFromSchema(r.schema, r.fieldIDs)
	sealed, err := r.indexResources(resources)
	if err != nil {
		return err
	}
	loaded, err := r.provider.loadSealedContributions(call.ctx, sealed, stats)
	if err != nil {
		return merr.Wrapf(err, "load sealed BM25 stats for data version %s", call.target.String())
	}
	for field, value := range loaded {
		stats[field] = value
	}
	if r.barrier != nil {
		if err := r.barrier(call.ctx); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.materialization != call || r.currentStats != nil || !r.currentVersion.EQ(call.target) {
		if err := call.ctx.Err(); err != nil {
			return err
		}
		return context.Canceled
	}
	for id, segment := range r.growingStore.segments {
		_, sealedHere := sealed[id]
		if segment.sealedAt == nil && (segment.flushed || sealedHere) {
			return merr.WrapErrServiceNotReadyMsg("BM25 segment %d final commit is pending", id)
		}
	}
	growing, growingStats := r.growingStore.selectForDataVersion(call.target, sealed, nil)
	for _, segmentStats := range growingStats {
		stats.merge(segmentStats)
	}
	if err := call.ctx.Err(); err != nil {
		return err
	}
	r.currentStats = stats
	r.currentSealed = sealed
	r.currentGrowing = growing
	r.growingStore.cleanup(call.target, growing)
	return nil
}

func (r *oracleRuntime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return
	}
	if event.Message != nil {
		if err := r.applyLiveMessage(ctx, event.Message); err != nil {
			panic(merr.Wrap(err, "failed to apply live event to IDF oracle runtime"))
		}
		return
	}
	if event.SegmentSealed != nil {
		r.applySegmentSealed(event.SegmentSealed.SegmentID, event.SegmentSealed.SealedAtDataVersion)
	}
}

func (r *oracleRuntime) applyLiveMessage(_ context.Context, msg message.ImmutableMessage) error {
	if msg == nil {
		return nil
	}
	switch msg.MessageType() {
	case message.MessageTypeCreateSegment:
		created := message.MustAsImmutableCreateSegmentMessageV2(msg)
		segmentID := created.Header().GetSegmentId()
		partitionID := created.Header().GetPartitionId()
		if !r.includesPartition(partitionID) {
			return nil
		}
		r.mu.Lock()
		r.growingStore.registerSegment(segmentID, partitionID, msg.TimeTick())
		if r.currentStats != nil {
			_, sealed := r.currentSealed[segmentID]
			if _, ok := r.currentGrowing[segmentID]; !ok {
				if !sealed {
					r.currentGrowing[segmentID] = struct{}{}
				}
			}
		}
		r.mu.Unlock()
	case message.MessageTypeInsert, message.MessageTypeTxn:
		return walview.ForEachSegmentInsertMessage(msg, 0, func(insert walview.SegmentInsertMessage) error {
			if !r.includesPartition(insert.Assignment.GetPartitionId()) {
				return nil
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			segmentID, stats, err := r.growingStore.appendInsert(insert)
			if err != nil {
				return err
			}
			if r.currentStats != nil {
				if _, sealed := r.currentSealed[segmentID]; !sealed {
					r.currentGrowing[segmentID] = struct{}{}
					r.currentStats.merge(stats)
				}
			}
			return nil
		})
	case message.MessageTypeManualFlush, message.MessageTypeFlushAll, message.MessageTypeAlterWAL, message.MessageTypeCreateSnapshot:
		r.mu.Lock()
		for id, segment := range r.growingStore.segments {
			if segment.createTimeTick < msg.TimeTick() {
				r.growingStore.markFlushed(id)
			}
		}
		r.mu.Unlock()
	case message.MessageTypeFlush:
		r.mu.Lock()
		r.growingStore.markFlushed(message.MustAsImmutableFlushMessageV2(msg).Header().GetSegmentId())
		r.mu.Unlock()
	}
	return nil
}

func (r *oracleRuntime) applySegmentSealed(segmentID int64, sealedAt qviews.DataVersion) {
	r.mu.Lock()
	if _, exists := r.growingStore.segments[segmentID]; exists {
		r.growingStore.markSealed(segmentID, sealedAt)
	}
	r.growingStore.cleanup(r.currentVersion, r.currentGrowing)
	r.mu.Unlock()
}

func (r *oracleRuntime) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		call := r.materialization
		r.mu.Unlock()
		if call != nil {
			<-call.done
		}
		r.advanceMu.Lock()
		defer r.advanceMu.Unlock()
		r.mu.Lock()
		defer r.mu.Unlock()
		r.currentStats = nil
		r.currentSealed = nil
		r.currentGrowing = nil
		r.growingStore.segments = nil
	})
}

func (r *oracleRuntime) computeDiff(ctx context.Context, target qviews.DataVersion) (*idfDiff, error) {
	resources, err := r.provider.getSealedBM25Resources(ctx, r.collectionID, r.vchannel, target, r.partitionIDs, r.loadInfoVersion)
	if err != nil {
		return nil, err
	}
	next, err := r.indexResources(resources)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	current := r.currentSealed
	r.mu.RUnlock()
	added, removed := make(map[int64]*datapb.StreamingNodeBM25Resource), make(map[int64]*datapb.StreamingNodeBM25Resource)
	for id, resource := range current {
		if !proto.Equal(resource, next[id]) {
			removed[id] = resource
		}
	}
	for id, resource := range next {
		if !proto.Equal(resource, current[id]) {
			added[id] = resource
		}
	}
	fields := newBM25StatsFromSchema(r.schema, r.fieldIDs)
	negative, err := r.provider.loadSealedContributions(ctx, removed, fields)
	if err != nil {
		return nil, err
	}
	positive, err := r.provider.loadSealedContributions(ctx, added, fields)
	if err != nil {
		return nil, err
	}
	return &idfDiff{target: target, positive: positive, negative: negative, nextSealed: next}, nil
}

func (r *oracleRuntime) commitDiff(ctx context.Context, diff *idfDiff) error {
	if r.barrier != nil {
		if err := r.barrier(ctx); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !diff.target.GT(r.currentVersion) {
		return nil
	}
	for id, segment := range r.growingStore.segments {
		_, sealed := diff.nextSealed[id]
		if segment.sealedAt == nil && (segment.flushed || sealed) {
			return nodescheduler.ErrDelay
		}
	}
	nextGrowing, growingStats := r.growingStore.selectForDataVersion(diff.target, diff.nextSealed, r.currentGrowing)
	for id := range r.currentGrowing {
		if _, ok := nextGrowing[id]; !ok {
			diff.negative.merge(growingStats[id])
		}
	}
	for id := range nextGrowing {
		if _, ok := r.currentGrowing[id]; !ok {
			diff.positive.merge(growingStats[id])
		}
	}
	// Validate all fields before changing any part of the aggregate.
	for field, stats := range r.currentStats {
		if err := stats.ValidateDelta(diff.positive.getOrCreate(field), diff.negative.getOrCreate(field)); err != nil {
			return err
		}
	}
	for field, stats := range r.currentStats {
		stats.ApplyDelta(diff.positive[field], diff.negative[field])
	}
	r.currentVersion, r.currentSealed, r.currentGrowing = diff.target, diff.nextSealed, nextGrowing
	r.growingStore.cleanup(r.currentVersion, r.currentGrowing)
	return nil
}

func (p *Provider) getSealedBM25Resources(ctx context.Context, collectionID int64, vchannel string, version qviews.DataVersion, partitions []int64, loadInfo uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
	response, err := p.fetchResources(ctx, collectionID, vchannel, version, partitions, loadInfo)
	if err != nil {
		return nil, err
	}
	return response.GetBm25Resources(), nil
}

func (r *oracleRuntime) includesPartition(id int64) bool {
	return r.partitionIDs == nil || slices.Contains(r.partitionIDs, id)
}

// BuildIDF deliberately ignores the query DataVersion: all views share the
// latest successfully published aggregate.
func (r *oracleRuntime) BuildIDFBatch(ctx context.Context, requests []queryresource.IDFRequest) ([]queryresource.IDFResult, error) {
	if err := r.ensureMaterialized(ctx); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, context.Canceled
	}
	results := make([]queryresource.IDFResult, len(requests))
	for i, req := range requests {
		stats, ok := r.currentStats[req.FieldID]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("BM25 field %d not found in oracle", req.FieldID)
		}
		result := &results[i]
		result.Avgdl = stats.GetAvgdl()
		// An empty latest corpus does not imply an older query view is empty.
		if result.Avgdl <= 0 {
			result.Avgdl = 1
		}
		for _, tf := range req.TFs.GetContents() {
			result.Vectors = append(result.Vectors, stats.BuildIDF(tf))
		}
	}
	return results, nil
}

// Preparing a query view requests refresh, but never builds a versioned oracle.
func (r *oracleRuntime) indexResources(resources []*datapb.StreamingNodeBM25Resource) (map[int64]*datapb.StreamingNodeBM25Resource, error) {
	indexed := make(map[int64]*datapb.StreamingNodeBM25Resource, len(resources))
	for _, resource := range resources {
		if resource == nil {
			return nil, merr.WrapErrDataIntegrityMsg("nil sealed BM25 resource")
		}
		if !r.includesPartition(resource.GetPartitionId()) {
			continue
		}
		id := resource.GetSegmentId()
		if _, ok := indexed[id]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate sealed BM25 resource for segment %d", id)
		}
		indexed[id] = proto.Clone(resource).(*datapb.StreamingNodeBM25Resource)
	}
	return indexed, nil
}

func (p *Provider) fetchResources(ctx context.Context, collectionID int64, vchannel string, version qviews.DataVersion, partitions []int64, loadInfo uint64) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
	response, err := p.client.GetStreamingNodeQueryViewResources(ctx, &datapb.GetStreamingNodeQueryViewResourcesRequest{
		CollectionId: collectionID, Vchannel: vchannel, DataVersion: version.IntoProto(), PartitionIds: partitions, LoadInfoVersion: loadInfo,
	})
	if err := merr.CheckRPCCall(response, err); err != nil {
		return nil, err
	}
	if err := validateResourceResponseFor(collectionID, vchannel, version, response); err != nil {
		return nil, err
	}
	return response, nil
}
