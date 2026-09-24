package idf

import (
	"context"
	"maps"
	"sync"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type bm25Stats map[int64]*storage.BM25Stats

func newBM25StatsFromSchema(schema *schemapb.CollectionSchema) bm25Stats {
	stats := make(bm25Stats)
	if schema == nil {
		return stats
	}
	for _, function := range schema.GetFunctions() {
		if function.GetType() != schemapb.FunctionType_BM25 || len(function.GetOutputFieldIds()) == 0 {
			continue
		}
		stats.getOrCreate(function.GetOutputFieldIds()[0])
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

func (s bm25Stats) clone() bm25Stats {
	cloned := make(bm25Stats, len(s))
	for fieldID, stats := range s {
		if stats != nil {
			cloned[fieldID] = stats.Clone()
		}
	}
	return cloned
}

func (s bm25Stats) merge(src bm25Stats) {
	for fieldID, srcStats := range src {
		if srcStats == nil {
			continue
		}
		s.getOrCreate(fieldID).Merge(srcStats)
	}
}

func (s bm25Stats) minus(src bm25Stats) {
	for fieldID, srcStats := range src {
		if srcStats == nil {
			continue
		}
		s.getOrCreate(fieldID).Minus(srcStats)
	}
}

type growingSegmentStats struct {
	partitionID int64
	stats       bm25Stats
	flushed     bool
	sealedAt    *qviews.DataVersion
}

type growingStatsStore struct {
	mu       sync.RWMutex
	schema   *schemapb.CollectionSchema
	segments map[int64]*growingSegmentStats
}

func newGrowingStatsStore(schema *schemapb.CollectionSchema) *growingStatsStore {
	return &growingStatsStore{
		schema:   schema,
		segments: make(map[int64]*growingSegmentStats),
	}
}

func (s *growingStatsStore) registerSegment(segmentID int64, partitionID int64) {
	if segmentID == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.segments[segmentID]; ok {
		return
	}
	s.segments[segmentID] = &growingSegmentStats{
		partitionID: partitionID,
		stats:       newBM25StatsFromSchema(s.schema),
	}
}

func (s *growingStatsStore) appendStats(segmentID int64, partitionID int64, stats bm25Stats) {
	if segmentID == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	segment := s.segments[segmentID]
	if segment == nil {
		segment = &growingSegmentStats{
			partitionID: partitionID,
			stats:       newBM25StatsFromSchema(s.schema),
		}
		s.segments[segmentID] = segment
	}
	if segment.partitionID == 0 {
		segment.partitionID = partitionID
	}
	segment.stats.merge(stats)
}

func (s *growingStatsStore) appendInsert(insert walview.SegmentInsertMessage) (int64, bm25Stats, error) {
	segmentID := insert.Assignment.GetSegmentAssignment().GetSegmentId()
	partitionID := insert.Assignment.GetPartitionId()
	stats := newBM25StatsFromSchema(s.schema)
	if err := collectGrowingInsertStats(stats, s.schema, insert); err != nil {
		return 0, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	segment := s.segments[segmentID]
	if segment == nil {
		segment = &growingSegmentStats{
			partitionID: partitionID,
			stats:       newBM25StatsFromSchema(s.schema),
		}
		s.segments[segmentID] = segment
	}
	if segment.flushed {
		return 0, nil, errors.Errorf("BM25 growing segment %d already flushed", segmentID)
	}
	if segment.sealedAt != nil {
		return 0, nil, errors.Errorf("BM25 growing segment %d already sealed", segmentID)
	}
	if segment.partitionID == 0 {
		segment.partitionID = partitionID
	}
	segment.stats.merge(stats)
	return segmentID, stats, nil
}

func (s *growingStatsStore) markFlushed(segmentID int64) {
	if segmentID == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	segment := s.segments[segmentID]
	if segment == nil {
		segment = &growingSegmentStats{stats: newBM25StatsFromSchema(s.schema)}
		s.segments[segmentID] = segment
	}
	segment.flushed = true
}

func (s *growingStatsStore) markSealed(segmentID int64, sealedAt qviews.DataVersion) {
	if segmentID == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	segment := s.segments[segmentID]
	if segment == nil {
		segment = &growingSegmentStats{stats: newBM25StatsFromSchema(s.schema)}
		s.segments[segmentID] = segment
	}
	if segment.sealedAt != nil && !segment.sealedAt.EQ(sealedAt) {
		panic("conflicting sealed data version for BM25 growing segment")
	}
	value := sealedAt
	segment.sealedAt = &value
}

func (s *growingStatsStore) snapshotForDataVersion(
	target qviews.DataVersion,
	targetSealed map[int64]*sealedBm25Stats,
	current map[int64]struct{},
) (map[int64]struct{}, map[int64]bm25Stats) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
			stats[segmentID] = segment.stats.clone()
		}
	}
	return next, stats
}

func (s *growingStatsStore) cleanup(currentDataVersion qviews.DataVersion, currentGrowing map[int64]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	nextSealed map[int64]*sealedBm25Stats
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

	advanceMu       sync.Mutex
	mu              sync.RWMutex
	lazy            bool
	closed          bool
	currentVersion  qviews.DataVersion
	currentStats    bm25Stats
	currentSealed   map[int64]*sealedBm25Stats
	currentGrowing  map[int64]struct{}
	prepared        map[qviews.DataVersion]map[int64]*sealedBm25Stats
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
	r := &oracleRuntime{
		provider:        provider,
		lazy:            lazy,
		collectionID:    walView.CollectionID,
		vchannel:        walView.VChannel,
		partitionIDs:    append([]int64(nil), walView.PartitionIDs...),
		loadInfoVersion: walView.LoadInfoVersion,
		schema:          walView.Schema,
		currentVersion:  walView.SegmentSnapshot.DataVersion,
		prepared:        make(map[qviews.DataVersion]map[int64]*sealedBm25Stats),
		growingStore:    newGrowingStatsStore(walView.Schema),
	}
	if lazy {
		if err := r.loadInitialGrowing(ctx, walView); err != nil {
			return nil, err
		}
		return r, nil
	}

	r.currentStats = newBM25StatsFromSchema(walView.Schema)
	sealed, err := provider.acquireSealedContributions(ctx, initialResources, r.currentStats)
	if err != nil {
		return nil, err
	}
	r.currentSealed = sealed
	if err := r.loadInitialGrowing(ctx, walView); err != nil {
		r.releaseSealed(sealed)
		return nil, err
	}
	var growingStats map[int64]bm25Stats
	r.currentGrowing, growingStats = r.growingStore.snapshotForDataVersion(
		walView.SegmentSnapshot.DataVersion,
		r.currentSealed,
		nil,
	)
	for _, stats := range growingStats {
		r.currentStats.merge(stats)
	}
	if err := ctx.Err(); err != nil {
		r.releaseSealed(sealed)
		return nil, err
	}
	r.growingStore.cleanup(r.currentVersion, r.currentGrowing)
	return r, nil
}

func (r *oracleRuntime) loadInitialGrowing(ctx context.Context, walView walview.VChannelWALView) error {
	for _, segment := range walView.SegmentSnapshot.Segments {
		r.growingStore.registerSegment(segment.SegmentID, segment.PartitionID)
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
	stats := newBM25StatsFromSchema(r.schema)
	for _, binlogs := range segment.Data.PersistedStorage.GetBinlogs() {
		for _, fieldBinlog := range binlogs.GetBm25Binlog() {
			fieldStats := stats.getOrCreate(fieldBinlog.GetFieldID())
			for _, binlog := range fieldBinlog.GetBinlogs() {
				bytes, err := r.provider.chunkManager.Read(ctx, binlog.GetLogPath())
				if err != nil {
					return err
				}
				loaded, err := storage.NewBM25StatsWithBytes(bytes)
				if err != nil {
					return err
				}
				fieldStats.Merge(loaded)
			}
		}
	}
	r.growingStore.appendStats(segment.SegmentID, segment.PartitionID, stats)
	return nil
}

func (r *oracleRuntime) BuildIDF(ctx context.Context, _ qviews.DataVersion, fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	if err := r.ensureMaterialized(ctx); err != nil {
		return nil, 0, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	stats, ok := r.currentStats[fieldID]
	if !ok {
		return nil, 0, merr.WrapErrServiceInternalMsg("BM25 field %d not found in oracle", fieldID)
	}
	idfs := make([][]byte, 0, len(tfs.GetContents()))
	for _, tf := range tfs.GetContents() {
		idfs = append(idfs, stats.BuildIDF(tf))
	}
	return idfs, stats.GetAvgdl(), nil
}

func (r *oracleRuntime) PrepareDataVersion(ctx context.Context, target qviews.DataVersion) error {
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
		r.mu.Unlock()
		if call != nil && !call.target.EQ(target) {
			call.cancel()
		}
		r.growingStore.cleanup(target, nil)
		return nil
	}
	if _, ok := r.prepared[target]; ok {
		r.mu.Unlock()
		return r.advancePrepared(ctx, target)
	}
	r.mu.Unlock()

	resources, err := r.provider.getSealedBM25Resources(
		ctx,
		r.collectionID,
		r.vchannel,
		target,
		r.partitionIDs,
		r.loadInfoVersion,
	)
	if err != nil {
		return err
	}
	sealed, err := r.provider.acquireSealedContributions(ctx, resources, nil)
	if err != nil {
		return err
	}

	r.mu.Lock()
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		r.releaseSealed(sealed)
		return err
	}
	closed := r.closed
	if closed || !target.GT(r.currentVersion) {
		r.mu.Unlock()
		r.releaseSealed(sealed)
		if closed {
			return context.Canceled
		}
		return nil
	}
	if _, ok := r.prepared[target]; ok {
		r.mu.Unlock()
		r.releaseSealed(sealed)
		return r.advancePrepared(ctx, target)
	}
	if r.prepared == nil {
		r.prepared = make(map[qviews.DataVersion]map[int64]*sealedBm25Stats)
	}
	r.prepared[target] = sealed
	r.mu.Unlock()
	return r.advancePrepared(ctx, target)
}

func (r *oracleRuntime) advancePrepared(ctx context.Context, target qviews.DataVersion) error {
	r.advanceMu.Lock()
	defer r.advanceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.RLock()
	closed := r.closed
	current := r.currentVersion
	r.mu.RUnlock()
	if closed {
		return context.Canceled
	}
	if !target.GT(current) {
		return nil
	}
	diff, err := r.computeDiff(ctx, target)
	if err != nil {
		return err
	}
	r.commitDiff(ctx, diff)
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.RLock()
	closed = r.closed
	advanced := !target.GT(r.currentVersion)
	r.mu.RUnlock()
	if closed {
		return context.Canceled
	}
	if !advanced {
		return merr.WrapErrServiceUnavailableMsg("BM25 stats did not advance to data version %s", target.String())
	}
	return nil
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
			materializationCtx, cancel := context.WithCancel(context.Background())
			call = &materializationCall{
				target: r.currentVersion,
				ctx:    materializationCtx,
				cancel: cancel,
				done:   make(chan struct{}),
			}
			r.materialization = call
			go r.materialize(call)
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

func (r *oracleRuntime) materialize(call *materializationCall) {
	var (
		sealed    map[int64]*sealedBm25Stats
		resultErr error
		committed bool
	)
	defer func() {
		if !committed {
			r.releaseSealed(sealed)
		}

		r.mu.Lock()
		call.err = resultErr
		if r.materialization == call {
			r.materialization = nil
		}
		close(call.done)
		r.mu.Unlock()
	}()
	defer call.cancel()

	resources, err := r.provider.getSealedBM25Resources(
		call.ctx,
		r.collectionID,
		r.vchannel,
		call.target,
		r.partitionIDs,
		r.loadInfoVersion,
	)
	if err != nil {
		resultErr = merr.Wrapf(err, "get sealed BM25 resources for data version %s", call.target.String())
		return
	}
	stats := newBM25StatsFromSchema(r.schema)
	sealed, err = r.provider.acquireSealedContributions(call.ctx, resources, stats)
	if err != nil {
		resultErr = merr.Wrapf(err, "load sealed BM25 stats for data version %s", call.target.String())
		return
	}
	var currentGrowing map[int64]struct{}
	r.mu.Lock()
	if r.closed || r.materialization != call || r.currentStats != nil || !r.currentVersion.EQ(call.target) {
		resultErr = call.ctx.Err()
		if resultErr == nil {
			resultErr = context.Canceled
		}
		r.mu.Unlock()
		return
	}
	growing, growingStats := r.growingStore.snapshotForDataVersion(call.target, sealed, nil)
	for _, segmentStats := range growingStats {
		stats.merge(segmentStats)
	}
	if resultErr = call.ctx.Err(); resultErr != nil {
		r.mu.Unlock()
		return
	}
	r.currentStats = stats
	r.currentSealed = sealed
	r.currentGrowing = growing
	// Cleanup only needs a stable segment membership snapshot.
	currentGrowing = maps.Clone(growing)
	committed = true
	r.mu.Unlock()

	r.growingStore.cleanup(call.target, currentGrowing)
}

func (r *oracleRuntime) ReleaseDataVersion(dataVersion qviews.DataVersion) {
	r.mu.Lock()
	prepared := r.prepared[dataVersion]
	delete(r.prepared, dataVersion)
	r.mu.Unlock()
	if prepared != nil {
		r.releaseSealed(prepared)
	}
}

func (r *oracleRuntime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	if event.Message != nil {
		if err := r.applyLiveMessage(ctx, event.Message); err != nil {
			panic(errors.Wrap(err, "failed to apply live event to IDF oracle runtime"))
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
		r.mu.Lock()
		r.growingStore.registerSegment(segmentID, partitionID)
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
	case message.MessageTypeFlush:
		r.mu.Lock()
		r.growingStore.markFlushed(message.MustAsImmutableFlushMessageV2(msg).Header().GetSegmentId())
		r.mu.Unlock()
	}
	return nil
}

func (r *oracleRuntime) applySegmentSealed(segmentID int64, sealedAt qviews.DataVersion) {
	r.mu.Lock()
	r.growingStore.markSealed(segmentID, sealedAt)
	currentVersion := r.currentVersion
	currentGrowing := maps.Clone(r.currentGrowing)
	r.mu.Unlock()
	r.growingStore.cleanup(currentVersion, currentGrowing)
}

func (r *oracleRuntime) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		call := r.materialization
		r.mu.Unlock()
		if call != nil {
			call.cancel()
		}
		if call != nil {
			<-call.done
		}
		r.mu.Lock()
		sealed := r.currentSealed
		prepared := r.prepared
		r.currentSealed = nil
		r.currentGrowing = nil
		r.prepared = nil
		r.materialization = nil
		r.mu.Unlock()
		r.releaseSealed(sealed)
		for _, version := range prepared {
			r.releaseSealed(version)
		}
	})
}

func (r *oracleRuntime) computeDiff(ctx context.Context, target qviews.DataVersion) (*idfDiff, error) {
	r.mu.Lock()
	prepared, ok := r.prepared[target]
	if ok {
		delete(r.prepared, target)
	}
	r.mu.Unlock()

	var nextSealed map[int64]*sealedBm25Stats
	if ok {
		nextSealed = prepared
	} else {
		resources, err := r.provider.getSealedBM25Resources(ctx, r.collectionID, r.vchannel, target, r.partitionIDs, r.loadInfoVersion)
		if err != nil {
			return nil, err
		}
		nextSealed, err = r.provider.acquireSealedContributions(ctx, resources, nil)
		if err != nil {
			return nil, err
		}
	}
	keepNext := false
	defer func() {
		if !keepNext {
			r.releaseSealed(nextSealed)
		}
	}()
	r.mu.RLock()
	currentSealed := r.currentSealed
	r.mu.RUnlock()

	diff := &idfDiff{
		target:     target,
		positive:   make(bm25Stats),
		negative:   make(bm25Stats),
		nextSealed: nextSealed,
	}
	for segmentID, sealedStats := range currentSealed {
		if next := nextSealed[segmentID]; next != nil && sealedStats != nil && next.key == sealedStats.key {
			continue
		}
		stats, err := sealedStats.FetchStats()
		if err != nil {
			return nil, err
		}
		diff.negative.merge(stats)
	}
	for segmentID, sealedStats := range nextSealed {
		if current := currentSealed[segmentID]; current != nil && sealedStats != nil && current.key == sealedStats.key {
			continue
		}
		stats, err := sealedStats.FetchStats()
		if err != nil {
			return nil, err
		}
		diff.positive.merge(stats)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keepNext = true
	return diff, nil
}

func (r *oracleRuntime) commitDiff(ctx context.Context, diff *idfDiff) {
	var (
		oldSealed map[int64]*sealedBm25Stats
		obsolete  []map[int64]*sealedBm25Stats
	)
	releaseNew := false
	r.mu.Lock()
	if r.closed || ctx.Err() != nil {
		releaseNew = true
	} else if !diff.target.GT(r.currentVersion) {
		releaseNew = true
	} else {
		nextGrowing, growingStats := r.growingStore.snapshotForDataVersion(diff.target, diff.nextSealed, r.currentGrowing)
		r.currentStats.minus(diff.negative)
		for segmentID := range r.currentGrowing {
			if _, ok := nextGrowing[segmentID]; !ok {
				r.currentStats.minus(growingStats[segmentID])
			}
		}
		r.currentStats.merge(diff.positive)
		for segmentID := range nextGrowing {
			if _, ok := r.currentGrowing[segmentID]; !ok {
				r.currentStats.merge(growingStats[segmentID])
			}
		}
		r.currentVersion = diff.target
		oldSealed = r.currentSealed
		r.currentSealed = diff.nextSealed
		r.currentGrowing = nextGrowing
		obsolete = r.takePreparedThroughLocked(diff.target)
	}
	currentVersion := r.currentVersion
	currentGrowing := maps.Clone(r.currentGrowing)
	r.mu.Unlock()

	if releaseNew {
		r.releaseSealed(diff.nextSealed)
		return
	}
	r.releaseSealed(oldSealed)
	for _, sealed := range obsolete {
		r.releaseSealed(sealed)
	}
	r.growingStore.cleanup(currentVersion, currentGrowing)
}

func (r *oracleRuntime) takePreparedThroughLocked(target qviews.DataVersion) []map[int64]*sealedBm25Stats {
	obsolete := make([]map[int64]*sealedBm25Stats, 0)
	for version, prepared := range r.prepared {
		if version.GT(target) {
			continue
		}
		obsolete = append(obsolete, prepared)
		delete(r.prepared, version)
	}
	return obsolete
}

func (r *oracleRuntime) releaseSealed(sealed map[int64]*sealedBm25Stats) {
	for _, sealedStats := range sealed {
		r.provider.sealedCache.release(sealedStats)
	}
}

func (p *Provider) getSealedBM25Resources(
	ctx context.Context,
	collectionID int64,
	vchannel string,
	dataVersion qviews.DataVersion,
	partitionIDs []int64,
	loadInfoVersion uint64,
) ([]*datapb.StreamingNodeBM25Resource, error) {
	resp, err := p.client.GetStreamingNodeQueryViewResources(ctx, &datapb.GetStreamingNodeQueryViewResourcesRequest{
		CollectionId:    collectionID,
		Vchannel:        vchannel,
		DataVersion:     dataVersion.IntoProto(),
		LoadInfoVersion: loadInfoVersion,
		PartitionIds:    append([]int64(nil), partitionIDs...),
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}
	if err := validateResourceResponseFor(collectionID, vchannel, dataVersion, resp); err != nil {
		return nil, err
	}
	return resp.GetBm25Resources(), nil
}

func (p *Provider) acquireSealedContributions(
	ctx context.Context,
	resources []*datapb.StreamingNodeBM25Resource,
	mergeInto bm25Stats,
) (map[int64]*sealedBm25Stats, error) {
	seen := make(map[int64]struct{}, len(resources))
	for _, resource := range resources {
		if resource == nil {
			return nil, merr.WrapErrDataIntegrityMsg("nil sealed BM25 resource")
		}
		segmentID := resource.GetSegmentId()
		if _, ok := seen[segmentID]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate sealed BM25 resource for segment %d", segmentID)
		}
		seen[segmentID] = struct{}{}
	}

	contributions := make(map[int64]*sealedBm25Stats, len(resources))
	keepContributions := false
	defer func() {
		if keepContributions {
			return
		}
		for _, sealedStats := range contributions {
			p.sealedCache.release(sealedStats)
		}
	}()

	limiter := p.sealedStatsLoadLimiter
	if limiter == nil {
		limiter = getGlobalSealedStatsLoadLimiter()
	}
	type loadResult struct {
		segmentID   int64
		sealedStats *sealedBm25Stats
		stats       bm25Stats
	}
	results := make(chan loadResult, len(resources))
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for result := range results {
			contributions[result.segmentID] = result.sealedStats
			if mergeInto != nil {
				mergeInto.merge(result.stats)
			}
		}
	}()

	group, groupCtx := errgroup.WithContext(ctx)
	var acquireErr error
	for _, resource := range resources {
		err := limiter.Acquire(groupCtx)
		if ctxErr := groupCtx.Err(); err == nil && ctxErr != nil {
			limiter.Release()
			err = ctxErr
		}
		if err != nil {
			acquireErr = groupCtx.Err()
			break
		}
		group.Go(func() error {
			defer limiter.Release()
			stats, sealedStats, err := p.sealedCache.acquire(groupCtx, p.chunkManager, resource, mergeInto != nil)
			if err != nil {
				return err
			}
			results <- loadResult{segmentID: resource.GetSegmentId(), sealedStats: sealedStats, stats: stats}
			return nil
		})
	}
	groupErr := group.Wait()
	close(results)
	<-collectorDone
	if groupErr != nil {
		return nil, groupErr
	}
	if acquireErr != nil {
		return nil, acquireErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keepContributions = true
	return contributions, nil
}
