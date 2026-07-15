package idf

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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

type sealedContribution struct {
	segmentID   int64
	partitionID int64
	stats       bm25Stats
	lease       *segmentCacheLease
}

type growingContribution struct {
	segmentID   int64
	partitionID int64
	stats       bm25Stats
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

func (s *growingStatsStore) snapshotForDataVersion(target qviews.DataVersion, targetSealed map[int64]struct{}) map[int64]growingContribution {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[int64]growingContribution)
	for segmentID, segment := range s.segments {
		if _, ok := targetSealed[segmentID]; ok {
			continue
		}
		if segment.sealedAt != nil && !segment.sealedAt.GT(target) {
			continue
		}
		result[segmentID] = growingContribution{
			segmentID:   segmentID,
			partitionID: segment.partitionID,
			stats:       segment.stats.clone(),
		}
	}
	return result
}

func (s *growingStatsStore) cleanup(currentDataVersion qviews.DataVersion, currentGrowing map[int64]growingContribution) {
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
	target        qviews.DataVersion
	revision      uint64
	positive      bm25Stats
	negative      bm25Stats
	nextSealed    map[int64]sealedContribution
	nextGrowing   map[int64]growingContribution
	acquiredLease []*segmentCacheLease
}

type oracleRuntime struct {
	provider *Provider

	collectionID int64
	vchannel     string
	settings     *viewpb.QueryViewSettings
	schema       *schemapb.CollectionSchema

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	notify  chan struct{}
	closeCh chan struct{}

	mu             sync.RWMutex
	pending        qviews.DataVersion
	hasPending     bool
	currentVersion qviews.DataVersion
	currentStats   bm25Stats
	currentSealed  map[int64]sealedContribution
	currentGrowing map[int64]growingContribution
	growingStore   *growingStatsStore
	revision       uint64

	closeOnce sync.Once
}

func newOracleRuntime(
	ctx context.Context,
	provider *Provider,
	walView walview.VChannelWALView,
	settings *viewpb.QueryViewSettings,
	initialResources []*datapb.StreamingNodeBM25Resource,
) (*oracleRuntime, error) {
	runtimeCtx, cancel := context.WithCancel(context.Background())
	r := &oracleRuntime{
		provider:       provider,
		collectionID:   walView.CollectionID,
		vchannel:       walView.VChannel,
		settings:       settings,
		schema:         walView.Schema,
		ctx:            runtimeCtx,
		cancel:         cancel,
		notify:         make(chan struct{}, 1),
		closeCh:        make(chan struct{}),
		currentVersion: walView.SegmentSnapshot.DataVersion,
		currentStats:   newBM25StatsFromSchema(walView.Schema),
		currentSealed:  make(map[int64]sealedContribution),
		currentGrowing: make(map[int64]growingContribution),
		growingStore:   newGrowingStatsStore(walView.Schema),
	}
	sealed, err := provider.acquireSealedContributions(ctx, initialResources)
	if err != nil {
		cancel()
		return nil, err
	}
	for _, contribution := range sealed {
		r.currentSealed[contribution.segmentID] = contribution
		r.currentStats.merge(contribution.stats)
	}
	if err := r.loadInitialGrowing(ctx, walView); err != nil {
		r.releaseSealed(sealed)
		cancel()
		return nil, err
	}
	targetSealed := segmentSetFromSealed(r.currentSealed)
	r.currentGrowing = r.growingStore.snapshotForDataVersion(walView.SegmentSnapshot.DataVersion, targetSealed)
	for _, contribution := range r.currentGrowing {
		r.currentStats.merge(contribution.stats)
	}
	r.wg.Add(1)
	go r.advanceLoop()
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

func (r *oracleRuntime) BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats, ok := r.currentStats[fieldID]
	if !ok {
		return nil, 0, errors.Errorf("bm25 field %d not found in oracle", fieldID)
	}
	idfs := make([][]byte, 0, len(tfs.GetContents()))
	for _, tf := range tfs.GetContents() {
		idfs = append(idfs, stats.BuildIDF(tf))
	}
	return idfs, stats.GetAvgdl(), nil
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
		changed := false
		if _, ok := r.currentSealed[segmentID]; !ok {
			if _, ok := r.currentGrowing[segmentID]; !ok {
				r.currentGrowing[segmentID] = growingContribution{
					segmentID:   segmentID,
					partitionID: partitionID,
					stats:       newBM25StatsFromSchema(r.schema),
				}
				changed = true
			}
		}
		if changed {
			r.revision++
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
			if contribution, ok := r.currentGrowing[segmentID]; ok {
				contribution.stats.merge(stats)
				r.currentGrowing[segmentID] = contribution
				r.currentStats.merge(stats)
			}
			r.revision++
			return nil
		})
	case message.MessageTypeFlush:
		r.mu.Lock()
		r.growingStore.markFlushed(message.MustAsImmutableFlushMessageV2(msg).Header().GetSegmentId())
		r.revision++
		r.mu.Unlock()
	}
	return nil
}

func (r *oracleRuntime) applySegmentSealed(segmentID int64, sealedAt qviews.DataVersion) {
	r.mu.Lock()
	r.growingStore.markSealed(segmentID, sealedAt)
	currentVersion := r.currentVersion
	currentGrowing := cloneGrowingContributions(r.currentGrowing)
	r.revision++
	r.mu.Unlock()
	r.growingStore.cleanup(currentVersion, currentGrowing)
}

func (r *oracleRuntime) MaybeAdvance(target qviews.DataVersion) {
	r.mu.Lock()
	if !target.GT(r.currentVersion) {
		r.mu.Unlock()
		return
	}
	if !r.hasPending || target.GT(r.pending) {
		r.pending = target
		r.hasPending = true
	}
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

func (r *oracleRuntime) Advance(target qviews.DataVersion) {
	r.MaybeAdvance(target)
}

func (r *oracleRuntime) Close() {
	r.closeOnce.Do(func() {
		r.cancel()
		close(r.closeCh)
		select {
		case r.notify <- struct{}{}:
		default:
		}
		r.wg.Wait()
		r.mu.Lock()
		sealed := r.currentSealed
		r.currentSealed = nil
		r.currentGrowing = nil
		r.mu.Unlock()
		r.releaseSealed(sealed)
	})
}

func (r *oracleRuntime) advanceLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.notify:
		case <-r.closeCh:
			return
		}
		for {
			target, ok := r.popPending()
			if !ok {
				break
			}
			diff, err := r.computeDiff(r.ctx, target)
			if err != nil {
				continue
			}
			if committed, retry := r.commitDiff(diff); !committed && retry {
				r.MaybeAdvance(target)
			}
		}
	}
}

func (r *oracleRuntime) popPending() (qviews.DataVersion, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.hasPending {
		return qviews.DataVersion{}, false
	}
	target := r.pending
	r.pending = qviews.DataVersion{}
	r.hasPending = false
	return target, true
}

func (r *oracleRuntime) computeDiff(ctx context.Context, target qviews.DataVersion) (*idfDiff, error) {
	resources, err := r.provider.getSealedBM25Resources(ctx, r.collectionID, r.vchannel, target, r.settings)
	if err != nil {
		return nil, err
	}
	nextSealed, err := r.provider.acquireSealedContributions(ctx, resources)
	if err != nil {
		return nil, err
	}
	targetSealed := segmentSetFromSealed(nextSealed)

	r.mu.RLock()
	nextGrowing := r.growingStore.snapshotForDataVersion(target, targetSealed)
	currentSealed := cloneSealedContributions(r.currentSealed)
	currentGrowing := cloneGrowingContributions(r.currentGrowing)
	revision := r.revision
	r.mu.RUnlock()

	diff := &idfDiff{
		target:      target,
		revision:    revision,
		positive:    make(bm25Stats),
		negative:    make(bm25Stats),
		nextSealed:  nextSealed,
		nextGrowing: nextGrowing,
	}
	for _, contribution := range nextSealed {
		diff.acquiredLease = append(diff.acquiredLease, contribution.lease)
	}
	for segmentID, contribution := range currentSealed {
		if _, ok := nextSealed[segmentID]; !ok {
			diff.negative.merge(contribution.stats)
		}
	}
	for segmentID, contribution := range currentGrowing {
		if _, ok := nextGrowing[segmentID]; !ok {
			diff.negative.merge(contribution.stats)
		}
	}
	for segmentID, contribution := range nextSealed {
		if _, ok := currentSealed[segmentID]; !ok {
			diff.positive.merge(contribution.stats)
		}
	}
	for segmentID, contribution := range nextGrowing {
		if _, ok := currentGrowing[segmentID]; !ok {
			diff.positive.merge(contribution.stats)
		}
	}
	return diff, nil
}

func (r *oracleRuntime) commitDiff(diff *idfDiff) (bool, bool) {
	var oldSealed map[int64]sealedContribution
	releaseNew := false
	retry := false
	r.mu.Lock()
	if diff.revision != r.revision {
		releaseNew = true
		retry = diff.target.GT(r.currentVersion)
	} else if !diff.target.GT(r.currentVersion) {
		releaseNew = true
	} else {
		r.currentStats.minus(diff.negative)
		r.currentStats.merge(diff.positive)
		r.currentVersion = diff.target
		oldSealed = r.currentSealed
		r.currentSealed = diff.nextSealed
		r.currentGrowing = diff.nextGrowing
		r.revision++
	}
	currentVersion := r.currentVersion
	currentGrowing := cloneGrowingContributions(r.currentGrowing)
	r.mu.Unlock()

	if releaseNew {
		for _, lease := range diff.acquiredLease {
			lease.Close()
		}
		return false, retry
	}
	for segmentID, contribution := range oldSealed {
		if _, ok := diff.nextSealed[segmentID]; !ok && contribution.lease != nil {
			contribution.lease.Close()
		}
	}
	r.growingStore.cleanup(currentVersion, currentGrowing)
	return true, false
}

func (r *oracleRuntime) releaseSealed(sealed map[int64]sealedContribution) {
	for _, contribution := range sealed {
		if contribution.lease != nil {
			contribution.lease.Close()
		}
	}
}

func (p *Provider) getSealedBM25Resources(
	ctx context.Context,
	collectionID int64,
	vchannel string,
	dataVersion qviews.DataVersion,
	settings *viewpb.QueryViewSettings,
) ([]*datapb.StreamingNodeBM25Resource, error) {
	resp, err := p.client.GetStreamingNodeQueryViewResources(ctx, &datapb.GetStreamingNodeQueryViewResourcesRequest{
		CollectionId: collectionID,
		Vchannel:     vchannel,
		DataVersion:  dataVersion.IntoProto(),
		Settings:     settings,
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
) (map[int64]sealedContribution, error) {
	contributions := make(map[int64]sealedContribution, len(resources))
	for _, resource := range resources {
		stats, lease, err := p.sealedCache.acquire(ctx, p.chunkManager, resource)
		if err != nil {
			for _, contribution := range contributions {
				if contribution.lease != nil {
					contribution.lease.Close()
				}
			}
			return nil, err
		}
		contributions[resource.GetSegmentId()] = sealedContribution{
			segmentID:   resource.GetSegmentId(),
			partitionID: resource.GetPartitionId(),
			stats:       stats,
			lease:       lease,
		}
	}
	return contributions, nil
}

func segmentSetFromSealed(sealed map[int64]sealedContribution) map[int64]struct{} {
	result := make(map[int64]struct{}, len(sealed))
	for segmentID := range sealed {
		result[segmentID] = struct{}{}
	}
	return result
}

func cloneSealedContributions(src map[int64]sealedContribution) map[int64]sealedContribution {
	dst := make(map[int64]sealedContribution, len(src))
	for segmentID, contribution := range src {
		contribution.stats = contribution.stats.clone()
		dst[segmentID] = contribution
	}
	return dst
}

func cloneGrowingContributions(src map[int64]growingContribution) map[int64]growingContribution {
	dst := make(map[int64]growingContribution, len(src))
	for segmentID, contribution := range src {
		contribution.stats = contribution.stats.clone()
		dst[segmentID] = contribution
	}
	return dst
}
