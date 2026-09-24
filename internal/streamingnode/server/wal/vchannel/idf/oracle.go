package idf

import (
	"context"
	"slices"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

type growingStatsStore struct {
	mu       sync.RWMutex
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.segments[segmentID]; ok {
		return
	}
	s.segments[segmentID] = &growingSegmentStats{
		createTimeTick: createTimeTick,
		partitionID:    partitionID,
		stats:          newBM25StatsFromSchema(s.schema, s.fieldIDs),
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
			stats:       newBM25StatsFromSchema(s.schema, s.fieldIDs),
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
	stats := newBM25StatsFromSchema(s.schema, s.fieldIDs)
	if err := collectGrowingInsertStats(stats, s.schema, insert); err != nil {
		return 0, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	segment := s.segments[segmentID]
	if segment == nil {
		segment = &growingSegmentStats{
			partitionID: partitionID,
			stats:       newBM25StatsFromSchema(s.schema, s.fieldIDs),
		}
		s.segments[segmentID] = segment
	}
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	segment := s.segments[segmentID]
	if segment == nil {
		segment = &growingSegmentStats{stats: newBM25StatsFromSchema(s.schema, s.fieldIDs)}
		s.segments[segmentID] = segment
	}
	if segment.sealedAt != nil && !segment.sealedAt.EQ(sealedAt) {
		panic("conflicting sealed data version for BM25 growing segment")
	}
	value := sealedAt
	segment.sealedAt = &value
}

// Sealed contributions retain only immutable resource descriptors. Growing
// contributions are owned exclusively by growingStore and the aggregate.
type oracleRuntime struct {
	provider         *Provider
	scheduler        nodescheduler.Scheduler
	collectionID     int64
	vchannel         string
	partitionIDs     []int64
	fieldIDs         []int64
	loadInfoVersion  uint64
	schema           *schemapb.CollectionSchema
	barrier          func(context.Context) error
	ctx              context.Context
	cancel           context.CancelFunc
	ioMu             sync.Mutex
	mu               sync.RWMutex
	closed           bool
	currentVersion   qviews.DataVersion
	currentStats     bm25Stats
	currentSealed    map[int64]*datapb.StreamingNodeBM25Resource
	growingStore     *growingStatsStore
	pending          qviews.DataVersion
	advanceScheduled bool
	advanceHandle    nodescheduler.TaskHandle
}

func newOracleRuntime(ctx context.Context, provider *Provider, view walview.VChannelWALView, resources []*datapb.StreamingNodeBM25Resource) (*oracleRuntime, error) {
	scheduler := provider.scheduler
	if scheduler == nil {
		scheduler = nodescheduler.Get()
	}
	schema := proto.Clone(view.Schema).(*schemapb.CollectionSchema)
	loadedFields := loadFieldIDs(view.LoadFields)
	lifetime, cancel := context.WithCancel(context.Background())
	r := &oracleRuntime{
		provider:        provider,
		scheduler:       scheduler,
		collectionID:    view.CollectionID,
		vchannel:        view.VChannel,
		partitionIDs:    slices.Clone(view.PartitionIDs),
		fieldIDs:        loadedFields,
		loadInfoVersion: view.LoadInfoVersion,
		schema:          schema,
		barrier:         view.ResourceEventBarrier,
		ctx:             lifetime,
		cancel:          cancel,
		currentVersion:  view.SegmentSnapshot.DataVersion,
		currentStats:    newBM25StatsFromSchema(schema, loadedFields),
		currentSealed:   make(map[int64]*datapb.StreamingNodeBM25Resource),
		growingStore:    newGrowingStatsStore(schema, loadedFields),
	}

	for _, resource := range resources {
		if !r.includesPartition(resource.GetPartitionId()) {
			continue
		}
		stats, err := loadSealedSegmentStats(ctx, provider.chunkManager, resource, r.currentStats)
		if err != nil {
			cancel()
			return nil, err
		}
		r.currentStats.merge(stats)
		r.currentSealed[resource.GetSegmentId()] = proto.Clone(resource).(*datapb.StreamingNodeBM25Resource)
	}
	if err := r.loadInitialGrowing(ctx, view); err != nil {
		cancel()
		return nil, err
	}
	for id, segment := range r.growingStore.segments {
		if _, sealed := r.currentSealed[id]; sealed || segment.sealedAt != nil && !segment.sealedAt.GT(r.currentVersion) {
			delete(r.growingStore.segments, id)
		} else {
			r.currentStats.merge(segment.stats)
		}
	}
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
	loaded, err := loadSealedSegmentStats(ctx, r.provider.chunkManager, resource, r.currentStats)
	if err != nil {
		return err
	}
	stats.merge(loaded)
	r.growingStore.appendStats(segment.SegmentID, segment.PartitionID, stats)
	return nil
}

func (r *oracleRuntime) includesPartition(id int64) bool {
	return r.partitionIDs == nil || slices.Contains(r.partitionIDs, id)
}

// BuildIDF deliberately ignores the query DataVersion: all views share the
// latest successfully published aggregate.
func (r *oracleRuntime) BuildIDF(_ qviews.DataVersion, fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	results, err := r.BuildIDFBatch([]queryresource.IDFRequest{{FieldID: fieldID, TFs: tfs}})
	if err != nil {
		return nil, 0, err
	}
	return results[0].Vectors, results[0].Avgdl, nil
}

func (r *oracleRuntime) BuildIDFBatch(requests []queryresource.IDFRequest) ([]queryresource.IDFResult, error) {
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
func (r *oracleRuntime) RequestRefresh(_ context.Context, target qviews.DataVersion) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return context.Canceled
	}
	r.scheduleLocked(target)
	return nil
}

func (r *oracleRuntime) Advance(target qviews.DataVersion) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.scheduleLocked(target)
	}
}

func (r *oracleRuntime) scheduleLocked(target qviews.DataVersion) {
	if target.GT(r.pending) {
		r.pending = target
	}
	if r.advanceScheduled || !r.pending.GT(r.currentVersion) {
		return
	}
	r.advanceScheduled = true
	r.advanceHandle = r.scheduler.Submit(oracleAdvanceTask{runtime: r})
}

func (r *oracleRuntime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if event.SegmentSealed != nil {
		id := event.SegmentSealed.SegmentID
		if _, exists := r.growingStore.segments[id]; exists {
			r.growingStore.markSealed(id, event.SegmentSealed.SealedAtDataVersion)
		}
	}
	msg := event.Message
	if msg == nil {
		return
	}
	switch msg.MessageType() {
	case message.MessageTypeCreateSegment:
		created := message.MustAsImmutableCreateSegmentMessageV2(msg)
		if !r.includesPartition(created.Header().GetPartitionId()) {
			return
		}
		r.growingStore.registerSegment(created.Header().GetSegmentId(), created.Header().GetPartitionId(), msg.TimeTick())
	case message.MessageTypeInsert, message.MessageTypeTxn:
		err := walview.ForEachSegmentInsertMessage(msg, 0, func(insert walview.SegmentInsertMessage) error {
			if !r.includesPartition(insert.Assignment.GetPartitionId()) {
				return nil
			}
			_, stats, err := r.growingStore.appendInsert(insert)
			if err == nil {
				r.currentStats.merge(stats)
			}
			return err
		})
		if err != nil {
			panic(merr.Wrap(err, "apply live BM25 insert"))
		}
	case message.MessageTypeManualFlush, message.MessageTypeFlushAll, message.MessageTypeAlterWAL, message.MessageTypeCreateSnapshot:
		// Match VChannelRecoveryModule.flushAllSegmentsCreatedBefore, including
		// flushes whose compaction output no longer contains the original IDs.
		for id, segment := range r.growingStore.segments {
			if segment.createTimeTick < msg.TimeTick() {
				r.growingStore.markFlushed(id)
			}
		}
	case message.MessageTypeFlush:
		flushed := message.MustAsImmutableFlushMessageV2(msg).Header()
		if _, exists := r.growingStore.segments[flushed.GetSegmentId()]; exists {
			r.growingStore.markFlushed(flushed.GetSegmentId())
		}
	}
}

// BeforeRelease keeps the old DataView alive until its sealed descriptors have
// been replaced. The last view closes the entire oracle instead.
func (r *oracleRuntime) BeforeRelease(ctx context.Context, target qviews.DataVersion) error {
	r.mu.RLock()
	ready := r.closed || r.currentVersion.GTE(target)
	r.mu.RUnlock()
	if ready {
		return nil
	}
	return r.refresh(ctx, target)
}

func (r *oracleRuntime) refresh(ctx context.Context, target qviews.DataVersion) error {
	if !r.ioMu.TryLock() {
		return nodescheduler.ErrDelay
	}
	defer r.ioMu.Unlock()
	r.mu.RLock()
	if r.closed || r.currentVersion.GTE(target) {
		r.mu.RUnlock()
		return nil
	}
	r.mu.RUnlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()
	defer cancel()
	response, err := r.provider.fetchResources(ctx, r.collectionID, r.vchannel, target, r.partitionIDs, r.loadInfoVersion)
	if err != nil {
		return err
	}
	r.mu.RLock()
	previous := r.currentSealed // Only refresh replaces this immutable map, under ioMu.
	r.mu.RUnlock()
	next := make(map[int64]*datapb.StreamingNodeBM25Resource)
	for _, resource := range response.GetBm25Resources() {
		if !r.includesPartition(resource.GetPartitionId()) {
			continue
		}
		next[resource.GetSegmentId()] = proto.Clone(resource).(*datapb.StreamingNodeBM25Resource)
	}
	positive, negative := make(bm25Stats), make(bm25Stats)
	for id, resource := range previous {
		if proto.Equal(resource, next[id]) {
			continue
		}
		stats, err := loadSealedSegmentStats(ctx, r.provider.chunkManager, resource, newBM25StatsFromSchema(r.schema, r.fieldIDs))
		if err != nil {
			return err
		}
		negative.merge(stats)
	}
	for id, resource := range next {
		if proto.Equal(resource, previous[id]) {
			continue
		}
		stats, err := loadSealedSegmentStats(ctx, r.provider.chunkManager, resource, newBM25StatsFromSchema(r.schema, r.fieldIDs))
		if err != nil {
			return err
		}
		positive.merge(stats)
	}
	// The owner lock orders the barrier after any flush that could have produced
	// this Coord snapshot, including compaction outputs with different segment IDs.
	if r.barrier != nil {
		if err := r.barrier(ctx); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	evicted := make([]int64, 0)
	for id, segment := range r.growingStore.segments {
		if segment.flushed && segment.sealedAt == nil {
			return nodescheduler.ErrDelay
		}
		_, sealed := next[id]
		if sealed && segment.sealedAt == nil {
			return nodescheduler.ErrDelay
		}
		if sealed || segment.sealedAt != nil && !segment.sealedAt.GT(target) {
			negative.merge(segment.stats)
			evicted = append(evicted, id)
		}
	}
	// Validate every field first: no partial commit on a corrupt resource read.
	for field, stats := range r.currentStats {
		if err := stats.ValidateDelta(positive.getOrCreate(field), negative.getOrCreate(field)); err != nil {
			return err
		}
	}
	for field, stats := range r.currentStats {
		stats.ApplyDelta(positive[field], negative[field])
	}
	for _, id := range evicted {
		delete(r.growingStore.segments, id)
	}
	r.currentVersion, r.currentSealed = target, next
	return nil
}

type oracleAdvanceTask struct{ runtime *oracleRuntime }

func (t oracleAdvanceTask) Execute(ctx context.Context) error {
	r := t.runtime
	r.mu.RLock()
	target, closed := r.pending, r.closed
	if r.currentVersion.GT(target) {
		target = r.currentVersion
	}
	r.mu.RUnlock()
	if closed {
		return nil
	}
	err := r.refresh(ctx, target)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	if err != nil && !errors.Is(err, nodescheduler.ErrDelay) {
		mlog.RatedWarn(ctx, 0.2, "refresh shared BM25 aggregate failed", mlog.FieldVChannel(r.vchannel), mlog.Err(err))
	}
	if err != nil || r.pending.GT(r.currentVersion) {
		return nodescheduler.ErrDelay
	}
	r.advanceScheduled = false
	return nil
}

func (r *oracleRuntime) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.cancel()
	if r.advanceHandle != nil {
		r.advanceHandle.Cancel()
	}
	r.mu.Unlock()
	// Do not wait for a queued scheduler task: Close can itself run on a worker.
	r.ioMu.Lock()
	defer r.ioMu.Unlock()
	r.mu.Lock()
	r.currentStats, r.currentSealed, r.growingStore.segments = nil, nil, nil
	r.mu.Unlock()
}

func (p *Provider) getSealedBM25Resources(ctx context.Context, collectionID int64, vchannel string, version qviews.DataVersion, partitions []int64, loadInfo uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
	response, err := p.fetchResources(ctx, collectionID, vchannel, version, partitions, loadInfo)
	if err != nil {
		return nil, err
	}
	return response.GetBm25Resources(), nil
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
