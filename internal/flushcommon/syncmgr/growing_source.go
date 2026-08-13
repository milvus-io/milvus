// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syncmgr

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type GrowingFlushConfig struct {
	SegmentBasePath         string
	PartitionBasePath       string
	CollectionID            int64
	PartitionID             int64
	Schema                  *schemapb.CollectionSchema
	TextFieldIDs            []int64
	TextLobPaths            []string
	TextInlineThreshold     int64
	TextMaxLobFileBytes     int64
	TextFlushThresholdBytes int64
	BM25FieldIDs            []int64
	BM25StatsLogIDs         []int64
	WriteMergedBM25Stats    bool
	PKStatsFieldID          int64
	PKStatsLogID            int64
	PKStatsBlob             []byte
	MergedPKStatsBlob       []byte
	ReadVersion             int64
	WriterFormat            string
	SchemaBasedPattern      string
	SchemaBasedFormats      string
	AllowedFieldIDs         []int64
	ColumnGroups            []storagecommon.ColumnGroup
}

type GrowingFlushResult struct {
	ManifestPath  string
	NumRows       int64
	TimestampFrom uint64
	TimestampTo   uint64
	// FlushedFieldIDs is the authoritative set of columns the flush actually
	// wrote. It may be a subset of the flush schema: non-materialized
	// function-output columns are skipped (backfilled later by bump-schema
	// compaction). All binlog meta must be derived from this set, never from
	// the schema.
	FlushedFieldIDs        []int64
	ColumnGroupMemorySizes map[int64]int64
	FieldNullCounts        map[int64]int64
	BM25Stats              map[int64]*storage.BM25Stats
}

// GrowingFlushSource is a growing segment that can flush a range of its own rows
// to storage.
//
// Every range on this interface is a TIMESTAMP range, never a row range. Row
// offsets exist only inside the source segment and mean nothing to this package:
// after a restart the segment is rebuilt by a WAL replay and its offsets start
// over at zero, while the flush path's notion of "what I already flushed" is a
// WAL position. The timestamps here are that position's projection — the caller
// keeps the full MsgPosition (the MsgID is what recovery seeks by) and hands the
// source only the part it can resolve against its own rows. The projection is
// sound because every range on this interface lives on ONE vchannel, where the
// TimeTick order is monotonic and message timestamps are unique; timestamps
// from different physical channels are not comparable and must never meet here.
// Resolving both ends inside the source is what makes the range correct no
// matter where the replay started — a row count kept on the caller's side could
// not be.
type GrowingFlushSource interface {
	// TSafe is the source's consumption watermark: every row with a timestamp
	// <= it has been received AND fully written. It is a raw read, deliberately
	// not a wait.
	//
	// Do NOT reach for the delegator's waitTSafe instead. That is a
	// query-serving policy function with two escape hatches — external tables
	// and the DowngradeTsafe switch — that return a nil error WITHOUT the
	// watermark having reached the requested timestamp. Serving a slightly
	// stale read is a deliberate trade there; flushing on it would advance the
	// channel checkpoint past rows that were never written anywhere, which is
	// silent data loss. Correctness here may not depend on a config switch.
	//
	// Not a wait, either: the flush path must never block on the source. The
	// two are INDEPENDENT readers of the same WAL — the flusher reads it
	// in-process, the delegator opens its own consumer — with no shared queue
	// and no backpressure edge, so their relative lag has no structural bound
	// and "wait for the source" is an unbounded wait, not a short one. Behind
	// is a normal outcome — skip the round and retry.
	TSafe() uint64

	// MaterializedFieldIDs returns the field ids with materialized columns in
	// the source segment. The flush layout must be trimmed to this set; a
	// non-materialized column is legally absent (a dropped field or a
	// function output backfilled by bump-schema compaction). A live segment
	// always has materialized columns, so an empty set is an error, not a
	// no-op.
	MaterializedFieldIDs(ctx context.Context) ([]int64, error)

	// PrimaryKeys returns the primary keys of the rows in (startTs, endTs].
	// Same range as FlushGrowingData and resolved the same way, so the stats
	// built from it describe exactly the rows that get written.
	PrimaryKeys(ctx context.Context, startTs, endTs uint64) ([]storage.PrimaryKey, error)

	// FlushGrowingData writes the rows in (startTs, endTs] to storage.
	//
	// startTs is the timestamp of the position this segment was last flushed
	// through; rows at or below it are already persisted and are excluded.
	// endTs is the timestamp of the position being flushed to — the caller
	// persists THAT position as the checkpoint, unchanged, so this call must
	// cover it exactly.
	//
	// The caller must have established TSafe() >= endTs first. The source's own
	// resolution is additionally bounded by what it has finished writing, so it
	// can never read a half-written row — but that bound alone would silently
	// flush LESS than endTs names, and the caller would then publish a
	// checkpoint for rows that are not in storage.
	FlushGrowingData(ctx context.Context, startTs, endTs uint64, config *GrowingFlushConfig) (*GrowingFlushResult, error)

	// CommitGrowingFlush tells the source that the flush through
	// flushedThroughTs is durable, releasing the rows it persisted.
	// flushedThroughTs is the timestamp of the position that was flushed to —
	// the same fence the flush used, so the source releases exactly what was
	// written. The caller invokes it unconditionally on the segment's final
	// flush or drop; whether it means anything is the source's own business
	// (a source that retains rows past the flush may release them here; the
	// delegator source, which retains nothing, implements it as a no-op).
	CommitGrowingFlush(flushedThroughTs uint64)

	Release()
}

type GrowingSourceState int

const (
	GrowingSourceUnavailable GrowingSourceState = iota
	GrowingSourcePending
	GrowingSourceUsable
)

type GrowingSourceProvider interface {
	// GetGrowingFlushSource resolves the source for a flush that intends to run
	// up to endPos. The position carries its own fence (its timestamp), so no
	// separate row bound is passed.
	GetGrowingFlushSource(segmentID int64, endPos *msgpb.MsgPosition) (GrowingFlushSource, GrowingSourceState)
}

type GrowingSourceRegistry struct {
	mu        sync.RWMutex
	nextToken uint64
	providers map[string]map[uint64]GrowingSourceProvider
}

type GrowingSourceRegistration struct {
	registry *GrowingSourceRegistry
	channel  string
	token    uint64
}

func NewGrowingSourceRegistry() *GrowingSourceRegistry {
	return &GrowingSourceRegistry{
		providers: make(map[string]map[uint64]GrowingSourceProvider),
	}
}

func (r *GrowingSourceRegistry) Register(channel string, provider GrowingSourceProvider) *GrowingSourceRegistration {
	if provider == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextToken++
	token := r.nextToken
	if _, ok := r.providers[channel]; !ok {
		r.providers[channel] = make(map[uint64]GrowingSourceProvider)
	}
	r.providers[channel][token] = provider
	return &GrowingSourceRegistration{
		registry: r,
		channel:  channel,
		token:    token,
	}
}

func (r *GrowingSourceRegistry) Unregister(registration *GrowingSourceRegistration) {
	if registration == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	providers, ok := r.providers[registration.channel]
	if !ok {
		return
	}
	delete(providers, registration.token)
	if len(providers) == 0 {
		delete(r.providers, registration.channel)
	}
}

func (r *GrowingSourceRegistry) ProviderCount(channel string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.providers[channel])
}

// LatestRegistrationToken returns the highest registration token currently live
// for the channel, or 0 if no provider is registered. Tokens are allocated from
// a global monotonic counter, so a value larger than one observed earlier means
// a provider registered after that observation — the signal the write buffer
// uses to lift a release-time admission fence once the channel is served by a
// fresh subscription.
func (r *GrowingSourceRegistry) LatestRegistrationToken(channel string) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var latest uint64
	for token := range r.providers[channel] {
		if token > latest {
			latest = token
		}
	}
	return latest
}

func (r *GrowingSourceRegistry) getProviders(channel string) []GrowingSourceProvider {
	r.mu.RLock()
	channelProviders := r.providers[channel]
	tokens := make([]uint64, 0, len(channelProviders))
	for token := range channelProviders {
		tokens = append(tokens, token)
	}
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})
	providers := make([]GrowingSourceProvider, 0, len(tokens))
	for _, token := range tokens {
		providers = append(providers, channelProviders[token])
	}
	r.mu.RUnlock()
	return providers
}

func (r *GrowingSourceRegistry) Resolve(channel string, segmentID int64, endPos *msgpb.MsgPosition) (GrowingFlushSource, GrowingSourceState) {
	hasPending := false
	for _, provider := range r.getProviders(channel) {
		if provider == nil {
			continue
		}
		source, state := provider.GetGrowingFlushSource(segmentID, endPos)
		if source == nil {
			if state == GrowingSourcePending {
				hasPending = true
			}
			continue
		}
		switch state {
		case GrowingSourceUsable:
			return source, GrowingSourceUsable
		case GrowingSourcePending:
			hasPending = true
			source.Release()
		default:
			source.Release()
		}
	}
	if hasPending {
		return nil, GrowingSourcePending
	}
	return nil, GrowingSourceUnavailable
}

var defaultGrowingSourceRegistry = NewGrowingSourceRegistry()

func DefaultGrowingSourceRegistry() *GrowingSourceRegistry {
	return defaultGrowingSourceRegistry
}

type GrowingSourceSyncTask struct {
	collectionID  int64
	partitionID   int64
	segmentID     int64
	channelName   string
	startPosition *msgpb.MsgPosition
	checkpoint    *msgpb.MsgPosition
	batchRows     int64
	// flushFromTs is the timestamp of the position this segment was last
	// flushed through; rows at or below it are already persisted. The UPPER
	// fence is not a separate field: it is checkpoint.GetTimestamp(), the
	// position this task publishes, so the range written and the position
	// published cannot drift apart.
	flushFromTs uint64
	level       datapb.SegmentLevel
	isFlush     bool
	isDrop      bool

	metacache  metacache.MetaCache
	metaWriter MetaWriter
	schema     *schemapb.CollectionSchema
	source     GrowingFlushSource

	chunkManager  storage.ChunkManager
	allocator     allocator.Interface
	storageConfig *indexpb.StorageConfig
	manifestPath  string
	flushedSize   int64
	insertBinlogs map[int64]*datapb.FieldBinlog
	bm25Stats     map[int64]*storage.BM25Stats
	singlePKStats *storage.PrimaryKeyStats

	committedManifestPath  string
	committedBM25Stats     map[int64]*storage.BM25Stats
	committedInsertBinlogs map[int64]*datapb.FieldBinlog
	committedPKStats       *storage.PrimaryKeyStats

	preparedColumnGroups    []storagecommon.ColumnGroup
	prepared                bool
	storageMetricsPublished bool
	sourceFinalized         bool

	failureCallback func(error)
	tr              *timerecord.TimeRecorder
}

func NewGrowingSourceSyncTask() *GrowingSourceSyncTask {
	return new(GrowingSourceSyncTask)
}

func (t *GrowingSourceSyncTask) WithCollectionID(collectionID int64) *GrowingSourceSyncTask {
	t.collectionID = collectionID
	return t
}

func (t *GrowingSourceSyncTask) WithPartitionID(partitionID int64) *GrowingSourceSyncTask {
	t.partitionID = partitionID
	return t
}

func (t *GrowingSourceSyncTask) WithSegmentID(segmentID int64) *GrowingSourceSyncTask {
	t.segmentID = segmentID
	return t
}

func (t *GrowingSourceSyncTask) WithChannelName(channelName string) *GrowingSourceSyncTask {
	t.channelName = channelName
	return t
}

func (t *GrowingSourceSyncTask) WithStartPosition(position *msgpb.MsgPosition) *GrowingSourceSyncTask {
	t.startPosition = position
	return t
}

func (t *GrowingSourceSyncTask) WithCheckpoint(position *msgpb.MsgPosition) *GrowingSourceSyncTask {
	t.checkpoint = position
	return t
}

func (t *GrowingSourceSyncTask) WithBatchRows(batchRows int64) *GrowingSourceSyncTask {
	t.batchRows = batchRows
	return t
}

func (t *GrowingSourceSyncTask) WithFlushFromTs(ts uint64) *GrowingSourceSyncTask {
	t.flushFromTs = ts
	return t
}

func (t *GrowingSourceSyncTask) WithLevel(level datapb.SegmentLevel) *GrowingSourceSyncTask {
	t.level = level
	return t
}

func (t *GrowingSourceSyncTask) WithFlush() *GrowingSourceSyncTask {
	t.isFlush = true
	return t
}

func (t *GrowingSourceSyncTask) WithDrop() *GrowingSourceSyncTask {
	t.isDrop = true
	return t
}

func (t *GrowingSourceSyncTask) WithMetaCache(metacache metacache.MetaCache) *GrowingSourceSyncTask {
	t.metacache = metacache
	return t
}

func (t *GrowingSourceSyncTask) WithMetaWriter(metaWriter MetaWriter) *GrowingSourceSyncTask {
	t.metaWriter = metaWriter
	return t
}

func (t *GrowingSourceSyncTask) WithSchema(schema *schemapb.CollectionSchema) *GrowingSourceSyncTask {
	t.schema = schema
	return t
}

func (t *GrowingSourceSyncTask) WithSource(source GrowingFlushSource) *GrowingSourceSyncTask {
	t.source = source
	return t
}

func (t *GrowingSourceSyncTask) WithCommittedFlush(manifestPath string, bm25Stats map[int64]*storage.BM25Stats, insertBinlogs ...map[int64]*datapb.FieldBinlog) *GrowingSourceSyncTask {
	t.committedManifestPath = manifestPath
	t.committedBM25Stats = bm25Stats
	if len(insertBinlogs) > 0 {
		t.committedInsertBinlogs = cloneFieldBinlogMap(insertBinlogs[0])
	}
	return t
}

func (t *GrowingSourceSyncTask) WithCommittedPKStats(pkStats *storage.PrimaryKeyStats) *GrowingSourceSyncTask {
	t.committedPKStats = pkStats
	return t
}

func (t *GrowingSourceSyncTask) WithAllocator(allocator allocator.Interface) *GrowingSourceSyncTask {
	t.allocator = allocator
	return t
}

func (t *GrowingSourceSyncTask) WithStorageConfig(storageConfig *indexpb.StorageConfig) *GrowingSourceSyncTask {
	t.storageConfig = storageConfig
	return t
}

func (t *GrowingSourceSyncTask) WithChunkManager(cm storage.ChunkManager) *GrowingSourceSyncTask {
	t.chunkManager = cm
	return t
}

func (t *GrowingSourceSyncTask) SetChunkManager(cm storage.ChunkManager) { t.chunkManager = cm }

func (t *GrowingSourceSyncTask) SetDrop() { t.isDrop = true }

func (t *GrowingSourceSyncTask) WithFailureCallback(callback func(error)) *GrowingSourceSyncTask {
	t.failureCallback = callback
	return t
}

func (t *GrowingSourceSyncTask) SegmentID() int64 {
	return t.segmentID
}

func (t *GrowingSourceSyncTask) Checkpoint() *msgpb.MsgPosition {
	return t.checkpoint
}

func (t *GrowingSourceSyncTask) StartPosition() *msgpb.MsgPosition {
	return t.startPosition
}

func (t *GrowingSourceSyncTask) ChannelName() string {
	return t.channelName
}

func (t *GrowingSourceSyncTask) IsFlush() bool {
	return t.isFlush
}

func (t *GrowingSourceSyncTask) IsDrop() bool {
	return t.isDrop
}

func (t *GrowingSourceSyncTask) ManifestPath() string {
	return t.manifestPath
}

func (t *GrowingSourceSyncTask) HasCommittedFlush() bool {
	return t.committedManifestPath != "" || t.manifestPath != ""
}

func (t *GrowingSourceSyncTask) CommittedManifestPath() string {
	if t.committedManifestPath != "" {
		return t.committedManifestPath
	}
	return t.manifestPath
}

func (t *GrowingSourceSyncTask) CommittedBM25Stats() map[int64]*storage.BM25Stats {
	if len(t.committedBM25Stats) > 0 {
		return t.committedBM25Stats
	}
	return t.bm25Stats
}

func (t *GrowingSourceSyncTask) CommittedInsertBinlogs() map[int64]*datapb.FieldBinlog {
	if len(t.committedInsertBinlogs) > 0 {
		return cloneFieldBinlogMap(t.committedInsertBinlogs)
	}
	return cloneFieldBinlogMap(t.insertBinlogs)
}

func (t *GrowingSourceSyncTask) CommittedPKStats() *storage.PrimaryKeyStats {
	if t.committedPKStats != nil {
		return t.committedPKStats
	}
	return t.singlePKStats
}

func (t *GrowingSourceSyncTask) BatchRows() int64 {
	return t.batchRows
}

// PayloadBytes and friends are always 0 for a growing-source task: its rows
// stay pinned in the segcore growing segment, never in resident Go memory, so
// nothing here counts against flush backpressure or the buffered-size gauge
// ("0 = nothing resident").
func (t *GrowingSourceSyncTask) PayloadBytes() int64 { return 0 }

func (t *GrowingSourceSyncTask) InsertPayloadBytes() int64 { return 0 }

func (t *GrowingSourceSyncTask) DeletePayloadBytes() int64 { return 0 }

// CommittedFlushRecord is a flush whose DATA reached storage but whose metadata
// commit did not. A retry must re-publish exactly what was written, so the
// position and the row count are FROZEN here alongside the manifest.
//
// Re-deriving them from the live ledger would be silent data loss: by the time
// the retry runs, later packs may have been recorded, and the retry would then
// publish the newer position while reusing the old manifest — acking away rows
// that were never persisted.
//
// IsFlush/IsDrop are frozen with the manifest for the same reason, NOT
// re-derived from the segment's state at retry time: a periodic sync to T1
// whose ack failed can be retried after the segment sealed with T2 data
// recorded; deriving the flag then would replay the T1-only manifest as the
// FINAL flush.
type CommittedFlushRecord struct {
	Checkpoint     *msgpb.MsgPosition
	BatchRows      int64
	FlushThroughTs uint64
	IsFlush        bool
	IsDrop         bool
	ManifestPath   string
	BM25Stats      map[int64]*storage.BM25Stats
	InsertBinlogs  map[int64]*datapb.FieldBinlog
	PKStats        *storage.PrimaryKeyStats
}

// CommittedFlushRecord freezes this task's committed-but-unacked flush in one
// step, so the caller stores a single struct instead of transcribing fields
// (which could silently drift as fields are added).
func (t *GrowingSourceSyncTask) CommittedFlushRecord() CommittedFlushRecord {
	return CommittedFlushRecord{
		Checkpoint:     t.Checkpoint(),
		BatchRows:      t.BatchRows(),
		FlushThroughTs: t.FlushThroughTs(),
		IsFlush:        t.IsFlush(),
		IsDrop:         t.IsDrop(),
		ManifestPath:   t.CommittedManifestPath(),
		BM25Stats:      storage.CloneBM25StatsMap(t.CommittedBM25Stats()),
		InsertBinlogs:  t.CommittedInsertBinlogs(),
		PKStats:        t.CommittedPKStats(),
	}
}

// SourceFinalized reports whether this task delivered its terminal flush fence
// to the growing source. A later metadata-only Drop at the same fence may use
// this as proof that it does not need to reacquire a source which the terminal
// notification was allowed to release.
func (t *GrowingSourceSyncTask) SourceFinalized() bool {
	return t.sourceFinalized
}

// FlushThroughTs is the upper fence of this task's range and the timestamp of
// the position it publishes. Derived from the checkpoint rather than stored, so
// the two cannot disagree.
func (t *GrowingSourceSyncTask) FlushThroughTs() uint64 {
	return t.checkpoint.GetTimestamp()
}

// FlushFromTs is the lower fence: the position this segment was last flushed
// through.
func (t *GrowingSourceSyncTask) FlushFromTs() uint64 {
	return t.flushFromTs
}

func (t *GrowingSourceSyncTask) HandleError(err error) {
	if errors.IsAny(err, merr.ErrSegmentNotFound, merr.ErrChannelNotFound) {
		return
	}
	if t.failureCallback != nil {
		t.failureCallback(err)
	}
	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel, t.level.String()).Inc()
	if !t.IsFlush() {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel, t.level.String()).Inc()
	}
}

func (t *GrowingSourceSyncTask) ReleaseSource() {
	if t.source != nil {
		t.source.Release()
		t.source = nil
	}
}

// Prepare materializes the growing segment's rows into object storage. It owns
// no write buffer payload — the rows stay pinned in the growing segment until
// Commit hands them over — so a failed attempt costs nothing but a round trip.
func (t *GrowingSourceSyncTask) Prepare(ctx context.Context) error {
	t.tr = timerecord.NewTimeRecorder("growingSourceSyncTask")

	segment, ok := t.metacache.GetSegmentByID(t.segmentID)
	if !ok {
		// The segment only leaves the metacache once its last task has
		// committed, and progress.syncing keeps at most one growing-source task
		// in flight per segment — so this is an invariant violation for drop
		// tasks too, not a race to tolerate. Skipping instead of failing would
		// let finishGrowingSourceSync ack the flushed range and advance the channel
		// checkpoint for rows that were never materialized, so the WAL could
		// not replay them either. SyncTerminal sends this to the fatal handler
		// rather than a retry loop.
		return merr.WrapErrSegmentNotFound(t.segmentID, "segment removed while its growing source sync task was still in flight")
	}
	// The expected row count is the caller's OWN tally of the WAL messages it
	// recorded in this range — not a difference of two offsets. It is not used
	// to bound the flush (the timestamp fences do that); it is cross-checked
	// against what the source reports it wrote, which is the one place a
	// divergence between "what the WAL said" and "what the growing segment
	// actually holds" can be caught.
	expectedRows := t.batchRows
	if expectedRows < 0 {
		return merr.WrapErrDataIntegrityMsg("growing source batch rows is negative, batchRows=%d segmentID=%d",
			expectedRows, t.segmentID)
	}
	if t.checkpoint.GetTimestamp() < t.flushFromTs {
		// Deterministic: both fences are fixed for this task, so a retry
		// re-derives the same inverted range.
		return merr.WrapErrDataIntegrityMsg("growing source flush range is inverted, flushFromTs=%d flushThroughTs=%d segmentID=%d",
			t.flushFromTs, t.checkpoint.GetTimestamp(), t.segmentID)
	}
	columnGroups, err := t.getColumnGroups(segment)
	if err != nil {
		return err
	}
	// Unification point: from here on the intended layout and the layout the
	// flush actually writes are one. Every consumer below (writer config,
	// binlog meta, metacache current split) sees the same trimmed groups.
	columnGroups, err = t.trimColumnGroupsToMaterialized(ctx, columnGroups)
	if err != nil {
		return err
	}

	materialized := false
	switch {
	case t.committedManifestPath != "":
		t.manifestPath = t.committedManifestPath
		t.bm25Stats = t.committedBM25Stats
		t.insertBinlogs = cloneFieldBinlogMap(t.committedInsertBinlogs)
		t.singlePKStats = t.committedPKStats
		t.flushedSize = growingSourceFlushedSizeFromBinlogs(t.insertBinlogs)
	case expectedRows == 0:
		t.manifestPath = segment.ManifestPath()
		t.flushedSize = 0
	default:
		if err := t.flushGrowingData(ctx, segment, columnGroups, expectedRows); err != nil {
			return err
		}
		materialized = true
	}

	if t.metaWriter != nil && expectedRows > 0 && len(columnGroups) > 0 && len(t.insertBinlogs) == 0 {
		return merr.WrapErrDataIntegrityMsg("growing source committed flush missing insert binlog summary, segmentID=%d flushThroughTs=%d",
			t.segmentID, t.checkpoint.GetTimestamp())
	}
	t.preparedColumnGroups = columnGroups
	t.prepared = true
	if materialized {
		t.observeStorageWriteMetrics()
	}
	return nil
}

// observeStorageWriteMetrics mirrors SyncTask's physical-write contract. It is
// called only for a new materialization, never for a committed-manifest replay,
// so a metadata retry cannot double-count rows or bytes already in storage.
func (t *GrowingSourceSyncTask) observeStorageWriteMetrics() {
	if t.storageMetricsPublished {
		return
	}
	t.storageMetricsPublished = true
	nodeID := paramtable.GetStringNodeID()
	metrics.DataNodeWriteDataCount.WithLabelValues(
		nodeID, metrics.StreamingDataSourceLabel, metrics.InsertLabel, fmt.Sprint(t.collectionID)).Add(float64(t.batchRows))
	metrics.DataNodeFlushedSize.WithLabelValues(
		nodeID, metrics.StreamingDataSourceLabel, t.level.String()).Add(float64(t.flushedSize))
	metrics.DataNodeFlushedRows.WithLabelValues(
		nodeID, metrics.StreamingDataSourceLabel).Add(float64(t.batchRows))
	metrics.DataNodeSave2StorageLatency.WithLabelValues(
		nodeID, t.level.String()).Observe(float64(t.tr.RecordSpan().Milliseconds()))
}

// flushGrowingData is the one step that writes bytes. Splitting it out keeps
// Prepare readable and keeps the "did we materialize anything" decision in one
// switch above.
func (t *GrowingSourceSyncTask) flushGrowingData(ctx context.Context, segment *metacache.SegmentInfo, columnGroups []storagecommon.ColumnGroup, expectedRows int64) error {
	if t.source == nil {
		return merr.WrapErrServiceInternalMsg("growing flush source is nil")
	}
	flushThroughTs := t.checkpoint.GetTimestamp()
	// The source must have CONSUMED past the upper fence, not merely have some
	// rows. Its own resolution is bounded by what it finished writing, so
	// without this it would silently flush less than the fence names while the
	// caller goes on to publish that fence as the checkpoint — rows that exist
	// nowhere, with the WAL already advanced past them.
	//
	// A raw read, and no waiting: see GrowingFlushSource.TSafe. Behind is a
	// normal outcome; the write buffer re-drives this task on a later timetick.
	if tsafe := t.source.TSafe(); tsafe < flushThroughTs {
		return merr.WrapErrServiceInternalMsg("growing flush source has not consumed the flush range yet, tsafe=%d flushThroughTs=%d segmentID=%d",
			tsafe, flushThroughTs, t.segmentID)
	}
	config, err := t.buildFlushConfig(segment, columnGroups)
	if err != nil {
		return err
	}
	var insertSummaryLogIDs []int64
	if t.metaWriter != nil && len(columnGroups) > 0 {
		insertSummaryLogIDs, err = t.allocLogIDs(len(columnGroups), "growing source insert summary")
		if err != nil {
			return err
		}
	}
	if err := t.fillPrimaryKeyStatsConfig(ctx, t.flushFromTs, flushThroughTs, expectedRows, config); err != nil {
		return err
	}
	result, err := t.source.FlushGrowingData(ctx, t.flushFromTs, flushThroughTs, config)
	if err != nil {
		return errors.Wrap(err, "flush growing source data")
	}
	if result == nil || result.ManifestPath == "" {
		return merr.WrapErrDataIntegrityMsg("growing source flush returned empty manifest")
	}
	// The one cross-check between the two sides: the caller counted these rows
	// off the WAL, the source counted them off its own storage. They are derived
	// independently, so a mismatch means the growing segment does not hold what
	// the WAL said it should — publishing it would corrupt the segment's row
	// accounting permanently.
	if result.NumRows != expectedRows {
		return merr.WrapErrDataIntegrityMsg("growing source flush row count mismatch, expected=%d actual=%d flushFromTs=%d flushThroughTs=%d segmentID=%d",
			expectedRows, result.NumRows, t.flushFromTs, flushThroughTs, t.segmentID)
	}
	t.manifestPath = result.ManifestPath
	if len(result.BM25Stats) > 0 {
		t.bm25Stats = result.BM25Stats
	}
	t.flushedSize = growingSourceFlushedSizeFromResult(result)
	if t.metaWriter != nil && len(columnGroups) > 0 {
		t.insertBinlogs, err = buildGrowingSourceInsertBinlogs(columnGroups, result, insertSummaryLogIDs)
		if err != nil {
			return err
		}
	}
	return nil
}

// Commit publishes what Prepare materialized and only then hands the flushed
// rows back to the growing segment. CommitGrowingFlush is the point of no
// return for those rows, so it must not run until the metadata that describes
// them is durable.
func (t *GrowingSourceSyncTask) Commit(ctx context.Context) error {
	if !t.prepared {
		return merr.WrapErrServiceInternalMsg("growing source commit before prepare, segmentID=%d", t.segmentID)
	}
	log := t.getLogger()
	columnGroups := t.preparedColumnGroups

	if t.metaWriter != nil {
		if err := t.metaWriter.UpdateGrowingSourceSync(ctx, t); err != nil {
			return err
		}
	}

	actions := make([]metacache.SegmentAction, 0, 6)
	if t.batchRows > 0 {
		actions = append(actions, metacache.FinishSyncing(t.batchRows))
	}
	if t.manifestPath != "" {
		actions = append(actions, metacache.UpdateManifestPath(t.manifestPath))
	}
	if len(t.bm25Stats) > 0 {
		actions = append(actions, metacache.MergeBm25Stats(t.bm25Stats))
	}
	if t.singlePKStats != nil {
		actions = append(actions, metacache.RollStats(t.singlePKStats))
	}
	if len(columnGroups) > 0 {
		actions = append(actions, metacache.UpdateCurrentSplit(columnGroups))
	}
	// Advance the flush fence in the SAME transaction that publishes the data,
	// so it can never name a position whose rows are not durable yet.
	actions = append(actions, metacache.SetLastFlushPosition(t.checkpoint))
	if t.IsFlush() {
		// Drop may arrive after this task was built. Preserve that monotonic state
		// atomically with the rest of the commit; the write buffer will build a
		// separate drop task after this frozen flush settles.
		actions = append(actions, func(info *metacache.SegmentInfo) {
			if info.State() != commonpb.SegmentState_Dropped {
				metacache.UpdateState(commonpb.SegmentState_Flushed)(info)
			}
		})
	}
	t.metacache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(t.segmentID))
	if t.isDrop {
		t.metacache.RemoveSegments(metacache.WithSegmentIDs(t.segmentID))
		log.Info(ctx, "dropped growing source segment removed")
	}

	// Captured before ReleaseSource nils the field; the read pin must be
	// returned before the commit notification so the final release it can
	// trigger is never blocked on the flusher's own pin.
	source := t.source
	t.ReleaseSource()
	if source != nil && (t.IsFlush() || t.IsDrop()) {
		// Same fence the flush used, so the source releases exactly the rows
		// that were written — no offset crosses this boundary.
		source.CommitGrowingFlush(t.checkpoint.GetTimestamp())
		t.sourceFinalized = true
	}

	if !t.IsFlush() {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel, t.level.String()).Inc()
	}
	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel, t.level.String()).Inc()
	storagev2.PublishFilesystemMetricsWithConfig(t.storageConfig)
	log.Info(ctx, "growing source sync task done",
		mlog.Uint64("flushFromTs", t.flushFromTs),
		mlog.Uint64("flushThroughTs", t.checkpoint.GetTimestamp()),
		mlog.Int64("batchRows", t.batchRows),
		mlog.String("manifestPath", t.manifestPath),
		mlog.Duration("timeTaken", t.tr.ElapseSpan()))
	return nil
}

func (t *GrowingSourceSyncTask) getLogger() *mlog.Logger {
	return mlog.With(
		mlog.Int64("collectionID", t.collectionID),
		mlog.Int64("partitionID", t.partitionID),
		mlog.Int64("segmentID", t.segmentID),
		mlog.String("channel", t.channelName),
	)
}

func (t *GrowingSourceSyncTask) getColumnGroups(segment *metacache.SegmentInfo) ([]storagecommon.ColumnGroup, error) {
	return resolveColumnGroups(segment, t.schema, t.segmentID, func() map[int64]storagecommon.ColumnStats {
		return map[int64]storagecommon.ColumnStats{}
	}), nil
}

// filterColumnGroupFields keeps only the fields keep() accepts, trimming the
// parallel Columns array in lockstep so downstream consumers that map over
// Columns (e.g. SchemaBasedPattern) never see a dropped field. Groups left
// empty are removed. The skipped field ids are returned for logging.
func filterColumnGroupFields(columnGroups []storagecommon.ColumnGroup, keep func(fieldID int64) bool) ([]storagecommon.ColumnGroup, []int64) {
	skipped := make([]int64, 0)
	trimmed := make([]storagecommon.ColumnGroup, 0, len(columnGroups))
	for _, columnGroup := range columnGroups {
		fields := make([]int64, 0, len(columnGroup.Fields))
		columns := make([]int, 0, len(columnGroup.Columns))
		for i, fieldID := range columnGroup.Fields {
			if !keep(fieldID) {
				skipped = append(skipped, fieldID)
				continue
			}
			fields = append(fields, fieldID)
			if i < len(columnGroup.Columns) {
				columns = append(columns, columnGroup.Columns[i])
			}
		}
		if len(fields) == 0 {
			continue
		}
		columnGroup.Fields = fields
		columnGroup.Columns = columns
		trimmed = append(trimmed, columnGroup)
	}
	return trimmed, skipped
}

// trimColumnGroupsToMaterialized trims the flush layout to the columns the
// source segment has actually materialized (plus system fields, which live
// outside the insert record). A non-materialized column is legally absent —
// a dropped field or a function output backfilled later by bump-schema
// compaction; real schema/data inconsistency is segcore's concern and the
// flush verifies it internally. A group left empty is dropped entirely. On a
// committed-flush ack retry the source is gone; the committed binlogs are
// the persisted truth and the layout is trimmed to them instead.
func (t *GrowingSourceSyncTask) trimColumnGroupsToMaterialized(ctx context.Context, columnGroups []storagecommon.ColumnGroup) ([]storagecommon.ColumnGroup, error) {
	if t.schema == nil || len(columnGroups) == 0 {
		return columnGroups, nil
	}
	if t.source == nil {
		if len(t.committedInsertBinlogs) == 0 {
			return columnGroups, nil
		}
		// The committed binlogs are keyed by column group id; the flushed
		// field ids live in ChildFields.
		committed := typeutil.NewSet[int64]()
		for _, fieldBinlog := range t.committedInsertBinlogs {
			committed.Insert(fieldBinlog.GetChildFields()...)
		}
		trimmed, skipped := filterColumnGroupFields(columnGroups, func(fieldID int64) bool {
			return committed.Contain(fieldID)
		})
		if len(skipped) > 0 {
			mlog.Info(ctx, "trim growing flush layout to committed binlogs on ack retry",
				mlog.Int64("segmentID", t.segmentID),
				mlog.Int64s("fieldIDs", skipped))
		}
		return trimmed, nil
	}
	materialized, err := t.source.MaterializedFieldIDs(ctx)
	if err != nil {
		return nil, err
	}
	// A live growing segment materializes its creation-schema columns in the
	// InsertRecord ctor, so an empty set has no legal meaning — refuse it
	// instead of writing a layout that may disagree with the data.
	if len(materialized) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg(
			"growing flush source reported empty materialized field ids for segment %d", t.segmentID)
	}
	materializedSet := typeutil.NewSet(materialized...)
	trimmed, skipped := filterColumnGroupFields(columnGroups, func(fieldID int64) bool {
		return materializedSet.Contain(fieldID) || common.IsSystemField(fieldID)
	})
	if len(skipped) > 0 {
		mlog.Info(ctx, "exclude non-materialized columns from growing flush layout",
			mlog.Int64("segmentID", t.segmentID),
			mlog.Int64s("fieldIDs", skipped))
	}
	return trimmed, nil
}

func (t *GrowingSourceSyncTask) schemaBasedPattern(columnGroups []storagecommon.ColumnGroup) (string, error) {
	if len(columnGroups) == 0 {
		return "", nil
	}
	arrowSchema, err := storage.ConvertToArrowSchema(t.schema, true)
	if err != nil {
		return "", merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", t.schema.GetName(), err.Error()))
	}
	schemaBasedPattern, err := packed.SchemaBasedPattern(arrowSchema, columnGroups)
	if err != nil {
		return "", merr.WrapErrServiceInternal(
			fmt.Sprintf("can not build schema based writer pattern %s", err.Error()))
	}
	return schemaBasedPattern, nil
}

func (t *GrowingSourceSyncTask) buildFlushConfig(segment *metacache.SegmentInfo, columnGroups []storagecommon.ColumnGroup) (*GrowingFlushConfig, error) {
	if segment.GetStorageVersion() != storage.StorageV3 {
		return nil, merr.WrapErrDataIntegrityMsg("growing source flush requires StorageV3 segment, segmentID=%d storageVersion=%d",
			t.segmentID, segment.GetStorageVersion())
	}
	segmentBasePath := path.Join(t.chunkManager.RootPath(), common.SegmentInsertLogPath,
		metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID))
	partitionBasePath := path.Join(t.chunkManager.RootPath(), common.SegmentInsertLogPath,
		metautil.JoinIDPath(t.collectionID, t.partitionID))

	allowedFieldIDs, allowedFieldSet := allowedFieldsFromColumnGroups(columnGroups)
	var textFieldIDs []int64
	var textLobPaths []string
	var bm25FieldIDs []int64
	var bm25StatsLogIDs []int64
	if t.schema != nil {
		for _, field := range typeutil.GetAllFieldSchemas(t.schema) {
			if !fieldAllowed(allowedFieldSet, field.GetFieldID()) {
				continue
			}
			if field.GetDataType() == schemapb.DataType_Text {
				fieldID := field.GetFieldID()
				textFieldIDs = append(textFieldIDs, fieldID)
				textLobPaths = append(textLobPaths, fmt.Sprintf("%s/lobs/%d", partitionBasePath, fieldID))
			}
		}
		for _, function := range t.schema.GetFunctions() {
			if function.GetType() == schemapb.FunctionType_BM25 && len(function.GetOutputFieldIds()) > 0 {
				outputFieldID := function.GetOutputFieldIds()[0]
				if fieldAllowed(allowedFieldSet, outputFieldID) {
					bm25FieldIDs = append(bm25FieldIDs, outputFieldID)
				}
			}
		}
	}
	if len(bm25FieldIDs) > 0 {
		var err error
		bm25StatsLogIDs, err = t.allocBM25StatsLogIDs(len(bm25FieldIDs))
		if err != nil {
			return nil, err
		}
	}
	writerFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	schemaBasedPattern, err := t.schemaBasedPattern(columnGroups)
	if err != nil {
		return nil, err
	}
	readVersion, err := growingSourceReadVersion(segment.ManifestPath(), columnGroups)
	if err != nil {
		return nil, err
	}
	schemaBasedFormats := strings.Join(storagecommon.ColumnGroupFormats(columnGroups, writerFormat), ",")

	return &GrowingFlushConfig{
		SegmentBasePath:         segmentBasePath,
		PartitionBasePath:       partitionBasePath,
		CollectionID:            t.collectionID,
		PartitionID:             t.partitionID,
		Schema:                  t.schema,
		TextFieldIDs:            textFieldIDs,
		TextLobPaths:            textLobPaths,
		TextInlineThreshold:     paramtable.Get().DataNodeCfg.TextInlineThreshold.GetAsInt64(),
		TextMaxLobFileBytes:     paramtable.Get().DataNodeCfg.TextMaxLobFileBytes.GetAsInt64(),
		TextFlushThresholdBytes: paramtable.Get().DataNodeCfg.TextFlushThresholdBytes.GetAsInt64(),
		BM25FieldIDs:            bm25FieldIDs,
		BM25StatsLogIDs:         bm25StatsLogIDs,
		WriteMergedBM25Stats:    t.IsFlush() && t.level != datapb.SegmentLevel_L0 && t.schema != nil && hasBM25Function(t.schema),
		ReadVersion:             readVersion,
		WriterFormat:            writerFormat,
		SchemaBasedPattern:      schemaBasedPattern,
		SchemaBasedFormats:      schemaBasedFormats,
		AllowedFieldIDs:         allowedFieldIDs,
		ColumnGroups:            columnGroups,
	}, nil
}

func (t *GrowingSourceSyncTask) fillPrimaryKeyStatsConfig(ctx context.Context, startTs, endTs uint64, expectedRows int64, config *GrowingFlushConfig) error {
	if expectedRows == 0 {
		return nil
	}
	if t.source == nil {
		return merr.WrapErrServiceInternalMsg("growing flush source is nil")
	}
	pks, err := t.source.PrimaryKeys(ctx, startTs, endTs)
	if err != nil {
		return err
	}
	if int64(len(pks)) != expectedRows {
		return merr.WrapErrDataIntegrityMsg(
			"growing source primary key count mismatch, segmentID=%d expected=%d actual=%d",
			t.segmentID, expectedRows, len(pks))
	}
	var pkField *schemapb.FieldSchema
	for _, field := range t.schema.GetFields() {
		if field.GetIsPrimaryKey() {
			pkField = field
			break
		}
	}
	if pkField == nil {
		return merr.WrapErrDataIntegrityMsg("growing source flush schema has no primary field, segmentID=%d", t.segmentID)
	}
	stats, err := storage.NewPrimaryKeyStats(pkField.GetFieldID(), int64(pkField.GetDataType()), expectedRows)
	if err != nil {
		return err
	}
	for _, pk := range pks {
		if pk.Type() != pkField.GetDataType() {
			return merr.WrapErrDataIntegrityMsg(
				"growing source primary key type mismatch, segmentID=%d expected=%s actual=%s",
				t.segmentID, pkField.GetDataType().String(), pk.Type().String())
		}
		stats.Update(pk)
	}
	blob, err := storage.NewInsertCodec().SerializePkStats(stats, expectedRows)
	if err != nil {
		return err
	}
	logIDs, err := t.allocLogIDs(1, "growing source primary key stats")
	if err != nil {
		return err
	}
	config.PKStatsFieldID = pkField.GetFieldID()
	config.PKStatsLogID = logIDs[0]
	config.PKStatsBlob = blob.Value
	t.singlePKStats = stats
	if t.IsFlush() && t.level != datapb.SegmentLevel_L0 {
		serializer, err := NewStorageSerializer(t.metacache, t.schema)
		if err != nil {
			return err
		}
		mergedBlob, err := serializer.serializeMergedPkStatsWith(&SyncPack{
			segmentID: t.segmentID,
		}, stats)
		if err != nil {
			return err
		}
		if mergedBlob != nil {
			config.MergedPKStatsBlob = mergedBlob.Value
		}
	}
	return nil
}

func growingSourceReadVersion(manifestPath string, columnGroups []storagecommon.ColumnGroup) (int64, error) {
	if manifestPath == "" {
		return packed.ManifestEarliest, nil
	}
	_, version, err := packedManifestVersion(manifestPath)
	if err != nil {
		return 0, err
	}
	if version == packed.ManifestEarliest {
		return version, nil
	}
	if err := validateColumnGroupFormats(columnGroups, manifestPath); err != nil {
		return 0, err
	}
	return version, nil
}

func allowedFieldsFromColumnGroups(columnGroups []storagecommon.ColumnGroup) ([]int64, typeutil.Set[int64]) {
	if len(columnGroups) == 0 {
		return nil, nil
	}
	allowed := typeutil.NewSet[int64]()
	for _, group := range columnGroups {
		allowed.Insert(group.Fields...)
	}
	if allowed.Len() == 0 {
		return nil, nil
	}
	// Sorted on purpose: the IDs cross into GrowingFlushConfig.AllowedFieldIDs,
	// and Set.Collect ordering is unspecified.
	allowedFieldIDs := allowed.Collect()
	sort.Slice(allowedFieldIDs, func(i, j int) bool {
		return allowedFieldIDs[i] < allowedFieldIDs[j]
	})
	return allowedFieldIDs, allowed
}

func fieldAllowed(allowed typeutil.Set[int64], fieldID int64) bool {
	if allowed.Len() == 0 {
		return true
	}
	return allowed.Contain(fieldID)
}

func (t *GrowingSourceSyncTask) allocBM25StatsLogIDs(count int) ([]int64, error) {
	return t.allocLogIDs(count, "bm25 stats")
}

func (t *GrowingSourceSyncTask) allocLogIDs(count int, purpose string) ([]int64, error) {
	if t.allocator == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("id allocator is nil when allocating %s ids", purpose))
	}
	if count <= 0 {
		return nil, nil
	}
	// One batched round trip; per-ID AllocOne would cost N allocator RPCs and
	// add a partial-allocation failure mode the batch API does not have.
	start, _, err := t.allocator.Alloc(uint32(count))
	if err != nil {
		return nil, err
	}
	ids := make([]int64, count)
	for i := range ids {
		ids[i] = start + int64(i)
	}
	return ids, nil
}

func buildGrowingSourceInsertBinlogs(columnGroups []storagecommon.ColumnGroup, result *GrowingFlushResult, logIDs []int64) (map[int64]*datapb.FieldBinlog, error) {
	if result == nil || result.NumRows <= 0 || len(columnGroups) == 0 {
		return nil, nil
	}
	if len(logIDs) != len(columnGroups) {
		return nil, merr.WrapErrDataIntegrityMsg("growing source insert summary log id count mismatch, logIDs=%d columnGroups=%d",
			len(logIDs), len(columnGroups))
	}
	logIDByGroup := make(map[int64]int64, len(columnGroups))
	for i, columnGroup := range columnGroups {
		logIDByGroup[columnGroup.GroupID] = logIDs[i]
	}
	// result.FlushedFieldIDs is the authoritative set of columns actually
	// written; the flush skips legally-absent columns (dropped fields,
	// non-materialized function outputs), so the binlog meta must be trimmed
	// to it. A group left empty is dropped.
	if len(result.FlushedFieldIDs) > 0 {
		flushedSet := typeutil.NewSet(result.FlushedFieldIDs...)
		columnGroups, _ = filterColumnGroupFields(columnGroups, func(fieldID int64) bool {
			return flushedSet.Contain(fieldID)
		})
	}
	for _, columnGroup := range columnGroups {
		if _, ok := result.ColumnGroupMemorySizes[columnGroup.GroupID]; !ok {
			return nil, merr.WrapErrDataIntegrityMsg("growing source missing column group memory size, groupID=%d fields=%v",
				columnGroup.GroupID, columnGroup.Fields)
		}
	}
	memorySize := func(columnGroupID int64) int64 {
		return result.ColumnGroupMemorySizes[columnGroupID]
	}
	fieldNullCounts := func(columnGroup storagecommon.ColumnGroup) map[int64]int64 {
		counts := make(map[int64]int64, len(columnGroup.Fields))
		for _, fieldID := range columnGroup.Fields {
			counts[fieldID] = result.FieldNullCounts[fieldID]
		}
		return counts
	}
	return buildV3ColumnGroupFieldBinlogs(
		columnGroups,
		result.NumRows,
		result.TimestampFrom,
		result.TimestampTo,
		func(columnGroupID int64) int64 { return 0 },
		memorySize,
		func(columnGroupID int64) int64 { return logIDByGroup[columnGroupID] },
		nil,
		fieldNullCounts,
	), nil
}

func growingSourceFlushedSizeFromResult(result *GrowingFlushResult) int64 {
	if result == nil {
		return 0
	}
	var size int64
	for _, memorySize := range result.ColumnGroupMemorySizes {
		size += memorySize
	}
	return size
}

func growingSourceFlushedSizeFromBinlogs(binlogs map[int64]*datapb.FieldBinlog) int64 {
	var size int64
	for _, fieldBinlog := range binlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if binlog.GetLogSize() > 0 {
				size += binlog.GetLogSize()
				continue
			}
			size += binlog.GetMemorySize()
		}
	}
	return size
}

func cloneFieldBinlogMap(binlogs map[int64]*datapb.FieldBinlog) map[int64]*datapb.FieldBinlog {
	if len(binlogs) == 0 {
		return nil
	}
	cloned := make(map[int64]*datapb.FieldBinlog, len(binlogs))
	for fieldID, binlog := range binlogs {
		if binlog == nil {
			continue
		}
		cloned[fieldID] = proto.Clone(binlog).(*datapb.FieldBinlog)
	}
	return cloned
}

func packedManifestVersion(manifestPath string) (string, int64, error) {
	return packed.UnmarshalManifestPath(manifestPath)
}

func (t *GrowingSourceSyncTask) startPositions() []*datapb.SegmentStartPosition {
	startPos := lo.Map(t.metacache.GetSegmentsBy(
		metacache.WithSegmentState(commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing),
		metacache.WithLevel(datapb.SegmentLevel_L1),
		metacache.WithStartPosNotRecorded(),
	), func(info *metacache.SegmentInfo, _ int) *datapb.SegmentStartPosition {
		return &datapb.SegmentStartPosition{
			SegmentID:     info.SegmentID(),
			StartPosition: info.StartPosition(),
		}
	})
	if t.level == datapb.SegmentLevel_L0 {
		startPos = append(startPos, &datapb.SegmentStartPosition{SegmentID: t.segmentID, StartPosition: t.startPosition})
	}
	return startPos
}
