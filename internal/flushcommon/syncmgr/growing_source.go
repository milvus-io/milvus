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
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type GrowingFlushConfig struct {
	SegmentBasePath      string
	PartitionBasePath    string
	CollectionID         int64
	PartitionID          int64
	TextFieldIDs         []int64
	TextLobPaths         []string
	BM25FieldIDs         []int64
	BM25StatsLogIDs      []int64
	WriteMergedBM25Stats bool
	ReadVersion          int64
}

type GrowingFlushResult struct {
	ManifestPath string
	NumRows      int64
	BM25Stats    map[int64]*storage.BM25Stats
}

type GrowingFlushSource interface {
	CurrentOffset() int64
	FlushGrowingData(ctx context.Context, startOffset, endOffset int64, config *GrowingFlushConfig) (*GrowingFlushResult, error)
	Release()
}

type GrowingFlushSourceCommitter interface {
	CommitGrowingFlush(targetOffset int64)
}

type GrowingSourceState int

const (
	GrowingSourceUnavailable GrowingSourceState = iota
	GrowingSourcePending
	GrowingSourceUsable
)

type GrowingSourceProvider interface {
	GetGrowingFlushSource(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) (GrowingFlushSource, GrowingSourceState)
}

type GrowingSourceReleaseHandoffSegment struct {
	SegmentID    int64
	TargetOffset int64
}

type GrowingSourceReleaseHandoffProvider interface {
	PrepareGrowingSourceReleaseHandoff(ctx context.Context, fenceTs uint64, segments []GrowingSourceReleaseHandoffSegment) error
	IsReleaseAllowed(segmentID int64, checkpointTs uint64) bool
	IsReleasePrepared(segmentID int64, checkpointTs uint64) bool
	MarkReleaseDetached(segmentID int64)
	ClearReleasePrepared(segmentID int64)
	ReleasePreparedSegments() []int64
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

func (r *GrowingSourceRegistry) PrepareGrowingSourceReleaseHandoff(ctx context.Context, channel string, fenceTs uint64, segments []GrowingSourceReleaseHandoffSegment) error {
	handoffProviders := make([]GrowingSourceReleaseHandoffProvider, 0)
	for _, provider := range r.getProviders(channel) {
		handoffProvider, ok := provider.(GrowingSourceReleaseHandoffProvider)
		if !ok {
			continue
		}
		handoffProviders = append(handoffProviders, handoffProvider)
	}
	if len(handoffProviders) == 0 {
		return merr.WrapErrChannelNotAvailable(channel, "no local growing-source release handoff provider")
	}

	for _, handoffProvider := range handoffProviders {
		if err := handoffProvider.PrepareGrowingSourceReleaseHandoff(ctx, fenceTs, segments); err != nil {
			return err
		}
	}

	for _, segment := range segments {
		if segment.TargetOffset > 0 {
			if !r.IsReleasePrepared(channel, segment.SegmentID, fenceTs) {
				return merr.WrapErrSegmentNotFound(segment.SegmentID, "growing-source release handoff source is not prepared")
			}
			continue
		}
		if !r.IsReleaseAllowed(channel, segment.SegmentID, fenceTs) {
			return merr.WrapErrSegmentNotFound(segment.SegmentID, "growing-source release handoff is not allowed")
		}
	}
	return nil
}

func (r *GrowingSourceRegistry) IsReleaseAllowed(channel string, segmentID int64, checkpointTs uint64) bool {
	for _, provider := range r.getProviders(channel) {
		handoffProvider, ok := provider.(GrowingSourceReleaseHandoffProvider)
		if !ok {
			continue
		}
		if handoffProvider.IsReleaseAllowed(segmentID, checkpointTs) {
			return true
		}
	}
	return false
}

func (r *GrowingSourceRegistry) IsReleasePrepared(channel string, segmentID int64, checkpointTs uint64) bool {
	for _, provider := range r.getProviders(channel) {
		handoffProvider, ok := provider.(GrowingSourceReleaseHandoffProvider)
		if !ok {
			continue
		}
		if handoffProvider.IsReleasePrepared(segmentID, checkpointTs) {
			return true
		}
	}
	return false
}

func (r *GrowingSourceRegistry) ClearReleasePrepared(channel string, segmentID int64) {
	for _, provider := range r.getProviders(channel) {
		handoffProvider, ok := provider.(GrowingSourceReleaseHandoffProvider)
		if !ok {
			continue
		}
		handoffProvider.ClearReleasePrepared(segmentID)
	}
}

func (r *GrowingSourceRegistry) MarkReleaseDetached(channel string, segmentID int64) {
	for _, provider := range r.getProviders(channel) {
		handoffProvider, ok := provider.(GrowingSourceReleaseHandoffProvider)
		if !ok {
			continue
		}
		handoffProvider.MarkReleaseDetached(segmentID)
	}
}

func (r *GrowingSourceRegistry) ReleasePreparedSegments(channel string) []int64 {
	var segments []int64
	for _, provider := range r.getProviders(channel) {
		handoffProvider, ok := provider.(GrowingSourceReleaseHandoffProvider)
		if !ok {
			continue
		}
		segments = append(segments, handoffProvider.ReleasePreparedSegments()...)
	}
	return lo.Uniq(segments)
}

func (r *GrowingSourceRegistry) Resolve(channel string, segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) (GrowingFlushSource, GrowingSourceState) {
	hasPending := false
	for _, provider := range r.getProviders(channel) {
		if provider == nil {
			continue
		}
		source, state := provider.GetGrowingFlushSource(segmentID, targetOffset, endPos)
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
	targetOffset  int64
	level         datapb.SegmentLevel
	isFlush       bool
	isDrop        bool

	metacache  metacache.MetaCache
	metaWriter MetaWriter
	schema     *schemapb.CollectionSchema
	source     GrowingFlushSource

	chunkManager storage.ChunkManager
	allocator    allocator.Interface
	manifestPath string
	flushedSize  int64
	bm25Stats    map[int64]*storage.BM25Stats

	committedManifestPath string
	committedBM25Stats    map[int64]*storage.BM25Stats

	writeRetryOpts  []retry.Option
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

func (t *GrowingSourceSyncTask) WithTargetOffset(targetOffset int64) *GrowingSourceSyncTask {
	t.targetOffset = targetOffset
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

func (t *GrowingSourceSyncTask) WithCommittedFlush(manifestPath string, bm25Stats map[int64]*storage.BM25Stats) *GrowingSourceSyncTask {
	t.committedManifestPath = manifestPath
	t.committedBM25Stats = bm25Stats
	return t
}

func (t *GrowingSourceSyncTask) WithAllocator(allocator allocator.Interface) *GrowingSourceSyncTask {
	t.allocator = allocator
	return t
}

func (t *GrowingSourceSyncTask) WithChunkManager(cm storage.ChunkManager) *GrowingSourceSyncTask {
	t.chunkManager = cm
	return t
}

func (t *GrowingSourceSyncTask) WithWriteRetryOptions(opts ...retry.Option) *GrowingSourceSyncTask {
	t.writeRetryOpts = opts
	return t
}

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

func (t *GrowingSourceSyncTask) BatchRows() int64 {
	return t.batchRows
}

func (t *GrowingSourceSyncTask) TargetOffset() int64 {
	return t.targetOffset
}

func (t *GrowingSourceSyncTask) HandleError(err error) {
	if t.failureCallback != nil {
		t.failureCallback(err)
	}
	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel, t.level.String()).Inc()
}

func (t *GrowingSourceSyncTask) ReleaseSource() {
	if t.source != nil {
		t.source.Release()
		t.source = nil
	}
}

func (t *GrowingSourceSyncTask) Run(ctx context.Context) (err error) {
	t.tr = timerecord.NewTimeRecorder("growingSourceSyncTask")
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.String("channel", t.channelName),
	)
	commitSource := false
	defer func() {
		committer, shouldCommit := t.source.(GrowingFlushSourceCommitter)
		t.ReleaseSource()
		if commitSource && shouldCommit && (t.IsFlush() || t.IsDrop()) {
			committer.CommitGrowingFlush(t.targetOffset)
		}
		if err != nil {
			t.HandleError(err)
		}
	}()

	segment, ok := t.metacache.GetSegmentByID(t.segmentID)
	if !ok {
		if t.isDrop {
			log.Info("segment dropped, discard growing source sync task")
			return nil
		}
		log.Warn("segment not found in metacache")
		return nil
	}
	expectedRows := t.targetOffset - segment.FlushedRows()
	if expectedRows < 0 {
		return merr.WrapErrServiceInternalMsg("growing source target offset is behind flushed rows, flushedRows=%d targetOffset=%d segmentID=%d",
			segment.FlushedRows(), t.targetOffset, t.segmentID)
	}
	if t.committedManifestPath != "" {
		t.manifestPath = t.committedManifestPath
		t.bm25Stats = t.committedBM25Stats
	} else if expectedRows == 0 {
		t.manifestPath = segment.ManifestPath()
	} else {
		if t.source == nil {
			return merr.WrapErrServiceInternalMsg("growing flush source is nil")
		}
		if t.source.CurrentOffset() < t.targetOffset {
			return merr.WrapErrServiceInternalMsg("growing flush source is behind target offset, current=%d target=%d", t.source.CurrentOffset(), t.targetOffset)
		}
		config, err := t.buildFlushConfig(segment)
		if err != nil {
			return err
		}
		result, err := t.source.FlushGrowingData(ctx, segment.FlushedRows(), t.targetOffset, config)
		if err != nil {
			return errors.Wrap(err, "flush growing source data")
		}
		if result == nil || result.ManifestPath == "" {
			return merr.WrapErrDataIntegrityMsg("growing source flush returned empty manifest")
		}
		if result.NumRows != expectedRows {
			return merr.WrapErrDataIntegrityMsg("growing source flush row count mismatch, expected=%d actual=%d flushedRows=%d targetOffset=%d segmentID=%d",
				expectedRows, result.NumRows, segment.FlushedRows(), t.targetOffset, t.segmentID)
		}
		t.manifestPath = result.ManifestPath
		if len(result.BM25Stats) > 0 {
			t.bm25Stats = result.BM25Stats
		}
	}
	t.flushedSize = expectedRows

	if t.metaWriter != nil {
		if err := t.metaWriter.UpdateGrowingSourceSync(ctx, t); err != nil {
			return err
		}
	}

	actions := make([]metacache.SegmentAction, 0, 3)
	if t.batchRows > 0 {
		actions = append(actions, metacache.FinishSyncing(t.batchRows))
	}
	if t.manifestPath != "" {
		actions = append(actions, metacache.UpdateManifestPath(t.manifestPath))
	}
	if len(t.bm25Stats) > 0 {
		actions = append(actions, metacache.MergeBm25Stats(t.bm25Stats))
	}
	if t.IsFlush() {
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Flushed))
	}
	t.metacache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(t.segmentID))
	if t.isDrop {
		t.metacache.RemoveSegments(metacache.WithSegmentIDs(t.segmentID))
		log.Info("dropped growing source segment removed")
	}
	commitSource = true

	metrics.DataNodeWriteDataCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.StreamingDataSourceLabel, metrics.InsertLabel, fmt.Sprint(t.collectionID)).Add(float64(t.batchRows))
	metrics.DataNodeFlushedRows.WithLabelValues(paramtable.GetStringNodeID(), metrics.StreamingDataSourceLabel).Add(float64(t.batchRows))
	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel, t.level.String()).Inc()
	log.Info("growing source sync task done",
		zap.Int64("targetOffset", t.targetOffset),
		zap.Int64("batchRows", t.batchRows),
		zap.String("manifestPath", t.manifestPath),
		zap.Duration("timeTaken", t.tr.ElapseSpan()))
	return nil
}

func (t *GrowingSourceSyncTask) buildFlushConfig(segment *metacache.SegmentInfo) (*GrowingFlushConfig, error) {
	segmentBasePath := path.Join(t.chunkManager.RootPath(), common.SegmentInsertLogPath,
		metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID))
	partitionBasePath := path.Join(t.chunkManager.RootPath(), common.SegmentInsertLogPath,
		metautil.JoinIDPath(t.collectionID, t.partitionID))

	var textFieldIDs []int64
	var textLobPaths []string
	var bm25FieldIDs []int64
	var bm25StatsLogIDs []int64
	if t.schema != nil {
		for _, field := range t.schema.GetFields() {
			if field.GetDataType() == schemapb.DataType_Text {
				fieldID := field.GetFieldID()
				textFieldIDs = append(textFieldIDs, fieldID)
				textLobPaths = append(textLobPaths, fmt.Sprintf("%s/lobs/%d", partitionBasePath, fieldID))
			}
		}
		for _, function := range t.schema.GetFunctions() {
			if function.GetType() == schemapb.FunctionType_BM25 && len(function.GetOutputFieldIds()) > 0 {
				bm25FieldIDs = append(bm25FieldIDs, function.GetOutputFieldIds()[0])
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

	return &GrowingFlushConfig{
		SegmentBasePath:      segmentBasePath,
		PartitionBasePath:    partitionBasePath,
		CollectionID:         t.collectionID,
		PartitionID:          t.partitionID,
		TextFieldIDs:         textFieldIDs,
		TextLobPaths:         textLobPaths,
		BM25FieldIDs:         bm25FieldIDs,
		BM25StatsLogIDs:      bm25StatsLogIDs,
		WriteMergedBM25Stats: t.IsFlush() && t.level != datapb.SegmentLevel_L0 && t.schema != nil && hasBM25Function(t.schema),
		ReadVersion:          manifestVersion(segment.ManifestPath()),
	}, nil
}

func (t *GrowingSourceSyncTask) allocBM25StatsLogIDs(count int) ([]int64, error) {
	if t.allocator == nil {
		return nil, merr.WrapErrServiceInternal("id allocator is nil when allocating bm25 stats log ids")
	}
	ids := make([]int64, count)
	for i := range ids {
		id, err := t.allocator.AllocOne()
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func manifestVersion(manifestPath string) int64 {
	if manifestPath == "" {
		return packed.ManifestEarliest
	}
	if _, version, err := packedManifestVersion(manifestPath); err == nil {
		return version
	}
	return packed.ManifestEarliest
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
