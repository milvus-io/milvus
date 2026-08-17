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

package delegator

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const unknownGrowingSourceChannel = "unknown"

// delegatorGrowingSourceProvider exposes this delegator's growing segments as
// flush sources.
//
// It deliberately owns no lifetime of its own over those segments. A growing
// segment is reachable here only while the segment manager still holds it, and
// the flush that reads it pins it for the duration of that read
// (GetGrowingFlushSource -> PinIfNotReleased, released by
// delegatorGrowingFlushSource.Release).
//
// Nothing here keeps a segment alive past its release, and nothing needs to:
// the release side waits instead. Before a channel is unsubscribed, the write
// buffer blocks in waitGrowingFlushDrained until every segment that still owed
// a growing-source flush has finished one, so by the time these segments are
// dropped no flush still needs them.
//
// That ordering is load-bearing, not defensive. These rows exist ONLY in the
// growing segment — the write buffer keeps no copy in growing-source mode — so
// a flush whose source is dropped mid-flight can never be completed by anyone:
// getSyncTask fails with errGrowingSourceUnavailable on every retry, and the
// progress entry, which is deleted only once its batches drain, pins the
// channel checkpoint forever. Do not make release skip that wait.
type delegatorGrowingSourceProvider struct {
	segmentManager segments.SegmentManager
	getTSafe       func() uint64

	mu          sync.Mutex
	channelName string
	// serving gates every answer this provider gives. It goes false→true once
	// in Activate and true→false once in Deactivate, and it never goes back:
	// the answers this provider hands out are turned into STICKY decisions by
	// the caller (a Pending commits a segment to growing-source mode for its
	// whole lifetime), so an answer from a provider that is not serving is not
	// merely late, it is permanently wrong.
	serving      bool
	registration *syncmgr.GrowingSourceRegistration
}

func newDelegatorGrowingSourceProvider(segmentManager segments.SegmentManager, getTSafe ...func() uint64) *delegatorGrowingSourceProvider {
	provider := &delegatorGrowingSourceProvider{
		segmentManager: segmentManager,
		channelName:    unknownGrowingSourceChannel,
	}
	if len(getTSafe) > 0 {
		provider.getTSafe = getTSafe[0]
	}
	return provider
}

func (p *delegatorGrowingSourceProvider) SetChannelName(channelName string) {
	if channelName == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.channelName = channelName
}

func (p *delegatorGrowingSourceProvider) GetGrowingFlushSource(segmentID int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	if !p.isServing() {
		return nil, syncmgr.GrowingSourceUnavailable
	}
	segment := p.segmentManager.GetGrowing(segmentID)
	if segment == nil {
		if p.behindEndPos(endPos) {
			return nil, syncmgr.GrowingSourcePending
		}
		return nil, syncmgr.GrowingSourceUnavailable
	}
	// The pin is the whole lifetime contract: LocalSegment.Release blocks until
	// the refcount drops to zero, so a segment cannot be freed under a flush
	// that is reading it. The provider deliberately keeps no second counter of
	// its own — one would only track the same window less reliably.
	if err := segment.PinIfNotReleased(); err != nil {
		return nil, syncmgr.GrowingSourceUnavailable
	}
	source := &delegatorGrowingFlushSource{segmentID: segmentID, segment: segment, provider: p}
	return source, syncmgr.GrowingSourceUsable
}

// behindEndPos reports whether this delegator has yet to consume endPos, which
// is why a segment it does not hold may still show up later.
//
// The serving re-check is not redundant with the one in GetGrowingFlushSource:
// Deactivate can land between the two, and this is the branch that produces
// Pending. Being wrong here is not a lost round trip, it is a segment committed
// to a source that will never exist.
func (p *delegatorGrowingSourceProvider) behindEndPos(endPos *msgpb.MsgPosition) bool {
	if endPos == nil || endPos.GetTimestamp() == 0 || p.getTSafe == nil {
		return false
	}
	if !p.isServing() {
		return false
	}
	return p.getTSafe() < endPos.GetTimestamp()
}

func (p *delegatorGrowingSourceProvider) isServing() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.serving
}

// Activate publishes this provider to the registry. It is called from
// ShardDelegator.Start, i.e. only once the watch has fully succeeded.
//
// Activate and Deactivate are NOT safe to interleave from independent
// operations: reactivating a deactivated provider would re-register a provider
// whose delegator is already closed. The invariant that forbids it lives one
// layer up — QueryNode.channelOpLock serializes WatchDmChannels and
// UnsubDmChannel per channel, so for one provider instance Activate always
// happens-before its Deactivate, exactly once each.
//
// Registering earlier — at construction — makes the provider reachable while
// the pipeline may still fail to build: it would answer Pending (its segments
// do not exist yet), the write buffer would take that as "growing-source" and
// stop keeping the rows, and the rollback would then unregister the provider,
// leaving a progress entry whose source can never appear.
func (p *delegatorGrowingSourceProvider) Activate() {
	p.mu.Lock()
	if p.serving || p.registration != nil {
		p.mu.Unlock()
		return
	}
	// Set before registering, never after: once registered the provider is
	// reachable, and answering Unavailable in that gap would push a segment to
	// the write buffer for good.
	p.serving = true
	channelName := p.channelName
	p.mu.Unlock()

	registration := syncmgr.DefaultGrowingSourceRegistry().Register(channelName, p)

	p.mu.Lock()
	if !p.serving {
		// Deactivate ran while we were registering; it saw no registration to
		// undo, so this one is ours to release.
		p.mu.Unlock()
		syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)
		return
	}
	p.registration = registration
	p.mu.Unlock()
}

// Deactivate stops this provider serving and unregisters it. Clearing `serving`
// first is what makes it safe for a caller that copied the provider pointer
// before Unregister took effect.
//
// It deliberately does not wait for flushes already holding a source. Each one
// pins its segment, and that pin — not anything tracked here — is what keeps
// the segment alive until the flush lets go.
func (p *delegatorGrowingSourceProvider) Deactivate() {
	p.mu.Lock()
	p.serving = false
	registration := p.registration
	p.registration = nil
	p.mu.Unlock()
	syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)
}

type delegatorGrowingFlushSource struct {
	segmentID int64
	segment   segments.Segment
	provider  *delegatorGrowingSourceProvider
	once      sync.Once
}

// TSafe is the delegator's consumption watermark: a raw read of the value the
// pipeline publishes at the END of each message pack (deleteNode), after every
// insert in that pack has been applied to its growing segment and acknowledged.
// So tsafe >= T means every row with timestamp <= T is present and complete.
//
// Deliberately NOT shardDelegator.waitTSafe: that is a query-serving policy
// function whose external-table and DowngradeTsafe branches return success
// without the watermark having advanced. Fine for serving a slightly stale read;
// fatal for a flush, which would then publish a checkpoint past rows that were
// never written. And deliberately not a wait at all — see
// syncmgr.GrowingFlushSource.TSafe.
func (s *delegatorGrowingFlushSource) TSafe() uint64 {
	if s.provider == nil || s.provider.getTSafe == nil {
		return 0
	}
	return s.provider.getTSafe()
}

func (s *delegatorGrowingFlushSource) FlushGrowingData(ctx context.Context, startTs, endTs uint64, config *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error) {
	result, err := s.segment.FlushData(ctx, startTs, endTs, &segments.FlushConfig{
		SegmentBasePath:         config.SegmentBasePath,
		PartitionBasePath:       config.PartitionBasePath,
		CollectionID:            config.CollectionID,
		PartitionID:             config.PartitionID,
		Schema:                  config.Schema,
		TextFieldIDs:            config.TextFieldIDs,
		TextLobPaths:            config.TextLobPaths,
		TextInlineThreshold:     config.TextInlineThreshold,
		TextMaxLobFileBytes:     config.TextMaxLobFileBytes,
		TextFlushThresholdBytes: config.TextFlushThresholdBytes,
		BM25FieldIDs:            config.BM25FieldIDs,
		BM25StatsLogIDs:         config.BM25StatsLogIDs,
		WriteMergedBM25Stats:    config.WriteMergedBM25Stats,
		PKStatsFieldID:          config.PKStatsFieldID,
		PKStatsLogID:            config.PKStatsLogID,
		PKStatsBlob:             config.PKStatsBlob,
		MergedPKStatsBlob:       config.MergedPKStatsBlob,
		ReadVersion:             config.ReadVersion,
		WriterFormat:            config.WriterFormat,
		SchemaBasedPattern:      config.SchemaBasedPattern,
		SchemaBasedFormats:      config.SchemaBasedFormats,
		AllowedFieldIDs:         config.AllowedFieldIDs,
		ColumnGroups:            config.ColumnGroups,
	})
	if err != nil || result == nil {
		return nil, err
	}
	return &syncmgr.GrowingFlushResult{
		ManifestPath:           result.ManifestPath,
		NumRows:                result.NumRows,
		TimestampFrom:          result.TimestampFrom,
		TimestampTo:            result.TimestampTo,
		FlushedFieldIDs:        result.FlushedFieldIDs,
		ColumnGroupMemorySizes: result.ColumnGroupMemorySizes,
		FieldNullCounts:        result.FieldNullCounts,
		BM25Stats:              result.BM25Stats,
	}, nil
}

// materializedFieldIDsProvider is the capability a source segment must expose
// for the flush layout to be trimmed to its materialized columns.
type materializedFieldIDsProvider interface {
	MaterializedFieldIDs(ctx context.Context) ([]int64, error)
}

func (s *delegatorGrowingFlushSource) MaterializedFieldIDs(ctx context.Context) ([]int64, error) {
	provider, ok := s.segment.(materializedFieldIDsProvider)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("growing flush source segment does not expose materialized field ids")
	}
	return provider.MaterializedFieldIDs(ctx)
}

type primaryKeysProvider interface {
	PrimaryKeys(ctx context.Context, startTs, endTs uint64) ([]storage.PrimaryKey, error)
}

func (s *delegatorGrowingFlushSource) PrimaryKeys(ctx context.Context, startTs, endTs uint64) ([]storage.PrimaryKey, error) {
	provider, ok := s.segment.(primaryKeysProvider)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("growing flush source segment does not expose primary keys")
	}
	return provider.PrimaryKeys(ctx, startTs, endTs)
}

func (s *delegatorGrowingFlushSource) Release() {
	s.once.Do(func() {
		if s.segment != nil {
			s.segment.Unpin()
		}
	})
}

// CommitGrowingFlush is a no-op for the delegator source.
//
// The call itself stays part of the GrowingFlushSource contract: it is the
// point of no return the flusher signals after the rows are durable, and the
// sync task's ordering guarantee (nothing before a successful meta write may
// call it) is asserted by
// TestGrowingSourceSyncTaskCommitMetaWriteFailureIsBeforePointOfNoReturn.
// The delegator has nothing to release on that signal: a growing segment here
// is only reachable while the segment manager holds it, and the read pin is
// already returned by Release. Nothing currently consumes the fence, so
// recording it would be dead state.
func (s *delegatorGrowingFlushSource) CommitGrowingFlush(flushThroughTs uint64) {}
