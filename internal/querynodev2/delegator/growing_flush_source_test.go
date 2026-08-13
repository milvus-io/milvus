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
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDelegatorGrowingFlushSourcePassesTaskSchema(t *testing.T) {
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{Name: "task-schema"}
	segment := segments.NewMockSegment(t)
	segment.EXPECT().
		FlushData(ctx, uint64(3), uint64(7), mock.Anything).
		RunAndReturn(func(_ context.Context, startTs uint64, endTs uint64, config *segments.FlushConfig) (*segments.FlushResult, error) {
			require.EqualValues(t, 3, startTs)
			require.EqualValues(t, 7, endTs)
			require.True(t, config.Schema == schema)
			return &segments.FlushResult{
				ManifestPath:           "manifest",
				NumRows:                4,
				TimestampFrom:          100,
				TimestampTo:            200,
				ColumnGroupMemorySizes: map[int64]int64{100: 64},
				FieldNullCounts:        map[int64]int64{100: 1},
			}, nil
		})

	source := &delegatorGrowingFlushSource{segment: segment}
	result, err := source.FlushGrowingData(ctx, 3, 7, &syncmgr.GrowingFlushConfig{
		Schema: schema,
	})
	require.NoError(t, err)
	require.Equal(t, "manifest", result.ManifestPath)
	require.EqualValues(t, 4, result.NumRows)
	require.EqualValues(t, 100, result.TimestampFrom)
	require.EqualValues(t, 200, result.TimestampTo)
	require.EqualValues(t, 64, result.ColumnGroupMemorySizes[100])
	require.EqualValues(t, 1, result.FieldNullCounts[100])
}

// The segment pin is the provider's entire lifetime contract: resolving takes
// exactly one pin and Release drops exactly one, however many times it is
// called. Nothing counts leases on the side — LocalSegment.Release already
// blocks on the refcount, and a second counter here would only track the same
// window less reliably.
func TestDelegatorGrowingSourceProviderPinsOnceAndReleaseIsIdempotent(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager)
	provider.Activate()
	defer provider.Deactivate()

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().Unpin().Once()

	source, state := provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUsable, state)
	require.NotNil(t, source)

	source.Release()
	source.Release()
}

// A provider must not be reachable before Activate. Registering at
// construction made it answer while the watch could still fail: it would report
// Pending (its segments do not exist yet), the write buffer would fix the
// segment to growing-source mode and stop keeping the rows, and the rollback
// would then unregister it — leaving progress whose source can never appear.
func TestDelegatorGrowingSourceProviderNotReachableBeforeActivate(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_0_101v0"
	segmentManager := segments.NewMockSegmentManager(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, func() uint64 { return 100 })
	provider.SetChannelName(channel)
	registry := syncmgr.DefaultGrowingSourceRegistry()

	require.Zero(t, registry.ProviderCount(channel))
	// Even called directly it must refuse, so a stray reference cannot commit a
	// segment to a source that is not serving yet.
	source, state := provider.GetGrowingFlushSource(1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)

	provider.Activate()
	defer provider.Deactivate()
	require.Equal(t, 1, registry.ProviderCount(channel))
}

// Regression: GrowingSourceRegistry.Resolve copies the provider slice and drops
// its lock before calling in, so a pointer copied just before Deactivate still
// gets called. A dead delegator has a frozen TSafe, so without the serving gate
// it answers Pending — and Pending is not a retryable "not yet" to the caller,
// it fixes the segment in growing-source mode permanently.
func TestDelegatorGrowingSourceProviderCopiedPointerRefusesAfterDeactivate(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_0_101v0"
	segmentManager := segments.NewMockSegmentManager(t)
	// TSafe frozen behind the fence, exactly like a delegator that stopped
	// consuming.
	provider := newDelegatorGrowingSourceProvider(segmentManager, func() uint64 { return 100 })
	provider.SetChannelName(channel)
	registry := syncmgr.DefaultGrowingSourceRegistry()
	provider.Activate()

	// What Resolve does: copy the pointer, then call it outside the lock.
	copied := provider

	provider.Deactivate()
	require.Zero(t, registry.ProviderCount(channel))

	source, state := copied.GetGrowingFlushSource(1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state,
		"a deactivated provider must never answer Pending: the caller makes that answer sticky")
	require.Nil(t, source)

	// And through the registry, which is the production path.
	source, state = registry.Resolve(channel, 1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)
}

// The provider does not decide readiness: resolving only pins the segment, and
// how far it may be flushed is a timestamp question the caller asks through
// TSafe. This is a raw read of the delegator's watermark, never
// shardDelegator.waitTSafe, whose external-table and DowngradeTsafe branches
// report success without the watermark having advanced.
func TestDelegatorGrowingFlushSourceReportsRawTSafe(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	currentTSafe := uint64(4242)
	provider := newDelegatorGrowingSourceProvider(segmentManager, func() uint64 {
		return currentTSafe
	})
	provider.Activate()
	defer provider.Deactivate()

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(nil).Once()
	segment.EXPECT().Unpin().Once()

	source, state := provider.GetGrowingFlushSource(1001, nil)
	require.Equal(t, syncmgr.GrowingSourceUsable, state)
	require.NotNil(t, source)
	require.EqualValues(t, 4242, source.TSafe())

	currentTSafe = 9999
	require.EqualValues(t, 9999, source.TSafe())
	source.Release()
}

func TestDelegatorGrowingSourceProviderMissingSegmentPendingUntilTSafeCaughtUp(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	currentTSafe := uint64(100)
	provider := newDelegatorGrowingSourceProvider(segmentManager, func() uint64 {
		return currentTSafe
	})
	provider.Activate()
	defer provider.Deactivate()

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()
	source, state := provider.GetGrowingFlushSource(1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourcePending, state)
	require.Nil(t, source)

	currentTSafe = 200
	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(nil).Once()
	source, state = provider.GetGrowingFlushSource(1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)
}

// A segment that exists but cannot be pinned is being released. The answer must
// be Unavailable, never Pending: Pending would commit the segment to a source
// that is going away, and the caller makes that answer sticky.
func TestDelegatorGrowingSourceProviderPinFailureIsUnavailableNotPending(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	segment := segments.NewMockSegment(t)
	// TSafe is behind the fence, so a missing segment WOULD be Pending — the pin
	// failure must still win.
	provider := newDelegatorGrowingSourceProvider(segmentManager, func() uint64 { return 100 })
	provider.Activate()
	defer provider.Deactivate()

	segmentManager.EXPECT().GetGrowing(int64(1001)).Return(segment).Once()
	segment.EXPECT().PinIfNotReleased().Return(merr.WrapErrSegmentNotLoaded(1001, "segment released")).Once()

	source, state := provider.GetGrowingFlushSource(1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state)
	require.Nil(t, source)
}

// Deactivate landing between GetGrowingFlushSource's serving check and the
// behindEndPos re-check must yield Unavailable, not Pending: the delegator is
// dead, its TSafe is frozen behind the fence forever, so "not yet" is a lie.
// The interleaving is forced deterministically by deactivating from inside the
// segment lookup.
func TestDelegatorGrowingSourceProviderDeactivateDuringLookupIsUnavailableNotPending(t *testing.T) {
	segmentManager := segments.NewMockSegmentManager(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager, func() uint64 { return 100 })
	provider.Activate()

	segmentManager.EXPECT().GetGrowing(int64(1001)).RunAndReturn(func(int64) segments.Segment {
		// The race under test: Deactivate lands after the first serving check
		// passed, before behindEndPos runs.
		provider.Deactivate()
		return nil
	}).Once()

	source, state := provider.GetGrowingFlushSource(1001, &msgpb.MsgPosition{Timestamp: 200})
	require.Equal(t, syncmgr.GrowingSourceUnavailable, state,
		"behindEndPos must re-check serving: a dead delegator's frozen TSafe may not produce Pending")
	require.Nil(t, source)
}

// Deactivate before Activate ever ran is a safe no-op.
func TestDelegatorGrowingSourceProviderDeactivateBeforeActivateIsNoop(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_deactivate_first_v0"
	segmentManager := segments.NewMockSegmentManager(t)
	provider := newDelegatorGrowingSourceProvider(segmentManager)
	provider.SetChannelName(channel)

	require.NotPanics(t, func() {
		provider.Deactivate()
		provider.Deactivate()
	})
	require.Zero(t, syncmgr.DefaultGrowingSourceRegistry().ProviderCount(channel))
}

// Activate registers outside the provider lock, so a Deactivate can land in the
// window between `serving = true` and the registration being stored. The
// cleanup branch in Activate must then release the registration itself, or it
// leaks into the process-global registry forever. The window is racy, so this
// hammers it and asserts the invariant that must hold on every interleaving:
// once Deactivate (ordered after Activate) has run, no registration remains.
func TestDelegatorGrowingSourceProviderActivateDeactivateRaceLeaksNoRegistration(t *testing.T) {
	registry := syncmgr.DefaultGrowingSourceRegistry()
	for i := 0; i < 300; i++ {
		channel := fmt.Sprintf("by-dev-rootcoord-dml_activate_race_%d_v0", i)
		segmentManager := segments.NewMockSegmentManager(t)
		provider := newDelegatorGrowingSourceProvider(segmentManager)
		provider.SetChannelName(channel)

		started := make(chan struct{})
		done := make(chan struct{})
		go func() {
			close(started)
			provider.Activate()
			close(done)
		}()
		<-started
		// Concurrent with Activate: may hit before serving is set, inside the
		// registration window, or after it.
		provider.Deactivate()
		<-done
		// Production ordering guarantee (channelOpLock): the final Deactivate
		// happens after Activate returned.
		provider.Deactivate()

		require.Zero(t, registry.ProviderCount(channel),
			"no interleaving of Activate/Deactivate may leak a registration")
	}
}

// FlushGrowingData is a pure field-for-field bridge between the syncmgr config
// and the segment flush config, and between the segment result and the syncmgr
// result. Every field must survive the crossing — a dropped one silently
// corrupts the flush (wrong paths, missing stats, wrong layout).
func TestDelegatorGrowingFlushSourceFullFieldMapping(t *testing.T) {
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{Name: "full-mapping"}
	columnGroups := []storagecommon.ColumnGroup{{
		GroupID: 7,
		Columns: []int{0, 1},
		Fields:  []int64{100, 101},
		Format:  "parquet",
	}}
	bm25 := storage.NewBM25Stats()

	segmentResult := &segments.FlushResult{
		ManifestPath:           "manifest/path",
		NumRows:                42,
		TimestampFrom:          100,
		TimestampTo:            200,
		FlushedFieldIDs:        []int64{100, 101},
		ColumnGroupMemorySizes: map[int64]int64{7: 64},
		FieldNullCounts:        map[int64]int64{101: 3},
		BM25Stats:              map[int64]*storage.BM25Stats{102: bm25},
	}

	segment := segments.NewMockSegment(t)
	segment.EXPECT().
		FlushData(ctx, uint64(3), uint64(7), mock.Anything).
		RunAndReturn(func(_ context.Context, _ uint64, _ uint64, config *segments.FlushConfig) (*segments.FlushResult, error) {
			require.Equal(t, &segments.FlushConfig{
				SegmentBasePath:         "seg/base",
				PartitionBasePath:       "part/base",
				CollectionID:            11,
				PartitionID:             22,
				Schema:                  schema,
				TextFieldIDs:            []int64{103},
				TextLobPaths:            []string{"part/base/lobs/103"},
				TextInlineThreshold:     128,
				TextMaxLobFileBytes:     1 << 20,
				TextFlushThresholdBytes: 1 << 16,
				BM25FieldIDs:            []int64{102},
				BM25StatsLogIDs:         []int64{9001},
				WriteMergedBM25Stats:    true,
				PKStatsFieldID:          100,
				PKStatsLogID:            9002,
				PKStatsBlob:             []byte("pk-stats"),
				MergedPKStatsBlob:       []byte("merged-pk-stats"),
				ReadVersion:             5,
				WriterFormat:            "loon",
				SchemaBasedPattern:      "pattern",
				SchemaBasedFormats:      "formats",
				AllowedFieldIDs:         []int64{100, 101, 102, 103},
				ColumnGroups:            columnGroups,
			}, config)
			return segmentResult, nil
		}).Once()

	source := &delegatorGrowingFlushSource{segment: segment}
	result, err := source.FlushGrowingData(ctx, 3, 7, &syncmgr.GrowingFlushConfig{
		SegmentBasePath:         "seg/base",
		PartitionBasePath:       "part/base",
		CollectionID:            11,
		PartitionID:             22,
		Schema:                  schema,
		TextFieldIDs:            []int64{103},
		TextLobPaths:            []string{"part/base/lobs/103"},
		TextInlineThreshold:     128,
		TextMaxLobFileBytes:     1 << 20,
		TextFlushThresholdBytes: 1 << 16,
		BM25FieldIDs:            []int64{102},
		BM25StatsLogIDs:         []int64{9001},
		WriteMergedBM25Stats:    true,
		PKStatsFieldID:          100,
		PKStatsLogID:            9002,
		PKStatsBlob:             []byte("pk-stats"),
		MergedPKStatsBlob:       []byte("merged-pk-stats"),
		ReadVersion:             5,
		WriterFormat:            "loon",
		SchemaBasedPattern:      "pattern",
		SchemaBasedFormats:      "formats",
		AllowedFieldIDs:         []int64{100, 101, 102, 103},
		ColumnGroups:            columnGroups,
	})
	require.NoError(t, err)
	require.Equal(t, &syncmgr.GrowingFlushResult{
		ManifestPath:           "manifest/path",
		NumRows:                42,
		TimestampFrom:          100,
		TimestampTo:            200,
		FlushedFieldIDs:        []int64{100, 101},
		ColumnGroupMemorySizes: map[int64]int64{7: 64},
		FieldNullCounts:        map[int64]int64{101: 3},
		BM25Stats:              map[int64]*storage.BM25Stats{102: bm25},
	}, result)
}

// An empty flush range yields (nil, nil) from the segment; the source must
// forward exactly that, not a non-nil empty result the caller would try to
// publish as a flushed batch.
func TestDelegatorGrowingFlushSourceNilResultPassthrough(t *testing.T) {
	ctx := context.Background()
	segment := segments.NewMockSegment(t)
	segment.EXPECT().FlushData(ctx, uint64(10), uint64(10), mock.Anything).Return(nil, nil).Once()

	source := &delegatorGrowingFlushSource{segment: segment}
	result, err := source.FlushGrowingData(ctx, 10, 10, &syncmgr.GrowingFlushConfig{})
	require.NoError(t, err)
	require.Nil(t, result)
}
