package service

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReleaseManualFlushPreparer(t *testing.T) {
	ctx := context.Background()
	releaseSegmentIDs := []int64{1001}
	affectedSegmentIDs := []int64{1002}

	extra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: affectedSegmentIDs})
	assert.NoError(t, err)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	fenceCall := wbManager.EXPECT().FenceGrowingSourceAdmission("vchannel")
	fenceCall.Return()

	wal := mock_wal.NewMockWAL(t)
	appendCall := wal.EXPECT().Append(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		flushMsg, err := message.AsMutableManualFlushMessageV2(msg)
		if err != nil {
			return false
		}
		return flushMsg.VChannel() == "vchannel" &&
			flushMsg.Header().GetCollectionId() == 10
	}))
	appendCall.Return(&types.AppendResult{
		TimeTick: 200,
		Extra:    extra,
	}, nil)
	// The admission fence MUST precede the ManualFlush append: everything
	// admitted before the fence is sealed by the append's timestamp, so fencing
	// after it would leave a window for an unsealed growing-source segment that
	// the drain then waits on forever. Swapping the order in production makes
	// this test fail.
	appendCall.NotBefore(fenceCall.Call)

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil)

	progressCall := wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001, 1002})
	progressCall.Return([]writebuffer.GrowingFlushSegmentProgress{
		{
			SegmentID:          1001,
			FlushThroughTs:     10,
			NeedReleaseHandoff: true,
			SourceMode:         metacache.FlushSourceGrowing,
		},
	}, nil)
	// The progress snapshot and the drain both run after the fence message is
	// in the WAL; only then is the set of segments owing a handoff frozen.
	progressCall.NotBefore(appendCall.Call)
	drainCall := wbManager.EXPECT().WaitGrowingFlushDrained(mock.Anything, "vchannel", []int64{1001})
	drainCall.Return(nil)
	drainCall.NotBefore(appendCall.Call)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	assert.NoError(t, preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", releaseSegmentIDs))
}

func TestReleaseManualFlushPreparerNoGrowingProgress(t *testing.T) {
	ctx := context.Background()
	releaseSegmentIDs := []int64{1001}

	manager, wbManager := newDrainReadyMocks(t, nil, releaseSegmentIDs, []writebuffer.GrowingFlushSegmentProgress{
		{
			SegmentID:          1001,
			NeedReleaseHandoff: false,
			SourceMode:         metacache.FlushSourceUnknown,
		},
	})
	wbManager.EXPECT().WaitGrowingFlushDrained(mock.Anything, "vchannel", []int64{}).Return(nil)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	assert.NoError(t, preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", releaseSegmentIDs))
}

func TestReleaseManualFlushPreparerFencesEmptyInitialSegments(t *testing.T) {
	ctx := context.Background()
	affectedSegmentIDs := []int64{1002}

	manager, wbManager := newDrainReadyMocks(t, affectedSegmentIDs, affectedSegmentIDs, drainReadyGrowingProgress(1002))
	wbManager.EXPECT().WaitGrowingFlushDrained(mock.Anything, "vchannel", []int64{1002}).Return(nil)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	assert.NoError(t, preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", nil))
}

func TestReleaseManualFlushPreparerSkipNonGrowingSource(t *testing.T) {
	ctx := context.Background()
	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(false, true)

	preparer := NewReleaseManualFlushPreparer(mock_walmanager.NewMockManager(t), wbManager)
	assert.NoError(t, preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001}))
}

// A channel ABSENT from the buffer manager is not "growing-source disabled":
// DropChannel detaches the buffer from the map before its long final Close, so
// the buffer may still be alive and owed inside that window. The prepare must
// NOT succeed silently — it must surface ChannelNotFound, which the QueryNode
// release guards classify as a transient refusal (never structural, see
// growingflush.IsPrepareStructurallyUnavailable) so the release is retried.
func TestReleaseManualFlushPreparerDetachedChannelRefused(t *testing.T) {
	ctx := context.Background()

	extra, err := anypb.New(&message.ManualFlushExtraResponse{})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		TimeTick: 200,
		Extra:    extra,
	}, nil)
	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil)

	wbManager := writebuffer.NewMockBufferManager(t)
	// found=false: the channel is not in the manager's map.
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(false, false)
	wbManager.EXPECT().FenceGrowingSourceAdmission("vchannel").Return()
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001}).
		Return(nil, merr.WrapErrChannelNotFound("vchannel"))
	// WaitGrowingFlushDrained intentionally has no expectation: a call fails the test.

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	err = preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.Error(t, err, "detached channel must NOT be treated as a safe skip")
	assert.ErrorIs(t, err, merr.ErrChannelNotFound)
}

// Partial-release counterpart of the detached-channel case: found=false must
// fall through to the progress check and propagate its ChannelNotFound instead
// of answering "no debt".
func TestReleaseSegmentsPreparerDetachedChannelRefused(t *testing.T) {
	ctx := context.Background()

	manager := mock_walmanager.NewMockManager(t)
	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(false, false)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001}).
		Return(nil, merr.WrapErrChannelNotFound("vchannel"))
	// GetAvailableWAL intentionally has no expectation: no nudge may be appended
	// before the debt question is answerable.

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	pending, err := preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.Error(t, err, "detached channel must NOT be treated as a safe skip")
	assert.ErrorIs(t, err, merr.ErrChannelNotFound)
	assert.False(t, pending)
}

// newDrainReadyMocks wires the happy path up to (and including) the progress
// snapshot, so tests can vary only the drain outcome. extraSegmentIDs feeds
// the ManualFlush append result, progressSegmentIDs is the id set the progress
// snapshot must be asked for, and progress is its answer.
func newDrainReadyMocks(t *testing.T, extraSegmentIDs, progressSegmentIDs []int64, progress []writebuffer.GrowingFlushSegmentProgress) (*mock_walmanager.MockManager, *writebuffer.MockBufferManager) {
	extra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: extraSegmentIDs})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		TimeTick: 200,
		Extra:    extra,
	}, nil)

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().FenceGrowingSourceAdmission("vchannel").Return()
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", progressSegmentIDs).
		Return(progress, nil)
	return manager, wbManager
}

// drainReadyGrowingProgress is the canonical single-segment answer of the
// progress snapshot: one growing-source segment owing a release handoff.
func drainReadyGrowingProgress(segmentID int64) []writebuffer.GrowingFlushSegmentProgress {
	return []writebuffer.GrowingFlushSegmentProgress{
		{
			SegmentID:          segmentID,
			FlushThroughTs:     10,
			NeedReleaseHandoff: true,
			SourceMode:         metacache.FlushSourceGrowing,
		},
	}
}

// TestReleaseManualFlushPreparerDrainErrorFailsRelease pins the contract of
// snapshotAndDrain: the wait is what makes the release safe, so an error from
// WaitGrowingFlushDrained MUST fail PrepareReleaseManualFlush. Swallowing it
// would let the caller unsubscribe the channel while a growing-source flush is
// still in flight, permanently stranding the rows it owns.
func TestReleaseManualFlushPreparerDrainErrorFailsRelease(t *testing.T) {
	ctx := context.Background()
	drainErr := errors.New("drain failed")

	manager, wbManager := newDrainReadyMocks(t, nil, []int64{1001}, drainReadyGrowingProgress(1001))
	wbManager.EXPECT().WaitGrowingFlushDrained(mock.Anything, "vchannel", []int64{1001}).Return(drainErr)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.ErrorIs(t, err, drainErr)
}

func TestReleaseManualFlushPreparerDrainCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	manager, wbManager := newDrainReadyMocks(t, nil, []int64{1001}, drainReadyGrowingProgress(1001))
	wbManager.EXPECT().
		WaitGrowingFlushDrained(mock.Anything, "vchannel", []int64{1001}).
		RunAndReturn(func(ctx context.Context, vchannel string, segmentIDs []int64) error {
			return ctx.Err()
		})

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.ErrorIs(t, err, context.Canceled)
}

// The following tests pin the load-bearing early-error paths: each error must
// propagate AND must stop the flow before the drain step. Reaching the drain
// without a fence message in the WAL could block until the caller's deadline.
// The absence of expectations on the later mocks makes any such call fail the
// test.

func TestReleaseManualFlushPreparerGetAvailableWALError(t *testing.T) {
	ctx := context.Background()
	walErr := errors.New("wal unavailable")

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(nil, walErr)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().FenceGrowingSourceAdmission("vchannel").Return()

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.ErrorIs(t, err, walErr)
}

func TestReleaseManualFlushPreparerAppendError(t *testing.T) {
	ctx := context.Background()
	appendErr := errors.New("append failed")

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).Return(nil, appendErr)

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().FenceGrowingSourceAdmission("vchannel").Return()

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	err := preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.ErrorIs(t, err, appendErr)
}

func TestReleaseManualFlushPreparerGetProgressError(t *testing.T) {
	ctx := context.Background()
	progressErr := errors.New("progress failed")

	extra, err := anypb.New(&message.ManualFlushExtraResponse{})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		TimeTick: 200,
		Extra:    extra,
	}, nil)

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().FenceGrowingSourceAdmission("vchannel").Return()
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001}).
		Return(nil, progressErr)
	// WaitGrowingFlushDrained intentionally has no expectation: a call fails the test.

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	err = preparer.PrepareReleaseManualFlush(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.ErrorIs(t, err, progressErr)
}

// PrepareReleaseSegments is the partial-release guard: it must report the debt
// of the REQUESTED segments only, nudge the flush with a ManualFlush, and never
// fence admission or wait for the drain — the channel it is called on stays
// subscribed, so both of those would damage the surviving partitions (the fence
// is channel-wide and only reopens on re-subscription) or block the RPC.
func TestReleaseSegmentsPreparerPendingNudgesWithoutFenceOrDrain(t *testing.T) {
	ctx := context.Background()

	extra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: []int64{1001, 1002}})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		flushMsg, err := message.AsMutableManualFlushMessageV2(msg)
		if err != nil {
			return false
		}
		return flushMsg.VChannel() == "vchannel" && flushMsg.Header().GetCollectionId() == 10
	})).Return(&types.AppendResult{TimeTick: 200, Extra: extra}, nil)

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001}).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{SegmentID: 1001, NeedReleaseHandoff: true, SourceMode: metacache.FlushSourceGrowing},
		}, nil)
	// FenceGrowingSourceAdmission and WaitGrowingFlushDrained intentionally have
	// no expectation: a call to either fails the test.

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	pending, err := preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.NoError(t, err)
	assert.True(t, pending)
}

// GetGrowingFlushProgress unions the requested ids with every tracked
// growing-source segment of the channel. A sibling segment that owes a flush but
// is NOT being released must not block this release.
func TestReleaseSegmentsPreparerIgnoresUnrequestedSegments(t *testing.T) {
	ctx := context.Background()

	manager := mock_walmanager.NewMockManager(t)
	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001}).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{SegmentID: 1001, NeedReleaseHandoff: false, SourceMode: metacache.FlushSourceGrowing},
			{SegmentID: 2002, NeedReleaseHandoff: true, SourceMode: metacache.FlushSourceGrowing},
		}, nil)
	// GetAvailableWAL intentionally has no expectation: nothing may be appended
	// when the released segments owe nothing.

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	pending, err := preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.NoError(t, err)
	assert.False(t, pending)
}

func TestReleaseSegmentsPreparerSkipsNonGrowingSourceChannel(t *testing.T) {
	ctx := context.Background()

	manager := mock_walmanager.NewMockManager(t)
	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(false, true)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	pending, err := preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.NoError(t, err)
	assert.False(t, pending)
}

// A progress error must surface, not be swallowed into "no debt": the caller
// treats a non-structural error as "retry", which keeps the segments alive.
func TestReleaseSegmentsPreparerProgressError(t *testing.T) {
	ctx := context.Background()
	progressErr := errors.New("progress failed")

	manager := mock_walmanager.NewMockManager(t)
	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001}).
		Return(nil, progressErr)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	pending, err := preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.ErrorIs(t, err, progressErr)
	assert.False(t, pending)
}

// The guard's caller retries on the coordinator's cadence, so a debt that does
// not settle would otherwise append one collection-scoped ManualFlush per retry
// for as long as the flush is stuck. Within the interval only the first check
// appends — but every check still reports pending, because suppressing the
// nudge must never turn into allowing the release.
func TestReleaseSegmentsPreparerRateLimitsNudge(t *testing.T) {
	ctx := context.Background()

	extra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: []int64{1001}})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).
		Return(&types.AppendResult{TimeTick: 200, Extra: extra}, nil).Once()

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil).Once()

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001}).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{SegmentID: 1001, NeedReleaseHandoff: true, SourceMode: metacache.FlushSourceGrowing},
		}, nil)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	preparer.nudgeLimiter.interval = time.Hour

	for i := 0; i < 5; i++ {
		pending, err := preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
		assert.NoError(t, err)
		assert.True(t, pending, "a suppressed nudge must still report the debt")
	}
}

func TestReleaseSegmentsPreparerNudgesAgainAfterInterval(t *testing.T) {
	ctx := context.Background()

	extra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: []int64{1001}})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).
		Return(&types.AppendResult{TimeTick: 200, Extra: extra}, nil).Twice()

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil).Twice()

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001}).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{SegmentID: 1001, NeedReleaseHandoff: true, SourceMode: metacache.FlushSourceGrowing},
		}, nil)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	preparer.nudgeLimiter.interval = 50 * time.Millisecond

	pending, err := preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.NoError(t, err)
	assert.True(t, pending)

	// Suppressed.
	pending, err = preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.NoError(t, err)
	assert.True(t, pending)

	time.Sleep(60 * time.Millisecond)

	pending, err = preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.NoError(t, err)
	assert.True(t, pending)
}

// The limit is per (collection, vchannel): one busy channel must not starve
// another channel's — or another collection's — first nudge.
func TestReleaseSegmentsPreparerRateLimitIsPerCollectionAndChannel(t *testing.T) {
	ctx := context.Background()

	extra, err := anypb.New(&message.ManualFlushExtraResponse{})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).
		Return(&types.AppendResult{TimeTick: 200, Extra: extra}, nil).Times(3)

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil).Times(3)

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush(mock.Anything).Return(true, true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, mock.Anything, []int64{1001}).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{SegmentID: 1001, NeedReleaseHandoff: true, SourceMode: metacache.FlushSourceGrowing},
		}, nil)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	preparer.nudgeLimiter.interval = time.Hour

	pchannel := types.PChannelInfo{Name: "pchannel", Term: 1}
	for _, target := range []struct {
		collectionID int64
		vchannel     string
	}{
		{10, "vchannel-a"},
		{10, "vchannel-b"},
		{20, "vchannel-a"},
		// repeats of the three above must all be suppressed
		{10, "vchannel-a"},
		{10, "vchannel-b"},
		{20, "vchannel-a"},
	} {
		pending, err := preparer.PrepareReleaseSegments(ctx, pchannel, target.collectionID, target.vchannel, []int64{1001})
		assert.NoError(t, err)
		assert.True(t, pending)
	}
}

// The nudge is best effort: a failed append consumes the interval's slot with
// no compensation. That is harmless — the next check within the interval still
// reports the debt (release stays refused) without appending, and the next
// interval re-nudges.
func TestReleaseSegmentsPreparerFailedNudgeWastesTheInterval(t *testing.T) {
	ctx := context.Background()
	appendErr := errors.New("append failed")

	extra, err := anypb.New(&message.ManualFlushExtraResponse{})
	assert.NoError(t, err)

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Append(mock.Anything, mock.Anything).Return(nil, appendErr).Once()
	wal.EXPECT().Append(mock.Anything, mock.Anything).
		Return(&types.AppendResult{TimeTick: 200, Extra: extra}, nil).Once()

	manager := mock_walmanager.NewMockManager(t)
	manager.EXPECT().GetAvailableWAL(mock.Anything).Return(wal, nil).Twice()

	wbManager := writebuffer.NewMockBufferManager(t)
	wbManager.EXPECT().AllowGrowingSourceFlush("vchannel").Return(true, true)
	wbManager.EXPECT().
		GetGrowingFlushProgress(mock.Anything, "vchannel", []int64{1001}).
		Return([]writebuffer.GrowingFlushSegmentProgress{
			{SegmentID: 1001, NeedReleaseHandoff: true, SourceMode: metacache.FlushSourceGrowing},
		}, nil)

	preparer := NewReleaseManualFlushPreparer(manager, wbManager)
	preparer.nudgeLimiter.interval = 50 * time.Millisecond

	pending, err := preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.ErrorIs(t, err, appendErr)
	assert.False(t, pending)

	// Within the interval the failed nudge is not retried, but the debt is
	// still reported.
	pending, err = preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.NoError(t, err)
	assert.True(t, pending)

	// The next interval re-nudges.
	time.Sleep(60 * time.Millisecond)
	pending, err = preparer.PrepareReleaseSegments(ctx, types.PChannelInfo{Name: "pchannel", Term: 1}, 10, "vchannel", []int64{1001})
	assert.NoError(t, err)
	assert.True(t, pending)
}
