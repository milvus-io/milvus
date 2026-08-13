package writebuffer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// SealCheckpointPinSuite covers the [seal -> sync task construction] window: a
// segment whose write buffer was already drained by an earlier periodic flush is
// sealed by a ManualFlush fence, and nothing else pins the channel checkpoint
// until the terminal metadata-only task exists.
type SealCheckpointPinSuite struct {
	suite.Suite

	collID      int64
	channelName string
	collSchema  *schemapb.CollectionSchema
	metacache   metacache.MetaCache
	syncMgr     *syncmgr.MockSyncManager
	wb          *writeBufferBase
}

func (s *SealCheckpointPinSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	s.collID = 100
	s.channelName = "by-dev-rootcoord-dml_0v0"
	s.collSchema = pkVectorSchema("seal_pin_collection")
}

func (s *SealCheckpointPinSuite) SetupTest() {
	s.metacache = metacache.NewMetaCache(&datapb.ChannelWatchInfo{
		Schema: s.collSchema,
		Vchan: &datapb.VchannelInfo{
			CollectionID: s.collID,
			ChannelName:  s.channelName,
		},
	}, func(*datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, metacache.NewBM25StatsFactory)
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())

	var err error
	s.wb, err = newWriteBufferBase(s.channelName, s.metacache, s.syncMgr, &writeBufferOption{
		metaWriter: syncmgr.NewMockMetaWriter(s.T()),
	})
	s.Require().NoError(err)
	s.wb.allowGrowingSourceFlush = false
}

// addGrowingSegment registers a drained growing segment: it has rows in binlogs
// already (an earlier periodic flush wrote them) and nothing left in the write
// buffer.
func (s *SealCheckpointPinSuite) addGrowingSegment(segmentID int64) {
	s.metacache.AddSegment(&datapb.SegmentInfo{
		ID:            segmentID,
		PartitionID:   10,
		CollectionID:  s.collID,
		InsertChannel: s.channelName,
		State:         commonpb.SegmentState_Growing,
	}, func(*datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, metacache.NewBM25StatsFactory)
}

func (s *SealCheckpointPinSuite) setConsumedCheckpoint(ts uint64) {
	s.wb.mut.Lock()
	defer s.wb.mut.Unlock()
	s.wb.checkpoint = &msgpb.MsgPosition{ChannelName: s.channelName, Timestamp: ts}
}

// TestSealPinsCheckpointBeforeTaskExists is the reproduction: the fence sealed
// the segment, but no sync task was built yet. Nothing may let the channel
// checkpoint advance past the fence position in this window.
func (s *SealCheckpointPinSuite) TestSealPinsCheckpointBeforeTaskExists() {
	const segmentID = int64(2001)
	s.addGrowingSegment(segmentID)
	s.setConsumedCheckpoint(100)

	s.Require().NoError(s.wb.SealSegments(context.Background(), []int64{segmentID}))

	// Timeticks keep flowing; no sync round has run.
	s.setConsumedCheckpoint(500)

	s.EqualValues(100, s.wb.GetCheckpoint().GetTimestamp(),
		"checkpoint must not advance past the fence that sealed the segment")
}

// TestPinHoldsAcrossTaskLifetime covers the in-flight terminal task and a
// retryable failure of it: neither may release the fence pin.
func (s *SealCheckpointPinSuite) TestPinHoldsAcrossTaskLifetime() {
	const segmentID = int64(2002)
	s.addGrowingSegment(segmentID)
	s.setConsumedCheckpoint(100)
	s.Require().NoError(s.wb.SealSegments(context.Background(), []int64{segmentID}))
	s.setConsumedCheckpoint(500)

	s.wb.mut.Lock()
	task, err := s.wb.getSyncTask(context.Background(), segmentID)
	s.Require().NoError(err)
	syncTask := task.(*syncmgr.SyncTask)
	entry := s.wb.writeBufferSyncEntryLocked(syncTask)
	s.Require().NotNil(entry)
	s.wb.mut.Unlock()

	s.True(syncTask.IsFlush())
	s.EqualValues(100, s.wb.GetCheckpoint().GetTimestamp(), "in-flight terminal task keeps the fence pinned")

	// Retryable failure: the flush still owes a commit, so the pin stays.
	err = s.wb.finishWriteBufferSync(context.Background(), entry, syncTask, merr.WrapErrIoFailedReason("transient s3 failure"))
	s.Error(err)
	s.setConsumedCheckpoint(900)
	s.EqualValues(100, s.wb.GetCheckpoint().GetTimestamp(), "a retryable failure keeps the fence pinned")

	segment, ok := s.metacache.GetSegmentByID(segmentID)
	s.Require().True(ok)
	s.Equal(commonpb.SegmentState_Flushing, segment.State())
	s.EqualValues(100, segment.PendingFlushCheckpoint().GetTimestamp())
}

// TestPinReleasedOnFlushCommit: the flush commit removes the segment from the
// metacache, so the derived candidate disappears with it.
func (s *SealCheckpointPinSuite) TestPinReleasedOnFlushCommit() {
	const segmentID = int64(2003)
	s.addGrowingSegment(segmentID)
	s.setConsumedCheckpoint(100)
	s.Require().NoError(s.wb.SealSegments(context.Background(), []int64{segmentID}))

	s.wb.mut.Lock()
	task, err := s.wb.getSyncTask(context.Background(), segmentID)
	s.Require().NoError(err)
	syncTask := task.(*syncmgr.SyncTask)
	entry := s.wb.writeBufferSyncEntryLocked(syncTask)
	s.Require().NotNil(entry)
	s.wb.mut.Unlock()

	s.setConsumedCheckpoint(500)
	s.Require().NoError(s.wb.finishWriteBufferSync(context.Background(), entry, syncTask, nil))

	_, ok := s.metacache.GetSegmentByID(segmentID)
	s.False(ok, "a committed flush removes the segment")
	s.EqualValues(500, s.wb.GetCheckpoint().GetTimestamp(), "the checkpoint advances once the flush is durable")
}

// TestPinReleasedOnDrop: a dropped segment owes no flush any more, so the
// derived candidate stops matching without anyone unregistering it.
func (s *SealCheckpointPinSuite) TestPinReleasedOnDrop() {
	const segmentID = int64(2004)
	s.addGrowingSegment(segmentID)
	s.setConsumedCheckpoint(100)
	s.Require().NoError(s.wb.SealSegments(context.Background(), []int64{segmentID}))
	s.setConsumedCheckpoint(500)
	s.EqualValues(100, s.wb.GetCheckpoint().GetTimestamp())

	s.wb.mut.Lock()
	s.wb.dropPartitions([]int64{10})
	s.wb.mut.Unlock()

	segment, ok := s.metacache.GetSegmentByID(segmentID)
	s.Require().True(ok)
	s.Equal(commonpb.SegmentState_Dropped, segment.State())
	s.EqualValues(500, s.wb.GetCheckpoint().GetTimestamp(), "a dropped segment owes no flush and must not pin")
}

// TestReSealDoesNotMovePinLater: the earliest un-committed fence is the one
// recovery has to replay from.
func (s *SealCheckpointPinSuite) TestReSealDoesNotMovePinLater() {
	const segmentID = int64(2005)
	s.addGrowingSegment(segmentID)
	s.setConsumedCheckpoint(100)
	s.Require().NoError(s.wb.SealSegments(context.Background(), []int64{segmentID}))

	s.setConsumedCheckpoint(700)
	s.Require().NoError(s.wb.SealSegments(context.Background(), []int64{segmentID}))

	segment, ok := s.metacache.GetSegmentByID(segmentID)
	s.Require().True(ok)
	s.EqualValues(100, segment.PendingFlushCheckpoint().GetTimestamp())
	s.EqualValues(100, s.wb.GetCheckpoint().GetTimestamp(), "a re-seal must not push the pin forward")
}

// TestSealAllSegmentsPins: FlushAll / AlterWAL seal every growing segment and
// have the same crash window as a targeted seal.
func (s *SealCheckpointPinSuite) TestSealAllSegmentsPins() {
	const segmentID = int64(2006)
	s.addGrowingSegment(segmentID)
	s.setConsumedCheckpoint(100)

	s.wb.SealAllSegments(context.Background())

	s.setConsumedCheckpoint(500)
	s.EqualValues(100, s.wb.GetCheckpoint().GetTimestamp())
}

// TestNoStalePinAfterSequenceOfOps is the leak invariant: after seal/flush,
// seal/drop and a close, no checkpoint candidate may reference a segment that is
// no longer in the metacache, and the checkpoint must not be stuck.
func (s *SealCheckpointPinSuite) TestNoStalePinAfterSequenceOfOps() {
	ctx := context.Background()

	runFlush := func(segmentID int64, sealTs, commitTs uint64) {
		s.addGrowingSegment(segmentID)
		s.setConsumedCheckpoint(sealTs)
		s.Require().NoError(s.wb.SealSegments(ctx, []int64{segmentID}))
		s.wb.mut.Lock()
		task, err := s.wb.getSyncTask(ctx, segmentID)
		s.Require().NoError(err)
		syncTask := task.(*syncmgr.SyncTask)
		entry := s.wb.writeBufferSyncEntryLocked(syncTask)
		s.Require().NotNil(entry)
		s.wb.mut.Unlock()
		s.setConsumedCheckpoint(commitTs)
		s.Require().NoError(s.wb.finishWriteBufferSync(ctx, entry, syncTask, nil))
	}

	// seal + flush success
	runFlush(3001, 100, 200)
	// seal + drop
	s.addGrowingSegment(3002)
	s.setConsumedCheckpoint(300)
	s.Require().NoError(s.wb.SealSegments(ctx, []int64{3002}))
	s.wb.mut.Lock()
	s.wb.dropPartitions([]int64{10})
	s.metacache.RemoveSegments(metacache.WithSegmentIDs(3002))
	s.wb.mut.Unlock()
	// seal + flush success again
	runFlush(3003, 400, 500)

	s.setConsumedCheckpoint(900)

	// No candidate may outlive its segment.
	for _, segment := range s.metacache.GetSegmentsBy() {
		s.Fail("no segment should remain", "segment %d still in metacache", segment.SegmentID())
	}
	s.Nil(earliestReplayOriginForTest(s.wb), "no payload floor may outlive its task")
	s.EqualValues(900, s.wb.GetCheckpoint().GetTimestamp(), "the checkpoint must not be stuck behind a vanished segment")

	s.wb.Close(ctx, false)
	s.EqualValues(900, s.wb.GetCheckpoint().GetTimestamp())
}

// selectSealedSegments is the selection half of a sync round: getSegmentsToSync
// under wb.mut with the sealed-segments policy.
func (s *SealCheckpointPinSuite) selectSealedSegments() []int64 {
	s.wb.mut.Lock()
	defer s.wb.mut.Unlock()
	return s.wb.getSegmentsToSync(s.wb.checkpoint.GetTimestamp(), GetSealedSegmentsPolicy(s.metacache))
}

// runSelectionSyncRound is one production-shaped [selection -> submission]
// round, exactly what a timetick drives: the sealed-segments selection, then
// syncSegments outside the lock for whatever the gate let through.
func (s *SealCheckpointPinSuite) runSelectionSyncRound() []int64 {
	ids := s.selectSealedSegments()
	if len(ids) > 0 {
		s.wb.syncSegments(context.Background(), ids)
	}
	return ids
}

// TestEmptyBufferSealProducesExactlyOneFlushTask pins the invariant directly:
// sealing a segment whose payload is empty (owned mode) produces exactly ONE
// metadata-only flush task — a second selection/sync round run while the first
// task is still in flight must be blocked by the Sealed->Flushing claim plus
// the in-flight queue entry, and after the task commits the debt is settled so
// no further task is ever produced.
func (s *SealCheckpointPinSuite) TestEmptyBufferSealProducesExactlyOneFlushTask() {
	const segmentID = int64(2101)
	s.addGrowingSegment(segmentID)
	s.setConsumedCheckpoint(100)

	// Capture every task handed to the sync manager, but complete NONE of them:
	// the completion callback is held so the first task stays in flight across
	// the second round.
	var built []syncmgr.Task
	var finishCallbacks []func(error) error
	s.syncMgr.EXPECT().SyncData(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			built = append(built, task)
			finishCallbacks = append(finishCallbacks, callbacks...)
			return conc.Go(func() (struct{}, error) { return struct{}{}, nil }), nil
		}).Maybe()

	s.Require().NoError(s.wb.SealSegments(context.Background(), []int64{segmentID}))

	// Round 1 (first timetick): the empty-buffer seal debt becomes one task.
	s.Require().Equal([]int64{segmentID}, s.runSelectionSyncRound())
	s.Require().Len(built, 1, "the seal debt must build exactly one task")
	firstTask, ok := built[0].(*syncmgr.SyncTask)
	s.Require().True(ok, "an owned-mode seal must build an owned-path task")
	s.True(firstTask.IsFlush(), "the empty-buffer seal task must carry the flush flag")

	segment, exists := s.metacache.GetSegmentByID(segmentID)
	s.Require().True(exists)
	s.Equal(commonpb.SegmentState_Flushing, segment.State(),
		"building the task must claim the seal (Sealed -> Flushing)")

	// Round 2 (second timetick), first task NOT completed: the claim plus the
	// in-flight queue entry must block a duplicate.
	s.Empty(s.runSelectionSyncRound(),
		"a claimed in-flight flush must not be re-selected")
	s.Len(built, 1, "no second task may be built while the first is in flight")

	// Complete the one task successfully: the flush debt settles.
	s.Require().Len(finishCallbacks, 1)
	s.Require().NoError(finishCallbacks[0](nil))
	_, exists = s.metacache.GetSegmentByID(segmentID)
	s.False(exists, "a committed flush removes the segment")

	// Round 3: the debt is settled — nothing may produce another task.
	s.Empty(s.runSelectionSyncRound(), "a settled flush debt must not re-trigger")
	s.Len(built, 1, "exactly one task over the whole lifetime")

	s.wb.mut.RLock()
	s.Empty(s.wb.writeBufferSyncQueues, "no residual queue debt may survive the settled flush")
	s.wb.mut.RUnlock()
}

// TestEmptyRefSealInFlightAttemptBlocksSecondTask is the ref-mode twin at the
// selection/build gate: while a growing-source attempt is in flight
// (payload.syncing), a sealed ref-mode segment must be selected by no round and
// a direct build must return no task WITHOUT consuming the Sealed claim; once
// the attempt settles, the debt becomes visible exactly once.
func (s *SealCheckpointPinSuite) TestEmptyRefSealInFlightAttemptBlocksSecondTask() {
	const segmentID = int64(2102)
	s.addGrowingSegment(segmentID)
	s.setConsumedCheckpoint(100)
	seedRefLedgerForTest(s.wb, segmentID, func(p *refPayload) { p.syncing = true })

	s.Require().NoError(s.wb.SealSegments(context.Background(), []int64{segmentID}))

	// Two selection rounds while the attempt is in flight: both must be empty.
	for round := 1; round <= 2; round++ {
		s.Empty(s.selectSealedSegments(), "round %d must not select a segment with an in-flight growing attempt", round)
	}

	// A direct build is refused too, and must not consume the seal claim: the
	// completion callback of the in-flight attempt re-drives from Sealed.
	s.wb.mut.Lock()
	task, err := s.wb.getSyncTask(context.Background(), segmentID)
	s.wb.mut.Unlock()
	s.NoError(err)
	s.Nil(task, "no second task may be built while a growing attempt is in flight")
	segment, exists := s.metacache.GetSegmentByID(segmentID)
	s.Require().True(exists)
	s.Equal(commonpb.SegmentState_Sealed, segment.State(),
		"a refused build must leave the Sealed claim for the re-drive")

	// Attempt settles: the flush debt becomes selectable, exactly once.
	seedRefLedgerForTest(s.wb, segmentID, func(p *refPayload) { p.syncing = false })
	s.Equal([]int64{segmentID}, s.selectSealedSegments(), "the settled attempt must expose the seal debt again")
}

func TestSealCheckpointPin(t *testing.T) {
	suite.Run(t, new(SealCheckpointPinSuite))
}
