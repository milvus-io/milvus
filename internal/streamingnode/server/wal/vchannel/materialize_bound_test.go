package vchannel

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// recordingMaterializer records the materialize calls it receives.
type recordingMaterializer struct {
	mu      sync.Mutex
	batches []MaterializeBatch
}

type MaterializeBatch struct {
	TargetTimeTick uint64
	TimeTicks      []uint64
}

func (m *recordingMaterializer) Materialize(_ context.Context, req transformlog.MaterializeRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	batch := MaterializeBatch{TargetTimeTick: req.TargetTimeTick}
	for _, entry := range req.Entries {
		batch.TimeTicks = append(batch.TimeTicks, entry.GetTimeTick())
	}
	m.batches = append(m.batches, batch)
	return nil
}

// newMaterializeBoundTestModule builds a module whose summary view has staged
// transform records at the given timeticks, ready for materialization.
func newMaterializeBoundTestModule(t *testing.T, scheduler *recordingVChannelScheduler, segmentMetas map[int64]*streamingpb.SegmentAssignmentMeta, timeticks ...uint64) *VChannelRecoveryModule {
	t.Helper()
	module, err := NewModule(ModuleConfig{
		PChannel:                 "p1",
		VChannel:                 "v1",
		VChannelMeta:             &streamingpb.VChannelMeta{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
		Segments:                 segmentMetas,
		TransformLogMaterializer: &recordingMaterializer{},
		Runtime:                  moduleapi.Runtime{Scheduler: scheduler},
	})
	require.NoError(t, err)
	for _, timetick := range timeticks {
		observeVChannelDelete(t, module, "v1", timetick)
	}
	return module
}

func TestVChannelAdvancesTransformMaterializationAfterL1Commit(t *testing.T) {
	ctx := context.Background()
	scheduler := &recordingVChannelScheduler{}
	segmentMetas := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: newMaterializationBlockerMeta(1, 100, false),
		2: newMaterializationBlockerMeta(2, 200, false),
	}
	module := newMaterializeBoundTestModule(t, scheduler, segmentMetas, 100, 200, 300)

	// Observation schedules the materialize task directly: no barrier or
	// summary flush event drives it. The L1 upper bound (min create tick of
	// the uncommitted segments, 100) caps the first batch.
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	assert.Equal(t, uint64(100), module.transformLog.MaterializedTimeTick())

	first := segment.NewSegmentViewFromMetaWithConfig(
		newMaterializationBlockerMeta(1, 100, true),
		nil,
		module.segmentViewConfig(),
	)
	module.mu.Lock()
	module.segments[1] = first
	module.mu.Unlock()
	module.SegmentDataUpdated(1, first)
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	assert.Equal(t, uint64(200), module.transformLog.MaterializedTimeTick())

	second := segment.NewSegmentViewFromMetaWithConfig(
		newMaterializationBlockerMeta(2, 200, true),
		nil,
		module.segmentViewConfig(),
	)
	module.mu.Lock()
	module.segments[2] = second
	module.mu.Unlock()
	module.SegmentDataUpdated(2, second)
	require.Len(t, scheduler.tasks, 3)
	require.NoError(t, scheduler.tasks[2].Execute(ctx))
	assert.Equal(t, uint64(300), module.transformLog.MaterializedTimeTick())

	// The frontier is mirrored into the vchannel meta for the next checkpoint.
	module.mu.Lock()
	assert.Equal(t, uint64(300), module.vchannelView.AssignmentMeta().GetTransformMaterializedTimeTick())
	module.mu.Unlock()
}

func newMaterializationBlockerMeta(segmentID int64, createTimeTick uint64, l1CommitDone bool) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		PartitionId:  segmentID,
		SegmentId:    segmentID,
		Vchannel:     "v1",
		L1CommitDone: l1CommitDone,
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: createTimeTick,
		},
	}
}

func TestVChannelMaterializeBoundAdvancesAfterSegmentCleanup(t *testing.T) {
	ctx := context.Background()
	scheduler := &recordingVChannelScheduler{}
	segmentMetas := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: newMaterializationBlockerMeta(1, 100, false),
		2: newMaterializationBlockerMeta(2, 200, false),
	}
	module := newMaterializeBoundTestModule(t, scheduler, segmentMetas, 100, 200, 300)

	// Observation drives the first batch; the bound pins it at 100.
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	assert.Equal(t, uint64(100), module.transformLog.MaterializedTimeTick())

	// Segment 1 is cleaned up (snapshot persisted and completeSegmentCleanup
	// invoked); its create tick must stop pinning the bound.
	first := module.segments[1]
	module.mu.Lock()
	if module.pendingCleanup == nil {
		module.pendingCleanup = make(map[int64]*segment.SegmentView)
	}
	module.pendingCleanup[1] = first
	module.mu.Unlock()
	module.completeSegmentCleanup(1, first)
	require.Equal(t, uint64(200), module.materializeUpperBound)

	// The advance releases a new materialize task up to the next blocker.
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	assert.Equal(t, uint64(200), module.transformLog.MaterializedTimeTick())
}

func TestVChannelMaterializeBoundRetractsOnNewBlocker(t *testing.T) {
	segmentMetas := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: newMaterializationBlockerMeta(1, 100, false),
	}
	module, err := NewModule(ModuleConfig{
		PChannel:     "p1",
		VChannel:     "v1",
		VChannelMeta: &streamingpb.VChannelMeta{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
		Segments:     segmentMetas,
		Runtime:      moduleapi.Runtime{},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(100), module.materializeUpperBound)

	// A new L1 segment with an earlier create tick retracts the bound.
	blocker := segment.NewSegmentViewFromMetaWithConfig(
		newMaterializationBlockerMeta(3, 50, false),
		nil,
		module.segmentViewConfig(),
	)
	module.mu.Lock()
	module.segments[3] = blocker
	module.mu.Unlock()
	module.SegmentDataUpdated(3, blocker)
	require.Equal(t, uint64(50), module.materializeUpperBound)
}

func TestVChannelConcurrentDataUpdateAndBlockerScan(t *testing.T) {
	segmentMetas := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: newMaterializationBlockerMeta(1, 100, false),
		2: newMaterializationBlockerMeta(2, 200, false),
	}
	module, err := NewModule(ModuleConfig{
		PChannel:     "p1",
		VChannel:     "v1",
		VChannelMeta: &streamingpb.VChannelMeta{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
		Segments:     segmentMetas,
		Runtime:      moduleapi.Runtime{},
	})
	require.NoError(t, err)
	view := module.segments[1]

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			module.SegmentDataUpdated(1, view)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			view.L1MaterializationBlockerTimeTick()
		}
	}()
	wg.Wait()
}
