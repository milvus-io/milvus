//go:build test

package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// newTestRecoveryStorage creates a recoveryStorageImpl with basic setup for unit testing.
func newTestRecoveryStorage(t *testing.T) *recoveryStorageImpl {
	paramtable.Init()
	resource.InitForTest(t)
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(0),
		TimeTick:  0,
	})
	rs.segments = make(map[int64]*segmentRecoveryInfo)
	rs.vchannels = make(map[string]*vchannelRecoveryInfo)
	return rs
}

// addActiveVChannel adds an active vchannel to the recovery storage.
func addActiveVChannel(rs *recoveryStorageImpl, vchannel string, collectionID int64, partitionIDs []int64) {
	partitions := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(partitionIDs))
	for _, pid := range partitionIDs {
		partitions = append(partitions, &streamingpb.PartitionInfoOfVChannel{PartitionId: pid})
	}
	rs.vchannels[vchannel] = &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: vchannel,
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: collectionID,
				Partitions:   partitions,
			},
		},
	}
}

// addDroppedVChannel adds a DROPPED vchannel to the recovery storage.
func addDroppedVChannel(rs *recoveryStorageImpl, vchannel string, collectionID int64) {
	rs.vchannels[vchannel] = &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: vchannel,
			State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: collectionID,
			},
		},
	}
}

// addGrowingSegment adds a growing segment to the recovery storage.
func addGrowingSegment(rs *recoveryStorageImpl, segmentID, collectionID, partitionID int64, vchannel string) {
	rs.segments[segmentID] = &segmentRecoveryInfo{
		meta: &streamingpb.SegmentAssignmentMeta{
			CollectionId:   collectionID,
			PartitionId:    partitionID,
			SegmentId:      segmentID,
			Vchannel:       vchannel,
			State:          streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			StorageVersion: 1,
			Stat: &streamingpb.SegmentAssignmentStat{
				MaxBinarySize:         1024,
				CreateSegmentTimeTick: 10,
			},
		},
		dirty: true,
	}
}

// buildDropCollectionMsg builds a DropCollection immutable message.
func buildDropCollectionMsg(vchannel string, collectionID int64, timetick uint64, msgID int64) message.ImmutableDropCollectionMessageV1 {
	msg := message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(msgID)).
		IntoImmutableMessage(rmq.NewRmqID(msgID))
	return message.MustAsImmutableDropCollectionMessageV1(msg)
}

func TestHandleDropCollection_VChannelAlreadyDropped_FlushesOrphanedSegments(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// Set up: vchannel is already DROPPED (from a prior persist).
	addDroppedVChannel(rs, "v1", 100)

	// Orphaned GROWING segments were recreated during WAL replay (from CreateSegment messages
	// that appear before the DropCollection in the WAL).
	addGrowingSegment(rs, 1001, 100, 200, "v1")
	addGrowingSegment(rs, 1002, 100, 201, "v1")

	// Also add a segment from a different collection — should NOT be flushed.
	addActiveVChannel(rs, "v2", 101, []int64{300})
	addGrowingSegment(rs, 2001, 101, 300, "v2")

	// Replay DropCollection for the already-dropped vchannel.
	dropMsg := buildDropCollectionMsg("v1", 100, 50, 50)
	rs.handleDropCollection(dropMsg)

	// Verify: orphaned segments for collection 100 are flushed.
	assert.False(t, rs.segments[1001].IsGrowing(), "segment 1001 should be flushed")
	assert.False(t, rs.segments[1002].IsGrowing(), "segment 1002 should be flushed")

	// Verify: segment from different collection is still growing.
	assert.True(t, rs.segments[2001].IsGrowing(), "segment 2001 should still be growing")
}

func TestHandleDropCollection_VChannelNotFound_FlushesOrphanedSegments(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// vchannel does not exist at all (cleaned from etcd).
	// But orphaned GROWING segments exist from WAL replay.
	addGrowingSegment(rs, 1001, 100, 200, "v1")

	dropMsg := buildDropCollectionMsg("v1", 100, 50, 50)
	rs.handleDropCollection(dropMsg)

	// Segment should be flushed even though vchannel doesn't exist.
	assert.False(t, rs.segments[1001].IsGrowing(), "segment 1001 should be flushed")
}

func TestHandleDropCollection_NormalCase_StillWorks(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// Normal case: vchannel is ACTIVE with segments.
	addActiveVChannel(rs, "v1", 100, []int64{200})
	addGrowingSegment(rs, 1001, 100, 200, "v1")

	dropMsg := buildDropCollectionMsg("v1", 100, 50, 50)
	rs.handleDropCollection(dropMsg)

	// vchannel should be marked as DROPPED.
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, rs.vchannels["v1"].meta.State)

	// Segment should be flushed.
	assert.False(t, rs.segments[1001].IsGrowing(), "segment 1001 should be flushed")
}

func TestGetSnapshot_FiltersOrphanedSegments(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// Active vchannel with a growing segment.
	addActiveVChannel(rs, "v1", 100, []int64{200})
	addGrowingSegment(rs, 1001, 100, 200, "v1")

	// Dropped vchannel with an orphaned growing segment.
	addDroppedVChannel(rs, "v2", 101)
	addGrowingSegment(rs, 2001, 101, 300, "v2")

	// Growing segment with no vchannel at all (vchannel cleaned from etcd).
	addGrowingSegment(rs, 3001, 102, 400, "v3")

	snapshot := rs.getSnapshot()

	// Only the segment for the active vchannel should be in the snapshot.
	assert.Len(t, snapshot.VChannels, 1)
	assert.Contains(t, snapshot.VChannels, "v1")

	assert.Len(t, snapshot.SegmentAssignments, 1)
	assert.Contains(t, snapshot.SegmentAssignments, int64(1001))

	// Orphaned segments should NOT be in the snapshot.
	assert.NotContains(t, snapshot.SegmentAssignments, int64(2001))
	assert.NotContains(t, snapshot.SegmentAssignments, int64(3001))
}

func TestGetSnapshot_FiltersSegmentsWithDroppedPartition(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// Active vchannel with partitions 200 and 201.
	addActiveVChannel(rs, "v1", 100, []int64{200, 201})

	// Segment on active partition — should be kept.
	addGrowingSegment(rs, 1001, 100, 200, "v1")

	// Segment on another active partition — should be kept.
	addGrowingSegment(rs, 1002, 100, 201, "v1")

	// Segment on a dropped partition (999 not in vchannel's partition list) — should be filtered.
	addGrowingSegment(rs, 1003, 100, 999, "v1")

	snapshot := rs.getSnapshot()

	assert.Len(t, snapshot.SegmentAssignments, 2)
	assert.Contains(t, snapshot.SegmentAssignments, int64(1001))
	assert.Contains(t, snapshot.SegmentAssignments, int64(1002))
	assert.NotContains(t, snapshot.SegmentAssignments, int64(1003))
}

func TestHandleCreateSegment_SkipsForDroppedVChannel(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// vchannel is DROPPED.
	addDroppedVChannel(rs, "v1", 100)

	// Try to create a segment on the dropped vchannel.
	createMsg := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   100,
			SegmentId:      1001,
			PartitionId:    200,
			StorageVersion: 1,
			MaxSegmentSize: 1024,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(50).
		WithLastConfirmed(rmq.NewRmqID(50)).
		IntoImmutableMessage(rmq.NewRmqID(50))

	rs.handleCreateSegment(message.MustAsImmutableCreateSegmentMessageV2(createMsg))

	// Segment should NOT have been created.
	assert.Empty(t, rs.segments, "no segment should be created for a dropped vchannel")
}

func TestHandleCreateSegment_SkipsForNonExistentVChannel(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// No vchannel exists at all.
	createMsg := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v_nonexistent").
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   100,
			SegmentId:      1001,
			PartitionId:    200,
			StorageVersion: 1,
			MaxSegmentSize: 1024,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(50).
		WithLastConfirmed(rmq.NewRmqID(50)).
		IntoImmutableMessage(rmq.NewRmqID(50))

	rs.handleCreateSegment(message.MustAsImmutableCreateSegmentMessageV2(createMsg))

	// Segment should NOT have been created.
	assert.Empty(t, rs.segments, "no segment should be created for a non-existent vchannel")
}

func TestHandleCreateSegment_NormalCase_StillWorks(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// Active vchannel.
	addActiveVChannel(rs, "v1", 100, []int64{200})

	createMsg := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   100,
			SegmentId:      1001,
			PartitionId:    200,
			StorageVersion: 1,
			MaxSegmentSize: 1024,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(50).
		WithLastConfirmed(rmq.NewRmqID(50)).
		IntoImmutableMessage(rmq.NewRmqID(50))

	rs.handleCreateSegment(message.MustAsImmutableCreateSegmentMessageV2(createMsg))

	// Segment should be created normally.
	assert.Len(t, rs.segments, 1)
	assert.Contains(t, rs.segments, int64(1001))
	assert.True(t, rs.segments[1001].IsGrowing())
}

func TestFullReplayScenario_DroppedCollectionReplay(t *testing.T) {
	// Simulates the full bug scenario: Kafka offset reset causes WAL replay
	// of CreateCollection → CreateSegment → Insert → DropCollection for a
	// collection that was already dropped and cleaned from etcd.
	rs := newTestRecoveryStorage(t)

	// Step 1: CreateCollection replayed (vchannel re-created)
	createCollMsg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 100,
			PartitionIds: []int64{200},
		}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(rmq.NewRmqID(10)).
		IntoImmutableMessage(rmq.NewRmqID(10))
	rs.handleCreateCollection(message.MustAsImmutableCreateCollectionMessageV1(createCollMsg))

	// Step 2: CreateSegment replayed
	createSegMsg := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   100,
			SegmentId:      1001,
			PartitionId:    200,
			StorageVersion: 1,
			MaxSegmentSize: 1024,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(20).
		WithLastConfirmed(rmq.NewRmqID(20)).
		IntoImmutableMessage(rmq.NewRmqID(20))
	rs.handleCreateSegment(message.MustAsImmutableCreateSegmentMessageV2(createSegMsg))

	// Step 3: DropCollection replayed — should flush the segment and mark vchannel dropped.
	dropMsg := buildDropCollectionMsg("v1", 100, 30, 30)
	rs.handleDropCollection(dropMsg)

	// After drop: vchannel is DROPPED, segment is FLUSHED.
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, rs.vchannels["v1"].meta.State)
	assert.False(t, rs.segments[1001].IsGrowing())

	// Snapshot should be empty (no active vchannels, no growing segments).
	snapshot := rs.getSnapshot()
	assert.Empty(t, snapshot.VChannels)
	assert.Empty(t, snapshot.SegmentAssignments)
}

func TestFullReplayScenario_PartialEtcdPersist(t *testing.T) {
	// Simulates: vchannel marked DROPPED in etcd (saved), but segments NOT saved as FLUSHED.
	// On recovery, WAL replay starts from old checkpoint and recreates segments.
	rs := newTestRecoveryStorage(t)

	// State from etcd: vchannel already DROPPED (from prior persist).
	addDroppedVChannel(rs, "v1", 100)

	// WAL replay recreates segments (CreateSegment messages before DropCollection).
	addGrowingSegment(rs, 1001, 100, 200, "v1")
	addGrowingSegment(rs, 1002, 100, 201, "v1")

	// Then DropCollection is replayed again.
	dropMsg := buildDropCollectionMsg("v1", 100, 50, 50)
	rs.handleDropCollection(dropMsg)

	// All segments should be flushed.
	assert.False(t, rs.segments[1001].IsGrowing())
	assert.False(t, rs.segments[1002].IsGrowing())

	// Snapshot should be clean.
	snapshot := rs.getSnapshot()
	assert.Empty(t, snapshot.VChannels)
	assert.Empty(t, snapshot.SegmentAssignments)
}
