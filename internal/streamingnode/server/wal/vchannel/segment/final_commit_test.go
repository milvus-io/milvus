package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestBuildCommitL1SegmentRequestPreservesDurableStorageState(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:       1,
		PartitionId:        2,
		SegmentId:          3,
		Vchannel:           "v1",
		CheckpointTimeTick: 50,
		Stat: &streamingpb.SegmentAssignmentStat{
			ModifiedRows:          10,
			CreateSegmentTimeTick: 20,
		},
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			DeltaBinlog: []*datapb.FieldBinlog{{FieldID: 100}},
			Statistics:  &datapb.Statistics{InsertBinlogSize: 123, DeltaBinlogSize: 45},
		},
	}

	req := buildCommitL1SegmentRequest(10, meta)

	require.Len(t, req.GetDeltalogs(), 1)
	assert.Equal(t, int64(100), req.GetDeltalogs()[0].GetFieldID())
	require.NotNil(t, req.GetStats())
	assert.Equal(t, int64(123), req.GetStats().GetInsertBinlogSize())
	assert.Equal(t, int64(45), req.GetStats().GetDeltaBinlogSize())
	assert.True(t, req.GetWithFullBinlogs())

	// The checkpoint position must be non-nil or DataCoord skips the update
	// and the flushed segment drops out of channel recovery.
	require.Len(t, req.GetCheckPoints(), 1)
	cp := req.GetCheckPoints()[0]
	assert.Equal(t, int64(3), cp.GetSegmentID())
	assert.Equal(t, int64(10), cp.GetNumOfRows())
	require.NotNil(t, cp.GetPosition())
	assert.Equal(t, "v1", cp.GetPosition().GetChannelName())
	assert.Equal(t, uint64(50), cp.GetPosition().GetTimestamp())

	require.Len(t, req.GetStartPositions(), 1)
	sp := req.GetStartPositions()[0]
	assert.Equal(t, int64(3), sp.GetSegmentID())
	require.NotNil(t, sp.GetStartPosition())
	assert.Equal(t, "v1", sp.GetStartPosition().GetChannelName())
	assert.Equal(t, uint64(20), sp.GetStartPosition().GetTimestamp())
}
