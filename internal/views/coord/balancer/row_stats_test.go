package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestRowLedgerDistinguishesUnknownAndPublishedZero(t *testing.T) {
	ledger := newRowCountLedger()
	ledger.segmentRowCounts[1] = 100
	ledger.segmentRowCounts[2] = 200
	shard := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	stats := &coordview.ShardStats{Segments: map[int64]*coordview.SegmentStats{
		1: {SegmentID: 1, RowNum: 0, HasRowNum: true, Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}},
		2: {SegmentID: 2, Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStatePreparing}},
		3: {SegmentID: 3, RowNum: 42, HasRowNum: true, Nodes: map[int64]coordview.SegmentState{2: coordview.SegmentStateReady}},
	}}
	ledger.replaceShardRowStats(shard, stats)
	require.Equal(t, NodeRowStats{PendingRowCount: 200}, ledger.nodeRowCount[1])
	require.Equal(t, NodeRowStats{PendingRowCount: 42}, ledger.nodeRowCount[2])
	ledger.replaceShardRowStats(shard, stats)
	require.Equal(t, NodeRowStats{PendingRowCount: 200}, ledger.nodeRowCount[1], "replacing contributions must not double count")
	ledger.replaceShardRowStats(shard, nil)
	require.Empty(t, ledger.nodeRowCount)
}
