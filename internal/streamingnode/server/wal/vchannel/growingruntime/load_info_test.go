package growingruntime

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestLoadInfoFromVisibleSegmentUsesPersistedRowCount(t *testing.T) {
	visible := walview.VisibleSegment{
		SegmentID:   10,
		PartitionID: 20,
		Assignment: &streamingpb.SegmentAssignmentMeta{
			CollectionId: 30,
			Vchannel:     "v1",
			Stat: &streamingpb.SegmentAssignmentStat{
				ModifiedRows: 1_000,
			},
		},
		Data: walview.SegmentSnapshotData{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				ManifestPath: "manifest",
				Binlogs: []*streamingpb.L1SegmentBinLogs{
					{
						FieldBinlog: []*datapb.FieldBinlog{
							{FieldID: 0, Binlogs: []*datapb.Binlog{{EntriesNum: 100}, {EntriesNum: 50}}},
							{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 150}}},
						},
					},
					{
						FieldBinlog: []*datapb.FieldBinlog{
							{FieldID: 0, Binlogs: []*datapb.Binlog{{EntriesNum: 80}}},
							{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 80}}},
						},
					},
				},
			},
		},
	}

	loadInfo := loadInfoFromVisibleSegment(visible)
	require.Equal(t, int64(230), loadInfo.GetNumOfRows())
}

// Snapshot assignment stats cover only durable data after recovery-storage extraction.
func TestLoadInfoFromVisibleSegmentUsesDurableAssignmentStats(t *testing.T) {
	visible := walview.VisibleSegment{Assignment: &streamingpb.SegmentAssignmentMeta{CheckpointTimeTick: 100, Stat: &streamingpb.SegmentAssignmentStat{ModifiedRows: 1000}}, Data: walview.SegmentSnapshotData{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{ManifestPath: "manifest"}}}
	require.Equal(t, int64(1000), loadInfoFromVisibleSegment(visible).GetNumOfRows())
}
