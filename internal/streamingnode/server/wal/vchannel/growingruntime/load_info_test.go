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

func TestLoadInfoFromVisibleSegmentFallsBackOnlyAtDurableMetaBoundary(t *testing.T) {
	tests := []struct {
		name           string
		metaCheckpoint uint64
		dataCheckpoint uint64
		expected       int64
	}{
		{name: "data caught up", metaCheckpoint: 100, dataCheckpoint: 100, expected: 1_000},
		{name: "meta ahead", metaCheckpoint: 101, dataCheckpoint: 100, expected: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			visible := walview.VisibleSegment{
				Assignment: &streamingpb.SegmentAssignmentMeta{
					CheckpointTimeTick:     test.metaCheckpoint,
					DataCheckpointTimeTick: test.dataCheckpoint,
					Stat: &streamingpb.SegmentAssignmentStat{
						ModifiedRows: 1_000,
					},
				},
				Data: walview.SegmentSnapshotData{
					PersistedStorage: &streamingpb.L1SegmentPersistedStorage{ManifestPath: "manifest"},
				},
			}

			loadInfo := loadInfoFromVisibleSegment(visible)
			require.Equal(t, test.expected, loadInfo.GetNumOfRows())
		})
	}
}
