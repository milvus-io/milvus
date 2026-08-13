package datacoord

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestValidateImportResultManifest(t *testing.T) {
	manifest := &datapb.ImportResultManifest{
		JobId: 1, TaskId: 2, RunId: 3, PlanningGeneration: 4,
		TaskPlanDigest: []byte("plan"), TotalRows: 10, TotalPhysicalBytes: 20,
		Segments: []*datapb.SegmentResult{{
			PhysicalSegmentId: 9, Rows: 10, PhysicalBytes: 20,
			Materialized: true,
			MaxTimestamp: 100, Statistics: &datapb.Statistics{TimestampTo: 100},
		}},
	}
	require.NoError(t, validateImportResultManifest(manifest, 1, 2, 3, 4, []byte("plan"), []int64{9}))
	manifest.Segments[0].MaxTimestamp = 99
	require.Error(t, validateImportResultManifest(manifest, 1, 2, 3, 4, []byte("plan"), []int64{9}))
}
