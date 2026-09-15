package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newInvalidExternalStatsResult(t *testing.T) (*statsTask, *versionManagerImpl, *workerpb.StatsResult) {
	t.Helper()
	task, versions := newV4StatsTestTask(t)
	task.Version = 2
	task.PartitionID = 2
	collection, _ := task.meta.collections.Get(1)
	collection.Schema.Fields[0].ExternalField = "json"
	segment := task.meta.GetSegment(context.Background(), 20)
	segment.PartitionID = 2
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = packed.MarshalManifestPath("files/insert_log/1/2/20", 1)
	result := validV4StatsResult()
	result.CollectionID, result.PartitionID = 1, 2
	result.BaseManifest = segment.ManifestPath
	result.JsonKeyStatsLogs[100].Version = 2
	result.JsonKeyStatsLogs[100].JsonKeyStatsDataFormat = 3
	result.JsonKeyStatsLogs[100].Files = []string{"shared_key_index/orphan"}
	task.meta.chunkManager = &mockeyChunkManager{}
	return task, versions, result
}

func TestJSONStatsV4InvalidResultCleanupOwnership(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(*statsTask, *workerpb.StatsResult)
		clean bool
	}{
		{name: "owned old-format output", clean: true},
		{name: "partial output", clean: true, alter: func(task *statsTask, _ *workerpb.StatsResult) { task.JsonStatsFieldIds = []int64{100, 101} }},
		{name: "wrong task", alter: func(_ *statsTask, r *workerpb.StatsResult) { r.TaskID++ }},
		{name: "wrong segment", alter: func(_ *statsTask, r *workerpb.StatsResult) { r.SegmentID++ }},
		{name: "wrong collection", alter: func(_ *statsTask, r *workerpb.StatsResult) { r.CollectionID++ }},
		{name: "wrong partition", alter: func(_ *statsTask, r *workerpb.StatsResult) { r.PartitionID++ }},
		{name: "wrong build", alter: func(_ *statsTask, r *workerpb.StatsResult) { r.JsonKeyStatsLogs[100].BuildID++ }},
		{name: "wrong attempt", alter: func(_ *statsTask, r *workerpb.StatsResult) { r.JsonKeyStatsLogs[100].Version++ }},
		{name: "wrong field", alter: func(_ *statsTask, r *workerpb.StatsResult) { r.JsonKeyStatsLogs[100].FieldID++ }},
		{name: "unrequested field", alter: func(_ *statsTask, r *workerpb.StatsResult) {
			r.JsonKeyStatsLogs[101] = r.JsonKeyStatsLogs[100]
			r.JsonKeyStatsLogs[101].FieldID = 101
			delete(r.JsonKeyStatsLogs, 100)
		}},
		{name: "no output", alter: func(_ *statsTask, r *workerpb.StatsResult) { r.JsonKeyStatsLogs = nil }},
		{name: "wrong manifest base", alter: func(_ *statsTask, r *workerpb.StatsResult) {
			r.BaseManifest = packed.MarshalManifestPath("files/insert_log/1/2/999", 1)
		}},
		{name: "invalid manifest", alter: func(_ *statsTask, r *workerpb.StatsResult) { r.BaseManifest = "invalid" }},
		{name: "internal collection", alter: func(task *statsTask, _ *workerpb.StatsResult) {
			collection, _ := task.meta.collections.Get(1)
			collection.Schema.Fields[0].ExternalField = ""
		}},
		{name: "storage V2", alter: func(task *statsTask, _ *workerpb.StatsResult) {
			task.meta.GetSegment(context.Background(), 20).StorageVersion = storage.StorageV2
		}},
		{name: "snapshot protected", alter: func(task *statsTask, _ *workerpb.StatsResult) {
			task.meta.snapshotMeta = &snapshotMeta{segmentReferencedByGC: typeutil.NewSet[int64](20)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, _, result := newInvalidExternalStatsResult(t)
			if tc.alter != nil {
				tc.alter(task, result)
			}
			var removed []string
			remove := mockey.Mock((*mockeyChunkManager).Remove).To(func(_ *mockeyChunkManager, _ context.Context, file string) error {
				removed = append(removed, file)
				return nil
			}).Build()
			t.Cleanup(func() { remove.UnPatch() })
			require.NoError(t, task.cleanupInvalidJSONStatsResultFiles(context.Background(), result))
			if tc.clean {
				require.Equal(t, []string{"files/insert_log/1/2/20/_stats/json_stats.100/shared_key_index/orphan"}, removed)
			} else {
				require.Empty(t, removed)
			}
		})
	}
}

func TestJSONStatsV4InvalidResultKeepsReferencedAndForeignFiles(t *testing.T) {
	task, _, result := newInvalidExternalStatsResult(t)
	const prefix = "files/insert_log/1/2/20/_stats/json_stats.100/"
	segment := task.meta.GetSegment(context.Background(), 20)
	segment.JsonKeyStats = map[int64]*datapb.JsonKeyStats{100: {FieldID: 100, BuildID: 29, Files: []string{"meta.json", prefix + "shared_key_index/live"}}}
	result.JsonKeyStatsLogs[100].Files = []string{
		"meta.json", "met", "shared_key_index/live", "shared_key_index/li", "shared_key_index", "shared_key_index/orphan", prefix + "shared_key_index/orphan",
		"", "../../outside", "/foreign/file", "files/insert_log/1/2/999/_stats/json_stats.100/foreign",
		"files/insert_log/1/2/20/_stats/json_stats.101/foreign",
	}
	result.TextStatsLogs = map[int64]*datapb.TextIndexStats{100: {Files: []string{"foreign-text-file"}}}
	var removed []string
	remove := mockey.Mock((*mockeyChunkManager).Remove).To(func(_ *mockeyChunkManager, _ context.Context, file string) error {
		removed = append(removed, file)
		return nil
	}).Build()
	t.Cleanup(func() { remove.UnPatch() })
	require.NoError(t, task.cleanupInvalidJSONStatsResultFiles(context.Background(), result))
	require.Equal(t, []string{prefix + "shared_key_index/orphan"}, removed)
	require.Equal(t, int64(29), segment.JsonKeyStats[100].BuildID)
}

func TestJSONStatsV4InvalidResultRetriesCleanupBeforeReset(t *testing.T) {
	task, versions, result := newInvalidExternalStatsResult(t)
	result.JsonKeyStatsLogs[100].Files = []string{"shared_key_index/orphan-a", "shared_key_index/orphan-b"}
	const prefix = "files/insert_log/1/2/20/_stats/json_stats.100/shared_key_index/"
	var calls []string
	remove := mockey.Mock((*mockeyChunkManager).Remove).To(func(_ *mockeyChunkManager, _ context.Context, file string) error {
		calls = append(calls, file)
		switch len(calls) {
		case 2:
			return merr.WrapErrServiceUnavailableMsg("temporary deletion failure")
		case 3:
			return merr.WrapErrIoKeyNotFound(file)
		default:
			return nil
		}
	}).Build()
	t.Cleanup(func() { remove.UnPatch() })
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil).Twice()
	cluster.EXPECT().DropStats(int64(10), int64(30)).Run(func(_, _ int64) { require.Len(t, calls, 4) }).Return(nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
	require.False(t, versions.SupportsJSONStatsWriter(10))
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
	require.Equal(t, []string{prefix + "orphan-a", prefix + "orphan-b", prefix + "orphan-a", prefix + "orphan-b"}, calls)
	require.Empty(t, task.meta.GetSegment(context.Background(), 20).GetJsonKeyStats())
}
