package datacoord

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
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

// This store models exact remote object keys: a differently spelled key is a
// different object, and deleting an absent key must not remove the original.
type statsCleanupMemoryStore struct {
	storage.ChunkManager
	objects map[string]struct{}
	removed []string
	failAt  int
}

func (s *statsCleanupMemoryStore) Remove(_ context.Context, key string) error {
	s.removed = append(s.removed, key)
	if len(s.removed) == s.failAt {
		return merr.WrapErrServiceUnavailableMsg("temporary deletion failure")
	}
	if _, exists := s.objects[key]; !exists {
		return merr.WrapErrIoKeyNotFound(key)
	}
	delete(s.objects, key)
	return nil
}

func queryInvalidStatsResult(t *testing.T, task *statsTask, result *workerpb.StatsResult) {
	t.Helper()
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QueryStats(task.NodeID, mock.Anything).Return(&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil).Once()
	cluster.EXPECT().DropStats(task.NodeID, task.GetTaskID()).Return(nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
}

func TestJSONStatsV4InvalidResultCleanupPreservesRemoteKeys(t *testing.T) {
	for _, base := range []string{
		"files/insert_log/1/2/20",
		"files//insert_log/1/2/20",
		"files//insert_log/1/2/20/",
		"files/./insert_log/1/2/20",
		"files/tmp/../insert_log/1/2/20",
	} {
		for _, full := range []bool{false, true} {
			mode := "relative"
			if full {
				mode = "complete"
			}
			t.Run(base+"/"+mode, func(t *testing.T) {
				task, _, result := newInvalidExternalStatsResult(t)
				segment := task.meta.GetSegment(context.Background(), 20)
				segment.ManifestPath = packed.MarshalManifestPath(base, 1)
				result.BaseManifest = segment.ManifestPath
				prefix := base + "/_stats/json_stats.100/"
				names := []string{"meta.json", "shared_key_index/./orphan", "shared_key_index/nested//orphan", "shared_key_index/nested/../orphan", "../orphan"}
				cm := &statsCleanupMemoryStore{objects: make(map[string]struct{})}
				var files, keys []string
				for _, name := range names {
					key := prefix + name
					cm.objects[key] = struct{}{}
					keys = append(keys, key)
					if full {
						files = append(files, key)
					} else {
						files = append(files, name)
					}
				}
				result.JsonKeyStatsLogs[100].Files = files
				task.meta.chunkManager = cm

				queryInvalidStatsResult(t, task, result)

				require.Equal(t, keys, cm.removed)
				require.Empty(t, cm.objects)
				require.Empty(t, segment.GetJsonKeyStats())
			})
		}
	}
}

func TestJSONStatsV4InvalidResultKeepsReferencedAndForeignFiles(t *testing.T) {
	for _, base := range []string{"files/insert_log/1/2/20", "files//insert_log/1/2/20"} {
		t.Run(base, func(t *testing.T) {
			task, _, result := newInvalidExternalStatsResult(t)
			prefix := base + "/_stats/json_stats.100/"
			segment := task.meta.GetSegment(context.Background(), 20)
			segment.ManifestPath = packed.MarshalManifestPath(base, 1)
			result.BaseManifest = segment.ManifestPath
			segment.JsonKeyStats = map[int64]*datapb.JsonKeyStats{100: {FieldID: 100, BuildID: 29, Files: []string{"meta.json", prefix + "shared_key_index/live", "shared_key_index/nested/../literal-live"}}}
			foreignSegment := "files/insert_log/1/2/999/_stats/json_stats.100/foreign"
			foreignField := base + "/_stats/json_stats.101/foreign"
			live := []string{prefix + "meta.json", prefix + "shared_key_index/live", prefix + "shared_key_index/nested/../literal-live", foreignSegment, foreignField, "foreign-text-file"}
			orphans := []string{prefix + "shared_key_index/orphan", prefix + "shared_key_index/literal-live"}
			cm := &statsCleanupMemoryStore{objects: make(map[string]struct{})}
			for _, key := range append(append([]string{}, live...), orphans...) {
				cm.objects[key] = struct{}{}
			}
			result.JsonKeyStatsLogs[100].Files = []string{
				"meta.json", "met", "shared_key_index/live", "shared_key_index/li", "shared_key_index",
				"shared_key_index/nested/../literal-live", "shared_key_index/nested/..",
				"shared_key_index/orphan", prefix + "shared_key_index/orphan", "shared_key_index/literal-live",
				"", "/foreign/file", foreignSegment, foreignField,
			}
			result.TextStatsLogs = map[int64]*datapb.TextIndexStats{100: {Files: []string{"foreign-text-file"}}}
			task.meta.chunkManager = cm

			queryInvalidStatsResult(t, task, result)

			require.Equal(t, orphans, cm.removed)
			require.Len(t, cm.objects, len(live))
			for _, key := range live {
				require.Contains(t, cm.objects, key)
			}
			require.Equal(t, int64(29), segment.JsonKeyStats[100].BuildID)
		})
	}
}

func TestJSONStatsV4InvalidResultCleanupLocalPaths(t *testing.T) {
	task, _, result := newInvalidExternalStatsResult(t)
	root := t.TempDir()
	base := filepath.Join(root, "insert_log", "1", "2", "20")
	prefix := filepath.Join(base, "_stats", "json_stats.100")
	segment := task.meta.GetSegment(context.Background(), 20)
	segment.ManifestPath = packed.MarshalManifestPath(root+"/layout/../insert_log/1/2/20", 1)
	result.BaseManifest = packed.MarshalManifestPath(base, 1)
	segment.JsonKeyStats = map[int64]*datapb.JsonKeyStats{100: {FieldID: 100, BuildID: 29, Files: []string{"shared_key_index/nested/../live"}}}
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	task.meta.chunkManager = cm
	orphan := filepath.Join(prefix, "orphan")
	live := filepath.Join(prefix, "shared_key_index", "live")
	outside := filepath.Join(base, "outside")
	foreignField := filepath.Join(base, "_stats", "json_stats.101", "foreign")
	foreignSegment := filepath.Join(root, "insert_log", "1", "2", "999", "_stats", "json_stats.100", "foreign")
	for _, file := range []string{orphan, live, outside, foreignField, foreignSegment} {
		require.NoError(t, cm.Write(context.Background(), file, []byte("keep unless owned and unreferenced")))
	}
	result.JsonKeyStatsLogs[100].Files = []string{
		"sub/../orphan", prefix + "/./orphan", "shared_key_index/live", "shared_key_index/li", "shared_key_index",
		"../../outside", "../json_stats.101/foreign", foreignField, foreignSegment,
	}

	queryInvalidStatsResult(t, task, result)

	require.NoFileExists(t, orphan)
	for _, file := range []string{live, outside, foreignField, foreignSegment} {
		require.FileExists(t, file)
	}
	require.Equal(t, int64(29), segment.JsonKeyStats[100].BuildID)
}

func TestJSONStatsV4InvalidResultRetriesCleanupBeforeReset(t *testing.T) {
	task, versions, result := newInvalidExternalStatsResult(t)
	const base = "files//insert_log/1/2/20"
	const prefix = base + "/_stats/json_stats.100/shared_key_index/"
	segment := task.meta.GetSegment(context.Background(), 20)
	segment.ManifestPath = packed.MarshalManifestPath(base, 1)
	result.BaseManifest = segment.ManifestPath
	result.JsonKeyStatsLogs[100].Files = []string{"shared_key_index/orphan-a", "shared_key_index/orphan-b"}
	cm := &statsCleanupMemoryStore{objects: map[string]struct{}{prefix + "orphan-a": {}, prefix + "orphan-b": {}}, failAt: 2}
	task.meta.chunkManager = cm
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil).Twice()
	cluster.EXPECT().DropStats(int64(10), int64(30)).Run(func(_, _ int64) { require.Empty(t, cm.objects) }).Return(nil).Once()

	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
	require.False(t, versions.SupportsJSONStatsWriter(10))
	require.Equal(t, map[string]struct{}{prefix + "orphan-b": {}}, cm.objects)
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
	require.Equal(t, []string{prefix + "orphan-a", prefix + "orphan-b", prefix + "orphan-a", prefix + "orphan-b"}, cm.removed)
	require.Empty(t, cm.objects)
	require.Empty(t, segment.GetJsonKeyStats())
}
