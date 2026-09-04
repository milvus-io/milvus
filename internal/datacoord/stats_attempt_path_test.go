// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newAttemptReaderVersionManager() IndexEngineVersionManager {
	m := newIndexEngineVersionManager()
	m.AddNode(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1, V3StatsAttemptPath: true}})
	return m
}

func TestStatsAttemptPathReaderCapabilities(t *testing.T) {
	m := newIndexEngineVersionManager().(*versionManagerImpl)
	assert.False(t, m.supportsV3StatsAttemptReaders())
	old := &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}}
	m.Startup(map[string]*sessionutil.Session{"old": old})
	assert.False(t, m.supportsV3StatsAttemptReaders())
	upgraded := &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1, V3StatsAttemptPath: true}}
	m.Update(upgraded)
	assert.True(t, m.supportsV3StatsAttemptReaders(), "capability changes even with the same release version")
	m.AddNode(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 2}})
	assert.False(t, m.supportsV3StatsAttemptReaders())
	m.Startup(map[string]*sessionutil.Session{"upgraded": upgraded})
	assert.True(t, m.supportsV3StatsAttemptReaders(), "rewatch removes departed readers")
	m.RemoveNode(upgraded)
	assert.False(t, m.supportsV3StatsAttemptReaders())
}

type statsAttemptCatalog struct {
	stubCatalog
	saved *indexpb.StatsTask
}

func (c *statsAttemptCatalog) SaveStatsTask(_ context.Context, task *indexpb.StatsTask) error {
	c.saved = proto.Clone(task).(*indexpb.StatsTask)
	return nil
}

type statsAttemptCluster struct {
	session.Cluster
	result *workerpb.StatsResult
	drops  int
}

func (c *statsAttemptCluster) QueryStats(int64, *workerpb.QueryJobsRequest) (*workerpb.StatsResults, error) {
	return &workerpb.StatsResults{Results: []*workerpb.StatsResult{c.result}}, nil
}

func (c *statsAttemptCluster) DropStats(int64, int64) error {
	c.drops++
	return context.DeadlineExceeded
}

func newStatsAttemptFixture(sub indexpb.StatsSubJob) (*statsTask, *statsAttemptCatalog) {
	catalog := &statsAttemptCatalog{}
	segment := &datapb.SegmentInfo{
		ID: 10, CollectionID: 1, PartitionID: 2, NumOfRows: 10,
		State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath("files/insert_log/1/2/10", 1),
	}
	task := &indexpb.StatsTask{
		TaskID: 42, NodeID: 3, SegmentID: 10, TargetSegmentID: 10,
		CollectionID: 1, PartitionID: 2, SubJobType: sub, State: indexpb.JobState_JobStateInProgress,
	}
	mt := &meta{ctx: context.Background(), catalog: catalog, segments: NewSegmentsInfo(), statsTaskMeta: &statsTaskMeta{
		ctx: context.Background(), catalog: catalog, keyLock: lock.NewKeyLock[int64](),
		tasks:           typeutil.NewConcurrentMap[int64, *indexpb.StatsTask](),
		segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
	}}
	mt.segments.SetSegment(10, NewSegmentInfo(segment))
	mt.statsTaskMeta.tasks.Insert(42, proto.Clone(task).(*indexpb.StatsTask))
	return newStatsTask(task, 1, mt, nil, nil, newAttemptReaderVersionManager()), catalog
}

func TestStatsAttemptPathNodeFilter(t *testing.T) {
	st, catalog := newStatsAttemptFixture(indexpb.StatsSubJob_TextIndexJob)
	assert.False(t, st.CanRunOnNode(3, &session.WorkerSlots{}))
	slots := &session.WorkerSlots{SupportsV3StatsAttemptPath: true}
	assert.True(t, st.CanRunOnNode(3, slots))
	st.ievm = newIndexEngineVersionManager()
	assert.False(t, st.CanRunOnNode(3, slots))
	st.CreateTaskOnWorker(3, &statsAttemptCluster{})
	assert.Nil(t, catalog.saved, "no assignment while reader capability is unknown")
	segment := st.meta.GetSegment(context.Background(), 10).Clone()
	segment.StorageVersion = storage.StorageV2
	st.meta.segments.SetSegment(10, segment)
	assert.True(t, st.CanRunOnNode(3, &session.WorkerSlots{}), "legacy storage needs no attempt capability")
}

func TestStatsAttemptPathRejectsLegacyAndPublishesIsolatedResults(t *testing.T) {
	for _, sub := range []indexpb.StatsSubJob{indexpb.StatsSubJob_TextIndexJob, indexpb.StatsSubJob_JsonKeyIndexJob} {
		t.Run(sub.String(), func(t *testing.T) {
			for _, file := range []string{"meta.json", "41/meta.json", "42/../meta.json", "42/meta.json"} {
				t.Run(file, func(t *testing.T) {
					st, catalog := newStatsAttemptFixture(sub)
					result := &workerpb.StatsResult{TaskID: 42, State: indexpb.JobState_JobStateFinished}
					if sub == indexpb.StatsSubJob_TextIndexJob {
						result.TextStatsLogs = map[int64]*datapb.TextIndexStats{100: {
							FieldID: 100, BuildID: 42,
							Files: []string{"files/insert_log/1/2/10/_stats/text_index.100/" + file},
						}}
					} else {
						result.JsonKeyStatsLogs = map[int64]*datapb.JsonKeyStats{100: {FieldID: 100, BuildID: 42, Files: []string{file}}}
					}
					worker := &statsAttemptCluster{result: result}
					if file != "42/meta.json" {
						st.QueryTaskOnWorker(worker)
						assert.Equal(t, indexpb.JobState_JobStateRetry, st.GetState())
						assert.Equal(t, 1, worker.drops, "lost Drop response does not permit publishing legacy files")
						assert.Empty(t, catalog.updateActions)
						assert.Equal(t, packed.MarshalManifestPath("files/insert_log/1/2/10", 1), st.meta.GetSegment(context.Background(), 10).GetManifestPath())
						return
					}
					// A finished result is held across an incompatible reader, then actually
					// committed when that reader upgrades; no Fatal path or premature publish.
					st.ievm = newIndexEngineVersionManager()
					st.QueryTaskOnWorker(worker)
					assert.Equal(t, indexpb.JobState_JobStateInProgress, st.GetState())
					assert.Empty(t, catalog.updateActions)
					assert.Zero(t, worker.drops)
					committed := false
					mockCommit := mockey.Mock(packed.CommitManifestUpdates).To(func(base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
						committed = true
						require.Len(t, updates.Stats, 1)
						require.Contains(t, fmt.Sprint(updates.Stats), "/42/meta.json")
						return packed.MarshalManifestPath(base, version+1), nil
					}).Build()
					defer mockCommit.UnPatch()
					st.ievm = newAttemptReaderVersionManager()
					st.QueryTaskOnWorker(worker)
					assert.True(t, committed)
					assert.NotEmpty(t, catalog.updateActions)
					assert.Equal(t, indexpb.JobState_JobStateFinished, st.GetState())
					assert.Equal(t, indexpb.JobState_JobStateFinished, catalog.saved.GetState())
				})
			}
		})
	}
}
