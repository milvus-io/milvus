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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *statsTaskSuite) TestQueryTaskOnWorkerRetriesBeforeStatsPublication() {
	const base = `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	const committed = `{"base_path":"files/insert_log/1/2/1179","ver":3}`
	for _, jobType := range []indexpb.StatsSubJob{indexpb.StatsSubJob_TextIndexJob, indexpb.StatsSubJob_JsonKeyIndexJob} {
		for _, failure := range []struct {
			name     string
			err      error
			manifest string
		}{
			{name: "storage throttling", err: merr.ErrIoTooManyRequests},
			{name: "storage permission", err: merr.ErrIoPermissionDenied},
			{name: "prepared manifest validation", manifest: `{"base_path":"another/segment","ver":3}`},
		} {
			s.Run(jobType.String()+"/"+failure.name, func() {
				restoreSegment := s.installJSONStatsSegment(base)
				defer restoreSegment()
				origCtx, origStatsCatalog := s.mt.ctx, s.mt.statsTaskMeta.catalog
				origTask := s.mt.statsTaskMeta.GetStatsTask(s.taskID)
				defer func() {
					s.mt.ctx, s.mt.statsTaskMeta.catalog = origCtx, origStatsCatalog
					s.mt.statsTaskMeta.tasks.Insert(s.taskID, origTask)
				}()
				s.mt.ctx = context.Background()
				catalog := &mockeyDataCoordCatalog{}
				s.mt.catalog, s.mt.statsTaskMeta.catalog = catalog, catalog
				current := proto.Clone(origTask).(*indexpb.StatsTask)
				current.State = indexpb.JobState_JobStateInProgress
				current.SubJobType = jobType
				s.mt.statsTaskMeta.tasks.Insert(s.taskID, current)

				recovered := false
				mockManifest := mockey.Mock(packed.CommitManifestUpdates).To(
					func(string, int64, *indexpb.StorageConfig, *packed.ManifestUpdates) (string, error) {
						if !recovered {
							return failure.manifest, failure.err
						}
						return committed, nil
					}).Build()
				defer mockManifest.UnPatch()
				writes := 0
				mockWrite := mockey.Mock((*mockeyDataCoordCatalog).Update).To(
					func(*mockeyDataCoordCatalog, context.Context, ...metastore.UpdateAction) error {
						writes++
						return nil
					}).Build()
				defer mockWrite.UnPatch()
				mockSaveTask := mockey.Mock((*mockeyDataCoordCatalog).SaveStatsTask).Return(nil).Build()
				defer mockSaveTask.UnPatch()
				fatalCalled := false
				mockFatal := mockey.Mock(mlog.Fatal).To(func(context.Context, string, ...mlog.Field) {
					fatalCalled = true
				}).Build()
				defer mockFatal.UnPatch()

				result := &workerpb.StatsResult{
					TaskID: s.taskID, State: indexpb.JobState_JobStateFinished,
					TextStatsLogs:    map[int64]*datapb.TextIndexStats{500: {FieldID: 500, BuildID: s.taskID}},
					JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{500: {FieldID: 500, BuildID: s.taskID}},
				}
				cluster := session.NewMockCluster(s.T())
				cluster.EXPECT().QueryStats(mock.Anything, mock.Anything).Return(
					&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil).Twice()
				st := s.newJSONStatsTask()
				st.SubJobType = jobType
				st.ievm = newAttemptReaderVersionManager()
				st.QueryTaskOnWorker(cluster)
				s.False(fatalCalled)
				s.Zero(writes, "pre-publication failures must not change catalog metadata")
				s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
				s.Equal(base, s.mt.GetSegment(context.Background(), s.segID).GetManifestPath())

				// The same finished worker result can be published on the next poll.
				recovered = true
				st.QueryTaskOnWorker(cluster)
				s.False(fatalCalled)
				s.Equal(1, writes)
				s.Equal(indexpb.JobState_JobStateFinished, st.GetState())
				s.Equal(committed, s.mt.GetSegment(context.Background(), s.segID).GetManifestPath())
			})
		}
	}
}

func (s *statsTaskSuite) TestQueryTaskOnWorkerKeepsV2PublicationFailStop() {
	for _, jobType := range []indexpb.StatsSubJob{indexpb.StatsSubJob_JsonKeyIndexJob, indexpb.StatsSubJob_Sort} {
		s.Run(jobType.String(), func() {
			restoreSegment := s.installJSONStatsSegment("")
			defer restoreSegment()
			s.mt.segments.segments[s.segID].StorageVersion = storage.StorageV2
			origCtx := s.mt.ctx
			s.mt.ctx = context.Background()
			defer func() { s.mt.ctx = origCtx }()
			origTask := s.mt.statsTaskMeta.GetStatsTask(s.taskID)
			current := proto.Clone(origTask).(*indexpb.StatsTask)
			current.State = indexpb.JobState_JobStateInProgress
			current.SubJobType = jobType
			s.mt.statsTaskMeta.tasks.Insert(s.taskID, current)
			defer s.mt.statsTaskMeta.tasks.Insert(s.taskID, origTask)
			s.mt.catalog = &mockeyDataCoordCatalog{}
			mockWrite := mockey.Mock((*mockeyDataCoordCatalog).AlterSegments).Return(merr.ErrServiceUnavailable).Build()
			defer mockWrite.UnPatch()
			fatalCalled := false
			mockFatal := mockey.Mock(mlog.Fatal).To(func(context.Context, string, ...mlog.Field) {
				fatalCalled = true
			}).Build()
			defer mockFatal.UnPatch()
			result := &workerpb.StatsResult{
				TaskID: s.taskID, State: indexpb.JobState_JobStateFinished,
				JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{500: {FieldID: 500, BuildID: s.taskID}},
				StatsLogs:        []*datapb.FieldBinlog{{FieldID: 500, Binlogs: []*datapb.Binlog{{LogID: 5}}}},
			}
			cluster := session.NewMockCluster(s.T())
			cluster.EXPECT().QueryStats(mock.Anything, mock.Anything).Return(
				&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil)
			st := s.newJSONStatsTask()
			st.SubJobType = jobType
			st.QueryTaskOnWorker(cluster)
			s.True(fatalCalled)
			s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
			s.Empty(s.mt.GetSegment(context.Background(), s.segID).GetJsonKeyStats())
			s.Empty(s.mt.GetSegment(context.Background(), s.segID).GetStatslogs())
		})
	}
}
