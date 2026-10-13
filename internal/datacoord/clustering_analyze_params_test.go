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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestClusteringAnalyzeK(t *testing.T) {
	for _, tc := range []struct{ rows, dim, chunk, min, max, expected int64 }{
		{0, 128, 1024, 16, 128, 16},
		{100, 128, 1024, 16, 128, 50},
		{101, 128, 1024, 16, 128, 51},
		{10000, 128, 1024, 16, 128, 128},
	} {
		k, err := clusteringAnalyzeK(tc.rows, tc.dim, tc.chunk, tc.min, tc.max)
		require.NoError(t, err)
		require.Equal(t, tc.expected, k)
	}
	for _, args := range [][5]int64{{-1, 128, 1024, 1, 128}, {1, 0, 1024, 1, 128}, {1, 128, 0, 1, 128}, {1, 128, 1024, 129, 128}, {math.MaxInt64, 2, 1, 1, 128}, {1, math.MaxInt64, 1, 1, 128}} {
		_, err := clusteringAnalyzeK(args[0], args[1], args[2], args[3], args[4])
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
}

func (s *ClusteringCompactionTaskSuite) TestAnalyzeParametersSurviveCompactionStateRetry() {
	paramtable.Init()
	task := s.generateBasicTask(true)
	for _, id := range []int64{101, 102} {
		s.meta.AddSegment(context.Background(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: 1, PartitionID: 10, State: commonpb.SegmentState_Flushed, NumOfRows: 1000}})
	}
	s.Require().NoError(task.doAnalyze())
	saved := proto.Clone(s.meta.analyzeMeta.GetTask(task.GetTaskProto().GetAnalyzeTaskID())).(*indexpb.AnalyzeTask)
	s.Require().Positive(saved.NumClusters)
	s.Equal(int64(32), saved.Dim)
	orig := Params.KnowhereConfig.ClusterType.SwapTempValue("changed-provider")
	defer Params.KnowhereConfig.ClusterType.SwapTempValue(orig)
	// A retry after AddAnalyzeTask succeeded must not replace persisted metadata.
	s.Require().NoError(task.doAnalyze())
	s.True(proto.Equal(saved, s.meta.analyzeMeta.GetTask(saved.TaskID)))
}

func (s *analyzeTaskSuite) TestStaleAnalyzeResultCannotBeAdopted() {
	current := s.mt.analyzeMeta.GetTask(s.taskID)
	result := &workerpb.AnalyzeResult{TaskID: s.taskID, Version: current.GetVersion() - 1, State: indexpb.JobState_JobStateFinished, CentroidsFile: "old-generation"}
	s.ErrorIs(s.mt.analyzeMeta.FinishTask(s.taskID, result), merr.ErrServiceInternal)
	s.Same(current, s.mt.analyzeMeta.GetTask(s.taskID))
}

func (s *ClusteringCompactionTaskSuite) TestFailedCompactionCleansItsAnalyzeTask() {
	task := s.generateBasicTask(true)
	task.GetTaskProto().AnalyzeTaskID = 777
	task.GetTaskProto().State = datapb.CompactionTaskState_failed
	task.GetTaskProto().ResultSegments = nil
	task.GetTaskProto().InputSegments = nil
	s.Require().NoError(s.meta.analyzeMeta.AddAnalyzeTask(&indexpb.AnalyzeTask{TaskID: 777, Version: 2, CollectionID: 1, PartitionID: 10, FieldID: 101, State: indexpb.JobState_JobStateFailed}))
	// A different task must survive this cleanup.
	s.Require().NoError(s.meta.analyzeMeta.AddAnalyzeTask(&indexpb.AnalyzeTask{TaskID: 778, Version: 3, State: indexpb.JobState_JobStateFinished}))
	s.Require().True(task.Clean())
	s.Nil(s.meta.analyzeMeta.GetTask(777))
	s.NotNil(s.meta.analyzeMeta.GetTask(778))
}
