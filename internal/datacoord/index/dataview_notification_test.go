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

package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
)

type indexDataViewRecorder struct {
	dataview.Manager
	calls []int64
}

func (r *indexDataViewRecorder) Recompute(_ context.Context, id int64) error {
	r.calls = append(r.calls, id)
	return nil
}

func TestDataViewCompactionAdmissionIndexWithoutManifestNotifies(t *testing.T) {
	for _, fake := range []bool{true, false} {
		t.Run(map[bool]string{true: "no training", false: "no artifact"}[fake], func(t *testing.T) {
			m, dm, err := NewMemoryMetaWithIndex(t)
			require.NoError(t, err)
			dm.IndexMeta.Indexes[1] = map[int64]*model.Index{10: {CollectionID: 1, FieldID: 2, IndexID: 10}}
			recorder := &indexDataViewRecorder{}
			m.DataViewManager = recorder
			index := &model.SegmentIndex{CollectionID: 1, PartitionID: 1, SegmentID: 3, IndexID: 10, BuildID: 100, IndexState: commonpb.IndexState_Unissued}
			require.NoError(t, dm.IndexMeta.AddSegmentIndex(t.Context(), index))
			task := &indexBuildTask{SegmentIndex: index, meta: m, indexMeta: dm.IndexMeta}
			if fake {
				require.NoError(t, task.UpdateStateWithMeta(indexpb.JobState_JobStateFinished, "no training required"))
			} else {
				require.NoError(t, task.SetJobInfo(&workerpb.IndexTaskInfo{BuildID: 100, State: commonpb.IndexState_Finished}))
			}
			require.Equal(t, commonpb.IndexState_Finished, dm.IndexMeta.GetSegmentIndexState(1, 3, 10).GetState())
			require.Equal(t, []int64{1}, recorder.calls)
		})
	}
}
