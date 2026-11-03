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

package importv2

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCopyFailurePreservesTypedStatus(t *testing.T) {
	for _, err := range []error{merr.ErrServiceUnavailable, merr.ErrDataIntegrity} {
		mockey.PatchConvey(err.Error(), t, func() {
			manager := NewTaskManager()
			task := NewCopySegmentTask(t.Context(), &datapb.CopySegmentRequest{
				TaskID:  1,
				Sources: []*datapb.CopySegmentSource{{SegmentId: 10}}, Targets: []*datapb.CopySegmentTarget{{SegmentId: 20}},
			}, manager, nil, nil, nil, nil, "", "").(*CopySegmentTask)
			manager.Add(task)
			mockey.Mock((*CopySegmentTask).copySingleSegment).Return(nil, err).Build()
			_ = conc.BlockOnAll(task.Execute()...)
			failed := manager.Get(1).(*CopySegmentTask)
			require.Equal(t, datapb.ImportTaskStateV2_Failed, failed.GetState())
			require.ErrorIs(t, merr.Error(failed.GetFailureStatus()), err)
			clone := failed.Clone().(*CopySegmentTask)
			clone.GetFailureStatus().Reason = "changed"
			require.NotEqual(t, clone.GetFailureStatus().Reason, failed.GetFailureStatus().Reason)
		})
	}
}

func TestCopyValidationFailureCarriesDataIntegrity(t *testing.T) {
	manager := NewTaskManager()
	task := NewCopySegmentTask(t.Context(), &datapb.CopySegmentRequest{TaskID: 1}, manager, nil, nil, nil, nil, "", "")
	manager.Add(task)
	require.Empty(t, task.Execute())
	failed := manager.Get(1).(*CopySegmentTask)
	require.ErrorIs(t, merr.Error(failed.GetFailureStatus()), merr.ErrDataIntegrity)
}
