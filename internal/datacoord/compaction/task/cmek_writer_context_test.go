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

package task

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type compactionWriterContextCipher struct {
	hook.Cipher
	key []byte
}

func (c *compactionWriterContextCipher) GetUnsafeKey(int64, int64) []byte { return c.key }

func TestCompactionRejectsUnavailableWriterContext(t *testing.T) {
	paramtable.Init()
	cipher := &compactionWriterContextCipher{}
	patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
	t.Cleanup(func() { patch.UnPatch() })
	for _, kind := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction, datapb.CompactionType_Level0DeleteCompaction,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			cipher.key = nil
			mt := NewMockCompactionMeta(t)
			if kind == datapb.CompactionType_Level0DeleteCompaction {
				mt.EXPECT().GetHealthySegment(mock.Anything, int64(31)).Return(&meta.SegmentInfo{
					SegmentInfo: &datapb.SegmentInfo{
						ID: 31, CollectionID: 23, Level: datapb.SegmentLevel_L0,
						InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed,
					},
				})
				mt.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.SegmentInfo{{
					SegmentInfo: &datapb.SegmentInfo{
						ID: 32, CollectionID: 23, Level: datapb.SegmentLevel_L1,
						InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed,
					},
				}})
			} else {
				mt.EXPECT().GetHealthySegment(mock.Anything, int64(31)).Return(&meta.SegmentInfo{
					SegmentInfo: &datapb.SegmentInfo{ID: 31, CollectionID: 23},
				})
			}
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil)
			catalog := mocks.NewDataCoordCatalog(t)
			taskMeta := NewCompactionTaskMetaOverCatalog(catalog)
			taskProto := &datapb.CompactionTask{
				PlanID: 1, CollectionID: 23, PartitionID: 10, Channel: "ch-1", Type: kind,
				State:         datapb.CompactionTaskState_pipelining,
				InputSegments: []int64{31},
				Schema: &schemapb.CollectionSchema{Properties: []*commonpb.KeyValuePair{
					{Key: common.EncryptionEzIDKey, Value: "17"},
				}},
			}
			var task CompactionTask
			switch kind {
			case datapb.CompactionType_BumpSchemaVersionCompaction:
				task = NewBumpSchemaVersionTask(taskProto, alloc, mt, taskMeta, index.NewTestVersionManager())
			case datapb.CompactionType_Level0DeleteCompaction:
				task = NewL0CompactionTask(taskProto, alloc, mt, taskMeta)
			default:
				task = NewMixCompactionTask(taskProto, alloc, mt, taskMeta, index.NewTestVersionManager())
			}
			plan, err := task.BuildCompactionRequest()
			require.Nil(t, plan)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.True(t, merr.IsRetryableErr(err))
			cluster := session.NewMockCluster(t)
			// Retryable key errors must keep the compaction task pipelining without
			// dispatching or failing it.
			task.CreateTaskOnWorker(42, cluster, task.GetTaskResource())
			require.Equal(t, datapb.CompactionTaskState_pipelining, task.GetTaskProto().GetState())
			catalog.AssertNotCalled(t, "SaveCompactionTask", mock.Anything, mock.Anything)
		})
	}
}
