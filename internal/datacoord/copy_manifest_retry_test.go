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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCopiedManifestMonotonicAdoption(t *testing.T) {
	manifest := func(version int64) string { return packed.MarshalManifestPath("files/2001", version) }
	for _, tc := range []struct {
		name, current, incoming, expected string
		storageVersion                    int64
		wantError                         bool
	}{
		{name: "first publication", incoming: manifest(1), expected: manifest(1), storageVersion: storage.StorageV3},
		{name: "cleanup placeholder", current: manifest(0), incoming: manifest(1), expected: manifest(1), storageVersion: storage.StorageV3},
		{name: "same revision", current: manifest(1), incoming: manifest(1), expected: manifest(1), storageVersion: storage.StorageV3},
		{name: "forward revision", current: manifest(1), incoming: manifest(2), expected: manifest(2), storageVersion: storage.StorageV3},
		{name: "newer writer wins", current: manifest(2), incoming: manifest(1), expected: manifest(2), storageVersion: storage.StorageV3},
		{name: "different base", current: manifest(2), incoming: packed.MarshalManifestPath("other/2001", 3), expected: manifest(2), storageVersion: storage.StorageV3, wantError: true},
		{name: "legacy pointer", current: "old", incoming: "new", expected: "new", storageVersion: storage.StorageV2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, m := newCopySegmentTaskTestMeta(t, createTestCopyTask(100, 2001).(*copySegmentTask))
			segment := newTestCopySegment(2001)
			segment.StorageVersion, segment.ManifestPath = tc.storageVersion, tc.current
			require.NoError(t, m.AddSegment(t.Context(), segment))
			err := m.UpdateSegmentsInfo(t.Context(), updateCopiedManifest(2001, tc.incoming))
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expected, m.GetSegment(t.Context(), 2001).GetManifestPath())
		})
	}
}

func TestRestoreCopyReplayPreservesNewerManifest(t *testing.T) {
	for _, failure := range []string{"text index metadata", "task completion"} {
		for _, recoverTask := range []bool{false, true} {
			t.Run(failure+map[bool]string{false: "/same task", true: "/recovered task"}[recoverTask], func(t *testing.T) {
				mockey.PatchConvey("retry completes missing metadata without reverting a newer manifest", t, func() {
					ctx := t.Context()
					task := createTestCopyTask(100, 2001).(*copySegmentTask)
					copies, m := newCopySegmentTaskTestMeta(t, task)
					task.copyMeta, task.meta = copies, m
					job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
					job.CreatesCollection = true
					require.NoError(t, copies.AddJob(ctx, job))
					segment := newTestCopySegment(2001)
					segment.StorageVersion = storage.StorageV3
					require.NoError(t, m.AddSegment(ctx, segment))
					copied := packed.MarshalManifestPath("files/2001", 1)
					newer := packed.MarshalManifestPath("files/2001", 2)
					response := &datapb.QueryCopySegmentResponse{
						TaskID: task.GetTaskId(), State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
						SegmentResults: []*datapb.CopySegmentResult{{
							SegmentId: 2001, ManifestPath: copied, ManifestIndexRewritten: proto.Bool(true),
							TextIndexInfos: map[int64]*datapb.TextIndexStats{101: {FieldID: 101}},
						}},
					}
					var patch *mockey.Mocker
					if failure == "text index metadata" {
						patch = mockey.Mock((*meta).UpdateSegment).Return(merr.ErrServiceUnavailable).Build()
					} else {
						var original func(*kvdatacoord.Catalog, context.Context, *datapb.CopySegmentTask) error
						patch = mockey.Mock((*kvdatacoord.Catalog).SaveCopySegmentTask).Origin(&original).To(
							func(c *kvdatacoord.Catalog, ctx context.Context, record *datapb.CopySegmentTask) error {
								if record.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskCompleted {
									return merr.ErrServiceUnavailable
								}
								return original(c, ctx, record)
							}).Build()
					}
					require.ErrorIs(t, SyncCopySegmentTask(task, response, copies, m), merr.ErrServiceUnavailable)
					patch.UnPatch()
					require.Equal(t, copied, m.GetSegment(ctx, 2001).GetManifestPath())
					require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState())
					require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, copies.GetTask(ctx, task.GetTaskId()).GetState())
					require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(ctx, 100).GetState())
					// Model a successful background index/stats publication during the retry interval.
					require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateManifest(2001, newer)))
					if recoverTask {
						var err error
						copies, err = NewCopySegmentMeta(ctx, m.catalog, m, nil, nil)
						require.NoError(t, err)
						task = copies.GetTask(ctx, 1001).(*copySegmentTask)
					}
					require.NoError(t, SyncCopySegmentTask(task, response, copies, m))
					installed := m.GetSegment(ctx, 2001)
					require.Equal(t, newer, installed.GetManifestPath())
					require.Contains(t, installed.GetTextStatsLogs(), int64(101), "a newer pointer must not skip unfinished index metadata")
					require.True(t, installed.GetIsImporting())
					require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, copies.GetTask(ctx, 1001).GetState())
				})
			})
		}
	}
}
