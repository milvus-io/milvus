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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestPublishedManifestSnapshot(t *testing.T) {
	s := &Server{meta: newEmptyTestMeta()}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	fixtures := []*datapb.SegmentInfo{
		{ID: 1, CollectionID: 10, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, ManifestPath: "collection10/v1"},
		{ID: 2, CollectionID: 20, State: commonpb.SegmentState_Growing, Level: datapb.SegmentLevel_L0, ManifestPath: "collection20/v1"},
		{ID: 3, CollectionID: 20, State: commonpb.SegmentState_Flushing, ManifestPath: "collection20/v2"},
		{ID: 4, State: commonpb.SegmentState_Dropped, ManifestPath: "dropped"},
		{ID: 5, State: commonpb.SegmentState_NotExist, ManifestPath: "missing"},
		{ID: 6, State: commonpb.SegmentState_Flushed},
		{ID: 7, State: commonpb.SegmentState_Flushed, IsImporting: true, ManifestPath: "import"},
		{ID: 8, State: commonpb.SegmentState_Flushed, IsInvisible: true, ManifestPath: "intermediate"},
		{ID: 10, CollectionID: 30, State: commonpb.SegmentState_Growing, ManifestPath: packed.MarshalManifestPath("files/insert_log/30/1/10", 0)},
		{ID: 9, State: commonpb.SegmentState_SegmentStateNone, ManifestPath: "none"},
	}
	for _, segment := range fixtures {
		s.meta.segments.SetSegment(segment.ID, NewSegmentInfo(segment))
	}
	got, err := s.GetPublishedManifestPaths(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"collection10/v1", "collection20/v1", "collection20/v2", packed.MarshalManifestPath("files/insert_log/30/1/10", 0)}, got)
	// Returned strings are detached from later metadata publication; no clones of
	// schemas/binlogs/index metadata are necessary.
	s.meta.segMu.Lock()
	fixtures[0].ManifestPath = "collection10/v2"
	s.meta.segMu.Unlock()
	require.Contains(t, got, "collection10/v1")
	// A caller can take the write lock after enumeration; no lock spans Talon I/O.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.GetPublishedManifestPaths(ctx)
	require.ErrorIs(t, err, context.Canceled)
	s.UpdateStateCode(commonpb.StateCode_Abnormal)
	_, err = s.GetPublishedManifestPaths(context.Background())
	require.Error(t, err)
}
