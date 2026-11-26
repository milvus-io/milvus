// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stats

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
)

func TestLegacySortFieldLogicalSizes(t *testing.T) {
	ctx := context.Background()
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	before := packed.MarshalManifestPath("files/insert_log/1/2/231", 7)
	after := packed.MarshalManifestPath("files/insert_log/1/2/231", 8)
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 231, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
		NumOfRows: 3, ManifestPath: before, FieldLogicalSizesManifest: before,
		Stats: &datapb.Statistics{FieldLogicalSizes: &datapb.FieldLogicalSizes{Version: 1, RowCount: 3, Fields: map[int64]*datapb.FieldLogicalSize{100: {}}}},
	})))
	st := &StatsTask{Meta: mt, StatsTask: &indexpb.StatsTask{TaskID: 8, CollectionID: 1, TargetSegmentID: 231, SubJobType: indexpb.StatsSubJob_Sort}}
	require.NoError(t, st.SetJobInfo(ctx, &workerpb.StatsResult{Manifest: before}))
	require.NotNil(t, meta.SegmentFieldLogicalSizes(mt.GetSegment(ctx, 231).SegmentInfo).GetFields()[100])
	require.NoError(t, st.SetJobInfo(ctx, &workerpb.StatsResult{BaseManifest: before, Manifest: after}))
	updated := mt.GetSegment(ctx, 231)
	require.Equal(t, after, updated.GetManifestPath())
	require.Nil(t, updated.GetStats().GetFieldLogicalSizes())
}

// shouldPublishPreparedManifest is the guard that keeps a dropped segment from
// entering CommitSegmentManifest in the first place: GetSegment returns dropped
// segments, so without the health check a retired segment would take the
// prepared-commit path and fail. A healthy segment with a newer worker manifest
// still takes it.
func TestShouldPublishPreparedManifestSkipsUnhealthySegment(t *testing.T) {
	base := "/tmp/milvus/insert_log/1/10/"
	mt, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             231,
		State:          commonpb.SegmentState_Dropped,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(base+"231", 7),
	})))
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             232,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(base+"232", 7),
	})))

	st := &StatsTask{Meta: mt}
	require.False(t, st.shouldPublishPreparedManifest(context.Background(), 231,
		&workerpb.StatsResult{Manifest: packed.MarshalManifestPath(base+"231", 8)}))
	require.True(t, st.shouldPublishPreparedManifest(context.Background(), 232,
		&workerpb.StatsResult{Manifest: packed.MarshalManifestPath(base+"232", 8)}))
}
