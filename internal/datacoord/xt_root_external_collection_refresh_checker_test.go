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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestExternalCollectionRefreshChecker_IndexWait_DataStaysQueryVisible pins the
// contract the wait actually offers, because it is easy to read the option's
// name as a visibility barrier and it is not one: what the wait holds back is
// the job's COMPLETION SIGNAL, not the data.
//
// The refreshed segments are applied by the same applyJobInfo call the ungated
// path makes, at the same point in the same tick, and they land as Flushed.
// GetQueryVChanPositions - the source of QueryCoord's target, both directly and
// through GetRecoveryInfoV2 - admits a flushed segment with no compaction
// ancestry unconditionally; the index state only ever decides whether to serve
// a compacted segment or its unindexed parents. So a loaded replica can serve
// the refreshed rows by brute force while the job still reads InProgress, and
// that is true with the parameter on and off alike: the wait neither adds the
// exposure nor removes it.
//
// Excluding refreshed segments from the target until their indexes are ready
// would be a different feature - a staged segment state visible to the index
// builder but not to QueryCoord - and it is not this one. This test fails if
// that ever changes silently.
func TestExternalCollectionRefreshChecker_IndexWait_DataStaysQueryVisible(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	const (
		collectionID = int64(100)
		segmentID    = int64(556)
		channel      = "by-dev-rootcoord-dml_ext_0v0"
	)

	// A collection with one refreshed segment: flushed, no compaction ancestry,
	// no index record - exactly what an apply leaves behind.
	stage := func(t *testing.T) (*externalCollectionRefreshMeta, *meta, *ServerHandler) {
		t.Helper()
		catalog := &stubCatalog{}
		mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(
			[]*datapb.ExternalCollectionRefreshJob{{
				JobId: 1, CollectionId: collectionID,
				State: indexpb.JobState_JobStateInProgress, TaskIds: []int64{1001},
			}}, nil).Build()
		mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(
			[]*datapb.ExternalCollectionRefreshTask{{
				TaskId: 1001, JobId: 1, CollectionId: collectionID,
				State: indexpb.JobState_JobStateFinished, Progress: 100,
			}}, nil).Build()
		refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		require.NoError(t, err)

		ms := metacache.NewMetaStore(nil)
		mt := &meta{
			segments:  NewSegmentsInfo(ms),
			metaStore: ms,
		}
		mt.AddCollection(&collectionInfo{
			ID: collectionID, Schema: &schemapb.CollectionSchema{Name: "coll"},
		})
		mt.segments.SetSegment(segmentID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: segmentID, CollectionID: collectionID, InsertChannel: channel,
			State:       commonpb.SegmentState_Flushed,
			NumOfRows:   1024,
			DmlPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 100},
			Binlogs:     []*datapb.FieldBinlog{{FieldID: 1}},
		}})
		mt.metaStore.LoadChannelCheckpoints(map[string]*msgpb.MsgPosition{
			channel: {ChannelName: channel, Timestamp: 100},
		})

		// The segment carries no index, so it is still debt for the whole wait.
		mockey.Mock((*indexMeta).GetUnindexedSegments).Return([]int64{segmentID}).Build()

		return refreshMeta, mt, &ServerHandler{s: &Server{meta: mt, domainMetas: domainMetas{
			indexMeta:          &indexMeta{},
			partitionStatsMeta: &partitionStatsMeta{partitionStatsInfos: map[string]map[int64]*partitionStatsInfo{}},
		}}}
	}

	flushedIDs := func(h *ServerHandler) []int64 {
		return h.GetQueryVChanPositions(&channelMeta{
			Name: channel, CollectionID: collectionID,
		}).GetFlushedSegmentIds()
	}

	t.Run("a waiting job's segments are already in the query target", func(t *testing.T) {
		mockey.PatchConvey("visible while waiting", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			refreshMeta, mt, handler := stage(t)
			checker := newRefreshChecker(ctx, mt, handler.s.indexMeta, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error { return nil },
				nil, nil, nil)

			checker.aggregateJobState(refreshMeta.GetJob(1))

			waiting := refreshMeta.GetJob(1)
			require.Equal(t, indexpb.JobState_JobStateInProgress, waiting.GetState(),
				"the job must still be waiting for this assertion to mean anything")
			require.NotZero(t, waiting.GetIndexWaitStartedTime())

			assert.Contains(t, flushedIDs(handler), segmentID,
				"the wait holds the completion signal, not the data - an unindexed refreshed "+
					"segment is in the query target and may be brute-force scanned")
		})
	})

	t.Run("the ungated path exposes exactly the same set", func(t *testing.T) {
		mockey.PatchConvey("visible with the wait off", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "false")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			refreshMeta, mt, handler := stage(t)
			checker := newRefreshChecker(ctx, mt, handler.s.indexMeta, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error { return nil },
				nil, nil, nil)

			checker.aggregateJobState(refreshMeta.GetJob(1))

			require.Equal(t, indexpb.JobState_JobStateFinished, refreshMeta.GetJob(1).GetState(),
				"off, the job finishes as soon as its data lands")
			assert.Contains(t, flushedIDs(handler), segmentID,
				"the same unindexed segment is in the target here, which is why the wait "+
					"does not widen the exposure - it is master's, not this feature's")
		})
	})
}
