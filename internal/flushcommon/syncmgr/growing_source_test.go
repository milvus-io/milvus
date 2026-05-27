package syncmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type fakeCommitGrowingFlushSource struct {
	commits []int64
}

func (s *fakeCommitGrowingFlushSource) CurrentOffset() int64 {
	return 10
}

func (s *fakeCommitGrowingFlushSource) FlushGrowingData(context.Context, int64, int64, *GrowingFlushConfig) (*GrowingFlushResult, error) {
	return &GrowingFlushResult{ManifestPath: "manifest", NumRows: 10}, nil
}

func (s *fakeCommitGrowingFlushSource) Release() {
}

func (s *fakeCommitGrowingFlushSource) CommitGrowingFlush(targetOffset int64) {
	s.commits = append(s.commits, targetOffset)
}

func TestGrowingSourceSyncTaskCommitRetainedSourceOnlyOnFinalization(t *testing.T) {
	run := func(t *testing.T, finalize func(*GrowingSourceSyncTask), expectCommit bool) {
		segmentID := int64(1)
		mc := metacache.NewMockMetaCache(t)
		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			ID:           segmentID,
			PartitionID:  2,
			ManifestPath: "manifest",
		}, pkoracle.NewBloomFilterSet(), nil)
		metacache.UpdateNumOfRows(10)(segment)
		source := &fakeCommitGrowingFlushSource{}

		mc.EXPECT().GetSegmentByID(segmentID).Return(segment, true)
		mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
			action(segment)
		}).Return()
		mc.EXPECT().RemoveSegments(mock.Anything).Return(nil).Maybe()

		task := NewGrowingSourceSyncTask().
			WithCollectionID(3).
			WithPartitionID(2).
			WithSegmentID(segmentID).
			WithChannelName("ch").
			WithStartPosition(&msgpb.MsgPosition{Timestamp: 100}).
			WithCheckpoint(&msgpb.MsgPosition{Timestamp: 200}).
			WithBatchRows(0).
			WithTargetOffset(10).
			WithMetaCache(mc).
			WithSource(source)
		if finalize != nil {
			finalize(task)
		}

		require.NoError(t, task.Run(context.Background()))
		if expectCommit {
			require.Equal(t, []int64{10}, source.commits)
		} else {
			require.Empty(t, source.commits)
		}
	}

	t.Run("non_final_sync_does_not_commit_retained_source", func(t *testing.T) {
		run(t, nil, false)
	})

	t.Run("flush_finalization_commits_retained_source", func(t *testing.T) {
		run(t, func(task *GrowingSourceSyncTask) {
			task.WithFlush()
		}, true)
	})

	t.Run("drop_finalization_commits_retained_source", func(t *testing.T) {
		run(t, func(task *GrowingSourceSyncTask) {
			task.WithDrop()
		}, true)
	})
}
