package segment

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// coordStub is a minimal MixCoordClient that only implements AllocSegment and
// SaveBinlogPaths, enough to drive the lifecycle writer error classification.
type coordStub struct {
	types.MixCoordClient
	allocSegment    func(ctx context.Context, req *datapb.AllocSegmentRequest) (*datapb.AllocSegmentResponse, error)
	saveBinlogPaths func(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error)
}

func (c *coordStub) AllocSegment(ctx context.Context, req *datapb.AllocSegmentRequest, _ ...grpc.CallOption) (*datapb.AllocSegmentResponse, error) {
	return c.allocSegment(ctx, req)
}

func (c *coordStub) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return c.saveBinlogPaths(ctx, req)
}

func newCommitL1SegmentTestMeta() *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		CollectionId: 1,
		PartitionId:  1,
		SegmentId:    1,
		Vchannel:     "v1",
		Stat:         &streamingpb.SegmentAssignmentStat{},
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: "manifest",
		},
	}
}

// TestCommitL1SegmentIgnoresSegmentNotFound covers the Ignore classification:
// a segment that no longer exists in DataCoord has nothing to commit, so the
// error is swallowed instead of being retried or failing the segment.
func TestCommitL1SegmentIgnoresSegmentNotFound(t *testing.T) {
	w := &segmentLifecycleWriter{
		serverID: 1,
		coord: &coordStub{saveBinlogPaths: func(_ context.Context, _ *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
			return merr.Status(merr.WrapErrSegmentNotFound(1)), nil
		}},
	}
	require.NoError(t, w.CommitL1Segment(context.Background(), newCommitL1SegmentTestMeta()))
}

// TestCommitL1SegmentFailsOnInputError covers the Unrecoverable
// classification for request-content rejections (e.g. a TEXT segment saved
// with a pre-V3 storage version): DataCoord will never accept the same
// request, so the error is marked unrecoverable and fails the segment
// instead of hot-looping on it.
func TestCommitL1SegmentFailsOnInputError(t *testing.T) {
	w := &segmentLifecycleWriter{
		serverID: 1,
		coord: &coordStub{saveBinlogPaths: func(_ context.Context, _ *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
			return merr.Status(merr.WrapErrParameterInvalid("v2", "v3")), nil
		}},
	}
	err := w.CommitL1Segment(context.Background(), newCommitL1SegmentTestMeta())
	require.Error(t, err)
	require.False(t, retry.IsRecoverable(err))
}

// TestEnsureGrowingSegmentTaskFailsOnCoordInputError drives the full
// classification chain end to end: DataCoord rejects the AllocSegment request
// with an InputError status, the lifecycle writer marks it unrecoverable, and
// the task layer fails the segment with a clean (non-ErrDelay) error instead
// of requeueing a terminal failure.
func TestEnsureGrowingSegmentTaskFailsOnCoordInputError(t *testing.T) {
	w := &segmentLifecycleWriter{
		serverID: 1,
		coord: &coordStub{allocSegment: func(_ context.Context, _ *datapb.AllocSegmentRequest) (*datapb.AllocSegmentResponse, error) {
			return &datapb.AllocSegmentResponse{Status: merr.Status(merr.WrapErrParameterInvalid("v2", "v3"))}, nil
		}},
	}
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1"},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{
			lifecycle: w,
			runtime:   moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}},
		},
	)
	view.mu.Lock()
	task := view.newEnsureGrowingSegmentTaskLocked(0)
	view.mu.Unlock()

	err := task.Execute(context.Background())
	require.Error(t, err)
	require.False(t, errors.Is(err, nodescheduler.ErrDelay), "terminal error must not be requeued")
	require.Error(t, view.unrecoverableErr())
}
