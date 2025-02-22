package assignment

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestAssignmentService(t *testing.T) {
	s := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordAssignmentServiceClient](t)
	c := mock_streamingpb.NewMockStreamingCoordAssignmentServiceClient(t)
	s.EXPECT().GetService(mock.Anything).Return(c, nil)
	cc := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverClient(t)
	c.EXPECT().AssignmentDiscover(mock.Anything).Return(cc, nil)
	k := 0
	closeCh := make(chan struct{})
	cc.EXPECT().Send(mock.Anything).Return(nil)
	cc.EXPECT().CloseSend().Return(nil)
	cc.EXPECT().Recv().RunAndReturn(func() (*streamingpb.AssignmentDiscoverResponse, error) {
		resps := []*streamingpb.AssignmentDiscoverResponse{
			{
				Response: &streamingpb.AssignmentDiscoverResponse_FullAssignment{
					FullAssignment: &streamingpb.FullStreamingNodeAssignmentWithVersion{
						Version: &streamingpb.VersionPair{Global: 1, Local: 2},
						Assignments: []*streamingpb.StreamingNodeAssignment{
							{
								Node:     &streamingpb.StreamingNodeInfo{ServerId: 1},
								Channels: []*streamingpb.PChannelInfo{{Name: "c1", Term: 1}, {Name: "c2", Term: 2}},
							},
						},
					},
				},
			},
			{
				Response: &streamingpb.AssignmentDiscoverResponse_FullAssignment{
					FullAssignment: &streamingpb.FullStreamingNodeAssignmentWithVersion{
						Version: &streamingpb.VersionPair{Global: 2, Local: 3},
						Assignments: []*streamingpb.StreamingNodeAssignment{
							{
								Node:     &streamingpb.StreamingNodeInfo{ServerId: 1},
								Channels: []*streamingpb.PChannelInfo{{Name: "c1", Term: 1}, {Name: "c2", Term: 2}},
							},
							{
								Node:     &streamingpb.StreamingNodeInfo{ServerId: 2},
								Channels: []*streamingpb.PChannelInfo{{Name: "c3", Term: 1}, {Name: "c4", Term: 2}},
							},
						},
					},
				},
			},
			nil,
		}
		errs := []error{
			nil,
			nil,
			io.ErrUnexpectedEOF,
		}
		if k > len(resps) {
			return nil, io.EOF
		} else if k == len(resps) {
			<-closeCh
			k++
			return &streamingpb.AssignmentDiscoverResponse{
				Response: &streamingpb.AssignmentDiscoverResponse_Close{},
			}, nil
		}
		time.Sleep(25 * time.Millisecond)
		k++
		return resps[k-1], errs[k-1]
	})

	assignmentService := NewAssignmentService(s)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var finalAssignments *types.VersionedStreamingNodeAssignments
	err := assignmentService.AssignmentDiscover(ctx, func(vsna *types.VersionedStreamingNodeAssignments) error {
		finalAssignments = vsna
		return nil
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, finalAssignments.Version.EQ(typeutil.VersionInt64Pair{Global: 2, Local: 3}))

	assignmentService.ReportAssignmentError(ctx, types.PChannelInfo{Name: "c1", Term: 1}, errors.New("test"))

	// test close
	go close(closeCh)
	time.Sleep(10 * time.Millisecond)
	assignmentService.Close()

	// running assignment service should be closed too.
	err = assignmentService.AssignmentDiscover(ctx, func(vsna *types.VersionedStreamingNodeAssignments) error {
		return nil
	})
	se := status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, se.Code)

	err = assignmentService.ReportAssignmentError(ctx, types.PChannelInfo{Name: "c1", Term: 1}, errors.New("test"))
	se = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, se.Code)
}
