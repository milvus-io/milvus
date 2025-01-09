package discover

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestAssignmentDiscover(t *testing.T) {
	resource.InitForTest()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb func(typeutil.VersionInt64Pair, []types.PChannelInfoAssigned) error) error {
		versions := []typeutil.VersionInt64Pair{
			{Global: 1, Local: 2},
			{Global: 1, Local: 3},
		}
		pchans := [][]types.PChannelInfoAssigned{
			{
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
			},
			{
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel2", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
			},
		}
		for i := 0; i < len(versions); i++ {
			cb(versions[i], pchans[i])
		}
		<-ctx.Done()
		return context.Cause(ctx)
	})
	b.EXPECT().MarkAsUnavailable(mock.Anything, mock.Anything).Return(nil)

	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	k := 0
	reqs := []*streamingpb.AssignmentDiscoverRequest{
		{
			Command: &streamingpb.AssignmentDiscoverRequest_ReportError{
				ReportError: &streamingpb.ReportAssignmentErrorRequest{
					Pchannel: &streamingpb.PChannelInfo{
						Name: "pchannel",
						Term: 1,
					},
					Err: &streamingpb.StreamingError{
						Code: streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
					},
				},
			},
		},
		{
			Command: &streamingpb.AssignmentDiscoverRequest_Close{},
		},
	}
	streamServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.AssignmentDiscoverRequest, error) {
		if k >= len(reqs) {
			return nil, io.EOF
		}
		req := reqs[k]
		k++
		return req, nil
	})
	streamServer.EXPECT().Send(mock.Anything).Return(nil)
	ads := NewAssignmentDiscoverServer(b, streamServer)
	ads.Execute()
}
