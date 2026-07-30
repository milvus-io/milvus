package discover

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_manager"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAssignmentDiscover(t *testing.T) {
	mc := mock_manager.NewMockManagerClient(t)
	mc.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg1"},
	}, nil)
	resource.InitForTest(resource.OptStreamingManagerClient(mc))
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
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
			cb(balancer.WatchChannelAssignmentsCallbackParam{
				Version:            versions[i],
				CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "pchannel"}},
				Relations:          pchans[i],
			})
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

func TestMarkUnavailableLoopBatchesAndDeduplicates(t *testing.T) {
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().MarkAsUnavailable(mock.Anything, mock.MatchedBy(func(channels []types.PChannelInfo) bool {
		return len(channels) == 3 &&
			channels[0].Name == "pchannel-1" && channels[0].Term == 2 &&
			channels[1].Name == "pchannel-2" && channels[1].Term == 1 &&
			channels[2].Name == "pchannel-3" && channels[2].Term == 1
	})).Return(nil).Once()

	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	ads := NewAssignmentDiscoverServer(b, streamServer)

	unavailableCh := make(chan types.PChannelInfo, 4)
	unavailableCh <- types.PChannelInfo{Name: "pchannel-1", Term: 1}
	unavailableCh <- types.PChannelInfo{Name: "pchannel-2", Term: 1}
	unavailableCh <- types.PChannelInfo{Name: "pchannel-1", Term: 2}
	unavailableCh <- types.PChannelInfo{Name: "pchannel-3", Term: 1}
	close(unavailableCh)

	ads.markUnavailableLoop(unavailableCh)
}

func TestMarkUnavailableLoopFlushesSeparateBursts(t *testing.T) {
	b := mock_balancer.NewMockBalancer(t)
	batches := make(chan []types.PChannelInfo, 2)
	b.EXPECT().MarkAsUnavailable(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, channels []types.PChannelInfo) error {
		batches <- channels
		return nil
	}).Twice()

	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	ads := NewAssignmentDiscoverServer(b, streamServer)

	unavailableCh := make(chan types.PChannelInfo)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ads.markUnavailableLoop(unavailableCh)
	}()

	unavailableCh <- types.PChannelInfo{Name: "pchannel-1", Term: 1}
	firstBatch := <-batches
	unavailableCh <- types.PChannelInfo{Name: "pchannel-2", Term: 1}
	close(unavailableCh)
	<-done
	secondBatch := <-batches

	if len(firstBatch) != 1 || firstBatch[0].Name != "pchannel-1" ||
		len(secondBatch) != 1 || secondBatch[0].Name != "pchannel-2" {
		t.Fatalf("expected separate single-channel batches, got first=%v second=%v", firstBatch, secondBatch)
	}
}
