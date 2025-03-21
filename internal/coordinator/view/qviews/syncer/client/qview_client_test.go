package client

import (
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_assignment"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_viewpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestQViewServiceClient(t *testing.T) {
	qnSyncService := newMockSyncService(t)
	snSyncService := newMockSyncService(t)
	assignmentWatcher := mock_assignment.NewMockWatcher(t)
	assignmentWatcher.EXPECT().Watch(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, pia *types.PChannelInfoAssigned) error {
			return ctx.Err()
		}).Maybe()
	assignmentWatcher.EXPECT().Get(mock.Anything, mock.Anything).Return(nil).RunAndReturn(
		func(ctx context.Context, s string) *types.PChannelInfoAssigned {
			return &types.PChannelInfoAssigned{}
		}).Maybe()

	client := NewQueryViewServiceClient(qnSyncService.Service, snSyncService.Service, assignmentWatcher)

	// Test sn syncer
	mockedSyncer := snSyncService.CreateNewMockSyncer(t)
	recv := make(chan SyncMessage, 100)
	syncer := client.CreateSyncer(SyncOption{
		WorkNode: qviews.NewStreamingNodeFromVChannel("v1"),
		Receiver: recv,
	})
	syncer.SyncAtBackground(&viewpb.SyncQueryViewsRequest{
		QueryViews: []*viewpb.QueryViewOfShard{},
	})
	mockedSyncer.Recv <- &viewpb.SyncResponse{
		Response: &viewpb.SyncResponse_Views{},
	}
	resp := <-recv
	assert.Equal(t, resp.GetWorkNode(), qviews.NewStreamingNodeFromVChannel("v1"))
	_ = resp.(SyncResponseMessage)
	syncer.Close()

	// Test qn syncer
	mockedSyncer = qnSyncService.CreateNewMockSyncer(t)
	recv = make(chan SyncMessage, 100)
	syncer = client.CreateSyncer(SyncOption{
		WorkNode: qviews.NewQueryNode(4),
		Receiver: recv,
	})
	syncer.SyncAtBackground(&viewpb.SyncQueryViewsRequest{
		QueryViews: []*viewpb.QueryViewOfShard{},
	})
	mockedSyncer.Recv <- &viewpb.SyncResponse{
		Response: &viewpb.SyncResponse_Views{},
	}
	resp = <-recv
	assert.Equal(t, resp.GetWorkNode(), qviews.NewQueryNode(4))
	_ = resp.(SyncResponseMessage)
	syncer.Close()

	// Test sn error path
	err := errors.New("error")
	mockedSyncer = snSyncService.CreateNewMockSyncer(t)
	recv = make(chan SyncMessage, 100)
	syncer = client.CreateSyncer(SyncOption{
		WorkNode: qviews.NewStreamingNodeFromVChannel("v1"),
		Receiver: recv,
	})
	syncer.SyncAtBackground(&viewpb.SyncQueryViewsRequest{
		QueryViews: []*viewpb.QueryViewOfShard{},
	})
	mockedSyncer.Recv <- err
	resp = <-recv
	assert.Equal(t, resp.GetWorkNode(), qviews.NewStreamingNodeFromVChannel("v1"))
	assert.Error(t, resp.(SyncErrorMessage).Error)
	syncer.SyncAtBackground(&viewpb.SyncQueryViewsRequest{
		QueryViews: []*viewpb.QueryViewOfShard{},
	})
	syncer.Close()

	// Test qn error path
	mockedSyncer = qnSyncService.CreateNewMockSyncer(t)
	recv = make(chan SyncMessage, 100)
	syncer = client.CreateSyncer(SyncOption{
		WorkNode: qviews.NewQueryNode(4),
		Receiver: recv,
	})
	syncer.SyncAtBackground(&viewpb.SyncQueryViewsRequest{
		QueryViews: []*viewpb.QueryViewOfShard{},
	})
	mockedSyncer.Recv <- err
	resp = <-recv
	assert.Equal(t, resp.GetWorkNode(), qviews.NewQueryNode(4))
	assert.Error(t, resp.(SyncErrorMessage).Error)
	syncer.SyncAtBackground(&viewpb.SyncQueryViewsRequest{
		QueryViews: []*viewpb.QueryViewOfShard{},
	})
	syncer.Close()

	// Test qn gone
	qnSyncService.Client.EXPECT().Sync(mock.Anything).Unset()
	qnSyncService.Client.EXPECT().Sync(mock.Anything).Return(nil, picker.ErrSubConnNotExist).Maybe()
	recv = make(chan SyncMessage, 100)
	syncer = client.CreateSyncer(SyncOption{
		WorkNode: qviews.NewQueryNode(4),
		Receiver: recv,
	})
	resp = <-recv
	assert.True(t, errors.Is(resp.(SyncErrorMessage).Error, ErrNodeGone))
	syncer.Close()

	// Test close path
	mockedSyncer = qnSyncService.CreateNewMockSyncer(t)
	recv = make(chan SyncMessage, 100)
	syncer = client.CreateSyncer(SyncOption{
		WorkNode: qviews.NewQueryNode(4),
		Receiver: recv,
	})
	syncer.Close()
	mockedSyncer.Recv <- err
	mockedSyncer.Recv <- err
	mockedSyncer.Recv <- err
	mockedSyncer.Recv <- err
}

type mockSyncService struct {
	Client  *mock_viewpb.MockQueryViewSyncServiceClient
	Service *mock_lazygrpc.MockService[viewpb.QueryViewSyncServiceClient]
}

type mockSyncer struct {
	Client *mock_viewpb.MockQueryViewSyncService_SyncClient
	Recv   chan interface{}
}

func newMockSyncService(t *testing.T) *mockSyncService {
	syncServiceClient := mock_viewpb.NewMockQueryViewSyncServiceClient(t)
	s := &mockSyncService{
		Client:  syncServiceClient,
		Service: mock_lazygrpc.NewMockService[viewpb.QueryViewSyncServiceClient](t),
	}
	s.Service.EXPECT().GetService(mock.Anything).Return(syncServiceClient, nil).Maybe()
	return s
}

func (s *mockSyncService) CreateNewMockSyncer(t *testing.T) *mockSyncer {
	syncClient := mock_viewpb.NewMockQueryViewSyncService_SyncClient(t)
	syncClient.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	closed := make(chan struct{})
	syncClient.EXPECT().CloseSend().RunAndReturn(func() error {
		close(closed)
		return nil
	})
	recvCh := make(chan interface{}, 1000)
	syncClient.EXPECT().Recv().RunAndReturn(func() (*viewpb.SyncResponse, error) {
		select {
		case <-closed:
			return nil, io.EOF
		case resp := <-recvCh:
			switch resp := resp.(type) {
			case *viewpb.SyncResponse:
				return resp, nil
			case error:
				return nil, resp
			default:
				panic("unknown type")
			}
		}
	}).Maybe()
	s.Client.EXPECT().Sync(mock.Anything).Unset()
	s.Client.EXPECT().Sync(mock.Anything).Return(syncClient, nil).Maybe()

	return &mockSyncer{
		Client: syncClient,
		Recv:   recvCh,
	}
}
