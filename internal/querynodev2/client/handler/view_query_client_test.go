package handler

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestClientSearchOnViewRoutesByQueryNodeID(t *testing.T) {
	searchReq := &viewpb.SearchOnViewRequest{}
	searchResp := &viewpb.SearchOnViewResponse{}
	service := &fakeViewQueryServiceClient{
		searchOnView: func(ctx context.Context, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(11), serverID)
			require.Same(t, searchReq, req)
			return searchResp, nil
		},
	}
	client := &clientImpl{
		lifetime: typeutil.NewLifetime(),
	}
	client.queryViewClient = &queryViewClient{
		owner:   client,
		service: fakeLazyService[viewpb.ViewQueryServiceClient]{service: service},
	}

	resp, err := client.QueryViewClient().SearchOnView(context.Background(), 11, searchReq)

	require.NoError(t, err)
	require.Same(t, searchResp, resp)
}

func TestClientPreservesViewQueryRPCError(t *testing.T) {
	viewErr := viewerror.NewViewInvalidated("stale view")
	service := &fakeViewQueryServiceClient{
		queryOnView: func(context.Context, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
			return nil, viewerror.ConvertViewError(viewpb.ViewQueryService_QueryOnView_FullMethodName, viewerror.NewGRPCStatusFromViewError(viewErr).Err())
		},
	}
	client := &clientImpl{
		lifetime: typeutil.NewLifetime(),
	}
	client.queryViewClient = &queryViewClient{
		owner:   client,
		service: fakeLazyService[viewpb.ViewQueryServiceClient]{service: service},
	}

	_, err := client.QueryViewClient().QueryOnView(context.Background(), 11, &viewpb.QueryOnViewRequest{})

	require.Error(t, err)
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
	require.Nil(t, viewerror.RPCContextFromDetails(status.Convert(err).Details()))
}

type fakeLazyService[T any] struct {
	service T
}

func (s fakeLazyService[T]) GetConn(context.Context) (*grpc.ClientConn, error) {
	return nil, nil
}

func (s fakeLazyService[T]) GetService(context.Context) (T, error) {
	return s.service, nil
}

func (s fakeLazyService[T]) Close() {}

type fakeViewQueryServiceClient struct {
	viewpb.ViewQueryServiceClient
	searchOnView func(context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	queryOnView  func(context.Context, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
}

func (c *fakeViewQueryServiceClient) SearchOnView(ctx context.Context, req *viewpb.SearchOnViewRequest, _ ...grpc.CallOption) (*viewpb.SearchOnViewResponse, error) {
	return c.searchOnView(ctx, req)
}

func (c *fakeViewQueryServiceClient) QueryOnView(ctx context.Context, req *viewpb.QueryOnViewRequest, _ ...grpc.CallOption) (*viewpb.QueryOnViewResponse, error) {
	return c.queryOnView(ctx, req)
}

func TestMissingSessionCarriesPreDispatchResolverRevision(t *testing.T) {
	changed := make(chan struct{})
	owner := &clientImpl{lifetime: typeutil.NewLifetime(), sessionChanges: &sessionChanges{changed: changed}}
	service := &fakeViewQueryServiceClient{}
	patch := mockey.Mock((*fakeViewQueryServiceClient).QueryOnView).To(func(_ *fakeViewQueryServiceClient, ctx context.Context, _ *viewpb.QueryOnViewRequest, _ ...grpc.CallOption) (*viewpb.QueryOnViewResponse, error) {
		require.True(t, contextutil.IsFailFastPick(ctx))
		// An update before the RPC returns must wake the consumer of this error.
		owner.sessionChanges.mu.Lock()
		close(changed)
		owner.sessionChanges.changed = make(chan struct{})
		owner.sessionChanges.mu.Unlock()
		return nil, picker.ErrSubConnNotExist
	}).Build()
	defer patch.UnPatch()
	client := &queryViewClient{owner: owner, service: fakeLazyService[viewpb.ViewQueryServiceClient]{service: service}}
	_, err := client.QueryOnView(t.Context(), 11, &viewpb.QueryOnViewRequest{})
	var unavailable *viewerror.NodeSessionUnavailable
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, (<-chan struct{})(changed), unavailable.Changed)
	require.True(t, picker.IsErrSubConnNoExist(unavailable.Cause))
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotEmpty(t, err.Error())
	select {
	case <-unavailable.Changed:
	default:
		t.Fatal("resolver update lost")
	}
}

// Local admission failures must not be attributed to the selected remote node.
func TestClientAdmissionFailureHasNoRPCContext(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		owner := &clientImpl{lifetime: typeutil.NewLifetime()}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if stopped {
			owner.lifetime.SetState(typeutil.LifetimeStateStopped)
		} else {
			cancel()
		}
		client := &queryViewClient{owner: owner}
		_, err := client.SearchOnView(ctx, 11, &viewpb.SearchOnViewRequest{})
		require.Error(t, err)
		require.Nil(t, viewerror.RPCContextFromDetails(status.Convert(err).Details()))
		if stopped {
			require.True(t, viewerror.AsViewError(err).IsOnShutdown())
		} else {
			require.Same(t, context.Canceled, err)
		}
	}
}
