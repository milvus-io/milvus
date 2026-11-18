package handler

import (
	"context"
	"net"
	"path"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/json"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
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
		availability: balancer.NewNodeAvailability(),
		lifetime:     typeutil.NewLifetime(),
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
		availability: balancer.NewNodeAvailability(),
		lifetime:     typeutil.NewLifetime(),
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

func TestMissingSessionCarriesPickerRevision(t *testing.T) {
	owner := &clientImpl{lifetime: typeutil.NewLifetime(), availability: balancer.NewNodeAvailability()}
	_, changed := owner.UnavailableNodes()
	service := &fakeViewQueryServiceClient{}
	patch := mockey.Mock((*fakeViewQueryServiceClient).QueryOnView).To(func(_ *fakeViewQueryServiceClient, ctx context.Context, _ *viewpb.QueryOnViewRequest, _ ...grpc.CallOption) (*viewpb.QueryOnViewResponse, error) {
		require.True(t, contextutil.IsFailFastPick(ctx))
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

// A ready peer keeps the ClientConn ready while the other QN connects. Its
// SubConn transition must update routing without another resolver publication.
func TestQueryNodePickerReportsReadinessByNodeID(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	etcd, root := kvfactory.GetEtcdAndPath()
	prefix := path.Join(root, "picker-test", funcutil.RandomString(12))
	patch := mockey.Mock(sessionutil.GetSessionPrefixByRole).Return(prefix).Build()
	defer patch.UnPatch()
	register := func(id int64) (net.Listener, string) {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = listener.Close() })
		key := path.Join(prefix, strconv.FormatInt(id, 10))
		data, err := json.Marshal(sessionutil.SessionRaw{ServerID: id, Address: listener.Addr().String(), Version: "3.0.0"})
		require.NoError(t, err)
		_, err = etcd.Put(ctx, key, string(data))
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			_, err := etcd.Delete(cleanup, key)
			require.NoError(t, err)
		})
		return listener, key
	}
	serve := func(listener net.Listener) *grpc.Server {
		server := grpc.NewServer()
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		return server
	}
	qn11, key11 := register(11)
	qn22, _ := register(22)
	serve(qn11)
	client := NewClient(etcd)
	t.Cleanup(client.Close)
	waitFor := func(want []int64) {
		t.Helper()
		for {
			unavailable, changed := client.UnavailableNodes()
			if slices.Equal(unavailable, want) {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("unavailable %v, want %v", unavailable, want)
			case <-changed:
			}
		}
	}
	waitFor([]int64{22})
	server22 := serve(qn22)
	waitFor(nil)
	// Deleting QN11 changes membership even though both snapshots have no
	// unready nodes. A failed pick can now confirm this exact session is gone.
	_, revision := client.UnavailableNodes()
	_, err := etcd.Delete(ctx, key11)
	require.NoError(t, err)
	select {
	case <-revision:
	case <-ctx.Done():
		t.Fatal("picker missed node removal")
	}
	require.Eventually(t, func() bool {
		_, err := client.QueryViewClient().SearchOnView(ctx, 11, &viewpb.SearchOnViewRequest{})
		var missing *viewerror.NodeSessionUnavailable
		return errors.As(err, &missing) && missing.Gone
	}, 5*time.Second, 10*time.Millisecond)
	server22.Stop()
	waitFor([]int64{22})
}
