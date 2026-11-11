package handler

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	grpcresolver "google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type statsDiscoveryResolver struct{ resolver.Resolver }

func (*statsDiscoveryResolver) Watch(context.Context, func(discoverer.VersionedState) error) error {
	panic("mock with mockey")
}

type statsDiscoveryBuilder struct{ resolver.Builder }

func (*statsDiscoveryBuilder) Resolver() resolver.Resolver { panic("mock with mockey") }

func TestShardStatsPChannelDiscovery(t *testing.T) {
	mockey.PatchConvey("uses assignment snapshots, not pending views", t, func() {
		r := &statsDiscoveryResolver{}
		mockey.Mock((*statsDiscoveryBuilder).Resolver).Return(r).Build()
		mockey.Mock((*statsDiscoveryResolver).Watch).To(func(_ *statsDiscoveryResolver, ctx context.Context, cb func(discoverer.VersionedState) error) error {
			require.NoError(t, cb(discoverer.VersionedState{}))
			state := discoverer.VersionedState{State: grpcresolver.State{Addresses: []grpcresolver.Address{{Addr: "sn", BalancerAttributes: attributes.WithChannelAssignmentInfo(new(attributes.Attributes), &types.StreamingNodeAssignment{NodeInfo: types.StreamingNodeInfo{ServerID: 1}, Channels: map[string]types.PChannelInfo{"p": {Name: "p"}, "q": {Name: "q"}}})}}}}
			require.NoError(t, cb(state))
			return context.Canceled
		}).Build()
		client := &queryViewSyncClient{owner: &handlerClientImpl{rb: &statsDiscoveryBuilder{}}}
		var snapshots [][]string
		err := client.WatchPChannels(t.Context(), func(channels []string) { snapshots = append(snapshots, channels) })
		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, snapshots, 2)
		require.Empty(t, snapshots[0])
		require.ElementsMatch(t, []string{"p", "q"}, snapshots[1])
	})
}
