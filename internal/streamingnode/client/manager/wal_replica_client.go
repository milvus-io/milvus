package manager

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// WALReplicaClient shares a session resolver and connection for all PChannels.
// The same session snapshot identifies both the writer and its physical node.
type WALReplicaClient struct {
	resolver resolver.Builder
	service  lazygrpc.Service[streamingpb.StreamingNodeWALReplicaServiceClient]
}

func NewWALReplicaClient(etcd *clientv3.Client) *WALReplicaClient {
	rb, conn := newSNConnection(etcd)
	return &WALReplicaClient{resolver: rb, service: lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingNodeWALReplicaServiceClient)}
}

func (c *WALReplicaClient) Discover(ctx context.Context) (resolver.VersionedState, error) {
	return c.resolver.Resolver().GetLatestState(ctx)
}

func WriterFor(state resolver.VersionedState, pchannel string) (*types.PChannelInfoAssigned, error) {
	var writer *types.PChannelInfoAssigned
	for node, session := range state.Sessions() {
		if session.Stopping {
			continue
		}
		for _, ad := range session.WALReplicas {
			if ad.Pchannel != pchannel || ad.GetWriterTerm() <= 0 {
				continue
			}
			if writer != nil {
				return nil, status.NewChannelNotExist(pchannel)
			}
			writer = &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: pchannel, WALReplicaID: ad.WalReplicaId, Term: ad.GetWriterTerm(), AccessMode: types.AccessModeRW}, Node: types.StreamingNodeInfo{ServerID: node, Address: session.Address}}
		}
	}
	if writer == nil {
		return nil, status.NewChannelNotExist(pchannel)
	}
	return writer, nil
}

func (c *WALReplicaClient) Watch(ctx context.Context, target *types.PChannelInfoAssigned, subscriber int64) (streamingpb.StreamingNodeWALReplicaService_WatchWALReplicaStateClient, error) {
	service, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := service.WatchWALReplicaState(contextutil.WithPickServerID(ctx, target.Node.ServerID))
	if err != nil {
		return nil, err
	}
	err = stream.Send(&streamingpb.WatchWALReplicaStateRequest{Request: &streamingpb.WatchWALReplicaStateRequest_Open{Open: &streamingpb.OpenWALReplicaWatch{Pchannel: target.Channel.Name, WriterReplicaId: target.Channel.WALReplicaID, WriterTerm: target.Channel.Term, SubscriberReplicaId: subscriber}}})
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (c *WALReplicaClient) Sync(ctx context.Context, nodeID int64) (streamingpb.StreamingNodeWALReplicaService_SyncWALReplicasClient, error) {
	service, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}
	return service.SyncWALReplicas(contextutil.WithPickServerID(ctx, nodeID))
}

func (c *WALReplicaClient) Close() {
	c.service.Close()
	c.resolver.Close()
}

func (c *WALReplicaClient) Prepare(ctx context.Context, source int64, caller int64, req *streamingpb.WALHandoffRequest) (uint64, error) {
	service, err := c.service.GetService(ctx)
	if err != nil {
		return 0, err
	}
	resp, err := service.PreparingWALHandoff(contextutil.WithPickServerID(contextutil.WithWALReplicaCaller(ctx, caller), source), req)
	return resp.GetPersistedTimeTick(), err
}

func (c *WALReplicaClient) Catchup(ctx context.Context, source int64, caller int64, req *streamingpb.WALHandoffRequest) error {
	service, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}
	_, err = service.WALHandoffCatchup(contextutil.WithPickServerID(contextutil.WithWALReplicaCaller(ctx, caller), source), req)
	return err
}

func (c *WALReplicaClient) Seal(ctx context.Context, source int64, caller int64, req *streamingpb.WALHandoffRequest) error {
	service, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}
	_, err = service.SealWALHandoff(contextutil.WithPickServerID(contextutil.WithWALReplicaCaller(ctx, caller), source), req)
	return err
}
