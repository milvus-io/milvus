package metastore

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// StreamingCoordCataLog is the interface for streamingcoord catalog
// All write operation of catalog is reliable, the error will only be returned if the ctx is canceled,
// otherwise it will retry until success.
type StreamingCoordCataLog interface {
	// GetCChannel get the control channel from metastore.
	GetCChannel(ctx context.Context) (*streamingpb.CChannelMeta, error)

	// SaveCChannel save the control channel to metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SaveCChannel(ctx context.Context, info *streamingpb.CChannelMeta) error

	// GetVersion get the streaming version from metastore.
	GetVersion(ctx context.Context) (*streamingpb.StreamingVersion, error)

	// SaveVersion save the streaming version to metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SaveVersion(ctx context.Context, version *streamingpb.StreamingVersion) error

	// physical channel watch related

	// ListPChannel list all pchannels on milvus.
	ListPChannel(ctx context.Context) ([]*streamingpb.PChannelMeta, error)

	// SavePChannel save a pchannel info to metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SavePChannels(ctx context.Context, info []*streamingpb.PChannelMeta) error

	// ListBroadcastTask list all broadcast tasks.
	// Used to recovery the broadcast tasks.
	ListBroadcastTask(ctx context.Context) ([]*streamingpb.BroadcastTask, error)

	// SaveBroadcastTask save the broadcast task to metastore.
	// Make the task recoverable after restart.
	// When broadcast task is done, it will be removed from metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SaveBroadcastTask(ctx context.Context, broadcastID uint64, task *streamingpb.BroadcastTask) error

	// SaveReplicateConfiguration saves the replicate configuration to metastore.
	// Only return error if the ctx is canceled, otherwise it will retry until success.
	SaveReplicateConfiguration(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error

	// GetReplicateConfiguration gets the replicate configuration from metastore.
	GetReplicateConfiguration(ctx context.Context) (*streamingpb.ReplicateConfigurationMeta, error)
}
