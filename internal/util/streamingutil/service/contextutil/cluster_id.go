package contextutil

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

const clusterIDKey = "cluster-id"

// WithClusterID attaches cluster id to context.
func WithClusterID(ctx context.Context, clusterID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, clusterIDKey, clusterID)
}

// GetClusterID gets cluster id from context.
func GetClusterID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.NewInvalidArgument("cluster id not found from context")
	}
	msg := md.Get(clusterIDKey)
	if len(msg) == 0 {
		return "", status.NewInvalidArgument("cluster id not found in context")
	}
	if msg[0] == "" {
		return "", status.NewInvalidArgument("cluster id is empty")
	}
	return msg[0], nil
}
