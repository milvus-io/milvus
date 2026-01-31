package contextutil

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
		return "", merr.WrapErrServiceInternalMsg("cluster id not found from context")
	}
	msg := md.Get(clusterIDKey)
	if len(msg) == 0 {
		return "", merr.WrapErrServiceInternalMsg("cluster id not found in context")
	}
	if msg[0] == "" {
		return "", merr.WrapErrServiceInternalMsg("cluster id is empty")
	}
	return msg[0], nil
}
