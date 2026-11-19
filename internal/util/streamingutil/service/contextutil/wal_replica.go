package contextutil

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"
)

const walReplicaCallerKey = "wal-replica-caller-node-id"

func WithWALReplicaCaller(ctx context.Context, node int64) context.Context {
	return metadata.AppendToOutgoingContext(ctx, walReplicaCallerKey, strconv.FormatInt(node, 10))
}

func WALReplicaCaller(ctx context.Context) int64 {
	values := metadata.ValueFromIncomingContext(ctx, walReplicaCallerKey)
	if len(values) != 1 {
		return 0
	}
	node, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return 0
	}
	return node
}
