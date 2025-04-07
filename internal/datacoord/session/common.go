package session

import (
	"context"

	"github.com/milvus-io/milvus/internal/types"
)

type DataNodeCreatorFunc func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error)
