package querynode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type Client struct {
	ctx context.Context
	querypb.QueryNodeClient
}
