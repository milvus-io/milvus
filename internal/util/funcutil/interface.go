package funcutil

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type StateComponent interface {
	GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error)
}
