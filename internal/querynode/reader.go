package querynode

import (
	"context"
)

func Init() {
	Params.Init()
}

func StartQueryNode(ctx context.Context) {
	node := NewQueryNode(ctx, 0)

	node.Start()
}
