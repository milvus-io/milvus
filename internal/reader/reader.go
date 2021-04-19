package reader

import (
	"context"
)

func Init() {
	Params.Init()
}

func StartQueryNode(ctx context.Context, pulsarURL string) {
	node := NewQueryNode(ctx, 0, pulsarURL)

	node.Start()
}
