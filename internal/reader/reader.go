package reader

import (
	"context"
)

func StartQueryNode(ctx context.Context, pulsarURL string) {
	node := NewQueryNode(ctx, 0, pulsarURL)

	node.Start()
}
