package reader

import (
	"context"
	"testing"
	"time"
)

const ctxTimeInMillisecond = 2000
const closeWithDeadline = true

// NOTE: start pulsar and etcd before test
func TestQueryNode_start(t *testing.T) {
	Params.Init()

	var ctx context.Context
	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	pulsarAddr, err := Params.PulsarAddress()
	if err != nil {
		panic(err)
	}
	node := NewQueryNode(ctx, 0, "pulsar://"+pulsarAddr)
	node.Start()
}
