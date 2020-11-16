package reader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gParams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
)

const ctxTimeInMillisecond = 2000
const closeWithDeadline = true

// NOTE: start pulsar and etcd before test
func TestQueryNode_start(t *testing.T) {
	err := gParams.GParams.LoadYaml("config.yaml")
	assert.NoError(t, err)

	var ctx context.Context
	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	pulsarAddr, _ := gParams.GParams.Load("pulsar.address")
	pulsarPort, _ := gParams.GParams.Load("pulsar.port")
	pulsarAddr += ":" + pulsarPort
	pulsarAddr = "pulsar://" + pulsarAddr

	node := NewQueryNode(ctx, 0, pulsarAddr)
	node.Start()
}
