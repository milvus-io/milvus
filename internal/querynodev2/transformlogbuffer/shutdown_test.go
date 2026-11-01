package transformlogbuffer

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestConfiguredBufferCloseJoinsWorkers(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b := NewConfigured(ctx, newFakeStreamManager())
	var closed sync.WaitGroup
	for range 2 {
		closed.Go(b.Close)
	}
	cancel() // Context cancellation and explicit owner close may race.
	closed.Wait()
	b.drainMu.Lock()
	require.True(t, b.drainClosed)
	require.Zero(t, b.drainWorkers)
	b.drainMu.Unlock()
}
