package streamingnode

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/util/netutil"
)

func TestStartGRPCServerCancelledContextReturnsAndStopsCleanly(t *testing.T) {
	listener, err := netutil.NewListener(netutil.OptIP("127.0.0.1"))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		ctx:        ctx,
		cancel:     cancel,
		listener:   listener,
		grpcServer: grpc.NewServer(),
	}

	cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.startGPRCServer(ctx)
	}()

	select {
	case startErr := <-done:
		require.ErrorIs(t, startErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("startGPRCServer did not return after startup context cancellation")
	}

	stopDone := make(chan struct{})
	go func() {
		s.stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not finish after startGPRCServer cancellation")
	}
	assert.NotNil(t, s.grpcServer)
}
