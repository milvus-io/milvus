package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

type demandHandler struct{ wal.TransformLogEventHandler }

func (*demandHandler) Close() { panic("mockey") }

func TestNewDemandSurvivesPreviousAttemptCleanup(t *testing.T) {
	patch := mockey.Mock((*demandHandler).Close).Return().Build()
	defer patch.UnPatch()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, stopped, proceed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	secondAttempt := make(chan context.Context, 1)
	calls := 0
	stream := NewResumableStream(ctx, "p", func(attempt context.Context, _ string) (wal.TransformLogStream, error) {
		calls++
		if calls == 1 {
			close(entered)
			<-attempt.Done()
			close(stopped)
			select {
			case <-proceed:
			case <-ctx.Done():
			}
		} else {
			secondAttempt <- attempt
			<-attempt.Done()
		}
		return nil, attempt.Err()
	}).(*resumableStream)
	defer stream.Close()
	first := stream.newSubscription(wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: &demandHandler{}})
	require.NotNil(t, first)
	stream.wakeResume()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("missing first attempt")
	}
	require.NoError(t, first.Close())
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("idle attempt was not canceled")
	}
	// Register the new logical subscription while the canceled factory is
	// still returning. Its wakeup must survive that attempt's cleanup.
	second := stream.newSubscription(wal.TransformLogSubscriptionOption{VChannel: "p_2v0", Handler: &demandHandler{}})
	require.NotNil(t, second)
	stream.wakeResume()
	close(proceed)
	select {
	case attempt := <-secondAttempt:
		require.NoError(t, attempt.Err(), "old cancellation affected the new connection")
	case <-ctx.Done():
		t.Fatal("new demand was lost")
	}
	require.NoError(t, stream.Close())
}
