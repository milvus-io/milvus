package viewresource

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSchedulerSubmitAfterCloseCompletesTask(t *testing.T) {
	scheduler := NewScheduler(1)
	scheduler.Close()

	task := newResourceBuildTask(context.Background(), func(context.Context) (*QueryRuntime, error) {
		return nil, errors.New("unexpected build")
	})

	scheduler.Submit(task)
	select {
	case <-task.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for canceled task")
	}
	runtime, err := task.Result()
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, runtime)
}
