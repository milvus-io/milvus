package process

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestGetAddressReturnsBeforeProcessJoin(t *testing.T) {
	notifier := syncutil.NewAsyncTaskNotifier[error]()
	mp := &MilvusProcess{notifier: notifier}
	// Process exit cancels the workers before background can join them and
	// finish the notifier. Address discovery must return without that result.
	notifier.Cancel()
	t.Cleanup(func() { notifier.Finish(nil) })
	done := make(chan error, 1)
	go func() {
		_, err := mp.GetAddress(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("address discovery is waiting for the worker join that depends on it")
	}
}
