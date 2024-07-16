package syncutil

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestAsyncTaskNotifier(t *testing.T) {
	n := NewAsyncTaskNotifier[error]()
	assert.NotNil(t, n.Context())

	select {
	case <-n.FinishChan():
		t.Errorf("should not done")
		return
	case <-n.Context().Done():
		t.Error("should not cancel")
		return
	default:
	}

	finishErr := errors.New("test")

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		done := false
		cancel := false
		cancelCh := n.Context().Done()
		doneCh := n.FinishChan()
		for i := 0; ; i += 1 {
			select {
			case <-doneCh:
				done = true
				doneCh = nil
			case <-cancelCh:
				cancel = true
				cancelCh = nil
				n.Finish(finishErr)
			}
			if cancel && done {
				return
			}
			if i == 0 {
				assert.True(t, cancel && !done)
			} else if i == 1 {
				assert.True(t, cancel && done)
			}
		}
	}()
	n.Cancel()
	n.BlockUntilFinish()
	assert.ErrorIs(t, n.BlockAndGetResult(), finishErr)
	<-ch
}
