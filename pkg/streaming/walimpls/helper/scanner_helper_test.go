package helper

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestScanner(t *testing.T) {
	h := NewScannerHelper("test")
	assert.NotNil(t, h.Context())
	assert.Equal(t, h.Name(), "test")
	assert.NotNil(t, h.Context())

	select {
	case <-h.Done():
		t.Errorf("should not done")
		return
	case <-h.Context().Done():
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
		cancelCh := h.Context().Done()
		doneCh := h.Done()
		for i := 0; ; i += 1 {
			select {
			case <-doneCh:
				done = true
				doneCh = nil
			case <-cancelCh:
				cancel = true
				cancelCh = nil
				h.Finish(finishErr)
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
	h.Close()
	assert.ErrorIs(t, h.Error(), finishErr)
	<-ch
}
