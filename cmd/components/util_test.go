package components

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestExitWithTimeout(t *testing.T) {
	// only normal path can be tested.
	targetErr := errors.New("stop error")
	err := exitWhenStopTimeout(func() error {
		time.Sleep(1 * time.Second)
		return targetErr
	}, 5*time.Second)
	assert.ErrorIs(t, err, targetErr)
}

func TestStopWithTimeout(t *testing.T) {
	ch := make(chan struct{})
	stop := func() error {
		<-ch
		return nil
	}

	err := stopWithTimeout(stop, 1*time.Second)
	assert.ErrorIs(t, err, errStopTimeout)

	targetErr := errors.New("stop error")
	stop = func() error {
		return targetErr
	}

	err = stopWithTimeout(stop, 1*time.Second)
	assert.ErrorIs(t, err, targetErr)
}
