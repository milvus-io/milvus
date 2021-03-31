package retry

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestImpl(t *testing.T) {
	attempts := 10
	sleep := time.Millisecond * 1
	maxSleepTime := time.Millisecond * 16

	var err error

	naiveF := func() error {
		return nil
	}
	err = Impl(attempts, sleep, naiveF, maxSleepTime)
	assert.Equal(t, err, nil)

	errorF := func() error {
		return errors.New("errorF")
	}
	err = Impl(attempts, sleep, errorF, maxSleepTime)
	assert.NotEqual(t, err, nil)

	begin := 0
	stop := 2
	interruptF := func() error {
		if begin >= stop {
			return NoRetryError(errors.New("interrupt here"))
		}
		begin++
		return errors.New("interruptF")
	}
	err = Impl(attempts, sleep, interruptF, maxSleepTime)
	assert.NotEqual(t, err, nil)

	begin = 0
	stop = attempts / 2
	untilSucceedF := func() error {
		if begin >= stop {
			return nil
		}
		begin++
		return errors.New("untilSucceedF")
	}
	err = Impl(attempts, sleep, untilSucceedF, maxSleepTime)
	assert.Equal(t, err, nil)

	begin = 0
	stop = attempts * 2
	noRetryF := func() error {
		if begin >= stop {
			return nil
		}
		begin++
		return errors.New("noRetryF")
	}
	err = Impl(attempts, sleep, noRetryF, maxSleepTime)
	assert.NotEqual(t, err, nil)
}
