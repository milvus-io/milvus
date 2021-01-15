package indexbuilder

import (
	"log"
	"time"
)

// Reference: https://blog.cyeam.com/golang/2018/08/27/retry

func RetryImpl(attempts int, sleep time.Duration, fn func() error, maxSleepTime time.Duration) error {
	if err := fn(); err != nil {
		if s, ok := err.(InterruptError); ok {
			return s.error
		}

		if attempts--; attempts > 0 {
			log.Printf("retry func error: %s. attempts #%d after %s.", err.Error(), attempts, sleep)
			time.Sleep(sleep)
			if sleep < maxSleepTime {
				return RetryImpl(attempts, 2*sleep, fn, maxSleepTime)
			}
			return RetryImpl(attempts, maxSleepTime, fn, maxSleepTime)
		}
		return err
	}
	return nil
}

func Retry(attempts int, sleep time.Duration, fn func() error) error {
	maxSleepTime := time.Millisecond * 1000
	return RetryImpl(attempts, sleep, fn, maxSleepTime)
}

type InterruptError struct {
	error
}

func NoRetryError(err error) InterruptError {
	return InterruptError{err}
}
