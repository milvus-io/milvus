package msgstream

import (
	"log"
	"time"
)

// Reference: https://blog.cyeam.com/golang/2018/08/27/retry

func Retry(attempts int, sleep time.Duration, fn func() error) error {
	if err := fn(); err != nil {
		if s, ok := err.(InterruptError); ok {
			return s.error
		}

		if attempts--; attempts > 0 {
			log.Printf("retry func error: %s. attempts #%d after %s.", err.Error(), attempts, sleep)
			time.Sleep(sleep)
			return Retry(attempts, 2*sleep, fn)
		}
		return err
	}
	return nil
}

type InterruptError struct {
	error
}

func NoRetryError(err error) InterruptError {
	return InterruptError{err}
}
