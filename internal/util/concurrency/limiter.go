package concurrency

type Limiter struct {
	ch chan struct{}
}

// Create a new limiter with given concurrency level
func NewLimiter(limit int) *Limiter {
	return &Limiter{
		ch: make(chan struct{}, limit),
	}
}

// Acquire to spawn a new goroutine to run task,
// this blocks until internal channel is not full
// NOTE: call Release() after the task finished
func (limiter *Limiter) Acquire() {
	limiter.ch <- struct{}{}
}

// Use case: try acquire
func (limiter *Limiter) Inner() chan<- struct{} {
	return limiter.ch
}

// Release a goroutine
func (limiter *Limiter) Release() {
	<-limiter.ch
}

// The number of running tasks
func (limiter *Limiter) Len() int {
	return len(limiter.ch)
}

// The concurrency level
func (limiter *Limiter) Cap() int {
	return cap(limiter.ch)
}
