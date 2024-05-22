package typeutil

var (
	_ ChanSignalNotifier[struct{}] = (*ChanSignal[struct{}])(nil)
	_ ChanSignalListener[struct{}] = (*ChanSignal[struct{}])(nil)
)

// ChanSignalNotifier is a signal channel notifier.
type ChanSignalNotifier[T any] interface {
	MustNotify(t T)

	Notify(t T) bool

	Release()
}

// ChanSignalListener is a signal channel listener.
type ChanSignalListener[T any] interface {
	Chan() <-chan T
}

// NewChanSignal creates a new signal channel.
// The channel is buffered with size 1, so it will never block the notifier.
// Example:
// s := NewChanSignal()
// defer s.Release()
func NewChanSignal[T any]() *ChanSignal[T] {
	return &ChanSignal[T]{
		ch: make(chan T, 1),
	}
}

// ChanSignal is a signal channel.
// Recv zero or one signal from this channel.
// Notify method is not concurrent safe with other Notify method.
type ChanSignal[T any] struct {
	ch   chan T
	sent bool
}

// MustNotify sends a signal to the channel.
// Panic if the channel has been notified.
func (c *ChanSignal[T]) MustNotify(t T) {
	if !c.Notify(t) {
		panic("notify multiple times")
	}
}

// Notify sends a signal to the channel.
// Return false if the channel has been notified.
func (c *ChanSignal[T]) Notify(t T) bool {
	if c.sent {
		return false
	}
	c.ch <- t
	c.sent = true
	return true
}

// Chan returns the underlying channel.
func (c *ChanSignal[T]) Chan() <-chan T {
	return c.ch
}

// Release releases the underlying channel.
// Release should be called after the channel is allocated.
func (c *ChanSignal[T]) Release() {
	close(c.ch)
}
