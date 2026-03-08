package consumer

var _ Consumer = (*consumerImpl)(nil)

// Consumer is the interface that wraps the basic consume method on grpc stream.
// Consumer is work on a single stream on grpc,
// so Consumer cannot recover from failure because of the stream is broken.
type Consumer interface {
	// Error returns the error of scanner failed.
	// Will block until scanner is closed or Chan is dry out.
	Error() error

	// Done returns a channel which will be closed when scanner is finished or closed.
	Done() <-chan struct{}

	// Close the consumer, release the underlying resources.
	Close() error
}
