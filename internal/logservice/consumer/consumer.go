package consumer

var _ ResumableConsumer = (*resumableConsumerImpl)(nil)

// ResumableConsumer is the interface for consuming message to log service.
// ResumableConsumer select a right log node to consume automatically.
// ResumableConsumer will do automatic resume from stream broken and log node re-balance.
// All error in these package should be marked by logservice/errs package.
type ResumableConsumer interface {
	// Channel returns the channel of the consumer.
	Channel() string

	// Done returns a channel which will be closed when scanner is finished or closed.
	Done() <-chan struct{}

	// Close the scanner, release the underlying resources.
	// Return the error same with `Error`
	Close()
}
