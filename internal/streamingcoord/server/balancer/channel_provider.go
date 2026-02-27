package balancer

// ChannelProvider provides initial channels and ongoing notification
// of dynamically added PChannels.
type ChannelProvider interface {
	// GetInitialChannels returns the initial set of channel names
	// known at startup time. Called once during recovery.
	GetInitialChannels() []string

	// NewIncomingChannels returns a read-only channel that delivers
	// slices of newly added channel names. Each send contains only
	// names not previously sent. The channel is closed when the provider stops.
	NewIncomingChannels() <-chan []string

	// Close stops the provider and closes the notification channel.
	Close()
}
