package consts

import "time"

const (
	// DefaultServerMaxSendSize defines the maximum size of data per grpc request can send by server side.
	DefaultServerMaxSendSize = 512 * 1024 * 1024

	// DefaultServerMaxRecvSize defines the maximum size of data per grpc request can receive by server side.
	DefaultServerMaxRecvSize = 256 * 1024 * 1024

	// DefaultClientMaxSendSize defines the maximum size of data per grpc request can send by client side.
	DefaultClientMaxSendSize = 256 * 1024 * 1024

	// DefaultClientMaxRecvSize defines the maximum size of data per grpc request can receive by client side.
	DefaultClientMaxRecvSize = 512 * 1024 * 1024

	// DefaultLogLevel defines the log level of grpc
	DefaultLogLevel = "WARNING"

	// Grpc Timeout related configs
	DefaultDialTimeout      = 200 * time.Millisecond
	DefaultKeepAliveTime    = 10000 * time.Millisecond
	DefaultKeepAliveTimeout = 20000 * time.Millisecond

	// Grpc retry policy
	DefaultMaxAttempts                = 10
	DefaultInitialBackoff     float64 = 0.2
	DefaultMaxBackoff         float64 = 10
	DefaultCompressionEnabled bool    = false
)
