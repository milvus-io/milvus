package grpcconfigs

import "math"

const (
	DefaultServerMaxSendSize = math.MaxInt32
	DefaultServerMaxRecvSize = math.MaxInt32
	DefaultClientMaxSendSize = 100 * 1024 * 1024
	DefaultClientMaxRecvSize = 100 * 1024 * 1024
)
