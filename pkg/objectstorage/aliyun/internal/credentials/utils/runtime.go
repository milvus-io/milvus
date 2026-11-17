// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package utils

import (
	"context"
	"net"
	"time"
)

// Runtime is for setting timeout, proxy and host
type Runtime struct {
	ReadTimeout    int
	ConnectTimeout int
	Proxy          string
	Host           string
	STSEndpoint    string
}

// NewRuntime returns a Runtime
func NewRuntime(readTimeout, connectTimeout int, proxy string, host string) *Runtime {
	return &Runtime{
		ReadTimeout:    readTimeout,
		ConnectTimeout: connectTimeout,
		Proxy:          proxy,
		Host:           host,
	}
}

// Timeout is for connect Timeout
func Timeout(connectTimeout time.Duration) func(cxt context.Context, net, addr string) (c net.Conn, err error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{
			Timeout:   connectTimeout,
			DualStack: true,
		}).DialContext(ctx, network, address)
	}
}
