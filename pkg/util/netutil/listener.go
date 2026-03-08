package netutil

import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

// NewListener creates a new listener that listens on the specified network and IP address.
func NewListener(opts ...Opt) (*NetListener, error) {
	config := getNetListenerConfig(opts...)
	if config.tlsConfig != nil {
		return newTLSListener(config.tlsConfig, opts...)
	}

	// Use the highPriorityToUsePort if it is set.
	if config.highPriorityToUsePort != 0 {
		if lis, err := net.Listen(config.net, fmt.Sprintf(":%d", config.highPriorityToUsePort)); err == nil {
			return &NetListener{
				Listener: lis,
				port:     config.highPriorityToUsePort,
				address:  fmt.Sprintf("%s:%d", config.ip, config.highPriorityToUsePort),
			}, nil
		}
	}
	// Otherwise use the port number specified by the user.
	lis, err := net.Listen(config.net, fmt.Sprintf(":%d", config.port))
	if err != nil {
		return nil, err
	}
	return &NetListener{
		Listener: lis,
		port:     lis.Addr().(*net.TCPAddr).Port,
		address:  fmt.Sprintf("%s:%d", config.ip, lis.Addr().(*net.TCPAddr).Port),
	}, nil
}

// newTLSListener creates a new listener that listens on the specified network and IP address with TLS.
func newTLSListener(c *tls.Config, opts ...Opt) (*NetListener, error) {
	config := getNetListenerConfig(opts...)
	// Use the highPriorityToUsePort if it is set.
	if config.highPriorityToUsePort != 0 {
		if lis, err := tls.Listen(config.net, fmt.Sprintf(":%d", config.highPriorityToUsePort), c); err == nil {
			return &NetListener{
				Listener: lis,
				port:     config.highPriorityToUsePort,
				address:  fmt.Sprintf("%s:%d", config.ip, config.highPriorityToUsePort),
			}, nil
		}
	}
	// Otherwise use the port number specified by the user.
	lis, err := tls.Listen(config.net, fmt.Sprintf(":%d", config.port), c)
	if err != nil {
		return nil, err
	}
	return &NetListener{
		Listener: lis,
		port:     lis.Addr().(*net.TCPAddr).Port,
		address:  fmt.Sprintf("%s:%d", config.ip, lis.Addr().(*net.TCPAddr).Port),
	}, nil
}

// NetListener is a wrapper around a net.Listener that provides additional functionality.
type NetListener struct {
	net.Listener
	port    int
	address string
}

// Port returns the port that the listener is listening on.
func (nl *NetListener) Port() int {
	return nl.port
}

// Address returns the address that the listener is listening on.
func (nl *NetListener) Address() string {
	return nl.address
}

// netListenerConfig contains the configuration for a NetListener.
type netListenerConfig struct {
	net                   string
	ip                    string
	highPriorityToUsePort int
	port                  int
	tlsConfig             *tls.Config
}

// getNetListenerConfig returns a netListenerConfig with the default values.
func getNetListenerConfig(opts ...Opt) *netListenerConfig {
	defaultConfig := &netListenerConfig{
		net:                   "tcp",
		ip:                    funcutil.GetLocalIP(),
		highPriorityToUsePort: 0,
		port:                  0,
	}
	for _, opt := range opts {
		opt(defaultConfig)
	}
	return defaultConfig
}

// Opt is a function that configures a netListenerConfig.
type Opt func(*netListenerConfig)

// OptNet sets the network type for the listener.
func OptNet(net string) Opt {
	return func(nlc *netListenerConfig) {
		nlc.net = net
	}
}

// OptIP sets the IP address for the listener.
func OptIP(ip string) Opt {
	return func(nlc *netListenerConfig) {
		nlc.ip = ip
	}
}

// OptHighPriorityToUsePort sets the port number to use for the listener.
func OptHighPriorityToUsePort(port int) Opt {
	return func(nlc *netListenerConfig) {
		if nlc.port != 0 {
			panic("OptHighPriorityToUsePort and OptPort are mutually exclusive")
		}
		nlc.highPriorityToUsePort = port
	}
}

// OptPort sets the port number to use for the listener.
func OptPort(port int) Opt {
	return func(nlc *netListenerConfig) {
		if nlc.highPriorityToUsePort != 0 {
			panic("OptHighPriorityToUsePort and OptPort are mutually exclusive")
		}
		nlc.port = port
	}
}

// OptTLS sets the TLS configuration for the listener.
func OptTLS(c *tls.Config) Opt {
	return func(nlc *netListenerConfig) {
		nlc.tlsConfig = c
	}
}
