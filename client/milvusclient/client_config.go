package milvusclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

const (
	disableDatabase uint64 = 1 << iota
	disableJSON
	disableDynamicSchema
	disableParitionKey
)

var regexValidScheme = regexp.MustCompile(`^https?:\/\/`)

// DefaultGrpcOpts is GRPC options for milvus client.
var DefaultGrpcOpts = []grpc.DialOption{
	grpc.WithBlock(),
	grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                5 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}),
	grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  100 * time.Millisecond,
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   3 * time.Second,
		},
		MinConnectTimeout: 3 * time.Second,
	}),
	grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(math.MaxInt32), // math.MaxInt32 = 2147483647, 2GB - 1
	),
}

// ClientConfig for milvus client. Configure it before New and do not copy or
// mutate it afterward. Use GetServerVersion for concurrent version reads.
type ClientConfig struct {
	Address  string // Remote address, "localhost:19530".
	Username string // Username for auth.
	Password string // Password for auth.
	DBName   string // DBName for this client.

	EnableTLSAuth bool   // Enable TLS Auth for transport security.
	APIKey        string // API key

	tlsConfig *tls.Config // Custom TLS config, set via WithTLSConfig method.

	DialOptions []grpc.DialOption // Dial options for GRPC.

	RetryRateLimit *RetryRateLimitOption // option for retry on rate limit inteceptor

	DisableConn bool

	// ConnectionPoolSize is the number of independent gRPC connections.
	// Values less than 1 use a single connection.
	ConnectionPoolSize int
	// ConnectionMaxAge controls proactive replacement of pooled unary connections.
	// Values <= 0 disable replacement. When enabled, one additional non-rotating
	// connection carries streaming RPCs, GetService calls, and telemetry.
	ConnectionMaxAge time.Duration
	// ConnectionDrainTimeout limits how long a replaced connection waits for
	// in-flight unary RPCs. Zero waits indefinitely. A slot's next rotation timer
	// starts after draining; a positive timeout can interrupt unfinished RPCs.
	ConnectionDrainTimeout time.Duration
	// ConnectionRotationTimeout limits dialing and handshaking a replacement.
	// Values <= 0 use 10 seconds.
	ConnectionRotationTimeout time.Duration

	// TelemetryConfig for client telemetry settings
	TelemetryConfig *TelemetryConfig

	// ServerVersion is updated by Connect handshakes. Use GetServerVersion
	// for concurrent reads; do not write this field while the client is in use.
	ServerVersion string
	stateMut      sync.RWMutex // protects ServerVersion and flags
	parsedAddress *url.URL
	flags         uint64 // internal flags
}

type RetryRateLimitOption struct {
	MaxRetry   uint
	MaxBackoff time.Duration
}

func (cfg *ClientConfig) parse() error {
	// Prepend default fake tcp:// scheme for remote address.
	address := cfg.Address
	if !regexValidScheme.MatchString(address) {
		address = fmt.Sprintf("tcp://%s", address)
	}

	remoteURL, err := url.Parse(address)
	if err != nil {
		return errors.Wrap(err, "milvus address parse fail")
	}
	// Remote Host should never be empty.
	if remoteURL.Host == "" {
		return errors.New("empty remote host of milvus address")
	}
	// Use DBName in remote url path.
	if cfg.DBName == "" {
		cfg.DBName = strings.TrimLeft(remoteURL.Path, "/")
	}
	// Always enable tls auth for https remote url.
	if remoteURL.Scheme == "https" {
		cfg.EnableTLSAuth = true
	}
	if remoteURL.Port() == "" && cfg.EnableTLSAuth {
		remoteURL.Host += ":443"
	}
	if cfg.ConnectionDrainTimeout < 0 {
		return merr.WrapErrParameterInvalidMsg("connection drain timeout must not be negative")
	}
	cfg.parsedAddress = remoteURL
	return nil
}

// Get parsed remote milvus address, should be called after parse was called.
func (c *ClientConfig) getParsedAddress() string {
	return c.parsedAddress.Host
}

func (cfg *ClientConfig) getConnectionPoolSize() int {
	if cfg.ConnectionPoolSize < 1 {
		return 1
	}
	return cfg.ConnectionPoolSize
}

func (cfg *ClientConfig) getConnectionRotationTimeout() time.Duration {
	if cfg.ConnectionRotationTimeout <= 0 {
		return 10 * time.Second
	}
	return cfg.ConnectionRotationTimeout
}

// useDatabase change the inner db name.
func (c *ClientConfig) useDatabase(dbName string) {
	c.DBName = dbName
}

func (c *ClientConfig) setServerInfo(serverInfo string) {
	c.stateMut.Lock()
	defer c.stateMut.Unlock()
	c.ServerVersion = serverInfo
}

// GetServerVersion returns the version reported by the latest successful
// Connect handshake. It is safe to call while connections are rotating.
func (c *ClientConfig) GetServerVersion() string {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()
	return c.ServerVersion
}

func (c *ClientConfig) getRetryOnRateLimitInterceptor() grpc.UnaryClientInterceptor {
	if c.RetryRateLimit == nil {
		c.RetryRateLimit = c.defaultRetryRateLimitOption()
	}

	return RetryOnRateLimitInterceptor(c.RetryRateLimit.MaxRetry, c.RetryRateLimit.MaxBackoff, func(ctx context.Context, attempt uint) time.Duration {
		return 10 * time.Millisecond * time.Duration(math.Pow(3, float64(attempt)))
	})
}

func (c *ClientConfig) defaultRetryRateLimitOption() *RetryRateLimitOption {
	return &RetryRateLimitOption{
		MaxRetry:   75,
		MaxBackoff: 3 * time.Second,
	}
}

// addFlags set internal flags
func (c *ClientConfig) addFlags(flags uint64) {
	c.stateMut.Lock()
	defer c.stateMut.Unlock()
	c.flags |= flags
}

// hasFlags check flags is set
func (c *ClientConfig) hasFlags(flags uint64) bool {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()
	return (c.flags & flags) > 0
}

func (c *ClientConfig) resetFlags(flags uint64) {
	c.stateMut.Lock()
	defer c.stateMut.Unlock()
	c.flags &= ^flags
}

// WithTLSConfig sets the custom TLS configuration and enables TLS auth.
// This method should be used to configure custom TLS settings (e.g., mTLS, custom CA).
func (c *ClientConfig) WithTLSConfig(tlsConfig *tls.Config) *ClientConfig {
	c.tlsConfig = tlsConfig
	c.EnableTLSAuth = true
	return c
}

// WithGrpcAuthority sets the gRPC :authority header, used for proxy-based routing.
// DefaultGrpcOpts are always applied by dialOptions(), so only the authority option is needed here.
func (c *ClientConfig) WithGrpcAuthority(authority string) *ClientConfig {
	c.DialOptions = []grpc.DialOption{grpc.WithAuthority(authority)}
	return c
}
