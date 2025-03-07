package milvusclient

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
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

// ClientConfig for milvus client.
type ClientConfig struct {
	Address  string // Remote address, "localhost:19530".
	Username string // Username for auth.
	Password string // Password for auth.
	DBName   string // DBName for this client.

	EnableTLSAuth bool   // Enable TLS Auth for transport security.
	APIKey        string // API key

	DialOptions []grpc.DialOption // Dial options for GRPC.

	RetryRateLimit *RetryRateLimitOption // option for retry on rate limit inteceptor

	DisableConn bool

	ServerVersion string // ServerVersion
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
	cfg.parsedAddress = remoteURL
	return nil
}

// Get parsed remote milvus address, should be called after parse was called.
func (c *ClientConfig) getParsedAddress() string {
	return c.parsedAddress.Host
}

// useDatabase change the inner db name.
func (c *ClientConfig) useDatabase(dbName string) {
	c.DBName = dbName
}

func (c *ClientConfig) setServerInfo(serverInfo string) {
	c.ServerVersion = serverInfo
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
	c.flags |= flags
}

// hasFlags check flags is set
func (c *ClientConfig) hasFlags(flags uint64) bool {
	return (c.flags & flags) > 0
}

func (c *ClientConfig) resetFlags(flags uint64) {
	c.flags &= ^flags
}
