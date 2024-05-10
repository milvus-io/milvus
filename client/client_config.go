package client

import (
	"crypto/tls"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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

	// RetryRateLimit *RetryRateLimitOption // option for retry on rate limit inteceptor

	DisableConn bool

	identifier    string // Identifier for this connection
	ServerVersion string // ServerVersion
	parsedAddress *url.URL
	flags         uint64 // internal flags
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

// useDatabase change the inner db name.
func (c *ClientConfig) setIdentifier(identifier string) {
	c.identifier = identifier
}

func (c *ClientConfig) setServerInfo(serverInfo string) {
	c.ServerVersion = serverInfo
}

// Get parsed grpc dial options, should be called after parse was called.
func (c *ClientConfig) getDialOption() []grpc.DialOption {
	options := c.DialOptions
	if c.DialOptions == nil {
		// Add default connection options.
		options = make([]grpc.DialOption, len(DefaultGrpcOpts))
		copy(options, DefaultGrpcOpts)
	}

	// Construct dial option.
	if c.EnableTLSAuth {
		options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	options = append(options,
		grpc.WithChainUnaryInterceptor(grpc_retry.UnaryClientInterceptor(
			grpc_retry.WithMax(6),
			grpc_retry.WithBackoff(func(attempt uint) time.Duration {
				return 60 * time.Millisecond * time.Duration(math.Pow(3, float64(attempt)))
			}),
			grpc_retry.WithCodes(codes.Unavailable, codes.ResourceExhausted)),
		// c.getRetryOnRateLimitInterceptor(),
		))

	// options = append(options, grpc.WithChainUnaryInterceptor(
	// 	createMetaDataUnaryInterceptor(c),
	// ))
	return options
}

// func (c *ClientConfig) getRetryOnRateLimitInterceptor() grpc.UnaryClientInterceptor {
// 	if c.RetryRateLimit == nil {
// 		c.RetryRateLimit = c.defaultRetryRateLimitOption()
// 	}

// 	return RetryOnRateLimitInterceptor(c.RetryRateLimit.MaxRetry, c.RetryRateLimit.MaxBackoff, func(ctx context.Context, attempt uint) time.Duration {
// 		return 10 * time.Millisecond * time.Duration(math.Pow(3, float64(attempt)))
// 	})
// }

// func (c *ClientConfig) defaultRetryRateLimitOption() *RetryRateLimitOption {
// 	return &RetryRateLimitOption{
// 		MaxRetry:   75,
// 		MaxBackoff: 3 * time.Second,
// 	}
// }

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
