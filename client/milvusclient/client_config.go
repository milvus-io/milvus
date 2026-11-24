package milvusclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
)

const (
	disableDatabase uint64 = 1 << iota
	disableJSON
	disableDynamicSchema
	disableParitionKey
)

var regexValidScheme = regexp.MustCompile(`^https?:\/\/`)

// Retry policy numbers shared with the pymilvus, C++ and Java SDKs. All three
// baselines apply the same default profile: at most 75 attempts, an initial
// 10ms backoff multiplied by 3 per attempt, capped at 3s, and no overall
// timeout by default. Against a persistently failing server a call made with
// context.Background() can therefore hold for roughly 3.5 minutes (sum of the
// 75 backoffs) before surfacing the error; pass a context with a deadline
// (e.g. context.WithTimeout) to bound the whole retry window.
const (
	defaultRetryTimes          = 75
	defaultRetryInitialBackoff = 10 * time.Millisecond
	defaultRetryMaxBackoff     = 3 * time.Second
	defaultRetryMultiplier     = 3
)

// retryableCodes lists the gRPC status codes the transport-level retry
// interceptor retries on. It mirrors the shared retry policy of the pymilvus,
// C++ and Java SDKs, which retry every code except DEADLINE_EXCEEDED,
// PERMISSION_DENIED, UNAUTHENTICATED, INVALID_ARGUMENT, ALREADY_EXISTS,
// RESOURCE_EXHAUSTED and UNIMPLEMENTED. Two adjustments are forced by the retry
// middleware: Canceled and DeadlineExceeded are context errors that grpc_retry
// never retries, so they are not listed even though pymilvus's ignore set does
// not include Canceled; and the Milvus application-level rate limit is retried
// by the dedicated rate-limit interceptor rather than through the gRPC
// RESOURCE_EXHAUSTED status code.
//
// The widened set targets genuine transport failures. Real Milvus delivers
// application errors embedded in resp.Status (gRPC OK), so these codes are
// rarely surfaced for business RPCs; still, retrying an ambiguous transport
// failure on a non-idempotent write (Insert/Upsert/Delete) is at-least-once.
// Callers that need to bound the retry window should pass a context deadline,
// and callers that need a specific call not to be retried can use
// grpc_retry.Disable() per call. Flush stays on the standard retry policy,
// matching pymilvus, which wraps its flush RPC with retry_on_rpc_failure.
var retryableCodes = []codes.Code{
	codes.Unknown,
	codes.NotFound,
	codes.FailedPrecondition,
	codes.Aborted,
	codes.OutOfRange,
	codes.Internal,
	codes.Unavailable,
	codes.DataLoss,
}

// retryBackoff returns the exponential backoff for the given retry attempt,
// aligned with the other SDKs: initialBackoff * multiplier^(attempt-1), capped
// at maxBackoff (the first retry waits 10ms, the second 30ms, ...). The capped
// loop avoids the duration overflow a raw power computation would hit for large
// attempts.
func retryBackoff(attempt uint, maxBackoff time.Duration) time.Duration {
	wait := defaultRetryInitialBackoff
	for i := uint(1); i < attempt && wait < maxBackoff; i++ {
		wait *= defaultRetryMultiplier
	}
	if wait > maxBackoff {
		return maxBackoff
	}
	return wait
}

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

	tlsConfig *tls.Config // Custom TLS config, set via WithTLSConfig method.

	DialOptions []grpc.DialOption // Dial options for GRPC.

	RetryRateLimit *RetryRateLimitOption // option for retry on rate limit inteceptor

	DisableConn bool

	// TelemetryConfig for client telemetry settings
	TelemetryConfig *TelemetryConfig

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
		// honor a caller-configured MaxBackoff above or below the parity 3s cap
		return retryBackoff(attempt, c.RetryRateLimit.MaxBackoff)
	})
}

func (c *ClientConfig) defaultRetryRateLimitOption() *RetryRateLimitOption {
	return &RetryRateLimitOption{
		MaxRetry:   defaultRetryTimes,
		MaxBackoff: defaultRetryMaxBackoff,
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
