package gcp

import (
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/atomic"
	"golang.org/x/oauth2"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WrapHTTPTransport wraps http.Transport, add an auth header to support GCP native auth
type WrapHTTPTransport struct {
	tokenSrc     oauth2.TokenSource
	backend      transport
	currentToken atomic.Pointer[oauth2.Token]
}

// transport abstracts http.Transport to simplify test
type transport interface {
	RoundTrip(req *http.Request) (*http.Response, error)
}

// NewWrapHTTPTransport constructs a new WrapHTTPTransport
func NewWrapHTTPTransport(secure bool) (*WrapHTTPTransport, error) {
	tokenSrc := newMetadataTokenSource()
	// in fact never return err
	backend, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default transport")
	}
	return &WrapHTTPTransport{
		tokenSrc: tokenSrc,
		backend:  backend,
	}, nil
}

const (
	xAmzPrefix  = "X-Amz-"
	xGoogPrefix = "X-Goog-"
)

// RoundTrip wraps original http.RoundTripper by Adding a Bearer token acquired from tokenSrc
func (t *WrapHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// GCS's XML API only honors x-amz-* headers when the request is signed with
	// HMAC keys. With OAuth 2.0 (Bearer token) authentication they must be sent
	// as their x-goog-* counterparts — e.g. minio's CopyObject sends
	// x-amz-copy-source, which GCS rejects with "Invalid argument" under Bearer
	// auth unless it is translated to x-goog-copy-source.
	for k, v := range req.Header {
		if strings.HasPrefix(k, xAmzPrefix) {
			req.Header[xGoogPrefix+strings.TrimPrefix(k, xAmzPrefix)] = v
			delete(req.Header, k)
		}
	}

	// here Valid() means the token won't be expired in 10 sec
	// so the http client timeout shouldn't be longer, or we need to change the default `expiryDelta` time
	currentToken := t.currentToken.Load()
	if currentToken.Valid() && validToken(currentToken, time.Now()) {
		req.Header.Set("Authorization", "Bearer "+currentToken.AccessToken)
	} else {
		newToken, err := t.tokenSrc.Token()
		if err != nil || !validToken(newToken, time.Now()) {
			if !validToken(currentToken, time.Now()) {
				if err != nil {
					return nil, merr.Wrap(err, "acquire GCP token")
				}
				return nil, merr.WrapErrServiceUnavailableMsg("GCP token response is incomplete or expired")
			}
			newToken = currentToken
		} else {
			t.currentToken.Store(copyToken(newToken))
		}
		req.Header.Set("Authorization", "Bearer "+newToken.AccessToken)
	}

	return t.backend.RoundTrip(req)
}

const GcsDefaultAddress = "storage.googleapis.com"

// NewMinioClient returns a minio.Client which is compatible for GCS
func NewMinioClient(address string, opts *minio.Options) (*minio.Client, error) {
	if opts == nil {
		opts = &minio.Options{}
	}
	if address == "" {
		address = GcsDefaultAddress
		opts.Secure = true
	}

	// adhoc to remove port of gcs address to let minio-go know it's gcs
	if strings.Contains(address, GcsDefaultAddress) {
		address = GcsDefaultAddress
	}

	if opts.Creds != nil {
		// if creds is set, use it directly
		return minio.New(address, opts)
	}
	// opts.Creds == nil, assume using IAM
	// If a transport was already set (e.g., with custom TLS config), use it as backend;
	// otherwise create a new default transport.
	var backend transport
	if opts.Transport != nil {
		backend = opts.Transport
	} else {
		defaultTr, err := minio.DefaultTransport(opts.Secure)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create default transport")
		}
		backend = defaultTr
	}
	tokenSrc := newMetadataTokenSource()
	opts.Transport = &WrapHTTPTransport{tokenSrc: tokenSrc, backend: backend}
	opts.Creds = credentials.NewStaticV2("", "", "")
	return minio.New(address, opts)
}
