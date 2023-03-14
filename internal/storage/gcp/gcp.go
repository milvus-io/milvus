package gcp

import (
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/atomic"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
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
	tokenSrc := google.ComputeTokenSource("")
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

// RoundTrip wraps original http.RoundTripper by Adding a Bearer token acquired from tokenSrc
func (t *WrapHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// here Valid() means the token won't be expired in 10 sec
	// so the http client timeout shouldn't be longer, or we need to change the default `expiryDelta` time
	currentToken := t.currentToken.Load()
	if currentToken.Valid() {
		req.Header.Set("Authorization", "Bearer "+currentToken.AccessToken)
	} else {
		newToken, err := t.tokenSrc.Token()
		if err != nil {
			return nil, errors.Wrap(err, "failed to acquire token")
		}
		t.currentToken.Store(newToken)
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
	transport, err := NewWrapHTTPTransport(opts.Secure)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default transport")
	}
	opts.Transport = transport
	opts.Creds = credentials.NewStaticV2("", "", "")
	return minio.New(address, opts)
}
