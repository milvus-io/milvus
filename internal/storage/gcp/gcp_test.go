package gcp

import (
	"net/http"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
)

func TestNewMinioClient(t *testing.T) {
	t.Run("iam ok", func(t *testing.T) {
		minioCli, err := NewMinioClient("", nil)
		assert.NoError(t, err)
		assert.Equal(t, GcsDefaultAddress, minioCli.EndpointURL().Host)
		assert.Equal(t, "https", minioCli.EndpointURL().Scheme)
	})

	t.Run("ak sk ok", func(t *testing.T) {
		minioCli, err := NewMinioClient(GcsDefaultAddress+":443", &minio.Options{
			Creds:  credentials.NewStaticV2("ak", "sk", ""),
			Secure: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, GcsDefaultAddress, minioCli.EndpointURL().Host)
		assert.Equal(t, "https", minioCli.EndpointURL().Scheme)
	})

	t.Run("create failed", func(t *testing.T) {
		defaultTransBak := minio.DefaultTransport
		defer func() {
			minio.DefaultTransport = defaultTransBak
		}()
		minio.DefaultTransport = func(secure bool) (*http.Transport, error) {
			return nil, errors.New("mock error")
		}
		_, err := NewMinioClient("", nil)
		assert.Error(t, err)
	})
}

type mockTransport struct {
	err error
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, m.err
}

type mockTokenSource struct {
	token string
	err   error
}

func (m *mockTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: m.token}, m.err
}

func TestGCPWrappedHTTPTransport_RoundTrip(t *testing.T) {
	ts, err := NewWrapHTTPTransport(true)
	assert.NoError(t, err)
	ts.backend = &mockTransport{}
	ts.tokenSrc = &mockTokenSource{token: "mocktoken"}

	t.Run("valid token ok", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://example.com", nil)
		assert.NoError(t, err)
		_, err = ts.RoundTrip(req)
		assert.NoError(t, err)
		assert.Equal(t, "Bearer mocktoken", req.Header.Get("Authorization"))
	})

	t.Run("invalid token, refresh failed", func(t *testing.T) {
		ts.currentToken.Store(nil)
		ts.tokenSrc = &mockTokenSource{err: errors.New("mock error")}
		req, err := http.NewRequest("GET", "http://example.com", nil)
		assert.NoError(t, err)
		_, err = ts.RoundTrip(req)
		assert.Error(t, err)
	})

	t.Run("invalid token, refresh ok", func(t *testing.T) {
		ts.currentToken.Store(nil)
		ts.tokenSrc = &mockTokenSource{err: nil}
		req, err := http.NewRequest("GET", "http://example.com", nil)
		assert.NoError(t, err)
		_, err = ts.RoundTrip(req)
		assert.NoError(t, err)
	})

	ts.currentToken.Store(&oauth2.Token{})
	t.Run("valid token, call failed", func(t *testing.T) {
		ts.backend = &mockTransport{err: errors.New("mock error")}
		req, err := http.NewRequest("GET", "http://example.com", nil)
		assert.NoError(t, err)
		_, err = ts.RoundTrip(req)
		assert.Error(t, err)
	})
}
