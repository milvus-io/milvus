package gcp

import (
	"context"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type cachedTokenSource struct {
	mu     sync.Mutex
	source oauth2.TokenSource
	reset  func() (oauth2.TokenSource, error)
	cached *oauth2.Token
	now    func() time.Time
}

func validToken(token *oauth2.Token, now time.Time) bool {
	if token == nil || token.AccessToken == "" || token.Expiry.IsZero() || !token.Expiry.After(now) {
		return false
	}
	if token.TokenType != "" && !strings.EqualFold(token.TokenType, "Bearer") {
		return false
	}
	return strings.IndexFunc(token.AccessToken, func(r rune) bool { return r <= 0x20 || r >= 0x7f }) < 0
}

func copyToken(token *oauth2.Token) *oauth2.Token {
	if token == nil {
		return nil
	}
	copy := *token
	return &copy
}

func newCachedTokenSource(source oauth2.TokenSource, reset func() (oauth2.TokenSource, error)) *cachedTokenSource {
	return &cachedTokenSource{source: source, reset: reset, now: time.Now}
}

func (s *cachedTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if validToken(s.cached, now) && s.cached.Expiry.Sub(now) > 225*time.Second {
		return copyToken(s.cached), nil
	}
	candidate, err := s.source.Token()
	now = s.now()
	if err == nil && validToken(candidate, now) {
		s.cached = copyToken(candidate)
		return copyToken(s.cached), nil
	}
	if err == nil {
		err = merr.WrapErrServiceUnavailableMsg("GCP credential refresh returned an empty, invalid or expired token")
		// Clear a poisoned SDK cache without discovering a different identity.
		if s.reset != nil {
			if source, resetErr := s.reset(); resetErr == nil {
				s.source = source
			}
		}
	}
	if validToken(s.cached, s.now()) {
		return copyToken(s.cached), nil
	}
	return nil, merr.Wrap(err, "refresh GCP credentials")
}

func newMetadataTokenSource() oauth2.TokenSource {
	return newCachedTokenSource(google.ComputeTokenSource(""), func() (oauth2.TokenSource, error) {
		return google.ComputeTokenSource(""), nil
	})
}

// ProtectCredentials retains the ADC identity selected by the caller. It never
// repeats default credential discovery when a token refresh fails.
func ProtectCredentials(ctx context.Context, creds *google.Credentials, scopes ...string) {
	data := append([]byte(nil), creds.JSON...)
	selectedScopes := append([]string(nil), scopes...)
	reset := func() (oauth2.TokenSource, error) {
		if len(data) == 0 {
			// FindDefaultCredentials uses metadata when its JSON is empty.
			return google.ComputeTokenSource("", selectedScopes...), nil
		}
		selected, err := google.CredentialsFromJSON(ctx, data, selectedScopes...)
		if err != nil {
			return nil, err
		}
		return selected.TokenSource, nil
	}
	creds.TokenSource = newCachedTokenSource(creds.TokenSource, reset)
}
