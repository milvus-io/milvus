package gcp

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

type tokenSourceFunc func() (*oauth2.Token, error)

func (f tokenSourceFunc) Token() (*oauth2.Token, error) { return f() }

func TestTokenCacheRejectsBadRefreshAndRecovers(t *testing.T) {
	now := time.Unix(1700000000, 0)
	old := &oauth2.Token{AccessToken: "old-token", Expiry: now.Add(time.Minute)}
	next := old
	var failure error
	source := newCachedTokenSource(tokenSourceFunc(func() (*oauth2.Token, error) { return next, failure }), nil)
	source.now = func() time.Time { return now }
	got, err := source.Token()
	require.NoError(t, err)
	require.Equal(t, old, got)
	for _, bad := range []*oauth2.Token{nil, {AccessToken: "", Expiry: now.Add(time.Hour)}, {AccessToken: " ", Expiry: now.Add(time.Hour)}, {AccessToken: "past", Expiry: now}, {AccessToken: "missing-expiry"}, {AccessToken: "bad-type", TokenType: "MAC", Expiry: now.Add(time.Hour)}} {
		next = bad
		got, err = source.Token()
		require.NoError(t, err)
		require.Equal(t, old, got)
	}
	failure = errors.New("synthetic refresh error")
	got, err = source.Token()
	require.NoError(t, err)
	require.Equal(t, old, got)
	now = old.Expiry
	_, err = source.Token()
	require.Error(t, err)
	failure = nil
	next = &oauth2.Token{AccessToken: "rotated-token", Expiry: now.Add(time.Hour)}
	got, err = source.Token()
	require.NoError(t, err)
	require.Equal(t, next, got)
	got.AccessToken = "caller-mutation"
	got, err = source.Token()
	require.NoError(t, err)
	require.Equal(t, "rotated-token", got.AccessToken)
}

func TestPoisonedInnerCacheIsRecreated(t *testing.T) {
	now := time.Unix(1700000000, 0)
	resets := 0
	source := newCachedTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: " ", Expiry: now.Add(time.Hour)}), func() (oauth2.TokenSource, error) {
		resets++
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "good-token", Expiry: now.Add(time.Hour)}), nil
	})
	source.now = func() time.Time { return now }
	_, err := source.Token()
	require.Error(t, err)
	got, err := source.Token()
	require.NoError(t, err)
	require.Equal(t, "good-token", got.AccessToken)
	require.Equal(t, 1, resets)
}

func TestTokenCacheRechecksCompletionTimeAndSerializes(t *testing.T) {
	now := time.Unix(1700000000, 0)
	calls := 0
	source := newCachedTokenSource(tokenSourceFunc(func() (*oauth2.Token, error) {
		calls++
		return &oauth2.Token{AccessToken: "valid", Expiry: now.Add(time.Hour)}, nil
	}), nil)
	source.now = func() time.Time { return now }
	var wg sync.WaitGroup
	errors := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := source.Token(); errors <- err }()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, 1, calls)
	now = now.Add(59 * time.Minute)
	source.source = tokenSourceFunc(func() (*oauth2.Token, error) {
		candidate := &oauth2.Token{AccessToken: "late", Expiry: now.Add(time.Second)}
		now = now.Add(2 * time.Minute)
		return candidate, nil
	})
	_, err := source.Token()
	require.Error(t, err)
	require.Equal(t, "valid", source.cached.AccessToken)
}
