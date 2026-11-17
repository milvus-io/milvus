package credentialcache

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

func tuple(id string, expiration time.Time) credentials.Value {
	return credentials.Value{AccessKeyID: id, SecretAccessKey: id + "-secret", SessionToken: id + "-token", Expiration: expiration, SignerType: credentials.SignatureV4}
}

func TestRefreshPreservesValidTupleAndRecovers(t *testing.T) {
	now := time.Unix(1700000000, 0)
	old := tuple("old", now.Add(2*time.Minute))
	next := old
	var fetchErr error
	p := New(func() (credentials.Value, error) { return next, fetchErr }, 3*time.Minute, true, false)
	p.now = func() time.Time { return now }
	got, err := p.Retrieve()
	require.NoError(t, err)
	require.Equal(t, old, got)
	for _, bad := range []credentials.Value{{}, tuple("expired", now.Add(-time.Second)), tuple("no-expiry", time.Time{}), {AccessKeyID: "partial", SecretAccessKey: "secret", Expiration: old.Expiration}} {
		next = bad
		got, err = p.Retrieve()
		require.NoError(t, err)
		require.Equal(t, old, got)
	}
	fetchErr = errors.New("synthetic network failure")
	got, err = p.Retrieve()
	require.NoError(t, err)
	require.Equal(t, old, got)
	now = old.Expiration
	_, err = p.Retrieve()
	require.Error(t, err)
	fetchErr = nil
	next = tuple("new", now.Add(time.Hour))
	got, err = p.Retrieve()
	require.NoError(t, err)
	require.Equal(t, next, got)
}

func TestRechecksExpirationAfterFetch(t *testing.T) {
	now := time.Unix(1700000000, 0)
	old := tuple("old", now.Add(time.Minute))
	calls := 0
	p := New(func() (credentials.Value, error) {
		calls++
		if calls == 1 {
			return old, nil
		}
		candidate := tuple("new", now.Add(time.Second))
		now = now.Add(2 * time.Minute)
		return candidate, nil
	}, 3*time.Minute, true, false)
	p.now = func() time.Time { return now }
	_, err := p.Retrieve()
	require.NoError(t, err)
	_, err = p.Retrieve()
	require.Error(t, err)
	require.Equal(t, old, p.cached)
}

func TestStaticCredentialsAndConcurrentReaders(t *testing.T) {
	static := credentials.Value{AccessKeyID: "static", SecretAccessKey: "secret", SignerType: credentials.SignatureV4}
	calls := 0
	p := New(func() (credentials.Value, error) { calls++; return static, nil }, time.Minute, false, true)
	var wg sync.WaitGroup
	results := make(chan credentials.Value, 16)
	errors := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); v, err := p.Retrieve(); results <- v; errors <- err }()
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	for result := range results {
		require.Equal(t, static, result)
	}
	require.Equal(t, 1, calls)
	unknownExpiry := New(func() (credentials.Value, error) { return static, nil }, time.Minute, false, false)
	_, err := unknownExpiry.Retrieve()
	require.Error(t, err)
}
