package objectstorage

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIAMRefreshPreservesCredentialsAndSource(t *testing.T) {
	for _, key := range []string{"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_REGION", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_CONTAINER_AUTHORIZATION_TOKEN"} {
		t.Setenv(key, "")
	}
	var mode atomic.Int32
	var calls atomic.Int32
	expiration := time.Now().Add(time.Minute).UTC().Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		switch mode.Load() {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		case 2:
			_, _ = w.Write([]byte(`{"AccessKeyId":"partial"}`))
			return
		}
		ak := "old"
		if mode.Load() == 3 {
			ak = "rotated"
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"AccessKeyId": ak, "SecretAccessKey": "synthetic-secret", "Token": "synthetic-token", "Expiration": expiration})
	}))
	defer server.Close()
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", server.URL)
	provider := newIAMCredentials()
	old, err := provider.Get()
	require.NoError(t, err)
	require.Equal(t, "old", old.AccessKeyID)
	for _, invalid := range []int32{1, 2} {
		mode.Store(invalid)
		got, err := provider.Get()
		require.NoError(t, err)
		require.Equal(t, old, got)
	}
	mode.Store(3)
	got, err := provider.Get()
	require.NoError(t, err)
	require.Equal(t, "rotated", got.AccessKeyID)
	before := calls.Load()
	t.Setenv("AWS_ROLE_ARN", "different-role")
	got, err = provider.Get()
	require.NoError(t, err)
	require.Equal(t, "rotated", got.AccessKeyID)
	require.Equal(t, before, calls.Load())
}
