package credentials

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/utils"
)

type wireReply struct {
	status int
	body   string
}

func tokenBody(kind, generation, expiry string, partial bool) string {
	fields := map[string]string{"AccessKeyId": generation + "_AK", "AccessKeySecret": generation + "_SK", "SecurityToken": generation + "_TOKEN", "Expiration": expiry}
	if partial {
		fields["AccessKeySecret"] = ""
	}
	var response any = map[string]any{"Credentials": fields}
	if kind == "ecs" {
		fields["Code"] = "Success"
		response = fields
	}
	if kind == "uri" {
		response = fields
	}
	if kind == "rsa" {
		fields = map[string]string{"SessionAccessKeyId": generation + "_AK", "SessionAccessKeySecret": generation + "_SK", "Expiration": expiry}
		if partial {
			fields["SessionAccessKeySecret"] = ""
		}
		response = map[string]any{"SessionAccessKey": fields}
	}
	b, _ := json.Marshal(response)
	return string(b)
}

func TestRefreshersPreserveValidatedSnapshots(t *testing.T) {
	// Exercise actual HTTP requests and JSON parsers; only route the credential
	// endpoint to a TLS loopback service. Credential methods are not mocked.
	var reply atomic.Pointer[wireReply]
	var requests atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		v := reply.Load()
		w.WriteHeader(v.status)
		_, _ = io.WriteString(w, v.body)
	}))
	defer server.Close()
	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	previousHook := hookDo
	hookDo = func(_ func(*http.Request) (*http.Response, error)) func(*http.Request) (*http.Response, error) {
		return func(req *http.Request) (*http.Response, error) {
			copy := req.Clone(req.Context())
			copy.URL.Scheme = endpoint.Scheme
			copy.URL.Host = endpoint.Host
			copy.Host = endpoint.Host
			return server.Client().Do(copy)
		}
	}
	defer func() { hookDo = previousHook }()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	keyText := base64.StdEncoding.EncodeToString(pkcs8)
	file := filepath.Join(t.TempDir(), "oidc-token")
	require.NoError(t, os.WriteFile(file, []byte("synthetic-token"), 0o600))
	for _, kind := range []string{"oidc", "role", "ecs", "uri", "rsa"} {
		t.Run(kind, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			var credential Credential
			var updater *credentialUpdater
			switch kind {
			case "oidc":
				c := newOIDCRoleArnCredential("", "", "synthetic-role", "synthetic-provider", file, "session", "", 900, &utils.Runtime{})
				credential = c
				updater = c.credentialUpdater
			case "role":
				c := newRAMRoleArnCredential("source-ak", "source-sk", "synthetic-role", "session", "", 900, &utils.Runtime{})
				credential = c
				updater = c.credentialUpdater
			case "ecs":
				c := newEcsRAMRoleCredential("synthetic-role", 0, &utils.Runtime{})
				credential = c
				updater = c.credentialUpdater
			case "uri":
				c := newURLCredential("http://synthetic.invalid/credentials")
				credential = c
				updater = c.credentialUpdater
			case "rsa":
				c := newRsaKeyPairCredential(keyText, "synthetic-public-key", 900, &utils.Runtime{})
				credential = c
				updater = c.credentialUpdater
			}
			updater.now = func() time.Time { return now }
			reply.Store(&wireReply{503, "temporary unavailable"})
			_, err := RetrieveSnapshot(credential)
			require.Error(t, err)
			oldExpiry := now.Add(10 * time.Minute)
			reply.Store(&wireReply{200, tokenBody(kind, "OLD", oldExpiry.Format(time.RFC3339), false)})
			old, err := RetrieveSnapshot(credential)
			require.NoError(t, err)
			require.Equal(t, "OLD_AK", old.AccessKeyId)
			require.Equal(t, oldExpiry, old.Expiration)
			require.False(t, old.Static)
			before := requests.Load()
			same, err := RetrieveSnapshot(credential)
			require.NoError(t, err)
			require.Equal(t, old, same)
			require.Equal(t, before, requests.Load())
			now = oldExpiry.Add(-10 * time.Second)
			if kind == "oidc" {
				alternate := filepath.Join(t.TempDir(), "alternate-token")
				require.NoError(t, os.WriteFile(alternate, []byte("different-identity"), 0o600))
				t.Setenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE", alternate)
				require.NoError(t, os.Remove(file))
				before := requests.Load()
				got, err := RetrieveSnapshot(credential)
				require.NoError(t, err)
				require.Equal(t, old, got)
				require.Equal(t, before, requests.Load(), "unreadable selected token must not use environment fallback")
				require.NoError(t, os.WriteFile(file, []byte("synthetic-token"), 0o600))
			}
			for _, bad := range []wireReply{{503, "temporary unavailable"}, {200, ""}, {200, "{"}, {200, "null"}, {200, tokenBody(kind, "BAD", oldExpiry.Format(time.RFC3339), true)}, {200, tokenBody(kind, "BAD", "not-a-time", false)}, {200, tokenBody(kind, "BAD", "", false)}, {200, tokenBody(kind, "BAD", now.Add(-time.Second).Format(time.RFC3339), false)}} {
				reply.Store(&bad)
				got, err := RetrieveSnapshot(credential)
				require.NoError(t, err)
				require.Equal(t, old, got)
				require.Equal(t, oldExpiry, updater.expiresAt)
				ak, err := credential.GetAccessKeyId()
				require.NoError(t, err)
				require.Equal(t, "OLD_AK", *ak)
			}
			newExpiry := now.Add(10 * time.Minute)
			reply.Store(&wireReply{200, tokenBody(kind, "NEW", newExpiry.Format(time.RFC3339), false)})
			before = requests.Load()
			var wg sync.WaitGroup
			results := make(chan Snapshot, 16)
			errs := make(chan error, 16)
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); got, err := RetrieveSnapshot(credential); results <- got; errs <- err }()
			}
			wg.Wait()
			close(results)
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			for got := range results {
				require.Equal(t, "NEW_AK", got.AccessKeyId)
				require.Equal(t, "NEW_SK", got.AccessKeySecret)
				require.Equal(t, newExpiry, got.Expiration)
			}
			require.Equal(t, before+1, requests.Load(), "concurrent refresh must fetch once")
			now = newExpiry.Add(time.Second)
			reply.Store(&wireReply{503, "temporary unavailable"})
			got, err := RetrieveSnapshot(credential)
			require.Error(t, err)
			require.Empty(t, got.AccessKeyId)
			require.Equal(t, newExpiry, updater.expiresAt, "failed fetch must not mutate cache")
			reply.Store(&wireReply{200, tokenBody(kind, "RECOVERED", now.Add(time.Hour).Format(time.RFC3339), false)})
			got, err = RetrieveSnapshot(credential)
			require.NoError(t, err)
			require.Equal(t, "RECOVERED_AK", got.AccessKeyId)
		})
	}
}

func TestSelectionAndExplicitStaticLifetime(t *testing.T) {
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "static-ak")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "static-sk")
	t.Setenv("ALIBABA_CLOUD_ROLE_ARN", "synthetic-role")
	t.Setenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN", "synthetic-provider")
	t.Setenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE", "synthetic-token-path")
	credential, err := NewCredential(nil)
	require.NoError(t, err)
	require.Equal(t, "access_key", *credential.GetType())
	snapshot, err := RetrieveSnapshot(credential)
	require.NoError(t, err)
	require.True(t, snapshot.Static)
	require.True(t, snapshot.Expiration.IsZero())
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
	credential, err = NewCredential(nil)
	require.NoError(t, err)
	require.Equal(t, "oidc_role_arn", *credential.GetType())
	credential, err = NewCredential(&Config{Type: tea.String("sts"), AccessKeyId: tea.String("ak"), AccessKeySecret: tea.String("sk"), SecurityToken: tea.String("token")})
	require.NoError(t, err)
	snapshot, err = RetrieveSnapshot(credential)
	require.NoError(t, err)
	require.True(t, snapshot.Static)
	require.Equal(t, "token", snapshot.SecurityToken)
}
