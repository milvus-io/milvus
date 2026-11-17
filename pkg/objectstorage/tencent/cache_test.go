package tencent

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	tchttp "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/http"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/credentialcache"
)

func TestOIDCRefreshRetainsRealExpiryAndRecovers(t *testing.T) {
	file := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(file, []byte("synthetic-oidc-token"), 0o600))
	expiration := time.Now().Add(time.Minute).Truncate(time.Second)
	makeResponse := func(ak, token string, expires time.Time) []byte {
		data, err := json.Marshal(map[string]interface{}{"Response": map[string]interface{}{
			"Credentials": map[string]string{"TmpSecretId": ak, "TmpSecretKey": "synthetic-secret", "Token": token}, "ExpiredTime": expires.Unix(),
		}})
		require.NoError(t, err)
		return data
	}
	body := makeResponse("old", "old-token", expiration)
	var sendErr error
	source := &oidcSource{roleARN: "role", providerID: "provider", tokenFile: file, send: func(*tchttp.CommonRequest) ([]byte, error) { return body, sendErr }}
	provider := credentialcache.New(source.retrieve, 3*time.Minute, true, false)
	outer := credentials.New(provider)
	old, err := outer.Get()
	require.NoError(t, err)
	require.Equal(t, expiration, old.Expiration)
	for _, bad := range [][]byte{nil, []byte("{"), []byte(`{"Response":{}}`), makeResponse("partial", "", expiration), makeResponse("past", "token", time.Now().Add(-time.Minute))} {
		body = bad
		got, err := outer.Get()
		require.NoError(t, err)
		require.Equal(t, old, got)
	}
	sendErr = errors.New("synthetic STS failure")
	got, err := outer.Get()
	require.NoError(t, err)
	require.Equal(t, old, got)
	sendErr = nil
	body = makeResponse("new", "new-token", time.Now().Add(time.Hour))
	got, err = outer.Get()
	require.NoError(t, err)
	require.Equal(t, "new", got.AccessKeyID)
	require.Equal(t, "new-token", got.SessionToken)
}

func TestOIDCFirstPartialResponseFails(t *testing.T) {
	file := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(file, []byte("token"), 0o600))
	source := &oidcSource{tokenFile: file, send: func(*tchttp.CommonRequest) ([]byte, error) { return []byte(`{"Response":{}}`), nil }}
	provider := credentialcache.New(source.retrieve, time.Minute, true, false)
	_, err := provider.Retrieve()
	require.Error(t, err)
}
