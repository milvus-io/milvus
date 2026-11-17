package huawei

import (
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/model"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

func TestRejectedRefreshCannotReplaceValidHuaweiCache(t *testing.T) {
	expiration := time.Now().Add(time.Minute)
	old := credentials.Value{AccessKeyID: "old", SecretAccessKey: "secret", SessionToken: "token", Expiration: expiration}
	client := &mockIAMClient{}
	p := &HuaweiCredentialProvider{inited: true, iamClient: client, credentials: old, expiration: expiration}
	for _, bad := range []*model.CreateTemporaryAccessKeyByTokenResponse{
		nil,
		{Credential: makeFullCredential("new", "secret", "token", time.Now().Add(-time.Second).Format(time.RFC3339))},
		{Credential: makeFullCredential("new", "secret", "  ", time.Now().Add(time.Hour).Format(time.RFC3339))},
	} {
		client.response = bad
		p.lastReloadFailed = false
		got, err := p.Retrieve()
		require.NoError(t, err)
		require.Equal(t, old, got)
		require.Equal(t, old, p.credentials)
	}
	p.expiration = time.Now().Add(-time.Second)
	p.lastReloadFailed = false
	_, err := p.Retrieve()
	require.Error(t, err)
	client.response = &model.CreateTemporaryAccessKeyByTokenResponse{Credential: makeFullCredential("rotated", "secret", "token", time.Now().Add(time.Hour).Format(time.RFC3339))}
	p.lastReloadFailed = false
	got, err := p.Retrieve()
	require.NoError(t, err)
	require.Equal(t, "rotated", got.AccessKeyID)
}
