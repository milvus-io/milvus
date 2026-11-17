package aliyun

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/mocks"
)

func TestNewMinioClient(t *testing.T) {
	t.Run("ak sk ok", func(t *testing.T) {
		minioCli, err := NewMinioClient(OSSDefaultAddress+":443", &minio.Options{
			Creds:  credentials.NewStaticV2("ak", "sk", ""),
			Secure: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, OSSDefaultAddress+":443", minioCli.EndpointURL().Host)
		assert.Equal(t, "https", minioCli.EndpointURL().Scheme)
	})

	t.Run("iam failed", func(t *testing.T) {
		t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
		t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
		t.Setenv("ALIBABA_CLOUD_ROLE_ARN", "")
		t.Setenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN", "")
		t.Setenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE", "")
		t.Setenv("ALIBABA_CLOUD_CREDENTIALS_FILE", "/tmp/nonexistent-aliyun-test-profile")
		t.Setenv("ALIBABA_CLOUD_ECS_METADATA", "")
		_, err := NewMinioClient("", nil)
		assert.Error(t, err)
	})

	t.Run("iam ok", func(t *testing.T) {
		t.Setenv("ALIBABA_CLOUD_ROLE_ARN", "roleArn")
		t.Setenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN", "oidcProviderArn")
		t.Setenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE", "oidcTokenFilePath")
		minioCli, err := NewMinioClient("", nil)
		assert.NoError(t, err)
		assert.Equal(t, OSSDefaultAddress, minioCli.EndpointURL().Host)
		assert.Equal(t, "https", minioCli.EndpointURL().Scheme)
	})
}

func TestCredentialProvider_Retrieve(t *testing.T) {
	c := new(CredentialProvider)
	mockAliyunCreds := mocks.NewCredential(t)
	c.aliyunCreds = mockAliyunCreds
	kind := "sts"
	mockAliyunCreds.EXPECT().GetType().Return(&kind).Maybe()

	ak := "ak"
	sk := "sk"
	token := "token"
	errMock := errors.Errorf("mock")

	t.Run("get ak or sk or token failed", func(t *testing.T) {
		mockAliyunCreds.EXPECT().GetAccessKeyId().Return(nil, errMock).Times(1)
		_, err := c.Retrieve()
		assert.Error(t, err)

		mockAliyunCreds.EXPECT().GetAccessKeyId().Return(&ak, nil).Times(1)
		mockAliyunCreds.EXPECT().GetAccessKeySecret().Return(nil, errMock).Times(1)
		_, err = c.Retrieve()
		assert.Error(t, err)

		mockAliyunCreds.EXPECT().GetAccessKeyId().Return(&ak, nil).Times(1)
		mockAliyunCreds.EXPECT().GetAccessKeySecret().Return(&sk, nil).Times(1)
		mockAliyunCreds.EXPECT().GetSecurityToken().Return(nil, errMock).Times(1)
		_, err = c.Retrieve()
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		mockAliyunCreds.EXPECT().GetAccessKeyId().Return(&ak, nil).Times(1)
		mockAliyunCreds.EXPECT().GetAccessKeySecret().Return(&sk, nil).Times(1)
		mockAliyunCreds.EXPECT().GetSecurityToken().Return(&token, nil).Times(1)
		ret, err := c.Retrieve()
		assert.NoError(t, err)
		assert.Equal(t, ak, ret.AccessKeyID)
		assert.Equal(t, sk, ret.SecretAccessKey)
		assert.Equal(t, token, ret.SessionToken)
	})
}

func TestCredentialProvider_IsExpired(t *testing.T) {
	// MinIO must call Retrieve so the shared cache can enforce real expiration.
	assert.True(t, new(CredentialProvider).IsExpired())
}
