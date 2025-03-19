package aliyun

import (
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/objectstorage/aliyun/mocks"
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
		_, err := NewMinioClient("", nil)
		assert.Error(t, err)
	})

	t.Run("iam ok", func(t *testing.T) {
		os.Setenv("ALIBABA_CLOUD_ROLE_ARN", "roleArn")
		os.Setenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN", "oidcProviderArn")
		os.Setenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE", "oidcTokenFilePath")
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
		assert.Equal(t, c.akCache, ret.AccessKeyID)
	})
}

func TestCredentialProvider_IsExpired(t *testing.T) {
	c := new(CredentialProvider)
	mockAliyunCreds := mocks.NewCredential(t)
	c.aliyunCreds = mockAliyunCreds

	ak := "ak"
	errMock := errors.Errorf("mock")
	t.Run("expired", func(t *testing.T) {
		mockAliyunCreds.EXPECT().GetAccessKeyId().Return(&ak, nil).Times(1)
		assert.True(t, c.IsExpired())
	})

	t.Run("not expired", func(t *testing.T) {
		c.akCache = ak
		mockAliyunCreds.EXPECT().GetAccessKeyId().Return(&ak, nil).Times(1)
		assert.False(t, c.IsExpired())
	})

	t.Run("get failed, assume expired", func(t *testing.T) {
		mockAliyunCreds.EXPECT().GetAccessKeyId().Return(nil, errMock).Times(1)
		assert.True(t, c.IsExpired())
	})
}
