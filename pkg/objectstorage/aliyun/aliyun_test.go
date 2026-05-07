package aliyun

import (
	"os"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
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
	ak := "ak"
	sk := "sk"
	token := "token"
	errMock := errors.Errorf("mock")

	t.Run("get ak or sk or token failed", func(t *testing.T) {
		t.Run("ak", func(t *testing.T) {
			fakeAliyunCreds := &struct{ Credential }{}
			calls := &credentialCalls{}
			mockAK := mockey.Mock((*struct{ Credential }).GetAccessKeyId).To(
				func(*struct{ Credential }) (*string, error) {
					calls.ak++
					return nil, errMock
				},
			).Build()
			defer mockAK.UnPatch()

			c := &CredentialProvider{aliyunCreds: fakeAliyunCreds}
			_, err := c.Retrieve()
			assert.Error(t, err)
			assertCredentialCalls(t, calls, 1, 0, 0)
		})

		t.Run("sk", func(t *testing.T) {
			fakeAliyunCreds := &struct{ Credential }{}
			calls := &credentialCalls{}
			mockAK := mockey.Mock((*struct{ Credential }).GetAccessKeyId).To(
				func(*struct{ Credential }) (*string, error) {
					calls.ak++
					return &ak, nil
				},
			).Build()
			defer mockAK.UnPatch()
			mockSK := mockey.Mock((*struct{ Credential }).GetAccessKeySecret).To(
				func(*struct{ Credential }) (*string, error) {
					calls.sk++
					return nil, errMock
				},
			).Build()
			defer mockSK.UnPatch()

			c := &CredentialProvider{aliyunCreds: fakeAliyunCreds}
			_, err := c.Retrieve()
			assert.Error(t, err)
			assertCredentialCalls(t, calls, 1, 1, 0)
		})

		t.Run("token", func(t *testing.T) {
			fakeAliyunCreds := &struct{ Credential }{}
			calls := &credentialCalls{}
			mockAK := mockey.Mock((*struct{ Credential }).GetAccessKeyId).To(
				func(*struct{ Credential }) (*string, error) {
					calls.ak++
					return &ak, nil
				},
			).Build()
			defer mockAK.UnPatch()
			mockSK := mockey.Mock((*struct{ Credential }).GetAccessKeySecret).To(
				func(*struct{ Credential }) (*string, error) {
					calls.sk++
					return &sk, nil
				},
			).Build()
			defer mockSK.UnPatch()
			mockToken := mockey.Mock((*struct{ Credential }).GetSecurityToken).To(
				func(*struct{ Credential }) (*string, error) {
					calls.token++
					return nil, errMock
				},
			).Build()
			defer mockToken.UnPatch()

			c := &CredentialProvider{aliyunCreds: fakeAliyunCreds}
			_, err := c.Retrieve()
			assert.Error(t, err)
			assertCredentialCalls(t, calls, 1, 1, 1)
		})
	})

	t.Run("ok", func(t *testing.T) {
		fakeAliyunCreds := &struct{ Credential }{}
		calls := &credentialCalls{}
		mockAK := mockey.Mock((*struct{ Credential }).GetAccessKeyId).To(
			func(*struct{ Credential }) (*string, error) {
				calls.ak++
				return &ak, nil
			},
		).Build()
		defer mockAK.UnPatch()
		mockSK := mockey.Mock((*struct{ Credential }).GetAccessKeySecret).To(
			func(*struct{ Credential }) (*string, error) {
				calls.sk++
				return &sk, nil
			},
		).Build()
		defer mockSK.UnPatch()
		mockToken := mockey.Mock((*struct{ Credential }).GetSecurityToken).To(
			func(*struct{ Credential }) (*string, error) {
				calls.token++
				return &token, nil
			},
		).Build()
		defer mockToken.UnPatch()

		c := &CredentialProvider{aliyunCreds: fakeAliyunCreds}
		ret, err := c.Retrieve()
		assert.NoError(t, err)
		assert.Equal(t, ak, ret.AccessKeyID)
		assert.Equal(t, sk, ret.SecretAccessKey)
		assert.Equal(t, token, ret.SessionToken)
		assert.Equal(t, c.akCache, ret.AccessKeyID)
		assertCredentialCalls(t, calls, 1, 1, 1)
	})
}

func TestCredentialProvider_IsExpired(t *testing.T) {
	ak := "ak"
	errMock := errors.Errorf("mock")
	t.Run("expired", func(t *testing.T) {
		fakeAliyunCreds := &struct{ Credential }{}
		calls := &credentialCalls{}
		mockAK := mockey.Mock((*struct{ Credential }).GetAccessKeyId).To(
			func(*struct{ Credential }) (*string, error) {
				calls.ak++
				return &ak, nil
			},
		).Build()
		defer mockAK.UnPatch()

		c := &CredentialProvider{aliyunCreds: fakeAliyunCreds}
		assert.True(t, c.IsExpired())
		assertCredentialCalls(t, calls, 1, 0, 0)
	})

	t.Run("not expired", func(t *testing.T) {
		fakeAliyunCreds := &struct{ Credential }{}
		calls := &credentialCalls{}
		mockAK := mockey.Mock((*struct{ Credential }).GetAccessKeyId).To(
			func(*struct{ Credential }) (*string, error) {
				calls.ak++
				return &ak, nil
			},
		).Build()
		defer mockAK.UnPatch()

		c := &CredentialProvider{aliyunCreds: fakeAliyunCreds}
		c.akCache = ak
		assert.False(t, c.IsExpired())
		assertCredentialCalls(t, calls, 1, 0, 0)
	})

	t.Run("get failed, assume expired", func(t *testing.T) {
		fakeAliyunCreds := &struct{ Credential }{}
		calls := &credentialCalls{}
		mockAK := mockey.Mock((*struct{ Credential }).GetAccessKeyId).To(
			func(*struct{ Credential }) (*string, error) {
				calls.ak++
				return nil, errMock
			},
		).Build()
		defer mockAK.UnPatch()

		c := &CredentialProvider{aliyunCreds: fakeAliyunCreds}
		assert.True(t, c.IsExpired())
		assertCredentialCalls(t, calls, 1, 0, 0)
	})
}

type credentialCalls struct {
	ak    int
	sk    int
	token int
}

func assertCredentialCalls(t *testing.T, calls *credentialCalls, akCalls int, skCalls int, tokenCalls int) {
	t.Helper()

	assert.Equal(t, akCalls, calls.ak)
	assert.Equal(t, skCalls, calls.sk)
	assert.Equal(t, tokenCalls, calls.token)
}
