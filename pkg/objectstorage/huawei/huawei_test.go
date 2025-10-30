package huawei

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
)

const OBSDefaultAddress = "obs.cn-east-3.myhuaweicloud.com"

func TestNewMinioClient(t *testing.T) {
	t.Run("ak sk ok", func(t *testing.T) {
		minioCli, err := NewMinioClient(OBSDefaultAddress+":443", &minio.Options{
			Creds:  credentials.NewStaticV4("ak", "sk", ""),
			Secure: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, OBSDefaultAddress+":443", minioCli.EndpointURL().Host)
		assert.Equal(t, "https", minioCli.EndpointURL().Scheme)
	})

	t.Run("iam ok", func(t *testing.T) {
		minioCli, err := NewMinioClient("", &minio.Options{Region: "cn-east-3"})
		assert.NoError(t, err)
		assert.Equal(t, "obs.cn-east-3.myhuaweicloud.com", minioCli.EndpointURL().Host)
		assert.Equal(t, "https", minioCli.EndpointURL().Scheme)
	})
}

func TestHuaweiCredentialProvider_Retrieve(t *testing.T) {
	// Skip detailed mocking tests for now, as they require complex setup
	// This test focuses on the basic functionality
	t.Run("init error", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			initErr: errors.New("init failed"),
		}
		c.initOnce.Do(func() {}) // Mark as initialized

		_, err := c.Retrieve()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "init failed")
	})
}

func TestHuaweiCredentialProvider_IsExpired(t *testing.T) {
	c := &HuaweiCredentialProvider{}

	t.Run("expired - zero time", func(t *testing.T) {
		assert.True(t, c.IsExpired())
	})

	t.Run("expired - past time", func(t *testing.T) {
		c.expiration = time.Now().UTC().Add(-10 * time.Minute)
		assert.True(t, c.IsExpired())
	})

	t.Run("expired - within refresh window", func(t *testing.T) {
		c.expiration = time.Now().UTC().Add(3 * time.Minute)
		assert.True(t, c.IsExpired())
	})

	t.Run("not expired", func(t *testing.T) {
		c.expiration = time.Now().UTC().Add(10 * time.Minute)
		assert.False(t, c.IsExpired())
	})
}
