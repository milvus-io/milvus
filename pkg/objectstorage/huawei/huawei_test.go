package huawei

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/model"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockIAMClient implements iamTokenCreator for unit tests.
type mockIAMClient struct {
	response *model.CreateTemporaryAccessKeyByTokenResponse
	err      error
}

func (m *mockIAMClient) CreateTemporaryAccessKeyByToken(
	_ *model.CreateTemporaryAccessKeyByTokenRequest,
) (*model.CreateTemporaryAccessKeyByTokenResponse, error) {
	return m.response, m.err
}

func makeFullCredential(ak, sk, token, expiresAt string) *model.Credential {
	return &model.Credential{
		Access:        ak,
		Secret:        sk,
		Securitytoken: token,
		ExpiresAt:     expiresAt,
	}
}

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

func TestNewCredentialProvider_Singleton(t *testing.T) {
	// Reset the global singleton for this test
	globalCredProviderMu.Lock()
	globalCredProvider = nil
	globalCredProviderMu.Unlock()

	p1 := NewCredentialProvider()
	p2 := NewCredentialProvider()
	// Both calls should return the same singleton instance
	assert.Same(t, p1, p2)

	// Clean up
	globalCredProviderMu.Lock()
	globalCredProvider = nil
	globalCredProviderMu.Unlock()
}

func TestNewCredentialProvider_ConcurrentAccess(t *testing.T) {
	globalCredProviderMu.Lock()
	globalCredProvider = nil
	globalCredProviderMu.Unlock()

	var wg sync.WaitGroup
	results := make([]credentials.Provider, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = NewCredentialProvider()
		}(i)
	}
	wg.Wait()

	// All goroutines should get the same instance
	for i := 1; i < 10; i++ {
		assert.Same(t, results[0], results[i])
	}

	globalCredProviderMu.Lock()
	globalCredProvider = nil
	globalCredProviderMu.Unlock()
}

func TestHuaweiCredentialProvider_Retrieve(t *testing.T) {
	t.Run("not initialized", func(t *testing.T) {
		c := &HuaweiCredentialProvider{}
		// Without proper env vars, initClients will fail and Retrieve returns error
		_, err := c.Retrieve()
		assert.Error(t, err)
	})

	t.Run("returns cached credentials when not expired", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			inited: true,
			credentials: credentials.Value{
				AccessKeyID:     "CACHED_AK",
				SecretAccessKey: "CACHED_SK",
				SessionToken:    "CACHED_TOKEN",
				SignerType:      credentials.SignatureV4,
			},
			expiration: time.Now().UTC().Add(1 * time.Hour),
		}

		val, err := c.Retrieve()
		assert.NoError(t, err)
		assert.Equal(t, "CACHED_AK", val.AccessKeyID)
		assert.Equal(t, "CACHED_SK", val.SecretAccessKey)
		assert.Equal(t, "CACHED_TOKEN", val.SessionToken)
	})

	t.Run("re-init allowed after previous init failure", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			inited: false,
		}
		// First call fails (no env vars)
		_, err := c.Retrieve()
		assert.Error(t, err)
		// inited should still be false, allowing retry
		assert.False(t, c.inited)

		// Second call also fails but demonstrates retryability
		_, err = c.Retrieve()
		assert.Error(t, err)
	})
}

func TestHuaweiCredentialProvider_IsExpired(t *testing.T) {
	c := &HuaweiCredentialProvider{}

	t.Run("expired - zero time", func(t *testing.T) {
		assert.True(t, c.IsExpired())
	})

	t.Run("expired - past time", func(t *testing.T) {
		c.refreshMu.Lock()
		c.expiration = time.Now().UTC().Add(-10 * time.Minute)
		c.refreshMu.Unlock()
		assert.True(t, c.IsExpired())
	})

	t.Run("expired - within refresh window", func(t *testing.T) {
		c.refreshMu.Lock()
		c.expiration = time.Now().UTC().Add(2 * time.Minute) // within 3min grace period
		c.refreshMu.Unlock()
		assert.True(t, c.IsExpired())
	})

	t.Run("always expired - forces minio to call Retrieve", func(t *testing.T) {
		c.refreshMu.Lock()
		c.expiration = time.Now().UTC().Add(10 * time.Minute)
		c.refreshMu.Unlock()
		// IsExpired() always returns true so minio always calls Retrieve(),
		// which has its own cache-hit fast path.
		assert.True(t, c.IsExpired())
	})
}

func TestHuaweiCredentialProvider_InitClientsIdempotent(t *testing.T) {
	c := &HuaweiCredentialProvider{}
	// First call fails (no env vars)
	err1 := c.initClients()
	assert.Error(t, err1)
	assert.False(t, c.inited)

	// Second call also runs (not blocked by sync.Once), allowing retry
	err2 := c.initClients()
	assert.Error(t, err2)
	assert.False(t, c.inited)
}

func TestHuaweiCredentialProvider_IsInCooldown(t *testing.T) {
	t.Run("not in cooldown when no failure", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			lastReloadFailed: false,
		}
		assert.False(t, c.isInCooldown())
	})

	t.Run("urgent cooldown with empty credentials", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			lastReloadFailed:     true,
			lastFailedReloadTime: time.Now(),
			// expiration is zero → urgent cooldown (5s)
		}
		assert.True(t, c.isInCooldown())
	})

	t.Run("urgent cooldown expires after 5s", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			lastReloadFailed:     true,
			lastFailedReloadTime: time.Now().Add(-6 * time.Second),
			// expiration is zero → urgent cooldown (5s), 6s elapsed → expired
		}
		assert.False(t, c.isInCooldown())
	})

	t.Run("normal cooldown with valid credentials", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			lastReloadFailed:     true,
			lastFailedReloadTime: time.Now(),
			expiration:           time.Now().UTC().Add(1 * time.Hour), // valid creds → normal cooldown (30s)
		}
		assert.True(t, c.isInCooldown())
	})

	t.Run("normal cooldown expires after 30s", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			lastReloadFailed:     true,
			lastFailedReloadTime: time.Now().Add(-31 * time.Second),
			expiration:           time.Now().UTC().Add(1 * time.Hour), // valid creds → normal cooldown (30s)
		}
		assert.False(t, c.isInCooldown())
	})

	t.Run("urgent cooldown when credentials expired", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			lastReloadFailed:     true,
			lastFailedReloadTime: time.Now(),
			expiration:           time.Now().UTC().Add(-10 * time.Minute), // expired → urgent cooldown
		}
		assert.True(t, c.isInCooldown())
	})
}

func TestHuaweiCredentialProvider_RetrieveCooldown(t *testing.T) {
	t.Run("returns cached creds during cooldown", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			inited:               true,
			lastReloadFailed:     true,
			lastFailedReloadTime: time.Now(),
			credentials: credentials.Value{
				AccessKeyID:     "OLD_AK",
				SecretAccessKey: "OLD_SK",
				SessionToken:    "OLD_TOKEN",
				SignerType:      credentials.SignatureV4,
			},
			expiration: time.Now().UTC().Add(1 * time.Minute), // within 3min grace period, triggers refresh
		}

		val, err := c.Retrieve()
		assert.NoError(t, err)
		assert.Equal(t, "OLD_AK", val.AccessKeyID)
	})

	t.Run("returns error during cooldown with no cached creds", func(t *testing.T) {
		c := &HuaweiCredentialProvider{
			inited:               true,
			lastReloadFailed:     true,
			lastFailedReloadTime: time.Now(),
			// no cached credentials, expiration is zero
		}

		_, err := c.Retrieve()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cooldown")
	})
}

// TestNewMinioClient_NilOpts covers the opts==nil branch in NewMinioClient.
func TestNewMinioClient_NilOpts(t *testing.T) {
	client, err := NewMinioClient(OBSDefaultAddress+":443", nil)
	require.NoError(t, err)
	assert.Equal(t, OBSDefaultAddress+":443", client.EndpointURL().Host)
}

func TestHuaweiCredentialProvider_Retrieve_STSError_WithCachedCreds(t *testing.T) {
	c := &HuaweiCredentialProvider{
		inited: true,
		iamClient: &mockIAMClient{
			err: errors.New("STS network error"),
		},
		credentials: credentials.Value{
			AccessKeyID:     "CACHED_AK",
			SecretAccessKey: "CACHED_SK",
			SessionToken:    "CACHED_TOKEN",
			SignerType:      credentials.SignatureV4,
		},
		expiration: time.Now().UTC().Add(1 * time.Minute),
	}

	val, err := c.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "CACHED_AK", val.AccessKeyID)
	assert.True(t, c.lastReloadFailed)
}

func TestHuaweiCredentialProvider_Retrieve_STSError_NoCachedCreds(t *testing.T) {
	c := &HuaweiCredentialProvider{
		inited: true,
		iamClient: &mockIAMClient{
			err: errors.New("STS network error"),
		},
	}

	_, err := c.Retrieve()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create temporary access key")
	assert.True(t, c.lastReloadFailed)
}

func TestHuaweiCredentialProvider_Retrieve_NilCredential_NoCachedCreds(t *testing.T) {
	c := &HuaweiCredentialProvider{
		inited: true,
		iamClient: &mockIAMClient{
			response: &model.CreateTemporaryAccessKeyByTokenResponse{
				Credential: nil,
			},
		},
	}

	_, err := c.Retrieve()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete credential")
}

func TestHuaweiCredentialProvider_Retrieve_NilCredential_WithCachedCreds(t *testing.T) {
	c := &HuaweiCredentialProvider{
		inited: true,
		iamClient: &mockIAMClient{
			response: &model.CreateTemporaryAccessKeyByTokenResponse{
				Credential: nil,
			},
		},
		credentials: credentials.Value{
			AccessKeyID:     "CACHED_AK",
			SecretAccessKey: "CACHED_SK",
			SessionToken:    "CACHED_TOKEN",
			SignerType:      credentials.SignatureV4,
		},
		expiration: time.Now().UTC().Add(1 * time.Minute),
	}

	val, err := c.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "CACHED_AK", val.AccessKeyID)
}

func TestHuaweiCredentialProvider_Retrieve_BadExpiration_NoCachedCreds(t *testing.T) {
	c := &HuaweiCredentialProvider{
		inited: true,
		iamClient: &mockIAMClient{
			response: &model.CreateTemporaryAccessKeyByTokenResponse{
				Credential: makeFullCredential("AK", "SK", "TOKEN", "NOT-A-DATE"),
			},
		},
	}

	_, err := c.Retrieve()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse expiration time")
}

func TestHuaweiCredentialProvider_Retrieve_BadExpiration_WithCachedCreds(t *testing.T) {
	c := &HuaweiCredentialProvider{
		inited: true,
		iamClient: &mockIAMClient{
			response: &model.CreateTemporaryAccessKeyByTokenResponse{
				Credential: makeFullCredential("AK", "SK", "TOKEN", "NOT-A-DATE"),
			},
		},
		credentials: credentials.Value{
			AccessKeyID:     "CACHED_AK",
			SecretAccessKey: "CACHED_SK",
			SessionToken:    "CACHED_TOKEN",
			SignerType:      credentials.SignatureV4,
		},
		expiration: time.Now().UTC().Add(1 * time.Minute),
	}

	val, err := c.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "CACHED_AK", val.AccessKeyID)
}

func TestHuaweiCredentialProvider_Retrieve_Success(t *testing.T) {
	futureExpiry := time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339)
	c := &HuaweiCredentialProvider{
		inited: true,
		iamClient: &mockIAMClient{
			response: &model.CreateTemporaryAccessKeyByTokenResponse{
				Credential: makeFullCredential("NEW_AK", "NEW_SK", "NEW_TOKEN", futureExpiry),
			},
		},
	}

	val, err := c.Retrieve()
	require.NoError(t, err)
	assert.Equal(t, "NEW_AK", val.AccessKeyID)
	assert.Equal(t, "NEW_SK", val.SecretAccessKey)
	assert.Equal(t, "NEW_TOKEN", val.SessionToken)
	assert.Equal(t, credentials.SignatureV4, val.SignerType)
	assert.Equal(t, "NEW_AK", c.credentials.AccessKeyID)
	assert.False(t, c.expiration.IsZero())
	assert.False(t, c.lastReloadFailed)
	assert.Equal(t, int64(1), c.stsSuccessCount.Load())
}

func TestHuaweiCredentialProvider_Retrieve_CacheHitAfterSuccess(t *testing.T) {
	futureExpiry := time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339)
	mc := &mockIAMClient{
		response: &model.CreateTemporaryAccessKeyByTokenResponse{
			Credential: makeFullCredential("AK", "SK", "TOKEN", futureExpiry),
		},
	}

	c := &HuaweiCredentialProvider{inited: true, iamClient: mc}

	_, err := c.Retrieve()
	require.NoError(t, err)

	// Second call: expiration is 2h away, well past the 3min grace — should hit cache
	_, err = c.Retrieve()
	require.NoError(t, err)
}
