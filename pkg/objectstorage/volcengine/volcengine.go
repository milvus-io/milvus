package volcengine

import (
	"fmt"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
)

const DefaultDurationSeconds = 8 * 60 * 60 // 8h

// NewMinioClient returns a minio.Client which is compatible for aliyun OSS
func NewMinioClient(address string, opts *minio.Options) (*minio.Client, error) {
	if opts == nil {
		opts = &minio.Options{}
	}
	if opts.Creds == nil {
		credProvider, err := NewCredentialProvider()
		if err != nil {
			return nil, errors.Wrap(err, "failed to create credential provider")
		}
		opts.Creds = minioCred.New(credProvider)
	}
	if address == "" {
		address = fmt.Sprintf("tos-%s.volces.com", opts.Region)
		opts.Secure = true
	}
	return minio.New(address, opts)
}

// CredentialProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type CredentialProvider struct {
	akCache         string
	volcengineCreds *credentials.Credentials
}

func NewCredentialProvider() (minioCred.Provider, error) {
	provider := credentials.NewOIDCCredentialsProviderFromEnv()
	provider.DurationSeconds = DefaultDurationSeconds
	cred := credentials.NewCredentials(provider)
	_, err := cred.Get()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get volcengine credential")
	}
	return &CredentialProvider{volcengineCreds: cred}, nil
}

// Retrieve returns nil if it successfully retrieved the value.
// Error is returned if the value were not obtainable, or empty.
// according to the caller minioCred.Credentials.Get(),
// it already has a lock, so we don't need to worry about concurrency
func (c *CredentialProvider) Retrieve() (minioCred.Value, error) {
	ret := minioCred.Value{}
	cred, err := c.volcengineCreds.Get()
	if err != nil {
		return ret, errors.Wrap(err, "failed to retrieve credential from volcengine volcengine")
	}
	ret.AccessKeyID = cred.AccessKeyID
	c.akCache = cred.AccessKeyID
	ret.SecretAccessKey = cred.SecretAccessKey
	ret.SessionToken = cred.SessionToken
	return ret, nil
}

// IsExpired returns if the credentials are no longer valid, and need
// to be retrieved.
// according to the caller minioCred.Credentials.IsExpired(),
// it already has a lock, so we don't need to worry about concurrency
func (c CredentialProvider) IsExpired() bool {
	return c.volcengineCreds.IsExpired()
}
