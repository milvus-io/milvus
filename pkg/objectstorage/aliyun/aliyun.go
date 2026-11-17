package aliyun

import (
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage/credentialcache"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	OSSAddressFeatureString = "aliyuncs.com"
	OSSDefaultAddress       = "oss.aliyuncs.com"
)

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
		address = OSSDefaultAddress
		opts.Secure = true
	}
	return minio.New(address, opts)
}

// Credential is defined to mock aliyun credential.Credentials
//
//go:generate mockery --name=Credential --with-expecter
type Credential interface {
	credentials.Credential
}

// CredentialProvider implements "github.com/minio/minio-go/v7/pkg/credentials".Provider
// also implements transport
type CredentialProvider struct {
	aliyunCreds Credential
	once        sync.Once
	cache       *credentialcache.Provider
}

func NewCredentialProvider() (minioCred.Provider, error) {
	aliyunCreds, err := credentials.NewCredential(nil)
	if err != nil {
		return nil, merr.WrapErrServiceUnavailableErr(err, "initialize Aliyun credential source")
	}

	return &CredentialProvider{aliyunCreds: aliyunCreds}, nil
}

// Retrieve delegates source selection to the compatibility SDK and caches only
// complete atomic tuples with their real expiration.
func (c *CredentialProvider) Retrieve() (minioCred.Value, error) {
	c.once.Do(func() {
		kind := ""
		if c.aliyunCreds != nil && c.aliyunCreds.GetType() != nil {
			kind = *c.aliyunCreds.GetType()
		}
		allowStatic := kind == "access_key" || kind == "sts"
		requireToken := kind != "access_key" && kind != "rsa_key_pair"
		c.cache = credentialcache.New(func() (minioCred.Value, error) {
			snapshot, err := credentials.RetrieveSnapshot(c.aliyunCreds)
			if err != nil {
				return minioCred.Value{}, merr.WrapErrServiceUnavailableErr(err, "refresh Aliyun credential source")
			}
			return minioCred.Value{
				AccessKeyID: snapshot.AccessKeyId, SecretAccessKey: snapshot.AccessKeySecret,
				SessionToken: snapshot.SecurityToken, Expiration: snapshot.Expiration,
				SignerType: minioCred.SignatureV4,
			}, nil
		}, 3*time.Minute, requireToken, allowStatic)
	})
	return c.cache.Retrieve()
}

// Always enter the shared provider; its cache-hit path does not fetch remotely.
func (c *CredentialProvider) IsExpired() bool { return true }
