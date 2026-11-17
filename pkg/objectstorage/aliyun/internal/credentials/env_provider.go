// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import (
	"os"

	"github.com/alibabacloud-go/tea/tea"
)

type envProvider struct{}

var providerEnv = new(envProvider)

const (
	// EnvVarAccessKeyId is a name of ALIBABA_CLOUD_ACCESS_KEY_Id
	EnvVarAccessKeyId    = "ALIBABA_CLOUD_ACCESS_KEY_Id"
	EnvVarAccessKeyIdNew = "ALIBABA_CLOUD_ACCESS_KEY_ID"
	// EnvVarAccessKeySecret is a name of ALIBABA_CLOUD_ACCESS_KEY_SECRET
	EnvVarAccessKeySecret = "ALIBABA_CLOUD_ACCESS_KEY_SECRET"
)

func newEnvProvider() Provider {
	return &envProvider{}
}

func (p *envProvider) resolve() (*Config, error) {
	accessKeyId, ok1 := os.LookupEnv(EnvVarAccessKeyIdNew)
	if !ok1 || accessKeyId == "" {
		accessKeyId, ok1 = os.LookupEnv(EnvVarAccessKeyId)
	}
	accessKeySecret, ok2 := os.LookupEnv(EnvVarAccessKeySecret)
	if !ok1 || !ok2 {
		return nil, nil
	}
	if accessKeyId == "" {
		return nil, newCredentialError(EnvVarAccessKeyIdNew + " or " + EnvVarAccessKeyId + " cannot be empty")
	}
	if accessKeySecret == "" {
		return nil, newCredentialError(EnvVarAccessKeySecret + " cannot be empty")
	}
	config := &Config{
		Type:            tea.String("access_key"),
		AccessKeyId:     tea.String(accessKeyId),
		AccessKeySecret: tea.String(accessKeySecret),
	}
	return config, nil
}
