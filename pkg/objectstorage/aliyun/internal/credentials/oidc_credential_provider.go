// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import (
	"os"

	"github.com/alibabacloud-go/tea/tea"
)

type oidcCredentialsProvider struct{}

var providerOIDC = new(oidcCredentialsProvider)

func newOidcCredentialsProvider() Provider {
	return &oidcCredentialsProvider{}
}

func (p *oidcCredentialsProvider) resolve() (*Config, error) {
	roleArn, ok1 := os.LookupEnv(ENVRoleArn)
	oidcProviderArn, ok2 := os.LookupEnv(ENVOIDCProviderArn)
	oidcTokenFilePath, ok3 := os.LookupEnv(ENVOIDCTokenFile)
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}

	config := &Config{
		Type:              tea.String("oidc_role_arn"),
		RoleArn:           tea.String(roleArn),
		OIDCProviderArn:   tea.String(oidcProviderArn),
		OIDCTokenFilePath: tea.String(oidcTokenFilePath),
		RoleSessionName:   tea.String("defaultSessionName"),
	}
	roleSessionName, ok := os.LookupEnv(ENVRoleSessionName)
	if ok {
		config.RoleSessionName = tea.String(roleSessionName)
	}
	return config, nil
}
