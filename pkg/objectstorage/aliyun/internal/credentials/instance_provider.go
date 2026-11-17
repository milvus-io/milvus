// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import (
	"os"

	"github.com/alibabacloud-go/tea/tea"
)

type instanceCredentialsProvider struct{}

var providerInstance = new(instanceCredentialsProvider)

func newInstanceCredentialsProvider() Provider {
	return &instanceCredentialsProvider{}
}

func (p *instanceCredentialsProvider) resolve() (*Config, error) {
	roleName, ok := os.LookupEnv(ENVEcsMetadata)
	if !ok {
		return nil, nil
	}

	config := &Config{
		Type:     tea.String("ecs_ram_role"),
		RoleName: tea.String(roleName),
	}
	return config, nil
}
