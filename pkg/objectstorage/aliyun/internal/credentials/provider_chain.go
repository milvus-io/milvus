// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

type providerChain struct {
	Providers []Provider
}

var (
	defaultproviders = []Provider{providerEnv, providerOIDC, providerProfile, providerInstance}
	defaultChain     = newProviderChain(defaultproviders)
)

func newProviderChain(providers []Provider) Provider {
	return &providerChain{
		Providers: providers,
	}
}

func (p *providerChain) resolve() (*Config, error) {
	for _, provider := range p.Providers {
		config, err := provider.resolve()
		if err != nil {
			return nil, err
		} else if config == nil {
			continue
		}
		return config, err
	}
	return nil, newCredentialError("No credential found")
}
