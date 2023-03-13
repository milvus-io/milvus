package parser

type Config struct{}

type Option = func(c *Config)

func (c *Config) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(c)
	}
}

func defaultParserConfig() Config {
	return Config{}
}
