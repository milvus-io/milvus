package parser

import "testing"

func TestConfig_Apply(t *testing.T) {
	opts := []Option{
		func(c *Config) {
		},
		func(c *Config) {
		},
	}
	c := defaultParserConfig()
	c.Apply(opts...)
}
