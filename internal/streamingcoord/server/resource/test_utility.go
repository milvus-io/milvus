//go:build test
// +build test

package resource

// InitForTest initializes the singleton of resources for test.
func InitForTest(opts ...optResourceInit) {
	r = &resourceImpl{}
	for _, opt := range opts {
		opt(r)
	}
}
