package cgo

func getDefaultOpt() *options {
	return &options{
		name:     "unknown",
		releaser: nil,
	}
}

type options struct {
	name     string
	releaser func()
}

// Opt is the option type for future.
type Opt func(*options)

// WithReleaser sets the releaser function.
// When a future is ready, the releaser function will be called once.
func WithReleaser(releaser func()) Opt {
	return func(o *options) {
		o.releaser = releaser
	}
}

// WithName sets the name of the future.
// Only used for metrics.
func WithName(name string) Opt {
	return func(o *options) {
		o.name = name
	}
}
